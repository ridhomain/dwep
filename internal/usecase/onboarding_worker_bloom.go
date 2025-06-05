// internal/usecase/onboarding_worker_bloom.go
package usecase

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/panjf2000/ants/v2"
	"go.uber.org/zap"

	apperrors "gitlab.com/timkado/api/daisi-wa-events-processor/internal/apperrors"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/cache"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/config"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/model"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/observer"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/storage"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/tenant"
	"gitlab.com/timkado/api/daisi-wa-events-processor/pkg/logger"
	"gitlab.com/timkado/api/daisi-wa-events-processor/pkg/utils"
)

// OnboardingWorker manages the worker pool with bloom filter optimization
type OnboardingWorker struct {
	pool            *ants.PoolWithFunc
	contactRepo     storage.ContactRepo
	logRepo         storage.OnboardingLogRepo
	cfg             config.OnboardingWorkerPoolConfig
	baseLogger      *zap.Logger
	onboardingCache *cache.OnboardingCache
	batchProcessor  *BatchProcessor
	companyID       string
	stopChan        chan struct{}
	wg              sync.WaitGroup
}

// NewOnboardingWorker creates an optimized onboarding worker with bloom filters
func NewOnboardingWorker(
	cfg config.OnboardingWorkerPoolConfig,
	contactRepo storage.ContactRepo,
	logRepo storage.OnboardingLogRepo,
	baseLogger *zap.Logger,
	companyID string,
) (*OnboardingWorker, error) {
	// Create dual bloom filter cache
	// Expected: 500K onboarded, 100K non-existent, 0.01% FP rate
	onboardingCache := cache.NewOnboardingCache(
		companyID,
		500000, // Expected onboarded contacts
		100000, // Expected non-existent lookups
		0.01,   // False positive rate
	)

	// Create batch processor for logs
	batchProcessor := NewBatchProcessor(logRepo, 100)

	worker := &OnboardingWorker{
		contactRepo:     contactRepo,
		logRepo:         logRepo,
		cfg:             cfg,
		baseLogger:      baseLogger.Named("onboarding_worker"),
		onboardingCache: onboardingCache,
		batchProcessor:  batchProcessor,
		companyID:       companyID,
		stopChan:        make(chan struct{}),
	}

	// Create worker pool
	pool, err := ants.NewPoolWithFunc(cfg.PoolSize, func(i interface{}) {
		taskData, ok := i.(OnboardingTaskData)
		if !ok {
			worker.baseLogger.Error("Invalid task data type received", zap.Any("data", i))
			return
		}
		worker.processOnboardingTask(taskData)
	},
		ants.WithExpiryDuration(cfg.ExpiryTime),
		ants.WithNonblocking(false),
		ants.WithMaxBlockingTasks(cfg.QueueSize),
		ants.WithPanicHandler(func(err interface{}) {
			worker.baseLogger.Error("Panic recovered in onboarding worker",
				zap.Any("panic_error", err),
				zap.Stack("stack"))
			observer.IncWorkerPanic(companyID, "onboarding")
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create onboarding worker pool: %w", err)
	}

	worker.pool = pool

	// Start cache warmup in background
	worker.wg.Add(1)
	go func() {
		defer worker.wg.Done()
		worker.warmupBloomFilters()
	}()

	// Start periodic stats reporting
	worker.wg.Add(1)
	go func() {
		defer worker.wg.Done()
		worker.reportStats()
	}()

	worker.baseLogger.Info("Onboarding worker initialized with bloom filters",
		zap.String("company_id", companyID),
		zap.Int("pool_size", cfg.PoolSize),
		zap.Int("queue_size", cfg.QueueSize))

	return worker, nil
}

// SubmitTask submits a new onboarding check task to the worker pool
func (w *OnboardingWorker) SubmitTask(taskData OnboardingTaskData) error {
	start := time.Now()
	observer.IncOnboardingTasksSubmitted(w.companyID)
	observer.SetOnboardingQueueLength(w.pool.Waiting())

	err := w.pool.Invoke(taskData)
	duration := time.Since(start)

	if err != nil {
		w.baseLogger.Warn("Failed to submit onboarding task",
			zap.String("message_id", taskData.Message.MessageID),
			zap.Duration("submit_duration", duration),
			zap.Error(err))
		observer.IncOnboardingTasksProcessed(w.companyID, "submit_error")

		if errors.Is(err, ants.ErrPoolOverload) {
			return fmt.Errorf("onboarding pool overload: %w", err)
		}
		return fmt.Errorf("failed to invoke onboarding task: %w", err)
	}

	return nil
}

// processOnboardingTask processes a single onboarding task
func (w *OnboardingWorker) processOnboardingTask(taskData OnboardingTaskData) {
	log := logger.FromContextOr(taskData.Ctx, w.baseLogger).With(
		zap.String("task_message_id", taskData.Message.MessageID),
		zap.String("task_company_id", taskData.Message.CompanyID))

	start := time.Now()
	status := "success"

	taskCtx := tenant.WithCompanyID(taskData.Ctx, taskData.Message.CompanyID)

	// Clean phone number
	phoneNumber := cleanPhoneNumber(taskData.Message.FromUser)
	if phoneNumber == "" {
		observer.IncOnboardingTasksProcessed(w.companyID, "skipped_empty_phone")
		return
	}

	// STEP 1: Check bloom filters
	cacheStatus := w.onboardingCache.CheckOnboardingStatus(phoneNumber, taskData.Message.AgentID)

	switch cacheStatus {
	case cache.StatusMaybeOnboarded:
		// Might be onboarded, need to check DB
		contact, err := w.contactRepo.FindByPhoneAndAgentID(taskCtx, phoneNumber, taskData.Message.AgentID)
		if err != nil && !errors.Is(err, apperrors.ErrNotFound) {
			log.Error("Error checking potentially onboarded contact",
				zap.String("phone_number", phoneNumber),
				zap.Error(err))
			observer.IncOnboardingTasksProcessed(w.companyID, "failure_contact_check")
			return
		}

		if contact != nil && contact.Origin != "" {
			// Confirmed: already onboarded
			log.Debug("Skipping: Contact already onboarded (confirmed)",
				zap.String("origin", contact.Origin))
			observer.IncOnboardingTasksProcessed(w.companyID, "skipped_already_onboarded")
			return
		} else {
			// False positive - not actually onboarded
			w.onboardingCache.RecordFalsePositive("onboarded")
			// Continue to process
		}

	case cache.StatusMaybeNotExist:
		// Might not exist, need to check DB
		contact, err := w.contactRepo.FindByPhoneAndAgentID(taskCtx, phoneNumber, taskData.Message.AgentID)
		if err != nil && !errors.Is(err, apperrors.ErrNotFound) {
			log.Error("Error checking potentially non-existent contact",
				zap.String("phone_number", phoneNumber),
				zap.Error(err))
			observer.IncOnboardingTasksProcessed(w.companyID, "failure_contact_check")
			return
		}

		if contact == nil {
			// Confirmed: doesn't exist - create new
			w.createNewContact(taskCtx, taskData, phoneNumber, &status)
			goto metrics
		} else {
			// False positive - actually exists
			w.onboardingCache.RecordFalsePositive("nonexist")

			if contact.Origin == "" {
				// Exists but not onboarded - update
				w.updateExistingContact(taskCtx, taskData, contact, phoneNumber, &status)
			} else {
				// Already onboarded
				w.onboardingCache.MarkOnboarded(phoneNumber, taskData.Message.AgentID)
				log.Debug("Skipping: Contact already onboarded",
					zap.String("origin", contact.Origin))
				status = "skipped_already_onboarded"
			}
			goto metrics
		}

	case cache.StatusUnknown:
		// Not in cache, need full DB check
		contact, err := w.contactRepo.FindByPhoneAndAgentID(taskCtx, phoneNumber, taskData.Message.AgentID)
		if err != nil && !errors.Is(err, apperrors.ErrNotFound) {
			log.Error("Error checking contact existence",
				zap.String("phone_number", phoneNumber),
				zap.Error(err))
			observer.IncOnboardingTasksProcessed(w.companyID, "failure_contact_check")
			return
		}

		if contact == nil {
			// Doesn't exist - create and mark in cache
			w.createNewContact(taskCtx, taskData, phoneNumber, &status)
			w.onboardingCache.MarkNonExistent(phoneNumber, taskData.Message.AgentID)
		} else if contact.Origin == "" {
			// Exists but not onboarded - update
			w.updateExistingContact(taskCtx, taskData, contact, phoneNumber, &status)
		} else {
			// Already onboarded - mark in cache
			w.onboardingCache.MarkOnboarded(phoneNumber, taskData.Message.AgentID)
			log.Debug("Skipping: Contact already onboarded",
				zap.String("origin", contact.Origin))
			status = "skipped_already_onboarded"
		}
	}

metrics:
	// Record metrics
	duration := time.Since(start)
	observer.ObserveOnboardingProcessingDuration(w.companyID, duration)
	observer.IncOnboardingTasksProcessed(w.companyID, status)
	observer.SetOnboardingWorkersActive(w.pool.Running())
}

// createNewContact creates a new contact during onboarding
func (w *OnboardingWorker) createNewContact(ctx context.Context, taskData OnboardingTaskData, phoneNumber string, status *string) {
	log := logger.FromContext(ctx)

	contactUUID := uuid.New().String()
	newContact := model.Contact{
		ID:                    contactUUID,
		PhoneNumber:           phoneNumber,
		CompanyID:             taskData.Message.CompanyID,
		FirstMessageID:        taskData.Message.MessageID,
		FirstMessageTimestamp: taskData.Message.MessageTimestamp,
		Origin:                "onboarding",
		AgentID:               taskData.Message.AgentID,
		LastMetadata:          taskData.MetadataJSON,
		CreatedAt:             utils.Now(),
		UpdatedAt:             utils.Now(),
	}

	// Apply onboarding metadata
	if taskData.OnboardingMetadata != nil {
		if tags, ok := taskData.OnboardingMetadata["tags"].(string); ok {
			newContact.Tags = tags
		}
		if assignedTo, ok := taskData.OnboardingMetadata["assigned_to"].(string); ok {
			newContact.AssignedTo = assignedTo
		}
		if notes, ok := taskData.OnboardingMetadata["notes"].(string); ok {
			newContact.Notes = notes
		}
	}

	if err := w.contactRepo.Save(ctx, newContact); err != nil {
		log.Error("Error saving new contact", zap.Error(err))
		*status = "failure_contact_save"
		return
	}

	// Mark as onboarded in cache
	w.onboardingCache.MarkOnboarded(phoneNumber, taskData.Message.AgentID)

	// Add to batch processor
	w.batchProcessor.Add(ctx, model.OnboardingLog{
		MessageID:    taskData.Message.MessageID,
		AgentID:      taskData.Message.AgentID,
		CompanyID:    taskData.Message.CompanyID,
		PhoneNumber:  phoneNumber,
		Timestamp:    taskData.Message.MessageTimestamp,
		CreatedAt:    utils.Now(),
		LastMetadata: taskData.Message.LastMetadata,
	})

	log.Info("Successfully created new contact",
		zap.String("contact_id", newContact.ID))
}

// updateExistingContact updates an existing contact during onboarding
func (w *OnboardingWorker) updateExistingContact(ctx context.Context, taskData OnboardingTaskData, contact *model.Contact, phoneNumber string, status *string) {
	log := logger.FromContext(ctx)

	contact.Origin = "onboarding"
	contact.FirstMessageID = taskData.Message.MessageID
	contact.FirstMessageTimestamp = taskData.Message.MessageTimestamp
	contact.UpdatedAt = utils.Now()

	// Apply onboarding metadata if fields are empty
	if taskData.OnboardingMetadata != nil {
		if tags, ok := taskData.OnboardingMetadata["tags"].(string); ok && contact.Tags == "" {
			contact.Tags = tags
		}
		if assignedTo, ok := taskData.OnboardingMetadata["assigned_to"].(string); ok && contact.AssignedTo == "" {
			contact.AssignedTo = assignedTo
		}
		if notes, ok := taskData.OnboardingMetadata["notes"].(string); ok && contact.Notes == "" {
			contact.Notes = notes
		}
	}

	if err := w.contactRepo.Update(ctx, *contact); err != nil {
		log.Error("Error updating contact", zap.Error(err))
		*status = "failure_contact_update"
		return
	}

	// Mark as onboarded in cache
	w.onboardingCache.MarkOnboarded(phoneNumber, taskData.Message.AgentID)

	// Add to batch processor
	w.batchProcessor.Add(ctx, model.OnboardingLog{
		MessageID:    taskData.Message.MessageID,
		AgentID:      taskData.Message.AgentID,
		CompanyID:    taskData.Message.CompanyID,
		PhoneNumber:  phoneNumber,
		Timestamp:    taskData.Message.MessageTimestamp,
		CreatedAt:    utils.Now(),
		LastMetadata: taskData.Message.LastMetadata,
	})

	log.Info("Successfully updated contact with origin")
}

// warmupBloomFilters pre-populates bloom filters with existing data
func (w *OnboardingWorker) warmupBloomFilters() {
	ctx := context.Background()
	ctx = tenant.WithCompanyID(ctx, w.companyID)

	w.baseLogger.Info("Starting bloom filter warmup", zap.String("company_id", w.companyID))
	start := time.Now()

	// Load onboarded contacts in batches
	batchSize := 5000
	offset := 0
	totalOnboarded := 0

	for {
		// Find contacts with origin (already onboarded)
		contacts, err := w.contactRepo.FindByOriginPaginated(ctx, "onboarding", batchSize, offset)
		if err != nil {
			w.baseLogger.Error("Failed to load onboarded contacts",
				zap.Error(err),
				zap.Int("offset", offset))
			break
		}

		if len(contacts) == 0 {
			break
		}

		// Add to bloom filter
		for _, contact := range contacts {
			w.onboardingCache.MarkOnboarded(contact.PhoneNumber, contact.AgentID)
			totalOnboarded++
		}

		offset += len(contacts)

		// Also load from historical_sync origin
		if offset == 0 {
			historicalContacts, _ := w.contactRepo.FindByOriginPaginated(ctx, "historical_sync", batchSize, 0)
			for _, contact := range historicalContacts {
				w.onboardingCache.MarkOnboarded(contact.PhoneNumber, contact.AgentID)
				totalOnboarded++
			}
		}

		// Prevent excessive memory usage during warmup
		if totalOnboarded >= 100000 {
			w.baseLogger.Info("Bloom filter warmup limit reached",
				zap.Int("total_loaded", totalOnboarded))
			break
		}
	}

	duration := time.Since(start)
	w.baseLogger.Info("Bloom filter warmup completed",
		zap.String("company_id", w.companyID),
		zap.Int("onboarded_loaded", totalOnboarded),
		zap.Duration("duration", duration))

	observer.ObserveCacheWarmupDuration(w.companyID, duration)
	observer.SetCacheWarmupSize(w.companyID, totalOnboarded)
}

// reportStats periodically reports cache and worker stats
func (w *OnboardingWorker) reportStats() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			stats := w.onboardingCache.GetStats()
			w.baseLogger.Info("Onboarding worker stats",
				zap.String("company_id", w.companyID),
				zap.Int64("bloom_hits", stats.Hits),
				zap.Int64("bloom_misses", stats.Misses),
				zap.Float64("bloom_hit_rate", stats.HitRate),
				zap.Int64("false_positives", stats.FalsePositives),
				zap.Float64("false_positive_rate", stats.FalsePositiveRate),
				zap.Uint64("onboarded_filter_size", stats.OnboardedSize),
				zap.Uint64("nonexist_filter_size", stats.NonExistSize),
				zap.Int("pool_running", w.pool.Running()),
				zap.Int("pool_waiting", w.pool.Waiting()))

			observer.SetBloomFilterStats(w.companyID, stats.HitRate, stats.FalsePositiveRate)
			observer.SetWorkerPoolStats(w.companyID, "onboarding", w.pool.Running(), w.pool.Waiting())

		case <-w.stopChan:
			return
		}
	}
}

// Stop gracefully shuts down the worker pool
func (w *OnboardingWorker) Stop() {
	w.baseLogger.Info("Stopping onboarding worker", zap.String("company_id", w.companyID))

	// Stop background goroutines
	close(w.stopChan)

	// Stop batch processor
	w.batchProcessor.Stop()

	// Release pool
	w.pool.Release()

	// Wait for background tasks
	w.wg.Wait()

	// Log final stats
	stats := w.onboardingCache.GetStats()
	w.baseLogger.Info("Onboarding worker stopped",
		zap.String("company_id", w.companyID),
		zap.Float64("final_hit_rate", stats.HitRate),
		zap.Float64("final_fp_rate", stats.FalsePositiveRate))
}

// cleanPhoneNumber performs basic cleaning
func cleanPhoneNumber(phone string) string {
	cleaned := strings.ReplaceAll(phone, "+", "")
	cleaned = strings.ReplaceAll(cleaned, "-", "")
	cleaned = strings.ReplaceAll(cleaned, " ", "")
	cleaned = strings.ReplaceAll(cleaned, "(", "")
	cleaned = strings.ReplaceAll(cleaned, ")", "")
	cleaned = strings.TrimSpace(cleaned)
	return cleaned
}
