// internal/usecase/onboarding_worker.go
package usecase

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/panjf2000/ants/v2"
	"go.uber.org/zap"
	"gorm.io/datatypes"

	apperrors "gitlab.com/timkado/api/daisi-wa-events-processor/internal/apperrors"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/config"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/model"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/observer"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/storage"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/tenant"
	"gitlab.com/timkado/api/daisi-wa-events-processor/pkg/logger"
)

// OnboardingTaskData holds the necessary data for an onboarding task.
type OnboardingTaskData struct {
	Ctx          context.Context
	Message      model.Message
	MetadataJSON datatypes.JSON
	// Campaign fields from message payload
	IsCampaignMessage  bool
	CampaignTags       string
	CampaignAssignedTo string
	CampaignOrigin     string
}

// IOnboardingWorker defines the interface for the onboarding worker pool.
type IOnboardingWorker interface {
	SubmitTask(taskData OnboardingTaskData) error
	Stop()
	WarmUpBloomFilter(ctx context.Context, agentID string) error
	GetBloomFilter() *OnboardingBloomFilter
}

// OnboardingWorker manages the worker pool for processing onboarding logs.
type OnboardingWorker struct {
	pool        *ants.PoolWithFunc
	contactRepo storage.ContactRepo
	logRepo     storage.OnboardingLogRepo
	bloomFilter *OnboardingBloomFilter
	cfg         config.OnboardingWorkerPoolConfig
	baseLogger  *zap.Logger
}

// Ensure OnboardingWorker implements IOnboardingWorker
var _ IOnboardingWorker = (*OnboardingWorker)(nil)

// NewOnboardingWorker creates and initializes a new onboarding worker pool.
func NewOnboardingWorker(
	cfg config.OnboardingWorkerPoolConfig,
	contactRepo storage.ContactRepo,
	logRepo storage.OnboardingLogRepo,
	baseLogger *zap.Logger,
) (*OnboardingWorker, error) {
	worker := &OnboardingWorker{
		contactRepo: contactRepo,
		logRepo:     logRepo,
		bloomFilter: NewOnboardingBloomFilter(baseLogger),
		cfg:         cfg,
		baseLogger:  baseLogger.Named("onboarding_worker"),
	}

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
			worker.baseLogger.Error("Panic recovered in onboarding worker", zap.Any("panic_error", err), zap.Stack("stack"))
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create onboarding worker pool: %w", err)
	}
	worker.pool = pool
	worker.baseLogger.Info("Onboarding worker pool initialized",
		zap.Int("pool_size", cfg.PoolSize),
		zap.Int("queue_size", cfg.QueueSize),
		zap.Duration("expiry_time", cfg.ExpiryTime),
		zap.Duration("max_block_time", cfg.MaxBlock),
	)
	return worker, nil
}

// GetBloomFilter returns the bloom filter instance
func (w *OnboardingWorker) GetBloomFilter() *OnboardingBloomFilter {
	return w.bloomFilter
}

// SubmitTask submits a new onboarding check task to the worker pool.
func (w *OnboardingWorker) SubmitTask(taskData OnboardingTaskData) error {
	start := time.Now()
	observer.IncOnboardingTasksSubmitted(taskData.Message.CompanyID)
	observer.SetOnboardingQueueLength(w.pool.Waiting())

	err := w.pool.Invoke(taskData)

	duration := time.Since(start)

	if err != nil {
		w.baseLogger.Warn("Failed to submit onboarding task to pool",
			zap.String("message_id", taskData.Message.MessageID),
			zap.String("company_id", taskData.Message.CompanyID),
			zap.Duration("submit_duration", duration),
			zap.Error(err),
		)
		observer.IncOnboardingTasksProcessed(taskData.Message.CompanyID, "submit_error")
		if errors.Is(err, ants.ErrPoolOverload) {
			return fmt.Errorf("onboarding pool overload: %w", err)
		}
		return fmt.Errorf("failed to invoke onboarding task: %w", err)
	}

	w.baseLogger.Debug("Successfully submitted onboarding task",
		zap.String("message_id", taskData.Message.MessageID),
		zap.String("company_id", taskData.Message.CompanyID),
		zap.Duration("submit_duration", duration),
	)
	return nil
}

// processOnboardingTask contains the actual logic executed by a worker goroutine.
func (w *OnboardingWorker) processOnboardingTask(taskData OnboardingTaskData) {
	log := logger.FromContextOr(taskData.Ctx, w.baseLogger).With(
		zap.String("task_message_id", taskData.Message.MessageID),
		zap.String("task_company_id", taskData.Message.CompanyID),
	)

	start := time.Now()
	status := "success"

	log.Debug("Processing onboarding task")

	taskCtx := tenant.WithCompanyID(taskData.Ctx, taskData.Message.CompanyID)

	// 1. Basic check
	if taskData.Message.Flow != model.MessageFlowIncoming || taskData.Message.FromPhone == "" {
		log.Debug("Skipping onboarding task: not applicable message type")
		observer.IncOnboardingTasksProcessed(taskData.Message.CompanyID, "skipped_not_applicable")
		return
	}

	phoneNumber := cleanPhoneNumber(taskData.Message.FromPhone)
	if phoneNumber == "" {
		log.Warn("Skipping onboarding task: empty phone number after cleaning", zap.String("original_from", taskData.Message.FromPhone))
		observer.IncOnboardingTasksProcessed(taskData.Message.CompanyID, "skipped_empty_phone")
		return
	}

	var processingErr error

	// 2. Quick Bloom filter check
	if w.bloomFilter.MightExist(taskData.Message.CompanyID, taskData.Message.AgentID, phoneNumber) {
		log.Debug("Contact might be onboarded (bloom filter positive), checking database")

		// Bloom filter says it might exist, need to verify with DB
		existingLog, err := w.logRepo.FindByPhoneAndAgentID(taskCtx, phoneNumber, taskData.Message.AgentID)
		if err != nil && !errors.Is(err, apperrors.ErrNotFound) {
			log.Error("Error checking onboarding log", zap.Error(err))
			status = "failure_log_check"
			processingErr = err
		}

		if processingErr == nil && existingLog != nil {
			// Confirmed: already onboarded
			log.Debug("Contact confirmed onboarded, processing campaign update only")
			if taskData.IsCampaignMessage {
				w.processCampaignUpdate(taskCtx, taskData, log)
			}
			status = "skipped_already_onboarded"
			observer.IncOnboardingTasksProcessed(taskData.Message.CompanyID, status)
			return
		}

		// False positive from bloom filter, continue with onboarding
		if processingErr == nil {
			log.Debug("False positive from bloom filter, proceeding with onboarding")
		}
	}

	// 3. Check if contact exists
	existingContact, err := w.contactRepo.FindByPhoneAndAgentID(taskCtx, phoneNumber, taskData.Message.AgentID)
	if err != nil && !errors.Is(err, apperrors.ErrNotFound) && !errors.Is(err, apperrors.ErrBadRequest) {
		log.Error("Error checking contact existence by phone and agent",
			zap.String("phone_number", phoneNumber),
			zap.String("agent_id", taskData.Message.AgentID),
			zap.Error(err))
		status = "failure_contact_check"
		processingErr = err
	} else if errors.Is(err, apperrors.ErrBadRequest) {
		log.Warn("Skipping onboarding task: Invalid input for contact check",
			zap.String("phone_number", phoneNumber),
			zap.String("agent_id", taskData.Message.AgentID),
			zap.Error(err))
		status = "skipped_invalid_check_input"
		processingErr = err
	}

	// 4. Create or update contact
	if processingErr == nil {
		if existingContact == nil {
			// Create new contact
			log.Debug("Creating new contact for onboarding",
				zap.String("phone_number", phoneNumber),
				zap.String("agent_id", taskData.Message.AgentID))

			contactUUID := uuid.New().String()
			newContact := model.Contact{
				ID:                    contactUUID,
				PhoneNumber:           phoneNumber,
				CompanyID:             taskData.Message.CompanyID,
				FirstMessageID:        taskData.Message.MessageID,
				FirstMessageTimestamp: taskData.Message.MessageTimestamp,
				AgentID:               taskData.Message.AgentID,
				Status:                "ACTIVE",
				LastMetadata:          taskData.MetadataJSON,
			}

			// Set business data based on message type
			if taskData.IsCampaignMessage {
				newContact.Origin = taskData.CampaignOrigin
				newContact.Tags = taskData.CampaignTags
				newContact.AssignedTo = taskData.CampaignAssignedTo
			} else {
				newContact.Origin = "DirectChat"
				newContact.Tags = "Onboarded"
				newContact.AssignedTo = ""
			}

			contactSaveErr := w.contactRepo.Save(taskCtx, newContact)
			if contactSaveErr != nil {
				log.Error("Error saving new contact during onboarding", zap.Error(contactSaveErr))
				status = "failure_contact_save"
				processingErr = contactSaveErr
			} else {
				log.Info("Successfully created new contact during onboarding", zap.String("contact_id", newContact.ID))
			}
		} else {
			// Update existing contact
			needsUpdate := false
			contactToUpdate := *existingContact

			// For campaign messages, ALWAYS update
			if taskData.IsCampaignMessage {
				// Merge tags
				if taskData.CampaignTags != "" {
					existingTags := strings.Split(existingContact.Tags, ",")
					newTags := strings.Split(taskData.CampaignTags, ",")
					mergedTags := mergeTags(existingTags, newTags)
					contactToUpdate.Tags = strings.Join(mergedTags, ",")
					needsUpdate = true
				}

				// Replace AssignedTo
				if taskData.CampaignAssignedTo != "" {
					contactToUpdate.AssignedTo = taskData.CampaignAssignedTo
					needsUpdate = true
				}

				// Set Origin only if blank
				if existingContact.Origin == "" && taskData.CampaignOrigin != "" {
					contactToUpdate.Origin = taskData.CampaignOrigin
					needsUpdate = true
				}
			} else if existingContact.Origin == "" {
				// Non-campaign message, but contact not fully onboarded
				contactToUpdate.Origin = "DirectChat"
				contactToUpdate.Tags = addTagIfMissing(existingContact.Tags, "Onboarded")
				needsUpdate = true
			}

			// Set first message fields if not already set
			if existingContact.FirstMessageID == "" {
				contactToUpdate.FirstMessageID = taskData.Message.MessageID
				contactToUpdate.FirstMessageTimestamp = taskData.Message.MessageTimestamp
				needsUpdate = true
			}

			// Update if needed
			if needsUpdate {
				contactToUpdate.UpdatedAt = time.Now()
				contactToUpdate.LastMetadata = taskData.MetadataJSON

				if err := w.contactRepo.Update(taskCtx, contactToUpdate); err != nil {
					log.Error("Error updating contact during onboarding", zap.Error(err))
					status = "failure_contact_update"
					processingErr = err
				} else {
					log.Info("Successfully updated contact during onboarding")
				}
			}
		}

		// 5. Create onboarding log (ONE TIME ONLY)
		if processingErr == nil {
			// Double-check it doesn't already exist
			existingLog, _ := w.logRepo.FindByPhoneAndAgentID(taskCtx, phoneNumber, taskData.Message.AgentID)

			if existingLog == nil {
				newLog := model.OnboardingLog{
					MessageID:    taskData.Message.MessageID,
					AgentID:      taskData.Message.AgentID,
					CompanyID:    taskData.Message.CompanyID,
					PhoneNumber:  phoneNumber,
					Timestamp:    taskData.Message.MessageTimestamp,
					LastMetadata: taskData.MetadataJSON,
				}

				logSaveErr := w.logRepo.Save(taskCtx, newLog)
				if logSaveErr != nil {
					log.Error("Error saving new onboarding log", zap.Error(logSaveErr))
					status = "failure_log_save"
					processingErr = logSaveErr
				} else {
					log.Info("Successfully created onboarding log")
					// Add to bloom filter
					w.bloomFilter.Add(taskData.Message.CompanyID, taskData.Message.AgentID, phoneNumber)
				}
			} else {
				log.Debug("Onboarding log already exists for this contact")
				// Still add to bloom filter in case it was missing
				w.bloomFilter.Add(taskData.Message.CompanyID, taskData.Message.AgentID, phoneNumber)
			}
		}
	}

	// Record metrics
	duration := time.Since(start)
	observer.ObserveOnboardingProcessingDuration(taskData.Message.CompanyID, duration)
	observer.IncOnboardingTasksProcessed(taskData.Message.CompanyID, status)

	log.Debug("Finished processing onboarding task", zap.Duration("duration", duration), zap.String("final_status", status))
}

// processCampaignUpdate processes campaign updates for already onboarded contacts
func (w *OnboardingWorker) processCampaignUpdate(ctx context.Context, taskData OnboardingTaskData, log *zap.Logger) {
	if !taskData.IsCampaignMessage {
		return
	}

	phoneNumber := cleanPhoneNumber(taskData.Message.FromPhone)

	// Get contact from DB
	contact, err := w.contactRepo.FindByPhoneAndAgentID(ctx, phoneNumber, taskData.Message.AgentID)
	if err != nil {
		log.Error("Failed to find contact for campaign update", zap.Error(err))
		return
	}

	needsUpdate := false

	// Merge tags
	if taskData.CampaignTags != "" {
		existingTags := strings.Split(contact.Tags, ",")
		newTags := strings.Split(taskData.CampaignTags, ",")
		mergedTags := mergeTags(existingTags, newTags)
		contact.Tags = strings.Join(mergedTags, ",")
		needsUpdate = true
	}

	// Replace AssignedTo
	if taskData.CampaignAssignedTo != "" && contact.AssignedTo != taskData.CampaignAssignedTo {
		contact.AssignedTo = taskData.CampaignAssignedTo
		needsUpdate = true
	}

	// Set Origin only if blank
	if contact.Origin == "" && taskData.CampaignOrigin != "" {
		contact.Origin = taskData.CampaignOrigin
		needsUpdate = true
	}

	if needsUpdate {
		contact.UpdatedAt = time.Now()
		contact.LastMetadata = taskData.MetadataJSON

		if err := w.contactRepo.Update(ctx, *contact); err != nil {
			log.Error("Failed to update contact with campaign data", zap.Error(err))
		} else {
			log.Info("Successfully updated contact with campaign data")
		}
	}
}

// WarmUpBloomFilter loads existing onboarding logs into the bloom filter
func (w *OnboardingWorker) WarmUpBloomFilter(ctx context.Context, agentID string) error {
	companyID, err := tenant.FromContext(ctx)
	if err != nil {
		return err
	}

	log := logger.FromContext(ctx)
	const batchSize = 1000
	offset := 0
	totalLoaded := 0

	for {
		// Fetch onboarding logs in batches
		logs, err := w.logRepo.FindByAgentIDPaginated(ctx, agentID, batchSize, offset)
		if err != nil {
			return fmt.Errorf("failed to fetch onboarding logs: %w", err)
		}

		if len(logs) == 0 {
			break // No more logs
		}

		// Extract phone numbers
		phoneNumbers := make([]string, len(logs))
		for i, log := range logs {
			phoneNumbers[i] = log.PhoneNumber
		}

		// Batch add to bloom filter
		w.bloomFilter.AddBatch(companyID, agentID, phoneNumbers)

		totalLoaded += len(logs)
		offset += batchSize

		log.Debug("Loaded batch into bloom filter",
			zap.Int("batch_size", len(logs)),
			zap.Int("total_loaded", totalLoaded))

		// If we got less than batch size, we're done
		if len(logs) < batchSize {
			break
		}
	}

	log.Info("Bloom filter warm-up completed",
		zap.String("agent_id", agentID),
		zap.Int("total_contacts", totalLoaded))

	return nil
}

// cleanPhoneNumber performs basic cleaning.
func cleanPhoneNumber(phone string) string {
	cleaned := strings.ReplaceAll(phone, "+", "")
	// cleaned = strings.ReplaceAll(cleaned, "-", "")
	cleaned = strings.ReplaceAll(cleaned, " ", "")
	cleaned = strings.ReplaceAll(cleaned, "(", "")
	cleaned = strings.ReplaceAll(cleaned, ")", "")
	return cleaned
}

// mergeTags merges two tag lists, avoiding duplicates
func mergeTags(existing, new []string) []string {
	tagMap := make(map[string]bool)

	// Add existing tags
	for _, tag := range existing {
		tag = strings.TrimSpace(tag)
		if tag != "" {
			tagMap[tag] = true
		}
	}

	// Add new tags
	for _, tag := range new {
		tag = strings.TrimSpace(tag)
		if tag != "" {
			tagMap[tag] = true
		}
	}

	// Convert back to slice
	result := make([]string, 0, len(tagMap))
	for tag := range tagMap {
		result = append(result, tag)
	}

	return result
}

// addTagIfMissing adds a tag if it doesn't already exist
func addTagIfMissing(existingTags, newTag string) string {
	if existingTags == "" {
		return newTag
	}

	tags := strings.Split(existingTags, ",")
	for _, tag := range tags {
		if strings.TrimSpace(tag) == newTag {
			return existingTags // Tag already exists
		}
	}

	return existingTags + "," + newTag
}

// Stop gracefully shuts down the worker pool.
func (w *OnboardingWorker) Stop() {
	if w.pool != nil {
		w.baseLogger.Info("Releasing onboarding worker pool")
		start := time.Now()
		w.pool.Release()
		w.baseLogger.Info("Onboarding worker pool released", zap.Duration("duration", time.Since(start)))
	}
}
