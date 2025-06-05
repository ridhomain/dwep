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
	Ctx          context.Context // Context derived for the task, NOT the original request context
	Message      model.Message
	MetadataJSON datatypes.JSON
}

// IOnboardingWorker defines the interface for the onboarding worker pool.
type IOnboardingWorker interface {
	SubmitTask(taskData OnboardingTaskData) error
	Stop()
}

// OnboardingWorker manages the worker pool for processing onboarding logs.
type OnboardingWorker struct {
	pool        *ants.PoolWithFunc
	contactRepo storage.ContactRepo
	logRepo     storage.OnboardingLogRepo
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
		cfg:         cfg,
		baseLogger:  baseLogger.Named("onboarding_worker"), // Create a named logger
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
		ants.WithNonblocking(false), // Make it blocking if queue is full, controlled by MaxBlockTime
		ants.WithMaxBlockingTasks(cfg.QueueSize),
		ants.WithPanicHandler(func(err interface{}) {
			worker.baseLogger.Error("Panic recovered in onboarding worker", zap.Any("panic_error", err), zap.Stack("stack"))
			// Potentially increment a panic counter metric here
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

// SubmitTask submits a new onboarding check task to the worker pool.
func (w *OnboardingWorker) SubmitTask(taskData OnboardingTaskData) error {
	start := time.Now()
	observer.IncOnboardingTasksSubmitted(taskData.Message.CompanyID)
	observer.SetOnboardingQueueLength(w.pool.Waiting()) // Approximate queue length

	// Use Invoke with timeout
	err := w.pool.Invoke(taskData) // Pass task data directly

	duration := time.Since(start)

	if err != nil {
		w.baseLogger.Warn("Failed to submit onboarding task to pool",
			zap.String("message_id", taskData.Message.MessageID),
			zap.String("company_id", taskData.Message.CompanyID),
			zap.Duration("submit_duration", duration),
			zap.Error(err),
		)
		observer.IncOnboardingTasksProcessed(taskData.Message.CompanyID, "submit_error")
		// Potentially handle different errors like ants.ErrPoolOverload specifically
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
	// Use the logger derived from the task context if available, otherwise use base logger
	log := logger.FromContextOr(taskData.Ctx, w.baseLogger).With(
		zap.String("task_message_id", taskData.Message.MessageID),
		zap.String("task_company_id", taskData.Message.CompanyID),
	)

	start := time.Now()
	status := "success" // Default status for metrics

	log.Debug("Processing onboarding task") // Changed from Info to Debug

	// Add tenant ID to the task's context for repository operations
	taskCtx := tenant.WithCompanyID(taskData.Ctx, taskData.Message.CompanyID)

	// 1. Basic check (already done before submitting, but double-check)
	if taskData.Message.Flow != model.MessageFlowIncoming || taskData.Message.FromPhone == "" {
		log.Debug("Skipping onboarding task: not applicable message type")
		observer.IncOnboardingTasksProcessed(taskData.Message.CompanyID, "skipped_not_applicable")
		return
	}

	// 2. Clean and validate phone number
	phoneNumber := cleanPhoneNumber(taskData.Message.FromPhone)
	if phoneNumber == "" {
		log.Warn("Skipping onboarding task: empty phone number after cleaning", zap.String("original_from", taskData.Message.FromPhone))
		observer.IncOnboardingTasksProcessed(taskData.Message.CompanyID, "skipped_empty_phone")
		return
	}

	var processingErr error // Variable to capture the first critical error

	// 3. Check if contact exists for this phone number AND agent ID
	existingContact, err := w.contactRepo.FindByPhoneAndAgentID(taskCtx, phoneNumber, taskData.Message.AgentID)
	if err != nil && !errors.Is(err, apperrors.ErrNotFound) && !errors.Is(err, apperrors.ErrBadRequest) { // Treat NotFound and BadRequest as non-fatal for this step
		// Log other errors (e.g., database connection issues)
		log.Error("Error checking contact existence by phone and agent",
			zap.String("phone_number", phoneNumber),
			zap.String("agent_id", taskData.Message.AgentID),
			zap.Error(err))
		status = "failure_contact_check"
		processingErr = err
	} else if errors.Is(err, apperrors.ErrBadRequest) {
		// Handle case where AgentID might be empty if FindByPhoneAndAgentID returns BadRequest for that
		log.Warn("Skipping onboarding task: Invalid input for contact check (e.g., empty AgentID)",
			zap.String("phone_number", phoneNumber),
			zap.String("agent_id", taskData.Message.AgentID),
			zap.Error(err))
		status = "skipped_invalid_check_input"
		processingErr = err // Set processingErr to prevent further steps
	}

	// 4. If no processing error occurred SO FAR and contact DOES NOT exist for this specific phone+agent combo, proceed
	if processingErr == nil && existingContact == nil {
		log.Debug("Potential onboarding: Contact not found for this phone and agent combo, creating contact.",
			zap.String("phone_number", phoneNumber),
			zap.String("agent_id", taskData.Message.AgentID))

		contactUUID := uuid.New().String() // Generate a new UUID for the contact ID
		// 4.1 Create the new contact record
		newContact := model.Contact{
			ID:                    contactUUID,
			PhoneNumber:           phoneNumber,
			CompanyID:             taskData.Message.CompanyID,
			FirstMessageID:        taskData.Message.MessageID,        // Link to the message that triggered onboarding
			FirstMessageTimestamp: taskData.Message.MessageTimestamp, // Link to the message that triggered onboarding
			Origin:                "onboarding_worker",               // Indicate source
			AgentID:               taskData.Message.AgentID,          // Assign agent from message if available
			LastMetadata:          taskData.MetadataJSON,             // Store last known metadata
		}

		contactSaveErr := w.contactRepo.Save(taskCtx, newContact)
		if contactSaveErr != nil {
			// Failed to save the contact
			log.Error("Error saving new contact during onboarding", zap.Error(contactSaveErr))
			status = "failure_contact_save"
			processingErr = contactSaveErr
			// Do not proceed to log creation if contact save failed
		} else {
			log.Info("Successfully created new contact during onboarding", zap.String("contact_id", newContact.ID)) // Log the assigned ID if available

			// 5. Check if an onboarding log ALREADY exists for this message ID (check AFTER successful contact creation)
			existingLog, logErr := w.logRepo.FindByMessageID(taskCtx, taskData.Message.MessageID)
			if logErr != nil && !errors.Is(logErr, apperrors.ErrNotFound) { // Treat NotFound as non-fatal here
				log.Error("Error checking for existing onboarding log after contact creation", zap.Error(logErr))
				status = "failure_log_check"
				processingErr = logErr
			}

			// 6. If no existing log found, create a new one
			if processingErr == nil && existingLog == nil {
				newLog := model.OnboardingLog{
					MessageID:   taskData.Message.MessageID,
					AgentID:     taskData.Message.AgentID,
					CompanyID:   taskData.Message.CompanyID,
					PhoneNumber: phoneNumber,
					Timestamp:   taskData.Message.MessageTimestamp,
					// CreatedAt is handled by GORM
					LastMetadata: taskData.MetadataJSON,
				}

				logSaveErr := w.logRepo.Save(taskCtx, newLog)
				if logSaveErr != nil {
					// Failed to save the log
					log.Error("Error saving new onboarding log", zap.Error(logSaveErr))
					status = "failure_log_save"
					processingErr = logSaveErr
					// Note: Contact was already created successfully earlier.
				} else {
					log.Info("Successfully created onboarding log") // Keep this as Info
					// Status remains "success" (overall success for this path)
				}
			} else if processingErr == nil {
				// Log exists, successfully processed (no action needed for log, contact created)
				log.Debug("Onboarding log already exists for this message, contact was created")
				status = "skipped_log_exists_contact_created" // More specific status
			}
		} // End else block for successful contact save

	} else if processingErr == nil {
		// Contact exists for this phone+agent combo, successfully processed (no action needed)
		log.Debug("Skipping onboarding: Contact already exists for this phone and agent combo",
			zap.String("phone_number", phoneNumber),
			zap.String("agent_id", taskData.Message.AgentID))
		status = "skipped_contact_exists" // Keep status relatively simple, or make more specific like "skipped_contact_agent_pair_exists"
	}

	// Record metrics
	duration := time.Since(start)
	observer.ObserveOnboardingProcessingDuration(taskData.Message.CompanyID, duration)
	observer.IncOnboardingTasksProcessed(taskData.Message.CompanyID, status)

	log.Debug("Finished processing onboarding task", zap.Duration("duration", duration), zap.String("final_status", status))
}

// cleanPhoneNumber performs basic cleaning.
func cleanPhoneNumber(phone string) string {
	cleaned := strings.ReplaceAll(phone, "+", "")
	cleaned = strings.ReplaceAll(cleaned, "-", "")
	cleaned = strings.ReplaceAll(cleaned, " ", "")
	cleaned = strings.ReplaceAll(cleaned, "(", "")
	cleaned = strings.ReplaceAll(cleaned, ")", "")
	return cleaned
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
