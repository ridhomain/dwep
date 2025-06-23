package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/google/uuid"

	// "github.com/jackc/pgx/v5/pgconn" // No longer needed here
	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	apperrors "gitlab.com/timkado/api/daisi-wa-events-processor/internal/apperrors" // Import apperrors
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/model"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/observer"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/tenant"
	"gitlab.com/timkado/api/daisi-wa-events-processor/pkg/logger"
	"gitlab.com/timkado/api/daisi-wa-events-processor/pkg/utils"
)

// --- Contact Repository Methods --- DDL

// SaveContact saves or updates a contact record.
func (r *PostgresRepo) SaveContact(ctx context.Context, contact model.Contact) error {
	companyID, err := tenant.FromContext(ctx)
	if err != nil {
		// Consider if this should be Unauthorized or a specific TenantError
		return fmt.Errorf("%w: failed to get tenant ID from context: %w", apperrors.ErrUnauthorized, err)
	}
	if companyID != contact.CompanyID {
		// Mismatch implies potentially incorrect data provided or access attempt
		return fmt.Errorf("%w: contact CompanyID %s does not match tenant ID %s", apperrors.ErrBadRequest, contact.CompanyID, companyID)
	}

	operation := func() error {
		tx := r.db.WithContext(ctx).Begin()
		if tx.Error != nil {
			return fmt.Errorf("%w: failed to begin transaction: %w", apperrors.ErrDatabase, tx.Error)
		}
		var txErr error // Variable to hold potential error during commit/rollback check
		defer func() {
			if r := recover(); r != nil {
				tx.Rollback() // Attempt rollback
				panic(r)      // Re-panic
			} else if txErr != nil { // Check the captured error from the operation
				if rbErr := tx.Rollback().Error; rbErr != nil {
					// Log rollback failure but prioritize returning the original txErr
					logger.FromContext(ctx).Error("Failed to rollback transaction after error", zap.Error(rbErr), zap.NamedError("originalTxError", txErr))
				}
			}
		}() // End defer

		var existingContact model.Contact
		result := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("phone_number = ? AND agent_id = ?", contact.PhoneNumber, contact.AgentID).
			First(&existingContact)
		findErr := result.Error

		if findErr != nil {
			if errors.Is(findErr, gorm.ErrRecordNotFound) {
				// Contact doesn't exist, create it
				if createErr := tx.Create(&contact).Error; createErr != nil {
					txErr = checkConstraintViolation(createErr) // Check for constraints on create
					// txErr = fmt.Errorf("%w: failed to create contact: %w", apperrors.ErrDatabase, createErr) // old
					return txErr
				}
			} else {
				// Other error finding/locking the contact
				txErr = fmt.Errorf("%w: failed to lock contact row: %w", apperrors.ErrDatabase, findErr)
				return txErr
			}
		} else {
			// Contact exists, update it
			if updateErr := tx.Model(&existingContact).Updates(contact).Error; updateErr != nil {
				txErr = checkConstraintViolation(updateErr) // Check for constraints on update
				// txErr = fmt.Errorf("%w: failed to update contact: %w", apperrors.ErrDatabase, updateErr) // old
				return txErr
			}
		}
		// Commit the transaction
		if commitErr := tx.Commit().Error; commitErr != nil {
			txErr = fmt.Errorf("%w: failed to commit save transaction: %w", apperrors.ErrDatabase, commitErr)
			return txErr
		}
		return nil // Success
	} // End operation func

	commitPolicy := newRetryPolicy(ctx, commitRetryMaxElapsedTime)
	startTime := utils.Now()
	commitErr := retryableOperation(ctx, commitPolicy, "SaveContact Commit", operation)
	observer.ObserveDbOperationDuration("save", "contact", companyID, time.Since(startTime), commitErr)
	if commitErr != nil {
		logger.FromContext(ctx).Error("Failed to save contact after retries", zap.Error(commitErr))
		// Return the specific error captured within the operation (already wrapped)
		return commitErr
	}
	return nil
}

// UpdateContact updates specific fields of an existing contact record.
func (r *PostgresRepo) UpdateContact(ctx context.Context, contact model.Contact) error {
	companyID, err := tenant.FromContext(ctx)
	if err != nil {
		return fmt.Errorf("%w: failed to get tenant ID from context: %w", apperrors.ErrUnauthorized, err)
	}
	if companyID != contact.CompanyID {
		return fmt.Errorf("%w: contact CompanyID %s does not match tenant ID %s", apperrors.ErrBadRequest, contact.CompanyID, companyID)
	}
	contact.UpdatedAt = utils.Now()

	operation := func() error {
		tx := r.db.WithContext(ctx).Begin()
		if tx.Error != nil {
			return fmt.Errorf("%w: failed to begin transaction: %w", apperrors.ErrDatabase, tx.Error)
		}
		var txErr error
		defer func() {
			if r := recover(); r != nil {
				tx.Rollback()
				panic(r)
			} else if txErr != nil { // Check the captured error from the operation
				if rbErr := tx.Rollback().Error; rbErr != nil {
					logger.FromContext(ctx).Error("Failed to rollback transaction after error", zap.Error(rbErr), zap.NamedError("originalTxError", txErr))
				}
			}
		}() // End defer

		var existingContact model.Contact
		result := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("phone_number = ? AND agent_id = ?", contact.PhoneNumber, contact.AgentID).
			First(&existingContact)
		findErr := result.Error

		if findErr != nil {
			if errors.Is(findErr, gorm.ErrRecordNotFound) {
				// Wrap the ErrNotFound, keep it permanent for the retry policy
				txErr = fmt.Errorf("%w: contact not found for update (ID: %s, CompanyID: %s): %w", apperrors.ErrNotFound, contact.ID, companyID, findErr)
				return backoff.Permanent(txErr) // Make NotFound permanent for retry policy
			}
			// Other find/lock error
			txErr = fmt.Errorf("%w: failed to lock contact row for update: %w", apperrors.ErrDatabase, findErr)
			return txErr
		}

		updateResult := tx.Model(&existingContact).Updates(contact)
		if updateResult.Error != nil {
			txErr = checkConstraintViolation(updateResult.Error) // Check constraints on update
			// txErr = fmt.Errorf("%w: failed to update contact: %w", apperrors.ErrDatabase, updateResult.Error) // old
			return txErr
		}
		if updateResult.RowsAffected == 0 {
			// This isn't necessarily an error, but might indicate the update had no effect.
			// If finding the record succeeded, this might happen if input data matches existing data.
			logger.FromContext(ctx).Warn("UpdateContact resulted in 0 rows affected, potentially no change", zap.String("phone_number", contact.PhoneNumber), zap.String("agent_id", contact.AgentID))
		}
		// Commit the transaction
		if commitErr := tx.Commit().Error; commitErr != nil {
			txErr = fmt.Errorf("%w: failed to commit update transaction: %w", apperrors.ErrDatabase, commitErr)
			return txErr
		}
		return nil // Success
	} // End operation func

	commitPolicy := newRetryPolicy(ctx, commitRetryMaxElapsedTime)
	startTime := utils.Now()
	commitErr := retryableOperation(ctx, commitPolicy, "UpdateContact Commit", operation)
	observer.ObserveDbOperationDuration("update", "contact", companyID, time.Since(startTime), commitErr)
	if commitErr != nil {
		logger.FromContext(ctx).Error("Failed to update contact after retries", zap.Error(commitErr))
		// Return the specific error captured within the operation (already wrapped)
		return commitErr
	}
	return nil
}

// FindContactByID finds a contact by its ID.
func (r *PostgresRepo) FindContactByID(ctx context.Context, id string) (*model.Contact, error) {
	companyID, err := tenant.FromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to get tenant ID from context: %w", apperrors.ErrUnauthorized, err)
	}
	loggerCtx := logger.FromContext(ctx)

	var contact model.Contact
	operation := func() error {
		result := r.db.WithContext(ctx).Where("id = ? AND company_id = ?", id, companyID).First(&contact)
		// No need to checkConstraintViolation here, First only returns RecordNotFound or other DB errors
		if result.Error != nil {
			if errors.Is(result.Error, gorm.ErrRecordNotFound) {
				return fmt.Errorf("%w: contact_id %s: %w", apperrors.ErrNotFound, id, result.Error)
			}
			return fmt.Errorf("%w: query failed: %w", apperrors.ErrDatabase, result.Error)
		}
		return nil // Success
	} // End operation func

	readPolicy := newRetryPolicy(ctx, readRetryMaxElapsedTime)
	startTime := utils.Now()
	findErr := retryableOperation(ctx, readPolicy, "FindContactByID", operation)
	observer.ObserveDbOperationDuration("find_by_id", "contact", companyID, time.Since(startTime), findErr)

	if findErr != nil {
		// Check if the error returned by retryableOperation is ErrNotFound
		if errors.Is(findErr, apperrors.ErrNotFound) {
			// Return the specific ErrNotFound (without the original gorm.ErrRecordNotFound wrapper if preferred)
			// Or return the wrapped version from the operation func: return nil, findErr
			return nil, apperrors.ErrNotFound // Return the sentinel error directly
		}
		// Log and return other errors (already wrapped as ErrDatabase by operation)
		loggerCtx.Error("Failed to find contact by ID after retries",
			zap.String("contact_id", id),
			zap.String("company_id", companyID),
			zap.Error(findErr)) // findErr is already wrapped
		return nil, findErr
	}
	return &contact, nil
}

// FindContactByPhone finds a contact by its phone number.
func (r *PostgresRepo) FindContactByPhone(ctx context.Context, phone string) (*model.Contact, error) {
	companyID, err := tenant.FromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to get tenant ID from context: %w", apperrors.ErrUnauthorized, err)
	}
	loggerCtx := logger.FromContext(ctx)

	var contact model.Contact
	operation := func() error {
		result := r.db.WithContext(ctx).Where("phone_number = ? AND company_id = ?", phone, companyID).First(&contact)
		if result.Error != nil {
			if errors.Is(result.Error, gorm.ErrRecordNotFound) {
				return fmt.Errorf("%w: phone %s: %w", apperrors.ErrNotFound, phone, result.Error)
			}
			return fmt.Errorf("%w: query failed: %w", apperrors.ErrDatabase, result.Error)
		}
		return nil // Success
	} // End operation func

	readPolicy := newRetryPolicy(ctx, readRetryMaxElapsedTime)
	startTime := utils.Now()
	findErr := retryableOperation(ctx, readPolicy, "FindContactByPhone", operation)
	observer.ObserveDbOperationDuration("find_by_phone", "contact", companyID, time.Since(startTime), findErr)

	if findErr != nil {
		// Check if the error returned by retryableOperation is ErrNotFound
		if errors.Is(findErr, apperrors.ErrNotFound) {
			// Return the sentinel error directly
			return nil, apperrors.ErrNotFound
		}
		// Log and return other errors (already wrapped as ErrDatabase by operation)
		loggerCtx.Error("Failed to find contact by phone after retries",
			zap.String("phone", phone),
			zap.String("company_id", companyID),
			zap.Error(findErr)) // findErr is already wrapped
		return nil, findErr
	}
	return &contact, nil
}

// FindContactByPhoneAndAgentID finds a contact by its phone number and agent ID.
func (r *PostgresRepo) FindContactByPhoneAndAgentID(ctx context.Context, phone, agentID string) (*model.Contact, error) {
	companyID, err := tenant.FromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to get tenant ID from context: %w", apperrors.ErrUnauthorized, err)
	}
	loggerCtx := logger.FromContext(ctx)

	var contact model.Contact
	operation := func() error {
		// Ensure AgentID is not empty, otherwise the query might behave unexpectedly or match records where agent_id is NULL/empty if allowed by DB schema.
		if agentID == "" {
			// Return a specific error or handle as needed. Returning BadRequest as it's invalid input for this specific query.
			return backoff.Permanent(fmt.Errorf("%w: agentID cannot be empty for FindContactByPhoneAndAgentID", apperrors.ErrBadRequest))
		}

		result := r.db.WithContext(ctx).Where("phone_number = ? AND agent_id = ? AND company_id = ?", phone, agentID, companyID).First(&contact)
		if result.Error != nil {
			if errors.Is(result.Error, gorm.ErrRecordNotFound) {
				// Wrap ErrNotFound, making it permanent for retry policy
				return backoff.Permanent(fmt.Errorf("%w: phone %s, agent %s: %w", apperrors.ErrNotFound, phone, agentID, result.Error))
			}
			// Other DB error
			return fmt.Errorf("%w: query failed: %w", apperrors.ErrDatabase, result.Error)
		}
		return nil // Success
	} // End operation func

	readPolicy := newRetryPolicy(ctx, readRetryMaxElapsedTime)
	startTime := utils.Now()
	findErr := retryableOperation(ctx, readPolicy, "FindContactByPhoneAndAgentID", operation)
	observer.ObserveDbOperationDuration("find_by_phone_agent", "contact", companyID, time.Since(startTime), findErr)

	if findErr != nil {
		// Check if the error returned by retryableOperation is ErrNotFound or ErrBadRequest
		if errors.Is(findErr, apperrors.ErrNotFound) || errors.Is(findErr, apperrors.ErrBadRequest) {
			// Return the specific error directly (it's already wrapped and marked permanent if needed)
			return nil, findErr
		}
		// Log and return other errors (already wrapped as ErrDatabase by operation)
		loggerCtx.Error("Failed to find contact by phone and agent ID after retries",
			zap.String("phone", phone),
			zap.String("agent_id", agentID),
			zap.String("company_id", companyID),
			zap.Error(findErr)) // findErr is already wrapped
		return nil, findErr
	}
	return &contact, nil
}

// BulkUpsertContacts performs a bulk upsert operation for contact records.
func (r *PostgresRepo) BulkUpsertContacts(ctx context.Context, contacts []model.Contact) error {
	if len(contacts) == 0 {
		return nil
	}
	companyID, err := tenant.FromContext(ctx)
	if err != nil {
		return fmt.Errorf("%w: failed to get tenant ID from context: %w", apperrors.ErrUnauthorized, err)
	}
	loggerCtx := logger.FromContext(ctx)

	// Filter contacts to only include those matching the tenant ID
	validContacts := make([]model.Contact, 0, len(contacts))
	for i := range contacts { // Iterate using index
		if contacts[i].CompanyID != companyID {
			loggerCtx.Warn("Skipping contact in bulk upsert due to mismatched CompanyID",
				zap.String("contact_phone_number", contacts[i].PhoneNumber),
				zap.String("contact_company_id", contacts[i].CompanyID),
				zap.String("expected_company_id", companyID))
			continue // Skip this contact
		}
		contactUUID := uuid.New().String()
		contacts[i].ID = contactUUID
		// Ensure UpdatedAt is set for potential updates
		contacts[i].UpdatedAt = utils.Now()
		validContacts = append(validContacts, contacts[i])
	}

	// If no valid contacts remain after filtering, return early
	if len(validContacts) == 0 {
		loggerCtx.Info("No valid contacts found for bulk upsert after tenant ID filtering")
		return nil
	}

	operation := func() error {
		tx := r.db.WithContext(ctx).Begin()
		if tx.Error != nil {
			return fmt.Errorf("%w: failed to begin transaction: %w", apperrors.ErrDatabase, tx.Error)
		}
		var txErr error
		defer func() {
			if r := recover(); r != nil {
				tx.Rollback()
				panic(r)
			} else if txErr != nil { // Check the captured error from the operation
				if rbErr := tx.Rollback().Error; rbErr != nil {
					loggerCtx.Error("Failed to rollback transaction after error", zap.Error(rbErr), zap.NamedError("originalTxError", txErr))
				}
			}
		}() // End defer

		// Make sure the unique constraint exists in the DB on (chat_id)
		result := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "chat_id"}},                     // Use chat_id as unique constraint
			DoUpdates: clause.AssignmentColumns(model.ContactUpdateColumns()), // Columns to update on conflict
		}).Create(&validContacts) // Use the filtered slice

		if result.Error != nil {
			// GORM's OnConflict doesn't return constraint violation errors in the same way as a simple Create/Update.
			// It might mask them. We'll wrap generically for now.
			txErr = fmt.Errorf("%w: bulk upsert failed: %w", apperrors.ErrDatabase, result.Error)
			return txErr
		}

		// Commit the transaction
		if commitErr := tx.Commit().Error; commitErr != nil {
			txErr = fmt.Errorf("%w: failed to commit bulk upsert transaction: %w", apperrors.ErrDatabase, commitErr)
			return txErr
		}
		loggerCtx.Info("Bulk upsert successful", zap.Int("contacts_processed", len(validContacts)), zap.Int64("rows_affected", result.RowsAffected))
		return nil // Success
	} // End operation func

	commitPolicy := newRetryPolicy(ctx, commitRetryMaxElapsedTime)
	startTime := utils.Now()
	commitErr := retryableOperation(ctx, commitPolicy, "BulkUpsertContacts Commit", operation)
	observer.ObserveDbOperationDuration("bulk_upsert", "contact", companyID, time.Since(startTime), commitErr)
	if commitErr != nil {
		loggerCtx.Error("Failed to bulk upsert contacts after retries", zap.Error(commitErr))
		// Return the specific error captured within the operation (already wrapped)
		return commitErr
	}
	return nil
}

// FindContactsByAgentIDAndEmptyOriginPaginated finds contacts for a specific agent with empty origin field (not onboarded).
func (r *PostgresRepo) FindContactsByAgentIDAndEmptyOriginPaginated(ctx context.Context, agentID string, limit, offset int) ([]model.Contact, error) {
	companyID, err := tenant.FromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to get tenant ID: %w", apperrors.ErrUnauthorized, err)
	}
	loggerCtx := logger.FromContext(ctx)

	var contacts []model.Contact
	operation := func() error {
		result := r.db.WithContext(ctx).
			Where("agent_id = ? AND company_id = ? AND (origin IS NULL OR origin = '')", agentID, companyID).
			Order("created_at ASC").
			Limit(limit).
			Offset(offset).
			Find(&contacts)
		if result.Error != nil {
			return fmt.Errorf("%w: query failed: %w", apperrors.ErrDatabase, result.Error)
		}
		return nil // Success, even if no records found
	}

	readPolicy := newRetryPolicy(ctx, readRetryMaxElapsedTime)
	startTime := utils.Now()
	findErr := retryableOperation(ctx, readPolicy, "FindContactsByAgentIDAndEmptyOriginPaginated", operation)
	observer.ObserveDbOperationDuration("find_by_agent_empty_origin", "contact", companyID, time.Since(startTime), findErr)

	if findErr != nil {
		loggerCtx.Error("Failed to find contacts by agent_id with empty origin after retries",
			zap.String("agent_id", agentID),
			zap.String("company_id", companyID),
			zap.Int("limit", limit),
			zap.Int("offset", offset),
			zap.Error(findErr))
		return nil, findErr // Already wrapped
	}
	if contacts == nil { // Ensure empty slice is returned, not nil
		return []model.Contact{}, nil
	}
	return contacts, nil
}
