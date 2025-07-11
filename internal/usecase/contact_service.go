package usecase

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	apperrors "gitlab.com/timkado/api/daisi-wa-events-processor/internal/apperrors"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/tenant"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/validator"
	"gorm.io/datatypes"

	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/model"
	"gitlab.com/timkado/api/daisi-wa-events-processor/pkg/logger"
	"gitlab.com/timkado/api/daisi-wa-events-processor/pkg/utils"
	"go.uber.org/zap"
)

// ProcessHistoricalContacts handles the ingestion of historical contact data.
func (s *EventService) ProcessHistoricalContacts(ctx context.Context, contacts []model.UpsertContactPayload, metadata *model.LastMetadata) error {
	log := logger.FromContext(ctx)
	start := utils.Now()

	if len(contacts) == 0 {
		log.Warn("No contacts to process in historical contacts payload")
		return nil
	}

	// Extract tenant ID
	companyID, err := tenant.FromContext(ctx)
	if err != nil || companyID == "" {
		log.Error("Failed to get tenant ID from context",
			zap.Error(err),
		)
		return apperrors.NewFatal(err, "failed to get tenant ID from context")
	}

	// Convert metadata to datatypes.JSON for storage
	var metadataJSON datatypes.JSON
	if metadata != nil {
		metadataMap := map[string]interface{}{
			"consumer_sequence": metadata.ConsumerSequence,
			"stream_sequence":   metadata.StreamSequence,
			"stream":            metadata.Stream,
			"consumer":          metadata.Consumer,
			"domain":            metadata.Domain,
			"message_id":        metadata.MessageID,
			"message_subject":   metadata.MessageSubject,
			"processed_at":      utils.Now(),
		}
		metadataJSON = utils.MustMarshalJSON(metadataMap)
	}

	dbContacts := make([]model.Contact, 0, len(contacts))
	// Basic validation (more complex validation can be added if needed)
	for i, contact := range contacts {
		if err := validator.Validate(contact); err != nil {
			log.Error("Validation failed for historical contact",
				zap.String("phone_number", contact.PhoneNumber),
				zap.String("agent_id", contact.AgentID),
				zap.Int("index", i),
				zap.Error(err),
			)
			// Validation error is fatal
			return apperrors.NewFatal(err, "validation failed for historical contact at index %d", i)
		}

		// Validate that cluster matches tenant ID
		if err := validateClusterTenant(ctx, contact.CompanyID); err != nil {
			log.Error("CompanyID validation failed for historical contact",
				zap.String("phone_number", contact.PhoneNumber),
				zap.String("company_id", contact.CompanyID),
				zap.String("context_company_id", companyID),
				zap.Int("index", i),
				zap.Error(err),
			)
			// CompanyID mismatch is fatal
			return apperrors.NewFatal(err, "company validation failed for historical contact at index %d", i)
		}

		ctc := model.Contact{
			ChatID:       contact.ChatID,
			PhoneNumber:  contact.PhoneNumber,
			CompanyID:    contact.CompanyID,
			PushName:     contact.PushName,
			AgentID:      contact.AgentID,
			LastMetadata: metadataJSON,
		}

		// Set other timestamps
		if ctc.CreatedAt.IsZero() {
			ctc.CreatedAt = utils.Now()
		}
		ctc.UpdatedAt = utils.Now()
		dbContacts = append(dbContacts, ctc)
	}

	// Perform bulk upsert using the contact repository
	if err := s.contactRepo.BulkUpsert(ctx, dbContacts); err != nil {
		// Check the error type from the repository
		logFields := []zap.Field{
			zap.Int("count", len(dbContacts)),
			zap.Error(err),
		}
		if errors.Is(err, apperrors.ErrDatabase) || errors.Is(err, apperrors.ErrTimeout) || errors.Is(err, apperrors.ErrConflict) {
			log.Warn("Potentially retryable error during historical contact bulk upsert", logFields...)
			return apperrors.NewRetryable(err, "retryable repository error processing historical contacts")
		} else {
			log.Error("Fatal error during historical contact bulk upsert", logFields...)
			return apperrors.NewFatal(err, "fatal repository error processing historical contacts")
		}
	}

	log.Info("Successfully processed historical contacts",
		zap.Int("count", len(contacts)),
		zap.Duration("duration", time.Since(start)),
	)
	return nil
}

// UpsertContact processes the upsertion of a single contact.
func (s *EventService) UpsertContact(ctx context.Context, payload model.UpsertContactPayload, metadata *model.LastMetadata) error {
	log := logger.FromContext(ctx)

	// Validate input
	if err := validator.Validate(payload); err != nil {
		log.Error("Contact validation failed",
			zap.String("phone_number", payload.PhoneNumber),
			zap.String("agent_id", payload.AgentID),
			zap.Error(err),
		)
		return apperrors.NewFatal(err, "contact validation failed")
	}

	// Extract tenant ID
	companyID, err := tenant.FromContext(ctx)
	if err != nil || companyID == "" {
		log.Error("Failed to get tenant ID from context",
			zap.Error(err),
		)
		return apperrors.NewFatal(err, "failed to get tenant ID from context")
	}

	// Validate that CompanyID matches tenant ID
	if err := validateClusterTenant(ctx, payload.CompanyID); err != nil {
		log.Error("CompanyID validation failed for contact",
			zap.String("phone_number", payload.PhoneNumber),
			zap.String("company_id", payload.CompanyID),
			zap.String("context_company_id", companyID),
			zap.Error(err),
		)
		return apperrors.NewFatal(err, "contact CompanyID mismatch")
	}

	// Convert metadata to datatypes.JSON for storage
	var metadataJSON datatypes.JSON
	if metadata != nil {
		metadataMap := map[string]interface{}{
			"consumer_sequence": metadata.ConsumerSequence,
			"stream_sequence":   metadata.StreamSequence,
			"stream":            metadata.Stream,
			"consumer":          metadata.Consumer,
			"domain":            metadata.Domain,
			"message_id":        metadata.MessageID,
			"message_subject":   metadata.MessageSubject,
			"processed_at":      utils.Now(),
		}
		metadataJSON = utils.MustMarshalJSON(metadataMap)
	}

	// Transform to database model
	dbContact := model.Contact{
		PhoneNumber:  payload.PhoneNumber,
		CompanyID:    payload.CompanyID, // Already validated against companyID
		PushName:     payload.PushName,
		AgentID:      payload.AgentID,
		LastMetadata: metadataJSON,
	}
	// Set timestamps
	if dbContact.CreatedAt.IsZero() {
		dbContact.CreatedAt = utils.Now()
	}
	dbContact.UpdatedAt = utils.Now()

	// Save to repo
	if err := s.contactRepo.Save(ctx, dbContact); err != nil {
		logFields := []zap.Field{
			zap.String("phone_number", dbContact.PhoneNumber),
			zap.String("agent_id", dbContact.AgentID),
			zap.Error(err),
		}
		if errors.Is(err, apperrors.ErrDatabase) || errors.Is(err, apperrors.ErrTimeout) || errors.Is(err, apperrors.ErrConflict) {
			log.Warn("Potentially retryable error during contact upsert", logFields...)
			return apperrors.NewRetryable(err, "retryable repository error during contact upsert")
		} else {
			log.Error("Fatal error during contact upsert", logFields...)
			return apperrors.NewFatal(err, "fatal repository error during contact upsert")
		}
	}

	log.Info("Successfully upserted contact", zap.String("phone_number", dbContact.PhoneNumber), zap.String("agent_id", dbContact.AgentID))
	return nil
}

// UpdateContact processes the update of an existing contact using partial updates.
func (s *EventService) UpdateContact(ctx context.Context, payload model.UpdateContactPayload, metadata *model.LastMetadata) error {
	log := logger.FromContext(ctx)

	// Validate input
	if err := validator.Validate(payload); err != nil {
		log.Error("Contact update validation failed",
			zap.String("phone_number", payload.PhoneNumber),
			zap.String("agent_id", payload.AgentID),
			zap.Error(err),
		)
		return apperrors.NewFatal(err, "contact update validation failed")
	}

	// Extract tenant ID
	companyID, err := tenant.FromContext(ctx)
	if err != nil || companyID == "" {
		log.Error("Failed to get tenant ID from context",
			zap.Error(err),
		)
		return apperrors.NewFatal(err, "failed to get tenant ID from context")
	}

	// Validate that CompanyID matches tenant ID
	if err := validateClusterTenant(ctx, payload.CompanyID); err != nil {
		log.Error("CompanyID validation failed for contact update",
			zap.String("phone_number", payload.PhoneNumber),
			zap.String("company_id", payload.CompanyID),
			zap.String("context_company_id", companyID),
			zap.Error(err),
		)
		return apperrors.NewFatal(err, "contact update CompanyID mismatch")
	}

	// First, get the existing contact
	existingContact, err := s.contactRepo.FindByPhoneAndAgentID(ctx, payload.PhoneNumber, payload.AgentID)
	if err != nil {
		logFields := []zap.Field{
			zap.String("phone_number", payload.PhoneNumber),
			zap.String("agent_id", payload.AgentID),
			zap.Error(err),
		}
		// Decide if FindByPhoneAndAgentID error is retryable
		if errors.Is(err, apperrors.ErrNotFound) {
			log.Warn("Contact not found for update", logFields...)
			return apperrors.NewFatal(err, "contact not found for update (phone_number: %s, agent_id: %s)", payload.PhoneNumber, payload.AgentID)
		} else if errors.Is(err, apperrors.ErrDatabase) || errors.Is(err, apperrors.ErrTimeout) || errors.Is(err, apperrors.ErrConflict) {
			log.Warn("Potentially retryable error fetching contact for update", logFields...)
			return apperrors.NewRetryable(err, "retryable repository error fetching contact for update")
		} else {
			log.Error("Fatal error fetching contact for update", logFields...)
			return apperrors.NewFatal(err, "fatal repository error fetching contact for update")
		}
	}
	// Note: contactRepo.FindByID already returns ErrNotFound directly if record is not found.
	// So the explicit check `if existingContact == nil` is no longer needed here.

	// Verify CompanyID consistency (already checked by validateClusterTenant and FindByID scope)
	// if existingContact.CompanyID != payload.CompanyID { ... } // This check is likely redundant now

	// Convert metadata to datatypes.JSON for storage
	var metadataJSON datatypes.JSON
	if metadata != nil {
		metadataMap := map[string]interface{}{
			"consumer_sequence": metadata.ConsumerSequence,
			"stream_sequence":   metadata.StreamSequence,
			"stream":            metadata.Stream,
			"consumer":          metadata.Consumer,
			"domain":            metadata.Domain,
			"message_id":        metadata.MessageID,
			"message_subject":   metadata.MessageSubject,
			"processed_at":      utils.Now(),
		}
		metadataJSON = utils.MustMarshalJSON(metadataMap)
	}

	// Apply partial updates from payload
	updateNeeded := false
	contactToUpdate := *existingContact // Create a copy to modify

	// Always update metadata and UpdatedAt if an update is performed or metadata exists
	if updateNeeded || metadata != nil {
		contactToUpdate.LastMetadata = metadataJSON
		contactToUpdate.UpdatedAt = utils.Now()
		updateNeeded = true // Ensure update runs if only metadata changed
	}

	// Perform update only if changes were detected or metadata provided
	if updateNeeded {
		if err := s.contactRepo.Update(ctx, contactToUpdate); err != nil {
			logFields := []zap.Field{
				zap.String("phone_number", contactToUpdate.PhoneNumber),
				zap.String("agent_id", contactToUpdate.AgentID),
				zap.Error(err),
			}
			// Decide if Update error is retryable
			if errors.Is(err, apperrors.ErrDatabase) || errors.Is(err, apperrors.ErrTimeout) || errors.Is(err, apperrors.ErrConflict) {
				log.Warn("Potentially retryable error during contact update", logFields...)
				return apperrors.NewRetryable(err, "retryable repository error during contact update")
			} else {
				log.Error("Fatal error during contact update", logFields...)
				return apperrors.NewFatal(err, "fatal repository error during contact update")
			}
		}
		log.Info("Successfully updated contact", zap.String("phone_number", contactToUpdate.PhoneNumber), zap.String("agent_id", contactToUpdate.AgentID))
	} else {
		log.Info("No fields to update for contact", zap.String("phone_number", payload.PhoneNumber), zap.String("agent_id", payload.AgentID))
	}

	return nil
}

// SyncContactsPostScan syncs contacts after QR code scan with bloom filter optimization
func (s *EventService) SyncContactsPostScan(ctx context.Context, agentID string) error {
	companyID, err := tenant.FromContext(ctx)
	if err != nil {
		return fmt.Errorf("failed to get tenant ID: %w", err)
	}

	log := logger.FromContext(ctx)
	const batchSize = 500
	offset := 0
	processedCount := 0
	updatedCount := 0
	skippedGroupCount := 0

	// Get bloom filter from onboarding worker
	bloomFilter := s.onboardingWorker.GetBloomFilter()

	for {
		// Find contacts without Origin (not fully onboarded) in batches
		contacts, err := s.contactRepo.FindByAgentIDAndEmptyOriginPaginated(ctx, agentID, batchSize, offset)
		if err != nil {
			return fmt.Errorf("failed to fetch contacts: %w", err)
		}

		if len(contacts) == 0 {
			break // No more contacts
		}

		for _, contact := range contacts {
			processedCount++

			// Skip potential groups based on phone number length
			// WhatsApp group IDs are typically much longer than regular phone numbers
			// Regular phone numbers: 10-15 digits
			// Group IDs: 18+ characters
			cleanedPhone := strings.ReplaceAll(contact.PhoneNumber, "-", "")
			cleanedPhone = strings.ReplaceAll(cleanedPhone, " ", "")
			if len(cleanedPhone) > 17 {
				log.Debug("Skipping potential group contact (long ID)",
					zap.String("contact_id", contact.ID),
					zap.String("phone_number", contact.PhoneNumber),
					zap.Int("phone_length", len(cleanedPhone)))
				skippedGroupCount++
				continue
			}

			// Skip if already in bloom filter (already onboarded)
			if bloomFilter.MightExist(companyID, agentID, contact.PhoneNumber) {
				// Double-check with DB to handle false positives
				existingLog, _ := s.onboardingLogRepo.FindByPhoneAndAgentID(ctx, contact.PhoneNumber, agentID)
				if existingLog != nil {
					log.Debug("Contact already onboarded (found in bloom filter and DB), skipping",
						zap.String("contact_id", contact.ID))
					continue
				}
			}

			// Check if contact has first message IN
			firstInMsg, err := s.messageRepo.FindFirstIncomingMessageByPhoneAndAgent(ctx, contact.PhoneNumber, contact.AgentID)
			if err != nil && !errors.Is(err, apperrors.ErrNotFound) {
				log.Error("Error finding first message",
					zap.String("contact_id", contact.ID),
					zap.Error(err))
				continue
			}

			if firstInMsg != nil {
				// Update contact with default onboarding data (Phase 1 - hardcoded)
				contact.Origin = "DirectChat"
				contact.Tags = "Onboarded"
				if contact.AssignedTo == "" {
					contact.AssignedTo = ""
				}
				contact.FirstMessageID = firstInMsg.MessageID
				contact.FirstMessageTimestamp = firstInMsg.MessageTimestamp
				contact.UpdatedAt = utils.Now()

				if err := s.contactRepo.Update(ctx, contact); err != nil {
					log.Error("Failed to update contact in post-scan sync",
						zap.String("contact_id", contact.ID),
						zap.Error(err))
					continue
				}

				// Create onboarding log
				newLog := model.OnboardingLog{
					MessageID:   firstInMsg.MessageID,
					AgentID:     contact.AgentID,
					CompanyID:   contact.CompanyID,
					PhoneNumber: contact.PhoneNumber,
					Timestamp:   firstInMsg.MessageTimestamp,
					LastMetadata: utils.MustMarshalJSON(map[string]interface{}{
						"source":    "post_scan_sync",
						"synced_at": utils.Now(),
					}),
				}

				if err := s.onboardingLogRepo.Save(ctx, newLog); err != nil {
					log.Error("Failed to save onboarding log in sync",
						zap.String("contact_id", contact.ID),
						zap.Error(err))
				} else {
					// Add to bloom filter
					bloomFilter.Add(companyID, agentID, contact.PhoneNumber)
					updatedCount++
					log.Info("Successfully onboarded contact in post-scan sync",
						zap.String("contact_id", contact.ID),
						zap.String("phone_number", contact.PhoneNumber))
				}
			} else {
				log.Debug("No incoming message found for contact, skipping onboarding",
					zap.String("contact_id", contact.ID),
					zap.String("phone_number", contact.PhoneNumber))
			}
		}

		offset += batchSize

		log.Debug("Processed batch in post-scan sync",
			zap.Int("batch_size", len(contacts)),
			zap.Int("total_processed", processedCount),
			zap.Int("total_updated", updatedCount),
			zap.Int("skipped_groups", skippedGroupCount))

		if len(contacts) < batchSize {
			break
		}
	}

	log.Info("Post-scan sync completed",
		zap.String("agent_id", agentID),
		zap.Int("processed", processedCount),
		zap.Int("updated", updatedCount),
		zap.Int("skipped_groups", skippedGroupCount))

	return nil
}
