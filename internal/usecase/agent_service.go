package usecase

import (
	"context"
	"errors"

	apperrors "gitlab.com/timkado/api/daisi-wa-events-processor/internal/apperrors"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/model"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/tenant"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/validator"
	"gitlab.com/timkado/api/daisi-wa-events-processor/pkg/logger"
	"gitlab.com/timkado/api/daisi-wa-events-processor/pkg/utils"
	"go.uber.org/zap"
	"gorm.io/datatypes"
)

// UpsertAgent handles the ingestion or update of agent information.
func (s *EventService) UpsertAgent(ctx context.Context, payload model.UpsertAgentPayload, metadata *model.LastMetadata) error {
	log := logger.FromContext(ctx)

	// Validate input payload
	if err := validator.Validate(payload); err != nil {
		log.Error("Agent payload validation failed",
			zap.String("agent_id", payload.AgentID),
			zap.Error(err),
		)
		// Validation errors are fatal for this operation
		return apperrors.NewFatal(err, "agent payload validation failed")
	}

	// Extract tenant ID (expected to be the CompanyID)
	companyID, err := tenant.FromContext(ctx)
	if err != nil || companyID == "" {
		log.Error("Failed to get tenant ID from context for agent upsert",
			zap.String("agent_id", payload.AgentID),
			zap.Error(err),
		)
		// Cannot proceed without tenant context, fatal
		return apperrors.NewFatal(err, "failed to get tenant ID from context")
	}

	// Validate that the CompanyID in the payload matches the tenant ID from the context
	if err := validateClusterTenant(ctx, payload.CompanyID); err != nil {
		log.Error("CompanyID validation failed for agent",
			zap.String("agent_id", payload.AgentID),
			zap.String("payload_company_id", payload.CompanyID),
			zap.String("context_company_id", companyID),
			zap.Error(err),
		)
		// Mismatched company ID is fatal
		return apperrors.NewFatal(err, "agent CompanyID mismatch")
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

	// Transform payload to database model
	dbAgent := model.Agent{
		AgentID:      payload.AgentID,
		QRCode:       payload.QRCode,
		Status:       payload.Status,
		AgentName:    payload.AgentName,
		HostName:     payload.HostName,
		Version:      payload.Version,
		CompanyID:    payload.CompanyID, // Already validated against companyID
		LastMetadata: metadataJSON,
	}

	// Save to repo (SaveAgent handles both insert and update based on agent_id)
	if err := s.agentRepo.Save(ctx, dbAgent); err != nil {
		// The storage layer (via checkConstraintViolation) should already return
		// standard apperrors like ErrDatabase, ErrDuplicate, ErrBadRequest, ErrNotFound.
		// Decide if the overall UpsertAgent operation is retryable based on those.
		logFields := []zap.Field{
			zap.String("agent_id", dbAgent.AgentID),
			zap.Error(err),
		}

		// Check for errors that might indicate a transient issue
		if errors.Is(err, apperrors.ErrDatabase) || errors.Is(err, apperrors.ErrTimeout) || errors.Is(err, apperrors.ErrConflict) {
			// General DB errors, timeouts, or potential transient conflicts (like deadlocks mapped to ErrDatabase)
			// are considered retryable for this upsert operation.
			log.Warn("Potentially retryable error during agent upsert", logFields...)
			return apperrors.NewRetryable(err, "retryable repository error during agent upsert")
		} else {
			// Errors like ErrDuplicate, ErrBadRequest, ErrUnauthorized, ErrNotFound,
			// ErrValidation (though caught earlier), or any other non-database error
			// indicate a problem with the request or state that retrying won't fix.
			// Treat these, and any unexpected error, as fatal for this operation.
			log.Error("Fatal error during agent upsert", logFields...)
			return apperrors.NewFatal(err, "fatal repository error during agent upsert")
		}
	}

	log.Info("Successfully upserted agent", zap.String("agent_id", dbAgent.AgentID))
	return nil
}
