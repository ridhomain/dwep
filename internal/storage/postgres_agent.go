package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cenkalti/backoff/v4"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	apperrors "gitlab.com/timkado/api/daisi-wa-events-processor/internal/apperrors"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/model"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/observer"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/tenant"
	"gitlab.com/timkado/api/daisi-wa-events-processor/pkg/logger"
	"gitlab.com/timkado/api/daisi-wa-events-processor/pkg/utils"
)

// Define retry constant for bulk operations
const (
	bulkCommitRetryMaxElapsedTime = 15 * time.Second // Longer timeout for bulk operations
)

// --- Agent Repository Methods ---

// SaveAgent saves or updates an agent record. It uses agent_id as the unique identifier for upsert logic.
func (r *PostgresRepo) SaveAgent(ctx context.Context, agent model.Agent) error {
	companyID, err := tenant.FromContext(ctx)
	if err != nil {
		return fmt.Errorf("%w: failed to get tenant ID from context: %w", apperrors.ErrUnauthorized, err)
	}

	// throw error if companyID not same as agent.CompanyID
	if companyID != agent.CompanyID {
		return fmt.Errorf("%w: agent CompanyID %s does not match tenant ID %s", apperrors.ErrBadRequest, agent.CompanyID, companyID)
	}

	operation := func() error {
		logger.FromContext(ctx).Info("Begin DB Ops", zap.String("agent_id", agent.AgentID), zap.String("agent_name", agent.AgentName))

		tx := r.db.WithContext(ctx).Begin()
		if tx.Error != nil {
			return fmt.Errorf("%w: failed to begin transaction: %w", apperrors.ErrDatabase, tx.Error)
		}
		var txErr error
		defer func() {
			if p := recover(); p != nil {
				tx.Rollback()
				panic(p)
			} else if txErr != nil {
				if rbErr := tx.Rollback().Error; rbErr != nil {
					logger.FromContext(ctx).Error("Failed to rollback transaction after error", zap.Error(rbErr), zap.NamedError("originalTxError", txErr))
				}
			}
		}()

		var existingAgent model.Agent
		// Use agent_id and company_id to find existing record
		result := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("agent_id = ? AND company_id = ?", agent.AgentID, agent.CompanyID).
			First(&existingAgent)
		findErr := result.Error

		if findErr != nil {
			if errors.Is(findErr, gorm.ErrRecordNotFound) {
				// New record, insert
				logger.FromContext(ctx).Info("No Existing, Insert Record", zap.String("agent_id", agent.AgentID), zap.String("agent_name", agent.AgentName))
				if createErr := tx.Create(&agent).Error; createErr != nil {
					logger.FromContext(ctx).Error("Create Insert Record Error", zap.String("agent_id", agent.AgentID), zap.String("agent_name", agent.AgentName), zap.Error(createErr))
					txErr = checkConstraintViolation(createErr)
					return txErr
				}
			} else {
				// Failure during SELECT FOR UPDATE
				txErr = fmt.Errorf("%w: failed to lock agent row: %w", apperrors.ErrDatabase, findErr)
				return txErr
			}
		} else {
			// Update existing record using the found ID
			agent.ID = existingAgent.ID               // Ensure we update using the correct PK
			agent.CreatedAt = existingAgent.CreatedAt // Preserve original creation timestamp
			agent.UpdatedAt = utils.Now()             // Explicitly set updated time

			logger.FromContext(ctx).Info("Found Existing, Update Record", zap.String("agent_id", agent.AgentID), zap.String("agent_name", agent.AgentName))
			if updateErr := tx.Model(&existingAgent).Updates(agent).Error; updateErr != nil {
				txErr = checkConstraintViolation(updateErr)
				return txErr
			}
		}
		if commitErr := tx.Commit().Error; commitErr != nil {
			txErr = fmt.Errorf("%w: failed to commit save transaction: %w", apperrors.ErrDatabase, commitErr)
			return txErr
		}
		return nil
	}

	commitPolicy := newRetryPolicy(ctx, commitRetryMaxElapsedTime)
	startTime := utils.Now()
	commitErr := retryableOperation(ctx, commitPolicy, "SaveAgent Commit", operation)
	observer.ObserveDbOperationDuration("save", "agent", companyID, time.Since(startTime), commitErr)
	if commitErr != nil {
		logger.FromContext(ctx).Error("Failed to save agent after retries", zap.String("agent_id", agent.AgentID), zap.Error(commitErr))
		return commitErr // Already wrapped by operation or checkConstraintViolation
	}
	logger.FromContext(ctx).Info("Done DB Ops", zap.String("agent_id", agent.AgentID), zap.String("agent_name", agent.AgentName))
	return nil
}

// UpdateAgent updates specific fields of an existing agent record based on AgentID.
func (r *PostgresRepo) UpdateAgent(ctx context.Context, agent model.Agent) error {
	companyID, err := tenant.FromContext(ctx)
	if err != nil {
		return fmt.Errorf("%w: failed to get tenant ID from context: %w", apperrors.ErrUnauthorized, err)
	}
	// throw error if companyID not same as agent.CompanyID
	if companyID != agent.CompanyID {
		return fmt.Errorf("%w: agent CompanyID %s does not match tenant ID %s", apperrors.ErrBadRequest, agent.CompanyID, companyID)
	}

	agent.UpdatedAt = utils.Now() // Ensure UpdatedAt is set

	operation := func() error {
		tx := r.db.WithContext(ctx).Begin()
		if tx.Error != nil {
			return fmt.Errorf("%w: failed to begin transaction: %w", apperrors.ErrDatabase, tx.Error)
		}
		var txErr error
		defer func() {
			if p := recover(); p != nil {
				tx.Rollback()
				panic(p)
			} else if txErr != nil {
				if rbErr := tx.Rollback().Error; rbErr != nil {
					logger.FromContext(ctx).Error("Failed to rollback transaction after error", zap.Error(rbErr), zap.NamedError("originalTxError", txErr))
				}
			}
		}()

		// Find existing record by agent_id to get the internal primary key (ID)
		var existingAgent model.Agent
		result := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("agent_id = ? AND company_id = ?", agent.AgentID, agent.CompanyID).
			First(&existingAgent)
		findErr := result.Error

		if findErr != nil {
			if errors.Is(findErr, gorm.ErrRecordNotFound) {
				txErr = fmt.Errorf("%w: agent not found for update (AgentID: %s, CompanyID: %s): %w", apperrors.ErrNotFound, agent.AgentID, companyID, findErr)
				return backoff.Permanent(txErr) // Make NotFound permanent for retry policy
			}
			txErr = fmt.Errorf("%w: failed to lock agent row for update: %w", apperrors.ErrDatabase, findErr)
			return txErr
		}

		// Ensure we are updating the correct record using the internal ID
		agent.ID = existingAgent.ID               // Use the PK found
		agent.CreatedAt = existingAgent.CreatedAt // Preserve created_at
		updateResult := tx.Model(&existingAgent).Updates(agent)
		if updateResult.Error != nil {
			txErr = checkConstraintViolation(updateResult.Error)
			return txErr
		}
		if updateResult.RowsAffected == 0 {
			logger.FromContext(ctx).Warn("UpdateAgent resulted in 0 rows affected", zap.String("agent_id", agent.AgentID))
		}
		if commitErr := tx.Commit().Error; commitErr != nil {
			txErr = fmt.Errorf("%w: failed to commit update transaction: %w", apperrors.ErrDatabase, commitErr)
			return txErr
		}
		return nil
	}

	commitPolicy := newRetryPolicy(ctx, commitRetryMaxElapsedTime)
	startTime := utils.Now()
	commitErr := retryableOperation(ctx, commitPolicy, "UpdateAgent Commit", operation)
	observer.ObserveDbOperationDuration("update", "agent", companyID, time.Since(startTime), commitErr)
	if commitErr != nil {
		logger.FromContext(ctx).Error("Failed to update agent after retries", zap.String("agent_id", agent.AgentID), zap.Error(commitErr))
		return commitErr // Already wrapped
	}
	return nil
}

// UpdateAgentStatus updates only the status of a specific agent based on AgentID.
func (r *PostgresRepo) UpdateAgentStatus(ctx context.Context, agentID string, status string) error {
	companyID, err := tenant.FromContext(ctx)
	if err != nil {
		return fmt.Errorf("%w: failed to get tenant ID from context: %w", apperrors.ErrUnauthorized, err)
	}
	loggerCtx := logger.FromContext(ctx)

	operation := func() error {
		tx := r.db.WithContext(ctx).Begin()
		if tx.Error != nil {
			return fmt.Errorf("%w: failed to begin transaction: %w", apperrors.ErrDatabase, tx.Error)
		}
		var txErr error
		defer func() {
			if p := recover(); p != nil {
				tx.Rollback()
				panic(p)
			} else if txErr != nil {
				if rbErr := tx.Rollback().Error; rbErr != nil {
					loggerCtx.Error("Failed to rollback transaction after error", zap.Error(rbErr), zap.NamedError("originalTxError", txErr))
				}
			}
		}()

		updateResult := tx.Model(&model.Agent{}).
			Where("agent_id = ? AND company_id = ?", agentID, companyID).
			Updates(map[string]interface{}{"status": status, "updated_at": utils.Now()})

		if updateResult.Error != nil {
			// Apply constraint check logic here as well
			txErr = checkConstraintViolation(updateResult.Error)
			return txErr
		}
		if updateResult.RowsAffected == 0 {
			// Agent might not exist. Return ErrNotFound.
			loggerCtx.Warn("UpdateAgentStatus resulted in 0 rows affected, agent might not exist",
				zap.String("agent_id", agentID),
				zap.String("company_id", companyID))
			// Return ErrNotFound as the update targeted a non-existent record.
			txErr = fmt.Errorf("%w: agent_id %s not found for status update", apperrors.ErrNotFound, agentID)
			return txErr // Return error immediately, no need to commit
		}
		if commitErr := tx.Commit().Error; commitErr != nil {
			txErr = fmt.Errorf("%w: failed to commit agent status update transaction: %w", apperrors.ErrDatabase, commitErr)
			return txErr
		}
		return nil
	}

	commitPolicy := newRetryPolicy(ctx, commitRetryMaxElapsedTime)
	commitErr := retryableOperation(ctx, commitPolicy, "UpdateAgentStatus Commit", operation)
	if commitErr != nil {
		loggerCtx.Error("Failed to update agent status after retries",
			zap.String("agent_id", agentID),
			zap.String("company_id", companyID),
			zap.Error(commitErr))
		return commitErr // Already wrapped
	}
	return nil
}

// FindAgentByID finds an agent by its internal primary key ID.
func (r *PostgresRepo) FindAgentByID(ctx context.Context, id int64) (*model.Agent, error) {
	companyID, err := tenant.FromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to get tenant ID from context: %w", apperrors.ErrUnauthorized, err)
	}
	loggerCtx := logger.FromContext(ctx)

	var agent model.Agent
	operation := func() error {
		result := r.db.WithContext(ctx).Where("id = ? AND company_id = ?", id, companyID).First(&agent)
		if result.Error != nil {
			// Use checkConstraintViolation which handles ErrRecordNotFound -> ErrNotFound
			return checkConstraintViolation(result.Error)
		}
		return nil // Success
	}

	readPolicy := newRetryPolicy(ctx, readRetryMaxElapsedTime)
	findErr := retryableOperation(ctx, readPolicy, "FindAgentByID", operation)

	if findErr != nil {
		// Check if the specific error was ErrNotFound
		if errors.Is(findErr, apperrors.ErrNotFound) {
			return nil, apperrors.ErrNotFound // Return the standard error
		}
		// Log and return other wrapped errors (e.g., ErrDatabase)
		loggerCtx.Error("Failed to find agent by internal ID after retries",
			zap.Int64("id", id),
			zap.String("company_id", companyID),
			zap.Error(findErr)) // findErr is already wrapped
		return nil, findErr
	}
	return &agent, nil
}

// FindAgentByAgentID finds an agent by the AgentID (external identifier).
func (r *PostgresRepo) FindAgentByAgentID(ctx context.Context, agentID string) (*model.Agent, error) {
	companyID, err := tenant.FromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to get tenant ID from context: %w", apperrors.ErrUnauthorized, err)
	}
	loggerCtx := logger.FromContext(ctx)

	var agent model.Agent
	operation := func() error {
		result := r.db.WithContext(ctx).Where("agent_id = ? AND company_id = ?", agentID, companyID).First(&agent)
		if result.Error != nil {
			return checkConstraintViolation(result.Error)
		}
		return nil
	}

	readPolicy := newRetryPolicy(ctx, readRetryMaxElapsedTime)
	findErr := retryableOperation(ctx, readPolicy, "FindAgentByAgentID", operation)

	if findErr != nil {
		if errors.Is(findErr, apperrors.ErrNotFound) {
			return nil, apperrors.ErrNotFound
		}
		loggerCtx.Error("Failed to find agent by AgentID after retries",
			zap.String("agent_id", agentID),
			zap.String("company_id", companyID),
			zap.Error(findErr))
		return nil, findErr
	}
	return &agent, nil
}

// FindAgentsByStatus finds all agents matching a specific status.
func (r *PostgresRepo) FindAgentsByStatus(ctx context.Context, status string) ([]model.Agent, error) {
	companyID, err := tenant.FromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to get tenant ID from context: %w", apperrors.ErrUnauthorized, err)
	}
	loggerCtx := logger.FromContext(ctx)

	var agents []model.Agent
	operation := func() error {
		result := r.db.WithContext(ctx).Where("status = ? AND company_id = ?", status, companyID).Find(&agents)
		if result.Error != nil {
			// Find multiple doesn't return ErrRecordNotFound, check RowsAffected or return generic DB error
			return fmt.Errorf("%w: query failed: %w", apperrors.ErrDatabase, result.Error)
		}
		// No error if 0 rows found for Find multiple
		return nil
	}

	readPolicy := newRetryPolicy(ctx, readRetryMaxElapsedTime)
	findErr := retryableOperation(ctx, readPolicy, "FindAgentsByStatus", operation)

	if findErr != nil {
		// Log and return errors (already wrapped as ErrDatabase by operation)
		loggerCtx.Error("Failed to find agents by status after retries",
			zap.String("status", status),
			zap.String("company_id", companyID),
			zap.Error(findErr))
		return nil, findErr
	}
	return agents, nil
}

// FindAgentsByCompanyID finds all agents for a given company ID.
func (r *PostgresRepo) FindAgentsByCompanyID(ctx context.Context, companyID string) ([]model.Agent, error) {
	// Verify context tenant matches requested companyID for authorization
	contextCompanyID, err := tenant.FromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to get tenant ID from context: %w", apperrors.ErrUnauthorized, err)
	}
	if contextCompanyID != companyID {
		return nil, fmt.Errorf("%w: requested CompanyID %s does not match context tenant ID %s", apperrors.ErrUnauthorized, companyID, contextCompanyID)
	}
	loggerCtx := logger.FromContext(ctx)

	var agents []model.Agent
	operation := func() error {
		result := r.db.WithContext(ctx).Where("company_id = ?", companyID).Find(&agents)
		if result.Error != nil {
			return fmt.Errorf("%w: query failed: %w", apperrors.ErrDatabase, result.Error)
		}
		return nil
	}

	readPolicy := newRetryPolicy(ctx, readRetryMaxElapsedTime)
	findErr := retryableOperation(ctx, readPolicy, "FindAgentsByCompanyID", operation)

	if findErr != nil {
		loggerCtx.Error("Failed to find agents by CompanyID after retries",
			zap.String("company_id", companyID),
			zap.Error(findErr))
		return nil, findErr // Already wrapped
	}
	return agents, nil
}

// BulkUpsertAgents performs a bulk upsert operation for agent records.
// It uses agent_id and company_id as the conflict target.
func (r *PostgresRepo) BulkUpsertAgents(ctx context.Context, agents []model.Agent) error {
	if len(agents) == 0 {
		return nil
	}
	companyID, err := tenant.FromContext(ctx)
	if err != nil {
		return fmt.Errorf("%w: failed to get tenant ID from context: %w", apperrors.ErrUnauthorized, err)
	}
	loggerCtx := logger.FromContext(ctx)

	validAgents := make([]model.Agent, 0, len(agents))
	for i := range agents {
		if agents[i].CompanyID != companyID {
			loggerCtx.Warn("Skipping agent in bulk upsert due to mismatched CompanyID",
				zap.String("agent_id", agents[i].AgentID),
				zap.String("agent_company_id", agents[i].CompanyID),
				zap.String("expected_company_id", companyID))
			continue
		}
		agents[i].UpdatedAt = utils.Now() // Ensure UpdatedAt is set
		validAgents = append(validAgents, agents[i])
	}

	if len(validAgents) == 0 {
		loggerCtx.Info("No valid agents found for bulk upsert after tenant ID filtering")
		return nil
	}

	operation := func() error {
		tx := r.db.WithContext(ctx).Begin()
		if tx.Error != nil {
			return fmt.Errorf("%w: failed to begin transaction: %w", apperrors.ErrDatabase, tx.Error)
		}
		var txErr error
		defer func() {
			if p := recover(); p != nil {
				tx.Rollback()
				panic(p)
			} else if txErr != nil {
				if rbErr := tx.Rollback().Error; rbErr != nil {
					loggerCtx.Error("Failed to rollback transaction after error", zap.Error(rbErr), zap.NamedError("originalTxError", txErr))
				}
			}
		}()

		// Upsert based on agent_id and company_id constraint
		result := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "agent_id"}, {Name: "company_id"}}, // Conflict target
			DoUpdates: clause.AssignmentColumns(model.AgentUpdateColumns()),      // Columns to update
		}).Create(&validAgents)

		if result.Error != nil {
			// Wrap potentially masked errors from OnConflict
			txErr = fmt.Errorf("%w: bulk upsert agents failed: %w", apperrors.ErrDatabase, result.Error)
			return txErr
		}

		if commitErr := tx.Commit().Error; commitErr != nil {
			txErr = fmt.Errorf("%w: failed to commit bulk upsert agents transaction: %w", apperrors.ErrDatabase, commitErr)
			return txErr
		}
		loggerCtx.Info("Bulk upsert agents successful", zap.Int("agents_processed", len(validAgents)), zap.Int64("rows_affected", result.RowsAffected))
		return nil
	}

	commitPolicy := newRetryPolicy(ctx, bulkCommitRetryMaxElapsedTime) // Use potentially longer timeout for bulk
	commitErr := retryableOperation(ctx, commitPolicy, "BulkUpsertAgents Commit", operation)
	if commitErr != nil {
		loggerCtx.Error("Failed to bulk upsert agents after retries", zap.Error(commitErr))
		return commitErr // Already wrapped
	}
	return nil
}

// Note: DeleteAgent operation is not defined in the interface/PRD, omitting implementation.
