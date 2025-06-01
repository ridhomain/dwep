package storage

import (
	"context"
	"database/sql/driver"
	"testing"
	"time"

	apperrors "gitlab.com/timkado/api/daisi-wa-events-processor/internal/apperrors"
	"gitlab.com/timkado/api/daisi-wa-events-processor/pkg/logger"
	"go.uber.org/zap/zaptest"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"gorm.io/datatypes"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	gormLogger "gorm.io/gorm/logger"

	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/model"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/tenant"
	"gitlab.com/timkado/api/daisi-wa-events-processor/pkg/utils"
)

const (
	testTenantIDAgent = "tenant-agent-test-456"
)

// Helper to create a mock DB and PostgresRepo instance for testing
func newTestAgentRepo(t *testing.T) (*PostgresRepo, sqlmock.Sqlmock) {
	logger.Log = zaptest.NewLogger(t).Named("test")

	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	assert.NoError(t, err)

	gormDB, err := gorm.Open(postgres.New(postgres.Config{
		Conn: db,
	}), &gorm.Config{
		Logger:                 gormLogger.Default.LogMode(gormLogger.Silent),
		SkipDefaultTransaction: true,
	})
	assert.NoError(t, err)

	repo := &PostgresRepo{db: gormDB}
	return repo, mock
}

func newTestAgentRepoWithMatcher(t *testing.T, matcher sqlmock.QueryMatcher) (*PostgresRepo, sqlmock.Sqlmock) {
	logger.Log = zaptest.NewLogger(t).Named("test")

	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(matcher))
	assert.NoError(t, err)

	gormDB, err := gorm.Open(postgres.New(postgres.Config{
		Conn: db,
	}), &gorm.Config{
		Logger:                 gormLogger.Default.LogMode(gormLogger.Silent),
		SkipDefaultTransaction: true,
	})
	assert.NoError(t, err)

	repo := &PostgresRepo{db: gormDB}
	return repo, mock
}

// Helper to create context with tenant ID
func contextWithAgentTenant() context.Context {
	ctx := context.Background()
	ctx = tenant.WithCompanyID(ctx, testTenantIDAgent)
	// Add sender if needed for specific tests, though agent repo might not use it directly
	// ctx = tenant.WithSender(ctx, "test-sender")
	return ctx
}

// --- Agent Repository Tests ---

// TestPostgresRepo_SaveAgent_Insert tests saving a new agent record.
func TestPostgresRepo_SaveAgent_Insert(t *testing.T) {
	repo, mock := newTestAgentRepo(t)
	ctx := contextWithAgentTenant()

	agent := model.Agent{
		AgentID:      "agent-test-insert",
		CompanyID:    testTenantIDAgent, // Match context tenant ID
		Status:       "disconnected",
		AgentName:    "Test Insert Agent",
		LastMetadata: datatypes.JSON(utils.MustMarshalJSON(map[string]interface{}{"init": true})),
		// CreatedAt and UpdatedAt are usually handled by GORM/DB
	}

	mock.ExpectBegin()

	// Expect SELECT FOR UPDATE (will return not found)
	selectQuery := `SELECT * FROM "agents" WHERE agent_id = $1 AND company_id = $2 ORDER BY "agents"."id" LIMIT $3 FOR UPDATE`
	mock.ExpectQuery(selectQuery).
		WithArgs(agent.AgentID, agent.CompanyID, 1).
		WillReturnError(gorm.ErrRecordNotFound)

	// Expect INSERT query
	// Note: GORM v2 uses QueryRow for INSERT RETURNING. We need ExpectQuery.
	// The order of columns might vary based on struct definition, ensure it matches GORM's output.
	insertPattern := `INSERT INTO "agents" ("agent_id","qr_code","status","agent_name","host_name","version","company_id","created_at","updated_at","last_metadata") VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING "id"`
	mock.ExpectQuery(insertPattern).
		WithArgs(
			agent.AgentID, agent.QRCode, agent.Status, agent.AgentName, agent.HostName,
			agent.Version, agent.CompanyID, AnyTime{}, AnyTime{}, agent.LastMetadata,
		).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1)) // Return generated ID

	mock.ExpectCommit()

	err := repo.SaveAgent(ctx, agent)

	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestPostgresRepo_SaveAgent_Update tests updating an existing agent via SaveAgent (upsert logic).
func TestPostgresRepo_SaveAgent_Update(t *testing.T) {
	repo, mock := newTestAgentRepo(t)
	ctx := contextWithAgentTenant()
	now := time.Now()
	existingCreatedAt := now.Add(-time.Hour)
	existingID := int64(5)

	agent := model.Agent{
		AgentID:      "agent-test-upsert-update",
		CompanyID:    testTenantIDAgent,
		Status:       "connected", // Updated status
		Version:      "v1.1",      // Updated version
		HostName:     "new-host",  // Updated hostname
		LastMetadata: datatypes.JSON(`{"update": true}`),
		// ID might be 0 or existing ID, method handles it
		// CreatedAt should be preserved, UpdatedAt set by method
	}

	// Existing record data
	existingAgentCols := []string{"id", "agent_id", "company_id", "status", "version", "host_name", "created_at", "updated_at", "last_metadata"}
	existingAgentRows := sqlmock.NewRows(existingAgentCols).
		AddRow(existingID, agent.AgentID, agent.CompanyID, "disconnected", "v1.0", "old-host", existingCreatedAt, now.Add(-time.Minute), `{"init":true}`)

	mock.ExpectBegin()

	// Expect SELECT FOR UPDATE (finds existing)
	selectQuery := `SELECT * FROM "agents" WHERE agent_id = $1 AND company_id = $2 ORDER BY "agents"."id" LIMIT $3 FOR UPDATE`
	mock.ExpectQuery(selectQuery).
		WithArgs(agent.AgentID, agent.CompanyID, 1).
		WillReturnRows(existingAgentRows)

	// Expect UPDATE query
	// GORM's Updates will use the fields from the input 'agent' model.
	// It preserves CreatedAt and uses the existing ID.
	// Adjusted based on actual GORM output for Save (excludes zero-value fields like qr_code, agent_name)
	updatePattern := `UPDATE "agents" SET "id"=$1,"agent_id"=$2,"status"=$3,"host_name"=$4,"version"=$5,"company_id"=$6,"created_at"=$7,"updated_at"=$8,"last_metadata"=$9 WHERE "id" = $10`
	mock.ExpectExec(updatePattern).
		WithArgs(
			existingID,         // id ($1)
			agent.AgentID,      // agent_id ($2)
			agent.Status,       // status ($3)
			agent.HostName,     // host_name ($4)
			agent.Version,      // version ($5)
			agent.CompanyID,    // company_id ($6)
			existingCreatedAt,  // created_at ($7) (preserved)
			AnyTime{},          // updated_at ($8)
			agent.LastMetadata, // last_metadata ($9)
			existingID,         // WHERE id ($10)
		).
		WillReturnResult(sqlmock.NewResult(0, 1))

	mock.ExpectCommit()

	err := repo.SaveAgent(ctx, agent) // ID in agent might be 0, function should handle it

	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestPostgresRepo_SaveAgent_TenantMismatch tests saving an agent with a tenant mismatch.
func TestPostgresRepo_SaveAgent_TenantMismatch(t *testing.T) {
	repo, mock := newTestAgentRepo(t)
	ctx := contextWithAgentTenant()
	agent := model.Agent{AgentID: "agent-tenant-mismatch", CompanyID: "wrong-tenant"}
	err := repo.SaveAgent(ctx, agent)
	assert.Error(t, err)
	assert.ErrorIs(t, err, apperrors.ErrBadRequest) // Check for correct standard error
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestPostgresRepo_UpdateAgent_Success tests updating specific fields of an existing agent.
func TestPostgresRepo_UpdateAgent_Success(t *testing.T) {
	repo, mock := newTestAgentRepo(t)
	ctx := contextWithAgentTenant()
	now := time.Now()
	existingCreatedAt := now.Add(-time.Hour)
	existingID := int64(10)

	// Only provide fields intended for update + mandatory IDs
	agentUpdate := model.Agent{
		AgentID:   "agent-partial-update",
		CompanyID: testTenantIDAgent,
		Status:    "requires_pairing",
		AgentName: "Partially Updated Agent",
		// ID field is not strictly needed for input but helps clarity
		// CreatedAt will be preserved, UpdatedAt set by method
	}

	// Existing record data
	existingAgentCols := []string{"id", "agent_id", "company_id", "status", "agent_name", "created_at", "updated_at"}
	existingAgentRows := sqlmock.NewRows(existingAgentCols).
		AddRow(existingID, agentUpdate.AgentID, agentUpdate.CompanyID, "connected", "Original Agent", existingCreatedAt, now.Add(-time.Minute))

	mock.ExpectBegin()

	// Expect SELECT FOR UPDATE
	selectQuery := `SELECT * FROM "agents" WHERE agent_id = $1 AND company_id = $2 ORDER BY "agents"."id" LIMIT $3 FOR UPDATE`
	mock.ExpectQuery(selectQuery).
		WithArgs(agentUpdate.AgentID, agentUpdate.CompanyID, 1).
		WillReturnRows(existingAgentRows)

	// Expect UPDATE query - GORM Updates includes provided fields + updated_at
	// Match the ACTUAL query from the error log (includes id and created_at in SET)
	updatePattern := `UPDATE "agents" SET "id"=$1,"agent_id"=$2,"status"=$3,"agent_name"=$4,"company_id"=$5,"created_at"=$6,"updated_at"=$7 WHERE "id" = $8`
	mock.ExpectExec(updatePattern).
		WithArgs(
			existingID,            // $1 id (in SET)
			agentUpdate.AgentID,   // $2 agent_id
			agentUpdate.Status,    // $3 status
			agentUpdate.AgentName, // $4 agent_name
			agentUpdate.CompanyID, // $5 company_id
			existingCreatedAt,     // $6 created_at (in SET)
			AnyTime{},             // $7 updated_at
			existingID,            // $8 WHERE id
		).
		WillReturnResult(sqlmock.NewResult(0, 1))

	mock.ExpectCommit()

	err := repo.UpdateAgent(ctx, agentUpdate)

	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestPostgresRepo_UpdateAgent_NotFound tests updating a non-existent agent.
func TestPostgresRepo_UpdateAgent_NotFound(t *testing.T) {
	repo, mock := newTestAgentRepo(t)
	ctx := contextWithAgentTenant()

	agentUpdate := model.Agent{
		AgentID:   "agent-not-found-update",
		CompanyID: testTenantIDAgent,
		Status:    "connected",
	}

	mock.ExpectBegin()

	// Expect SELECT FOR UPDATE (will return not found)
	selectQuery := `SELECT * FROM "agents" WHERE agent_id = $1 AND company_id = $2 ORDER BY "agents"."id" LIMIT $3 FOR UPDATE`
	mock.ExpectQuery(selectQuery).
		WithArgs(agentUpdate.AgentID, agentUpdate.CompanyID, 1).
		WillReturnError(gorm.ErrRecordNotFound)

	mock.ExpectRollback() // Expect rollback because the defer will run after permanent error

	err := repo.UpdateAgent(ctx, agentUpdate)
	assert.Error(t, err)
	assert.ErrorIs(t, err, apperrors.ErrNotFound) // Check for ErrNotFound
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestPostgresRepo_UpdateAgent_TenantMismatch tests updating an agent with a tenant mismatch.
func TestPostgresRepo_UpdateAgent_TenantMismatch(t *testing.T) {
	repo, mock := newTestAgentRepo(t)
	ctx := contextWithAgentTenant()
	agent := model.Agent{AgentID: "agent-tenant-mismatch-update", CompanyID: "wrong-tenant"}
	err := repo.UpdateAgent(ctx, agent)
	assert.Error(t, err)
	assert.ErrorIs(t, err, apperrors.ErrBadRequest) // Check for correct standard error
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestPostgresRepo_UpdateAgentStatus_Success tests updating only the status.
func TestPostgresRepo_UpdateAgentStatus_Success(t *testing.T) {
	repo, mock := newTestAgentRepo(t)
	ctx := contextWithAgentTenant()
	agentID := "agent-status-update"
	newStatus := "disconnected"

	mock.ExpectBegin()
	// GORM uses map for Updates, order isn't guaranteed, use AnyArg or specific order
	// The exact columns might vary slightly, focus on key fields.
	updatePattern := `UPDATE "agents" SET "status"=$1,"updated_at"=$2 WHERE agent_id = $3 AND company_id = $4`
	mock.ExpectExec(updatePattern).
		WithArgs(newStatus, AnyTime{}, agentID, testTenantIDAgent).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	err := repo.UpdateAgentStatus(ctx, agentID, newStatus)
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestPostgresRepo_UpdateAgentStatus_NotFound tests updating status for non-existent agent.
func TestPostgresRepo_UpdateAgentStatus_NotFound(t *testing.T) {
	repo, mock := newTestAgentRepo(t)
	ctx := contextWithAgentTenant()
	agentID := "agent-status-not-found"
	newStatus := "disconnected"

	mock.ExpectBegin()
	updatePattern := `UPDATE "agents" SET "status"=$1,"updated_at"=$2 WHERE agent_id = $3 AND company_id = $4`
	mock.ExpectExec(updatePattern).
		WithArgs(newStatus, AnyTime{}, agentID, testTenantIDAgent).
		WillReturnResult(sqlmock.NewResult(0, 0)) // Simulate 0 rows affected
	mock.ExpectRollback() // Expect rollback because error is returned before commit

	err := repo.UpdateAgentStatus(ctx, agentID, newStatus)
	assert.Error(t, err)
	assert.ErrorIs(t, err, apperrors.ErrNotFound) // Check for ErrNotFound
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestPostgresRepo_FindAgentByID_Found tests finding an agent by internal ID.
func TestPostgresRepo_FindAgentByID_Found(t *testing.T) {
	repo, mock := newTestAgentRepo(t)
	ctx := contextWithAgentTenant()
	testID := int64(25)
	now := time.Now()

	// Expected data
	agentCols := []string{"id", "agent_id", "company_id", "status", "created_at", "updated_at"}
	agentRows := sqlmock.NewRows(agentCols).
		AddRow(testID, "agent-found-by-id", testTenantIDAgent, "connected", now.Add(-time.Hour), now.Add(-time.Minute))

	// Expect SELECT query using internal ID and company_id
	selectQuery := `SELECT * FROM "agents" WHERE id = $1 AND company_id = $2 ORDER BY "agents"."id" LIMIT $3`
	mock.ExpectQuery(selectQuery).
		WithArgs(testID, testTenantIDAgent, 1).
		WillReturnRows(agentRows)

	foundAgent, err := repo.FindAgentByID(ctx, testID)

	assert.NoError(t, err)
	assert.NotNil(t, foundAgent)
	assert.Equal(t, testID, foundAgent.ID)
	assert.Equal(t, "agent-found-by-id", foundAgent.AgentID)
	assert.Equal(t, testTenantIDAgent, foundAgent.CompanyID)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestPostgresRepo_FindAgentByID_NotFound tests finding a non-existent agent by internal ID.
func TestPostgresRepo_FindAgentByID_NotFound(t *testing.T) {
	repo, mock := newTestAgentRepo(t)
	ctx := contextWithAgentTenant()
	selectQuery := `SELECT * FROM "agents" WHERE id = $1 AND company_id = $2 ORDER BY "agents"."id" LIMIT $3`
	mock.ExpectQuery(selectQuery).WithArgs(int64(999), testTenantIDAgent, 1).WillReturnError(gorm.ErrRecordNotFound)

	found, err := repo.FindAgentByID(ctx, 999)
	// assert.NoError(t, err) // Old assertion
	assert.ErrorIs(t, err, apperrors.ErrNotFound) // New assertion
	assert.Nil(t, found)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestPostgresRepo_FindAgentByAgentID_Found tests finding an agent by agent_id.
func TestPostgresRepo_FindAgentByAgentID_Found(t *testing.T) {
	repo, mock := newTestAgentRepo(t)
	ctx := contextWithAgentTenant()
	agentID := "agent-find-me-by-agentid"
	now := time.Now()

	// Expected data
	agentCols := []string{"id", "agent_id", "company_id", "status", "created_at", "updated_at"}
	agentRows := sqlmock.NewRows(agentCols).
		AddRow(30, agentID, testTenantIDAgent, "connected", now.Add(-time.Hour), now.Add(-time.Minute))

	// Expect SELECT query using agent_id and company_id
	selectQuery := `SELECT * FROM "agents" WHERE agent_id = $1 AND company_id = $2 ORDER BY "agents"."id" LIMIT $3`
	mock.ExpectQuery(selectQuery).
		WithArgs(agentID, testTenantIDAgent, 1).
		WillReturnRows(agentRows)

	foundAgent, err := repo.FindAgentByAgentID(ctx, agentID)

	assert.NoError(t, err)
	assert.NotNil(t, foundAgent)
	assert.Equal(t, agentID, foundAgent.AgentID)
	assert.Equal(t, testTenantIDAgent, foundAgent.CompanyID)
	assert.Equal(t, int64(30), foundAgent.ID)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestPostgresRepo_FindAgentByAgentID_NotFound tests finding a non-existent agent by agent_id.
func TestPostgresRepo_FindAgentByAgentID_NotFound(t *testing.T) {
	repo, mock := newTestAgentRepo(t)
	ctx := contextWithAgentTenant()
	selectQuery := `SELECT * FROM "agents" WHERE agent_id = $1 AND company_id = $2 ORDER BY "agents"."id" LIMIT $3`
	mock.ExpectQuery(selectQuery).WithArgs("agent-id-404", testTenantIDAgent, 1).WillReturnError(gorm.ErrRecordNotFound)

	found, err := repo.FindAgentByAgentID(ctx, "agent-id-404")
	// assert.NoError(t, err) // Old assertion
	assert.ErrorIs(t, err, apperrors.ErrNotFound) // New assertion
	assert.Nil(t, found)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestPostgresRepo_FindAgentsByStatus_Found tests finding agents by status.
func TestPostgresRepo_FindAgentsByStatus_Found(t *testing.T) {
	repo, mock := newTestAgentRepo(t)
	ctx := contextWithAgentTenant()
	statusToFind := "connected"
	now := time.Now()

	// Expected data
	agentCols := []string{"id", "agent_id", "company_id", "status", "created_at", "updated_at"}
	agentRows := sqlmock.NewRows(agentCols).
		AddRow(40, "agent-conn-1", testTenantIDAgent, statusToFind, now.Add(-2*time.Hour), now.Add(-10*time.Minute)).
		AddRow(41, "agent-conn-2", testTenantIDAgent, statusToFind, now.Add(-3*time.Hour), now.Add(-5*time.Minute))

	// Expect SELECT query using status and company_id
	selectQuery := `SELECT * FROM "agents" WHERE status = $1 AND company_id = $2`
	mock.ExpectQuery(selectQuery).
		WithArgs(statusToFind, testTenantIDAgent).
		WillReturnRows(agentRows)

	foundAgents, err := repo.FindAgentsByStatus(ctx, statusToFind)

	assert.NoError(t, err)
	assert.NotNil(t, foundAgents)
	assert.Len(t, foundAgents, 2)
	assert.Equal(t, int64(40), foundAgents[0].ID)
	assert.Equal(t, "agent-conn-1", foundAgents[0].AgentID)
	assert.Equal(t, statusToFind, foundAgents[0].Status)
	assert.Equal(t, int64(41), foundAgents[1].ID)
	assert.Equal(t, "agent-conn-2", foundAgents[1].AgentID)
	assert.Equal(t, statusToFind, foundAgents[1].Status)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestPostgresRepo_FindAgentsByStatus_NotFound tests finding no agents by status.
func TestPostgresRepo_FindAgentsByStatus_NotFound(t *testing.T) {
	repo, mock := newTestAgentRepo(t)
	ctx := contextWithAgentTenant()
	statusToFind := "archived"

	// Expect SELECT query
	selectQuery := `SELECT * FROM "agents" WHERE status = $1 AND company_id = $2`
	mock.ExpectQuery(selectQuery).
		WithArgs(statusToFind, testTenantIDAgent).
		WillReturnRows(sqlmock.NewRows([]string{"id", "agent_id", "company_id", "status"})) // Return empty rows

	foundAgents, err := repo.FindAgentsByStatus(ctx, statusToFind)

	// Expect empty slice, nil error
	assert.NoError(t, err)
	assert.NotNil(t, foundAgents)
	assert.Len(t, foundAgents, 0)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestPostgresRepo_FindAgentsByCompanyID_Found tests finding agents by company ID.
func TestPostgresRepo_FindAgentsByCompanyID_Found(t *testing.T) {
	repo, mock := newTestAgentRepo(t)
	ctx := contextWithAgentTenant() // Context already has the tenant ID
	companyIDToFind := testTenantIDAgent
	now := time.Now()

	// Expected data
	agentCols := []string{"id", "agent_id", "company_id", "status", "created_at", "updated_at"}
	agentRows := sqlmock.NewRows(agentCols).
		AddRow(50, "agent-comp-1", companyIDToFind, "connected", now.Add(-time.Hour), now.Add(-1*time.Minute)).
		AddRow(51, "agent-comp-2", companyIDToFind, "disconnected", now.Add(-2*time.Hour), now.Add(-2*time.Minute))

	// Expect SELECT query - Use the single WHERE condition
	selectQuery := `SELECT * FROM "agents" WHERE company_id = $1`
	mock.ExpectQuery(selectQuery).
		WithArgs(companyIDToFind). // Only one argument now
		WillReturnRows(agentRows)

	foundAgents, err := repo.FindAgentsByCompanyID(ctx, companyIDToFind)

	assert.NoError(t, err)
	assert.NotNil(t, foundAgents)
	assert.Len(t, foundAgents, 2)
	assert.Equal(t, int64(50), foundAgents[0].ID)
	assert.Equal(t, "agent-comp-1", foundAgents[0].AgentID)
	assert.Equal(t, companyIDToFind, foundAgents[0].CompanyID)
	assert.Equal(t, int64(51), foundAgents[1].ID)
	assert.Equal(t, "agent-comp-2", foundAgents[1].AgentID)
	assert.Equal(t, companyIDToFind, foundAgents[1].CompanyID)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestPostgresRepo_FindAgentsByCompanyID_TenantMismatch tests finding agents by company ID with a tenant mismatch.
func TestPostgresRepo_FindAgentsByCompanyID_TenantMismatch(t *testing.T) {
	repo, _ := newTestAgentRepo(t)                                       // Mock not needed as it errors before DB query
	ctx := tenant.WithCompanyID(context.Background(), "actual_tenant_A") // Context tenant
	_, err := repo.FindAgentsByCompanyID(ctx, "requested_tenant_B")      // Requested tenant mismatch
	assert.Error(t, err)
	assert.ErrorIs(t, err, apperrors.ErrUnauthorized)
}

// TestPostgresRepo_FindAgentsByCompanyID_NotFound tests finding no agents by company ID.
func TestPostgresRepo_FindAgentsByCompanyID_NotFound(t *testing.T) {
	repo, mock := newTestAgentRepo(t)
	ctx := contextWithAgentTenant()
	companyIDToFind := testTenantIDAgent

	// Expect SELECT query - Use the single WHERE condition
	selectQuery := `SELECT * FROM "agents" WHERE company_id = $1`
	mock.ExpectQuery(selectQuery).
		WithArgs(companyIDToFind).                                                // Only one argument now
		WillReturnRows(sqlmock.NewRows([]string{"id", "agent_id", "company_id"})) // Empty rows

	foundAgents, err := repo.FindAgentsByCompanyID(ctx, companyIDToFind)

	// Expect empty slice, nil error
	assert.NoError(t, err)
	assert.NotNil(t, foundAgents)
	assert.Len(t, foundAgents, 0)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestPostgresRepo_BulkUpsertAgents_Success tests bulk upserting agents.
func TestPostgresRepo_BulkUpsertAgents_Success(t *testing.T) {
	// Use default QueryMatcherEqual by removing QueryMatcherRegex option
	repo, mock := newTestAgentRepoWithMatcher(t, sqlmock.QueryMatcherEqual)
	ctx := contextWithAgentTenant()
	now := time.Now()

	agents := []model.Agent{
		{AgentID: "bulk-agent-1", Status: "new", CompanyID: testTenantIDAgent, UpdatedAt: now, Version: "1.0"},
		{AgentID: "bulk-agent-2", Status: "new", CompanyID: testTenantIDAgent, UpdatedAt: now, AgentName: "Bulk Agent 2"},
		// Duplicate agent_id, should update existing fields and keep non-conflicting ones
		{AgentID: "bulk-agent-1", Status: "connected", CompanyID: testTenantIDAgent, UpdatedAt: now, Version: "1.1"},
	}

	// Filter out agents with mismatched CompanyID (done in the function, simulate here for arg matching)
	validAgents := []model.Agent{
		agents[0],
		agents[1],
		agents[2],
	}

	mock.ExpectBegin()

	// Use the exact SQL string from the latest error message
	exactUpsertSQL := `INSERT INTO "agents" ("agent_id","qr_code","status","agent_name","host_name","version","company_id","created_at","updated_at","last_metadata") VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,NULL),($10,$11,$12,$13,$14,$15,$16,$17,$18,NULL),($19,$20,$21,$22,$23,$24,$25,$26,$27,NULL) ON CONFLICT ("agent_id","company_id") DO UPDATE SET "qr_code"="excluded"."qr_code","status"="excluded"."status","agent_name"="excluded"."agent_name","host_name"="excluded"."host_name","version"="excluded"."version","updated_at"="excluded"."updated_at" RETURNING "id"`

	// Build expected args based on validAgents - match the actual query structure (27 args, no last_metadata)
	expectedArgs := []driver.Value{
		// agent 1 (new) - 9 args
		"bulk-agent-1", "", "new", "", "", "1.0", testTenantIDAgent, AnyTime{}, AnyTime{},
		// agent 2 (new) - 9 args
		"bulk-agent-2", "", "new", "Bulk Agent 2", "", "", testTenantIDAgent, AnyTime{}, AnyTime{},
		// agent 1 (update) - 9 args
		"bulk-agent-1", "", "connected", "", "", "1.1", testTenantIDAgent, AnyTime{}, AnyTime{},
	}

	// Define expected returning IDs (mocked)
	expectedIDs := make([]int64, len(validAgents))
	returningRows := sqlmock.NewRows([]string{"id"})
	for i := range validAgents {
		// Mock returning IDs (e.g., starting from 100)
		mockedID := int64(100 + i)
		expectedIDs[i] = mockedID
		returningRows.AddRow(mockedID)
	}

	// Expect the exact query
	mock.ExpectQuery(exactUpsertSQL).
		WithArgs(expectedArgs...).
		WillReturnRows(returningRows) // Return mocked IDs

	mock.ExpectCommit()

	err := repo.BulkUpsertAgents(ctx, agents) // Pass original list

	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestPostgresRepo_BulkUpsertAgents_SkipMismatchedTenant tests that agents with wrong company ID are skipped.
func TestPostgresRepo_BulkUpsertAgents_SkipMismatchedTenant(t *testing.T) {
	// Use default QueryMatcherEqual by removing QueryMatcherRegex option
	repo, mock := newTestAgentRepoWithMatcher(t, sqlmock.QueryMatcherEqual)
	ctx := contextWithAgentTenant()
	now := time.Now()

	agents := []model.Agent{
		{AgentID: "bulk-agent-ok-1", Status: "new", CompanyID: testTenantIDAgent, UpdatedAt: now, Version: "1.0"},
		{AgentID: "bulk-agent-wrong", Status: "new", CompanyID: "wrong-tenant-id", UpdatedAt: now, AgentName: "Wrong Company"},
		{AgentID: "bulk-agent-ok-2", Status: "connected", CompanyID: testTenantIDAgent, UpdatedAt: now, Version: "1.1"},
	}

	// Only the agents with the correct CompanyID should be processed
	validAgents := []model.Agent{
		agents[0],
		agents[2],
	}

	mock.ExpectBegin()

	// Use the exact SQL string generated for the TWO valid agents (from the error logs)
	exactUpsertSQL := `INSERT INTO "agents" ("agent_id","qr_code","status","agent_name","host_name","version","company_id","created_at","updated_at","last_metadata") VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,NULL),($10,$11,$12,$13,$14,$15,$16,$17,$18,NULL) ON CONFLICT ("agent_id","company_id") DO UPDATE SET "qr_code"="excluded"."qr_code","status"="excluded"."status","agent_name"="excluded"."agent_name","host_name"="excluded"."host_name","version"="excluded"."version","updated_at"="excluded"."updated_at" RETURNING "id"`

	// Build expected args based only on validAgents - match the actual query structure (18 args)
	expectedArgs := []driver.Value{
		// agent ok 1 - 9 args
		"bulk-agent-ok-1", "", "new", "", "", "1.0", testTenantIDAgent, AnyTime{}, AnyTime{},
		// agent ok 2 - 9 args
		"bulk-agent-ok-2", "", "connected", "", "", "1.1", testTenantIDAgent, AnyTime{}, AnyTime{},
	}

	// Define expected returning IDs (mocked)
	expectedIDs := make([]int64, len(validAgents))
	returningRows := sqlmock.NewRows([]string{"id"})
	for i := range validAgents {
		// Mock returning IDs (e.g., starting from 200)
		mockedID := int64(200 + i)
		expectedIDs[i] = mockedID
		returningRows.AddRow(mockedID)
	}

	// Expect the exact query for the valid agents
	mock.ExpectQuery(exactUpsertSQL).
		WithArgs(expectedArgs...).
		WillReturnRows(returningRows) // Return mocked IDs

	mock.ExpectCommit()

	err := repo.BulkUpsertAgents(ctx, agents) // Pass original list

	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestPostgresRepo_BulkUpsertAgents_EmptyList tests bulk upserting with an empty list.
func TestPostgresRepo_BulkUpsertAgents_EmptyList(t *testing.T) {
	repo, mock := newTestAgentRepo(t)
	ctx := contextWithAgentTenant()

	var agents []model.Agent // Empty list

	// No DB calls should be expected
	err := repo.BulkUpsertAgents(ctx, agents)

	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet()) // Ensures no Begin/Commit/Exec was called
}
