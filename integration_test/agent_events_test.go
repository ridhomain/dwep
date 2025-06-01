package integration_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"gitlab.com/timkado/api/daisi-wa-events-processor/pkg/logger"
	"go.uber.org/zap/zaptest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/model"
)

// AgentEventsTestSuite inherits from E2EIntegrationTestSuite
// to get access to DB, NATS, Tenant info, and helper methods.
type AgentEventsTestSuite struct {
	E2EIntegrationTestSuite // Embed the base suite
}

// TestAgentEventsSuite runs the AgentEvents test suite
func TestAgentEventsSuite(t *testing.T) {
	suite.Run(t, new(AgentEventsTestSuite))
}

// SetupTest runs before each test in this suite.
func (s *AgentEventsTestSuite) SetupTest() {
	logger.Log = zaptest.NewLogger(s.T()).Named("AgentEventsTestSuite")
	s.E2EIntegrationTestSuite.SetupTest() // Run base setup (DB truncation etc.)
	// Add any specific setup for agent event tests if needed
	t := s.T()
	t.Log("Waiting for daisi-wa-events-processor before agent event test...")
	time.Sleep(500 * time.Millisecond) // Shorter delay, adjust if needed
}

// TestAgentUpsertEvent tests the creation of an agent via NATS event.
func (s *AgentEventsTestSuite) TestAgentUpsertEvent() {
	t := s.T()
	tenantID := s.CompanyID // Access from embedded suite
	ctx := s.Ctx            // Use suite context

	// 1. Generate Agent Payload for Insert
	agentID := fmt.Sprintf("test-agent-%s", generateUUID())
	initialPayload := model.UpsertAgentPayload{
		AgentID:   agentID,
		QRCode:    "initial-qr-code",
		Status:    "disconnected",
		AgentName: "Test Agent Initial",
		HostName:  "host-initial",
		Version:   "v1.0.0",
		CompanyID: tenantID, // Ensure correct tenant
	}

	// 2. Marshal and Validate
	payloadBytes, err := json.Marshal(initialPayload)
	require.NoError(t, err, "Failed to marshal initial agent payload")

	// 3. Publish Event
	subject := fmt.Sprintf("%s.%s", model.V1Agents, tenantID)
	err = s.PublishEvent(ctx, subject, payloadBytes) // Pass suite context
	require.NoError(t, err, "Failed to publish agent upsert event")

	// 4. Wait for processing
	time.Sleep(2 * time.Second) // Adjust sleep time as needed

	// 5. Verify Database State (Insert)
	query := fmt.Sprintf("SELECT COUNT(*) FROM %q.agents WHERE agent_id = $1 AND company_id = $2 AND qr_code = $3 AND status = $4 AND agent_name = $5 AND host_name = $6 AND version = $7", s.CompanySchemaName)
	agentExists, err := s.VerifyPostgresData(ctx, query, 1, // Expect 1 row
		agentID, tenantID, initialPayload.QRCode, initialPayload.Status,
		initialPayload.AgentName, initialPayload.HostName, initialPayload.Version)
	require.NoError(t, err, "Failed to verify inserted agent data")
	assert.True(t, agentExists, "Agent record should exist in the database with correct data after upsert event")
}

// TestAgentUpdateEvent tests updating an existing agent via NATS event.
func (s *AgentEventsTestSuite) TestAgentUpdateEvent() {
	t := s.T()
	tenantID := s.CompanyID // Access from embedded suite
	ctx := s.Ctx            // Use suite context

	// 1. Initial Insert
	agentID := fmt.Sprintf("test-agent-update-%s", generateUUID())
	initialPayload := model.UpsertAgentPayload{
		AgentID:   agentID,
		QRCode:    "update-qr-initial",
		Status:    "connecting",
		AgentName: "Test Agent Update Initial",
		HostName:  "host-update-initial",
		Version:   "v1.1.0",
		CompanyID: tenantID,
	}
	payloadBytes, err := json.Marshal(initialPayload)
	require.NoError(t, err)
	subject := fmt.Sprintf("%s.%s", model.V1Agents, tenantID)
	err = s.PublishEvent(ctx, subject, payloadBytes) // Pass suite context
	require.NoError(t, err, "Failed to publish initial agent insert event")
	time.Sleep(1 * time.Second) // Wait for initial insert

	// Verify initial insert first to make sure it worked
	initialCheckQuery := fmt.Sprintf("SELECT COUNT(*) FROM %q.agents WHERE agent_id = $1 AND company_id = $2", s.CompanySchemaName)
	initialExists, err := s.VerifyPostgresData(ctx, initialCheckQuery, 1, agentID, tenantID)
	require.NoError(t, err, "Failed to verify initial agent insert before update")
	require.True(t, initialExists, "Initial agent record must exist before update test")

	// --- Fetch initial agent using the new helper ---
	initialAgent, err := s.GetAgentByAgentID(ctx, agentID, s.CompanySchemaName)
	require.NoError(t, err, "Failed to query initial agent for update test timestamps")
	require.NotNil(t, initialAgent, "Initial agent should not be nil when fetched by helper")
	initialCreatedAt := initialAgent.CreatedAt
	initialUpdatedAt := initialAgent.UpdatedAt
	// -------------------------------------------------

	// 2. Generate Agent Payload for Update
	updatePayload := model.UpsertAgentPayload{
		AgentID:   agentID, // Same AgentID
		QRCode:    "update-qr-final",
		Status:    "connected",               // Changed status
		AgentName: "Test Agent Update Final", // Changed name
		HostName:  "host-update-final",       // Changed host
		Version:   "v1.2.0",                  // Changed version
		CompanyID: tenantID,
	}

	// 3. Marshal and Validate Update Payload
	updatePayloadBytes, err := json.Marshal(updatePayload)
	require.NoError(t, err, "Failed to marshal update agent payload")

	// 4. Publish Update Event
	err = s.PublishEvent(ctx, subject, updatePayloadBytes) // Pass suite context
	require.NoError(t, err, "Failed to publish agent update event")

	// 5. Wait for processing
	time.Sleep(2 * time.Second) // Adjust sleep time as needed

	// 6. Verify Database State (Update)
	// Use VerifyPostgresData for checking the updated state
	updateQuery := fmt.Sprintf("SELECT COUNT(*) FROM %q.agents WHERE agent_id = $1 AND company_id = $2 AND qr_code = $3 AND status = $4 AND agent_name = $5 AND host_name = $6 AND version = $7", s.CompanySchemaName)
	updatedAgentExists, err := s.VerifyPostgresData(ctx, updateQuery, 1, // Expect 1 row
		agentID, tenantID, updatePayload.QRCode, updatePayload.Status,
		updatePayload.AgentName, updatePayload.HostName, updatePayload.Version)
	require.NoError(t, err, "Failed to verify updated agent data")
	assert.True(t, updatedAgentExists, "Agent record should exist with updated data")

	// --- Check timestamps using the new helper again ---
	updatedAgent, err := s.GetAgentByAgentID(ctx, agentID, s.CompanySchemaName)
	require.NoError(t, err, "Failed to query updated agent for timestamp check")
	require.NotNil(t, updatedAgent, "Updated agent should not be nil when fetched by helper")

	assert.Equal(t, initialCreatedAt.UTC().Truncate(time.Second), updatedAgent.CreatedAt.UTC().Truncate(time.Second), "CreatedAt should not change on update")
	assert.True(t, updatedAgent.UpdatedAt.After(initialUpdatedAt), "UpdatedAt should have been updated")
	// ------------------------------------------------------
}

// TestAgentEventDifferentTenant verifies that an event published for one tenant
// does not affect the database schema of another tenant.
func (s *AgentEventsTestSuite) TestAgentEventDifferentTenant() {
	t := s.T()
	primaryTenantID := s.CompanyID                         // The tenant the suite is configured for
	otherTenantID := "other-test-tenant-" + generateUUID() // A different tenant ID
	ctx := s.Ctx

	t.Logf("Primary Tenant: %s, Other Tenant: %s", primaryTenantID, otherTenantID)

	// 1. Generate Agent Payload for the *other* tenant
	agentID := fmt.Sprintf("test-agent-isolation-%s", generateUUID())
	otherTenantPayload := model.UpsertAgentPayload{
		AgentID:   agentID,
		QRCode:    "isolation-qr",
		Status:    "pending",
		AgentName: "Isolation Test Agent",
		HostName:  "host-isolation",
		Version:   "v1.0.0",
		CompanyID: otherTenantID, // <-- Use the different tenant ID here
	}

	// 2. Marshal and Validate
	payloadBytes, err := json.Marshal(otherTenantPayload)
	require.NoError(t, err, "Failed to marshal other tenant agent payload")

	// 3. Publish Event to the *other* tenant's subject
	subject := fmt.Sprintf("%s.%s", model.V1Agents, otherTenantID)
	err = s.PublishEvent(ctx, subject, payloadBytes) // Publish to other tenant subject
	require.NoError(t, err, "Failed to publish agent event for other tenant")

	// 4. Wait for processing
	time.Sleep(2 * time.Second) // Adjust sleep time as needed

	// 5. Verify Database State in the *primary* tenant's schema
	// We expect NO record for this agentID in the primary tenant's schema.
	// The VerifyPostgresData helper automatically uses s.CompanyID (primaryTenantID)
	query := fmt.Sprintf("SELECT COUNT(*) FROM %q.agents WHERE agent_id = $1", s.CompanySchemaName)
	// NOTE: We only query by agent_id because company_id check is implicit in the helper using s.CompanyID
	agentExistsInPrimaryTenant, err := s.VerifyPostgresData(ctx, query, 0, agentID) // Expect 0 rows
	require.NoError(t, err, "Failed to query primary tenant schema for other tenant's agent")
	assert.True(t, agentExistsInPrimaryTenant, "Agent record for other tenant should NOT exist in primary tenant's schema")

	t.Log("Agent tenant isolation test completed successfully")
}

// TestAgentUpsertEvent_OptionalFieldsNull tests upserting an agent where optional fields are empty/null.
func (s *AgentEventsTestSuite) TestAgentUpsertEvent_OptionalFieldsNull() {
	tenantID := s.CompanyID
	ctx := s.Ctx
	agentID := fmt.Sprintf("test-agent-nulls-%s", generateUUID())

	// Define overrides for optional fields
	overrides := map[string]interface{}{
		"agent_id":   agentID,
		"qr_code":    "",        // Empty optional string
		"status":     "pending", // Required field
		"agent_name": "",        // Empty optional string
		"host_name":  "",        // Empty optional string
		"version":    "",        // Empty optional string
		"company_id": tenantID,
	}

	// Generate and publish
	subject := fmt.Sprintf("%s.%s", model.V1Agents, tenantID)
	payloadBytes, err := s.GenerateNatsPayload(subject, overrides)
	s.Require().NoError(err, "Failed to generate agent payload with nulls/empties")

	err = s.PublishEvent(ctx, subject, payloadBytes)
	s.Require().NoError(err, "Failed to publish agent event with nulls/empties")

	s.T().Log("Publish agent event with nulls/empties completed")

	// Wait for processing and verify agent existence
	var agent *model.Agent // Declare agent here to be populated by Eventually

	s.Require().Eventually(func() bool {
		fetchedAgent, fetchErr := s.GetAgentByAgentID(ctx, agentID, s.CompanySchemaName)
		if fetchErr != nil {
			s.T().Logf("Eventually: GetAgentByAgentID for %s failed with error: %v. Retrying.", agentID, fetchErr)
			return false // Error occurred, retry
		}
		if fetchedAgent == nil {
			s.T().Logf("Eventually: Agent %s not found yet. Retrying.", agentID)
			return false // Agent not found, retry
		}
		s.T().Logf("Eventually: Agent %s found.", agentID)
		agent = fetchedAgent // Assign to outer scope variable on success
		return true          // Agent found and no error
	}, 10*time.Second, 500*time.Millisecond, "Agent with ID '%s' should appear in DB and be fetched without error", agentID)

	s.Assert().Equal(agentID, agent.AgentID)
	s.Assert().Equal(tenantID, agent.CompanyID)
	s.Assert().Equal("pending", agent.Status, "Status should be the required value provided")
	// Check plain string fields for expected empty value when DB is NULL/empty
	s.Assert().Equal("", agent.QRCode, "Optional QRCode should be empty string")
	s.Assert().Equal("", agent.AgentName, "Optional AgentName should be empty string for null")
	s.Assert().Equal("", agent.HostName, "Optional HostName should be empty string for null")
	s.Assert().Equal("", agent.Version, "Optional Version should be empty string")
}

// GetAgentByAgentID fetches a single agent record based on the external agent ID and tenant.
func (s *AgentEventsTestSuite) GetAgentByAgentID(ctx context.Context, agentID, schema string) (*model.Agent, error) {
	db, err := s.connectDB(s.PostgresDSN)
	if err != nil {
		return nil, fmt.Errorf("GetAgentByAgentID connect: %w", err)
	}
	defer db.Close()

	ctxQuery, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	var agent model.Agent
	// Use QueryRowContext and Scan directly
	query := fmt.Sprintf("SELECT id, agent_id, qr_code, status, agent_name, host_name, version, company_id, created_at, updated_at, last_metadata FROM %q.agents WHERE agent_id = $1 AND company_id = $2", schema)
	err = db.QueryRowContext(ctxQuery, query, agentID, s.CompanyID).Scan(
		&agent.ID,
		&agent.AgentID,
		&agent.QRCode,    // Assuming nullable string maps okay, otherwise use sql.NullString
		&agent.Status,    // Assuming nullable string maps okay
		&agent.AgentName, // Assuming nullable string maps okay
		&agent.HostName,  // Assuming nullable string maps okay
		&agent.Version,   // Assuming nullable string maps okay
		&agent.CompanyID,
		&agent.CreatedAt,
		&agent.UpdatedAt,
		&agent.LastMetadata, // Assuming JSONB maps okay
	)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// Return nil agent and nil error for not found, or wrap ErrNotFound? Returning nil, nil is common.
			return nil, nil
		}
		return nil, fmt.Errorf("GetAgentByAgentID query/scan failed in %s: %w. Query: %s, Args: [%s, %s]", schema, err, query, agentID, s.CompanyID)
	}
	return &agent, nil
}
