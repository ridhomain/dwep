package integration_test

import (
	"fmt"
	"testing"

	"gitlab.com/timkado/api/daisi-wa-events-processor/pkg/logger"
	"go.uber.org/zap/zaptest"

	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib" // PostgreSQL driver
	"github.com/stretchr/testify/suite"

	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/model"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/storage"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/tenant"
)

// AgentRepoTestSuite defines the suite for Agent repository tests
// It embeds BaseIntegrationSuite to get DB/NATS but not the app container.
type AgentRepoTestSuite struct {
	BaseIntegrationSuite
	repo             *storage.PostgresRepo // Repo instance for the suite's default CompanyID
	TenantSchemaName string
}

// SetupTest runs before each test in this suite.
// It initializes the repository for the suite's CompanyID and truncates the table.
func (s *AgentRepoTestSuite) SetupTest() {
	logger.Log = zaptest.NewLogger(s.T()).Named("AgentRepoTestSuite")
	// Use the DSN and CompanyID from the embedded BaseIntegrationSuite
	repo, err := storage.NewPostgresRepo(s.PostgresDSN, true, s.CompanyID)
	s.Require().NoError(err, "SetupTest: Failed to create repo for default tenant")
	s.Require().NotNil(repo, "SetupTest: Repo should not be nil")
	s.repo = repo

	// Clean the agents table before each test using connectDB
	tenantSchema := fmt.Sprintf("daisi_%s", s.CompanyID)
	db, err := connectDB(s.PostgresDSN)
	s.Require().NoError(err, "Failed to connect to tenant DB for cleanup")
	defer db.Close()
	s.TenantSchemaName = tenantSchema

	s.BaseIntegrationSuite.SetupTest()
}

// TearDownTest runs after each test in this suite.
func (s *AgentRepoTestSuite) TearDownTest() {
	if s.repo != nil {
		// Use the suite's context
		s.repo.Close(s.Ctx) // Close the connection used in the test
	}
}

// TestRunner runs the test suite
func TestAgentRepoSuite(t *testing.T) {
	suite.Run(t, new(AgentRepoTestSuite))
}

// --- Test Cases ---

func (s *AgentRepoTestSuite) TestSaveAgent() {
	// Use suite context and company ID
	ctx := tenant.WithCompanyID(s.Ctx, s.CompanyID)

	// 1. Create a new agent using the fixture generator
	agentID := "test-agent-" + uuid.New().String()
	overrides := &model.Agent{
		AgentID:   agentID,
		CompanyID: s.CompanyID,
		AgentName: "Initial Agent Name",
		Status:    "disconnected",
	}
	agentInterface, err := generateModelStruct("Agent", overrides)
	s.Require().NoError(err, "Failed to generate agent model")
	agent := agentInterface.(*model.Agent) // Type assertion

	// 2. Save the agent (create)
	err = s.repo.SaveAgent(ctx, *agent) // Use SaveAgent
	s.Require().NoError(err, "SaveAgent (create) failed")

	// 3. Verify creation using direct query via connectDB
	db, err := connectDB(s.PostgresDSN)
	s.Require().NoError(err, "Failed to connect to tenant DB for verification")
	defer db.Close()

	var retrievedAgent model.Agent
	query := fmt.Sprintf(`SELECT agent_id, company_id, agent_name, status FROM %q.agents WHERE agent_id = $1 AND company_id = $2`, s.TenantSchemaName)
	err = db.QueryRow(query, agent.AgentID, s.CompanyID).
		Scan(&retrievedAgent.AgentID, &retrievedAgent.CompanyID, &retrievedAgent.AgentName, &retrievedAgent.Status)
	s.Require().NoError(err, "Direct query failed for created agent")
	s.Require().Equal(agent.AgentID, retrievedAgent.AgentID)
	s.Require().Equal("Initial Agent Name", retrievedAgent.AgentName)
	s.Require().Equal("disconnected", retrievedAgent.Status)

	// 4. Update the agent
	agent.AgentName = "Updated Agent Name"
	agent.Status = "connected"
	agent.Version = "1.1.0"

	err = s.repo.SaveAgent(ctx, *agent) // Save again (update)
	s.Require().NoError(err, "SaveAgent (update) failed")

	// 5. Verify the update using direct query
	var updatedAgent model.Agent
	queryUpdated := fmt.Sprintf(`SELECT agent_id, agent_name, status, version FROM %q.agents WHERE agent_id = $1 AND company_id = $2`, s.TenantSchemaName)
	err = db.QueryRow(queryUpdated, agent.AgentID, s.CompanyID).
		Scan(&updatedAgent.AgentID, &updatedAgent.AgentName, &updatedAgent.Status, &updatedAgent.Version)
	s.Require().NoError(err, "Direct query failed for updated agent")
	s.Require().Equal(agent.AgentID, updatedAgent.AgentID)
	s.Require().Equal("Updated Agent Name", updatedAgent.AgentName)
	s.Require().Equal("connected", updatedAgent.Status)
	s.Require().Equal("1.1.0", updatedAgent.Version)
}

// TestAgentTenantIsolation verifies that operations are isolated to the correct tenant.
func (s *AgentRepoTestSuite) TestAgentTenantIsolation() {
	baseCtx := s.Ctx // Use suite context as base

	// Tenant A (uses the suite's default CompanyID and repo)
	tenantA_ID := s.CompanyID
	ctxA := tenant.WithCompanyID(baseCtx, tenantA_ID)
	agentAInterface, err := generateModelStruct("Agent", &model.Agent{
		AgentID:   "tenant-iso-agent-A-" + uuid.New().String(),
		CompanyID: tenantA_ID,
		AgentName: "Agent A",
	})
	s.Require().NoError(err)
	agentA := agentAInterface.(*model.Agent)

	// Tenant B (Create a new repo instance for a different tenant ID)
	tenantB_ID := "tenant_b_" + uuid.New().String()
	schemaB := fmt.Sprintf("daisi_%s", tenantB_ID)
	ctxB := tenant.WithCompanyID(baseCtx, tenantB_ID)

	// Create a new schema for Tenant B
	// Use the suite's PostgresDSN (host accessible) to create the repo for tenant B
	repoB, err := storage.NewPostgresRepo(s.PostgresDSN, true, tenantB_ID)
	s.Require().NoError(err, "Failed to create repo for Tenant B")
	s.Require().NotNil(repoB)
	defer repoB.Close(baseCtx)

	agentBInterface, err := generateModelStruct("Agent", &model.Agent{
		AgentID:   "tenant-iso-agent-B-" + uuid.New().String(),
		CompanyID: tenantB_ID,
		AgentName: "Agent B",
	})
	s.Require().NoError(err)
	agentB := agentBInterface.(*model.Agent)

	// 1. Save Agent A using Repo A (s.repo)
	err = s.repo.SaveAgent(ctxA, *agentA)
	s.Require().NoError(err, "Failed to save Agent A")

	// 2. Save Agent B using Repo B
	err = repoB.SaveAgent(ctxB, *agentB)
	s.Require().NoError(err, "Failed to save Agent B")

	// 3. Verify Agent A exists using direct connection to Schema A
	dbA, err := connectDB(s.PostgresDSN)
	s.Require().NoError(err, "Failed to connect to DB A")
	defer dbA.Close()
	var countA_dbA int
	queryA_dbA := fmt.Sprintf(`SELECT COUNT(*) FROM %q.agents WHERE agent_id = $1 AND company_id = $2`, s.TenantSchemaName)
	err = dbA.QueryRow(queryA_dbA, agentA.AgentID, tenantA_ID).Scan(&countA_dbA)
	s.Require().NoError(err, "Query A via DB A failed")
	s.Require().Equal(1, countA_dbA, "Agent A should exist in Tenant A schema via DB A")

	// 4. Verify Agent B exists using direct connection to Schema B
	dbB, err := connectDB(s.PostgresDSN)
	s.Require().NoError(err, "Failed to connect to DB B")
	defer dbB.Close()
	var countB_dbB int
	queryB_dbB := fmt.Sprintf(`SELECT COUNT(*) FROM %q.agents WHERE agent_id = $1 AND company_id = $2`, schemaB)
	err = dbB.QueryRow(queryB_dbB, agentB.AgentID, tenantB_ID).Scan(&countB_dbB)
	s.Require().NoError(err, "Query B via DB B failed")
	s.Require().Equal(1, countB_dbB, "Agent B should exist in Tenant B schema via DB B")

	// 5. Verify Agent B does NOT exist using direct connection to Schema A
	var countB_dbA int
	queryB_dbA := fmt.Sprintf(`SELECT COUNT(*) FROM %q.agents WHERE agent_id = $1`, s.TenantSchemaName)
	err = dbA.QueryRow(queryB_dbA, agentB.AgentID).Scan(&countB_dbA)
	s.Require().NoError(err, "Query B via DB A failed")
	s.Require().Equal(0, countB_dbA, "Agent B should NOT exist in Tenant A schema via DB A")

	// 6. Verify Agent A does NOT exist using direct connection to Schema B
	var countA_dbB int
	queryA_dbB := fmt.Sprintf(`SELECT COUNT(*) FROM %q.agents WHERE agent_id = $1`, schemaB)
	err = dbB.QueryRow(queryA_dbB, agentA.AgentID).Scan(&countA_dbB)
	s.Require().NoError(err, "Query A via DB B failed")
	s.Require().Equal(0, countA_dbB, "Agent A should NOT exist in Tenant B schema via DB B")

	// Cleanup: Drop Tenant B schema
	// Connect to default DB without search_path to drop schema
	defaultDb, err := connectDB(s.PostgresDSN)
	s.Require().NoError(err, "Cleanup: Failed to connect to default DB")
	defer defaultDb.Close()
	_, err = defaultDb.Exec(fmt.Sprintf("DROP SCHEMA IF EXISTS %q CASCADE", schemaB))
	s.Require().NoError(err, "Cleanup: Failed to drop Tenant B schema")
}

// Add TestFindAgentByAgentID if needed (requires method on repo)
