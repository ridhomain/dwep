package integration_test

import (
	"fmt"
	"testing"
	"time"

	"gitlab.com/timkado/api/daisi-wa-events-processor/pkg/logger"
	"go.uber.org/zap/zaptest"

	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib" // PostgreSQL driver
	"github.com/stretchr/testify/suite"
	"gorm.io/datatypes"

	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/model"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/storage"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/tenant"
	"gitlab.com/timkado/api/daisi-wa-events-processor/pkg/utils"
)

// MessageRepoTestSuite defines the suite for Message repository tests
// It embeds BaseIntegrationSuite to get DB/NATS but not the app container.
type MessageRepoTestSuite struct {
	BaseIntegrationSuite                       // Embed the base suite
	repo                 *storage.PostgresRepo // Repo instance for the suite's default CompanyID
	TenantSchemaName     string                // Added to store the schema name for the default tenant
}

// SetupSuite and TearDownSuite are inherited from BaseIntegrationSuite.

// SetupTest runs before each test in this suite.
// It initializes the repository for the suite's CompanyID and prepares the schema.
func (s *MessageRepoTestSuite) SetupTest() {
	logger.Log = zaptest.NewLogger(s.T()).Named("MessageRepoTestSuite")
	// Use DSN and CompanyID from the embedded BaseIntegrationSuite
	repo, err := storage.NewPostgresRepo(s.PostgresDSN, true, s.CompanyID)
	s.Require().NoError(err, "SetupTest: Failed to create repo for default tenant")
	s.Require().NotNil(repo, "SetupTest: Repo should not be nil")
	s.repo = repo

	// Store the tenant schema name for use in direct queries
	s.TenantSchemaName = fmt.Sprintf("daisi_%s", s.CompanyID)

	// BaseIntegrationSuite.SetupTest() is expected to handle schema creation (if needed)
	// and cleanup for s.TenantSchemaName. Explicit truncation of 'messages' table is removed.
	s.BaseIntegrationSuite.SetupTest()
}

// TearDownTest runs after each test in this suite.
func (s *MessageRepoTestSuite) TearDownTest() {
	if s.repo != nil {
		// Use the suite's context
		s.repo.Close(s.Ctx) // Close the connection used in the test
	}
}

// TestRunner runs the test suite
func TestMessageRepoSuite(t *testing.T) {
	suite.Run(t, new(MessageRepoTestSuite))
}

// --- Test Cases ---

func (s *MessageRepoTestSuite) TestSaveMessage() {
	// Use suite context and company ID
	ctx := tenant.WithCompanyID(s.Ctx, s.CompanyID)

	now := time.Now().UTC()
	messageTimestamp := now.Unix()
	messageDate := now.Format("2006-01-02") // YYYY-MM-DD format for querying date column

	// 1. Create a new message using the fixture generator
	messageID := "test-msg-" + uuid.New().String()
	chatID := "chat-" + uuid.New().String()
	overrides := &model.Message{
		MessageID:        messageID,
		CompanyID:        s.CompanyID, // Use suite CompanyID
		ChatID:           chatID,
		Jid:              "jid-" + uuid.New().String() + "@s.whatsapp.net",
		FromUser:         "sender@example.com",
		ToUser:           "receiver@example.com",
		Status:           "DELIVERED",
		Flow:             "INCOMING",
		MessageTimestamp: messageTimestamp,
		MessageObj:       datatypes.JSON(utils.MustMarshalJSON(map[string]string{"text": "Initial message"})),
		Key:              datatypes.JSON(utils.MustMarshalJSON(map[string]interface{}{"id": uuid.New().String(), "fromMe": false})),
	}
	msgInterface, err := generateModelStruct("Message", overrides)
	s.Require().NoError(err, "Failed to generate message model")
	msg := msgInterface.(*model.Message) // Type assertion

	// 2. Save the message (create)
	err = s.repo.SaveMessage(ctx, *msg)
	s.Require().NoError(err, "SaveMessage (create) failed")

	// 3. Verify creation using direct query via connectDB
	// Connect directly using s.PostgresDSN, without search_path
	db, err := connectDB(s.PostgresDSN)
	s.Require().NoError(err, "Failed to connect to DB for verification")
	defer db.Close()

	var retrievedMsg model.Message
	var retrievedDate string // Use string to scan date text
	query := fmt.Sprintf("SELECT message_id, company_id, chat_id, status, flow, message_timestamp, message_date::text FROM %q.messages WHERE message_id = $1 AND company_id = $2", s.TenantSchemaName)
	err = db.QueryRow(query, msg.MessageID, s.CompanyID).
		Scan(&retrievedMsg.MessageID, &retrievedMsg.CompanyID, &retrievedMsg.ChatID, &retrievedMsg.Status, &retrievedMsg.Flow, &retrievedMsg.MessageTimestamp, &retrievedDate)
	s.Require().NoError(err, "Direct query failed for created message")
	s.Require().Equal(msg.MessageID, retrievedMsg.MessageID)
	s.Require().Equal("DELIVERED", retrievedMsg.Status)
	s.Require().Equal(messageTimestamp, retrievedMsg.MessageTimestamp)
	s.Require().Equal(messageDate, retrievedDate) // Verify date partition key

	// 4. Update the message
	msg.Status = "READ"
	msg.MessageObj = datatypes.JSON(utils.MustMarshalJSON(map[string]string{"text": "Updated message"}))
	// MessageTimestamp shouldn't change, so MessageDate also remains the same

	err = s.repo.SaveMessage(ctx, *msg) // Save again (update)
	s.Require().NoError(err, "SaveMessage (update) failed")

	// 5. Verify the update using direct query
	var updatedMsg model.Message
	var updatedMsgObjStr string
	var updatedDate string
	queryUpdated := fmt.Sprintf("SELECT message_id, status, message_obj::text, message_date::text FROM %q.messages WHERE message_id = $1 AND company_id = $2", s.TenantSchemaName)
	err = db.QueryRow(queryUpdated, msg.MessageID, s.CompanyID).
		Scan(&updatedMsg.MessageID, &updatedMsg.Status, &updatedMsgObjStr, &updatedDate)
	s.Require().NoError(err, "Direct query failed for updated message")
	s.Require().Equal(msg.MessageID, updatedMsg.MessageID)
	s.Require().Equal("READ", updatedMsg.Status)
	s.Require().JSONEq(string(msg.MessageObj), updatedMsgObjStr) // Compare JSON content
	s.Require().Equal(messageDate, updatedDate)                  // Verify date partition key is unchanged
}

// TestMessageTenantIsolation verifies that operations are isolated to the correct tenant.
func (s *MessageRepoTestSuite) TestMessageTenantIsolation() {
	baseCtx := s.Ctx // Use suite context

	// Tenant A (uses the suite's default CompanyID and repo)
	tenantA_ID := s.CompanyID
	schemaA := s.TenantSchemaName // Use TenantSchemaName for tenant A
	ctxA := tenant.WithCompanyID(baseCtx, tenantA_ID)
	msgAInterface, err := generateModelStruct("Message", &model.Message{
		MessageID:        "tenant-iso-msg-A-" + uuid.New().String(),
		CompanyID:        tenantA_ID,
		ChatID:           "chat-A",
		MessageTimestamp: time.Now().Unix(),
	})
	s.Require().NoError(err)
	msgA := msgAInterface.(*model.Message)

	// Tenant B
	tenantB_ID := "tenant_b_msg_" + uuid.New().String()
	schemaB := fmt.Sprintf("daisi_%s", tenantB_ID)
	ctxB := tenant.WithCompanyID(baseCtx, tenantB_ID)
	// Use the suite's PostgresDSN to create repo for tenant B
	repoB, err := storage.NewPostgresRepo(s.PostgresDSN, true, tenantB_ID)
	s.Require().NoError(err, "Failed to create repo for Tenant B")
	s.Require().NotNil(repoB)
	defer repoB.Close(baseCtx)

	msgBInterface, err := generateModelStruct("Message", &model.Message{
		MessageID:        "tenant-iso-msg-B-" + uuid.New().String(),
		CompanyID:        tenantB_ID,
		ChatID:           "chat-B",
		MessageTimestamp: time.Now().Unix(),
	})
	s.Require().NoError(err)
	msgB := msgBInterface.(*model.Message)

	// 1. Save Message A using Repo A (s.repo)
	err = s.repo.SaveMessage(ctxA, *msgA)
	s.Require().NoError(err, "Failed to save Message A")

	// 2. Save Message B using Repo B
	err = repoB.SaveMessage(ctxB, *msgB)
	s.Require().NoError(err, "Failed to save Message B")

	// 3. Verify Message A exists using direct connection to Schema A
	dbA, err := connectDB(s.PostgresDSN) // Connect directly
	s.Require().NoError(err, "Failed to connect to DB for Tenant A verification")
	defer dbA.Close()
	var countA_dbA int
	queryA_dbA := fmt.Sprintf("SELECT COUNT(*) FROM %q.messages WHERE message_id = $1 AND company_id = $2", schemaA)
	err = dbA.QueryRow(queryA_dbA, msgA.MessageID, tenantA_ID).Scan(&countA_dbA)
	s.Require().NoError(err, "Query A via DB A failed")
	s.Require().Equal(1, countA_dbA, "Message A should exist in Tenant A schema via DB A")

	// 4. Verify Message B exists using direct connection to Schema B
	dbB, err := connectDB(s.PostgresDSN) // Connect directly
	s.Require().NoError(err, "Failed to connect to DB for Tenant B verification")
	defer dbB.Close()
	var countB_dbB int
	queryB_dbB := fmt.Sprintf("SELECT COUNT(*) FROM %q.messages WHERE message_id = $1 AND company_id = $2", schemaB)
	err = dbB.QueryRow(queryB_dbB, msgB.MessageID, tenantB_ID).Scan(&countB_dbB)
	s.Require().NoError(err, "Query B via DB B failed")
	s.Require().Equal(1, countB_dbB, "Message B should exist in Tenant B schema via DB B")

	// 5. Verify Message B does NOT exist using direct connection to Schema A
	var countB_dbA int
	queryB_dbA := fmt.Sprintf("SELECT COUNT(*) FROM %q.messages WHERE message_id = $1", schemaA) // Check against schemaA
	err = dbA.QueryRow(queryB_dbA, msgB.MessageID).Scan(&countB_dbA)
	s.Require().NoError(err, "Query B via DB A failed")
	s.Require().Equal(0, countB_dbA, "Message B should not be found via DB A")

	// 6. Verify Message A does NOT exist using direct connection to Schema B
	var countA_dbB int
	queryA_dbB := fmt.Sprintf("SELECT COUNT(*) FROM %q.messages WHERE message_id = $1", schemaB) // Check against schemaB
	err = dbB.QueryRow(queryA_dbB, msgA.MessageID).Scan(&countA_dbB)
	s.Require().NoError(err, "Query A via DB B failed")
	s.Require().Equal(0, countA_dbB, "Message A should not be found via DB B")

	// Cleanup: Drop Tenant B schema
	// Use the suite's default DSN which should have permissions
	defaultDbSQL, err := connectDB(s.PostgresDSN)
	s.Require().NoError(err, "Cleanup: Failed to connect to default DB")
	defer defaultDbSQL.Close()
	_, err = defaultDbSQL.Exec(fmt.Sprintf("DROP SCHEMA IF EXISTS %q CASCADE", schemaB)) // Use %q for schemaB
	s.Require().NoError(err, "Cleanup: Failed to drop Tenant B schema")
}

// Add TestBulkUpsertMessages if needed
