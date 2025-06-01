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

// ChatRepoTestSuite defines the suite for Chat repository tests
// It embeds BaseIntegrationSuite to get DB/NATS but not the app container.
type ChatRepoTestSuite struct {
	BaseIntegrationSuite
	repo             *storage.PostgresRepo // Repo instance for the suite's default CompanyID
	TenantSchemaName string
}

// SetupTest runs before each test in this suite.
// It initializes the repository for the suite's CompanyID and prepares the schema.
func (s *ChatRepoTestSuite) SetupTest() {
	logger.Log = zaptest.NewLogger(s.T()).Named("ChatRepoTestSuite")

	// Use the DSN and CompanyID from the embedded BaseIntegrationSuite
	repo, err := storage.NewPostgresRepo(s.PostgresDSN, true, s.CompanyID)
	s.Require().NoError(err, "SetupTest: Failed to create repo for default tenant")
	s.Require().NotNil(repo, "SetupTest: Repo should not be nil")
	s.repo = repo

	s.TenantSchemaName = fmt.Sprintf("daisi_%s", s.CompanyID)

	// BaseIntegrationSuite.SetupTest() is expected to handle schema creation (if needed)
	// and cleanup for the s.TenantSchemaName based on s.CompanyID.
	// Explicit truncation is removed to follow agent_repository_test.go pattern.
	s.BaseIntegrationSuite.SetupTest()
}

// TearDownTest runs after each test in this suite.
func (s *ChatRepoTestSuite) TearDownTest() {
	if s.repo != nil {
		// Use the suite's context
		s.repo.Close(s.Ctx) // Close the connection used in the test
	}
}

// TestRunner runs the test suite
func TestChatRepoSuite(t *testing.T) {
	suite.Run(t, new(ChatRepoTestSuite))
}

// --- Test Cases ---

func (s *ChatRepoTestSuite) TestSaveChat() {
	// Use suite context and company ID
	ctx := tenant.WithCompanyID(s.Ctx, s.CompanyID)

	now := time.Now().UTC()
	timestamp := now.Unix()

	// 1. Create a new chat using the fixture generator
	chatID := "test-chat-" + uuid.New().String()
	overrides := &model.Chat{
		ChatID:                chatID,
		CompanyID:             s.CompanyID,
		Jid:                   "jid-" + uuid.New().String() + "@s.whatsapp.net",
		PhoneNumber:           "+1234567890",
		PushName:              "Initial Pusher",
		UnreadCount:           2,
		ConversationTimestamp: timestamp,
		LastMessageObj:        datatypes.JSON(utils.MustMarshalJSON(map[string]string{"text": "First message"})),
	}
	chatInterface, err := generateModelStruct("Chat", overrides)
	s.Require().NoError(err, "Failed to generate chat model")
	chat := chatInterface.(*model.Chat) // Type assertion

	// 2. Save the chat (create)
	err = s.repo.SaveChat(ctx, *chat)
	s.Require().NoError(err, "SaveChat (create) failed")

	// 3. Verify creation using direct query via connectDB
	// Connect directly using s.PostgresDSN, without search_path
	db, err := connectDB(s.PostgresDSN)
	s.Require().NoError(err, "Failed to connect to DB for verification")
	defer db.Close()

	var retrievedChat model.Chat
	query := fmt.Sprintf("SELECT chat_id, company_id, jid, custom_name, unread_count, conversation_timestamp FROM %q.chats WHERE chat_id = $1 AND company_id = $2", s.TenantSchemaName)
	err = db.QueryRow(query, chat.ChatID, s.CompanyID).
		Scan(&retrievedChat.ChatID, &retrievedChat.CompanyID, &retrievedChat.Jid, &retrievedChat.UnreadCount, &retrievedChat.ConversationTimestamp)
	s.Require().NoError(err, "Direct query failed for created chat")
	s.Require().Equal(chat.ChatID, retrievedChat.ChatID)
	s.Require().Equal(int32(2), retrievedChat.UnreadCount)
	s.Require().Equal(timestamp, retrievedChat.ConversationTimestamp)

	// 4. Update the chat
	chat.UnreadCount = 0
	chat.LastMessageObj = datatypes.JSON(utils.MustMarshalJSON(map[string]string{"text": "Second message"}))
	// Assume ConversationTimestamp might update with a new message
	newTimestamp := now.Add(time.Minute).Unix()
	chat.ConversationTimestamp = newTimestamp

	err = s.repo.SaveChat(ctx, *chat) // Save again (update)
	s.Require().NoError(err, "SaveChat (update) failed")

	// 5. Verify the update using direct query
	var updatedChat model.Chat
	var updatedLastMsgObjStr string
	queryUpdated := fmt.Sprintf("SELECT chat_id, custom_name, unread_count, conversation_timestamp, last_message::text FROM %q.chats WHERE chat_id = $1 AND company_id = $2", s.TenantSchemaName)
	err = db.QueryRow(queryUpdated, chat.ChatID, s.CompanyID).
		Scan(&updatedChat.ChatID, &updatedChat.UnreadCount, &updatedChat.ConversationTimestamp, &updatedLastMsgObjStr)
	s.Require().NoError(err, "Direct query failed for updated chat")
	s.Require().Equal(chat.ChatID, updatedChat.ChatID)
	s.Require().Equal(int32(0), updatedChat.UnreadCount)
	s.Require().Equal(newTimestamp, updatedChat.ConversationTimestamp)
	s.Require().JSONEq(string(chat.LastMessageObj), updatedLastMsgObjStr)
}

// TestChatTenantIsolation verifies that operations are isolated to the correct tenant.
func (s *ChatRepoTestSuite) TestChatTenantIsolation() {
	baseCtx := s.Ctx // Use suite context as base

	// Tenant A (uses the suite's default CompanyID and repo)
	tenantA_ID := s.CompanyID
	schemaA := s.TenantSchemaName // Use TenantSchemaName for tenant A
	ctxA := tenant.WithCompanyID(baseCtx, tenantA_ID)
	chatAInterface, err := generateModelStruct("Chat", &model.Chat{
		ChatID:                "tenant-iso-chat-A-" + uuid.New().String(),
		CompanyID:             tenantA_ID,
		Jid:                   "jid-A@example.com",
		ConversationTimestamp: time.Now().Unix(),
	})
	s.Require().NoError(err)
	chatA := chatAInterface.(*model.Chat)

	// Tenant B
	tenantB_ID := "tenant_b_chat_" + uuid.New().String()
	schemaB := fmt.Sprintf("daisi_%s", tenantB_ID)
	ctxB := tenant.WithCompanyID(baseCtx, tenantB_ID)
	// Use the suite's PostgresDSN to create repo for tenant B
	repoB, err := storage.NewPostgresRepo(s.PostgresDSN, true, tenantB_ID)
	s.Require().NoError(err, "Failed to create repo for Tenant B")
	s.Require().NotNil(repoB)
	defer repoB.Close(baseCtx)

	chatBInterface, err := generateModelStruct("Chat", &model.Chat{
		ChatID:                "tenant-iso-chat-B-" + uuid.New().String(),
		CompanyID:             tenantB_ID,
		Jid:                   "jid-B@example.com",
		ConversationTimestamp: time.Now().Unix(),
	})
	s.Require().NoError(err)
	chatB := chatBInterface.(*model.Chat)

	// 1. Save Chat A using Repo A
	err = s.repo.SaveChat(ctxA, *chatA)
	s.Require().NoError(err, "Failed to save Chat A")

	// 2. Save Chat B using Repo B
	err = repoB.SaveChat(ctxB, *chatB)
	s.Require().NoError(err, "Failed to save Chat B")

	// 3. Verify Chat A exists using direct connection to Schema A
	dbA, err := connectDB(s.PostgresDSN) // Connect directly
	s.Require().NoError(err, "Failed to connect to DB for Tenant A verification")
	defer dbA.Close()
	var countA_dbA int
	queryA_dbA := fmt.Sprintf("SELECT COUNT(*) FROM %q.chats WHERE chat_id = $1 AND company_id = $2", schemaA)
	err = dbA.QueryRow(queryA_dbA, chatA.ChatID, tenantA_ID).Scan(&countA_dbA)
	s.Require().NoError(err, "Query A via DB A failed")
	s.Require().Equal(1, countA_dbA, "Chat A should exist in Tenant A schema via DB A")

	// 4. Verify Chat B exists using direct connection to Schema B
	dbB, err := connectDB(s.PostgresDSN) // Connect directly
	s.Require().NoError(err, "Failed to connect to DB for Tenant B verification")
	defer dbB.Close()
	var countB_dbB int
	queryB_dbB := fmt.Sprintf("SELECT COUNT(*) FROM %q.chats WHERE chat_id = $1 AND company_id = $2", schemaB)
	err = dbB.QueryRow(queryB_dbB, chatB.ChatID, tenantB_ID).Scan(&countB_dbB)
	s.Require().NoError(err, "Query B via DB B failed")
	s.Require().Equal(1, countB_dbB, "Chat B should exist in Tenant B schema via DB B")

	// 5. Verify Chat B does NOT exist using direct connection to Schema A
	var countB_dbA int
	queryB_dbA := fmt.Sprintf("SELECT COUNT(*) FROM %q.chats WHERE chat_id = $1", schemaA) // Check against schemaA
	err = dbA.QueryRow(queryB_dbA, chatB.ChatID).Scan(&countB_dbA)
	s.Require().NoError(err, "Query B via DB A failed")
	s.Require().Equal(0, countB_dbA, "Chat B should not be found via DB A")

	// 6. Verify Chat A does NOT exist using direct connection to Schema B
	var countA_dbB int
	queryA_dbB := fmt.Sprintf("SELECT COUNT(*) FROM %q.chats WHERE chat_id = $1", schemaB) // Check against schemaB
	err = dbB.QueryRow(queryA_dbB, chatA.ChatID).Scan(&countA_dbB)
	s.Require().NoError(err, "Query A via DB B failed")
	s.Require().Equal(0, countA_dbB, "Chat A should not be found via DB B")

	// Cleanup: Drop Tenant B schema
	defaultDbSQL, err := connectDB(s.PostgresDSN)
	s.Require().NoError(err, "Cleanup: Failed to connect to default DB")
	defer defaultDbSQL.Close()
	_, err = defaultDbSQL.Exec(fmt.Sprintf("DROP SCHEMA IF EXISTS %q CASCADE", schemaB)) // Use %q for schemaB
	s.Require().NoError(err, "Cleanup: Failed to drop Tenant B schema")
}

// Add TestBulkUpsertChats if needed
