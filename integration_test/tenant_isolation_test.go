package integration_test

import (
	"fmt"
	"testing"
	"time"

	"gitlab.com/timkado/api/daisi-wa-events-processor/pkg/logger"
	"go.uber.org/zap/zaptest"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/model"
)

// TenantIsolationTestSuite defines the suite for tenant isolation tests.
// It embeds E2EIntegrationTestSuite as it needs the app running.
type TenantIsolationTestSuite struct {
	E2EIntegrationTestSuite
}

// TestRunner runs the tenant isolation test suite
func TestTenantIsolationSuite(t *testing.T) {
	suite.Run(t, new(TenantIsolationTestSuite))
}

// SetupTest runs before each test in this suite.
func (s *TenantIsolationTestSuite) SetupTest() {
	s.E2EIntegrationTestSuite.SetupTest() // Run base setup (DB truncation)
	t := s.T()
	logger.Log = zaptest.NewLogger(t).Named("TenantIsolationTestSuite")
	t.Log("Waiting for daisi-wa-events-processor to potentially process previous test remnants...")
	time.Sleep(1 * time.Second) // Short delay before each test
}

// TestCrossTenantDataAccess tests that the service properly enforces tenant isolation
// and prevents accessing data from the wrong tenant.
// Test ID: TI-01
func (s *TenantIsolationTestSuite) TestCrossTenantDataAccess() {
	t := s.T()
	ctx := s.Ctx
	t.Log("Testing cross-tenant data access isolation...")

	// Start streaming app container logs
	s.StreamLogs(t)

	// Define tenant IDs and schemas
	companyID1 := s.CompanyID
	companyID2 := "tenant_two"
	tenantSchema1 := s.CompanySchemaName

	// Generate unique IDs
	chatID := fmt.Sprintf("uuid:chat-tenant1-%s", uuid.New().String())
	messageID := fmt.Sprintf("uuid:message-tenant1-%s", uuid.New().String())

	// --- 1. Create a chat in tenant 1 using map[string]interface{} ---
	chatSubject1 := fmt.Sprintf("%s.%s", SubjectMapping["chat_created"], companyID1)
	chatPayload1Map := map[string]interface{}{
		"chat_id":                chatID,
		"company_id":             companyID1,
		"jid":                    "6281234567890@s.whatsapp.net",
		"push_name":              "Company One User",
		"conversation_timestamp": time.Now().Unix(),
		"unread_count":           0,
		"is_group":               false,
	}
	chatPayload1Bytes, err := s.GenerateNatsPayload(chatSubject1, chatPayload1Map)
	s.Require().NoError(err, "Failed to generate chat payload for tenant 1")

	// Publish chat for tenant 1
	err = s.PublishEvent(ctx, chatSubject1, chatPayload1Bytes)
	s.Require().NoError(err, "Failed to publish chat for tenant 1")
	time.Sleep(2 * time.Second) // Wait for processing

	// --- 2. Create a message for the chat in tenant 1 using map[string]interface{} ---
	messageSubject1 := fmt.Sprintf("%s.%s", SubjectMapping["message_created"], companyID1)
	messagePayload1Map := map[string]interface{}{
		"message_id":        messageID,
		"chat_id":           chatID,
		"company_id":        companyID1,
		"message_obj":       map[string]interface{}{"text": "Company One Message"},
		"message_timestamp": time.Now().Unix(),
		"from":              "6281234567890@s.whatsapp.net",
		"status":            "sent",
	}
	messagePayload1Bytes, err := s.GenerateNatsPayload(messageSubject1, messagePayload1Map)
	s.Require().NoError(err, "Failed to generate message payload for tenant 1")

	// Publish message for tenant 1
	err = s.PublishEvent(ctx, messageSubject1, messagePayload1Bytes)
	s.Require().NoError(err, "Failed to publish message for tenant 1")
	time.Sleep(2 * time.Second) // Wait for processing

	// --- 3. Verify that data exists in tenant 1 ---
	verifyTenant1ChatQuery := "SELECT COUNT(*) FROM chats WHERE chat_id = $1 AND company_id = $2"
	tenant1ChatExists, err := verifyPostgresDataWithSchema(ctx, s.PostgresDSN, tenantSchema1, verifyTenant1ChatQuery, 1, chatID, companyID1)
	s.Require().NoError(err, "Failed to verify chat exists in tenant 1")
	s.Assert().True(tenant1ChatExists, "Chat should exist in tenant 1")

	verifyTenant1MessageQuery := "SELECT COUNT(*) FROM messages WHERE message_id = $1 AND company_id = $2"
	tenant1MessageExists, err := verifyPostgresDataWithSchema(ctx, s.PostgresDSN, tenantSchema1, verifyTenant1MessageQuery, 1, messageID, companyID1)
	s.Require().NoError(err, "Failed to verify message exists in tenant 1")
	s.Assert().True(tenant1MessageExists, "Message should exist in tenant 1")

	// --- 4. Try to update the chat with tenant 2 ID (cross-tenant access attempt) using map[string]interface{} ---
	chatUpdateSubject2 := fmt.Sprintf("%s.%s", SubjectMapping["chat_updated"], companyID1) // Subject for tenant 1
	crossTenantChatPayloadMap := map[string]interface{}{
		"id":                     chatID,     // For UpdateChatPayload, the ID field is 'id'
		"company_id":             companyID2, // Different tenant ID
		"conversation_timestamp": time.Now().Unix(),
		"unread_count":           1,
	}
	crossTenantChatPayloadBytes, err := s.GenerateNatsPayload(chatUpdateSubject2, crossTenantChatPayloadMap)
	s.Require().NoError(err, "Failed to generate cross-tenant chat update payload")

	// Publish cross-tenant chat update
	err = s.PublishEvent(ctx, chatUpdateSubject2, crossTenantChatPayloadBytes)
	s.Require().NoError(err, "Failed to publish cross-tenant chat update")
	time.Sleep(2 * time.Second) // Wait for processing

	// --- 5. Verify that the update did NOT affect tenant 1's data ---
	verifyUnchangedQuery := "SELECT COUNT(*) FROM chats WHERE chat_id = $1 AND company_id = $2 AND unread_count = $3"
	dataUnchanged, err := verifyPostgresDataWithSchema(ctx, s.PostgresDSN, tenantSchema1, verifyUnchangedQuery, 1, chatID, companyID1, 0)
	s.Require().NoError(err, "Failed to verify tenant 1 data unchanged")
	s.Assert().True(dataUnchanged, "Company 1 data should remain unchanged after cross-tenant update attempt")

	t.Log("Cross-tenant data access isolation test completed successfully")
}

// testMissingTenantID tests that the service properly handles events without a tenant ID.
// It should be rejected.
// Test ID: TI-02
func (s *TenantIsolationTestSuite) TestMissingTenantID() {
	t := s.T()
	ctx := s.Ctx
	t.Log("Testing missing tenant ID handling...")

	// Start streaming app container logs
	s.StreamLogs(t)

	// Generate unique IDs
	chatID := fmt.Sprintf("uuid:chat-missing-tenant-%s", uuid.New().String())
	messageID := fmt.Sprintf("uuid:message-missing-tenant-%s", uuid.New().String())

	// --- 1. Create a chat payload without CompanyID using fixture ---
	// Subject still needs a tenant suffix for routing, use default, but payload lacks it
	chatSubjectDefault := fmt.Sprintf("%s.%s", SubjectMapping["chat_created"], DefaultCompanyID)
	chatPayloadMissingBytes, err := s.GenerateNatsPayload(chatSubjectDefault, &model.UpsertChatPayload{
		ChatID: chatID,
		// CompanyID: "", // Omitted or explicitly set to empty string based on desired test
		Jid:                   "6281234567890@s.whatsapp.net",
		PushName:              "Missing Company User",
		ConversationTimestamp: time.Now().Unix(),
		UnreadCount:           0,
		IsGroup:               false,
	})
	s.Require().NoError(err, "Failed to generate chat without tenant ID")

	// Publish chat without tenant ID
	err = s.PublishEvent(ctx, chatSubjectDefault, chatPayloadMissingBytes)
	s.Require().NoError(err, "Failed to publish chat without tenant ID")
	time.Sleep(2 * time.Second) // Wait for processing

	// --- 2. Check that the chat was rejected (not stored in default or any schema) ---
	defaultTenantSchema := fmt.Sprintf("daisi_%s", DefaultCompanyID)
	checkDefaultSchemaQuery := "SELECT COUNT(*) FROM chats WHERE chat_id = $1"
	chatRejectedDefault, err := verifyPostgresDataWithSchema(ctx, s.PostgresDSN, defaultTenantSchema, checkDefaultSchemaQuery, 0, chatID)
	s.Require().NoError(err, "Failed to check default schema for missing tenant chat")
	s.Assert().True(chatRejectedDefault, "Chat with missing tenant ID should not be stored in default schema")
	// Could potentially check other schemas if necessary, but default should be sufficient.

	// --- 3. Create a message payload without CompanyID ---
	messageSubjectDefault := fmt.Sprintf("%s.%s", SubjectMapping["message_created"], DefaultCompanyID)
	messagePayloadMissingBytes, err := s.GenerateNatsPayload(messageSubjectDefault, &model.UpsertMessagePayload{
		MessageID: messageID,
		ChatID:    chatID, // Use the chat ID from above
		// CompanyID: "", // Omitted or explicitly set to empty string
		MessageObj:       map[string]interface{}{"text": "Message without tenant ID"},
		MessageTimestamp: time.Now().Unix(),
		FromUser:         "6281234567890@s.whatsapp.net",
		Status:           "sent",
	})
	s.Require().NoError(err, "Failed to generate message without tenant ID")

	// Publish message without tenant ID
	err = s.PublishEvent(ctx, messageSubjectDefault, messagePayloadMissingBytes)
	s.Require().NoError(err, "Failed to publish message without tenant ID")
	time.Sleep(2 * time.Second) // Wait for processing

	// --- 4. Check that the message was rejected ---
	checkMessageQuery := "SELECT COUNT(*) FROM messages WHERE message_id = $1"
	msgRejected, err := verifyPostgresDataWithSchema(ctx, s.PostgresDSN, defaultTenantSchema, checkMessageQuery, 0, messageID)
	s.Require().NoError(err, "Failed to check default schema for missing tenant message")
	s.Assert().True(msgRejected, "Message with missing tenant ID should not be stored")

	t.Log("Missing tenant ID test completed (rejected as expected)")
}

// testInvalidTenantID tests that the service properly handles events with an invalid tenant ID format.
// The service should reject such events.
// Test ID: TI-03
func (s *TenantIsolationTestSuite) TestInvalidTenantID() {
	t := s.T()
	ctx := s.Ctx
	t.Log("Testing invalid tenant ID format...")

	// Start streaming app container logs
	s.StreamLogs(t)

	// Define various invalid tenant ID formats
	// Note: The validity depends on schema naming constraints (e.g., no spaces, special chars)
	invalidTenantIDs := []string{
		"",               // Empty string
		" ",              // Whitespace
		"tenant*invalid", // With special characters likely invalid for schema names
		// "tenant-with-dash", // Dashes might be allowed depending on DB
		"tenant with spaces", // Spaces definitely invalid for schema names
		"12345",              // Starts with number (maybe invalid)
	}

	for _, invalidTenantID := range invalidTenantIDs {
		// Generate unique IDs for this iteration
		chatID := fmt.Sprintf("uuid:chat-invalid-tenant-%s-%s", invalidTenantID, uuid.New().String())
		messageID := fmt.Sprintf("uuid:message-invalid-tenant-%s-%s", invalidTenantID, uuid.New().String())
		// Schema name generation for verification (expecting schema not to be created)
		// Need a placeholder valid schema for DB check as the invalid one won't exist
		placeholderSchema := fmt.Sprintf("daisi_%s", DefaultCompanyID)

		t.Run(fmt.Sprintf("TenantID_%s", invalidTenantID), func(t *testing.T) {
			st := require.New(t) // Use subtest require

			t.Logf("Testing with invalid tenant ID: '%s'", invalidTenantID)

			// --- 1. Create chat payload with invalid tenant ID ---
			// Subject must still route, use default, but payload has invalid ID
			chatSubject := fmt.Sprintf("%s.%s", SubjectMapping["chat_created"], DefaultCompanyID)
			chatPayloadInvalidBytes, err := s.GenerateNatsPayload(chatSubject, &model.UpsertChatPayload{
				ChatID:    chatID,
				CompanyID: invalidTenantID, // The invalid ID
				Jid:       "628invalid@w.net",
			})
			st.NoError(err, "Failed to generate chat payload with invalid tenant ID")

			// Publish chat with invalid tenant ID
			err = s.PublishEvent(ctx, chatSubject, chatPayloadInvalidBytes)
			st.NoError(err, "Publishing chat with invalid tenant ID should succeed at NATS level")
			time.Sleep(2 * time.Second) // Wait for processing (and rejection)

			// --- 2. Check if the chat was rejected (should not be stored in ANY schema) ---
			// We check the default schema as a proxy, assuming it wouldn't store it there either.
			invalidChatQuery := "SELECT COUNT(*) FROM chats WHERE chat_id = $1"
			chatRejected, err := verifyPostgresDataWithSchema(ctx, s.PostgresDSN, placeholderSchema, invalidChatQuery, 0, chatID)
			st.NoError(err, "Failed to query for rejected chat")
			st.True(chatRejected, fmt.Sprintf("Chat with invalid tenant ID '%s' should be rejected", invalidTenantID))

			// --- 3. Create message payload with invalid tenant ID ---
			messageSubject := fmt.Sprintf("%s.%s", SubjectMapping["message_created"], DefaultCompanyID)
			messagePayloadInvalidBytes, err := s.GenerateNatsPayload(messageSubject, &model.UpsertMessagePayload{
				MessageID:  messageID,
				ChatID:     chatID,          // Can use the same chat ID
				CompanyID:  invalidTenantID, // Invalid tenant ID
				MessageObj: map[string]interface{}{"text": "Invalid tenant message"},
			})
			st.NoError(err, "Failed to generate message payload with invalid tenant ID")

			// Publish message with invalid tenant ID
			err = s.PublishEvent(ctx, messageSubject, messagePayloadInvalidBytes)
			st.NoError(err, "Publishing message with invalid tenant ID should succeed at NATS level")
			time.Sleep(2 * time.Second) // Wait for processing (and rejection)

			// --- 4. Check if the message was rejected ---
			invalidMessageQuery := "SELECT COUNT(*) FROM messages WHERE message_id = $1"
			messageRejected, err := verifyPostgresDataWithSchema(ctx, s.PostgresDSN, placeholderSchema, invalidMessageQuery, 0, messageID)
			st.NoError(err, "Failed to query for rejected message")
			st.True(messageRejected, fmt.Sprintf("Message with invalid tenant ID '%s' should be rejected", invalidTenantID))
		})
	}

	t.Log("Invalid tenant ID test completed successfully")
}
