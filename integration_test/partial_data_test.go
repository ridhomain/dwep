package integration_test

import (
	"fmt"
	"testing"
	"time"

	"gitlab.com/timkado/api/daisi-wa-events-processor/pkg/logger"
	"go.uber.org/zap/zaptest"

	"github.com/google/uuid"
	// "gorm.io/datatypes" // Removed unused import

	"github.com/stretchr/testify/suite"

	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/model"
)

// PartialDataTestSuite defines the suite for partial data handling tests
// It embeds E2EIntegrationTestSuite as it needs the app container running.
type PartialDataTestSuite struct {
	E2EIntegrationTestSuite
}

// TestRunner runs the partial data test suite
func TestPartialDataSuite(t *testing.T) {
	suite.Run(t, new(PartialDataTestSuite))
}

// SetupTest runs before each test in this suite.
func (s *PartialDataTestSuite) SetupTest() {
	s.E2EIntegrationTestSuite.SetupTest() // Run base setup (DB truncation)
	t := s.T()
	logger.Log = zaptest.NewLogger(s.T()).Named("PartialDataTestSuite")
	t.Log("Waiting for daisi-wa-events-processor to potentially process previous test remnants...")
	time.Sleep(1 * time.Second) // Short delay before each test
}

// --- Test Cases (Converted from original functions) ---

// testPartialChatUpdate tests that the service properly handles partial chat updates.
// Test ID: PD-01
func (s *PartialDataTestSuite) TestPartialChatUpdate() {
	t := s.T()
	ctx := s.Ctx // Use suite context
	tenantID := s.CompanyID
	tenantSchema := fmt.Sprintf("daisi_%s", tenantID)
	t.Log("Testing partial chat update...")

	// Start streaming app container logs
	// s.StreamLogs(t) // Use suite method - Assuming StreamLogs is called by BeforeTest in E2EIntegrationTestSuite

	// --- Create initial complete chat payload using fixture generator ---
	chatID := fmt.Sprintf("uuid:chat-partial-%s", uuid.New().String())
	initialChatOverrides := &model.UpsertChatPayload{ // Use the payload struct type
		ChatID:                chatID,
		CompanyID:             tenantID,
		Jid:                   "6281234567890@s.whatsapp.net",
		PushName:              "Initial Customer",
		GroupName:             "Initial Group",
		ConversationTimestamp: time.Now().Unix(),
		UnreadCount:           10,
		IsGroup:               true,
		NotSpam:               true,
	}

	fullChatSubject := fmt.Sprintf("%s.%s", SubjectMapping["chat_created"], tenantID)
	initialChatJSON, err := s.GenerateNatsPayload(fullChatSubject, initialChatOverrides)
	s.Require().NoError(err, "Failed to generate initial chat payload")

	// Publish initial chat
	err = s.PublishEvent(ctx, fullChatSubject, initialChatJSON)
	s.Require().NoError(err, "Failed to publish initial chat")

	// Wait for processing
	time.Sleep(2 * time.Second)

	// Verify initial chat was stored
	initialQuery := "SELECT COUNT(*) FROM chats WHERE chat_id = $1 AND company_id = $2 AND push_name = $3 AND unread_count = $4"
	exists, err := verifyPostgresDataWithSchema(ctx, s.PostgresDSN, tenantSchema, initialQuery, 1, chatID, tenantID, "Initial Customer", 10)
	s.Require().NoError(err, "Failed to verify initial chat exists")
	s.Require().True(exists, "Initial chat should exist with correct initial values")

	// --- Create partial update payload (using map for precise control) ---
	partialUpdatePayloadMap := map[string]interface{}{
		"id":                     chatID, // Field names MUST match the JSON schema exactly
		"company_id":             tenantID,
		"unread_count":           1,                 // Updating unread_count
		"conversation_timestamp": time.Now().Unix(), // Updating timestamp
	}

	// Publish partial update
	fullChatUpdateSubject := fmt.Sprintf("%s.%s", SubjectMapping["chat_updated"], tenantID)
	// Generate payload for update
	partialUpdateJSON, err := s.GenerateNatsPayload(fullChatUpdateSubject, partialUpdatePayloadMap)
	s.Require().NoError(err, "Failed to generate partial update payload")
	err = s.PublishEvent(ctx, fullChatUpdateSubject, partialUpdateJSON)
	s.Require().NoError(err, "Failed to publish partial chat update")

	// Wait for processing
	time.Sleep(2 * time.Second)

	// --- Verification ---
	verifyUpdateQuery := "SELECT COUNT(*) FROM chats WHERE chat_id = $1 AND company_id = $2 AND unread_count = 11 AND push_name = $3 AND is_group = $4"
	updatedAndPreserved, err := verifyPostgresDataWithSchema(ctx, s.PostgresDSN, tenantSchema, verifyUpdateQuery, 1, chatID, tenantID, "Initial Customer", true)
	s.Require().NoError(err, "Failed to verify partial update and preserved fields")
	s.Assert().True(updatedAndPreserved, "Unread count should be updated, and other fields (PushName, IsGroup) preserved")

	t.Log("Partial chat update test completed successfully")
}

// testPartialMessageUpdate tests that the service properly handles partial message updates.
// Test ID: PD-02
func (s *PartialDataTestSuite) TestPartialMessageUpdate() {
	t := s.T()
	ctx := s.Ctx
	tenantID := s.CompanyID
	tenantSchema := fmt.Sprintf("daisi_%s", tenantID)
	t.Log("Testing partial message update...")

	// Start streaming app container logs
	// s.StreamLogs(t)

	// Generate test IDs
	chatID := fmt.Sprintf("uuid:chat-msg-partial-%s", uuid.New().String())
	messageID := fmt.Sprintf("uuid:message-partial-%s", uuid.New().String())
	jid := "6281234567891@s.whatsapp.net"

	// First create a chat for the message to reference
	fullChatSubject := fmt.Sprintf("%s.%s", SubjectMapping["chat_created"], tenantID)
	chatOverrides := &model.UpsertChatPayload{
		ChatID:                chatID,
		CompanyID:             tenantID,
		Jid:                   jid,
		ConversationTimestamp: time.Now().Unix(),
		UnreadCount:           1,
	}
	chatJSON, err := s.GenerateNatsPayload(fullChatSubject, chatOverrides)
	s.Require().NoError(err, "Failed to generate chat payload")
	err = s.PublishEvent(ctx, fullChatSubject, chatJSON)
	s.Require().NoError(err, "Failed to publish chat")
	time.Sleep(1 * time.Second) // Shorter wait after chat creation

	// --- Create and publish initial complete message payload using fixture ---
	fullMessageCreateSubject := fmt.Sprintf("%s.%s", SubjectMapping["message_created"], tenantID)
	initialMessageOverrides := &model.UpsertMessagePayload{
		MessageID: messageID,
		CompanyID: tenantID,
		Jid:       jid,
		ChatID:    chatID,
		MessageObj: map[string]interface{}{
			"conversation": "Initial message content",
		},
		MessageTimestamp: time.Now().Unix(),
		FromPhone:        jid, // Assuming sender is the JID for simplicity. JSON key is "from_me" for boolean, or "from" for string sender id
		Status:           "sent",
		Flow:             "IN",
		// MessageType:       "text", // Add if present in UpsertMessagePayload
	}
	initialMessageJSON, err := s.GenerateNatsPayload(fullMessageCreateSubject, initialMessageOverrides)
	s.Require().NoError(err, "Failed to generate initial message payload")
	err = s.PublishEvent(ctx, fullMessageCreateSubject, initialMessageJSON)
	s.Require().NoError(err, "Failed to publish initial message")
	time.Sleep(2 * time.Second)

	// Verify initial message was stored
	initialMsgQuery := "SELECT COUNT(*) FROM messages WHERE message_id = $1 AND company_id = $2 AND status = $3"
	exists, err := verifyPostgresDataWithSchema(ctx, s.PostgresDSN, tenantSchema, initialMsgQuery, 1, messageID, tenantID, "sent")
	s.Require().NoError(err, "Failed to verify initial message exists")
	s.Require().True(exists, "Initial message should exist with status 'sent'")

	// --- Create partial message update payload (using map) ---
	partialMsgUpdatePayloadMap := map[string]interface{}{
		"id":         messageID, // Field names MUST match JSON schema
		"company_id": tenantID,
		// "jid":       jid,    // Not in UpdateMessagePayload
		// "chat_id":   chatID, // Not in UpdateMessagePayload
		"status": "read", // Only updating status
		// Missing: messageObj, messageTimestamp, from, flow etc.
	}

	// Publish partial update
	fullMessageUpdateSubject := fmt.Sprintf("%s.%s", SubjectMapping["message_updated"], tenantID)
	partialMsgUpdateJSON, err := s.GenerateNatsPayload(fullMessageUpdateSubject, partialMsgUpdatePayloadMap)
	s.Require().NoError(err, "Failed to generate partial message update payload")
	err = s.PublishEvent(ctx, fullMessageUpdateSubject, partialMsgUpdateJSON)
	s.Require().NoError(err, "Failed to publish partial message update")
	time.Sleep(2 * time.Second)

	// --- Verification ---
	verifyUpdateQuery := "SELECT COUNT(*) FROM messages WHERE message_id = $1 AND company_id = $2 AND status = $3 AND message_obj->>'conversation' = $4"
	updatedAndPreserved, err := verifyPostgresDataWithSchema(ctx, s.PostgresDSN, tenantSchema, verifyUpdateQuery, 1, messageID, tenantID, "read", "Initial message content")
	s.Require().NoError(err, "Failed to verify partial update and preserved fields")
	s.Assert().True(updatedAndPreserved, "Status should be updated to 'read', and message content preserved")

	t.Log("Partial message update test completed successfully")
}

// testEmptyFields tests that the service properly handles payloads with empty string values for non-required fields.
// Test ID: PD-03
func (s *PartialDataTestSuite) TestEmptyFields() {
	t := s.T()
	ctx := s.Ctx
	tenantID := s.CompanyID
	tenantSchema := fmt.Sprintf("daisi_%s", tenantID)
	t.Log("Testing empty fields in payloads...")

	// Start streaming app container logs
	// s.StreamLogs(t)

	// Generate test ID
	chatID := fmt.Sprintf("uuid:chat-empty-%s", uuid.New().String())
	jid := "6281234567892@s.whatsapp.net"

	// --- Create chat payload with empty strings using map[string]interface{} ---
	fullChatSubject := fmt.Sprintf("%s.%s", SubjectMapping["chat_created"], tenantID)
	chatOverrides := map[string]interface{}{
		"chat_id":                chatID,
		"company_id":             tenantID,
		"jid":                    jid,
		"push_name":              "", // Empty string
		"group_name":             "", // Empty string
		"conversation_timestamp": time.Now().Unix(),
		"unread_count":           3,
		"is_group":               false,
	}
	chatJSON, err := s.GenerateNatsPayload(fullChatSubject, chatOverrides)
	s.Require().NoError(err, "Failed to generate chat payload with empty fields")

	// Publish chat with empty fields
	err = s.PublishEvent(ctx, fullChatSubject, chatJSON)
	s.Require().NoError(err, "Failed to publish chat with empty fields")
	time.Sleep(2 * time.Second)

	// Verify the chat was stored correctly with empty/null fields
	query := "SELECT COUNT(*) FROM chats WHERE chat_id = $1 AND company_id = $2 AND (push_name = '' OR push_name IS NULL) AND (group_name = '' OR group_name IS NULL)"
	success, err := verifyPostgresDataWithSchema(ctx, s.PostgresDSN, tenantSchema, query, 1, chatID, tenantID)
	s.Require().NoError(err, "Failed to verify chat with empty fields")
	s.Assert().True(success, "Chat with empty fields should be processed correctly")

	// --- Now create a message with empty content using map[string]interface{} ---
	messageID := fmt.Sprintf("uuid:message-empty-%s", uuid.New().String())
	fullMessageCreateSubject := fmt.Sprintf("%s.%s", SubjectMapping["message_created"], tenantID)
	messageOverrides := map[string]interface{}{
		"message_id": messageID,
		"company_id": tenantID,
		"jid":        jid,
		"chat_id":    chatID,
		"message_obj": map[string]interface{}{
			"conversation": "", // Empty conversation
		},
		"message_timestamp": time.Now().Unix(),
		"from":              jid, // Assuming sender is JID, using snake_case "from"
		"status":            "sent",
		"flow":              "IN",
	}
	messageJSON, err := s.GenerateNatsPayload(fullMessageCreateSubject, messageOverrides)
	s.Require().NoError(err, "Failed to generate message payload with empty fields")

	// Publish message with empty fields
	err = s.PublishEvent(ctx, fullMessageCreateSubject, messageJSON)
	s.Require().NoError(err, "Failed to publish message with empty fields")
	time.Sleep(2 * time.Second)

	// --- Verification ---
	msgQuery := "SELECT COUNT(*) FROM messages WHERE message_id = $1 AND company_id = $2 AND (message_obj->>'conversation' = '' OR message_obj->>'conversation' IS NULL)"
	msgSuccess, err := verifyPostgresDataWithSchema(ctx, s.PostgresDSN, tenantSchema, msgQuery, 1, messageID, tenantID)
	s.Require().NoError(err, "Failed to verify message with empty content")
	s.Assert().True(msgSuccess, "Message with empty content should be processed correctly")

	t.Log("Empty fields test completed successfully")
}

// testIncompleteBatch tests that the service properly handles history batches with some invalid entries.
// Test ID: PD-05
func (s *PartialDataTestSuite) TestIncompleteBatch() {
	t := s.T()
	ctx := s.Ctx
	tenantID := s.CompanyID
	tenantSchema := fmt.Sprintf("daisi_%s", tenantID)
	t.Log("Testing incomplete history batch...")

	// Generate test IDs for valid chats
	validChat1ID := fmt.Sprintf("uuid:chat-batch-valid1-%s", uuid.New().String())
	validChat2ID := fmt.Sprintf("uuid:chat-batch-valid2-%s", uuid.New().String())
	convTs := time.Now().Unix()

	// --- Manually construct the JSON payload as a string ---
	historyBatchJSONString := fmt.Sprintf(`{
		"chats": [
			{ 
				"chat_id": "%s",
				"company_id": "%s",
				"jid": "6281234567894@s.whatsapp.net",
				"phone_number": "6281234567894",
				"conversation_timestamp": %d,
				"unread_count": 1,
				"is_group": false
			},
			{ 
				"chat_id": "invalid-missing-required-fields",
				"company_id": "%s",
				"conversation_timestamp": %d,
				"unread_count": 0,
				"is_group": false
				
			},
			{ 
				"chat_id": "%s",
				"company_id": "%s",
				"jid": "6281234567895@s.whatsapp.net",
				"phone_number": "6281234567894",
				"conversation_timestamp": %d,
				"unread_count": 2,
				"is_group": false
			}
		]
	}`, validChat1ID, tenantID, convTs, tenantID, convTs, validChat2ID, tenantID, convTs)

	historyBatchJSON := []byte(historyBatchJSONString)

	fullHistoryChatSubject := fmt.Sprintf("%s.%s", SubjectMapping["history_chat"], tenantID)

	// Publish the manually constructed batch without client-side validation
	err := s.PublishEventWithoutValidation(ctx, fullHistoryChatSubject, historyBatchJSON)
	s.Require().NoError(err, "Failed to publish incomplete batch using PublishEventWithoutValidation")

	time.Sleep(5 * time.Second) // Keep slightly longer wait for batch processing

	// Start streaming app container logs
	s.StreamLogs(t)

	// --- Verification ---
	// Verify the no chats were processed
	query1 := "SELECT COUNT(*) FROM chats WHERE chat_id = $1 AND company_id = $2"
	validChat1Stored, err := verifyPostgresDataWithSchema(ctx, s.PostgresDSN, tenantSchema, query1, 0, validChat1ID, tenantID)
	s.Require().NoError(err, "Failed to verify first valid chat")
	s.Assert().True(validChat1Stored, "First valid chat should not be processed and stored")

	query2 := "SELECT COUNT(*) FROM chats WHERE chat_id = $1 AND company_id = $2"
	validChat2Stored, err := verifyPostgresDataWithSchema(ctx, s.PostgresDSN, tenantSchema, query2, 0, validChat2ID, tenantID)
	s.Require().NoError(err, "Failed to verify second valid chat")
	s.Assert().True(validChat2Stored, "Second valid chat should not be processed and stored")

	// Verify the invalid chats were not stored
	queryInvalid1 := "SELECT COUNT(*) FROM chats WHERE chat_id = 'invalid-missing-required-fields' AND company_id = $1"
	invalidChat1NotStored, err := verifyPostgresDataWithSchema(ctx, s.PostgresDSN, tenantSchema, queryInvalid1, 0, tenantID)
	s.Require().NoError(err, "Failed to verify first invalid chat")
	s.Assert().True(invalidChat1NotStored, "First invalid chat (missing Jid) should not be processed")

	// Skipping verification for the second invalid chat due to marshalling difficulty of bad types.
	t.Log("Incomplete batch test completed successfully")
}
