package integration_test

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"gitlab.com/timkado/api/daisi-wa-events-processor/pkg/logger"
	"go.uber.org/zap/zaptest"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
)

// SemanticValidationTestSuite defines the suite for semantic data validation tests.
// It embeds E2EIntegrationTestSuite as it needs the app container running.
type SemanticValidationTestSuite struct {
	E2EIntegrationTestSuite
}

// TestRunner runs the semantic validation test suite
func TestSemanticValidationSuite(t *testing.T) {
	suite.Run(t, new(SemanticValidationTestSuite))
}

// SetupTest runs before each test in this suite.
func (s *SemanticValidationTestSuite) SetupTest() {
	s.E2EIntegrationTestSuite.SetupTest() // Run base setup (DB truncation)
	t := s.T()
	logger.Log = zaptest.NewLogger(t).Named("SemanticValidationTestSuite")
	t.Log("Waiting for daisi-wa-events-processor to potentially process previous test remnants...")
	time.Sleep(1 * time.Second) // Short delay before each test
}

// --- Test Cases (Converted from original functions) ---

// testInvalidTimestampFormat tests that the service properly handles events with invalid timestamp formats.
// Test ID: SM-01
func (s *SemanticValidationTestSuite) TestInvalidTimestampFormat() {
	t := s.T()
	ctx := s.Ctx
	tenantID := s.CompanyID
	tenantSchema := fmt.Sprintf("daisi_%s", tenantID)
	t.Log("Testing invalid timestamp format handling...")

	// Start streaming app container logs
	s.StreamLogs(t)

	// Create a chat payload with invalid timestamp format (Manual JSON needed for invalid type)
	invalidTimestampPayloadMap := map[string]interface{}{
		"chatID":                fmt.Sprintf("uuid:chat-invalid-ts-%s", uuid.New().String()),
		"companyID":             tenantID,
		"jid":                   "6281234567890@s.whatsapp.net",
		"pushName":              "Test Customer Invalid TS",
		"conversationTimestamp": "not-a-unix-timestamp", // Invalid format
		"unreadCount":           2,
		"isGroup":               true,
		"notSpam":               true,
	}
	invalidTimestampJSON, err := json.Marshal(invalidTimestampPayloadMap)
	s.Require().NoError(err, "Failed to marshal invalid timestamp payload map")

	// Publish the invalid timestamp payload
	fullChatSubject := fmt.Sprintf("%s.%s", SubjectMapping["chat_created"], tenantID)
	err = s.PublishEventWithoutValidation(ctx, fullChatSubject, invalidTimestampJSON)
	s.Assert().NoError(err, "Publishing payload with invalid timestamp format should succeed (NATS accepts bytes)")

	// Wait for processing (and potential rejection/logging)
	time.Sleep(3 * time.Second)

	// Verify the chat was NOT stored in PostgreSQL
	query := "SELECT COUNT(*) FROM chats WHERE chat_id = $1 AND company_id = $2"
	notStored, err := verifyPostgresDataWithSchema(ctx, s.PostgresDSN, tenantSchema, query, 0, invalidTimestampPayloadMap["chatID"], tenantID)
	s.Require().NoError(err, "Failed to verify PostgreSQL data")
	s.Assert().True(notStored, "Chat with invalid timestamp format should not be processed successfully")

	t.Log("Invalid timestamp format test completed")
}

// testFutureTimestamp tests that the service properly handles events with timestamps in the future.
// Test ID: SM-02
func (s *SemanticValidationTestSuite) TestFutureTimestamp() {
	t := s.T()
	ctx := s.Ctx
	tenantID := s.CompanyID
	tenantSchema := fmt.Sprintf("daisi_%s", tenantID)
	t.Log("Testing future timestamp handling...")

	// Start streaming app container logs
	s.StreamLogs(t)

	// Get a timestamp that's 1 year in the future
	futureTimestamp := time.Now().AddDate(1, 0, 0).Unix()
	chatID := fmt.Sprintf("uuid:chat-future-ts-%s", uuid.New().String())

	// Create a chat payload with future timestamp (Manual JSON needed)
	futureTimestampPayloadMap := map[string]interface{}{
		"chatID":                chatID,
		"companyID":             tenantID,
		"jid":                   "6281234567891@s.whatsapp.net",
		"pushName":              "Test Customer Future TS",
		"conversationTimestamp": futureTimestamp,
		"unreadCount":           1,
		"isGroup":               false,
		"notSpam":               true,
	}
	futureTimestampJSON, err := json.Marshal(futureTimestampPayloadMap)
	s.Require().NoError(err, "Failed to marshal future timestamp payload map")

	// Publish the future timestamp payload
	fullChatSubject := fmt.Sprintf("%s.%s", SubjectMapping["chat_created"], tenantID)
	err = s.PublishEventWithoutValidation(ctx, fullChatSubject, futureTimestampJSON)
	s.Assert().NoError(err, "Failed to publish payload with future timestamp")

	// Wait for processing
	time.Sleep(3 * time.Second)

	// Check if the service has handled the future timestamp
	// Note: The service might reject it or process it with a warning.
	// This test checks for REJECTION as the *stricter* behaviour.
	query := "SELECT COUNT(*) FROM chats WHERE chat_id = $1 AND company_id = $2"
	notStored, err := verifyPostgresDataWithSchema(ctx, s.PostgresDSN, tenantSchema, query, 0, chatID, tenantID)
	s.Require().NoError(err, "Failed to verify PostgreSQL data for future timestamp")
	s.Assert().True(notStored, "Service should ideally reject chat with future timestamp")
	// If the above assertion fails, it means the service stored it. Log this behaviour.
	if !notStored {
		t.Log("WARN: Service stored chat with future timestamp. Expected rejection, but processing is acceptable if logged.")
		// Consider adding log searching here if strict warning validation is needed.
	}

	t.Log("Future timestamp test completed")
}

// testInvalidReferences tests that the service properly handles messages referencing non-existent chats.
// Test ID: SM-04
func (s *SemanticValidationTestSuite) TestInvalidReferences() {
	t := s.T()
	ctx := s.Ctx
	tenantID := s.CompanyID
	tenantSchema := fmt.Sprintf("daisi_%s", tenantID)
	t.Log("Testing invalid references handling...")

	// Start streaming app container logs
	s.StreamLogs(t)

	// Generate a unique non-existent chat ID
	nonExistentChatID := fmt.Sprintf("uuid:non-existent-chat-%s", uuid.New().String())
	messageID := fmt.Sprintf("uuid:message-invalid-ref-%s", uuid.New().String())
	jid := "6281234567893@s.whatsapp.net"

	// First, verify the chat doesn't exist
	chatQuery := "SELECT COUNT(*) FROM chats WHERE chat_id = $1 AND company_id = $2"
	chatNotExists, err := verifyPostgresDataWithSchema(ctx, s.PostgresDSN, tenantSchema, chatQuery, 0, nonExistentChatID, tenantID)
	s.Require().NoError(err, "Failed to verify chat doesn't exist")
	s.Assert().True(chatNotExists, "Chat should not exist before test")

	// Create message referencing non-existent chat (Manual JSON needed)
	invalidRefPayloadMap := map[string]interface{}{
		"messageID": messageID,
		"companyID": tenantID,
		"chatID":    nonExistentChatID, // The invalid reference
		"jid":       jid,
		"messageObj": map[string]interface{}{
			"conversation": "This message references a non-existent chat",
		},
		"messageTimestamp": time.Now().Unix(),
		"from":             jid,
		"status":           "delivered",
		"flow":             "inbound",
	}
	invalidReferenceJSON, err := json.Marshal(invalidRefPayloadMap)
	s.Require().NoError(err, "Failed to marshal invalid reference payload map")

	// Publish the message with invalid reference
	fullMessageSubject := fmt.Sprintf("%s.%s", SubjectMapping["message_created"], tenantID)
	err = s.PublishEventWithoutValidation(ctx, fullMessageSubject, invalidReferenceJSON)
	s.Assert().NoError(err, "Failed to publish message with invalid reference")

	// Wait for processing
	time.Sleep(3 * time.Second)

	// Check if the service handled the invalid reference
	// Ideal behaviour: Message is rejected or stored in DLQ/error table.
	// Acceptable behaviour: Message is stored, but chat remains non-existent (might cause FK issues later).
	// Least desirable: Chat is auto-created (can hide upstream issues).

	// Check if message was stored
	messageQuery := "SELECT COUNT(*) FROM messages WHERE message_id = $1 AND company_id = $2"
	messageStored, err := verifyPostgresDataWithSchema(ctx, s.PostgresDSN, tenantSchema, messageQuery, 0, messageID, tenantID)
	s.Require().NoError(err, "Failed to query messages table")
	s.Assert().True(messageStored, "Message with invalid chat reference should NOT be stored successfully in messages table")

	// Re-verify chat doesn't exist (ensure no auto-creation)
	chatStillNotExists, err := verifyPostgresDataWithSchema(ctx, s.PostgresDSN, tenantSchema, chatQuery, 0, nonExistentChatID, tenantID)
	s.Require().NoError(err, "Failed to re-verify chat doesn't exist")
	s.Assert().True(chatStillNotExists, "Chat should NOT be auto-created when referenced by an invalid message")

	// If the message WAS stored (assertion failed), log a warning.
	if !messageStored {
		t.Log("WARN: Message was stored despite referencing a non-existent chat. Expected rejection.")
	}

	t.Log("Invalid references test completed")
}
