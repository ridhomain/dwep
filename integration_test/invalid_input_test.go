package integration_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"gitlab.com/timkado/api/daisi-wa-events-processor/pkg/logger"
	"go.uber.org/zap/zaptest"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
)

// InvalidInputTestSuite defines the suite for invalid input handling tests.
// It embeds E2EIntegrationTestSuite as it needs the app container running.
type InvalidInputTestSuite struct {
	E2EIntegrationTestSuite
}

// TestRunner runs the invalid input test suite
func TestInvalidInputSuite(t *testing.T) {
	suite.Run(t, new(InvalidInputTestSuite))
}

// SetupTest runs before each test in this suite.
func (s *InvalidInputTestSuite) SetupTest() {
	s.E2EIntegrationTestSuite.SetupTest() // Run base setup (DB truncation)
	t := s.T()
	logger.Log = zaptest.NewLogger(t).Named("InvalidInputTestSuite")
	t.Log("Waiting for daisi-wa-events-processor to potentially process previous test remnants...")
	time.Sleep(1 * time.Second) // Short delay before each test
}

// --- Test Cases (Converted from original functions) ---

// testMalformedJSON tests that the service properly handles malformed JSON payloads.
// Test ID: IV-01
func (s *InvalidInputTestSuite) TestMalformedJSON() {
	t := s.T()
	ctx := s.Ctx
	tenantID := s.CompanyID
	tenantSchema := fmt.Sprintf("daisi_%s", tenantID)
	t.Log("Testing malformed JSON payload handling...")

	// Start streaming app container logs to capture error messages
	// s.StreamLogs(t) // StreamLogs is called by the test suite's BeforeTest

	// Create malformed JSON payload (invalid JSON syntax)
	chatID := fmt.Sprintf("uuid:chat-malformed-%s", uuid.New().String())
	malformedJSON := []byte(fmt.Sprintf(`{
		"chatID": "%s",
		"companyID": "%s",
		"jid": "6281234567890@s.whatsapp.net",
		"conversationTimestamp": %d,
		"unreadCount": 2, this is invalid JSON
		"isGroup": true
	}`, chatID, tenantID, time.Now().Unix()))

	// Verify database state before sending invalid payload
	query := "SELECT COUNT(*) FROM chats WHERE chat_id = $1 AND company_id = $2"
	beforeCount, err := verifyPostgresDataWithSchema(ctx, s.PostgresDSN, tenantSchema, query, 0, chatID, tenantID)
	s.Require().NoError(err, "Failed to verify initial PostgreSQL state")
	s.Assert().True(beforeCount, "Database should not contain the test chat before test")

	// Publish the malformed JSON payload directly
	fullChatSubject := fmt.Sprintf("%s.%s", SubjectMapping["chat_created"], tenantID)
	err = s.PublishEventWithoutValidation(ctx, fullChatSubject, malformedJSON)
	s.Assert().NoError(err, "Publishing malformed JSON should succeed (NATS accepts bytes)")

	// Wait for processing (and expected rejection)
	time.Sleep(3 * time.Second)

	// Verify the chat was NOT stored in PostgreSQL (still doesn't exist)
	afterCount, err := verifyPostgresDataWithSchema(ctx, s.PostgresDSN, tenantSchema, query, 0, chatID, tenantID)
	s.Require().NoError(err, "Failed to verify PostgreSQL data after test")
	s.Assert().True(afterCount, "Chat with malformed JSON should not be processed successfully")

	// Rely on streamed logs for specific error details.
	t.Log("Malformed JSON payload test completed")
}

// testMissingRequiredFields tests that the service properly handles payloads with missing required fields.
// Test ID: IV-02
func (s *InvalidInputTestSuite) TestMissingRequiredFields() {
	t := s.T()
	ctx := s.Ctx
	tenantID := s.CompanyID
	// tenantSchema := fmt.Sprintf("daisi_%s", tenantID) // Schema not needed as nothing should be stored
	t.Log("Testing missing required fields in payload...")

	// Start streaming app container logs
	// s.StreamLogs(t)

	// Create JSON payload with missing required fields (e.g., missing chatID)
	missingFieldsPayloadMap := map[string]interface{}{
		// "chat_id": "MISSING", // Field name should be chat_id for map override if that's the model/JSON key
		"company_id":             tenantID,
		"jid":                    "6281234567891@s.whatsapp.net",
		"push_name":              "Test Missing Fields Cust",
		"conversation_timestamp": time.Now().Unix(),
		"unread_count":           1,
		"is_group":               false,
	}
	fullChatSubject := fmt.Sprintf("%s.%s", SubjectMapping["chat_created"], tenantID)

	missingFieldsJSON, err := json.Marshal(missingFieldsPayloadMap)
	s.Require().NoError(err, "Failed to marshal missingFieldsPayloadMap to JSON")

	// Publish the payload with missing required fields
	err = s.PublishEventWithoutValidation(ctx, fullChatSubject, missingFieldsJSON)
	s.Assert().NoError(err, "Publishing payload with missing fields should succeed (NATS accepts bytes)")

	// Wait for processing (and expected rejection)
	time.Sleep(3 * time.Second)

	// Since we're testing a negative case (validation error), there's no specific
	// database entry to check. We rely on the logs streamed to the test output.
	t.Log("Missing required fields test completed (verify logs for errors)")
}

// testInvalidFieldTypes tests that the service properly handles payloads with incorrect field types.
// Test ID: IV-03
func (s *InvalidInputTestSuite) TestInvalidFieldTypes() {
	t := s.T()
	ctx := s.Ctx
	tenantID := s.CompanyID
	tenantSchema := fmt.Sprintf("daisi_%s", tenantID)
	t.Log("Testing invalid field types in payload...")

	// Start streaming app container logs
	// s.StreamLogs(t)

	// Create JSON payload with invalid field types (unreadCount as string, isGroup as string)
	chatID := fmt.Sprintf("uuid:chat-invalid-types-%s", uuid.New().String())
	invalidTypesPayloadMap := map[string]interface{}{
		"chat_id":                chatID,
		"company_id":             tenantID,
		"jid":                    "6281234567892@s.whatsapp.net",
		"conversation_timestamp": "not-a-timestamp", // Invalid type
		"unread_count":           "two",             // Invalid type (should be number)
		"is_group":               "maybe",           // Invalid type (should be boolean)
	}
	fullChatSubject := fmt.Sprintf("%s.%s", SubjectMapping["chat_created"], tenantID)

	invalidTypesJSON, err := json.Marshal(invalidTypesPayloadMap)
	s.Require().NoError(err, "Failed to marshal invalidTypesPayloadMap to JSON")

	// Publish the payload with invalid field types
	err = s.PublishEventWithoutValidation(ctx, fullChatSubject, invalidTypesJSON)
	s.Assert().NoError(err, "Publishing payload with invalid field types should succeed (NATS accepts bytes)")

	// Wait for processing
	time.Sleep(3 * time.Second)

	// Verify the chat was NOT stored in PostgreSQL
	query := "SELECT COUNT(*) FROM chats WHERE chat_id = $1 AND company_id = $2"
	notStored, verifyErr := verifyPostgresDataWithSchema(ctx, s.PostgresDSN, tenantSchema, query, 0, chatID, tenantID)
	s.Require().NoError(verifyErr, "Failed to verify PostgreSQL data")
	s.Assert().True(notStored, "Chat with invalid field types should not be processed successfully")

	t.Log("Invalid field types test completed")
}

// testOversizedPayload tests that the service properly handles oversized payloads.
// Test ID: IV-04
func (s *InvalidInputTestSuite) TestOversizedPayload() {
	t := s.T()
	ctx := s.Ctx
	tenantID := s.CompanyID
	tenantSchema := fmt.Sprintf("daisi_%s", tenantID)
	t.Log("Testing oversized payload handling...")

	// Start streaming app container logs
	// s.StreamLogs(t)

	// Generate a large string for the payload (e.g., > 1MB which might exceed NATS default)
	// Note: This size might need adjustment based on actual NATS/service limits.
	largeString := generateLargeString(1024 * 1024 * 2) // ~2MB
	chatID := fmt.Sprintf("uuid:chat-oversized-%s", uuid.New().String())

	// Create oversized payload map
	oversizedPayloadMap := map[string]interface{}{
		"chat_id":                chatID,
		"company_id":             tenantID,
		"jid":                    "6281234567893@s.whatsapp.net",
		"conversation_timestamp": time.Now().Unix(),
		"unread_count":           1,
		"is_group":               false,
		"large_field":            largeString, // The oversized field
	}
	fullChatSubject := fmt.Sprintf("%s.%s", SubjectMapping["chat_created"], tenantID)

	oversizedJSON, err := json.Marshal(oversizedPayloadMap)
	s.Require().NoError(err, "Failed to marshal oversizedPayloadMap to JSON")

	// Try to publish the oversized payload
	err = s.PublishEventWithoutValidation(ctx, fullChatSubject, oversizedJSON)

	// We expect NATS to reject this with a specific error.
	if err != nil {
		t.Logf("Received expected error for oversized payload: %v", err)
		// Check if the error message indicates the size limit was exceeded.
		s.Assert().True(strings.Contains(err.Error(), "maximum payload exceeded") || strings.Contains(err.Error(), "message too large"),
			"Error message should indicate payload size limit exceeded, but got: %v", err)
	} else {
		t.Log("WARN: NATS did not reject oversized payload as expected. Service might handle it differently.")
		// If NATS accepted it, the service should ideally reject it upon processing.
		time.Sleep(3 * time.Second) // Wait for potential processing/rejection
	}

	// Verify the chat was NOT stored in PostgreSQL regardless of NATS behaviour
	query := "SELECT COUNT(*) FROM chats WHERE chat_id = $1 AND company_id = $2"
	notStored, verifyErr := verifyPostgresDataWithSchema(ctx, s.PostgresDSN, tenantSchema, query, 0, chatID, tenantID)
	s.Require().NoError(verifyErr, "Failed to verify PostgreSQL data after oversized payload test")
	s.Assert().True(notStored, "Chat with oversized payload should not be stored successfully")

	t.Log("Oversized payload test completed")
}

// testEmptyPayload tests that the service properly handles empty payloads.
// Test ID: IV-05
func (s *InvalidInputTestSuite) TestEmptyPayload() {
	t := s.T()
	ctx := s.Ctx
	tenantID := s.CompanyID
	t.Log("Testing empty payload handling...")

	// Start streaming app container logs
	// s.StreamLogs(t)

	// Create empty payload (zero bytes)
	emptyJSON := []byte{}

	// Publish the empty payload
	fullChatSubject := fmt.Sprintf("%s.%s", SubjectMapping["chat_created"], tenantID)
	err := s.PublishEventWithoutValidation(ctx, fullChatSubject, emptyJSON)
	s.Assert().NoError(err, "Publishing empty payload should succeed (NATS accepts zero bytes)")

	// Wait for processing (and expected rejection/logging)
	time.Sleep(3 * time.Second)

	// There's nothing specific to check in the database, as the empty payload
	// should be rejected by the service's input validation/unmarshalling.
	// Rely on streamed logs.
	t.Log("Empty payload test completed (verify logs for errors)")
}

// testInvalidChatSchema tests that the service properly handles chat payloads that violate the JSON schema.
// Test ID: SV-01
func (s *InvalidInputTestSuite) TestInvalidChatSchema() {
	t := s.T()
	ctx := s.Ctx
	tenantID := s.CompanyID
	tenantSchema := fmt.Sprintf("daisi_%s", tenantID)
	t.Log("Testing invalid chat schema handling...")

	// Start streaming app container logs
	// s.StreamLogs(t)

	// Create a chat payload with schema violations:
	chatID := fmt.Sprintf("uuid:chat-invalid-schema-%s", uuid.New().String())
	invalidSchemaPayloadMap := map[string]interface{}{
		"chat_id":                chatID,
		"company_id":             tenantID,
		"jid":                    12345,                  // Invalid type (should be string)
		"push_name":              map[string]int{"a": 1}, // Invalid type (should be string)
		"conversation_timestamp": "tomorrow",             // Invalid format
		"unread_count":           -10,                    // Invalid value (if restricted to >= 0)
		"is_group":               "yes",                  // Invalid enum/type
		"additional_field":       "not in schema",
	}
	fullChatSubject := fmt.Sprintf("%s.%s", SubjectMapping["chat_created"], tenantID)

	invalidSchemaJSON, err := json.Marshal(invalidSchemaPayloadMap)
	s.Require().NoError(err, "Failed to marshal invalidSchemaPayloadMap to JSON")

	// Publish the payload
	err = s.PublishEventWithoutValidation(ctx, fullChatSubject, invalidSchemaJSON)
	s.Assert().NoError(err, "Publishing invalid schema payload should succeed (NATS accepts bytes)")

	// Wait for processing (and expected rejection/logging)
	time.Sleep(3 * time.Second)

	// Verify the chat was NOT stored in PostgreSQL regardless of publish outcome
	query := "SELECT COUNT(*) FROM chats WHERE chat_id = $1 AND company_id = $2"
	notStored, verifyErr := verifyPostgresDataWithSchema(ctx, s.PostgresDSN, tenantSchema, query, 0, chatID, tenantID)
	s.Require().NoError(verifyErr, "Failed to verify PostgreSQL data after invalid schema test")
	s.Assert().True(notStored, "Chat with invalid schema should not be stored successfully")

	t.Log("Invalid chat schema test completed")
}

// testInvalidMessageSchema tests that the service properly handles message payloads that violate the JSON schema.
// Test ID: SV-02
func (s *InvalidInputTestSuite) TestInvalidMessageSchema() {
	t := s.T()
	ctx := s.Ctx
	tenantID := s.CompanyID
	tenantSchema := fmt.Sprintf("daisi_%s", tenantID)
	t.Log("Testing invalid message schema handling...")

	// Start streaming app container logs
	// s.StreamLogs(t)

	// Create a message payload with schema violations:
	messageID := fmt.Sprintf("uuid:message-invalid-schema-%s", uuid.New().String())
	chatID := fmt.Sprintf("uuid:chat-for-invalid-msg-%s", uuid.New().String()) // Need a chatID, even if it doesn't exist
	invalidSchemaPayloadMap := map[string]interface{}{
		"message_id":        messageID,
		"company_id":        tenantID,
		"chat_id":           chatID,
		"jid":               "6281234567895@s.whatsapp.net",
		"message_obj":       "not-an-object", // Invalid type
		"message_timestamp": -12345,          // Invalid value?
		"from_me":           true,            // Field name likely 'from_me' in model, or 'from' in JSON
		"status":            99,              // Invalid type/enum
		"flow":              "sideways",      // Invalid enum
		"extra_data":        "not in schema",
	}
	fullMessageSubject := fmt.Sprintf("%s.%s", SubjectMapping["message_created"], tenantID)

	invalidSchemaJSON, err := json.Marshal(invalidSchemaPayloadMap)
	s.Require().NoError(err, "Failed to marshal invalidSchemaPayloadMap for message to JSON")

	// Publish the payload
	err = s.PublishEventWithoutValidation(ctx, fullMessageSubject, invalidSchemaJSON)
	s.Assert().NoError(err, "Publishing invalid message schema payload should succeed (NATS accepts bytes)")

	// Wait for processing (and expected rejection/logging)
	time.Sleep(3 * time.Second)

	// Verify the message was NOT stored in PostgreSQL
	query := "SELECT COUNT(*) FROM messages WHERE message_id = $1 AND company_id = $2"
	notStored, verifyErr := verifyPostgresDataWithSchema(ctx, s.PostgresDSN, tenantSchema, query, 0, messageID, tenantID)
	s.Require().NoError(verifyErr, "Failed to verify PostgreSQL data after invalid message schema test")
	s.Assert().True(notStored, "Message with invalid schema should not be stored successfully")

	t.Log("Invalid message schema test completed")
}

// testInvalidHistorySchema tests that the service properly handles history payloads that violate the JSON schema.
// Test ID: SV-03
func (s *InvalidInputTestSuite) TestInvalidHistorySchema() {
	t := s.T()
	ctx := s.Ctx
	tenantID := s.CompanyID
	tenantSchema := fmt.Sprintf("daisi_%s", tenantID)
	t.Log("Testing invalid history schema handling...")

	// Start streaming app container logs
	// s.StreamLogs(t)

	// Create a history payload with invalid schema (e.g., top-level field incorrect type)
	invalidHistoryPayloadMap := map[string]interface{}{
		"chats":      "this should be an array", // Invalid type for the main list
		"company_id": tenantID,
	}

	fullHistorySubject := fmt.Sprintf("%s.%s", SubjectMapping["history_chat"], tenantID)
	// Expecting s.GenerateNatsPayload to fail schema validation
	// generateNatsPayload for history_chat expects HistoryChatPayload, which has a Chats field []UpsertChatPayload
	// So passing a map where "chats" is a string should fail validation within generateNatsPayload
	invalidHistoryJSON, err := json.Marshal(invalidHistoryPayloadMap)

	if err != nil {
		t.Logf("Received expected error during s.GenerateNatsPayload for history: %v", err)
		s.Assert().ErrorContains(err, "payload validation failed", "Error should be from schema validation within GenerateNatsPayload")
	} else {
		t.Log("WARN: s.GenerateNatsPayload did not return validation error for history. Attempting to publish.")
		publishErr := s.PublishEventWithoutValidation(ctx, fullHistorySubject, invalidHistoryJSON)
		s.Assert().NoError(publishErr, "Publishing should succeed if GenerateNatsPayload didn't error")
		time.Sleep(3 * time.Second)
	}

	// Verify no chats were added/modified unexpectedly
	query := "SELECT COUNT(*) FROM chats WHERE company_id = $1 AND chat_id LIKE 'uuid:chat-hist-invalid-%%'" // Check for specific IDs if needed
	notStored, verifyErr := verifyPostgresDataWithSchema(ctx, s.PostgresDSN, tenantSchema, query, 0, tenantID)
	s.Require().NoError(verifyErr, "Failed to verify PostgreSQL data after invalid history schema test")
	s.Assert().True(notStored, "No chats should be created/updated from an invalid history schema payload")

	t.Log("Invalid history schema test completed")
}

// testSchemaVersionMismatch tests that the service properly handles payloads sent to unsupported schema versions.
// Test ID: SV-04
func (s *InvalidInputTestSuite) TestSchemaVersionMismatch() {
	t := s.T()
	ctx := s.Ctx
	tenantID := s.CompanyID
	tenantSchema := fmt.Sprintf("daisi_%s", tenantID)
	t.Log("Testing schema version mismatch handling...")

	// Start streaming app container logs
	// s.StreamLogs(t)

	// Create a valid-looking payload map
	chatID := fmt.Sprintf("uuid:chat-version-mismatch-%s", uuid.New().String())
	versionMismatchPayloadMap := map[string]interface{}{
		"chat_id":                chatID,
		"company_id":             tenantID,
		"jid":                    "6281234567896@s.whatsapp.net",
		"conversation_timestamp": time.Now().Unix(),
		"unread_count":           3,
	}
	// Marshal directly as GenerateNatsPayload will fail due to unknown subject "v999..."
	versionMismatchJSON, err := json.Marshal(versionMismatchPayloadMap)
	s.Require().NoError(err, "Failed to marshal version mismatch payload map")

	// Publish to a non-existent version subject (e.g., v999)
	unsupportedVersionSubject := fmt.Sprintf("v999.chats.upsert.%s", tenantID)
	// Use s.PublishEvent directly
	err = s.PublishEventWithoutValidation(ctx, unsupportedVersionSubject, versionMismatchJSON)

	// For non-existent streams/subjects without consumers, JetStream publish might succeed
	// but the message won't be processed by our v1 handler.
	// Or it might fail if stream config prevents it.
	if err != nil {
		t.Logf("Received error publishing to unsupported subject (might be expected): %v", err)
		// Example: NATS error: NATS: API Error: Subject "v999.chats.upsert..." is not bound to stream "message_events_stream"
		// We don't strictly assert the error type, just log it.
	} else {
		t.Log("Publish to unsupported subject succeeded (NATS accepted). Verifying no processing.")
	}
	time.Sleep(3 * time.Second) // Allow time for potential (but unlikely) processing

	// Verify the chat was NOT stored in PostgreSQL
	query := "SELECT COUNT(*) FROM chats WHERE chat_id = $1 AND company_id = $2"
	notStored, verifyErr := verifyPostgresDataWithSchema(ctx, s.PostgresDSN, tenantSchema, query, 0, chatID, tenantID)
	s.Require().NoError(verifyErr, "Failed to verify PostgreSQL data after version mismatch test")
	s.Assert().True(notStored, "Chat published to unsupported version subject should not be stored")

	t.Log("Schema version mismatch test completed")
}

// Helper function to generate a large string
func generateLargeString(size int) string {
	// Create a string of the specified size
	chars := make([]byte, size)
	for i := 0; i < size; i++ {
		chars[i] = 'A' + byte(i%26)
	}
	return string(chars)
}

// countPostgresRows helper (can be moved to shared file)
func countPostgresRows(ctx context.Context, dsn string, query string, args ...interface{}) (int, error) {
	db, err := connectDB(dsn)
	if err != nil {
		return 0, fmt.Errorf("countPostgresRows: failed to connect to PostgreSQL: %w", err)
	}
	defer db.Close()

	var count int
	qCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	err = db.QueryRowContext(qCtx, query, args...).Scan(&count)
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, nil // No rows is not an error for counting
		}
		return 0, fmt.Errorf("countPostgresRows: failed to execute count query: %w. Query: %s, Args: %v", err, query, args)
	}
	return count, nil
}
