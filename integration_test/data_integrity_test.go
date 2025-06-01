package integration_test

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	"gitlab.com/timkado/api/daisi-wa-events-processor/pkg/logger"
	"go.uber.org/zap/zaptest"

	"github.com/stretchr/testify/suite"
)

// DataIntegrityTestSuite defines the suite for data integrity tests
// It embeds E2EIntegrationTestSuite as it needs the app container running.
type DataIntegrityTestSuite struct {
	E2EIntegrationTestSuite
}

// TestRunner runs the data integrity test suite
func TestDataIntegritySuite(t *testing.T) {
	suite.Run(t, new(DataIntegrityTestSuite))
}

// SetupTest runs before each test in this suite.
func (s *DataIntegrityTestSuite) SetupTest() {
	logger.Log = zaptest.NewLogger(s.T()).Named("DataIntegrityTestSuite")
	s.E2EIntegrationTestSuite.SetupTest() // Run base setup (DB truncation)
	// Add any specific setup for data integrity tests if needed
	t := s.T()
	t.Log("Waiting for daisi-wa-events-processor to potentially process previous test remnants...")
	time.Sleep(1 * time.Second) // Short delay before each test
}

// --- Test Cases (Converted from original functions) ---

// TestDuplicateMessageIDs tests that the service properly handles duplicate message IDs.
// Test ID: DI-01
func (s *DataIntegrityTestSuite) TestDuplicateMessageIDs() {
	t := s.T()
	ctx := s.Ctx // Use suite context
	t.Log("Testing duplicate message IDs handling...")

	// Start streaming app container logs
	// s.StreamLogs(t) // Use suite method

	// Generate test IDs
	chatID := fmt.Sprintf("uuid:chat-duplicate-%s", generateUUID())
	messageID := fmt.Sprintf("uuid:message-duplicate-%s", generateUUID())
	tenantID := s.CompanyID // Use suite CompanyID
	fullChatSubject := fmt.Sprintf("%s.%s", SubjectMapping["chat_created"], tenantID)
	fullMessageSubject := fmt.Sprintf("%s.%s", SubjectMapping["message_created"], tenantID)
	fullMessageUpdateSubject := fmt.Sprintf("%s.%s", SubjectMapping["message_updated"], tenantID)

	// First create a chat for the message to reference using generateNatsPayload
	chatOverrides := map[string]interface{}{
		"chat_id":                chatID, // Use model field name if generating model first
		"company_id":             tenantID,
		"jid":                    "6281234567890@s.whatsapp.net",
		"push_name":              "Duplicate Test Customer",
		"conversation_timestamp": time.Now().Unix(),
		"unread_count":           0,
		"is_group":               false,
		// Fields expected by UpsertChatPayload struct
	}
	chatJSON, err := s.GenerateNatsPayload(fullChatSubject, chatOverrides)
	s.Require().NoError(err, "Failed to generate chat payload")

	// Publish chat
	err = s.PublishEvent(ctx, fullChatSubject, chatJSON)
	s.Require().NoError(err, "Failed to publish chat")

	// Wait for processing
	time.Sleep(2 * time.Second)

	// Create an initial message payload using generateNatsPayload
	messageOverrides := map[string]interface{}{
		"message_id": messageID, // Use model field name
		"company_id": tenantID,
		"jid":        "6281234567890@s.whatsapp.net",
		"chat_id":    chatID,
		"message_obj": map[string]interface{}{
			"conversation": "Original message content",
		},
		"message_timestamp": time.Now().Unix(),
		"status":            "sent",
	}
	messageJSON, err := s.GenerateNatsPayload(fullMessageSubject, messageOverrides)
	s.Require().NoError(err, "Failed to generate message payload")

	// Publish the first instance of the message
	err = s.PublishEvent(ctx, fullMessageSubject, messageJSON)
	s.Require().NoError(err, "Failed to publish first message instance")

	// Wait for processing
	time.Sleep(2 * time.Second)

	// Verify the message was stored using suite DSN and company ID
	tenantSchema := fmt.Sprintf("daisi_%s", tenantID)
	query := fmt.Sprintf("SELECT COUNT(*) FROM messages WHERE message_id = $1 AND company_id = $2")
	messageExists, err := verifyPostgresDataWithSchema(ctx, s.PostgresDSN, tenantSchema, query, 1, messageID, tenantID)
	s.Require().NoError(err, "Failed to verify message exists")
	s.Assert().True(messageExists, "Original message should be stored")

	// Create a duplicate message payload with the same ID but different content
	duplicateOverrides := map[string]interface{}{
		"message_id": messageID, // Same ID
		"company_id": tenantID,
		"jid":        "6281234567890@s.whatsapp.net",
		"chat_id":    chatID,
		// "MessageType": "text",
		"message_obj": map[string]interface{}{
			"conversation": "Duplicate message content", // Different content
		},
		"message_timestamp": time.Now().Unix(), // Newer timestamp
		// "FromMe":           false,
		"status": "sent",
	}
	duplicateJSON, err := s.GenerateNatsPayload(fullMessageSubject, duplicateOverrides) // Still using upsert subject
	s.Require().NoError(err, "Failed to generate duplicate message payload")

	// Publish the duplicate message (as upsert)
	err = s.PublishEvent(ctx, fullMessageSubject, duplicateJSON)
	s.Require().NoError(err, "Failed to publish duplicate message")

	// Wait for processing
	time.Sleep(2 * time.Second)

	// Verify there's still only one message with that ID
	singleInstanceQuery := fmt.Sprintf("SELECT COUNT(*) FROM messages WHERE message_id = $1 AND company_id = $2")
	singleInstance, err := verifyPostgresDataWithSchema(ctx, s.PostgresDSN, tenantSchema, singleInstanceQuery, 1, messageID, tenantID)
	s.Require().NoError(err, "Failed to verify single message instance")
	s.Assert().True(singleInstance, "Should still have exactly one message with the given ID after duplicate upsert")

	// Verify which content was kept (implementation-dependent, but should be consistent)
	// Assuming the newer one overwrites (or upsert logic handles it)
	contentCheckQuery := fmt.Sprintf("SELECT COUNT(*) FROM messages WHERE message_id = $1 AND company_id = $2 AND message_obj->>'conversation' = $3")
	duplicateContentStored, err := verifyPostgresDataWithSchema(ctx, s.PostgresDSN, tenantSchema, contentCheckQuery, 1, messageID, tenantID, "Duplicate message content")
	s.Require().NoError(err, "Failed to check message content")
	s.Assert().True(duplicateContentStored, "Message content should be the 'duplicate' content after upsert")

	// Publish the duplicate again, but using the update subject this time
	updatePayloadOverrides := map[string]interface{}{
		"id":         messageID, // Corresponds to UpdateMessagePayload.ID
		"company_id": tenantID,
		"status":     "delivered", // New status
	}
	updateJSON, err := s.GenerateNatsPayload(fullMessageUpdateSubject, updatePayloadOverrides)
	s.Require().NoError(err, "Failed to generate update message payload")

	err = s.PublishEvent(ctx, fullMessageUpdateSubject, updateJSON)
	s.Require().NoError(err, "Failed to publish duplicate as update")

	// Wait for processing
	time.Sleep(2 * time.Second)

	// Verify there's still only one message
	singleAfterUpdateQuery := fmt.Sprintf("SELECT COUNT(*) FROM messages WHERE message_id = $1 AND company_id = $2")
	singleAfterUpdate, err := verifyPostgresDataWithSchema(ctx, s.PostgresDSN, tenantSchema, singleAfterUpdateQuery, 1, messageID, tenantID)
	s.Require().NoError(err, "Failed to verify single message after update")
	s.Assert().True(singleAfterUpdate, "Should still have exactly one message with the given ID after update")

	// Now verify that the update was applied (message_obj and status)
	// The contentCheckQuery above verifies the message_obj's conversation field.
	// For this specific update, "Updated via update subject" is not part of UpdateMessagePayload's fields.
	// So, we'll check if the status was updated, and the previous conversation content remains.
	statusUpdatedQuery := fmt.Sprintf("SELECT COUNT(*) FROM messages WHERE message_id = $1 AND company_id = $2 AND message_obj->>'conversation' = $3 AND status = $4")
	statusUpdated, err := verifyPostgresDataWithSchema(ctx, s.PostgresDSN, tenantSchema, statusUpdatedQuery, 1, messageID, tenantID, "Duplicate message content", "delivered")
	s.Require().NoError(err, "Failed to verify message status updated")
	s.Assert().True(statusUpdated, "Message status should be updated to 'delivered', and previous conversation content should remain")

	t.Log("Duplicate message IDs test completed successfully")
}

// TestConflictingUpdates tests how the service handles conflicting updates to the same record.
// Test ID: DI-04
func (s *DataIntegrityTestSuite) TestConflictingUpdates() {
	t := s.T()
	ctx := s.Ctx
	t.Log("Testing conflicting updates handling...")

	// Start streaming app container logs
	// s.StreamLogs(t)

	// Generate test IDs
	chatID := fmt.Sprintf("uuid:chat-conflict-%s", generateUUID())
	tenantID := s.CompanyID
	fullChatCreateSubject := fmt.Sprintf("%s.%s", SubjectMapping["chat_created"], tenantID)
	fullChatUpdateSubject := fmt.Sprintf("%s.%s", SubjectMapping["chat_updated"], tenantID)
	tenantSchema := fmt.Sprintf("daisi_%s", tenantID)

	// Create initial chat
	initialChatOverrides := map[string]interface{}{
		"chat_id":                chatID,
		"company_id":             tenantID,
		"jid":                    "6281234567890@s.whatsapp.net",
		"push_name":              "Initial Customer",
		"conversation_timestamp": time.Now().Unix(),
		"unread_count":           0,
		"is_group":               false,
	}
	initialChatJSON, err := s.GenerateNatsPayload(fullChatCreateSubject, initialChatOverrides)
	s.Require().NoError(err, "Failed to generate initial chat")

	// Publish initial chat
	err = s.PublishEvent(ctx, fullChatCreateSubject, initialChatJSON)
	s.Require().NoError(err, "Failed to publish initial chat")

	// Wait for processing
	time.Sleep(2 * time.Second)

	// Create first update with current timestamp
	firstUpdateTime := time.Now()
	firstUpdateOverrides := map[string]interface{}{
		"id":                     chatID, // Use UpdateChatPayload fields (ID)
		"company_id":             tenantID,
		"conversation_timestamp": firstUpdateTime.Unix(),
		"unread_count":           1,
	}
	firstUpdateJSON, err := s.GenerateNatsPayload(fullChatUpdateSubject, firstUpdateOverrides)
	s.Require().NoError(err, "Failed to generate first update")

	// Create second update with a timestamp slightly later
	secondUpdateTime := firstUpdateTime.Add(50 * time.Millisecond) // Ensure distinct timestamp
	secondUpdateOverrides := map[string]interface{}{
		"id":                     chatID,
		"company_id":             tenantID,
		"conversation_timestamp": secondUpdateTime.Unix(),
		"unread_count":           0, // This should be the final unread_count
	}
	secondUpdateJSON, err := s.GenerateNatsPayload(fullChatUpdateSubject, secondUpdateOverrides)
	s.Require().NoError(err, "Failed to generate second update")

	// Create third update with a timestamp slightly earlier than the second (to test out-of-order update processing)
	thirdUpdateTime := secondUpdateTime.Add(-10 * time.Millisecond)
	thirdUpdateOverrides := map[string]interface{}{
		"id":                     chatID,
		"company_id":             tenantID,
		"conversation_timestamp": thirdUpdateTime.Unix(),
		"unread_count":           1,
	}
	thirdUpdateJSON, err := s.GenerateNatsPayload(fullChatUpdateSubject, thirdUpdateOverrides)
	s.Require().NoError(err, "Failed to generate third update (earlier)")

	// Publish updates: first, then third (earlier than second), then second (latest)
	// This order simulates network latency or out-of-order arrival.
	err = s.PublishEvent(ctx, fullChatUpdateSubject, firstUpdateJSON)
	s.Require().NoError(err, "Failed to publish first update")

	err = s.PublishEvent(ctx, fullChatUpdateSubject, thirdUpdateJSON) // Publish 3rd (earlier ts) before 2nd (latest ts)
	s.Require().NoError(err, "Failed to publish third update (earlier)")

	err = s.PublishEvent(ctx, fullChatUpdateSubject, secondUpdateJSON)
	s.Require().NoError(err, "Failed to publish second update (latest)")

	// Wait for all updates to be processed
	time.Sleep(3 * time.Second)

	// Verify that the chat reflects the state of the LATEST update (secondUpdate)
	// based on conversation_timestamp, not arrival order.
	finalStateQuery := fmt.Sprintf("SELECT COUNT(*) FROM chats WHERE chat_id = $1 AND company_id = $2 AND unread_count = 2 AND conversation_timestamp = $3")
	finalStateCorrect, err := verifyPostgresDataWithSchema(ctx, s.PostgresDSN, tenantSchema, finalStateQuery, 1, chatID, tenantID, secondUpdateTime.Unix())
	s.Require().NoError(err, "Error querying final chat state")
	s.Assert().True(finalStateCorrect, "Chat should reflect the state from the latest timestamped update (unread_count=2)")

	t.Log("Conflicting updates test completed successfully")
}

// TestOrphanedReferences tests that the service properly handles messages that reference
// non-existent or deleted chats.
// Test ID: DI-04
func (s *DataIntegrityTestSuite) TestOrphanedReferences() {
	t := s.T()
	ctx := s.Ctx
	t.Log("Testing orphaned references handling...")

	// Start streaming app container logs
	// s.StreamLogs(t)

	// Generate test IDs
	chatID := fmt.Sprintf("uuid:chat-orphan-%s", generateUUID())
	chatToDeleteID := fmt.Sprintf("uuid:chat-to-delete-%s", generateUUID())
	messageID := fmt.Sprintf("uuid:message-orphan-%s", generateUUID())
	messageToOrphanID := fmt.Sprintf("uuid:message-will-orphan-%s", generateUUID())
	tenantID := s.CompanyID
	fullMessageSubject := fmt.Sprintf("%s.%s", SubjectMapping["message_created"], tenantID)
	fullChatSubject := fmt.Sprintf("%s.%s", SubjectMapping["chat_created"], tenantID)
	tenantSchema := fmt.Sprintf("daisi_%s", tenantID)

	// 1. First test: Create a message that references a non-existent chat
	orphanedMessageOverrides := map[string]interface{}{
		"message_id": messageID,
		"company_id": tenantID,
		"jid":        "6281234567890@s.whatsapp.net",
		"chat_id":    chatID, // This chat doesn't exist yet
		// "MessageType": "text",
		"message_obj": map[string]interface{}{
			"conversation": "Message with non-existent chat reference",
		},
		"message_timestamp": time.Now().Unix(),
		// "FromMe":           false,
		"status": "sent",
	}
	orphanedMessageJSON, err := s.GenerateNatsPayload(fullMessageSubject, orphanedMessageOverrides)
	s.Require().NoError(err, "Failed to generate orphaned message")

	// Publish orphaned message
	err = s.PublishEvent(ctx, fullMessageSubject, orphanedMessageJSON)
	s.Require().NoError(err, "Failed to publish orphaned message")

	// Wait for processing
	time.Sleep(2 * time.Second)

	// Verify the message was stored despite missing chat reference
	// The application might auto-create the chat or store the message with a null/invalid reference.
	// We check if the message exists.
	messageStoredQuery := fmt.Sprintf("SELECT COUNT(*) FROM messages WHERE message_id = $1 AND company_id = $2")
	messageStored, err := verifyPostgresDataWithSchema(ctx, s.PostgresDSN, tenantSchema, messageStoredQuery, 1, messageID, tenantID)
	s.Require().NoError(err, "Failed to verify orphaned message storage")
	s.Assert().True(messageStored, "Message with non-existent chat reference should be handled gracefully (stored)")

	// Optional: Verify if the chat was auto-created (depends on app logic)
	chatAutoCreatedQuery := fmt.Sprintf("SELECT COUNT(*) FROM chats WHERE chat_id = $1 AND company_id = $2")
	_, err = verifyPostgresDataWithSchema(ctx, s.PostgresDSN, tenantSchema, chatAutoCreatedQuery, -1, chatID, tenantID) // -1 means don't assert count
	if err == nil {
		t.Log("Chat might have been auto-created for orphaned message (verify if expected)")
	}

	// 2. Second test: Create a chat, add a message to it, then delete the chat
	// Create chat that will be deleted
	chatToDeleteOverrides := map[string]interface{}{
		"chat_id":                chatToDeleteID,
		"company_id":             tenantID,
		"jid":                    "6281234567891@s.whatsapp.net",
		"conversation_timestamp": time.Now().Unix(),
		"unread_count":           0,
		"is_group":               false,
	}
	chatToDeleteJSON, err := s.GenerateNatsPayload(fullChatSubject, chatToDeleteOverrides)
	s.Require().NoError(err, "Failed to generate chat to delete")

	// Publish chat
	err = s.PublishEvent(ctx, fullChatSubject, chatToDeleteJSON)
	s.Require().NoError(err, "Failed to publish chat to delete")

	// Wait for processing
	time.Sleep(2 * time.Second)

	// Add message to the chat
	messageToOrphanOverrides := map[string]interface{}{
		"message_id": messageToOrphanID,
		"company_id": tenantID,
		"jid":        "6281234567891@s.whatsapp.net",
		"chat_id":    chatToDeleteID,
		// "MessageType": "text",
		"message_obj": map[string]interface{}{
			"conversation": "Message that will become orphaned",
		},
		"message_timestamp": time.Now().Unix(),
		// "FromMe":           false,
		"status": "sent",
	}
	messageToOrphanJSON, err := s.GenerateNatsPayload(fullMessageSubject, messageToOrphanOverrides)
	s.Require().NoError(err, "Failed to generate message to orphan")

	// Publish message
	err = s.PublishEvent(ctx, fullMessageSubject, messageToOrphanJSON)
	s.Require().NoError(err, "Failed to publish message to orphan")

	// Wait for processing
	time.Sleep(2 * time.Second)

	// Verify both chat and message exist
	chatExistsQuery := fmt.Sprintf("SELECT COUNT(*) FROM chats WHERE chat_id = $1 AND company_id = $2")
	chatExists, err := verifyPostgresDataWithSchema(ctx, s.PostgresDSN, tenantSchema, chatExistsQuery, 1, chatToDeleteID, tenantID)
	s.Require().NoError(err, "Failed to verify chat exists")
	s.Assert().True(chatExists, "Chat should exist before deletion")

	messageExistsQuery := fmt.Sprintf("SELECT COUNT(*) FROM messages WHERE message_id = $1 AND company_id = $2")
	messageExists, err := verifyPostgresDataWithSchema(ctx, s.PostgresDSN, tenantSchema, messageExistsQuery, 1, messageToOrphanID, tenantID)
	s.Require().NoError(err, "Failed to verify message exists")
	s.Assert().True(messageExists, "Message should exist before chat deletion")

	// Now delete the chat using a direct SQL query (assuming no NATS delete event)
	deleteQuery := fmt.Sprintf("DELETE FROM chats WHERE chat_id = $1 AND company_id = $2")
	err = executeNonQuerySQLWithSchema(ctx, s.PostgresDSN, tenantSchema, deleteQuery, chatToDeleteID, tenantID)
	s.Require().NoError(err, "Failed to delete chat via SQL")

	// Wait for possible cascading effects or application handling
	time.Sleep(2 * time.Second)

	// Verify chat is gone
	chatGoneQuery := fmt.Sprintf("SELECT COUNT(*) FROM chats WHERE chat_id = $1 AND company_id = $2")
	chatGone, err := verifyPostgresDataWithSchema(ctx, s.PostgresDSN, tenantSchema, chatGoneQuery, 0, chatToDeleteID, tenantID)
	s.Require().NoError(err, "Failed to verify chat deletion")
	s.Assert().True(chatGone, "Chat should be deleted")

	// Verify message still exists or was handled gracefully (e.g., chat_id set to NULL)
	messageAfterDeletionQuery := fmt.Sprintf("SELECT chat_id FROM messages WHERE message_id = $1 AND company_id = $2")
	var orphanedChatID sql.NullString
	err = executeQueryWIthSchemaRowScan(ctx, s.PostgresDSN, tenantSchema, messageAfterDeletionQuery, []interface{}{&orphanedChatID}, messageToOrphanID, tenantID)
	s.Require().NoError(err, "Failed to query message after chat deletion")
	// Depending on DB schema (ON DELETE SET NULL or RESTRICT), check the result
	// If RESTRICT, the DELETE query would likely have failed earlier.
	// If SET NULL, check if orphanedChatID.Valid is false.
	// If message was deleted (CASCADE), the queryRowScan would return sql.ErrNoRows.
	t.Logf("Message's chat_id after parent deletion: Valid=%v, String='%s' (check if consistent with schema)", orphanedChatID.Valid, orphanedChatID.String)

	t.Log("Orphaned references test completed successfully")
}

// TestDataConsistency tests that data is consistent across PostgreSQL after complex ops.
// Test ID: DI-05
func (s *DataIntegrityTestSuite) TestDataConsistency() {
	t := s.T()
	ctx := s.Ctx
	t.Log("Testing data consistency across databases...")

	// Start streaming app container logs
	// s.StreamLogs(t)

	// Generate unique IDs for this test
	chatID := fmt.Sprintf("uuid:chat-consistency-%s", generateUUID())
	messageID := fmt.Sprintf("uuid:message-consistency-%s", generateUUID())
	tenantID := s.CompanyID
	fullChatCreateSubject := fmt.Sprintf("%s.%s", SubjectMapping["chat_created"], tenantID)
	fullChatUpdateSubject := fmt.Sprintf("%s.%s", SubjectMapping["chat_updated"], tenantID)
	fullMessageCreateSubject := fmt.Sprintf("%s.%s", SubjectMapping["message_created"], tenantID)
	fullMessageUpdateSubject := fmt.Sprintf("%s.%s", SubjectMapping["message_updated"], tenantID)
	tenantSchema := fmt.Sprintf("daisi_%s", tenantID)

	// 1. Create chat
	t.Log("Step 1: Creating chat")
	chatOverrides := map[string]interface{}{
		"chat_id":                chatID,
		"company_id":             tenantID,
		"jid":                    "6281234567899@s.whatsapp.net",
		"push_name":              "Consistency Test User",
		"conversation_timestamp": time.Now().Unix(),
		"unread_count":           0,
		"is_group":               false,
	}
	chatJSON, err := s.GenerateNatsPayload(fullChatCreateSubject, chatOverrides)
	s.Require().NoError(err, "Failed to generate chat")
	err = s.PublishEvent(ctx, fullChatCreateSubject, chatJSON)
	s.Require().NoError(err, "Failed to publish chat")
	time.Sleep(2 * time.Second)

	// 2. Create message
	t.Log("Step 2: Creating message")
	messageOverrides := map[string]interface{}{
		"message_id": messageID,
		"company_id": tenantID,
		"jid":        "6281234567899@s.whatsapp.net",
		"chat_id":    chatID,
		"message_obj": map[string]interface{}{
			"conversation": "Consistency test message",
		},
		"message_timestamp": time.Now().Unix(),
		"status":            "sent",
	}
	messageJSON, err := s.GenerateNatsPayload(fullMessageCreateSubject, messageOverrides)
	s.Require().NoError(err, "Failed to generate message")
	err = s.PublishEvent(ctx, fullMessageCreateSubject, messageJSON)
	s.Require().NoError(err, "Failed to publish message")
	time.Sleep(2 * time.Second)

	// 3. Update chat
	t.Log("Step 3: Updating chat")
	chatUpdateOverrides := map[string]interface{}{
		"id":                     chatID, // For UpdateChatPayload
		"company_id":             tenantID,
		"conversation_timestamp": time.Now().Unix(),
		"unread_count":           1,
	}
	chatUpdateJSON, err := s.GenerateNatsPayload(fullChatUpdateSubject, chatUpdateOverrides)
	s.Require().NoError(err, "Failed to generate chat update")
	err = s.PublishEvent(ctx, fullChatUpdateSubject, chatUpdateJSON)
	s.Require().NoError(err, "Failed to publish chat update")
	time.Sleep(2 * time.Second)

	// 4. Update message
	t.Log("Step 4: Updating message")
	messageUpdateOverrides := map[string]interface{}{
		"id":         messageID, // For UpdateMessagePayload
		"company_id": tenantID,
		"status":     "read",
	}
	messageUpdateJSON, err := s.GenerateNatsPayload(fullMessageUpdateSubject, messageUpdateOverrides)
	s.Require().NoError(err, "Failed to generate message update")
	err = s.PublishEvent(ctx, fullMessageUpdateSubject, messageUpdateJSON)
	s.Require().NoError(err, "Failed to publish message update")
	time.Sleep(2 * time.Second)

	// 5. Verify data in PostgreSQL
	t.Log("Step 5: Verifying data in PostgreSQL")
	chatPostgresQuery := fmt.Sprintf("SELECT COUNT(*) FROM chats WHERE chat_id = $1 AND company_id = $2 AND unread_count = 1")
	chatPostgresOK, err := verifyPostgresDataWithSchema(ctx, s.PostgresDSN, tenantSchema, chatPostgresQuery, 1, chatID, tenantID)
	s.Require().NoError(err, "Failed to verify chat in PostgreSQL")
	s.Assert().True(chatPostgresOK, "Chat in PostgreSQL should be consistent with updates")

	messagePostgresQuery := fmt.Sprintf("SELECT COUNT(*) FROM messages WHERE message_id = $1 AND company_id = $2 AND status = 'read'")
	messagePostgresOK, err := verifyPostgresDataWithSchema(ctx, s.PostgresDSN, tenantSchema, messagePostgresQuery, 1, messageID, tenantID)
	s.Require().NoError(err, "Failed to verify message in PostgreSQL")
	s.Assert().True(messagePostgresOK, "Message in PostgreSQL should be consistent with updates")

	// 7. Make one more update to ensure consistency is maintained
	t.Log("Step 6: Making final update")
	finalUpdateOverrides := map[string]interface{}{
		"id":         messageID, // For UpdateMessagePayload
		"company_id": tenantID,
		"status":     "delivered",
	}
	finalUpdateJSON, err := s.GenerateNatsPayload(fullMessageUpdateSubject, finalUpdateOverrides)
	s.Require().NoError(err, "Failed to generate final update")
	err = s.PublishEvent(ctx, fullMessageUpdateSubject, finalUpdateJSON)
	s.Require().NoError(err, "Failed to publish final update")
	time.Sleep(2 * time.Second)

	// 7. Verify final state in PostgreSQL
	t.Log("Step 7: Verifying final state")
	finalStateQuery := fmt.Sprintf("SELECT COUNT(*) FROM messages WHERE message_id = $1 AND company_id = $2 AND status = 'delivered'")
	finalStateOK, err := verifyPostgresDataWithSchema(ctx, s.PostgresDSN, tenantSchema, finalStateQuery, 1, messageID, tenantID)
	s.Require().NoError(err, "Failed to verify final state")
	s.Assert().True(finalStateOK, "Final message state should be consistent in PostgreSQL")

	t.Log("Data consistency test completed successfully")
}

// TestMismatchedTenantData tests that data intended for one tenant is not processed for another.
// Test ID: DI-03
func (s *DataIntegrityTestSuite) TestMismatchedTenantData() {
	t := s.T()
	ctx := s.Ctx
	t.Log("Testing mismatched tenant data handling...")

	// Start streaming app container logs
	// s.StreamLogs(t)

	// Define two tenant IDs for this test
	primaryTenantID := s.CompanyID                                // The tenant the suite is primarily configured for
	secondaryTenantID := "secondary-company-id-" + generateUUID() // A different tenant ID
	t.Logf("Primary Tenant ID: %s", primaryTenantID)
	t.Logf("Secondary Tenant ID: %s", secondaryTenantID)

	// Create schema names for verification
	primaryTenantSchema := fmt.Sprintf("daisi_%s", primaryTenantID)
	// We won't have direct access to secondaryTenantSchema through suite helpers
	// so we might need to construct queries carefully or assume it doesn't exist/get created.

	// Generate test IDs
	chatID := fmt.Sprintf("uuid:chat-mismatch-%s", generateUUID())

	// 1. Create a chat payload intended for the *secondaryTenantID* but publish to *primaryTenantID* subject
	mismatchedPayloadOverrides := map[string]interface{}{
		"chat_id":                chatID,
		"company_id":             secondaryTenantID, // Payload says secondary tenant
		"jid":                    "628999999999@s.whatsapp.net",
		"conversation_timestamp": time.Now().Unix(),
		"unread_count":           1,
	}
	// The subject will be for primaryTenantID
	primarySubject := fmt.Sprintf("%s.%s", SubjectMapping["chat_created"], primaryTenantID)
	mismatchedJSON, err := s.GenerateNatsPayload(primarySubject, mismatchedPayloadOverrides)
	// Note: GenerateNatsPayload might inject primaryTenantID into company_id based on subject if not careful.
	// We need to ensure our test override for company_id is respected or the test is invalid.
	// For this test, let's assume GenerateNatsPayload uses the map override if company_id is present.
	s.Require().NoError(err, "Failed to generate mismatched payload for primary subject")

	// Publish to primary tenant's subject, but payload internally specifies secondaryTenantID
	err = s.PublishEvent(ctx, primarySubject, mismatchedJSON)
	s.Require().NoError(err, "Failed to publish mismatched payload to primary subject")

	// Wait for processing
	time.Sleep(3 * time.Second)

	// Verification:
	// A) The data SHOULD NOT be in primaryTenantID's schema because the service should respect company_id in payload.
	//    Or, if the service prioritizes subject, this test becomes about that behavior.
	//    Assuming payload `company_id` is authoritative for routing within the handler logic if it differs from subject's tenant.
	queryPrimary := fmt.Sprintf("SELECT COUNT(*) FROM chats WHERE chat_id = $1 AND company_id = $2")
	rowsInPrimary, err := verifyPostgresDataWithSchema(ctx, s.PostgresDSN, primaryTenantSchema, queryPrimary, 0, chatID, secondaryTenantID)
	s.Require().NoError(err, "Error querying primary tenant schema for mismatched data (expecting 0 for secondaryTenantID)")
	s.Assert().True(rowsInPrimary, "Data with secondaryTenantID in payload should NOT be in primary tenant's schema when published to primary subject")

	rowsInPrimaryForPrimaryID, err := verifyPostgresDataWithSchema(ctx, s.PostgresDSN, primaryTenantSchema, queryPrimary, 0, chatID, primaryTenantID)
	s.Require().NoError(err, "Error querying primary tenant schema for mismatched data (expecting 0 for primaryTenantID)")
	s.Assert().True(rowsInPrimaryForPrimaryID, "Data should also not be in primary tenant's schema with primaryTenantID due to payload mismatch logic")

	// B) The data SHOULD be in secondaryTenantID's schema if the service correctly routed it based on payload.
	//    This requires the service to dynamically handle schemas or for the `secondaryTenantID` to have been onboarded.
	//    For this test, we assume the service *should* process it into the correct tenant schema if that schema exists.
	//    If `secondaryTenantID` schema doesn't exist, it might go to DLQ or be an error.
	//    Let's assume for now the service is expected to handle this gracefully (e.g., create schema or error out, not cross-contaminate).

	//    To verify this part robustly, we would need a way to check the secondary tenant's schema.
	//    Since E2EIntegrationTestSuite is tied to one CompanyID, directly using its helpers for secondaryTenantID is not straightforward.
	//    We might need a raw query to a hypothetical secondary schema.
	//    Given the constraints, we will focus on ensuring NO contamination in the primary schema.
	t.Logf("Verification focused on non-contamination of primary tenant (%s). Further checks for secondary tenant (%s) would require different setup.", primaryTenantID, secondaryTenantID)

	// 2. Create a chat payload intended for *primaryTenantID* and publish to *secondaryTenantID* subject.
	correctPayloadOverrides := map[string]interface{}{
		"chat_id":                chatID,          // Can reuse chatID for this part, ensure it's cleaned up
		"company_id":             primaryTenantID, // Payload for primary tenant
		"jid":                    "628111111111@s.whatsapp.net",
		"conversation_timestamp": time.Now().Unix(),
		"unread_count":           2,
	}
	// The subject will be for secondaryTenantID
	secondarySubject := fmt.Sprintf("%s.%s", SubjectMapping["chat_created"], secondaryTenantID)
	correctPayloadForSecondarySubjectJSON, err := s.GenerateNatsPayload(secondarySubject, correctPayloadOverrides)
	s.Require().NoError(err, "Failed to generate correct payload for secondary subject")

	// Publish to secondary tenant's subject, payload internally specifies primaryTenantID
	err = s.PublishEvent(ctx, secondarySubject, correctPayloadForSecondarySubjectJSON)
	s.Require().NoError(err, "Failed to publish correct payload to secondary subject")

	// Wait for processing
	time.Sleep(3 * time.Second)

	// Verification:
	// A) The data SHOULD NOT be in primaryTenantID's schema if subject-based routing is strict for consumers.
	//    If the consumer for `secondarySubject` processes it, it should go to `secondaryTenantID` data store.
	queryPrimaryAgain := fmt.Sprintf("SELECT COUNT(*) FROM chats WHERE chat_id = $1 AND company_id = $2")
	rowsInPrimaryAfterSecondaryPublish, err := verifyPostgresDataWithSchema(ctx, s.PostgresDSN, primaryTenantSchema, queryPrimaryAgain, 0, chatID, primaryTenantID)
	s.Require().NoError(err, "Error querying primary tenant schema after publishing to secondary subject")
	s.Assert().True(rowsInPrimaryAfterSecondaryPublish, "Data for primaryTenantID in payload, published to secondary subject, should NOT be in primary tenant's schema if consumer is tenant-specific")

	t.Log("Mismatched tenant data test completed successfully. Focus was on non-contamination of the primary test tenant.")
}
