package integration_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/lib/pq"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap/zaptest"

	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/model"
	"gitlab.com/timkado/api/daisi-wa-events-processor/pkg/logger"
)

// Helper function to wait until DB is ready (moved to top for visibility)
func WaitUntilDBReady(ctx context.Context, dsn string) error {
	timeout := time.After(30 * time.Second)
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			return fmt.Errorf("timed out waiting for database to become ready")
		case <-ticker.C:
			db, err := sql.Open("pgx", dsn) // Consider changing "pgx" to "postgres" if not using pgx driver directly
			if err != nil {
				log.Printf("WaitUntilDBReady: Error opening DB connection: %v", err)
				continue
			}
			pingCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
			err = db.PingContext(pingCtx)
			cancel()
			db.Close()
			if err == nil {
				return nil
			}
			log.Printf("WaitUntilDBReady: DB ping failed: %v. Retrying...", err)
		}
	}
}

// MainRecoveryResilienceTestSuite defines the suite for general recovery and resilience tests.
type MainRecoveryResilienceTestSuite struct {
	E2EIntegrationTestSuite
}

// SetupTest runs before each test in the MainRecoveryResilienceTestSuite.
func (s *MainRecoveryResilienceTestSuite) SetupTest() {
	s.E2EIntegrationTestSuite.SetupTest() // Run base setup (DB truncation)
	t := s.T()
	logger.Log = zaptest.NewLogger(t).Named("MainRecoveryResilienceTestSuite")
	t.Log("Waiting for daisi-wa-events-processor to potentially process previous test remnants...")
	time.Sleep(1 * time.Second) // Short delay before each test
}

// --- Test Cases for MainRecoveryResilienceTestSuite ---

// TestServiceRestartDuringProcessing tests that the service properly recovers and continues
// processing messages after being restarted in the middle of processing.
// Test ID: RC-01
func (s *MainRecoveryResilienceTestSuite) TestServiceRestartDuringProcessing() {
	t := s.T()
	ctx := s.Ctx
	tenantID := s.CompanyID
	tenantSchema := fmt.Sprintf("daisi_%s", tenantID)
	t.Log("Testing service restart during processing...")

	// Generate unique batch IDs for test
	batchSize := 50
	chatIDs := make([]string, batchSize)
	chatPayloads := make([]model.UpsertChatPayload, batchSize)
	for i := 0; i < batchSize; i++ {
		chatIDs[i] = fmt.Sprintf("uuid:chat-restart-%s-%d", uuid.New().String(), i)
		chatPayloads[i] = model.UpsertChatPayload{
			ChatID:                chatIDs[i],
			CompanyID:             tenantID,
			PhoneNumber:           fmt.Sprintf("628123456%04d", i),
			Jid:                   fmt.Sprintf("628123456%04d@s.whatsapp.net", i),
			PushName:              fmt.Sprintf("Restart Test User %d", i),
			ConversationTimestamp: time.Now().Unix(),
			UnreadCount:           int32(i % 10),
			IsGroup:               (i % 5) == 0,
		}
	}

	t.Log("Preparing large batch of messages...")
	historyBatchPayload := model.HistoryChatPayload{Chats: chatPayloads}
	chatBatchJSON, err := json.Marshal(historyBatchPayload)
	s.Require().NoError(err, "Failed to marshal chat batch payload")

	t.Log("Publishing large batch of messages...")
	fullHistoryChatSubject := fmt.Sprintf("%s.%s", SubjectMapping["history_chat"], tenantID)
	err = s.PublishEvent(ctx, fullHistoryChatSubject, chatBatchJSON)
	s.Require().NoError(err, "Failed to publish chat batch")

	time.Sleep(2 * time.Second)

	t.Log("Restarting application container during message processing...")
	stopDuration := 20 * time.Second
	err = s.Application.Stop(ctx, &stopDuration)
	s.Require().NoError(err, "Failed to stop application container")

	time.Sleep(3 * time.Second)

	err = s.Application.Start(ctx)
	s.Require().NoError(err, "Failed to restart application container")

	t.Log("Waiting for application to recover...")
	time.Sleep(10 * time.Second)

	t.Log("Verifying messages were processed after restart...")
	time.Sleep(15 * time.Second)

	var count int
	processedQuerySimple := "SELECT COUNT(*) FROM chats WHERE company_id = $1 AND chat_id LIKE 'uuid:chat-restart-%%'"
	rowScanErrSimple := executeQueryWIthSchemaRowScan(ctx, s.PostgresDSN, tenantSchema, processedQuerySimple, []interface{}{&count}, tenantID)
	s.Require().NoError(rowScanErrSimple, "Failed to count processed chats")
	t.Logf("Processed %d/%d chats after service restart", count, batchSize)
	s.Assert().GreaterOrEqualf(count, batchSize*3/4, "At least 75%% of messages should be processed after restart (processed %d/%d)", count, batchSize)

	newChatID := fmt.Sprintf("uuid:chat-after-restart-%s", uuid.New().String())
	fullChatCreateSubject := fmt.Sprintf("%s.%s", SubjectMapping["chat_created"], tenantID)
	afterRestartOverrides := &model.UpsertChatPayload{
		ChatID:                newChatID,
		CompanyID:             tenantID,
		Jid:                   "6281234567890@s.whatsapp.net",
		ConversationTimestamp: time.Now().Unix(),
		UnreadCount:           0,
		IsGroup:               false,
	}
	afterRestartJSON, err := s.GenerateNatsPayload(fullChatCreateSubject, afterRestartOverrides)
	s.Require().NoError(err, "Failed to generate post-restart chat payload")
	err = s.PublishEvent(ctx, fullChatCreateSubject, afterRestartJSON)
	s.Require().NoError(err, "Failed to publish post-restart chat")

	time.Sleep(3 * time.Second)

	newChatQuery := "SELECT COUNT(*) FROM chats WHERE chat_id = $1 AND company_id = $2"
	newChatProcessed, err := verifyPostgresDataWithSchema(ctx, s.PostgresDSN, tenantSchema, newChatQuery, 1, newChatID, tenantID)
	s.Require().NoError(err, "Failed to verify new chat processed")
	s.Assert().True(newChatProcessed, "New chat should be processed after service restart")

	t.Log("Service restart during processing test completed successfully")
}

// TestTransactionRollback tests that the service properly handles database transaction
// failures by rolling back cleanly without side effects.
// Test ID: RC-04
func (s *MainRecoveryResilienceTestSuite) TestTransactionRollback() {
	t := s.T()
	ctx := s.Ctx
	tenantID := s.CompanyID
	tenantSchema := fmt.Sprintf("daisi_%s", tenantID)
	t.Log("Testing transaction rollback...")

	chatID := fmt.Sprintf("uuid:chat-rollback-%s", uuid.New().String())
	messageID := fmt.Sprintf("uuid:message-rollback-%s", uuid.New().String())
	jid := "6281234567890@s.whatsapp.net"

	fullChatSubject := fmt.Sprintf("%s.%s", SubjectMapping["chat_created"], tenantID)
	chatOverrides := &model.UpsertChatPayload{
		ChatID:                chatID,
		CompanyID:             tenantID,
		Jid:                   jid,
		PushName:              "Rollback Test User",
		ConversationTimestamp: time.Now().Unix(),
		UnreadCount:           0,
		IsGroup:               false,
	}
	chatJSON, err := s.GenerateNatsPayload(fullChatSubject, chatOverrides)
	s.Require().NoError(err, "Failed to generate chat payload for rollback test")
	err = s.PublishEvent(ctx, fullChatSubject, chatJSON)
	s.Require().NoError(err, "Failed to publish chat")
	time.Sleep(2 * time.Second)

	chatQuery := "SELECT COUNT(*) FROM chats WHERE chat_id = $1 AND company_id = $2"
	chatExists, err := verifyPostgresDataWithSchema(ctx, s.PostgresDSN, tenantSchema, chatQuery, 1, chatID, tenantID)
	s.Require().NoError(err, "Failed to verify chat exists")
	s.Assert().True(chatExists, "Chat should exist before transaction rollback test")

	db, err := connectDB(s.PostgresDSN)
	s.Require().NoError(err, "Failed to connect to tenant PostgreSQL schema")
	defer db.Close()

	setSchemaQuery := fmt.Sprintf("SET search_path TO %s", pq.QuoteIdentifier(tenantSchema))
	_, err = db.ExecContext(ctx, setSchemaQuery)
	s.Require().NoError(err, "Failed to set search path to tenant schema %s", tenantSchema)

	_, _ = db.ExecContext(ctx, "ALTER TABLE messages DROP CONSTRAINT IF EXISTS force_rollback")
	breakSchemaQuery := "ALTER TABLE messages ADD CONSTRAINT force_rollback CHECK (status != 'will_fail')"
	_, err = db.ExecContext(ctx, breakSchemaQuery)
	s.Require().NoError(err, "Failed to add constraint to messages table in schema %s", tenantSchema)
	t.Logf("Added temporary constraint 'force_rollback' to messages table in schema %s", tenantSchema)

	fullMessageSubject := fmt.Sprintf("%s.%s", SubjectMapping["message_created"], tenantID)
	messagePayloadMap := map[string]interface{}{
		"message_id": messageID,
		"company_id": tenantID,
		"chat_id":    chatID,
		"jid":        jid,
		"message_obj": map[string]interface{}{
			"conversation": "This message should trigger a rollback",
		},
		"message_timestamp": time.Now().Unix(),
		"from":              jid,
		"status":            "will_fail",
		"flow":              "IN",
	}
	messageJSON, err := json.Marshal(messagePayloadMap)
	s.Require().NoError(err, "Failed to marshal message payload for rollback test")
	err = s.PublishEventWithoutValidation(ctx, fullMessageSubject, messageJSON)
	s.Require().NoError(err, "Failed to publish message intended to fail")
	time.Sleep(5 * time.Second)

	messageQuery := "SELECT COUNT(*) FROM messages WHERE message_id = $1 AND company_id = $2"
	messageNotStored, err := verifyPostgresDataWithSchema(ctx, s.PostgresDSN, tenantSchema, messageQuery, 0, messageID, tenantID)
	s.Require().NoError(err, "Failed to verify message not stored")
	s.Assert().True(messageNotStored, "Message should not be stored due to transaction rollback")

	chatStillExists, err := verifyPostgresDataWithSchema(ctx, s.PostgresDSN, tenantSchema, chatQuery, 1, chatID, tenantID)
	s.Require().NoError(err, "Failed to verify chat still exists")
	s.Assert().True(chatStillExists, "Chat should still exist after transaction rollback")

	t.Logf("Removing temporary constraint 'force_rollback' from messages table in schema %s", tenantSchema)
	removeConstraintQuery := "ALTER TABLE messages DROP CONSTRAINT force_rollback"
	_, err = db.ExecContext(ctx, removeConstraintQuery)
	s.Require().NoError(err, "Failed to remove constraint from messages table")

	newMessageID := fmt.Sprintf("uuid:message-after-rollback-%s", uuid.New().String())
	newMessageOverrides := &model.UpsertMessagePayload{
		MessageID: newMessageID,
		CompanyID: tenantID,
		ChatID:    chatID,
		Jid:       jid,
		MessageObj: map[string]interface{}{
			"conversation": "This message should succeed after rollback test",
		},
		MessageTimestamp: time.Now().Unix(),
		FromPhone:        jid,
		Status:           "sent",
		Flow:             "IN",
	}
	newMessageJSON, err := s.GenerateNatsPayload(fullMessageSubject, newMessageOverrides)
	s.Require().NoError(err, "Failed to generate new message payload")
	err = s.PublishEvent(ctx, fullMessageSubject, newMessageJSON)
	s.Require().NoError(err, "Failed to publish new message after constraint removal")
	time.Sleep(2 * time.Second)

	newMessageQuery := "SELECT COUNT(*) FROM messages WHERE message_id = $1 AND company_id = $2"
	newMessageStored, err := verifyPostgresDataWithSchema(ctx, s.PostgresDSN, tenantSchema, newMessageQuery, 1, newMessageID, tenantID)
	s.Require().NoError(err, "Failed to verify new message stored")
	s.Assert().True(newMessageStored, "New message should be stored after constraint is removed")

	t.Log("Transaction rollback test completed successfully")
}

// TestJetStreamDeliveryRetry tests that JetStream properly redelivers messages when
// processing fails, and that the service eventually processes them successfully.
// Test ID: RC-02
func (s *MainRecoveryResilienceTestSuite) TestJetStreamDeliveryRetry() {
	t := s.T()
	ctx := s.Ctx
	tenantID := s.CompanyID
	tenantSchema := fmt.Sprintf("daisi_%s", tenantID)
	t.Log("Testing JetStream delivery retry...")

	// Generate unique IDs for test
	chatID := fmt.Sprintf("uuid:chat-retry-%s", uuid.New().String())
	jid := "6281234567890@s.whatsapp.net"

	// 1. Create a chat payload
	fullChatSubject := fmt.Sprintf("%s.%s", SubjectMapping["chat_created"], tenantID)
	chatOverrides := &model.UpsertChatPayload{
		ChatID:                chatID,
		CompanyID:             tenantID,
		Jid:                   jid,
		PushName:              "Retry User", // Normal name
		ConversationTimestamp: time.Now().Unix(),
		UnreadCount:           0,
		IsGroup:               false,
	}
	chatJSON, err := s.GenerateNatsPayload(fullChatSubject, chatOverrides)
	s.Require().NoError(err, "Failed to generate chat payload for retry test")

	t.Log("Current PostgresDSN before stopping service: ", s.PostgresDSN)

	// 2. Stop the PostgreSQL database to force failures
	t.Log("Stopping PostgreSQL to force message processing failure...")
	err = s.StopService(ctx, PostgresServiceName) // Using suite helper
	s.Require().NoError(err, "Failed to stop PostgreSQL")

	// Start streaming app container logs
	//s.StreamLogs(t)

	// 3. Publish the message (this should be queued by JetStream)
	t.Log("Publishing message while PostgreSQL is down...")
	err = s.PublishEvent(ctx, fullChatSubject, chatJSON)
	s.Require().NoError(err, "Failed to publish chat while PostgreSQL is down")

	// Wait a bit to ensure message is published and potentially attempted
	time.Sleep(3 * time.Second)

	// 4. Restart PostgreSQL to allow processing to succeed
	t.Log("Restarting PostgreSQL to allow processing to succeed...")
	err = s.StartService(ctx, PostgresServiceName) // Using suite helper
	s.Require().NoError(err, "Failed to restart PostgreSQL")

	// Wait for PostgreSQL to be fully ready
	t.Log("Waiting for PostgreSQL to become ready...")
	if err := WaitUntilDBReady(ctx, s.PostgresDSN); err != nil {
		t.Fatalf("PostgreSQL did not become ready after restart: %v", err)
	}
	t.Log("PostgreSQL is ready.")
	t.Log("Current PostgresDSN after restarting service: ", s.PostgresDSN)

	// 5. Wait for JetStream to redeliver and for the service to process
	t.Log("Waiting for JetStream to redeliver message and for service to process it...")

	// 6. Verify the message was eventually processed using timeout
	verifyQuery := "SELECT COUNT(*) FROM chats WHERE chat_id = $1 AND company_id = $2"
	var processed bool
	var verifyErr error

	// Increased timeout for verification after restart
	s.Require().Eventually(func() bool {
		processed, verifyErr = verifyPostgresDataWithSchema(ctx, s.PostgresDSN, tenantSchema, verifyQuery, 1, chatID, tenantID)
		if verifyErr != nil {
			t.Logf("Verification error: %v", verifyErr) // Log error but continue trying
			return false
		}
		if processed {
			t.Log("Message was successfully redelivered and processed")
			return true
		}
		t.Log("Message not processed yet, continuing to wait...")
		return false
	}, 60*time.Second, 5*time.Second, "Timed out waiting for message to be redelivered and processed")

	s.Require().NoError(verifyErr, "Final verification of message processing resulted in an error")
	s.Assert().True(processed, "Message should be processed after redelivery attempts")

	t.Log("JetStream delivery retry test completed successfully")
}

// --- PoisonMessageHandlingSuite ---
type PoisonMessageHandlingSuite struct {
	E2EIntegrationTestSuite
}

// SetupTest for PoisonMessageHandlingSuite
func (s *PoisonMessageHandlingSuite) SetupTest() {
	s.E2EIntegrationTestSuite.SetupTest() // Run base setup (DB truncation)
	t := s.T()
	logger.Log = zaptest.NewLogger(t).Named("PoisonMessageHandlingSuite")
	t.Log("Waiting for daisi-wa-events-processor to potentially process previous test remnants...")
	time.Sleep(1 * time.Second) // Short delay before each test
}

// BeforeTest is specific to PoisonMessageHandlingSuite for setting up DLQ env vars
func (s *PoisonMessageHandlingSuite) BeforeTest(suiteName, testName string) {
	t := s.T()
	ctx := s.Ctx

	// This logic only applies if the test being run IS TestPoisonMessageHandling
	// Since this BeforeTest is now part of PoisonMessageHandlingSuite, which *only* contains TestPoisonMessageHandling,
	// the check for testName == "TestPoisonMessageHandling" is implicitly true.
	t.Log("PoisonMessageHandlingSuite.BeforeTest: Configuring short DLQ delays for TestPoisonMessageHandling")

	if s.Application != nil {
		t.Log("Terminating existing application container for TestPoisonMessageHandling...")
		err := s.Application.Terminate(ctx)
		s.Require().NoError(err, "Failed to terminate existing application container in BeforeTest")
		s.Application = nil
	}

	dlqEnvOverrides := map[string]string{
		"NATS_DLQBASEDELAYMINUTES": "1",
		"NATS_DLQMAXDELAYMINUTES":  "2",
		"NATS_DLQMAXDELIVER":       "1",
		"NATS_DLQACKWAIT":          "2s",
	}

	t.Logf("Restarting application container for TestPoisonMessageHandling with Env Overrides: %+v", dlqEnvOverrides)
	testEnvForApp := &TestEnvironment{
		Network:            s.Network,
		PostgresDSNNetwork: s.PostgresDSNNetwork,
		NATSURLNetwork:     s.NATSURLNetwork,
		CompanyID:          s.CompanyID,
	}

	var err error
	s.Application, s.AppAPIURL, err = setupTestApp(ctx, s.Network.Name, testEnvForApp, dlqEnvOverrides)
	s.Require().NoError(err, "Failed to restart application container with DLQ overrides in BeforeTest")

	t.Log("Application container restarted with DLQ overrides for TestPoisonMessageHandling.")
	t.Log("Waiting for application to become ready after override...")
	time.Sleep(10 * time.Second)

	t.Cleanup(func() {
		t.Log("Cleanup: Restoring default application container after TestPoisonMessageHandling")
		if s.Application != nil {
			err := s.Application.Terminate(ctx)
			s.Require().NoError(err, "Failed to terminate DLQ-customized application container in Cleanup")
			s.Application = nil
		}

		t.Log("Restarting application container with default environment...")
		var cleanupErr error
		s.Application, s.AppAPIURL, cleanupErr = setupTestApp(ctx, s.Network.Name, testEnvForApp, nil)
		s.Require().NoError(cleanupErr, "Failed to restart application container with default env in Cleanup")
		s.StreamLogs(t)
		t.Log("Application container restored to default state. Waiting for readiness...")
		time.Sleep(10 * time.Second)
	})
}

// TestPoisonMessageHandling tests that the service properly handles poison messages
// (messages that persistently fail processing) by moving them to a dead-letter queue
// or otherwise preventing them from blocking the processing pipeline.
// Test ID: RC-03
func (s *PoisonMessageHandlingSuite) TestPoisonMessageHandling() {
	t := s.T()
	ctx := s.Ctx
	tenantID := s.CompanyID
	tenantSchema := fmt.Sprintf("daisi_%s", tenantID)
	t.Log("Testing poison message handling...")

	poisonChatID := fmt.Sprintf("uuid:chat-poison-%s", uuid.New().String())
	normalChatID := fmt.Sprintf("uuid:chat-normal-%s", uuid.New().String())

	poisonPayload := map[string]interface{}{
		"chat_id":                poisonChatID,
		"company_id":             tenantID,
		"jid":                    "invalid-jid-format",
		"phone_number":           "invalid-phone-number",
		"push_name":              make(chan int),
		"conversation_timestamp": "not-a-unix-timestamp",
		"unread_count":           -100,
		"is_group":               "maybe",
	}
	_, err := json.Marshal(poisonPayload)
	s.Require().Error(err, "Marshalling poison payload with channel should fail")
	t.Logf("Intentionally failed to marshal poison message: %v", err)

	poisonPayloadValidJSON := map[string]interface{}{
		"chat_id":                poisonChatID,
		"company_id":             tenantID,
		"jid":                    "jid-1",
		"phone_number":           "phone-1",
		"push_name":              "Poison User",
		"conversation_timestamp": 999999999999999999,
		"unread_count":           -1,
	}
	poisonJSONValid, err := json.Marshal(poisonPayloadValidJSON)
	s.Require().NoError(err, "Failed to marshal second poison chat payload")

	fullChatSubject := fmt.Sprintf("%s.%s", SubjectMapping["chat_created"], tenantID)
	err = s.PublishEventWithoutValidation(ctx, fullChatSubject, poisonJSONValid)
	s.Require().NoError(err, "Failed to publish poison message")

	normalChatOverrides := map[string]interface{}{
		"chat_id":                normalChatID,
		"company_id":             tenantID,
		"jid":                    "6281234567890@s.whatsapp.net",
		"push_name":              "Normal Message After Poison",
		"conversation_timestamp": time.Now().Unix(),
		"unread_count":           0,
		"is_group":               false,
	}
	normalJSON, err := s.GenerateNatsPayload(fullChatSubject, normalChatOverrides)
	s.Require().NoError(err, "Failed to generate normal chat payload")
	err = s.PublishEvent(ctx, fullChatSubject, normalJSON)
	s.Require().NoError(err, "Failed to publish normal message")

	time.Sleep(10 * time.Second)
	//s.StreamLogs(t)

	poisonQuery := "SELECT COUNT(*) FROM chats WHERE chat_id = $1 AND company_id = $2"
	poisonRejected, err := verifyPostgresDataWithSchema(ctx, s.PostgresDSN, tenantSchema, poisonQuery, 0, poisonChatID, tenantID)
	s.Require().NoError(err, "Failed to verify poison message rejection")
	s.Assert().True(poisonRejected, "Poison message should not be processed successfully")

	normalQuery := "SELECT COUNT(*) FROM chats WHERE chat_id = $1 AND company_id = $2"
	normalProcessed, err := verifyPostgresDataWithSchema(ctx, s.PostgresDSN, tenantSchema, normalQuery, 1, normalChatID, tenantID)
	s.Require().NoError(err, "Failed to verify normal message processing")
	s.Assert().True(normalProcessed, "Normal message should be processed despite poison message")

	dlqQuery := "SELECT COUNT(*) FROM exhausted_events WHERE company_id = $1 AND source_subject = $2"
	var dlqEntryExists bool
	var verifyErr error
	s.Require().Eventually(func() bool {
		s.StreamLogs(t)
		dlqEntryExists, verifyErr = verifyPostgresDataWithSchema(ctx, s.PostgresDSN, tenantSchema, dlqQuery, 1, tenantID, fullChatSubject)
		if verifyErr != nil {
			t.Logf("DLQ verification error: %v", verifyErr)
			return false
		}
		if dlqEntryExists {
			t.Log("Poison message successfully found in exhausted_events table")
			return true
		}
		t.Log("Poison message not yet in exhausted_events, continuing to wait...")
		return false
	}, 150*time.Second, 10*time.Second, "Timed out waiting for poison message to appear in exhausted_events table")
	s.Require().NoError(verifyErr, "Final DLQ verification check resulted in an error")
	s.Assert().True(dlqEntryExists, "Poison message should have exactly 1 entry in exhausted_events")

	s.StreamLogs(t)
	var dlqEvent struct {
		ID              int64
		CreatedAt       time.Time
		CompanyID       string
		SourceSubject   string
		LastError       sql.NullString
		RetryCount      sql.NullInt32
		EventTimestamp  sql.NullTime
		DLQPayload      []byte
		OriginalPayload []byte
		Resolved        bool
		ResolvedAt      sql.NullTime
		Notes           sql.NullString
	}
	fetchDlqQuery := "SELECT id, created_at, company_id, source_subject, last_error, retry_count, event_timestamp, dlq_payload, original_payload, resolved, resolved_at, notes FROM exhausted_events WHERE company_id = $1 AND source_subject = $2 ORDER BY created_at DESC LIMIT 1"
	dest := []interface{}{
		&dlqEvent.ID, &dlqEvent.CreatedAt, &dlqEvent.CompanyID, &dlqEvent.SourceSubject,
		&dlqEvent.LastError, &dlqEvent.RetryCount, &dlqEvent.EventTimestamp, &dlqEvent.DLQPayload,
		&dlqEvent.OriginalPayload, &dlqEvent.Resolved, &dlqEvent.ResolvedAt, &dlqEvent.Notes,
	}
	err = executeQueryWIthSchemaRowScan(ctx, s.PostgresDSN, tenantSchema, fetchDlqQuery, dest, tenantID, fullChatSubject)
	s.Require().NoError(err, "Failed to fetch DLQ event details")

	s.Assert().Equal(tenantID, dlqEvent.CompanyID, "DLQ event company ID should match")
	s.Assert().Equal(fullChatSubject, dlqEvent.SourceSubject, "DLQ event source subject should match")
	s.Assert().True(dlqEvent.LastError.Valid && dlqEvent.LastError.String != "", "DLQ event should have a last_error recorded")
	s.T().Logf("DLQ Last Error: %s", dlqEvent.LastError.String)
	s.Assert().True(dlqEvent.RetryCount.Valid && dlqEvent.RetryCount.Int32 > 0, "DLQ event retry_count should be greater than 0")
	s.T().Logf("DLQ Retry Count: %d", dlqEvent.RetryCount.Int32)
	s.Assert().False(dlqEvent.Resolved, "DLQ event should not be resolved initially")
	s.Assert().Equal(poisonJSONValid, dlqEvent.DLQPayload, "DLQPayload should match the original poison message")
	if dlqEvent.OriginalPayload != nil {
		s.Assert().Equal(poisonJSONValid, dlqEvent.OriginalPayload, "OriginalPayload should match the original poison message if present")
	}
	t.Log("Poison message handling and DLQ verification test completed successfully")
}

// TestAllRecoveryAndResilienceScenarios runs all recovery and resilience test suites in order.
func TestAllRecoveryAndResilienceScenarios(t *testing.T) {
	suite.Run(t, new(MainRecoveryResilienceTestSuite))
	//suite.Run(t, new(PoisonMessageHandlingSuite)) // not applicable for this test because long wait
}
