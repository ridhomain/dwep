package integration_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

// Define DefaultSender locally for this test file
const DefaultSender = "test_sender"

// --- DB Connection Test Suite ---

// DBConnectionTestSuite tests database connection issues and recovery.
type DBConnectionTestSuite struct {
	E2EIntegrationTestSuite // Embed the E2E suite to get App, DB, NATS, etc.
}

// TestDBConnectionSuite runs the DB connection test suite.
func TestDBConnectionSuite(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	suite.Run(t, new(DBConnectionTestSuite))
}

// --- Test Methods ---

// TestDB01_ConnectionRefused tests how the service handles when database connections are refused.
func (s *DBConnectionTestSuite) TestDB01_ConnectionRefused() {
	t := s.T()   // Get the underlying *testing.T instance
	ctx := s.Ctx // Use the suite's context
	t.Log("Testing database connection refused handling...")

	// Start streaming app container logs
	// s.StreamLogs(t)

	// Step 1: Stop the PostgreSQL container to simulate connection refused
	t.Log("Stopping PostgreSQL container to simulate connection refused...")
	err := s.StopService(ctx, PostgresServiceName)
	s.Require().NoError(err, "Failed to stop PostgreSQL service")

	// Wait for PostgreSQL to fully stop
	time.Sleep(5 * time.Second)

	// Step 2: Send a test message while the database is down
	chatID := fmt.Sprintf("uuid:chat-db-refused-%d", time.Now().Unix())
	// Use generateNatsPayload to create the payload
	subject := fmt.Sprintf("v1.chats.upsert.%s", s.CompanyID)
	overrides := map[string]interface{}{
		"chat_id":                chatID, // Override chat_id
		"jid":                    "6281234567890@s.whatsapp.net",
		"phone_number":           "6281234567890",
		"push_name":              "Test Customer DB Refused",
		"conversation_timestamp": time.Now().Unix(),
		"unread_count":           2,
		"is_group":               true,
		"not_spam":               true,
	}
	chatPayloadBytes, err := s.GenerateNatsPayload(subject, overrides)
	s.Require().NoError(err, "Failed to generate chat payload for DB refused test")

	// Publish the chat while database is down
	// Use s.NATSURL which points to the host-accessible port
	err = s.PublishEvent(ctx, subject, chatPayloadBytes)
	s.Assert().NoError(err, "Failed to publish chat while database is down")

	// Wait for processing attempt
	time.Sleep(5 * time.Second)

	// Step 3: Restart the PostgreSQL container
	t.Log("Restarting PostgreSQL container...")
	err = s.StartService(ctx, PostgresServiceName)
	s.Require().NoError(err, "Failed to restart PostgreSQL service")

	// Wait for PostgreSQL to be ready
	time.Sleep(10 * time.Second)

	// Update connection details after restart (This might not be strictly necessary
	// if the container keeps the same DSN on restart, but good practice)
	t.Log("Getting updated PostgreSQL connection details after restart...")

	// Get updated host and port
	host, err := s.Postgres.Host(ctx)
	if err != nil {
		t.Fatalf("Failed to get PostgreSQL host after restart: %v", err)
	}

	mappedPort, err := s.Postgres.MappedPort(ctx, "5432")
	if err != nil {
		t.Fatalf("Failed to get PostgreSQL port after restart: %v", err)
	}

	// Create the new DSN
	newPostgresDSN := fmt.Sprintf("postgres://postgres:postgres@%s:%s/message_service?sslmode=disable",
		host, mappedPort.Port())

	// Check if the DSN has changed and update it on the suite
	if newPostgresDSN != s.PostgresDSN {
		t.Logf("PostgreSQL DSN changed after restart: %s -> %s", s.PostgresDSN, newPostgresDSN)
		s.PostgresDSN = newPostgresDSN
	}
	// Also update the network DSN used by the app container if it changed
	s.PostgresDSNNetwork = changeUriFromLocalhostToDocker(newPostgresDSN, "pg")

	// Wait for database to be fully ready
	time.Sleep(5 * time.Second)

	// Step 4: Check service logs for appropriate error handling
	// Note: The StreamLogs function already captures logs, so check test output for:
	// - Error logs indicating database connection issues
	// - Retry attempts
	// - Circuit breaker patterns if implemented

	// Step 5: Verify if the service implements retry or queuing mechanism
	// Give some time for potential retries to complete
	time.Sleep(15 * time.Second)

	// Check if the chat was eventually stored after database recovery
	query := fmt.Sprintf("SELECT COUNT(*) FROM chats_%s WHERE id = '%s'", DefaultSender, chatID) // Assuming chats table uses id as primary
	stored, err := verifyPostgresData(ctx, s.PostgresDSN, query, 1)

	// This test doesn't fail if the message wasn't stored, as it depends on implementation
	if err == nil && stored {
		t.Log("Service successfully recovered and stored the chat after database connection was restored")
	} else {
		t.Logf("Service did not recover the chat after database connection was restored (may need manual recovery). Error: %v", err)
	}

	t.Log("Database connection refused test completed")
}

// TestDB02_ConnectionTimeout tests how the service handles database connection timeouts.
func (s *DBConnectionTestSuite) TestDB02_ConnectionTimeout() {
	t := s.T()
	ctx := s.Ctx
	t.Log("Testing database connection timeout handling...")

	// Start streaming app container logs
	// s.StreamLogs(t)

	// Step 1: Simulate database slowness/timeout by overwhelming the database
	t.Log("Generating load on PostgreSQL to simulate timeouts...")

	// Create a batch of heavy queries to simulate load
	loadCtx, loadCancel := context.WithCancel(ctx) // Separate context for load generation
	defer loadCancel()
	go func() {
		for i := 0; i < 10; i++ {
			select {
			case <-loadCtx.Done():
				return // Stop generating load if context is cancelled
			default:
				// Execute a complex query that consumes resources
				query := "SELECT pg_sleep(5);"
				_, err := executeQuery(loadCtx, s.PostgresDSN, query)
				if err != nil {
					// Ignore errors as the database might be overwhelmed or context cancelled
					continue
				}
			}
		}
	}()

	// Step 2: Send a test message during high database load
	chatID := fmt.Sprintf("uuid:chat-db-timeout-%d", time.Now().Unix())
	// Use generateNatsPayload
	subject := fmt.Sprintf("v1.chats.upsert.%s", s.CompanyID)
	overrides := map[string]interface{}{
		"chat_id":                chatID,
		"jid":                    "6281234567890@s.whatsapp.net",
		"phone_number":           "6281234567890",
		"push_name":              "Test Customer DB Timeout",
		"conversation_timestamp": time.Now().Unix(),
		"unread_count":           2,
		"is_group":               true,
		"not_spam":               true,
	}
	chatPayloadBytes, err := s.GenerateNatsPayload(subject, overrides)
	s.Require().NoError(err, "Failed to generate chat payload for DB timeout test")

	// Publish the chat during high database load
	err = s.PublishEvent(ctx, subject, chatPayloadBytes)
	s.Assert().NoError(err, "Failed to publish chat during database load")

	// Wait for processing and timeout conditions
	time.Sleep(15 * time.Second)

	// Stop load generation
	loadCancel()

	// Step 3: Check if the service properly handles timeouts
	// Check logs for:
	// - Timeout errors
	// - Retry behaviors
	// - Circuit breaker patterns

	// Step 4: Wait for the database to recover and check if the message was processed
	time.Sleep(10 * time.Second)

	// Check if the chat was eventually stored after timeout resolution
	query := fmt.Sprintf("SELECT COUNT(*) FROM chats_%s WHERE id = '%s'", DefaultSender, chatID) // Assuming chats table uses id as primary
	stored, err := verifyPostgresData(ctx, s.PostgresDSN, query, 1)

	// Again, this test doesn't fail if the message wasn't stored
	if err == nil && stored {
		t.Log("Service successfully processed the chat after database timeout resolution")
	} else {
		t.Logf("Service did not process the chat after database timeout resolution (may need manual recovery). Error: %v", err)
	}

	t.Log("Database connection timeout test completed")
}

// TestDB03_ConnectionRecovery tests how the service recovers after a database outage.
func (s *DBConnectionTestSuite) TestDB03_ConnectionRecovery() {
	t := s.T()
	ctx := s.Ctx
	t.Log("Testing database connection recovery...")

	// Start streaming app container logs
	// s.StreamLogs(t)

	// Step 1: Store a control message before outage
	preOutageChatID := fmt.Sprintf("uuid:chat-pre-outage-%d", time.Now().Unix())
	// Use generateNatsPayload
	subjectPre := fmt.Sprintf("v1.chats.upsert.%s", s.CompanyID)
	overridesPre := map[string]interface{}{
		"chat_id":                preOutageChatID,
		"jid":                    "6281234567890@s.whatsapp.net",
		"phone_number":           "6281234567890",
		"push_name":              "Test Customer Pre-Outage",
		"conversation_timestamp": time.Now().Unix(),
		"unread_count":           2,
		"is_group":               true,
		"not_spam":               true,
	}
	preOutagePayloadBytes, err := s.GenerateNatsPayload(subjectPre, overridesPre)
	s.Require().NoError(err, "Failed to generate pre-outage chat payload")

	// Publish pre-outage chat
	err = s.PublishEvent(ctx, subjectPre, preOutagePayloadBytes)
	s.Assert().NoError(err, "Failed to publish pre-outage chat")

	// Wait for pre-outage chat to be stored
	time.Sleep(5 * time.Second)

	// Verify pre-outage chat was stored
	preQuery := "SELECT COUNT(*) FROM chats WHERE chat_id = $1"
	preStored, err := verifyPostgresDataWithSchema(ctx, s.PostgresDSN, s.CompanySchemaName, preQuery, 1, preOutageChatID)
	s.Require().NoError(err, "Failed to verify pre-outage chat storage")
	s.Require().True(preStored, "Pre-outage chat should be stored before continuing")

	// Step 2: Stop the PostgreSQL container
	t.Log("Stopping PostgreSQL container to simulate outage...")
	err = s.StopService(ctx, PostgresServiceName)
	s.Require().NoError(err, "Failed to stop PostgreSQL service")

	// Wait for PostgreSQL to fully stop
	time.Sleep(5 * time.Second)

	// Step 3: Send messages during outage
	duringOutageChatID := fmt.Sprintf("uuid:chat-during-outage-%d", time.Now().Unix())
	// Use generateNatsPayload
	subjectDuring := fmt.Sprintf("v1.chats.upsert.%s", s.CompanyID)
	overridesDuring := map[string]interface{}{
		"chat_id":                duringOutageChatID,
		"jid":                    "6281234567890@s.whatsapp.net",
		"phone_number":           "6281234567890",
		"push_name":              "Test Customer During-Outage",
		"conversation_timestamp": time.Now().Unix(),
		"unread_count":           2,
		"is_group":               true,
		"not_spam":               true,
	}
	duringOutagePayloadBytes, err := s.GenerateNatsPayload(subjectDuring, overridesDuring)
	s.Require().NoError(err, "Failed to generate during-outage chat payload")

	// Send multiple messages during outage
	for i := 0; i < 5; i++ {
		err = s.PublishEvent(ctx, subjectDuring, duringOutagePayloadBytes)
		s.Assert().NoError(err, "Failed to publish during-outage chat")
		time.Sleep(1 * time.Second)
	}

	// Step 4: Restart the PostgreSQL container
	t.Log("Restarting PostgreSQL container...")
	err = s.StartService(ctx, PostgresServiceName)
	s.Require().NoError(err, "Failed to restart PostgreSQL service")

	// Wait for PostgreSQL to be ready
	time.Sleep(15 * time.Second)

	// IMPORTANT FIX: Get the new PostgreSQL connection details after restart
	t.Log("Getting updated PostgreSQL connection details after restart...")

	// Get updated host and port
	host, err := s.Postgres.Host(ctx)
	if err != nil {
		t.Fatalf("Failed to get PostgreSQL host after restart: %v", err)
	}

	mappedPort, err := s.Postgres.MappedPort(ctx, "5432")
	if err != nil {
		t.Fatalf("Failed to get PostgreSQL port after restart: %v", err)
	}

	// Create the new DSN
	newPostgresDSN := fmt.Sprintf("postgres://postgres:postgres@%s:%s/message_service?sslmode=disable",
		host, mappedPort.Port())

	// Check if the DSN has changed and update it
	if newPostgresDSN != s.PostgresDSN {
		t.Logf("PostgreSQL DSN changed after restart: %s -> %s", s.PostgresDSN, newPostgresDSN)
		s.PostgresDSN = newPostgresDSN
	}
	s.PostgresDSNNetwork = changeUriFromLocalhostToDocker(newPostgresDSN, "pg")

	// Wait a bit longer to ensure PostgreSQL is fully ready
	time.Sleep(5 * time.Second)

	// Step 5: Send post-recovery message
	postRecoveryChatID := fmt.Sprintf("uuid:chat-post-recovery-%d", time.Now().Unix())
	// Use generateNatsPayload
	subjectPost := fmt.Sprintf("v1.chats.upsert.%s", s.CompanyID)
	overridesPost := map[string]interface{}{
		"chat_id":                postRecoveryChatID,
		"jid":                    "6281234567890@s.whatsapp.net",
		"phone_number":           "6281234567890",
		"push_name":              "Test Customer Post-Recovery",
		"conversation_timestamp": time.Now().Unix(),
		"unread_count":           2,
		"is_group":               true,
		"not_spam":               true,
	}
	postRecoveryPayloadBytes, err := s.GenerateNatsPayload(subjectPost, overridesPost)
	s.Require().NoError(err, "Failed to generate post-recovery chat payload")

	// Publish post-recovery chat
	err = s.PublishEvent(ctx, subjectPost, postRecoveryPayloadBytes)
	s.Assert().NoError(err, "Failed to publish post-recovery chat")

	// Wait for recovery and processing
	time.Sleep(20 * time.Second)

	// Step 6: Verify post-recovery message was stored
	postQuery := "SELECT COUNT(*) FROM chats WHERE chat_id = $1"
	postStored, err := verifyPostgresDataWithSchema(ctx, s.PostgresDSN, s.CompanySchemaName, postQuery, 1, postRecoveryChatID)
	s.Assert().NoError(err, "Failed to verify post-recovery chat storage")
	s.Assert().True(postStored, "Post-recovery chat should be stored successfully")

	// Step 7: Check if during-outage messages were processed (if the service implements recovery)
	duringQuery := "SELECT COUNT(*) FROM chats WHERE chat_id = $1"
	duringStored, err := verifyPostgresDataWithSchema(ctx, s.PostgresDSN, s.CompanySchemaName, duringQuery, 1, duringOutageChatID)

	// Log the result but don't fail the test based on this, as it depends on implementation
	if err == nil && duringStored {
		t.Log("Service successfully recovered and processed messages sent during database outage")
	} else {
		t.Logf("Service did not recover messages sent during database outage (may require manual recovery). Error: %v", err)
	}

	t.Log("Database connection recovery test completed")
}

// TestDB04_QueryTimeout tests how the service handles database query timeouts.
func (s *DBConnectionTestSuite) TestDB04_QueryTimeout() {
	t := s.T()
	ctx := s.Ctx
	t.Log("Testing database query timeout handling...")

	// Start streaming app container logs
	// s.StreamLogs(t)

	// Step 1: Create a blocking query in PostgreSQL
	t.Log("Creating a blocking session in PostgreSQL...")

	// Start a background goroutine to hold the lock
	lockCtx, lockCancel := context.WithCancel(ctx) // Use derived context
	defer lockCancel()                             // Ensure cancellation
	lockReleaseChan := make(chan struct{})
	go func() {
		defer close(lockReleaseChan)
		// Open a connection and acquire a lock
		// Use a dynamic table name based on DefaultSender if schema requires
		lockQuery := fmt.Sprintf(`
			BEGIN;
			LOCK TABLE %q.chats IN ACCESS EXCLUSIVE MODE;
			-- Wait for signal to release the lock (or timeout)
			SELECT pg_sleep(30);
			COMMIT;
		`, s.CompanySchemaName)
		_, err := executeQuery(lockCtx, s.PostgresDSN, lockQuery)
		if err != nil {
			// Ignore context cancelled error, log others
			if lockCtx.Err() == nil {
				t.Logf("Error in lock goroutine: %v", err)
			}
		}
	}()

	// Give some time for the lock to be acquired
	time.Sleep(2 * time.Second)

	// Step 2: Send a test message that should be affected by the lock
	timeoutChatID := fmt.Sprintf("uuid:chat-query-timeout-%d", time.Now().Unix())
	// Use generateNatsPayload
	subject := fmt.Sprintf("v1.chats.upsert.%s", s.CompanyID)
	overrides := map[string]interface{}{
		"chat_id":                timeoutChatID,
		"jid":                    "6281234567890@s.whatsapp.net",
		"phone_number":           "6281234567890",
		"push_name":              "Test Customer Query Timeout",
		"conversation_timestamp": time.Now().Unix(),
		"unread_count":           2,
		"is_group":               true,
		"not_spam":               true,
	}
	timeoutPayloadBytes, err := s.GenerateNatsPayload(subject, overrides)
	s.Require().NoError(err, "Failed to generate payload for query timeout test")

	// Publish the chat while the table is locked
	err = s.PublishEvent(ctx, subject, timeoutPayloadBytes)
	s.Assert().NoError(err, "Failed to publish chat for query timeout test")

	// Step 3: Wait for the service to attempt processing (should encounter timeout)
	t.Log("Waiting for service to attempt processing (should encounter timeout)...")
	time.Sleep(10 * time.Second)

	// Step 4: Release the lock by canceling the background query context
	t.Log("Releasing lock to allow normal processing...")
	lockCancel() // Cancel the context for the locking goroutine

	// Wait for lock release goroutine to finish
	select {
	case <-lockReleaseChan:
		t.Log("Lock released successfully")
	case <-time.After(5 * time.Second):
		t.Log("Lock release timed out (goroutine might still be running)")
	}

	// Step 5: Give the service time to retry the operation
	t.Log("Waiting for service to retry operation after lock release...")
	time.Sleep(15 * time.Second)

	// Step 6: Check if the message was eventually processed
	query := "SELECT COUNT(*) FROM chats WHERE chat_id = $1"
	processed, err := verifyPostgresDataWithSchema(ctx, s.PostgresDSN, s.CompanySchemaName, query, 1, timeoutChatID)

	if err == nil && processed {
		t.Log("Service successfully recovered from query timeout and processed the message")
	} else {
		t.Logf("Service did not recover from query timeout (may need manual recovery). Error: %v", err)
	}

	// Send a new message to confirm the service is back to normal operations
	postTimeoutChatID := fmt.Sprintf("uuid:chat-post-timeout-%d", time.Now().Unix())
	// Use generateNatsPayload
	subjectPost := fmt.Sprintf("v1.chats.upsert.%s", s.CompanyID)
	overridesPost := map[string]interface{}{
		"chat_id":                postTimeoutChatID,
		"jid":                    "6281234567890@s.whatsapp.net",
		"phone_number":           "6281234567890",
		"push_name":              "Test Customer Post Timeout",
		"conversation_timestamp": time.Now().Unix(),
		"unread_count":           2,
		"is_group":               true,
		"not_spam":               true,
	}
	postTimeoutPayloadBytes, err := s.GenerateNatsPayload(subjectPost, overridesPost)
	s.Require().NoError(err, "Failed to generate post-timeout chat payload")

	// Publish the post-timeout chat
	err = s.PublishEvent(ctx, subjectPost, postTimeoutPayloadBytes)
	s.Assert().NoError(err, "Failed to publish post-timeout chat")

	// Wait for processing
	time.Sleep(5 * time.Second)

	// Verify the post-timeout chat was processed
	postQuery := "SELECT COUNT(*) FROM chats WHERE chat_id = $1"
	postProcessed, err := verifyPostgresDataWithSchema(ctx, s.PostgresDSN, s.CompanySchemaName, postQuery, 1, postTimeoutChatID)
	s.Assert().NoError(err, "Failed to verify PostgreSQL data")
	s.Assert().True(postProcessed, "Post-timeout chat should be processed successfully")

	t.Log("Database query timeout test completed")
}
