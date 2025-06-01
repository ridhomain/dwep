package integration_test

import (
	"fmt"
	"testing"
	"time"

	"gitlab.com/timkado/api/daisi-wa-events-processor/pkg/logger"
	"go.uber.org/zap/zaptest"

	"github.com/google/uuid"
	natsgo "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/suite"

	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/model"
)

// NATSServerAndConnectionTestSuite defines the suite for NATS server and connection tests.
type NATSServerAndConnectionTestSuite struct {
	E2EIntegrationTestSuite
}

// TestNATSServerAndConnectionSuite runs the NATS server and connection test suite
func TestNATSServerAndConnectionSuite(t *testing.T) {
	suite.Run(t, new(NATSServerAndConnectionTestSuite))
}

// SetupTest runs before each test in this suite.
func (s *NATSServerAndConnectionTestSuite) SetupTest() {
	s.E2EIntegrationTestSuite.SetupTest() // Run base setup (DB truncation)
	t := s.T()
	logger.Log = zaptest.NewLogger(t).Named("NATSServerAndConnectionTestSuite")
	t.Log("Waiting for daisi-wa-events-processor to potentially process previous test remnants...")
	time.Sleep(1 * time.Second) // Short delay before each test
}

// TestNATSServerDown tests the application's behavior when the NATS server is unavailable
// MQ-01: NATS Server Down
func (s *NATSServerAndConnectionTestSuite) TestNATSServerDown() {
	t := s.T()
	ctx := s.Ctx
	tenantID := s.CompanyID
	tenantSchema := fmt.Sprintf("daisi_%s", tenantID)

	// Add logging to see container status
	t.Log("Starting NATS Server Down test")

	// Generate test data ID
	chatID := fmt.Sprintf("uuid:chat-nats-down-%s", uuid.New().String())

	// Create test payloads using the refactored helper/fixtures
	fullChatSubject := fmt.Sprintf("%s.%s", SubjectMapping["chat_created"], tenantID)
	payloadOverrides := map[string]interface{}{
		"chat_id":    chatID,
		"company_id": tenantID,
		"jid":        "628111@w.net",
	}
	testPayloads, err := s.GenerateNatsPayload(fullChatSubject, payloadOverrides)
	s.Require().NoError(err, "Failed to generate test payload")

	// Stop NATS container
	t.Log("Stopping NATS container to simulate server down")
	err = s.StopService(ctx, NatsServiceName) // Use suite method
	s.Require().NoError(err, "Failed to stop NATS container")

	// Try to publish a message when NATS is down
	t.Log("Attempting to publish message while NATS is down")
	// Use raw connect/publish as publishTestMessageNew includes connection
	nc, connErr := natsgo.Connect(s.NATSURL)
	if connErr != nil {
		t.Logf("Expected connection error received: %v", connErr)
		s.Assert().Error(connErr, "Connecting to NATS should fail when server is down")
	} else {
		nc.Close()
		t.Error("Expected NATS connection to fail, but it succeeded.")
	}

	// Test the service's behavior by checking logs for reconnection attempts
	t.Log("Observing logs for service reconnection attempts...")
	time.Sleep(10 * time.Second) // Give time for reconnection attempts to appear in logs

	// Start NATS container again
	t.Log("Starting NATS container again")
	err = s.StartService(ctx, NatsServiceName) // Use suite method
	s.Require().NoError(err, "Failed to start NATS container")

	// Wait for NATS to be ready using a helper
	t.Log("Waiting for NATS to become ready...")
	err = WaitUntilNatsReady(ctx, s.NATSURL)
	s.Require().NoError(err, "NATS did not become ready after restart")
	t.Log("NATS is ready.")

	// Now publish the message and verify that the service processes it correctly
	t.Log("Publishing message after NATS is back up")
	err = s.PublishEvent(ctx, fullChatSubject, testPayloads)
	s.Require().NoError(err, "Failed to publish message after NATS recovery")

	s.StreamLogs(t)

	// Wait for message processing
	time.Sleep(5 * time.Second)

	// Verify the message was processed correctly
	t.Log("Verifying message was processed after NATS recovery")
	verifyQuery := "SELECT COUNT(*) FROM chats WHERE chat_id = $1 AND company_id = $2"
	processed, err := verifyPostgresDataWithSchema(ctx, s.PostgresDSN, tenantSchema, verifyQuery, 1, chatID, tenantID)
	s.Require().NoError(err, "Failed to execute query")
	s.Assert().True(processed, "Message should be processed after NATS recovery")
}

// TestNATSConnectionDrop tests the application's behavior when NATS connection drops
// MQ-02: NATS Connection Drop
func (s *NATSServerAndConnectionTestSuite) TestNATSConnectionDrop() {
	t := s.T()
	ctx := s.Ctx
	tenantID := s.CompanyID
	tenantSchema := fmt.Sprintf("daisi_%s", tenantID)

	// Log test start
	t.Log("Starting NATS Connection Drop test")
	// s.StreamLogs(t)

	// Create test IDs
	chatID1 := fmt.Sprintf("uuid:chat-conn-drop1-%s", uuid.New().String())
	chatID2 := fmt.Sprintf("uuid:chat-conn-drop2-%s", uuid.New().String())

	// Create first test payload
	fullChatSubject := fmt.Sprintf("%s.%s", SubjectMapping["chat_created"], tenantID)
	testPayload1, err := s.GenerateNatsPayload(fullChatSubject, &model.UpsertChatPayload{ChatID: chatID1, CompanyID: tenantID, Jid: "628222@w.net"})
	s.Require().NoError(err, "Failed to generate payload 1")

	// First publish a message successfully and verify it works
	t.Log("Publishing initial message before connection drop")
	err = s.PublishEvent(ctx, fullChatSubject, testPayload1)
	s.Require().NoError(err, "Failed to publish initial message")
	time.Sleep(5 * time.Second)

	// Verify message was processed
	verifyQuery1 := "SELECT COUNT(*) FROM chats WHERE chat_id = $1 AND company_id = $2"
	processed1, err := verifyPostgresDataWithSchema(ctx, s.PostgresDSN, tenantSchema, verifyQuery1, 1, chatID1, tenantID)
	s.Require().NoError(err, "Failed to execute query for message 1")
	s.Assert().True(processed1, "Message 1 should be processed initially")

	// Restart the NATS container to simulate connection drop
	t.Log("Restarting NATS container to simulate connection drop")
	err = s.StopService(ctx, NatsServiceName)
	s.Require().NoError(err, "Failed to stop NATS container")
	time.Sleep(2 * time.Second) // Ensure it stopped
	err = s.StartService(ctx, NatsServiceName)
	s.Require().NoError(err, "Failed to restart NATS container")

	// Wait for NATS to be ready again
	t.Log("Waiting for NATS to become ready...")
	err = WaitUntilNatsReady(ctx, s.NATSURL)
	s.Require().NoError(err, "NATS did not become ready after restart")
	t.Log("NATS is ready.")

	// Create second test payload
	testPayload2, err := s.GenerateNatsPayload(fullChatSubject, &model.UpsertChatPayload{ChatID: chatID2, CompanyID: tenantID, Jid: "628333@w.net"})
	s.Require().NoError(err, "Failed to generate payload 2")

	// Publish another message after NATS restarts
	t.Log("Publishing message after connection recovery")
	err = s.PublishEvent(ctx, fullChatSubject, testPayload2)
	s.Require().NoError(err, "Failed to publish message after NATS restart")
	time.Sleep(5 * time.Second)

	// Verify the new message was processed
	verifyQuery2 := "SELECT COUNT(*) FROM chats WHERE chat_id = $1 AND company_id = $2"
	processed2, err := verifyPostgresDataWithSchema(ctx, s.PostgresDSN, tenantSchema, verifyQuery2, 1, chatID2, tenantID)
	s.Require().NoError(err, "Failed to execute query for second message")
	s.Assert().True(processed2, "Message 2 should be processed after NATS restart")
}
