package integration_test

import (
	"fmt"
	"testing"
	"time"

	"gitlab.com/timkado/api/daisi-wa-events-processor/pkg/logger"
	"go.uber.org/zap/zaptest"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
)

// NATSStreamLifecycleTestSuite defines the suite for NATS stream lifecycle tests.
type NATSStreamLifecycleTestSuite struct {
	E2EIntegrationTestSuite
}

// TestNATSStreamLifecycleSuite runs the NATS stream lifecycle test suite
func TestNATSStreamLifecycleSuite(t *testing.T) {
	suite.Run(t, new(NATSStreamLifecycleTestSuite))
}

// SetupTest runs before each test in this suite.
func (s *NATSStreamLifecycleTestSuite) SetupTest() {
	s.E2EIntegrationTestSuite.SetupTest() // Run base setup (DB truncation)
	t := s.T()
	logger.Log = zaptest.NewLogger(t).Named("NATSStreamLifecycleTestSuite")
	t.Log("Waiting for daisi-wa-events-processor to potentially process previous test remnants...")
	time.Sleep(1 * time.Second) // Short delay before each test
}

// TestNATSStreamMissing tests the application's behavior when a stream is missing
// MQ-03: NATS Stream Missing
func (s *NATSStreamLifecycleTestSuite) TestNATSStreamMissing() {
	t := s.T()
	ctx := s.Ctx
	tenantID := s.CompanyID
	tenantSchema := fmt.Sprintf("daisi_%s", tenantID)
	streamName := "wa_events_stream" // Assuming this is the target stream

	// Log test start
	t.Log("Starting NATS Stream Missing test with service restart")

	// Ensure NATS is ready before manipulating streams
	err := WaitUntilNatsReady(ctx, s.NATSURL)
	s.Require().NoError(err, "NATS is not ready initially")

	// Delete the stream if it exists
	t.Logf("Deleting stream '%s' if it exists", streamName)
	err = deleteStream(ctx, s.NATSURL, streamName)
	s.Require().NoError(err, "Failed to delete stream or stream doesn't exist")

	// Stop the application container
	t.Log("Stopping application container...")
	if s.Application != nil {
		if termErr := s.Application.Terminate(s.Ctx); termErr != nil {
			s.T().Fatalf("Failed to terminate application container: %v", termErr)
		}
		t.Log("Application container terminated.")
	}

	// Restart the application container
	// The setupTestApp function includes a wait strategy for the app to be ready
	t.Log("Restarting application container...")
	testEnvForApp := &TestEnvironment{ // Defined in main_test.go or a shared file
		Network:            s.Network,
		PostgresDSNNetwork: s.PostgresDSNNetwork,
		NATSURLNetwork:     s.NATSURLNetwork,
		CompanyID:          s.CompanyID,
	}
	var appSetupErr error
	// s.Application and s.AppAPIURL are fields in E2EIntegrationTestSuite
	s.Application, s.AppAPIURL, appSetupErr = setupTestApp(s.Ctx, s.Network.Name, testEnvForApp, nil)
	s.Require().NoError(appSetupErr, "Failed to restart application container")
	s.StreamLogs(t) // Stream logs from the new app container instance
	t.Log("Application container restarted and service should auto-create streams.")

	// Generate test data
	chatID := fmt.Sprintf("uuid:chat-stream-restarted-%s", uuid.New().String())

	// Create test payloads
	fullChatSubject := fmt.Sprintf("%s.%s", SubjectMapping["chat_created"], tenantID)
	testPayload, err := s.GenerateNatsPayload(fullChatSubject, map[string]interface{}{
		"chat_id":    chatID,
		"company_id": tenantID,
		"jid":        "628444@w.net",
	})
	s.Require().NoError(err, "Failed to generate test payload")

	// Try to publish a message - should succeed now as the service restart should have created the stream.
	t.Log("Publishing message after service restart (stream should be auto-created by service)")
	err = s.PublishEvent(ctx, fullChatSubject, testPayload)
	s.Require().NoError(err, "Failed to publish message after service restart. Stream '%s' might not have been auto-created.", streamName)
	t.Log("Publishing succeeded as expected.")

	// Wait for the service to process the message.
	// The service's NATS consumers should be active after restart.
	t.Log("Waiting for service to process message...")
	time.Sleep(10 * time.Second) // Adjusted wait time, can be tuned.

	// Verify the message was processed correctly (implying stream was created and consumer is working)
	t.Log("Verifying message was processed")
	verifyQuery := "SELECT COUNT(*) FROM chats WHERE chat_id = $1 AND company_id = $2"
	processed, err := verifyPostgresDataWithSchema(ctx, s.PostgresDSN, tenantSchema, verifyQuery, 1, chatID, tenantID)
	s.Require().NoError(err, "Failed to execute query to verify message processing")
	s.Assert().True(processed, "Message should be processed after service restart and stream auto-creation")

	// Verify stream now exists
	t.Logf("Verifying stream '%s' exists after service restart", streamName)
	streamExists, streamErr := checkStreamExists(ctx, s.NATSURL, streamName)
	s.Require().NoError(streamErr, "Failed to check stream existence after service restart")
	s.Assert().True(streamExists, "Stream '%s' should exist after the service restart and auto-creation", streamName)
}
