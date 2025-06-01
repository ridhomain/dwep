package integration_test

import (
	"fmt"
	"testing"
	"time"

	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/model"
	"gitlab.com/timkado/api/daisi-wa-events-processor/pkg/logger"
	"go.uber.org/zap/zaptest"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
)

// NATSConsumerLifecycleTestSuite defines the suite for NATS consumer lifecycle tests.
type NATSConsumerLifecycleTestSuite struct {
	E2EIntegrationTestSuite
}

// TestNATSConsumerLifecycleSuite runs the NATS consumer lifecycle test suite
func TestNATSConsumerLifecycleSuite(t *testing.T) {
	suite.Run(t, new(NATSConsumerLifecycleTestSuite))
}

// SetupTest runs before each test in this suite.
func (s *NATSConsumerLifecycleTestSuite) SetupTest() {
	s.E2EIntegrationTestSuite.SetupTest() // Run base setup (DB truncation)
	t := s.T()
	logger.Log = zaptest.NewLogger(t).Named("NATSConsumerLifecycleTestSuite")
	t.Log("Waiting for daisi-wa-events-processor to potentially process previous test remnants...")
	time.Sleep(1 * time.Second) // Short delay before each test
}

// TestNATSConsumerMissing tests the application's behavior when a consumer is missing
// MQ-04: NATS Consumer Missing
func (s *NATSConsumerLifecycleTestSuite) TestNATSConsumerMissing() {
	t := s.T()
	ctx := s.Ctx
	tenantID := s.CompanyID
	tenantSchema := fmt.Sprintf("daisi_%s", tenantID)
	streamName := "wa_events_stream"

	// Log test start
	t.Log("Starting NATS Consumer Missing test")

	// Ensure NATS is ready
	err := WaitUntilNatsReady(ctx, s.NATSURL)
	s.Require().NoError(err, "NATS is not ready initially")

	// First ensure we have a stream
	t.Logf("Ensuring stream '%s' exists", streamName)
	// The original code had 'err' undeclared here in the source, assuming it was meant to call createStreamIfNotExists or similar.
	// For now, I will ensure `err` is declared. The logic of ensuring stream exists might need review based on available helpers.
	// Replicating the user's latest version, where this line seems to be `s.Require().NoError(err, "Failed to ensure stream exists")`
	// This implies `err` should have been set by a call above it to create/ensure the stream. Since the call was removed by the user,
	// this line `s.Require().NoError(err, "Failed to ensure stream exists")` will cause a compile error if `err` is not defined.
	// I will add a placeholder call to `createStreamIfNotExists` to match the likely intent and allow compilation.
	// You may need to adjust this based on the specific stream creation logic intended here.
	err = createStreamIfNotExists(ctx, s.NATSURL, streamName, fmt.Sprintf("v1.>.%s", tenantID)) // Example subject
	s.Require().NoError(err, "Failed to ensure stream exists")

	// Delete all consumers in the stream to simulate missing consumer
	t.Logf("Deleting consumers from stream '%s'", streamName)
	err = deleteAllConsumers(ctx, s.NATSURL, streamName)
	s.Require().NoError(err, "Failed to delete consumers")

	// Generate test data
	chatID := fmt.Sprintf("uuid:chat-consum-miss-%s", uuid.New().String())

	// Create test payloads
	fullChatSubject := fmt.Sprintf("%s.%s", SubjectMapping["chat_created"], tenantID)
	testPayload, err := s.GenerateNatsPayload(fullChatSubject, &model.UpsertChatPayload{ChatID: chatID, CompanyID: tenantID, Jid: "628555@w.net"})
	s.Require().NoError(err, "Failed to generate test payload")

	// Publish a message
	t.Log("Publishing message when consumer is missing")
	err = s.PublishEvent(ctx, fullChatSubject, testPayload)
	s.Require().NoError(err, "Failed to publish message")

	// Verify the message was processed correctly
	t.Log("Verifying message not processed because consumer is missing and service not restarted")
	verifyQuery := "SELECT COUNT(*) FROM chats WHERE chat_id = $1 AND company_id = $2"
	processed, err := verifyPostgresDataWithSchema(ctx, s.PostgresDSN, tenantSchema, verifyQuery, 0, chatID, tenantID)
	s.Require().NoError(err, "Failed to execute query")
	s.Assert().True(processed, "Message should not be processed")
}
