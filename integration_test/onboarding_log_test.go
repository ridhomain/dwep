package integration_test

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"gitlab.com/timkado/api/daisi-wa-events-processor/pkg/logger"
	"go.uber.org/zap/zaptest"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/model"
)

// OnboardingLogTestSuite inherits from E2EIntegrationTestSuite
// to get access to DB, NATS, Tenant info, and helper methods.
type OnboardingLogTestSuite struct {
	E2EIntegrationTestSuite // Embed the base suite
}

// TestOnboardingLogSuite runs the OnboardingLog test suite
func TestOnboardingLogSuite(t *testing.T) {
	suite.Run(t, new(OnboardingLogTestSuite))
}

// SetupTest runs before each test in this suite.
func (s *OnboardingLogTestSuite) SetupTest() {
	s.E2EIntegrationTestSuite.SetupTest() // Run base setup
	// Add any specific setup for onboarding log tests if needed
	t := s.T()
	logger.Log = zaptest.NewLogger(t).Named("OnboardingLogTestSuite")
	t.Log("Waiting for daisi-wa-events-processor before onboarding log test...")
	time.Sleep(500 * time.Millisecond)
}

// Helper to generate a message payload for onboarding tests
func (s *OnboardingLogTestSuite) generateOnboardingMessagePayload(t require.TestingT, tenantID, phoneNumber, messageID string) []byte {
	payload := model.UpsertMessagePayload{
		MessageID:        messageID,
		ToPhone:          "agent-onboarding-test",
		FromPhone:        phoneNumber, // Key field for onboarding
		ChatID:           fmt.Sprintf("chat-onboarding-%s", uuid.NewString()),
		Jid:              fmt.Sprintf("%s@s.whatsapp.net", phoneNumber),
		Flow:             "IN", // Must be IN flow
		CompanyID:        tenantID,
		AgentID:          "agent-onboarding-test",
		Key:              &model.KeyPayload{ID: uuid.NewString(), FromMe: false, RemoteJid: fmt.Sprintf("%s@s.whatsapp.net", phoneNumber)},
		MessageObj:       map[string]interface{}{"conversation": "Onboarding test message"},
		Status:           "read",
		MessageTimestamp: time.Now().Unix(),
	}

	payloadBytes, err := json.Marshal(payload)
	require.NoError(t, err, "Failed to marshal onboarding message payload")
	return payloadBytes
}

// TestOnboardingLogCreation verifies that a log is created for an incoming message
// from a phone number not yet in the contacts table.
func (s *OnboardingLogTestSuite) TestOnboardingLogCreation() {
	t := s.T()
	tenantID := s.CompanyID // Access from embedded suite
	ctx := s.Ctx            // Use suite context
	// Generate a unique phone number unlikely to exist
	phoneNumber := fmt.Sprintf("6299%d", time.Now().UnixNano()%10000000)
	messageID := fmt.Sprintf("onboarding-create-%s", uuid.NewString())

	// Ensure contact does NOT exist (optional, good practice)
	contactCheckQuery := fmt.Sprintf("SELECT COUNT(*) FROM %q.contacts WHERE phone_number = $1 AND company_id = $2", s.CompanySchemaName)
	contactExists, err := s.VerifyPostgresData(ctx, contactCheckQuery, 0, phoneNumber, tenantID) // Expect 0 rows
	require.NoError(t, err, "Failed to check for existing contact")
	require.True(t, contactExists, "Test setup failed: Contact already exists for %s", phoneNumber)

	// 1. Generate and Publish IN flow message
	payloadBytes := s.generateOnboardingMessagePayload(t, tenantID, phoneNumber, messageID)
	subject := fmt.Sprintf("%s.%s", model.V1MessagesUpsert, tenantID)
	err = s.PublishEvent(ctx, subject, payloadBytes) // Access from embedded suite
	require.NoError(t, err, "Failed to publish onboarding message")

	// 2. Wait for processing
	time.Sleep(2 * time.Second) // Adjust sleep time as needed

	// 3. Verify Onboarding Log Entry
	logCheckQuery := fmt.Sprintf("SELECT COUNT(*) FROM %q.onboarding_log WHERE message_id = $1 AND company_id = $2 AND phone_number = $3", s.CompanySchemaName)
	logExists, err := s.VerifyPostgresData(ctx, logCheckQuery, 1, messageID, tenantID, phoneNumber) // Expect 1 row
	require.NoError(t, err, "Failed to query onboarding_log table")
	assert.True(t, logExists, "Onboarding log entry should have been created")
	// We could add QueryRowScan here if we needed to check Timestamp/CreatedAt values specifically
}

// TestOnboardingLogDuplicatePrevention verifies that sending the same message again
// does not create a duplicate onboarding log entry.
func (s *OnboardingLogTestSuite) TestOnboardingLogDuplicatePrevention() {
	t := s.T()
	tenantID := s.CompanyID // Access from embedded suite
	ctx := s.Ctx            // Use suite context
	phoneNumber := fmt.Sprintf("6288%d", time.Now().UnixNano()%10000000)
	messageID := fmt.Sprintf("onboarding-dupe-%s", uuid.NewString())

	// 1. Generate and Publish first message
	payloadBytes := s.generateOnboardingMessagePayload(t, tenantID, phoneNumber, messageID)
	subject := fmt.Sprintf("%s.%s", model.V1MessagesUpsert, tenantID)
	err := s.PublishEvent(ctx, subject, payloadBytes) // Access from embedded suite
	require.NoError(t, err, "Failed to publish first onboarding message")
	time.Sleep(2 * time.Second) // Wait for first processing

	// Verify first log entry exists
	logCountQuery := fmt.Sprintf("SELECT COUNT(*) FROM %q.onboarding_log WHERE message_id = $1 AND company_id = $2", s.CompanySchemaName)
	firstLogExists, err := s.VerifyPostgresData(ctx, logCountQuery, 1, messageID, tenantID)
	require.NoError(t, err, "Failed to count initial onboarding log")
	require.True(t, firstLogExists, "Initial onboarding log was not created")

	// 2. Publish the *exact same* message again
	err = s.PublishEvent(ctx, subject, payloadBytes) // Access from embedded suite
	require.NoError(t, err, "Failed to publish duplicate onboarding message")
	time.Sleep(2 * time.Second) // Wait for second processing

	// 3. Verify *still* only one log entry exists
	secondLogCheck, err := s.VerifyPostgresData(ctx, logCountQuery, 1, messageID, tenantID) // Still expect 1
	require.NoError(t, err, "Failed to count onboarding log after duplicate send")
	assert.True(t, secondLogCheck, "Duplicate onboarding log was created (should remain 1)")
}

// TestOnboardingLogSkipExistingContact verifies that no log is created if the contact
// already exists for the incoming message's phone number.
func (s *OnboardingLogTestSuite) TestOnboardingLogSkipExistingContact() {
	t := s.T()
	tenantID := s.CompanyID // Access from embedded suite
	ctx := s.Ctx            // Use suite context
	phoneNumber := fmt.Sprintf("6277%d", time.Now().UnixNano()%10000000)
	messageID := fmt.Sprintf("onboarding-skip-%s", uuid.NewString())

	// 1. Create the contact directly using ExecuteNonQuery
	contactID := fmt.Sprintf("contact-skip-%s", uuid.NewString())
	insertQuery := fmt.Sprintf(`INSERT INTO %q.contacts (id, phone_number, company_id, agent_id, created_at, updated_at) 
	                 VALUES ($1, $2, $3, $4, $5, $6)`, s.CompanySchemaName)
	now := time.Now()
	err := s.ExecuteNonQuery(ctx, insertQuery,
		contactID, phoneNumber, tenantID, "agent-predefined", now, now)
	require.NoError(t, err, "Failed to create predefined contact for test setup")

	// 2. Generate and Publish IN flow message for the existing contact
	payloadBytes := s.generateOnboardingMessagePayload(t, tenantID, phoneNumber, messageID)
	subject := fmt.Sprintf("%s.%s", model.V1MessagesUpsert, tenantID)
	err = s.PublishEvent(ctx, subject, payloadBytes) // Access from embedded suite
	require.NoError(t, err, "Failed to publish message for existing contact")

	// 3. Wait for processing
	time.Sleep(2 * time.Second) // Adjust sleep time as needed

	// 4. Verify NO Onboarding Log Entry was created
	logCheckQuery := fmt.Sprintf("SELECT COUNT(*) FROM %q.onboarding_log WHERE message_id = $1 AND company_id = $2", s.CompanySchemaName)
	logExists, err := s.VerifyPostgresData(ctx, logCheckQuery, 0, messageID, tenantID) // Expect 0 rows
	require.NoError(t, err, "Failed to count onboarding log for existing contact scenario")
	assert.True(t, logExists, "Onboarding log was created even though contact existed (should be 0)")
}
