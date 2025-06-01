package mock

import (
	"context"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/mock"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/model"
	"go.uber.org/zap/zaptest"
)

// MockSubscription is a helper for creating a mock nats.Subscription
// This is needed because we can't directly create nats.Subscription instances
func MockSubscription() *nats.Subscription {
	// Return a nil pointer that will be type cast to *nats.Subscription
	// This is a common pattern for mocking interfaces with concrete types
	return nil
}

// WithTestContext returns a context with testing logger and tenant ID
func WithTestContext(t *testing.T, companyID string) context.Context {
	ctx := context.Background()

	// Use a simple approach for testing
	testLogger := zaptest.NewLogger(t)
	ctx = context.WithValue(ctx, contextKey("logger"), testLogger)

	if companyID != "" {
		// Note: in a real implementation you would use tenant.WithTenantID
		// We're keeping it simple here to avoid introducing dependencies
		ctx = context.WithValue(ctx, contextKey("company_id"), companyID)
	}
	return ctx
}

// SetupMessageMetadata creates a MessageMetadata instance for testing
func SetupMessageMetadata(messageID, subject, companyID string) *model.MessageMetadata {
	return &model.MessageMetadata{
		MessageID:        messageID,
		MessageSubject:   subject,
		CompanyID:        companyID,
		StreamSequence:   1,
		ConsumerSequence: 1,
		Stream:           "test_stream",
		Consumer:         "test_consumer",
		NumDelivered:     1,
		NumPending:       0,
	}
}

// contextKey is a helper to create unique context keys
type contextKey string

// CustomAssert provides additional assertions for the mocks
type CustomAssert struct {
	T *testing.T
}

// AssertRouterRegistered checks that all expected routes were registered
func (a *CustomAssert) AssertRouterRegistered(mockRouter *RouterMock, expectedEvents []model.EventType) {
	for _, eventType := range expectedEvents {
		mockRouter.AssertCalled(a.T, "Register", eventType, mock.Anything)
	}
}

// NewAssert creates a new CustomAssert
func NewAssert(t *testing.T) *CustomAssert {
	return &CustomAssert{T: t}
}
