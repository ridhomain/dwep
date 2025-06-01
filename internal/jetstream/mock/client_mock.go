package mock

import (
	"context"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/mock"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/jetstream"
)

// ClientMock is a mock implementation of the JetStream Client
type ClientMock struct {
	mock.Mock
}

// Ensure ClientMock implements jetstream.ClientInterface
var _ jetstream.ClientInterface = (*ClientMock)(nil)

// SetupStream mocks the SetupStream method
func (m *ClientMock) SetupStream(ctx context.Context, streamConfig *nats.StreamConfig) error {
	args := m.Called(ctx, streamConfig)
	return args.Error(0)
}

// SetupConsumer mocks the SetupConsumer method
func (m *ClientMock) SetupConsumer(ctx context.Context, streamName string, consumerConfig *nats.ConsumerConfig) error {
	args := m.Called(ctx, streamName, consumerConfig)
	return args.Error(0)
}

// Subscribe mocks the Subscribe method
func (m *ClientMock) Subscribe(subject, consumer, group string, handler nats.MsgHandler) (*nats.Subscription, error) {
	args := m.Called(subject, consumer, group, handler)
	return args.Get(0).(*nats.Subscription), args.Error(1)
}

// SubscribePush mocks the SubscribePush method
func (m *ClientMock) SubscribePush(subject, consumer, group, stream string, handler nats.MsgHandler) (*nats.Subscription, error) {
	args := m.Called(subject, consumer, group, stream, handler)
	return args.Get(0).(*nats.Subscription), args.Error(1)
}

// SubscribePull mocks the SubscribePull method
func (m *ClientMock) SubscribePull(streamName, subject, consumer string) (*nats.Subscription, error) {
	args := m.Called(streamName, subject, consumer)
	return args.Get(0).(*nats.Subscription), args.Error(1)
}

// Publish mocks the Publish method
func (m *ClientMock) Publish(subject string, data []byte, headers map[string]string) error {
	args := m.Called(subject, data, headers)
	return args.Error(0)
}

// NatsConn returns the underlying *nats.Conn (mocked)
func (m *ClientMock) NatsConn() *nats.Conn {
	// For testing purposes, we might not need a real connection.
	// Return nil or a mock connection if necessary for specific tests.
	// For now, returning nil as it satisfies the interface.
	// If a test needs a specific *nats.Conn, it might need to be mocked further.
	args := m.Called()
	if args.Get(0) == nil {
		return nil // Return nil if mock is set up to return nil
	}
	// Allow returning a mocked *nats.Conn if needed
	return args.Get(0).(*nats.Conn)
}

// Close mocks the Close method
func (m *ClientMock) Close() {
	m.Called()
}

// MockSubscription is a helper for creating a mock nats.Subscription
// This is needed because we can't directly create nats.Subscription instances
func MockSubscription() *nats.Subscription {
	// Return a nil pointer that will be type cast to *nats.Subscription
	// This is a common pattern for mocking interfaces with concrete types
	return nil
}

// Example usage:
/*
func TestWithMockClient(t *testing.T) {
	mockClient := new(ClientMock)

	// Setup expectations
	mockClient.On("SetupStream", mock.Anything).Return(nil)
	mockClient.On("SetupConsumer", mock.Anything, "consumer1", "subject.test").Return(nil)
	mockClient.On("Subscribe", "subject.test", "consumer1", "group1", mock.Anything).Return(MockSubscription(), nil)
	mockClient.On("Publish", "subject.test", []byte("test data"), mock.Anything).Return(nil)
	mockClient.On("Close").Return()

	// Call the methods
	err := mockClient.SetupStream(context.Background())
	assert.NoError(t, err)

	err = mockClient.SetupConsumer(context.Background(), "consumer1", "subject.test")
	assert.NoError(t, err)

	sub, err := mockClient.Subscribe("subject.test", "consumer1", "group1", func(msg *nats.Msg) {})
	assert.NoError(t, err)
	assert.NotNil(t, sub)

	err = mockClient.Publish("subject.test", []byte("test data"), nil)
	assert.NoError(t, err)

	mockClient.Close()

	// Verify all expectations were met
	mockClient.AssertExpectations(t)
}
*/
