package mock

import (
	"context"
	"errors"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// Example service that uses the JetStream client
type ExampleService struct {
	client *ClientMock
}

// SetupSubscriptions demonstrates setting up subscriptions using the new signatures
func (s *ExampleService) SetupSubscriptions(ctx context.Context) error {
	// Create dummy configs for testing
	dummyStreamCfg := &nats.StreamConfig{
		Name:     "test-stream",
		Subjects: []string{"test.subject"},
	}
	dummyConsumerCfg := &nats.ConsumerConfig{
		Durable:        "consumer1",
		DeliverGroup:   "group1",
		FilterSubjects: []string{"test.subject"},
	}

	err := s.client.SetupStream(ctx, dummyStreamCfg)
	if err != nil {
		return err
	}

	err = s.client.SetupConsumer(ctx, "test-stream", dummyConsumerCfg)
	if err != nil {
		return err
	}

	_, err = s.client.Subscribe("test.subject", "consumer1", "group1", func(msg *nats.Msg) {
		// Handle message
	})
	return err
}

// PublishMessage demonstrates publishing a message
func (s *ExampleService) PublishMessage(message []byte) error {
	return s.client.Publish("test.subject", message, nil)
}

// TestClientMock demonstrates how to use the ClientMock
func TestClientMock(t *testing.T) {
	// Create a new mock
	mockClient := new(ClientMock)

	// Create a service that uses the mock
	service := &ExampleService{
		client: mockClient,
	}

	// Set up expectations matching the new signatures
	mockClient.On("SetupStream", mock.Anything, mock.AnythingOfType("*nats.StreamConfig")).Return(nil)
	mockClient.On("SetupConsumer", mock.Anything, "test-stream", mock.AnythingOfType("*nats.ConsumerConfig")).Return(nil)
	mockClient.On("Subscribe", "test.subject", "consumer1", "group1", mock.Anything).Return(MockSubscription(), nil)
	mockClient.On("Publish", "test.subject", []byte("test message"), mock.Anything).Return(nil)

	// Test successful subscription setup
	err := service.SetupSubscriptions(context.Background())
	assert.NoError(t, err)

	// Test successful message publishing
	err = service.PublishMessage([]byte("test message"))
	assert.NoError(t, err)

	// Verify all expectations were met
	mockClient.AssertExpectations(t)
}

// TestClientMockErrors demonstrates error handling with the mock
func TestClientMockErrors(t *testing.T) {
	// Create a new mock
	mockClient := new(ClientMock)

	// Create a service that uses the mock
	service := &ExampleService{
		client: mockClient,
	}

	// Set up expectations with errors
	expectedErr := errors.New("stream setup failed")
	mockClient.On("SetupStream", mock.Anything, mock.AnythingOfType("*nats.StreamConfig")).Return(expectedErr)

	// Test error handling
	err := service.SetupSubscriptions(context.Background())
	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)

	// Verify expectations
	mockClient.AssertExpectations(t)
}
