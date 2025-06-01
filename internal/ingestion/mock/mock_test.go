package mock

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/model"
)

// exampleHandler is a simple EventHandler implementation for testing
func exampleHandler(ctx context.Context, eventType model.EventType, metadata *model.MessageMetadata, rawEvent []byte) error {
	return nil
}

// TestRouterMock demonstrates how to use the RouterMock
func TestRouterMock(t *testing.T) {
	// Create the mock
	mockRouter := new(RouterMock)

	// Set up expectations
	eventType := model.EventType("test.event")
	mockRouter.On("Register", eventType, mock.Anything).Return()

	// Use the mock
	mockRouter.Register(eventType, exampleHandler)

	// Assert expectations are met (no errors)
	mockRouter.AssertExpectations(t)
}

// TestConsumerMock demonstrates how to use the ConsumerMock
func TestConsumerMock(t *testing.T) {
	// Create the mock
	mockConsumer := new(ConsumerMock)

	// Set up expectations
	mockConsumer.On("Setup").Return(nil)
	mockConsumer.On("Subscribe").Return(nil)
	mockConsumer.On("Start").Return(nil)
	mockConsumer.On("Stop").Return()

	// Use the mock
	err := mockConsumer.Setup()
	assert.NoError(t, err)

	err = mockConsumer.Subscribe()
	assert.NoError(t, err)

	err = mockConsumer.Start()
	assert.NoError(t, err)

	mockConsumer.Stop()

	// Assert
	mockConsumer.AssertExpectations(t)
}

// TestConsumerError demonstrates error handling with mocks
func TestConsumerError(t *testing.T) {
	// Create the mock
	mockConsumer := new(ConsumerMock)

	// Set up expectations with error
	expectedErr := errors.New("setup failed")
	mockConsumer.On("Setup").Return(expectedErr)

	// Use the mock
	err := mockConsumer.Setup()

	// Assert
	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
	mockConsumer.AssertExpectations(t)
}
