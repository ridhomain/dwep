package mock

import (
	"github.com/stretchr/testify/mock"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/ingestion"
)

// ConsumerMock is a mock implementation of the ingestion.ConsumerInterface
type ConsumerMock struct {
	mock.Mock
}

// Ensure ConsumerMock implements ConsumerInterface
var _ ingestion.ConsumerInterface = (*ConsumerMock)(nil)

// Setup mocks the Setup method
func (m *ConsumerMock) Setup() error {
	args := m.Called()
	return args.Error(0)
}

// Subscribe mocks the Subscribe method
func (m *ConsumerMock) Subscribe() error {
	args := m.Called()
	return args.Error(0)
}

// Start mocks the Start method
func (m *ConsumerMock) Start() error {
	args := m.Called()
	return args.Error(0)
}

// Stop mocks the Stop method
func (m *ConsumerMock) Stop() {
	m.Called()
}

// Example usage:
/*
func TestWithMockConsumer(t *testing.T) {
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
*/
