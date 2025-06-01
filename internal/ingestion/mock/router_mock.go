package mock

import (
	"context"

	"github.com/stretchr/testify/mock"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/ingestion"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/model"
)

// RouterMock is a mock implementation of the ingestion.RouterInterface
type RouterMock struct {
	mock.Mock
}

// Ensure RouterMock implements RouterInterface
var _ ingestion.RouterInterface = (*RouterMock)(nil)

// Register mocks the Register method
func (m *RouterMock) Register(eventType model.EventType, handler ingestion.EventHandler) {
	m.Called(eventType, handler)
}

// RegisterDefault mocks the RegisterDefault method
func (m *RouterMock) RegisterDefault(handler ingestion.EventHandler) {
	m.Called(handler)
}

// Route mocks the Route method
func (m *RouterMock) Route(ctx context.Context, metadata *model.MessageMetadata, rawEvent []byte) error {
	args := m.Called(ctx, metadata, rawEvent)
	return args.Error(0)
}

// Example usage:
/*
func TestWithMockRouter(t *testing.T) {
	// Create the mock
	mockRouter := new(RouterMock)

	// Set up expectations
	eventType := model.EventType("test.event")
	mockRouter.On("Register", eventType, mock.AnythingOfType("ingestion.EventHandler")).Return()
	mockRouter.On("Route", mock.Anything, mock.AnythingOfType("*model.MessageMetadata"), mock.Anything).Return(nil)

	// Use the mock
	mockRouter.Register(eventType, func(ctx context.Context, eventType model.EventType, metadata *model.MessageMetadata, rawEvent []byte) error {
		return nil
	})

	metadata := &model.MessageMetadata{MessageSubject: "test.event"}
	err := mockRouter.Route(context.Background(), metadata, []byte("test data"))

	// Assert
	assert.NoError(t, err)
	mockRouter.AssertExpectations(t)
}
*/
