package handler_test

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/apperrors"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/ingestion/handler"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/model"
	mocket "gitlab.com/timkado/api/daisi-wa-events-processor/internal/storage/mock" // Renamed to avoid conflict
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/tenant"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/usecase"
	"gitlab.com/timkado/api/daisi-wa-events-processor/pkg/logger"
	"gitlab.com/timkado/api/daisi-wa-events-processor/pkg/utils"
	"go.uber.org/zap"
)

const testCompanyID = "benchmark_tenant_001"

// MockOnboardingWorker provides a no-op implementation for IOnboardingWorker
type MockOnboardingWorker struct{}

func (m *MockOnboardingWorker) Start(ctx context.Context)                        {}
func (m *MockOnboardingWorker) Stop()                                            {}
func (m *MockOnboardingWorker) SubmitTask(task usecase.OnboardingTaskData) error { return nil }
func (m *MockOnboardingWorker) ProcessHistoricalWaMessage(payload model.UpsertMessagePayload) error {
	return nil
}

func TestMain(m *testing.M) {
	_ = logger.Initialize("fatal") // Or "error" to reduce noise during benchmarks
	os.Exit(m.Run())
}

func newBenchmarkContext(companyID string) context.Context {
	ctx := context.Background()
	requestID := uuid.NewString()
	ctx = tenant.WithCompanyID(ctx, companyID)
	ctx = tenant.WithRequestID(ctx, requestID)
	// Use the global logger from pkg/logger, initialized in TestMain
	scopedLogger := logger.Log.With(zap.String("company_id", companyID), zap.String("request_id", requestID))
	ctx = logger.WithLogger(ctx, scopedLogger)
	return ctx
}

func setupRealtimeHandlerWithMocks(b *testing.B) (*handler.RealtimeHandler, *mocket.RepositoryMock) {
	mockRepo := new(mocket.RepositoryMock)

	// Common find operations - adjust return values as needed for "update" flows
	mockRepo.ChatRepoMock.On("FindChatByChatID", mock.Anything, mock.AnythingOfType("string")).Return(&model.Chat{CompanyID: testCompanyID}, nil)
	mockRepo.MessageRepoMock.On("FindByMessageID", mock.Anything, mock.AnythingOfType("string")).Return(&model.Message{CompanyID: testCompanyID}, nil)
	mockRepo.ContactRepoMock.On("FindByID", mock.Anything, mock.AnythingOfType("string")).Return(&model.Contact{CompanyID: testCompanyID}, nil)
	mockRepo.AgentRepoMock.On("FindByAgentID", mock.Anything, mock.AnythingOfType("string")).Return(&model.Agent{CompanyID: testCompanyID}, nil)

	// Onboarding related mocks for ContactRepo
	mockRepo.ContactRepoMock.On("FindByPhoneAndAgentID", mock.Anything, mock.AnythingOfType("string"), mock.AnythingOfType("string")).Return(nil, apperrors.ErrNotFound) // Default to not found for new onboarding

	// Setup "Save" (Upsert) expectations
	mockRepo.ChatRepoMock.On("Save", mock.Anything, mock.AnythingOfType("model.Chat")).Return(nil)
	mockRepo.MessageRepoMock.On("Save", mock.Anything, mock.AnythingOfType("model.Message")).Return(nil)
	mockRepo.ContactRepoMock.On("Save", mock.Anything, mock.AnythingOfType("model.Contact")).Return(nil)
	mockRepo.AgentRepoMock.On("Save", mock.Anything, mock.AnythingOfType("model.Agent")).Return(nil) // For V1Agents

	// Onboarding related mocks for OnboardingLogRepo
	mockRepo.OnboardingLogRepoMock.On("FindByMessageID", mock.Anything, mock.AnythingOfType("string")).Return(nil, apperrors.ErrNotFound) // Default to not found for new log
	mockRepo.OnboardingLogRepoMock.On("Save", mock.Anything, mock.AnythingOfType("model.OnboardingLog")).Return(nil)

	// Setup "Update" expectations
	mockRepo.ChatRepoMock.On("Update", mock.Anything, mock.AnythingOfType("model.Chat")).Return(nil)
	mockRepo.MessageRepoMock.On("Update", mock.Anything, mock.AnythingOfType("model.Message")).Return(nil)
	mockRepo.ContactRepoMock.On("Update", mock.Anything, mock.AnythingOfType("model.Contact")).Return(nil)
	// AgentRepo might use Save for upsert logic or a specific UpdateStatus, adjust as needed.
	// For V1Agents which is UpsertAgent, "Save" above should cover it if it's an upsert.
	// If it uses a dedicated Update for agents, that mock should be added.

	var onboardingWorker usecase.IOnboardingWorker = &MockOnboardingWorker{}

	eventSvc := usecase.NewEventService(
		&mockRepo.ChatRepoMock,
		&mockRepo.MessageRepoMock,
		&mockRepo.ContactRepoMock,
		&mockRepo.AgentRepoMock,
		&mockRepo.OnboardingLogRepoMock,
		&mockRepo.ExhaustedEventRepoMock,
		onboardingWorker,
	)

	rtHandler := handler.NewRealtimeHandler(eventSvc)
	return rtHandler, mockRepo
}

// --- Benchmark Functions ---

// BenchmarkRealtimeHandler_ChatUpsert benchmarks V1ChatsUpsert event
func BenchmarkRealtimeHandler_ChatUpsert(b *testing.B) {
	rtHandler, mockRepo := setupRealtimeHandlerWithMocks(b)
	ctx := newBenchmarkContext(testCompanyID)

	payload := model.NewUpsertChatPayload(&model.UpsertChatPayload{CompanyID: testCompanyID})
	rawEvent, _ := json.Marshal(payload)
	metadata := &model.MessageMetadata{
		CompanyID: testCompanyID,
		Domain:    "test.domain",
		Stream:    "message_events_stream",
		MessageID: uuid.NewString(),
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := rtHandler.HandleEvent(ctx, model.V1ChatsUpsert, metadata, rawEvent)
		if err != nil {
			b.Fatalf("HandleEvent (V1ChatsUpsert) returned error: %v", err)
		}
	}
	b.StopTimer()
	mockRepo.AssertExpectations(b)
}

// BenchmarkRealtimeHandler_ChatUpdate benchmarks V1ChatsUpdate event
func BenchmarkRealtimeHandler_ChatUpdate(b *testing.B) {
	rtHandler, mockRepo := setupRealtimeHandlerWithMocks(b)
	ctx := newBenchmarkContext(testCompanyID)

	// Ensure the ID used in payload matches an ID that FindChatByChatID would be called with
	existingChatID := uuid.NewString()
	// mockRepo.ChatRepoMock.ExpectedCalls = nil // Clear previous general expectations for this specific setup
	// mockRepo.ChatRepoMock.On("FindChatByChatID", mock.Anything, existingChatID).Return(&model.Chat{ChatID: existingChatID, CompanyID: testCompanyID}, nil).Once()
	// mockRepo.ChatRepoMock.On("Update", mock.Anything, mock.MatchedBy(func(c model.Chat) bool { return c.ChatID == existingChatID })).Return(nil).Once()

	payload := model.UpdateChatPayload{ChatID: existingChatID, CompanyID: testCompanyID, ConversationTimestamp: utils.Now().Unix(), UnreadCount: 1}
	rawEvent, _ := json.Marshal(payload)
	metadata := &model.MessageMetadata{
		CompanyID: testCompanyID,
		Domain:    "test.domain",
		Stream:    "message_events_stream",
		MessageID: uuid.NewString(),
	}

	b.ReportAllocs()
	// b.ResetTimer() // ResetTimer is called before the loop by default
	// The specific mocks here with .Once() will cause issues for b.N.
	// Let's rely on the general mocks and remove the specific ones here.
	// Re-adding loop and removing specific mocks from here to rely on general setup
	// Remove the specific mockRepo lines above.
	// mockRepo.ChatRepoMock.ExpectedCalls = nil // Clear old one
	// mockRepo.MessageRepoMock.ExpectedCalls = nil
	// mockRepo.ContactRepoMock.ExpectedCalls = nil
	// mockRepo.AgentRepoMock.ExpectedCalls = nil
	// Re-setup general mocks from setupRealtimeHandlerWithMocks for this specific case or make them more robust.
	// For update, we need Find to succeed and then Update to succeed.
	// The `setupRealtimeHandlerWithMocks` should handle this with general mocks.
	// Let's ensure `FindChatByChatID` in `setupRealtimeHandlerWithMocks` returns a chat.

	// b.ResetTimer() // Reset again after mock adjustments if any.
	for i := 0; i < b.N; i++ {
		err := rtHandler.HandleEvent(ctx, model.V1ChatsUpdate, metadata, rawEvent)
		if err != nil {
			b.Fatalf("HandleEvent (V1ChatsUpdate) returned error: %v", err)
		}
	}
	b.StopTimer()
	mockRepo.AssertExpectations(b)
}

// BenchmarkRealtimeHandler_MessageUpsert benchmarks V1MessagesUpsert event
func BenchmarkRealtimeHandler_MessageUpsert(b *testing.B) {
	rtHandler, mockRepo := setupRealtimeHandlerWithMocks(b)
	ctx := newBenchmarkContext(testCompanyID)

	payload := model.NewUpsertMessagePayload(&model.UpsertMessagePayload{CompanyID: testCompanyID, ChatID: uuid.NewString()})
	rawEvent, _ := json.Marshal(payload)
	metadata := &model.MessageMetadata{
		CompanyID: testCompanyID,
		Domain:    "test.domain",
		Stream:    "message_events_stream",
		MessageID: uuid.NewString(),
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := rtHandler.HandleEvent(ctx, model.V1MessagesUpsert, metadata, rawEvent)
		if err != nil {
			b.Fatalf("HandleEvent (V1MessagesUpsert) returned error: %v", err)
		}
	}
	b.StopTimer()
	mockRepo.AssertExpectations(b)
}

// BenchmarkRealtimeHandler_MessageUpdate benchmarks V1MessagesUpdate event
func BenchmarkRealtimeHandler_MessageUpdate(b *testing.B) {
	rtHandler, mockRepo := setupRealtimeHandlerWithMocks(b)
	ctx := newBenchmarkContext(testCompanyID)

	existingMessageID := uuid.NewString()
	// Ensure FindByMessageID is mocked to return a message
	// This is covered by the general mock in setupRealtimeHandlerWithMocks

	payload := model.UpdateMessagePayload{MessageID: existingMessageID, CompanyID: testCompanyID, Status: "read"}
	rawEvent, _ := json.Marshal(payload)
	metadata := &model.MessageMetadata{
		CompanyID: testCompanyID,
		Domain:    "test.domain",
		Stream:    "message_events_stream",
		MessageID: uuid.NewString(),
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := rtHandler.HandleEvent(ctx, model.V1MessagesUpdate, metadata, rawEvent)
		if err != nil {
			b.Fatalf("HandleEvent (V1MessagesUpdate) returned error: %v", err)
		}
	}
	b.StopTimer()
	mockRepo.AssertExpectations(b)
}

// BenchmarkRealtimeHandler_ContactUpsert benchmarks V1ContactsUpsert event
func BenchmarkRealtimeHandler_ContactUpsert(b *testing.B) {
	rtHandler, mockRepo := setupRealtimeHandlerWithMocks(b)
	ctx := newBenchmarkContext(testCompanyID)

	payload := model.NewUpsertContactPayload(&model.UpsertContactPayload{CompanyID: testCompanyID})
	rawEvent, _ := json.Marshal(payload)
	metadata := &model.MessageMetadata{
		CompanyID: testCompanyID,
		Domain:    "test.domain",
		Stream:    "message_events_stream",
		MessageID: uuid.NewString(),
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := rtHandler.HandleEvent(ctx, model.V1ContactsUpsert, metadata, rawEvent)
		if err != nil {
			b.Fatalf("HandleEvent (V1ContactsUpsert) returned error: %v", err)
		}
	}
	b.StopTimer()
	mockRepo.AssertExpectations(b)
}

// BenchmarkRealtimeHandler_ContactUpdate benchmarks V1ContactsUpdate event
func BenchmarkRealtimeHandler_ContactUpdate(b *testing.B) {
	rtHandler, mockRepo := setupRealtimeHandlerWithMocks(b)
	ctx := newBenchmarkContext(testCompanyID)

	existingPhoneNumber := "628888888845"
	// Ensure FindByID for contact is mocked
	// This is covered by the general mock in setupRealtimeHandlerWithMocks

	pushName := "Updated PushName"
	payload := model.UpdateContactPayload{PhoneNumber: existingPhoneNumber, CompanyID: testCompanyID, PushName: &pushName}
	rawEvent, _ := json.Marshal(payload)
	metadata := &model.MessageMetadata{
		CompanyID: testCompanyID,
		Domain:    "test.domain",
		Stream:    "message_events_stream",
		MessageID: uuid.NewString(),
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := rtHandler.HandleEvent(ctx, model.V1ContactsUpdate, metadata, rawEvent)
		if err != nil {
			b.Fatalf("HandleEvent (V1ContactsUpdate) returned error: %v", err)
		}
	}
	b.StopTimer()
	mockRepo.AssertExpectations(b)
}

// BenchmarkRealtimeHandler_AgentEvent benchmarks V1Agents event (UpsertAgent)
func BenchmarkRealtimeHandler_AgentEvent(b *testing.B) {
	rtHandler, mockRepo := setupRealtimeHandlerWithMocks(b)
	ctx := newBenchmarkContext(testCompanyID)

	payload := model.NewUpsertAgentPayload(&model.UpsertAgentPayload{CompanyID: testCompanyID})
	rawEvent, _ := json.Marshal(payload)
	metadata := &model.MessageMetadata{
		CompanyID: testCompanyID,
		Domain:    "test.domain",
		Stream:    "message_events_stream",
		MessageID: uuid.NewString(),
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := rtHandler.HandleEvent(ctx, model.V1Agents, metadata, rawEvent)
		if err != nil {
			b.Fatalf("HandleEvent (V1Agents) returned error: %v", err)
		}
	}
	b.StopTimer()
	mockRepo.AssertExpectations(b)
}
