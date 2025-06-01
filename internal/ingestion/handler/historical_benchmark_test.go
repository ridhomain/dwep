package handler_test

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/apperrors"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/ingestion/handler"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/model"
	mocket "gitlab.com/timkado/api/daisi-wa-events-processor/internal/storage/mock" // Renamed to avoid conflict
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/usecase"
)

// Note: testCompanyID and MockOnboardingWorker are defined in realtime_benchmark_test.go
// TestMain is also defined there and will cover both benchmark files.

// newBenchmarkContext is also in realtime_benchmark_test.go

func setupHistoricalHandlerWithMocks(b *testing.B) (*handler.HistoricalHandler, *mocket.RepositoryMock) {
	mockRepo := new(mocket.RepositoryMock)

	// Setup BulkUpsert expectations
	mockRepo.ChatRepoMock.On("BulkUpsert", mock.Anything, mock.AnythingOfType("[]model.Chat")).Return(nil)
	mockRepo.MessageRepoMock.On("BulkUpsert", mock.Anything, mock.AnythingOfType("[]model.Message")).Return(nil)
	mockRepo.ContactRepoMock.On("BulkUpsert", mock.Anything, mock.AnythingOfType("[]model.Contact")).Return(nil)

	// Onboarding related mocks (since historical messages can also trigger onboarding checks via the worker)
	mockRepo.ContactRepoMock.On("FindByPhoneAndAgentID", mock.Anything, mock.AnythingOfType("string"), mock.AnythingOfType("string")).Return(nil, apperrors.ErrNotFound) // Default to not found for new onboarding
	mockRepo.ContactRepoMock.On("Save", mock.Anything, mock.AnythingOfType("model.Contact")).Return(nil)                                                                 // For individual contact saves triggered by worker
	mockRepo.OnboardingLogRepoMock.On("FindByMessageID", mock.Anything, mock.AnythingOfType("string")).Return(nil, apperrors.ErrNotFound)                                // Default to not found for new log
	mockRepo.OnboardingLogRepoMock.On("Save", mock.Anything, mock.AnythingOfType("model.OnboardingLog")).Return(nil)

	var onboardingWorker usecase.IOnboardingWorker = &MockOnboardingWorker{} // Defined in realtime_benchmark_test.go

	eventSvc := usecase.NewEventService(
		&mockRepo.ChatRepoMock,
		&mockRepo.MessageRepoMock,
		&mockRepo.ContactRepoMock,
		&mockRepo.AgentRepoMock, // Included for completeness of EventService, though not directly used by historical handlers here
		&mockRepo.OnboardingLogRepoMock,
		&mockRepo.ExhaustedEventRepoMock,
		onboardingWorker,
	)

	histHandler := handler.NewHistoricalHandler(eventSvc)
	return histHandler, mockRepo
}

// --- Benchmark Functions for Historical Handler ---

func BenchmarkHistoricalHandler_Chats(b *testing.B) {
	histHandler, mockRepo := setupHistoricalHandlerWithMocks(b)
	ctx := newBenchmarkContext(testCompanyID)
	metadata := &model.MessageMetadata{
		CompanyID: testCompanyID,
		Domain:    "test.domain",
		Stream:    "wa_history_events_stream",
		MessageID: uuid.NewString(),
	}

	counts := []int{1, 10, 100}

	for _, count := range counts {
		b.Run(fmt.Sprintf("count=%d", count), func(b *testing.B) {
			// Ensure CompanyID in payload matches context for validation to pass
			overrides := &model.UpsertChatPayload{CompanyID: testCompanyID}
			payload := model.NewHistoryChatPayload(&count, overrides)
			rawEvent, _ := json.Marshal(payload)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				err := histHandler.HandleEvent(ctx, model.V1HistoricalChats, metadata, rawEvent)
				if err != nil {
					b.Fatalf("HandleEvent (V1HistoricalChats, count=%d) returned error: %v", count, err)
				}
			}
			b.StopTimer()
			// Assert expectations for each sub-benchmark if mocks are specific to them
			// For general BulkUpsert mocks, asserting once after all sub-benchmarks might be okay,
			// but cleaner to assert per sub-run if possible, or ensure mocks are general.
			// mockRepo.AssertExpectations(b) // This might be too broad if mocks are general.
		})
	}
	// Assert general mock expectations after all sub-benchmarks if they are not count-specific.
	mockRepo.AssertExpectations(b)
}

func BenchmarkHistoricalHandler_Messages(b *testing.B) {
	histHandler, mockRepo := setupHistoricalHandlerWithMocks(b)
	ctx := newBenchmarkContext(testCompanyID)
	metadata := &model.MessageMetadata{
		CompanyID: testCompanyID,
		Domain:    "test.domain",
		Stream:    "wa_history_events_stream",
		MessageID: uuid.NewString(),
	}

	counts := []int{1, 10, 100}

	for _, count := range counts {
		b.Run(fmt.Sprintf("count=%d", count), func(b *testing.B) {
			// Ensure CompanyID in payload matches context
			overrides := &model.UpsertMessagePayload{CompanyID: testCompanyID, ChatID: uuid.NewString()}
			payload := model.NewHistoryMessagePayload(&count, overrides)
			rawEvent, _ := json.Marshal(payload)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				err := histHandler.HandleEvent(ctx, model.V1HistoricalMessages, metadata, rawEvent)
				if err != nil {
					b.Fatalf("HandleEvent (V1HistoricalMessages, count=%d) returned error: %v", count, err)
				}
			}
			b.StopTimer()
		})
	}
	mockRepo.AssertExpectations(b)
}

func BenchmarkHistoricalHandler_Contacts(b *testing.B) {
	histHandler, mockRepo := setupHistoricalHandlerWithMocks(b)
	ctx := newBenchmarkContext(testCompanyID)
	metadata := &model.MessageMetadata{
		CompanyID: testCompanyID,
		Domain:    "test.domain",
		Stream:    "wa_history_events_stream",
		MessageID: uuid.NewString(),
	}

	counts := []int{1, 10, 100}

	for _, count := range counts {
		b.Run(fmt.Sprintf("count=%d", count), func(b *testing.B) {
			// Ensure CompanyID in payload matches context
			overrides := &model.UpsertContactPayload{CompanyID: testCompanyID}
			payload := model.NewHistoryContactPayload(&count, overrides)
			rawEvent, _ := json.Marshal(payload)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				err := histHandler.HandleEvent(ctx, model.V1HistoricalContacts, metadata, rawEvent)
				if err != nil {
					b.Fatalf("HandleEvent (V1HistoricalContacts, count=%d) returned error: %v", count, err)
				}
			}
			b.StopTimer()
		})
	}
	mockRepo.AssertExpectations(b)
}
