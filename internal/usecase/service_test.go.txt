package usecase

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	storagemock "gitlab.com/timkado/api/daisi-wa-events-processor/internal/storage/mock"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/tenant"
	"gitlab.com/timkado/api/daisi-wa-events-processor/pkg/logger"
	"go.uber.org/zap/zaptest"
)

func TestValidateClusterTenant(t *testing.T) {
	logger.Log = zaptest.NewLogger(t).Named("test")
	tests := []struct {
		name          string
		ctx           context.Context
		cluster       string
		expectError   bool
		errorContains string
	}{
		{
			name:          "Success - Match",
			ctx:           tenant.WithCompanyID(context.Background(), "tenant-abc"),
			cluster:       "tenant-abc",
			expectError:   false,
			errorContains: "",
		},
		{
			name:          "Success - Empty Cluster",
			ctx:           tenant.WithCompanyID(context.Background(), "tenant-abc"),
			cluster:       "",
			expectError:   false,
			errorContains: "",
		},
		{
			name:          "Failure - Mismatch",
			ctx:           tenant.WithCompanyID(context.Background(), "tenant-abc"),
			cluster:       "tenant-xyz",
			expectError:   true,
			errorContains: "does not match tenant ID",
		},
		{
			name:          "Failure - No Company in Context",
			ctx:           context.Background(), // No tenant ID
			cluster:       "tenant-abc",
			expectError:   true,
			errorContains: "failed to get tenant ID",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateClusterTenant(tt.ctx, tt.cluster)
			if tt.expectError {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorContains)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestNewEventService(t *testing.T) {
	// Initialize mocks
	chatRepo := &storagemock.ChatRepoMock{}
	messageRepo := &storagemock.MessageRepoMock{}
	contactRepo := &storagemock.ContactRepoMock{}
	agentRepo := &storagemock.AgentRepoMock{}
	onboardingLogRepo := &storagemock.OnboardingLogRepoMock{}
	exhaustedEventRepo := &storagemock.ExhaustedEventRepoMock{}
	onboardingWorker := new(MockOnboardingWorker)

	svc := NewEventService(chatRepo, messageRepo, contactRepo, agentRepo, onboardingLogRepo, exhaustedEventRepo, onboardingWorker)

	// Basic assertion to ensure the service is created
	assert.NotNil(t, svc)

	// Add more test cases as needed
}
