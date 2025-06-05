package usecase

import (
	"context"
	"fmt"

	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/storage"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/tenant"
)

// EventService implements both historical and realtime event processing
type EventService struct {
	chatRepo           storage.ChatRepo
	messageRepo        storage.MessageRepo
	contactRepo        storage.ContactRepo
	agentRepo          storage.AgentRepo
	onboardingLogRepo  storage.OnboardingLogRepo
	exhaustedEventRepo storage.ExhaustedEventRepo
	onboardingWorker   IOnboardingWorker // Use the interface type
}

// NewEventService creates a new event service
func NewEventService(
	chatRepo storage.ChatRepo,
	messageRepo storage.MessageRepo,
	contactRepo storage.ContactRepo,
	agentRepo storage.AgentRepo,
	onboardingLogRepo storage.OnboardingLogRepo,
	exhaustedEventRepo storage.ExhaustedEventRepo,
	onboardingWorker IOnboardingWorker,
) *EventService {
	return &EventService{
		chatRepo:           chatRepo,
		messageRepo:        messageRepo,
		contactRepo:        contactRepo,
		agentRepo:          agentRepo,
		onboardingLogRepo:  onboardingLogRepo,
		exhaustedEventRepo: exhaustedEventRepo,
		onboardingWorker:   onboardingWorker, // Assign worker pool
	}
}

// validateClusterTenant validates that the cluster field matches the tenant ID from context
func validateClusterTenant(ctx context.Context, cluster string) error {
	if cluster == "" {
		return nil // Skip validation if cluster is not provided
	}

	companyID, err := tenant.FromContext(ctx)
	if err != nil {
		return fmt.Errorf("failed to get tenant ID: %w", err)
	}

	if cluster != companyID {
		return fmt.Errorf("cluster (%s) does not match tenant ID (%s)", cluster, companyID)
	}

	return nil
}
