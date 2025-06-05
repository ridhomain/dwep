// internal/usecase/onboarding_types.go

package usecase

import (
	"context"

	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/model"
	"gorm.io/datatypes"
)

// OnboardingTaskData holds the necessary data for an onboarding task
type OnboardingTaskData struct {
	Ctx                context.Context        // Context for the task
	Message            model.Message          // The message being processed
	MetadataJSON       datatypes.JSON         // NATS metadata
	OnboardingMetadata map[string]interface{} // Onboarding-specific metadata from upstream
}

// IOnboardingWorker defines the interface for the onboarding worker
type IOnboardingWorker interface {
	SubmitTask(taskData OnboardingTaskData) error
	Stop()
}
