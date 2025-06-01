package usecase

import (
	"context"

	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/model"
)

// ExhaustedService handles operations related to exhausted DLQ events.

// SaveExhaustedEvent calls the repository to save the exhausted event.
func (s *EventService) SaveExhaustedEvent(ctx context.Context, event model.ExhaustedEvent) error {
	// Note: EventService now holds the ExhaustedEventRepo directly
	err := s.exhaustedEventRepo.Save(ctx, event)
	if err != nil {
		// The repository layer already wraps the error appropriately.
		// Consider adding specific logging here if needed.
		return err
	}
	return nil
}
