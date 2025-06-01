package usecase

import (
	"context"
	"fmt"

	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/config"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/ingestion"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/ingestion/handler"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/jetstream"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/model"
	"gitlab.com/timkado/api/daisi-wa-events-processor/pkg/logger"
	"go.uber.org/zap"
)

// Processor orchestrates event processing
type Processor struct {
	service            *EventService
	jsClient           jetstream.ClientInterface
	realtimeConsumer   *ingestion.RealtimeConsumer
	historicalConsumer *ingestion.HistoricalConsumer
	eventRouter        ingestion.RouterInterface
	histHandler        handler.HistoricalHandlerInterface
	realtimeHandler    handler.RealtimeHandlerInterface
}

// NewProcessor creates a new processor with all components wired up
// Accepts the main config object to access NATS settings
func NewProcessor(service *EventService, jsClient jetstream.ClientInterface, cfg *config.Config, companyID string) *Processor {
	// Create the event router (shared by both consumers)
	router := ingestion.NewRouter()

	// Create handlers (used by the router)
	histHandler := handler.NewHistoricalHandler(service)
	realtimeHandler := handler.NewRealtimeHandler(service)

	// Create specific consumers using dedicated config from the main cfg object
	// Append companyID to consumer names for uniqueness
	realtimeCfg := cfg.NATS.Realtime // Access nested config
	realtimeCfg.Consumer = realtimeCfg.Consumer + companyID
	realtimeCfg.QueueGroup = realtimeCfg.QueueGroup + companyID
	// Pass DLQ subject from main config
	realtimeConsumer := ingestion.NewRealtimeConsumer(jsClient, router, realtimeCfg, companyID, cfg.NATS.DLQSubject)

	historicalCfg := cfg.NATS.Historical // Access nested config
	historicalCfg.Consumer = historicalCfg.Consumer + companyID
	historicalCfg.QueueGroup = historicalCfg.QueueGroup + companyID
	// Pass DLQ subject from main config
	historicalConsumer := ingestion.NewHistoricalConsumer(jsClient, router, historicalCfg, companyID, cfg.NATS.DLQSubject)

	return &Processor{
		service:            service,
		jsClient:           jsClient,
		realtimeConsumer:   realtimeConsumer,
		historicalConsumer: historicalConsumer,
		eventRouter:        router,
		histHandler:        histHandler,
		realtimeHandler:    realtimeHandler,
	}
}

// GetRouter returns the processor's event router.
func (p *Processor) GetRouter() ingestion.RouterInterface {
	return p.eventRouter
}

// Setup sets up the processor by registering handlers and setting up both consumers
func (p *Processor) Setup() error {
	// Register handlers for both versioned and legacy event types

	// Register versioned historical event handlers
	p.eventRouter.Register(model.V1HistoricalChats, p.histHandler.HandleEvent)
	p.eventRouter.Register(model.V1HistoricalMessages, p.histHandler.HandleEvent)
	p.eventRouter.Register(model.V1HistoricalContacts, p.histHandler.HandleEvent)

	// Register versioned realtime event handlers
	p.eventRouter.Register(model.V1ChatsUpsert, p.realtimeHandler.HandleEvent)
	p.eventRouter.Register(model.V1ChatsUpdate, p.realtimeHandler.HandleEvent)
	p.eventRouter.Register(model.V1MessagesUpsert, p.realtimeHandler.HandleEvent)
	p.eventRouter.Register(model.V1MessagesUpdate, p.realtimeHandler.HandleEvent)
	p.eventRouter.Register(model.V1ContactsUpsert, p.realtimeHandler.HandleEvent)
	p.eventRouter.Register(model.V1ContactsUpdate, p.realtimeHandler.HandleEvent)
	p.eventRouter.Register(model.V1Agents, p.realtimeHandler.HandleEvent)

	// Default handler for unknown event types, we can use this as dlq or for logging
	p.eventRouter.RegisterDefault(func(ctx context.Context, eventType model.EventType, metadata *model.MessageMetadata, rawEvent []byte) error {
		logger.FromContext(ctx).Warn("Unhandled event type",
			zap.String("type", string(eventType)),
			zap.String("version", eventType.GetVersion()),
			zap.String("base_type", string(eventType.GetBaseType())),
		)
		return nil
	})

	// Setup both consumers
	if err := p.realtimeConsumer.Setup(); err != nil {
		return fmt.Errorf("failed to setup realtime consumer: %w", err)
	}
	if err := p.historicalConsumer.Setup(); err != nil {
		return fmt.Errorf("failed to setup historical consumer: %w", err)
	}

	logger.Log.Info("Processor setup complete for both consumers")
	return nil
}

// Start starts the processor by starting both consumers
func (p *Processor) Start() error {
	logger.Log.Info("Starting event processor with both consumers...")

	// Add panic recovery for the entire processor start sequence
	defer func() {
		if r := recover(); r != nil {
			logger.Log.Error("[panic] Recovered from panic in processor",
				zap.Any("panic", r),
				zap.Stack("stack"),
			)
		}
	}()

	// Start both consumers
	if err := p.realtimeConsumer.Start(); err != nil {
		// Attempt to stop historical if realtime fails to start?
		// For now, just log and return the error.
		p.historicalConsumer.Stop() // Stop historical if realtime failed
		return fmt.Errorf("failed to start realtime consumer: %w", err)
	}
	if err := p.historicalConsumer.Start(); err != nil {
		// If historical fails, stop the already started realtime consumer
		p.realtimeConsumer.Stop()
		return fmt.Errorf("failed to start historical consumer: %w", err)
	}

	logger.Log.Info("Both consumers started successfully")
	return nil
}

// Stop stops the processor by stopping both consumers
func (p *Processor) Stop() {
	logger.Log.Info("Stopping event processor and both consumers...")
	// Stop consumers (consider order, maybe historical first?)
	p.historicalConsumer.Stop() // Stop historical first
	p.realtimeConsumer.Stop()   // Then stop realtime
	logger.Log.Info("Both consumers stopped")
}
