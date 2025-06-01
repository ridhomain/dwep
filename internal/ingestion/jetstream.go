package ingestion

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/apperrors"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/config"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/model"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/observer"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/tenant"
	"gitlab.com/timkado/api/daisi-wa-events-processor/pkg/utils"

	"github.com/nats-io/nats.go"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/jetstream"
	"gitlab.com/timkado/api/daisi-wa-events-processor/pkg/logger"
	"go.uber.org/zap"
)

// AckNakAction represents the decision made after processing a message
type AckNakAction int

const (
	ActionAck      AckNakAction = iota // Message processed successfully, ACK it
	ActionNak                          // Non-retryable error or DLQ failure, NAK immediately
	ActionNakDelay                     // Retryable error, NAK with calculated delay
	ActionDLQ                          // Max retries reached or fatal error, publish to DLQ then ACK
)

// baseConsumer holds shared components and logic for NATS consumers
type baseConsumer struct {
	client       jetstream.ClientInterface
	router       *Router
	companyID    string
	consumerType string // Add field: "realtime" or "historical"
	ctx          context.Context
	cancel       context.CancelFunc
	maxDeliver   int           // Configured MaxDeliver for this consumer
	dlqSubject   string        // Base DLQ subject from main config
	nakBaseDelay time.Duration // Base NAK delay for retries
	nakMaxDelay  time.Duration // Max NAK delay for retries
}

// newBaseConsumer creates the shared part of a consumer
func newBaseConsumer(client jetstream.ClientInterface, router *Router, companyID, consumerType string, maxDeliver int, dlqSubject string, nakBaseDelay, nakMaxDelay time.Duration) *baseConsumer {
	ctx, cancel := context.WithCancel(context.Background())
	loggerWithTenant := logger.Log.With(zap.String("company_id", companyID))
	ctx = logger.WithLogger(ctx, loggerWithTenant)
	ctx = tenant.WithCompanyID(ctx, companyID)

	return &baseConsumer{
		client:       client,
		router:       router,
		companyID:    companyID,
		consumerType: consumerType, // Set the consumer type
		ctx:          ctx,
		cancel:       cancel,
		maxDeliver:   maxDeliver,
		dlqSubject:   dlqSubject,
		nakBaseDelay: nakBaseDelay,
		nakMaxDelay:  nakMaxDelay,
	}
}

func modifySubjects(subjects []string, companyID string) (streamSubjects, consumerSubjects []string) {
	// Add wildcard * to each subject for stream
	for _, subject := range subjects {
		sb := fmt.Sprintf("%s.*", subject)
		csb := fmt.Sprintf("%s.%s", subject, companyID)
		streamSubjects = append(streamSubjects, sb)
		consumerSubjects = append(consumerSubjects, csb)
	}
	return streamSubjects, consumerSubjects
}

// determineAckNakAction decides the fate of a message based on processing result and metadata.
// It returns the action to take (ACK, NAK, NAK_DELAY, DLQ) and the delay duration if applicable.
func determineAckNakAction(
	processingErr error,
	metadata *nats.MsgMetadata,
	maxDeliver int,
	nakBaseDelay time.Duration,
	nakMaxDelay time.Duration,
) (action AckNakAction, delay time.Duration) {

	if processingErr == nil {
		// Success case
		return ActionAck, 0
	}

	// --- Error Handling Logic ---
	isRetryable := apperrors.IsRetryable(processingErr)
	numDelivered := metadata.NumDelivered // Cast is safe as NumDelivered is uint64

	// Decide if it goes to DLQ (Max retries OR Fatal error)
	if numDelivered >= uint64(maxDeliver) || !isRetryable {
		return ActionDLQ, 0 // DLQ action implies subsequent ACK if publish succeeds
	}

	// Otherwise, it's a retryable error with attempts remaining: NAK with delay
	attempt := numDelivered // Current attempt number (starts at 1)
	delay = nakBaseDelay
	if attempt > 1 {
		delay = nakBaseDelay * (1 << (attempt - 1)) // Equivalent to base * 2^(attempt-1)
	}
	if delay > nakMaxDelay {
		delay = nakMaxDelay
	}
	return ActionNakDelay, delay
}

// handleMessage is the core message processing logic, shared by different consumer types
func (bc *baseConsumer) handleMessage(msg *nats.Msg) {
	var err error
	startTime := utils.Now()
	processingErr := err // Initialize processingErr to avoid shadowing issues later

	// Recover from panics
	defer func() {
		// Observe duration regardless of outcome
		finalEventType, _ := model.MapToBaseEventType(msg.Subject) // Re-map in case of panic before mapping
		observer.ObserveEventProcessingDuration(string(finalEventType), bc.companyID, bc.consumerType, time.Since(startTime))

		// Handle panic recovery
		if r := recover(); r != nil {
			logFromCtx := logger.FromContext(bc.ctx)
			var msgID string
			if msg.Header != nil {
				msgID = msg.Header.Get("Nats-Msg-Id")
			}
			metadata, metadataErr := msg.Metadata()
			if metadataErr == nil {
				msgID = fmt.Sprintf("msg-%d", metadata.Sequence.Stream)
			}
			logFromCtx.Error("[panic] Recovered from panic in message handler",
				zap.Any("panic", r),
				zap.String("nats_message_id", msgID),
				zap.String("subject", msg.Subject),
				zap.Duration("duration", time.Since(startTime)),
				zap.String("consumerType", bc.consumerType),
				zap.Stack("stack"),
			)
			observer.IncEventsFailed(string(finalEventType), bc.companyID, bc.consumerType)
			observer.IncEventProcessingAction(string(finalEventType), bc.companyID, bc.consumerType, "panic_nak", "panic")
			if nakErr := msg.Nak(); nakErr != nil {
				logFromCtx.Error("Failed to NAK message after panic", zap.Error(nakErr))
			}
		}
	}()

	// Start with the base context
	msgCtx := bc.ctx
	logFromCtx := logger.FromContext(msgCtx)

	// Extract metadata
	var msgID string
	if msg.Header != nil {
		msgID = msg.Header.Get("Nats-Msg-Id")
	}

	eventType, found := model.MapToBaseEventType(msg.Subject)
	if !found {
		logFromCtx.Warn("Unknown event type", zap.String("subject", msg.Subject))
		if nakErr := msg.Nak(); nakErr != nil {
			logFromCtx.Error("Failed to NAK message for unknown event type", zap.Error(nakErr))
		}
		// Metric for unknown type (terminal NAK)
		observer.IncEventProcessingAction(string(eventType), bc.companyID, bc.consumerType, "nak_unknown_type", "unknown_event_type")
		return
	}

	metadata, err := msg.Metadata()
	if err != nil {
		logFromCtx.Error("Failed to read message metadata", zap.Error(err), zap.Duration("duration", time.Since(startTime)))
		if nakErr := msg.Nak(); nakErr != nil {
			logFromCtx.Error("Failed to NAK message", zap.Error(nakErr))
		}
		// Metric for metadata error (terminal NAK)
		observer.IncEventProcessingAction(string(eventType), bc.companyID, bc.consumerType, "nak_metadata_error", "metadata")
		return
	}
	messageStreamSeq := metadata.Sequence.Stream
	messageConsumerSeq := metadata.Sequence.Consumer
	if msgID == "" {
		msgID = fmt.Sprintf("msg-%d", messageStreamSeq)
	}

	internalMetadata := &model.MessageMetadata{
		StreamSequence:   messageStreamSeq,
		ConsumerSequence: messageConsumerSeq,
		NumDelivered:     metadata.NumDelivered,
		NumPending:       metadata.NumPending,
		Timestamp:        metadata.Timestamp,
		Stream:           metadata.Stream,   // Get actual stream name from metadata
		Consumer:         metadata.Consumer, // Get actual consumer name from metadata
		Domain:           metadata.Domain,
		MessageID:        msgID,
		MessageSubject:   msg.Subject,
		CompanyID:        bc.companyID, // Use companyID from baseConsumer
	}

	// Metrics and Logging Context Enhancement
	observer.IncEventsReceived(string(eventType), bc.companyID, bc.consumerType)

	msgCtx = logger.WithLogger(msgCtx, logFromCtx.With(
		zap.String("nats_message_id", msgID),
		zap.Uint64("stream_sequence", messageStreamSeq),
		zap.Uint64("consumer_sequence", messageConsumerSeq),
		zap.String("subject", msg.Subject),
		zap.String("stream", internalMetadata.Stream),     // Log actual stream
		zap.String("consumer", internalMetadata.Consumer), // Log actual consumer
		zap.String("consumerType", bc.consumerType),
	))

	// Route the message
	routingStartTime := utils.Now()
	processingErr = bc.router.Route(msgCtx, internalMetadata, msg.Data)
	observer.ObserveEventRoutingDuration(string(eventType), bc.companyID, bc.consumerType, time.Since(routingStartTime))

	// Post-routing actions (logging, metrics, ack/nack/DLQ)
	enhancedLog := logger.FromContext(msgCtx)

	// Determine the action based on the error and metadata
	action, nakDelay := determineAckNakAction(processingErr, metadata, bc.maxDeliver, bc.nakBaseDelay, bc.nakMaxDelay)

	// Determine error type string for metrics (before the switch)
	errorType := "none" // Default for success
	if processingErr != nil {
		errorType = observer.SanitizeErrorType(processingErr.Error()) // Use helper for categorization
	}

	switch action {
	case ActionAck:
		// Success case
		enhancedLog.Info("Successfully processed message", zap.Duration("duration", time.Since(startTime)))
		observer.IncEventsProcessed(string(eventType), bc.companyID, bc.consumerType)
		observer.IncEventProcessingAction(string(eventType), bc.companyID, bc.consumerType, "ack_success", errorType)
		if ackErr := msg.Ack(); ackErr != nil {
			enhancedLog.Error("Failed to ACK message after successful processing", zap.Error(ackErr))
		}

	case ActionNak:
		// Non-retryable error or DLQ failure (explicit NAK needed only if DLQ publish fails)
		// This case is primarily hit if DLQ publishing fails below.
		enhancedLog.Error("NAKing message immediately (non-retryable or DLQ failure)", zap.Error(processingErr), zap.Duration("duration", time.Since(startTime)))
		observer.IncEventsFailed(string(eventType), bc.companyID, bc.consumerType)
		observer.IncEventProcessingAction(string(eventType), bc.companyID, bc.consumerType, "nak_terminal", errorType)
		if nakErr := msg.Nak(); nakErr != nil {
			enhancedLog.Error("Failed to NAK message", zap.Error(nakErr))
		}

	case ActionNakDelay:
		// Retryable error, NAK with delay
		enhancedLog.Info("NAKing message with delay for redelivery (retryable error)",
			zap.Error(processingErr),
			zap.Uint64("num_delivered", metadata.NumDelivered),
			zap.Int("max_deliver", bc.maxDeliver),
			zap.Duration("nak_delay", nakDelay),
			zap.Duration("duration", time.Since(startTime)),
		)
		observer.IncEventsFailed(string(eventType), bc.companyID, bc.consumerType)
		observer.IncEventProcessingAction(string(eventType), bc.companyID, bc.consumerType, "nak_retry", errorType)
		if nakErr := msg.NakWithDelay(nakDelay); nakErr != nil {
			enhancedLog.Error("Failed to NAK message with delay", zap.Error(nakErr))
		}

	case ActionDLQ:
		// Max retries reached or fatal error: Send to DLQ
		isRetryable := apperrors.IsRetryable(processingErr) // Re-check for logging/payload
		logReason := "max delivery attempts reached"
		if !isRetryable {
			logReason = "fatal error encountered"
		}
		enhancedLog.Warn(fmt.Sprintf("Sending message to DLQ: %s", logReason),
			zap.Error(processingErr),
			zap.Uint64("num_delivered", metadata.NumDelivered),
			zap.Int("max_deliver", bc.maxDeliver),
			zap.Bool("is_retryable", isRetryable),
			zap.Duration("duration", time.Since(startTime)),
		)
		observer.IncEventsFailed(string(eventType), bc.companyID, bc.consumerType)

		// Determine error type string for DLQ payload
		var errorTypeString string
		if isRetryable {
			errorTypeString = "retryable"
		} else if apperrors.IsFatal(processingErr) { // Check explicitly for FatalError wrapper
			errorTypeString = "fatal"
		} else {
			enhancedLog.Warn("Error reaching DLQ is not explicitly Fatal or Retryable, classifying as fatal", zap.Error(processingErr))
			errorTypeString = "fatal" // Default unknown/unwrapped errors to fatal for DLQ
		}

		// Construct DLQ Payload
		dlqPayload := model.DLQPayload{
			SourceSubject:   msg.Subject,
			Company:         bc.companyID,
			OriginalPayload: json.RawMessage(msg.Data), // Use json.RawMessage
			Error:           processingErr.Error(),
			ErrorType:       errorTypeString, // Add the determined error type
			RetryCount:      metadata.NumDelivered,
			MaxRetry:        bc.maxDeliver,
			Timestamp:       time.Now().UTC(),
		}

		// Marshal DLQ Payload
		dlqData, marshalErr := json.Marshal(dlqPayload)
		if marshalErr != nil {
			enhancedLog.Error("Failed to marshal DLQ payload, NAKing original message without delay",
				zap.Error(marshalErr),
				zap.String("dlq_subject", bc.dlqSubject+"."+bc.companyID),
			)
			observer.IncEventsFailed(string(eventType), bc.companyID, bc.consumerType)
			observer.IncEventProcessingAction(string(eventType), bc.companyID, bc.consumerType, "nak_dlq_marshal_fail", "dlq_marshal_fail")
			if nakErr := msg.Nak(); nakErr != nil { // NAK without delay on marshal error
				enhancedLog.Error("Failed to NAK message after DLQ marshal error", zap.Error(nakErr))
			}
			return // Exit after NAK
		}

		// Prepare headers for DLQ message
		dlqHeaders := make(map[string]string)
		if msgID != "" { // Use the extracted msgID from earlier
			dlqHeaders["Original-Nats-Msg-Id"] = msgID
		}

		// Publish to DLQ
		dlqFullSubject := fmt.Sprintf("%s.%s", bc.dlqSubject, bc.companyID)
		publishErr := bc.client.Publish(dlqFullSubject, dlqData, dlqHeaders)
		if publishErr != nil {
			enhancedLog.Error("Failed to publish message to DLQ, NAKing original message without delay",
				zap.Error(publishErr),
				zap.String("dlq_subject", dlqFullSubject),
			)
			observer.IncEventsFailed(string(eventType), bc.companyID, bc.consumerType)
			observer.IncEventProcessingAction(string(eventType), bc.companyID, bc.consumerType, "nak_dlq_publish_fail", "dlq_publish_fail")
			// NAK the original message if DLQ publish fails (without delay)
			if nakErr := msg.Nak(); nakErr != nil {
				enhancedLog.Error("Failed to NAK message after DLQ publish error", zap.Error(nakErr))
			}
		} else {
			enhancedLog.Info("Message published to DLQ", zap.String("dlq_subject", dlqFullSubject))
			// ACK the original message *only* if DLQ publish succeeded
			observer.IncEventProcessingAction(string(eventType), bc.companyID, bc.consumerType, "dlq_published_ack_success", errorType)
			if ackErr := msg.Ack(); ackErr != nil {
				enhancedLog.Error("Failed to ACK message after successful DLQ publish", zap.Error(ackErr))
			}
		}
	}
}

// --- Specific Consumer Implementations ---

// RealtimeConsumer handles the real-time event stream
type RealtimeConsumer struct {
	base          *baseConsumer
	cfg           config.ConsumerNatsConfig
	sub           *nats.Subscription
	filterSubject string // Filter subject for the queue subscription
	dlqSubject    string // Store base DLQ subject
}

// NewRealtimeConsumer creates a consumer for the real-time stream
func NewRealtimeConsumer(client jetstream.ClientInterface, router *Router, cfg config.ConsumerNatsConfig, companyID string, dlqSubject string) *RealtimeConsumer {
	// Pass the delay values from the config and consumer type
	base := newBaseConsumer(client, router, companyID, "realtime", cfg.MaxDeliver, dlqSubject, cfg.NakBaseDelay, cfg.NakMaxDelay)
	return &RealtimeConsumer{
		base:       base,
		cfg:        cfg,
		dlqSubject: dlqSubject, // Store for potential use elsewhere if needed
	}
}

// Setup configures the NATS stream and consumer for real-time events
func (c *RealtimeConsumer) Setup() error {
	log := logger.FromContext(c.base.ctx)
	log.Info("Setting up RealtimeConsumer...", zap.String("stream", c.cfg.Stream), zap.String("consumer", c.cfg.Consumer))

	maxAgeRetention := time.Duration(c.cfg.MaxAge*24) * time.Hour
	// Add wildcard * to each subject for stream
	streamSubjects, consumerSubjects := modifySubjects(c.cfg.SubjectList, c.base.companyID)

	// 1. Create Stream Config
	streamCfg := &nats.StreamConfig{
		Name:      c.cfg.Stream,
		Subjects:  streamSubjects,
		Storage:   nats.FileStorage,  // Assuming FileStorage, adjust if needed
		Retention: nats.LimitsPolicy, // Assuming LimitsPolicy, adjust if needed
		MaxAge:    maxAgeRetention,
	}

	// Log stream config for debugging
	log.Info("RealtimeConsumer Stream Config Setup", zap.Any("streamConfig", streamCfg))

	// 2. Setup Stream
	if err := c.base.client.SetupStream(c.base.ctx, streamCfg); err != nil {
		log.Error("Failed to setup realtime stream", zap.Error(err), zap.String("stream", c.cfg.Stream))
		return fmt.Errorf("failed to setup realtime stream '%s': %w", c.cfg.Stream, err)
	}

	// 3. Create Consumer Config
	consumerCfg := &nats.ConsumerConfig{
		Durable:        c.cfg.Consumer, // Use tenant-specific name
		DeliverGroup:   c.cfg.QueueGroup,
		FilterSubjects: consumerSubjects,
		AckPolicy:      nats.AckExplicitPolicy,   // Keep existing policy
		DeliverSubject: nats.NewInbox(),          // Use unique inbox for push consumers
		MaxDeliver:     c.cfg.MaxDeliver,         // Use value from config
		AckWait:        30 * time.Second,         // Example: 30s ack wait
		MaxAckPending:  1000,                     // Example: Allow more pending acks
		ReplayPolicy:   nats.ReplayInstantPolicy, // Start immediately
		DeliverPolicy:  nats.DeliverLastPolicy,
	}
	c.filterSubject = "v1.>" // Store filter subject for subscription

	log.Info("RealtimeConsumer Consumer Config Setup", zap.Any("consumerConfig", consumerCfg))
	// 4. Setup Consumer
	if err := c.base.client.SetupConsumer(c.base.ctx, c.cfg.Stream, consumerCfg); err != nil {
		log.Error("Failed to setup realtime consumer", zap.Error(err), zap.String("stream", c.cfg.Stream), zap.String("consumer", c.cfg.Consumer))
		return fmt.Errorf("failed to setup realtime consumer '%s' for stream '%s': %w", c.cfg.Consumer, c.cfg.Stream, err)
	}

	log.Info("RealtimeConsumer setup complete")
	return nil
}

// Start subscribes to the NATS stream
func (c *RealtimeConsumer) Start() error {
	log := logger.FromContext(c.base.ctx)

	log.Info("Starting RealtimeConsumer subscription...", zap.String("stream", c.cfg.Stream), zap.String("consumer", c.cfg.Consumer))

	// Use SubscribePush for push-based delivery with DeliverSubject defined in ConsumerConfig
	sub, err := c.base.client.SubscribePush(c.filterSubject, c.cfg.Consumer, c.cfg.QueueGroup, c.cfg.Stream, c.base.handleMessage)
	// Note: Using "" as subject since FilterSubjects is set in ConsumerConfig
	// The QueueGroup here might be redundant if DeliverGroup is set in ConsumerConfig, but we keep it for clarity/compatibility

	if err != nil {
		log.Error("Failed to subscribe realtime consumer", zap.Error(err),
			zap.String("stream", c.cfg.Stream),
			zap.String("consumer", c.cfg.Consumer),
			zap.String("group", c.cfg.QueueGroup),
		)
		return fmt.Errorf("failed to subscribe realtime consumer '%s': %w", c.cfg.Consumer, err)
	}
	c.sub = sub
	log.Info("RealtimeConsumer subscribed successfully")
	return nil
}

// Stop unsubscribes and cleans up resources
func (c *RealtimeConsumer) Stop() {
	log := logger.FromContext(c.base.ctx)

	log.Info("Stopping RealtimeConsumer...", zap.String("stream", c.cfg.Stream), zap.String("consumer", c.cfg.Consumer))
	if c.sub != nil {
		if err := c.sub.Drain(); err != nil {
			log.Error("Error draining realtime subscription", zap.Error(err))
		}
		log.Info("Realtime subscription drained")
	}
	if c.base.cancel != nil {
		c.base.cancel()
	}
	log.Info("RealtimeConsumer stopped")
}

// HistoricalConsumer handles the historical event stream
type HistoricalConsumer struct {
	base          *baseConsumer
	cfg           config.ConsumerNatsConfig
	sub           *nats.Subscription
	filterSubject string // Filter subject for the queue subscription
	dlqSubject    string // Store base DLQ subject
}

// NewHistoricalConsumer creates a consumer for the historical stream
func NewHistoricalConsumer(client jetstream.ClientInterface, router *Router, cfg config.ConsumerNatsConfig, companyID string, dlqSubject string) *HistoricalConsumer {
	// Pass the delay values from the config and consumer type
	base := newBaseConsumer(client, router, companyID, "historical", cfg.MaxDeliver, dlqSubject, cfg.NakBaseDelay, cfg.NakMaxDelay)
	return &HistoricalConsumer{
		base:       base,
		cfg:        cfg,
		dlqSubject: dlqSubject, // Store for potential use elsewhere if needed
	}
}

// Setup configures the NATS stream and consumer for historical events
func (c *HistoricalConsumer) Setup() error {
	log := logger.FromContext(c.base.ctx)
	log.Info("Setting up HistoricalConsumer...", zap.String("stream", c.cfg.Stream), zap.String("consumer", c.cfg.Consumer))

	maxAgeRetention := time.Duration(c.cfg.MaxAge*24) * time.Hour
	// Add wildcard * to each subject for stream
	streamSubjects, consumerSubjects := modifySubjects(c.cfg.SubjectList, c.base.companyID)

	// 1. Create Stream Config
	streamCfg := &nats.StreamConfig{
		Name:      c.cfg.Stream,
		Subjects:  streamSubjects,
		Storage:   nats.FileStorage,
		Retention: nats.InterestPolicy,
		MaxAge:    maxAgeRetention,
	}

	// 2. Setup Stream
	if err := c.base.client.SetupStream(c.base.ctx, streamCfg); err != nil {
		log.Error("Failed to setup historical stream", zap.Error(err), zap.String("stream", c.cfg.Stream))
		return fmt.Errorf("failed to setup historical stream '%s': %w", c.cfg.Stream, err)
	}

	// 3. Create Consumer Config
	consumerCfg := &nats.ConsumerConfig{
		Durable:        c.cfg.Consumer,
		DeliverGroup:   c.cfg.QueueGroup,
		FilterSubjects: consumerSubjects,
		AckPolicy:      nats.AckExplicitPolicy,
		DeliverSubject: nats.NewInbox(),          // Unique inbox for push consumers
		MaxDeliver:     c.cfg.MaxDeliver,         // Use value from config
		AckWait:        60 * time.Second,         // Longer ack wait for potentially longer processing?
		MaxAckPending:  500,                      // Adjust pending based on expected throughput
		ReplayPolicy:   nats.ReplayInstantPolicy, // Start immediately
		DeliverPolicy:  nats.DeliverLastPolicy,
	}
	c.filterSubject = "v1.>" // Store filter subject for subscription

	// 4. Setup Consumer
	if err := c.base.client.SetupConsumer(c.base.ctx, c.cfg.Stream, consumerCfg); err != nil {
		log.Error("Failed to setup historical consumer", zap.Error(err), zap.String("stream", c.cfg.Stream), zap.String("consumer", c.cfg.Consumer))
		return fmt.Errorf("failed to setup historical consumer '%s' for stream '%s': %w", c.cfg.QueueGroup, c.cfg.Stream, err)
	}

	log.Info("HistoricalConsumer setup complete")
	return nil
}

// Start subscribes to the NATS stream
func (c *HistoricalConsumer) Start() error {
	log := logger.FromContext(c.base.ctx)
	log.Info("Starting HistoricalConsumer subscription...", zap.String("stream", c.cfg.Stream), zap.String("consumer", c.cfg.Consumer))

	sub, err := c.base.client.SubscribePush(c.filterSubject, c.cfg.Consumer, c.cfg.QueueGroup, c.cfg.Stream, c.base.handleMessage)
	if err != nil {
		log.Error("Failed to subscribe historical consumer", zap.Error(err),
			zap.String("stream", c.cfg.Stream),
			zap.String("consumer", c.cfg.Consumer),
			zap.String("group", c.cfg.QueueGroup),
		)
		return fmt.Errorf("failed to subscribe historical consumer '%s': %w", c.cfg.Consumer, err)
	}
	c.sub = sub
	log.Info("HistoricalConsumer subscribed successfully")
	return nil
}

// Stop unsubscribes and cleans up resources
func (c *HistoricalConsumer) Stop() {
	log := logger.FromContext(c.base.ctx)
	log.Info("Stopping HistoricalConsumer...", zap.String("stream", c.cfg.Stream), zap.String("consumer", c.cfg.Consumer))
	if c.sub != nil {
		if err := c.sub.Drain(); err != nil {
			log.Error("Error draining historical subscription", zap.Error(err))
		}
		log.Info("Historical subscription drained")
	}
	if c.base.cancel != nil {
		c.base.cancel()
	}
	log.Info("HistoricalConsumer stopped")
}

// SanitizeErrorType maps an error to a general category string for metrics.
func SanitizeErrorType(err error) string {
	if err == nil {
		return "none"
	}

	// Check for specific error types using errors.Is
	switch {
	case apperrors.IsDatabaseError(err):
		return "database"
	case apperrors.IsBadRequestError(err):
		return "validation"
	case apperrors.IsNotFoundError(err):
		return "not_found"
	case apperrors.IsUnauthorizedError(err):
		return "unauthorized"
	case apperrors.IsConflictError(err):
		return "conflict"
	case apperrors.IsTimeoutError(err):
		return "timeout"
	case apperrors.IsNATSError(err):
		return "nats"
	case strings.Contains(err.Error(), "panic"):
		return "panic"
		// Add more specific checks if needed
	}

	// Fallback to simple string checking for broader categories
	errStr := err.Error()
	switch {
	case strings.Contains(errStr, "unmarshal"), strings.Contains(errStr, "json"):
		return "unmarshal"
	default:
		return "unknown"
	}
}
