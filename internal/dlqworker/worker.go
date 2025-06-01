package dlqworker

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/panjf2000/ants/v2"
	"go.uber.org/zap"
	"gorm.io/datatypes"

	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/config"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/ingestion"
	internal_js "gitlab.com/timkado/api/daisi-wa-events-processor/internal/jetstream"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/model"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/observer"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/storage"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/tenant"
	"gitlab.com/timkado/api/daisi-wa-events-processor/pkg/logger"
)

const (
	maxRetries        = 5 // As defined in the plan
	defaultMsgChanCap = 100
	fetchBatchSize    = 10
	fetchMaxWait      = 5 * time.Second
)

// Worker handles processing messages from the DLQ.
type Worker struct {
	cfg    *config.Config
	logger *zap.Logger
	nc     *nats.Conn
	js     internal_js.ClientInterface
	pool   *ants.Pool
	router ingestion.RouterInterface
	store  storage.ExhaustedEventRepo
	msgCh  chan *nats.Msg
	stopWg sync.WaitGroup
	cancel context.CancelFunc
}

// NewWorker creates and initializes a new DLQ worker, including setting up the required JetStream resources.
func NewWorker(cfg *config.Config, logger *zap.Logger, nc *nats.Conn, jsClient internal_js.ClientInterface, router ingestion.RouterInterface, exhaustedRepo storage.ExhaustedEventRepo) (*Worker, error) {
	pool, err := ants.NewPool(cfg.NATS.DLQWorkers,
		ants.WithLogger(newAntsLoggerAdapter(logger.Named("ants_pool"))),
		ants.WithPanicHandler(func(err interface{}) {
			logger.Error("Worker panic caught", zap.Any("error", err), zap.Stack("stack"))
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create ants pool: %w", err)
	}

	// --- JetStream Setup for DLQ ---
	setupCtx := context.Background() // Use background context for setup
	dlqStreamName := cfg.NATS.DLQStream
	dlqSubject := cfg.NATS.DLQSubject + ".>" // Subject pattern for the stream and consumer
	// dlqDurableName := fmt.Sprintf("%s_worker_consumer", cfg.NATS.DLQSubject)
	// Replace dots in subject to create a valid durable name
	dlqSubjectCleaned := strings.ReplaceAll(cfg.NATS.DLQSubject, ".", "_")
	dlqDurableName := fmt.Sprintf("%s_worker_consumer", dlqSubjectCleaned)

	// 1. Define DLQ Stream Config
	// Assuming these config values might not exist, using defaults
	dlqMaxAge := time.Duration(cfg.NATS.DLQMaxAgeDays) * 24 * time.Hour

	dlqStreamCfg := &nats.StreamConfig{
		Name:      dlqStreamName,
		Subjects:  []string{dlqSubject}, // Stream listens on the base subject pattern
		Storage:   nats.FileStorage,
		Retention: nats.LimitsPolicy, // Retain messages until limits (like MaxAge) are hit
		MaxAge:    dlqMaxAge,
	}

	// 2. Setup DLQ Stream
	if err := jsClient.SetupStream(setupCtx, dlqStreamCfg); err != nil {
		pool.Release() // Clean up the pool if setup fails
		return nil, fmt.Errorf("failed to setup DLQ stream '%s': %w", dlqStreamName, err)
	}
	logger.Info("DLQ Stream setup complete", zap.String("stream", dlqStreamName))

	// 3. Define DLQ Consumer Config

	dlqConsumerCfg := &nats.ConsumerConfig{
		Durable:       dlqDurableName,
		FilterSubject: dlqSubject,             // Consumer filters for the specific DLQ subject pattern
		AckPolicy:     nats.AckExplicitPolicy, // We need to explicitly Ack/Nak/Term
		MaxDeliver:    cfg.NATS.DLQMaxDeliver,
		AckWait:       cfg.NATS.DLQAckWait,
		MaxAckPending: cfg.NATS.DLQMaxAckPending,
		DeliverPolicy: nats.DeliverAllPolicy,    // Ensure all messages are delivered eventually
		ReplayPolicy:  nats.ReplayInstantPolicy, // Start consuming immediately
		// Pull consumers don't use DeliverSubject or DeliverGroup
	}

	// 4. Setup DLQ Consumer
	if err := jsClient.SetupConsumer(setupCtx, dlqStreamName, dlqConsumerCfg); err != nil {
		pool.Release() // Clean up the pool if setup fails
		return nil, fmt.Errorf("failed to setup DLQ consumer '%s' for stream '%s': %w", dlqDurableName, dlqStreamName, err)
	}
	logger.Info("DLQ Consumer setup complete", zap.String("consumer", dlqDurableName))
	// --- End JetStream Setup ---

	worker := &Worker{
		cfg:    cfg,
		logger: logger.Named("dlq_worker"),
		nc:     nc,
		js:     jsClient,
		pool:   pool,
		router: router,
		store:  exhaustedRepo,
		msgCh:  make(chan *nats.Msg, defaultMsgChanCap),
	}

	worker.logger.Info("DLQ Worker initialized", zap.Int("pool_size", cfg.NATS.DLQWorkers))
	return worker, nil
}

// Start begins the DLQ processing loops (fetcher and dispatcher).
func (w *Worker) Start(ctx context.Context) error {
	derivedCtx, cancel := context.WithCancel(ctx)
	w.cancel = cancel

	w.logger.Info("Starting DLQ worker...")

	// Names are determined during setup, ensure consistency
	// Recreate the same valid durable name used in setup
	dlqSubjectCleaned := strings.ReplaceAll(w.cfg.NATS.DLQSubject, ".", "_")
	durableName := fmt.Sprintf("%s_worker_consumer", dlqSubjectCleaned)
	subSubject := fmt.Sprintf("%s.>", w.cfg.NATS.DLQSubject) // The subject the consumer filters on

	w.logger.Info("Attempting DLQ pull subscription",
		zap.String("stream", w.cfg.NATS.DLQStream),
		zap.String("subject", subSubject), // This is the filter subject for the pull
		zap.String("durable_name", durableName),
	)

	// SubscribePull uses the durable name to bind to the existing consumer
	// The subject here acts as a filter for the Pull operation, matching the consumer's FilterSubject
	sub, err := w.js.SubscribePull(w.cfg.NATS.DLQStream, subSubject, durableName)
	if err != nil {
		w.logger.Error("Failed to create DLQ pull subscription", zap.Error(err))
		cancel() // Cancel the derived context if subscription fails
		return fmt.Errorf("failed to create DLQ pull subscription: %w", err)
	}

	w.stopWg.Add(1)
	go w.fetchMessages(derivedCtx, sub)

	w.stopWg.Add(1)
	go w.dispatchMessages(derivedCtx)

	w.logger.Info("DLQ worker started successfully")

	<-derivedCtx.Done()
	w.logger.Info("DLQ worker context cancelled, initiating shutdown...")

	return nil
}

// Stop gracefully shuts down the DLQ worker.
func (w *Worker) Stop() {
	w.logger.Info("Stopping DLQ worker...")
	if w.cancel != nil {
		w.cancel()
	}

	w.stopWg.Wait()
	w.logger.Info("Fetcher and Dispatcher stopped")

	close(w.msgCh)
	w.logger.Info("Message channel closed")

	w.pool.Release()
	w.logger.Info("Worker pool released")
	w.logger.Info("DLQ worker stopped successfully")
}

// fetchMessages pulls messages from the JetStream subscription and sends them to msgCh.
func (w *Worker) fetchMessages(ctx context.Context, sub *nats.Subscription) {
	defer w.stopWg.Done()
	w.logger.Info("Starting DLQ message fetcher loop...")

	for {
		select {
		case <-ctx.Done():
			w.logger.Info("Fetcher loop stopping due to context cancellation")
			return
		default:
			observer.IncDlqFetchRequest()
			msgs, err := sub.Fetch(fetchBatchSize, nats.MaxWait(fetchMaxWait))
			if err != nil {
				if err == context.Canceled || err == nats.ErrTimeout || err == nats.ErrConnectionClosed {
					w.logger.Warn("Fetcher received expected error, no new event, continuing or stopping...", zap.Error(err))
					if ctx.Err() != nil {
						return
					}
					continue
				}
				observer.IncDlqFetchError()
				w.logger.Error("Fetcher loop error retrieving messages", zap.Error(err))
				time.Sleep(1 * time.Second)
				continue
			}

			if len(msgs) == 0 {
				continue
			}

			w.logger.Debug("Fetched messages from DLQ", zap.Int("count", len(msgs)))
			for _, msg := range msgs {
				select {
				case w.msgCh <- msg:
				case <-ctx.Done():
					w.logger.Info("Fetcher loop stopping while sending to channel")
					return
				}
			}
		}
	}
}

// dispatchMessages reads messages from msgCh and submits them to the worker pool.
func (w *Worker) dispatchMessages(ctx context.Context) {
	defer w.stopWg.Done()
	w.logger.Info("Starting DLQ message dispatcher loop...")

	for {
		observer.SetDlqQueueLength(len(w.msgCh))
		observer.SetDlqWorkersActive(w.pool.Running())

		select {
		case <-ctx.Done():
			w.logger.Info("Dispatcher loop stopping due to context cancellation")
			return
		case msg, ok := <-w.msgCh:
			if !ok {
				w.logger.Info("Message channel closed, dispatcher loop stopping")
				return
			}
			currentMsg := msg
			err := w.pool.Submit(func() {
				taskCtx, taskCancel := context.WithTimeout(context.Background(), 1*time.Minute)
				defer taskCancel()
				w.handleWithRetry(taskCtx, currentMsg)
			})
			if err != nil {
				w.logger.Error("Failed to submit task to ants pool", zap.Error(err))
				if nakErr := currentMsg.NakWithDelay(5 * time.Second); nakErr != nil {
					w.logger.Error("Failed to NAK message after pool submission error", zap.Error(nakErr))
					var tempPayload model.DLQPayload
					_ = json.Unmarshal(currentMsg.Data, &tempPayload)
					observer.IncDlqAckFailure(tempPayload.Company)
				}
			} else {
				var tempPayload model.DLQPayload
				_ = json.Unmarshal(currentMsg.Data, &tempPayload)
				observer.IncDlqTasksSubmitted(tempPayload.Company)
			}
		}
	}
}

// handleWithRetry processes a single message with backoff logic.
func (w *Worker) handleWithRetry(ctx context.Context, msg *nats.Msg) {
	startTime := time.Now()
	var companyID string
	defer func() {
		observer.ObserveDlqProcessingDuration(companyID, time.Since(startTime))
	}()

	meta, err := msg.Metadata()
	if err != nil {
		w.logger.Error("Failed to get message metadata", zap.Error(err))
		if ackErr := msg.Term(); ackErr != nil {
			w.logger.Error("Failed to terminate message after metadata error", zap.Error(ackErr))
		}
		observer.IncDlqAckFailure(companyID)
		return
	}

	var payload model.DLQPayload
	if err := json.Unmarshal(msg.Data, &payload); err != nil {
		w.logger.Error("Failed to unmarshal DLQ payload",
			zap.Error(err),
			zap.Uint64("sequence", meta.Sequence.Stream),
			zap.String("subject", msg.Subject),
			zap.ByteString("data", msg.Data),
		)
		if ackErr := msg.Term(); ackErr != nil {
			w.logger.Error("Failed to terminate message after unmarshal error", zap.Error(ackErr))
		}
		observer.IncDlqAckFailure(companyID)
		return
	}

	w.logger.Info("Processing DLQ message",
		zap.String("source_subject", payload.SourceSubject),
		zap.Uint64("stream_sequence", meta.Sequence.Stream),
		zap.Uint64("consumer_sequence", meta.Sequence.Consumer),
		zap.Uint64("num_delivered", meta.NumDelivered),
		zap.Uint64("payload_retry_count", payload.RetryCount),
	)

	w.logger.Debug("Attempting to route original event", zap.String("original_subject", payload.SourceSubject))
	routerMetadata := &model.MessageMetadata{
		MessageSubject:   payload.SourceSubject,
		CompanyID:        payload.Company,
		StreamSequence:   meta.Sequence.Stream,
		ConsumerSequence: meta.Sequence.Consumer,
		Timestamp:        meta.Timestamp,
		NumDelivered:     meta.NumDelivered,
	}
	handlerCtx := tenant.WithCompanyID(ctx, payload.Company)
	handlerCtx = logger.WithLogger(handlerCtx, w.logger.With(
		zap.String("original_subject", payload.SourceSubject),
		zap.String("dlq_company", payload.Company),
	))

	processingErr := w.router.Route(handlerCtx, routerMetadata, payload.OriginalPayload)

	if processingErr != nil {
		w.logger.Warn("Failed to process event from DLQ",
			zap.String("source_subject", payload.SourceSubject),
			zap.Uint64("num_delivered", meta.NumDelivered),
			zap.Error(processingErr),
		)

		if payload.RetryCount >= maxRetries {
			w.logger.Warn("Max retries exceeded, attempting to persist to exhausted store",
				zap.String("source_subject", payload.SourceSubject),
				zap.Int("retry_count", int(payload.RetryCount)),
			)

			exhaustedEvent := model.ExhaustedEvent{
				CompanyID:       payload.Company,
				SourceSubject:   payload.SourceSubject,
				LastError:       processingErr.Error(),
				RetryCount:      int(payload.RetryCount),
				EventTimestamp:  payload.Timestamp,
				DLQPayload:      datatypes.JSON(msg.Data),
				OriginalPayload: datatypes.JSON(payload.OriginalPayload),
			}

			if saveErr := w.store.Save(ctx, exhaustedEvent); saveErr != nil {
				w.logger.Error("Failed to save exhausted event to persistence store, terminating message anyway",
					zap.Error(saveErr),
					zap.String("source_subject", payload.SourceSubject),
				)
				if termErr := msg.Term(); termErr != nil {
					w.logger.Error("Failed to terminate message after persistence failure", zap.Error(termErr))
				}
				observer.IncDlqAckFailure(payload.Company)
				return
			}

			if termErr := msg.Term(); termErr != nil {
				w.logger.Error("Failed to terminate message after max retries", zap.Error(termErr))
			}
			observer.IncDlqTasksDropped(payload.Company)
			observer.IncDlqAckFailure(payload.Company)
			return
		}

		delay := calculateBackoffDelay(int(meta.NumDelivered), w.cfg.NATS.DLQBaseDelayMinutes, w.cfg.NATS.DLQMaxDelayMinutes)
		w.logger.Info("Retrying DLQ message with backoff",
			zap.String("source_subject", payload.SourceSubject),
			zap.Uint64("attempt", meta.NumDelivered),
			zap.Duration("delay", delay),
		)

		if nakErr := msg.NakWithDelay(delay); nakErr != nil {
			w.logger.Error("Failed to NAK message with delay", zap.Error(nakErr))
			observer.IncDlqAckFailure(payload.Company)
		} else {
			observer.IncDlqTaskRetry(payload.Company)
		}
		return
	}

	w.logger.Info("Successfully processed event from DLQ",
		zap.String("source_subject", payload.SourceSubject),
		zap.Uint64("attempt", meta.NumDelivered),
	)
	if ackErr := msg.Ack(); ackErr != nil {
		w.logger.Error("Failed to ACK successfully processed message", zap.Error(ackErr))
		observer.IncDlqAckFailure(payload.Company)
	} else {
		observer.IncDlqAckSuccess(payload.Company)
	}
}

func (w *Worker) persistExhaustedEvent(ctx context.Context, payload model.DLQPayload, finalError string) error {
	w.logger.Info("Persisting exhausted event (stub)", zap.String("source_subject", payload.SourceSubject))
	return nil
}

// calculateBackoffDelay calculates the delay based on retry count.
func calculateBackoffDelay(retryCount int, baseDelayMinutes, maxDelayMinutes int) time.Duration {
	baseDelay := time.Duration(baseDelayMinutes) * time.Minute
	maxDelay := time.Duration(maxDelayMinutes) * time.Minute

	if retryCount <= 0 {
		return baseDelay
	}

	delay := baseDelay * time.Duration(1<<uint(retryCount-1))

	if delay > maxDelay {
		delay = maxDelay
	}
	return delay
}

// --- Ants Logger Adapter ---

type antsLoggerAdapter struct {
	logger *zap.Logger
}

func newAntsLoggerAdapter(logger *zap.Logger) *antsLoggerAdapter {
	return &antsLoggerAdapter{logger: logger}
}

func (a *antsLoggerAdapter) Printf(format string, args ...interface{}) {
	a.logger.Info(fmt.Sprintf(format, args...))
}
