package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/panjf2000/ants/v2"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/config"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/jetstream"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/model"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/observer"
	"gitlab.com/timkado/api/daisi-wa-events-processor/pkg/logger"
	"go.uber.org/zap"
)

// IndividualTaskDetail holds info for a single message within a batch.
type IndividualTaskDetail struct {
	BaseSubject string
	CompanyID   string
}

// BatchTask represents a batch of messages to be processed by a worker.
type BatchTask struct {
	Tasks      []IndividualTaskDetail
	NatsClient jetstream.ClientInterface
}

const defaultBatchSize = 50

func main() {
	// --- Configuration & Flag Parsing ---
	cfg, err := config.LoadConfig("")
	if err != nil {
		fmt.Printf("Error loading config: %v\n", err)
		os.Exit(1)
	}

	natsURL := flag.String("url", cfg.NATS.URL, "NATS server URL")
	subjectsStr := flag.String("subjects", "v1.chats.upsert,v1.messages.upsert", "Comma-separated list of base NATS subjects")
	rate := flag.Int("rate", 100, "Target messages per second (total)")
	duration := flag.Duration("duration", 1*time.Minute, "Load test duration")
	concurrency := flag.Int("concurrency", 10, "Number of concurrent workers")
	companyIDsStr := flag.String("company_ids", cfg.Company.ID, "Comma-separated list of company IDs")
	batchSize := flag.Int("batch-size", defaultBatchSize, "Number of messages to generate/publish per worker batch")
	historyCount := flag.Int("history-count", 10, "Number of items per history payload (for history subjects)")
	metricsPort := flag.Int("metrics-port", 9091, "Port for Prometheus metrics endpoint")
	logLevel := flag.String("log-level", cfg.LogLevel, "Log level (debug, info, warn, error)")

	// --- Usage Function ---
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "NATS Load Generator (Batch Mode)\\n")
		fmt.Fprintf(os.Stderr, "Usage: %s [options]\\n\\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Generates load for the daisi-wa-events-processor by publishing messages to NATS.\\n\\n")
		fmt.Fprintf(os.Stderr, "Options:\\n")
		flag.PrintDefaults()
	}

	flag.Parse()

	if *batchSize <= 0 {
		*batchSize = defaultBatchSize
		fmt.Printf("Invalid batch size, using default: %d\n", defaultBatchSize)
	}
	if *historyCount <= 0 {
		*historyCount = 10 // Use the same default as the factory
		fmt.Printf("Invalid history count, using default: %d\n", 10)
	}

	// --- Initialization ---
	if err := logger.Initialize(*logLevel); err != nil {
		fmt.Printf("Failed to initialize logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	observer.InitMetrics(true)
	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // Ensure context is cancelled eventually

	// Start metrics server with graceful shutdown
	metricsServer := startMetricsServer(ctx, *metricsPort)
	var metricsWg sync.WaitGroup
	metricsWg.Add(1)
	go func() {
		defer metricsWg.Done()
		<-ctx.Done() // Wait for cancellation signal
		logger.Log.Info("Shutting down metrics server...")
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		if err := metricsServer.Shutdown(shutdownCtx); err != nil {
			logger.Log.Error("Metrics server shutdown error", zap.Error(err))
		} else {
			logger.Log.Info("Metrics server stopped gracefully.")
		}
	}()

	logger.Log.Info("Starting NATS Load Generator (Batch Mode)",
		zap.String("nats_url", *natsURL),
		zap.String("subjects", *subjectsStr),
		zap.Int("rate_per_sec", *rate),
		zap.Duration("duration", *duration),
		zap.Int("concurrency", *concurrency),
		zap.Int("batch_size", *batchSize),
		zap.String("company_ids", *companyIDsStr),
		zap.Int("metrics_port", *metricsPort),
		zap.String("log_level", *logLevel),
	)

	natsClient, err := jetstream.NewClient(*natsURL)
	if err != nil {
		logger.Log.Fatal("Failed to connect to NATS", zap.String("url", *natsURL), zap.Error(err))
	}
	defer natsClient.Close()
	logger.Log.Info("Connected to NATS", zap.String("url", *natsURL))

	baseSubjects := strings.Split(*subjectsStr, ",")
	companyIDs := strings.Split(*companyIDsStr, ",")
	if len(baseSubjects) == 0 || baseSubjects[0] == "" {
		logger.Log.Fatal("No base subjects provided")
	}
	if len(companyIDs) == 0 || companyIDs[0] == "" {
		logger.Log.Fatal("No company IDs provided")
	}

	rand.Seed(time.Now().UnixNano())
	gofakeit.Seed(time.Now().UnixNano()) // Seed gofakeit too

	// --- Worker Pool Setup ---
	var wg sync.WaitGroup
	pool, err := ants.NewPoolWithFunc(*concurrency, func(data interface{}) {
		batchWorkerFunc(data, &wg) // Use the new batch worker function
	})
	if err != nil {
		logger.Log.Fatal("Failed to create worker pool", zap.Error(err))
	}
	defer pool.Release()

	logger.Log.Info("Worker pool initialized", zap.Int("size", *concurrency))

	// --- Rate Limiting and Execution ---
	// Use the same context for the load loop
	// ctx, cancel := context.WithCancel(context.Background()) // Already defined above
	// defer cancel() // Already deferred above

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// WaitGroup for the load generation loop itself
	var loopWg sync.WaitGroup
	loopWg.Add(1)

	// Start the load generation loop
	go runBatchLoadLoop(ctx, *rate, *duration, *batchSize, baseSubjects, companyIDs, natsClient, pool, &wg, &loopWg)

	// Wait for stop signal or context cancellation (implicitly handled by timer in loop)
	select {
	case sig := <-sigChan:
		logger.Log.Info("Received termination signal, shutting down...", zap.String("signal", sig.String()))
		cancel() // Signal cancellation to loop and metrics server
	case <-ctx.Done():
		// This case might be hit if the duration finishes *before* a signal
		logger.Log.Info("Load generation duration finished or context cancelled externally.")
		// cancel() // Ensure cancellation is called if not already
	}

	// --- Graceful Shutdown ---
	logger.Log.Info("Waiting for load generation loop to finish submitting tasks...")
	loopWg.Wait() // Wait for runBatchLoadLoop goroutine to exit
	logger.Log.Info("Load generation loop finished.")

	logger.Log.Info("Waiting for active publishing worker tasks to complete...")
	wg.Wait() // Wait for all dispatched worker tasks to complete
	logger.Log.Info("All worker tasks finished.")

	// pool.Release() is handled by defer now

	logger.Log.Info("Closing NATS connection.") // NATS Close is handled by defer
	// natsClient.Close() // Handled by defer

	// Wait for metrics server to shut down (triggered by cancel())
	logger.Log.Info("Waiting for metrics server to stop...")
	metricsWg.Wait()

	logger.Log.Info("Load generator shutdown complete.")
}

func startMetricsServer(ctx context.Context, port int) *http.Server {
	logger.Log.Info("Starting Prometheus metrics server", zap.Int("port", port))
	mux := http.NewServeMux() // Use a dedicated mux
	mux.Handle("/metrics", promhttp.Handler())
	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}

	// Start server in a goroutine so it doesn't block
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Log.Error("Failed to start Prometheus metrics server", zap.Error(err))
			// Consider cancelling the main context here if metrics server fails to start
			// cancel() // If you passed cancel func here
		}
	}()

	return server // Return the server instance for graceful shutdown
}

// runBatchLoadLoop manages the rate-limited submission of BATCHES to the worker pool.
func runBatchLoadLoop(ctx context.Context, rate int, duration time.Duration, batchSize int, subjects, companies []string, nc jetstream.ClientInterface, pool *ants.PoolWithFunc, wg *sync.WaitGroup, loopWg *sync.WaitGroup) {
	defer loopWg.Done() // Signal that this loop goroutine has finished

	// Ticker controls the rate of individual message generation attempts
	ticker := time.NewTicker(time.Second / time.Duration(rate))
	defer ticker.Stop()

	durationTimer := time.NewTimer(duration)
	defer durationTimer.Stop()

	messageCounter := 0
	currentBatch := make([]IndividualTaskDetail, 0, batchSize)

	logger.Log.Info("Starting batch load generation loop",
		zap.Int("target_rate_per_sec", rate),
		zap.Duration("duration", duration),
		zap.Int("batch_size", batchSize),
	)

	// Function to submit the current batch
	submitBatch := func(batchToSubmit []IndividualTaskDetail) {
		if len(batchToSubmit) == 0 {
			return
		}
		batchData := BatchTask{
			Tasks:      batchToSubmit,
			NatsClient: nc,
		}
		// Increment WaitGroup for the number of tasks in this batch
		wg.Add(len(batchToSubmit))
		if err := pool.Invoke(batchData); err != nil {
			logger.Log.Warn("Failed to invoke worker pool for batch", zap.Int("batch_task_count", len(batchToSubmit)), zap.Error(err))
			// Need to decrement wg if invoke fails
			wg.Add(-len(batchToSubmit)) // Decrement by the number we failed to submit
			// Also record errors for these tasks
			for _, taskDetail := range batchToSubmit {
				observer.IncLoadgenPublishErrors(taskDetail.BaseSubject, taskDetail.CompanyID)
			}
		}
	}

	for {
		select {
		case <-ctx.Done():
			logger.Log.Info("Load generation loop stopping due to context cancellation. Submitting final partial batch...")
			submitBatch(currentBatch) // Submit any remaining tasks
			return
		case <-durationTimer.C:
			logger.Log.Info("Load generation loop stopping after specified duration. Submitting final partial batch...")
			submitBatch(currentBatch) // Submit any remaining tasks
			return
		case <-ticker.C:
			// Check if context is already cancelled before proceeding
			select {
			case <-ctx.Done():
				logger.Log.Debug("Context cancelled during ticker processing, skipping new task addition.")
				// Don't submit anything new, just let the main loop exit handle final batch
				return
			default:
				// Context not cancelled, proceed
			}

			// Determine which message to generate
			selectedSubject := subjects[messageCounter%len(subjects)]
			selectedCompany := companies[messageCounter%len(companies)]
			messageCounter++

			// Record the attempt
			observer.IncLoadgenMessagesAttempted(selectedSubject, selectedCompany)

			// Add to current batch
			currentBatch = append(currentBatch, IndividualTaskDetail{
				BaseSubject: selectedSubject,
				CompanyID:   selectedCompany,
			})

			// If batch is full, submit it
			if len(currentBatch) >= batchSize {
				submitBatch(currentBatch)
				currentBatch = make([]IndividualTaskDetail, 0, batchSize) // Reset for next batch
			}
		}
	}
}

// batchWorkerFunc processes a batch of tasks.
func batchWorkerFunc(data interface{}, wg *sync.WaitGroup) {
	batchTask := data.(BatchTask)
	// Retrieve history count from flags (we need access here or pass it down)
	// A bit hacky, but avoids changing the Task/BatchTask structs significantly
	historyCountFlag := flag.Lookup("history-count")
	historyCount := 10 // Default if flag lookup fails
	if historyCountFlag != nil {
		hc, err := strconv.Atoi(historyCountFlag.Value.String())
		if err == nil && hc > 0 {
			historyCount = hc
		}
	}

	for _, taskDetail := range batchTask.Tasks {
		// Process each task within the batch
		func(td IndividualTaskDetail) {
			defer wg.Done() // Done is called for each task in the batch

			finalSubject := fmt.Sprintf("%s.%s", td.BaseSubject, td.CompanyID)
			var payloadBytes []byte
			var payload interface{}
			var err error

			switch td.BaseSubject {
			case string(model.V1ChatsUpsert):
				payload = model.NewUpsertChatPayload(&model.UpsertChatPayload{CompanyID: td.CompanyID, AgentID: td.CompanyID})
			case string(model.V1MessagesUpsert):
				payload = model.NewUpsertMessagePayload(&model.UpsertMessagePayload{CompanyID: td.CompanyID, AgentID: td.CompanyID})
			case string(model.V1ContactsUpsert):
				payload = model.NewUpsertContactPayload(&model.UpsertContactPayload{CompanyID: td.CompanyID, AgentID: td.CompanyID})
			case string(model.V1Agents):
				payload = model.NewUpsertAgentPayload(&model.UpsertAgentPayload{CompanyID: td.CompanyID})
			case string(model.V1HistoricalChats):
				payload = model.NewHistoryChatPayload(&historyCount, &model.UpsertChatPayload{CompanyID: td.CompanyID, AgentID: td.CompanyID})
			case string(model.V1HistoricalMessages):
				payload = model.NewHistoryMessagePayload(&historyCount, &model.UpsertMessagePayload{CompanyID: td.CompanyID, AgentID: td.CompanyID})
			case string(model.V1HistoricalContacts):
				payload = model.NewHistoryContactPayload(&historyCount, &model.UpsertContactPayload{CompanyID: td.CompanyID, AgentID: td.CompanyID})
			case string(model.V1ChatsUpdate), string(model.V1MessagesUpdate), string(model.V1ContactsUpdate):
				logger.Log.Debug("Skipping update payload generation in loadgen batch", zap.String("subject", td.BaseSubject))
				observer.IncLoadgenMessagesPublished(td.BaseSubject, td.CompanyID)
				return // Skip marshal/publish for this task
			default:
				logger.Log.Error("Unsupported base subject for payload generation in batch", zap.String("subject", td.BaseSubject))
				observer.IncLoadgenPublishErrors(td.BaseSubject, td.CompanyID)
				return // Don't proceed for this task
			}

			payloadBytes, err = json.Marshal(payload)
			if err != nil {
				logger.Log.Error("Failed to marshal payload in batch",
					zap.String("subject", finalSubject),
					zap.String("type", fmt.Sprintf("%T", payload)),
					zap.Error(err))
				observer.IncLoadgenPublishErrors(td.BaseSubject, td.CompanyID)
				return // Don't proceed for this task
			}

			headers := map[string]string{"CompanyID": td.CompanyID}
			if err := batchTask.NatsClient.Publish(finalSubject, payloadBytes, headers); err != nil {
				logger.Log.Error("Failed to publish message in batch", zap.String("subject", finalSubject), zap.Error(err))
				observer.IncLoadgenPublishErrors(td.BaseSubject, td.CompanyID)
			} else {
				observer.IncLoadgenMessagesPublished(td.BaseSubject, td.CompanyID)
			}
		}(taskDetail) // Execute the processing logic for the taskDetail
	}
}
