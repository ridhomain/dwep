package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/config"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/dlqworker"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/healthcheck"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/jetstream"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/observer"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/storage"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/usecase"
	"gitlab.com/timkado/api/daisi-wa-events-processor/pkg/logger"
	"gitlab.com/timkado/api/daisi-wa-events-processor/pkg/utils"
	"go.uber.org/zap"
)

func main() {
	// Set timezone to UTC
	time.Local = time.UTC

	// Load configuration
	cfg, err := config.LoadConfig("")
	if err != nil {
		fmt.Printf("Failed to load config: %v\n", err)
		os.Exit(1)
	}

	// Initialize logger
	if err := logger.Initialize(cfg.LogLevel); err != nil {
		fmt.Printf("Failed to initialize logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	// Initialize Metrics conditionally
	// isProdLiveEnv := cfg.Environment == "production" || cfg.Environment == "live"
	metricsEnabled := cfg.Metrics.Enabled // Rely solely on the config flag
	observer.InitMetrics(metricsEnabled)

	// Log startup information
	logger.Log.Info("Starting Daisi WA Events Processor",
		zap.String("environment", cfg.Environment),
		zap.String("company_id", cfg.Company.ID),
		zap.String("nats_url", cfg.NATS.URL),
	)

	// Initialize repositories
	postgresRepo, err := initPostgresRepo(cfg.Database.PostgresDSN, cfg.Database.PostgresAutoMigrate, cfg.Company.ID)
	if err != nil {
		logger.Log.Fatal("Failed to initialize Postgres repository", zap.Error(err))
	}

	// Initialize JetStream client - now only takes URL
	jsClient, err := initJetStreamClient(cfg.NATS.URL)
	if err != nil {
		logger.Log.Fatal("Failed to initialize JetStream client", zap.Error(err))
	}

	// Create repository adapters for the service
	chatRepo := storage.NewChatRepoAdapter(postgresRepo)
	messageRepo := storage.NewMessageRepoAdapter(postgresRepo)
	contactRepo := storage.NewContactRepoAdapter(postgresRepo)
	agentRepo := storage.NewAgentRepoAdapter(postgresRepo)
	onboardingLogRepo := storage.NewOnboardingLogRepoAdapter(postgresRepo)
	exhaustedEventRepo := storage.NewExhaustedEventRepoAdapter(postgresRepo)

	// Create onboarding worker pool
	onboardingWorker, err := usecase.NewOnboardingWorker(
		cfg.WorkerPools.Onboarding,
		contactRepo,
		onboardingLogRepo,
		logger.Log, // Pass the base logger
	)
	if err != nil {
		logger.Log.Fatal("Failed to initialize onboarding worker pool", zap.Error(err))
	}

	// Create service, injecting the worker pool
	service := usecase.NewEventService(chatRepo, messageRepo, contactRepo, agentRepo, onboardingLogRepo, exhaustedEventRepo, onboardingWorker)

	// Create and set up processor - now takes the full config object
	processor := usecase.NewProcessor(service, jsClient, cfg, cfg.Company.ID)
	if err := processor.Setup(); err != nil {
		logger.Log.Fatal("Failed to set up processor", zap.Error(err))
	}

	// Create and initialize DLQ Worker - requires router from processor and exhausted repo
	dlqWorker, err := dlqworker.NewWorker(cfg, logger.Log, jsClient.NatsConn(), jsClient, processor.GetRouter(), exhaustedEventRepo)
	if err != nil {
		logger.Log.Fatal("Failed to initialize DLQ Worker", zap.Error(err))
	}

	// Create health check server
	healthServer := healthcheck.NewServer(strconv.Itoa(cfg.Server.Port), logger.Log)

	// Register metrics handler if enabled BEFORE starting the server
	if metricsEnabled {
		healthServer.RegisterMetricsHandler(promhttp.Handler())
		logger.Log.Info("Metrics endpoint enabled", zap.String("path", "/metrics"), zap.Int("port", cfg.Server.Port))
	} else {
		logger.Log.Info("Metrics endpoint disabled for environment", zap.String("environment", cfg.Environment))
	}

	// Start health check server (which now might include /metrics)
	healthServer.Start()

	logger.Log.Info("Health check endpoints available",
		zap.String("health", fmt.Sprintf("http://localhost:%d/health", cfg.Server.Port)),
		zap.String("readiness", fmt.Sprintf("http://localhost:%d/ready", cfg.Server.Port)),
	)

	// Start processor
	if err := processor.Start(); err != nil {
		logger.Log.Fatal("Failed to start processor", zap.Error(err))
	}

	// Start DLQ worker in a separate goroutine
	mainCtx, mainCancel := context.WithCancel(context.Background()) // Context for main lifecycle
	defer mainCancel()                                              // Ensure main context is cancelled on exit
	sigChan := make(chan os.Signal, 1)                              // Move channel creation before goroutine
	go func() {
		if err := dlqWorker.Start(mainCtx); err != nil {
			logger.Log.Error("DLQ Worker failed to start or encountered an error, initiating shutdown...", zap.Error(err))
			// Trigger shutdown
			mainCancel() // Cancel the main context
			// Attempt to send signal. This might block if main isn't reading yet,
			// but mainCancel() should eventually lead to shutdown.
			select {
			case sigChan <- syscall.SIGTERM: // Send signal if possible
			default: // Avoid blocking if channel is full or main isn't ready
				logger.Log.Warn("Could not send SIGTERM to signal channel immediately")
			}
		}
	}()

	// Wait for termination signal
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigChan
	logger.Log.Info("Received termination signal", zap.String("signal", sig.String()))

	// Signal main context cancellation for DLQ worker
	mainCancel()

	// Graceful shutdown with timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	logger.Log.Info("Starting graceful shutdown", zap.Duration("timeout", 30*time.Second))

	// Use WaitGroup to track shutdown of all components
	var wg sync.WaitGroup

	// Num components is now 5 (processor, dlq worker, health server, onboarding worker, databases)
	numComponents := 5
	wg.Add(numComponents)

	// Gracefully shutdown all components

	// Shutdown processor (JetStream consumer)
	utils.SafeGo(func() {
		defer wg.Done()
		logger.Log.Info("[shutdown] Stopping event processor")
		start := time.Now()
		processor.Stop()
		logger.Log.Info("[shutdown] Event processor stopped",
			zap.Duration("duration", time.Since(start)))
	}, func(r interface{}, stack []byte) {
		logger.Log.Error("[shutdown] Panic while stopping event processor",
			zap.Any("panic", r),
			zap.ByteString("stack", stack),
		)
		wg.Done() // Ensure WaitGroup is decremented even in case of panic
	})

	// Shutdown DLQ Worker
	utils.SafeGo(func() {
		defer wg.Done()
		logger.Log.Info("[shutdown] Stopping DLQ worker")
		start := time.Now()
		dlqWorker.Stop() // Call the DLQ worker's stop method
		logger.Log.Info("[shutdown] DLQ worker stopped",
			zap.Duration("duration", time.Since(start)))
	}, func(r interface{}, stack []byte) {
		logger.Log.Error("[shutdown] Panic while stopping DLQ worker",
			zap.Any("panic", r),
			zap.ByteString("stack", stack),
		)
		wg.Done()
	})

	// Shutdown Onboarding Worker Pool
	utils.SafeGo(func() {
		defer wg.Done()
		logger.Log.Info("[shutdown] Stopping onboarding worker pool")
		start := time.Now()
		onboardingWorker.Stop()
		logger.Log.Info("[shutdown] Onboarding worker pool stopped",
			zap.Duration("duration", time.Since(start)))
	}, func(r interface{}, stack []byte) {
		logger.Log.Error("[shutdown] Panic while stopping onboarding worker pool",
			zap.Any("panic", r),
			zap.ByteString("stack", stack),
		)
		wg.Done()
	})

	// Shutdown health check server (includes metrics if enabled)
	utils.SafeGo(func() {
		defer wg.Done()
		logger.Log.Info("[shutdown] Stopping health check server")
		start := time.Now()
		if err := healthServer.Stop(shutdownCtx); err != nil {
			logger.Log.Error("[shutdown] Error stopping health check server", zap.Error(err))
		} else {
			logger.Log.Info("[shutdown] Health check server stopped",
				zap.Duration("duration", time.Since(start)))
		}
	}, func(r interface{}, stack []byte) {
		logger.Log.Error("[shutdown] Panic while stopping health check server",
			zap.Any("panic", r),
			zap.ByteString("stack", stack),
		)
		wg.Done() // Ensure WaitGroup is decremented even in case of panic
	})

	// Close database connections
	utils.SafeGo(func() {
		defer wg.Done()

		// Close Postgres Connection (repo itself doesn't have Close, need underlying DB)
		logger.Log.Info("[shutdown] Closing PostgreSQL connection")
		pgStart := time.Now()
		if err := postgresRepo.Close(shutdownCtx); err != nil {
			logger.Log.Error("[shutdown] Failed to close PostgreSQL connection", zap.Error(err))
		} else {
			logger.Log.Info("[shutdown] PostgreSQL connection closed",
				zap.Duration("duration", time.Since(pgStart)))
		}

		// Close JetStream connection
		logger.Log.Info("[shutdown] Closing JetStream connection")
		jsStart := time.Now()
		jsClient.Close()
		logger.Log.Info("[shutdown] JetStream connection closed",
			zap.Duration("duration", time.Since(jsStart)))
	}, func(r interface{}, stack []byte) {
		logger.Log.Error("[shutdown] Panic while closing database connections",
			zap.Any("panic", r),
			zap.ByteString("stack", stack),
		)
		wg.Done() // Ensure WaitGroup is decremented even in case of panic
	})

	// Wait with a timeout for all components to shut down
	waitCh := make(chan struct{})
	go func() {
		wg.Wait()
		close(waitCh)
	}()

	select {
	case <-waitCh:
		logger.Log.Info("[shutdown] All components stopped gracefully")
	case <-shutdownCtx.Done():
		logger.Log.Warn("[shutdown] Graceful shutdown timed out, forcing exit")
	}

	logger.Log.Info("Daisi WA Events Processor shutdown complete")
}

// Initialize PostgreSQL repository
func initPostgresRepo(dsn string, autoMigrate bool, companyID string) (*storage.PostgresRepo, error) {
	if dsn == "" {
		return nil, fmt.Errorf("postgres DSN is required")
	}

	repo, err := storage.NewPostgresRepo(dsn, autoMigrate, companyID)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize postgres repository: %w", err)
	}

	logger.Log.Info("Initialized PostgreSQL repository")
	return repo, nil
}

// initJetStreamClient initializes the JetStream client - updated signature
func initJetStreamClient(url string) (*jetstream.Client, error) {
	client, err := jetstream.NewClient(url)
	if err != nil {
		return nil, fmt.Errorf("failed to create JetStream client: %w", err)
	}
	// Note: Stream and consumer setup is now handled within the processor/consumer Setup methods
	return client, nil
}
