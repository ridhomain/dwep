package healthcheck

import (
	"context"
	"net/http"

	"gitlab.com/timkado/api/daisi-wa-events-processor/pkg/utils"
	"go.uber.org/zap"
)

// Server represents a health check HTTP server
type Server struct {
	httpServer *http.Server
	mux        *http.ServeMux // Expose mux for adding handlers
	logger     *zap.Logger
}

// HealthResponse is the response structure for health check endpoints
type HealthResponse struct {
	Status  string            `json:"status"`
	Version string            `json:"version,omitempty"`
	Details map[string]string `json:"details,omitempty"`
}

// NewServer creates a new health check server
func NewServer(port string, logger *zap.Logger) *Server {
	mux := http.NewServeMux()

	server := &Server{
		httpServer: &http.Server{
			Addr:    ":" + port,
			Handler: mux,
		},
		mux:    mux, // Store the mux
		logger: logger,
	}

	// Register default health check endpoints
	mux.HandleFunc("/health", server.handleHealth)
	mux.HandleFunc("/ready", server.handleReady)

	return server
}

// RegisterMetricsHandler adds the /metrics endpoint handler.
// Should only be called if metrics are enabled.
func (s *Server) RegisterMetricsHandler(handler http.Handler) {
	s.logger.Info("Registering /metrics endpoint")
	s.mux.Handle("/metrics", handler)
}

// Start begins the HTTP server
func (s *Server) Start() {
	go func() {
		s.logger.Info("Starting health check server", zap.String("addr", s.httpServer.Addr))
		if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.logger.Error("Health check server error", zap.Error(err))
		}
	}()
}

// Stop gracefully shuts down the HTTP server
func (s *Server) Stop(ctx context.Context) error {
	s.logger.Info("Stopping health check server")
	return s.httpServer.Shutdown(ctx)
}

// handleHealth handles the /health endpoint for liveness probes
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	resp := HealthResponse{
		Status:  "UP",
		Version: "1.0.0",
	}

	utils.WriteJSONResponse(w, http.StatusOK, resp)
}

// handleReady handles the /ready endpoint for readiness probes
func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	// Here you could check database connections, etc.
	// For now, we'll just return OK
	resp := HealthResponse{
		Status: "READY",
		Details: map[string]string{
			"timestamp": utils.FormatISO8601(utils.Now()),
		},
	}

	utils.WriteJSONResponse(w, http.StatusOK, resp)
}
