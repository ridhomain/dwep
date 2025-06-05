package observer

import (
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	metricsEnabled = true // Flag to control metric collection

	// Labels for standard event metrics
	eventProcessingLabels = []string{"event_type", "company_id", "consumer_type"}
	// Labels for agent-specific event metrics
	agentEventLabels = []string{"event_type", "company_id", "agent_id", "jid"}
	// Labels for tracking specific processing actions
	eventActionLabels = []string{"event_type", "company_id", "consumer_type", "action", "error_type"}

	// Standard Event Counters (now with consumer_type)
	EventsReceivedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "message_event_service_events_received_total",
			Help: "Total number of events received from NATS, labeled by consumer type.",
		},
		eventProcessingLabels, // Use new labels
	)
	EventsProcessedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "message_event_service_events_processed_total",
			Help: "Total number of events successfully processed and acknowledged, labeled by consumer type.",
		},
		eventProcessingLabels, // Use new labels
	)
	EventsFailedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "message_event_service_events_failed_total",
			Help: "Total number of events that failed processing (resulting in Nack or error), labeled by consumer type.",
		},
		eventProcessingLabels, // Use new labels
	)

	// Agent-specific Event Counter (remains unchanged for now)
	EventWithAgentReceivedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "message_event_service_event_with_agent_received_total",
			Help: "Total number of events received that contain an agent_id.",
		},
		agentEventLabels, // Keep original labels
	)

	// New Histogram for Processing Duration
	EventProcessingDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "message_event_service_event_processing_duration_seconds",
			Help:    "Histogram of event processing durations.",
			Buckets: prometheus.ExponentialBuckets(0.01, 2, 12), // 10ms to ~20s
		},
		eventProcessingLabels, // Label by type, company, consumer
	)

	// New Histogram for Routing Duration
	EventRoutingDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "message_event_service_event_routing_duration_seconds",
			Help:    "Histogram of event routing specific durations (time spent in router.Route).",
			Buckets: prometheus.ExponentialBuckets(0.005, 2, 12), // 5ms to ~10s, potentially shorter than total processing
		},
		eventProcessingLabels, // Label by type, company, consumer
	)

	// New Counter for Specific Actions
	EventProcessingActionsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "message_event_service_event_processing_actions_total",
			Help: "Total count of specific actions taken after event processing, labeled by error type.",
		},
		eventActionLabels, // Label by type, company, consumer, action, error_type
	)

	// Global metrics instance
	Metrics *metricsStore
)

// Metrics related to DLQ processing
var (
	dlqFetchRequestsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "dlq_fetch_requests_total",
		Help: "Total number of fetch requests made to the DLQ stream.",
	})
	dlqFetchErrorsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "dlq_fetch_errors_total",
		Help: "Total number of errors encountered during DLQ fetch requests.",
	})
	dlqQueueLength = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "dlq_queue_length",
		Help: "Current number of messages waiting in the internal DLQ worker channel.",
	})
	dlqWorkersActive = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "dlq_workers_active",
		Help: "Current number of active worker goroutines in the DLQ pool.",
	})

	// Labels for tenant-specific DLQ metrics
	dlqTenantLabels = []string{"company_id"}

	dlqTasksSubmittedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dlq_tasks_submitted_total",
			Help: "Total number of tasks submitted to the DLQ worker pool.",
		},
		dlqTenantLabels,
	)
	dlqProcessingDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "dlq_processing_duration_seconds",
			Help:    "Histogram of processing durations for DLQ messages.",
			Buckets: prometheus.DefBuckets, // Default buckets: .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10
		},
		dlqTenantLabels,
	)
	// Note: Using NumDelivered directly as retry count might be slightly off if initial delivery fails before DLQ.
	// Using a label for retry count might be excessive cardinality. We'll count total retries for simplicity.
	dlqTaskRetriesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dlq_task_retries_total",
			Help: "Total number of retry attempts (NAKs with delay) for DLQ messages.",
		},
		dlqTenantLabels,
	)
	dlqAcksSuccessTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dlq_acks_success_total",
			Help: "Total number of successful acknowledgements (ACKs) for DLQ messages.",
		},
		dlqTenantLabels,
	)
	dlqAcksFailureTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dlq_acks_failure_total",
			Help: "Total number of failed acknowledgements (NAKs, Term) for DLQ messages (excluding retries).",
		},
		dlqTenantLabels,
	)
	dlqTasksDroppedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dlq_tasks_dropped_total",
			Help: "Total number of DLQ messages dropped after exceeding max retries.",
		},
		dlqTenantLabels,
	)
)

// Labels for database operations
var (
	dbOperationLabels = []string{"operation", "entity", "company_id", "status"} // Added status

	// New Histogram for Database Operation Duration
	DatabaseOperationDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "message_event_service_db_operation_duration_seconds",
			Help:    "Histogram of database operation durations.",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 15), // 1ms to ~16s
		},
		dbOperationLabels, // Label by operation, entity, company, status
	)
)

// --- New Onboarding Worker Pool Metrics ---
var (
	onboardingLabels       = []string{"company_id"}
	onboardingStatusLabels = []string{"company_id", "status"}

	onboardingTasksSubmittedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "onboarding_tasks_submitted_total",
			Help: "Total number of onboarding tasks submitted to the worker pool.",
		},
		onboardingLabels,
	)
	onboardingTasksProcessedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "onboarding_tasks_processed_total",
			Help: "Total number of onboarding tasks processed by the worker pool, labeled by final status.",
		},
		onboardingStatusLabels, // company_id, status
	)
	onboardingProcessingDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "onboarding_processing_duration_seconds",
			Help:    "Histogram of processing durations for onboarding tasks.",
			Buckets: prometheus.DefBuckets, // Default buckets are usually sufficient
		},
		onboardingLabels,
	)
	onboardingQueueLength = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "onboarding_queue_length",
		Help: "Approximate number of tasks waiting in the onboarding worker pool queue.",
	})
)

// --- New Load Generator Metrics ---
var (
	loadgenLabels = []string{"subject", "company_id"} // Labels for loadgen metrics

	loadgenMessagesAttemptedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "loadgen_messages_attempted_total",
			Help: "Total number of messages the load generator attempted to publish.",
		},
		loadgenLabels,
	)
	loadgenMessagesPublishedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "loadgen_messages_published_total",
			Help: "Total number of messages successfully published by the load generator.",
		},
		loadgenLabels,
	)
	loadgenPublishErrorsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "loadgen_publish_errors_total",
			Help: "Total number of errors encountered by the load generator during publishing.",
		},
		loadgenLabels,
	)
)

// metricsStore holds references to all Prometheus metrics.
type metricsStore struct {
	// Initialize fields if needed, though promauto handles it.
	// httpRequestsTotal:      httpRequestsTotal,
	// httpRequestDuration:    httpRequestDuration,
	// Example for DLQ (already handled by promauto):
	// dlqFetchRequestsTotal: dlqFetchRequestsTotal,
	// ... other dlq metrics
}

// InitMetrics initializes and registers the Prometheus metrics if enabled.
// Call this function during application startup.
func InitMetrics(enabled bool) {
	if !enabled {
		// Unregister default collectors if metrics are disabled
		// prometheus.Unregister(collectors.NewBuildInfoCollector()) // Causing issues, comment out
		// prometheus.Unregister(collectors.NewGoCollector())        // Causing issues, comment out
		// prometheus.Unregister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{})) // Causing issues, comment out
		return
	}

	metricsEnabled = true

	// If we reach here, metrics are enabled.
	// Metrics are already auto-registered via promauto, so no explicit registration needed here.
	// We can keep this function for potential future global setup if required.
	// Example: Registering custom collectors or setting global labels.
	Metrics = &metricsStore{
		// Initialize fields if needed, though promauto handles it.
		// httpRequestsTotal:      httpRequestsTotal,
		// httpRequestDuration:    httpRequestDuration,
		// Example for DLQ (already handled by promauto):
		// dlqFetchRequestsTotal: dlqFetchRequestsTotal,
		// ... other dlq metrics
	}
}

// IncEventsReceived increments the events received counter.
func IncEventsReceived(eventType, tenant, consumerType string) {
	if !metricsEnabled {
		return
	}
	EventsReceivedTotal.WithLabelValues(eventType, sanitizeTenant(tenant), consumerType).Inc()
}

// IncEventsProcessed increments the events processed counter.
func IncEventsProcessed(eventType, tenant, consumerType string) {
	if !metricsEnabled {
		return
	}
	EventsProcessedTotal.WithLabelValues(eventType, sanitizeTenant(tenant), consumerType).Inc()
}

// IncEventsFailed increments the events failed counter.
func IncEventsFailed(eventType, tenant, consumerType string) {
	if !metricsEnabled {
		return
	}
	EventsFailedTotal.WithLabelValues(eventType, sanitizeTenant(tenant), consumerType).Inc()
}

// sanitizeTenant ensures the tenant label is valid or returns a default value.
func sanitizeTenant(tenant string) string {
	if tenant == "" {
		return "unknown"
	}
	// Add more sanitization if needed (e.g., length limits, allowed characters)
	return tenant
}

// RecordHTTPRequest increments the request counter and observes duration.
// ... existing code ...

// --- DLQ Metric Helpers ---

// IncDlqFetchRequest increments the DLQ fetch request counter.
func IncDlqFetchRequest() {
	if Metrics != nil { // Check if metrics are initialized/enabled
		dlqFetchRequestsTotal.Inc()
	}
}

// IncDlqFetchError increments the DLQ fetch error counter.
func IncDlqFetchError() {
	if Metrics != nil {
		dlqFetchErrorsTotal.Inc()
	}
}

// SetDlqQueueLength sets the current DLQ internal queue length.
func SetDlqQueueLength(length int) {
	if Metrics != nil {
		dlqQueueLength.Set(float64(length))
	}
}

// IncDlqTasksSubmitted increments the counter for tasks submitted to the pool.
func IncDlqTasksSubmitted(companyID string) {
	if Metrics != nil {
		dlqTasksSubmittedTotal.WithLabelValues(sanitizeTenant(companyID)).Inc()
	}
}

// SetDlqWorkersActive sets the current number of active DLQ workers.
func SetDlqWorkersActive(count int) {
	if Metrics != nil {
		dlqWorkersActive.Set(float64(count))
	}
}

// ObserveDlqProcessingDuration records the processing time for a DLQ message.
func ObserveDlqProcessingDuration(companyID string, duration time.Duration) {
	if Metrics != nil {
		dlqProcessingDurationSeconds.WithLabelValues(sanitizeTenant(companyID)).Observe(duration.Seconds())
	}
}

// IncDlqTaskRetry increments the counter for DLQ message retry attempts.
func IncDlqTaskRetry(companyID string) {
	if Metrics != nil {
		dlqTaskRetriesTotal.WithLabelValues(sanitizeTenant(companyID)).Inc()
	}
}

// IncDlqAckSuccess increments the counter for successful DLQ message ACKs.
func IncDlqAckSuccess(companyID string) {
	if Metrics != nil {
		dlqAcksSuccessTotal.WithLabelValues(sanitizeTenant(companyID)).Inc()
	}
}

// IncDlqAckFailure increments the counter for failed DLQ message ACKs/TERMs (non-retry).
func IncDlqAckFailure(companyID string) {
	if Metrics != nil {
		dlqAcksFailureTotal.WithLabelValues(sanitizeTenant(companyID)).Inc()
	}
}

// IncDlqTasksDropped increments the counter for DLQ messages dropped after max retries.
func IncDlqTasksDropped(companyID string) {
	if Metrics != nil {
		dlqTasksDroppedTotal.WithLabelValues(sanitizeTenant(companyID)).Inc()
	}
}

// --- New/Modified Metric Helpers ---

// ObserveEventProcessingDuration records the processing time for a specific event.
func ObserveEventProcessingDuration(eventType, tenant, consumerType string, duration time.Duration) {
	if !metricsEnabled {
		return
	}
	EventProcessingDurationSeconds.WithLabelValues(eventType, sanitizeTenant(tenant), consumerType).Observe(duration.Seconds())
}

// ObserveEventRoutingDuration records the routing time for a specific event.
func ObserveEventRoutingDuration(eventType, tenant, consumerType string, duration time.Duration) {
	if !metricsEnabled {
		return
	}
	EventRoutingDurationSeconds.WithLabelValues(eventType, sanitizeTenant(tenant), consumerType).Observe(duration.Seconds())
}

// ObserveDbOperationDuration records the duration for a database operation.
func ObserveDbOperationDuration(operation, entity, companyID string, duration time.Duration, err error) {
	if !metricsEnabled {
		return
	}
	status := "success"
	if err != nil {
		status = "error"
	}
	DatabaseOperationDurationSeconds.WithLabelValues(operation, entity, sanitizeTenant(companyID), status).Observe(duration.Seconds())
}

// IncEventProcessingAction increments the counter for a specific processing outcome.
func IncEventProcessingAction(eventType, tenant, consumerType, action, errorType string) {
	if !metricsEnabled {
		return
	}
	// Sanitize errorType if needed, ensure it's not overly granular
	sanitizedErrorType := SanitizeErrorType(errorType)
	EventProcessingActionsTotal.WithLabelValues(eventType, sanitizeTenant(tenant), consumerType, action, sanitizedErrorType).Inc()
}

// SanitizeErrorType maps specific errors or provides a default category.
// Keep this simple to avoid high cardinality.
func SanitizeErrorType(errStr string) string {
	// If no error (e.g., for success actions), return "none"
	if errStr == "" || errStr == "none" {
		return "none"
	}

	// Simple categorization based on common patterns or known error types
	switch {
	case strings.Contains(errStr, "database"), strings.Contains(errStr, "SQL"), strings.Contains(errStr, "duplicate key"), strings.Contains(errStr, "constraint"), strings.Contains(errStr, "connection"):
		return "database"
	case strings.Contains(errStr, "validation failed"), strings.Contains(errStr, "bad request"), strings.Contains(errStr, "invalid"), strings.Contains(errStr, "missing field"):
		return "validation"
	case strings.Contains(errStr, "not found"), strings.Contains(errStr, "no rows"):
		return "not_found"
	case strings.Contains(errStr, "nats"), strings.Contains(errStr, "jetstream"):
		return "nats"
	case strings.Contains(errStr, "timeout"), strings.Contains(errStr, "deadline exceeded"):
		return "timeout"
	case strings.Contains(errStr, "unmarshal"), strings.Contains(errStr, "json"):
		return "unmarshal"
	case strings.Contains(errStr, "panic"):
		return "panic"
	default:
		return "unknown"
	}
}

// --- Onboarding Metric Helpers ---

// IncOnboardingTasksSubmitted increments the counter for submitted onboarding tasks.
func IncOnboardingTasksSubmitted(companyID string) {
	if Metrics != nil { // Use global Metrics check
		onboardingTasksSubmittedTotal.WithLabelValues(sanitizeTenant(companyID)).Inc()
	}
}

// IncOnboardingTasksProcessed increments the counter for processed onboarding tasks by status.
func IncOnboardingTasksProcessed(companyID, status string) {
	if Metrics != nil {
		onboardingTasksProcessedTotal.WithLabelValues(sanitizeTenant(companyID), status).Inc()
	}
}

// ObserveOnboardingProcessingDuration records the processing time for an onboarding task.
func ObserveOnboardingProcessingDuration(companyID string, duration time.Duration) {
	if Metrics != nil {
		onboardingProcessingDurationSeconds.WithLabelValues(sanitizeTenant(companyID)).Observe(duration.Seconds())
	}
}

// SetOnboardingQueueLength sets the current onboarding queue length.
func SetOnboardingQueueLength(length int) {
	if Metrics != nil {
		onboardingQueueLength.Set(float64(length))
	}
}

// --- Load Generator Metric Helpers ---

// IncLoadgenMessagesAttempted increments the counter for attempted message publications.
func IncLoadgenMessagesAttempted(subject, companyID string) {
	if Metrics != nil { // Use global Metrics check
		loadgenMessagesAttemptedTotal.WithLabelValues(subject, sanitizeTenant(companyID)).Inc()
	}
}

// IncLoadgenMessagesPublished increments the counter for successfully published messages.
func IncLoadgenMessagesPublished(subject, companyID string) {
	if Metrics != nil {
		loadgenMessagesPublishedTotal.WithLabelValues(subject, sanitizeTenant(companyID)).Inc()
	}
}

// IncLoadgenPublishErrors increments the counter for publishing errors.
func IncLoadgenPublishErrors(subject, companyID string) {
	if Metrics != nil {
		loadgenPublishErrorsTotal.WithLabelValues(subject, sanitizeTenant(companyID)).Inc()
	}
}
