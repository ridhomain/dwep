# Monitoring Guide (Prometheus & Grafana)

This guide explains how to monitor the Daisi WA Events Processor using Prometheus for metrics collection and Grafana for visualization.

## Prometheus Integration

The service exposes Prometheus metrics on its main HTTP port (default `8080`) at the `/metrics` path. This endpoint needs to be scraped by a Prometheus server.

### Scraping Configuration (`prometheus.yml`)

The provided `prometheus.yml` used in the load test environment (`docker-compose.loadtest.yaml`) demonstrates how to scrape the service instances:

```yaml
global:
  scrape_interval: 15s

scrape_configs:
  # ... other jobs like nats, nats_exporter ...

  - job_name: 'tenant_app_A' # Job for Tenant A service instances
    metrics_path: /metrics  # Path where metrics are exposed
    dns_sd_configs:         # Use DNS service discovery (suitable for Docker Compose/Kubernetes)
      - names:
          - tenant_app_A    # Service name defined in docker-compose.loadtest.yaml
        type: A
        port: 8080          # Port the service listens on (from SERVER_PORT)

  - job_name: 'tenant_app_B' # Job for Tenant B service instances
    metrics_path: /metrics
    dns_sd_configs:
      - names:
          - tenant_app_B
        type: A
        port: 8080

  # ... prometheus self-scrape job ...
```

Key points:
- Prometheus scrapes the `/metrics` endpoint on port `8080`.
- `dns_sd_configs` allows Prometheus to discover all container replicas for `tenant_app_A` and `tenant_app_B` within the Docker network. Similar principles apply in Kubernetes using service discovery.

## Key Application Metrics

The following metrics are exposed by the Daisi WA Events Processor (defined in `internal/observer/metrics.go`).

### 1. Event Processing Metrics

These metrics track the flow of events consumed from NATS.

*   **`message_event_service_events_received_total`**
    *   **Type:** Counter
    *   **Description:** Total number of events received from NATS, labeled by consumer type.
    *   **Labels:** `event_type`, `company_id`, `consumer_type` (`realtime` or `historical`)

*   **`message_event_service_events_processed_total`**
    *   **Type:** Counter
    *   **Description:** Total number of events successfully processed and acknowledged, labeled by consumer type.
    *   **Labels:** `event_type`, `company_id`, `consumer_type`

*   **`message_event_service_events_failed_total`**
    *   **Type:** Counter
    *   **Description:** Total number of events that failed processing (resulting in Nack or error), labeled by consumer type.
    *   **Labels:** `event_type`, `company_id`, `consumer_type`

*   **`message_event_service_event_with_agent_received_total`**
    *   **Type:** Counter
    *   **Description:** Total number of events received that contain an agent_id.
    *   **Labels:** `event_type`, `company_id`, `agent_id`, `jid`

*   **`message_event_service_event_processing_duration_seconds`**
    *   **Type:** Histogram
    *   **Description:** Histogram of event processing durations (from reception to ack/nack).
    *   **Labels:** `event_type`, `company_id`, `consumer_type`

*   **`message_event_service_event_routing_duration_seconds`**
    *   **Type:** Histogram
    *   **Description:** Histogram of event routing specific durations (time spent in router.Route).
    *   **Labels:** `event_type`, `company_id`, `consumer_type`

*   **`message_event_service_event_processing_actions_total`**
    *   **Type:** Counter
    *   **Description:** Total count of specific actions taken after event processing (e.g., ack, nack, dlq publish), labeled by error type.
    *   **Labels:** `event_type`, `company_id`, `consumer_type`, `action` (`ack_success`, `nack_retryable`, `nack_fatal`, `dlq_published_ack_success`, `nak_dlq_marshal_fail`, `nak_dlq_publish_fail`), `error_type` (`none`, `database`, `validation`, `not_found`, `nats`, `timeout`, `unmarshal`, `panic`, `unknown`)

### 2. DLQ Worker Metrics

These metrics track the Dead-Letter Queue worker's activity.

*   **`dlq_fetch_requests_total`**
    *   **Type:** Counter
    *   **Description:** Total number of fetch requests made by the worker to the DLQ stream.
    *   **Labels:** None

*   **`dlq_fetch_errors_total`**
    *   **Type:** Counter
    *   **Description:** Total number of errors encountered during DLQ fetch requests.
    *   **Labels:** None

*   **`dlq_queue_length`**
    *   **Type:** Gauge
    *   **Description:** Current number of messages waiting in the internal DLQ worker channel before being processed by the pool.
    *   **Labels:** None

*   **`dlq_workers_active`**
    *   **Type:** Gauge
    *   **Description:** Current number of active worker goroutines in the DLQ pool.
    *   **Labels:** None

*   **`dlq_tasks_submitted_total`**
    *   **Type:** CounterVec
    *   **Description:** Total number of tasks submitted to the DLQ worker pool.
    *   **Labels:** `company_id`

*   **`dlq_processing_duration_seconds`**
    *   **Type:** HistogramVec
    *   **Description:** Histogram of processing durations for DLQ messages.
    *   **Labels:** `company_id`

*   **`dlq_task_retries_total`**
    *   **Type:** CounterVec
    *   **Description:** Total number of retry attempts (NAKs with delay) for DLQ messages initiated by the worker.
    *   **Labels:** `company_id`

*   **`dlq_acks_success_total`**
    *   **Type:** CounterVec
    *   **Description:** Total number of successful acknowledgements (ACKs) for DLQ messages processed by the worker.
    *   **Labels:** `company_id`

*   **`dlq_acks_failure_total`**
    *   **Type:** CounterVec
    *   **Description:** Total number of failed acknowledgements (NAKs, Term) for DLQ messages processed by the worker (excluding delayed NAKs counted by `dlq_task_retries_total`).
    *   **Labels:** `company_id`

*   **`dlq_tasks_dropped_total`**
    *   **Type:** CounterVec
    *   **Description:** Total number of DLQ messages dropped after exceeding max retries by the worker (and persisted to `exhausted_events`).
    *   **Labels:** `company_id`

### 3. Database Metrics

*   **`message_event_service_db_operation_duration_seconds`**
    *   **Type:** HistogramVec
    *   **Description:** Histogram of database operation durations.
    *   **Labels:** `operation` (e.g., `UpsertChat`, `UpdateMessageStatus`), `entity` (e.g., `Chat`, `Message`), `company_id`, `status` (`success` or `error`)

### 4. Onboarding Worker Metrics

*   **`onboarding_tasks_submitted_total`**
    *   **Type:** CounterVec
    *   **Description:** Total number of onboarding tasks submitted to the worker pool.
    *   **Labels:** `company_id`

*   **`onboarding_tasks_processed_total`**
    *   **Type:** CounterVec
    *   **Description:** Total number of onboarding tasks processed by the worker pool, labeled by final status.
    *   **Labels:** `company_id`, `status` (`success_created`, `success_exists`, `error_db`, `error_other`)

*   **`onboarding_processing_duration_seconds`**
    *   **Type:** HistogramVec
    *   **Description:** Histogram of processing durations for onboarding tasks.
    *   **Labels:** `company_id`

*   **`onboarding_queue_length`**
    *   **Type:** Gauge
    *   **Description:** Approximate number of tasks waiting in the onboarding worker pool queue.
    *   **Labels:** None

## Grafana Visualization

Grafana is recommended for visualizing the Prometheus metrics. The `docker-compose.loadtest.yaml` includes a Grafana service configured with provisioning:

-   **Datasource Provisioning:** `grafana/provisioning/datasources/prometheus.yml` likely configures the Prometheus instance (`http://prometheus:9090`) as a datasource.
-   **Dashboard Provisioning:** `grafana/provisioning/dashboards/provider.yml` likely configures Grafana to load dashboard JSON files from `grafana/dashboards/`.

### Accessing Grafana

In the load test environment, Grafana is typically accessible at `http://localhost:3000`. Login with the default credentials (unless changed via environment variables, e.g., `admin`/`asdfqwer` as per `docker-compose.loadtest.yaml`).

### Example Dashboards / Queries

You can build Grafana dashboards using the metrics above. Check the `grafana/dashboards/` directory for any pre-built dashboards.

Here are some example PromQL queries you might use in Grafana panels:

*   **Total Event Rate (per second, by type & consumer):**
    ```promql
    sum by (event_type, company_id, consumer_type) (rate(message_event_service_events_received_total[5m]))
    ```

*   **Event Failure Rate (percentage, by type & consumer):**
    ```promql
    sum by (event_type, company_id, consumer_type) (rate(message_event_service_events_failed_total[5m]))
    /
    sum by (event_type, company_id, consumer_type) (rate(message_event_service_events_received_total[5m]))
    * 100
    ```

*   **95th Percentile Event Processing Duration (by type & consumer):**
    ```promql
    histogram_quantile(0.95, sum by (le, event_type, company_id, consumer_type) (rate(message_event_service_event_processing_duration_seconds_bucket[5m])))
    ```

*   **DLQ Worker Queue Length:**
    ```promql
    dlq_queue_length
    ```

*   **Rate of DLQ Messages Dropped (per second, by company):**
    ```promql
    sum by (company_id) (rate(dlq_tasks_dropped_total[5m]))
    ```

*   **Database Operation Error Rate (percentage, by operation & entity):**
    ```promql
    sum by (operation, entity, company_id) (rate(message_event_service_db_operation_duration_seconds_count{status="error"}[5m]))
    /
    sum by (operation, entity, company_id) (rate(message_event_service_db_operation_duration_seconds_count[5m]))
    * 100
    ```

*   **Onboarding Worker Queue Length:**
    ```promql
    onboarding_queue_length
    ```

## Alerting

While not configured by default in this setup, you can use Prometheus Alertmanager to define alerting rules based on these metrics. Examples include:

-   High rate of event processing failures (`message_event_service_events_failed_total`).
-   Consistently high event processing duration (`message_event_service_event_processing_duration_seconds`).
-   High DLQ queue length (`dlq_queue_length`).
-   Any increase in dropped DLQ tasks (`dlq_tasks_dropped_total`).
-   High rate of database errors (`message_event_service_db_operation_duration_seconds_count{status="error"}`).

## Further Information

Refer to the main [Troubleshooting Guide](troubleshooting.md) for diagnosing specific issues related to metrics or monitoring components. 