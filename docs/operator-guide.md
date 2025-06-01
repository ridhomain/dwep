# Operator Guide - Daisi WA Events Processor

This guide provides information for operators managing the Daisi WA Events Processor, focusing on monitoring, troubleshooting, and configuration related to the retry and Dead-Letter Queue (DLQ) system.

## 1. Monitoring the DLQ System

The health and performance of the DLQ worker and the overall retry process can be monitored using Prometheus metrics and structured logs.

### 1.1 Prometheus Metrics

The following key Prometheus metrics are exposed by the service (usually available at the `/metrics` endpoint on the health check port):

*   **DLQ Worker Activity:**
    *   `dlq_fetch_requests_total`: Counter for total fetch attempts made by the worker to the DLQ stream. Helps understand worker activity.
    *   `dlq_fetch_errors_total`: Counter for errors during DLQ fetches. Increases might indicate NATS connectivity issues or stream problems.
    *   `dlq_queue_length`: Gauge showing the current number of messages buffered in the worker's internal channel (`msgCh`) before being processed by the pool. High values might indicate the worker pool is saturated or processing is slow.
    *   `dlq_tasks_submitted_total`: Counter for total messages successfully submitted to the worker's processing pool (`ants` pool).
    *   `dlq_workers_active`: Gauge showing the current number of active goroutines processing DLQ messages in the pool. Should generally be less than or equal to the configured worker pool size (`NATS_DLQWORKERS`).

*   **DLQ Processing Outcome:**
    *   `dlq_processing_duration_seconds`: Histogram measuring the time taken to process a single DLQ message (including retries if applicable within that attempt). Useful for identifying performance bottlenecks.
    *   `dlq_task_retries_total`: Counter for messages that failed processing by the worker and were NAKed with a delay for a subsequent retry attempt. Indicates transient failures being retried.
    *   `dlq_acks_success_total`: Counter for messages successfully processed by the worker and ACKed. This is the desired outcome.
    *   `dlq_acks_failure_total`: Counter for messages that failed acknowledgment (e.g., Terminated due to unrecoverable errors, persistence failure, NAK failures). Excludes delayed NAKs counted by `dlq_task_retries_total`.
    *   `dlq_tasks_dropped_total`: Counter for messages that exceeded the maximum retry attempts by the worker and were successfully persisted to the `exhausted_events` table before being Terminated/ACKed. **Increases in this metric require investigation.**

*   **Main Consumer DLQ Actions:**
    *   `message_event_service_event_processing_actions_total{action="dlq_published_ack_success", ...}`: Counter tracking how many messages were successfully published to the DLQ by the main consumers.
    *   `message_event_service_event_processing_actions_total{action="nak_dlq_marshal_fail", ...}`: Counter for failures marshalling the DLQ payload.
    *   `message_event_service_event_processing_actions_total{action="nak_dlq_publish_fail", ...}`: Counter for failures publishing to the DLQ stream.

### 1.2 Logging

- **DLQ Worker Logs:** Filter logs for `logger: dlq_worker`. Key messages include:
    - "Starting DLQ worker..."
    - "Processing DLQ message" (Includes `source_subject`, sequences)
    - "Failed to process event from DLQ" (Indicates an error during reprocessing attempt)
    - "Retrying DLQ message with backoff" (Shows delay calculated)
    - "Max retries exceeded, attempting to persist to exhausted store"
    - "Failed to save exhausted event..." (Indicates failure persisting to Postgres)
    - "Successfully processed event from DLQ"
- **Main Consumer Logs:** Look for messages indicating DLQ publishing attempts or failures (search for "DLQ").

## 2. Handling Exhausted Events

Messages that fail all retry attempts by the DLQ worker are persisted to the `exhausted_events` table in the PostgreSQL database.

### 2.1 Inspecting Exhausted Events

Connect to the PostgreSQL database for the relevant tenant and query the `exhausted_events` table:

```sql
SELECT
    id,
    created_at,
    company_id,
    source_subject,
    last_error,
    retry_count,
    event_timestamp,
    dlq_payload::jsonb ->> 'original_payload' as original_payload, -- Extract original payload
    resolved,
    resolved_at,
    notes
FROM
    exhausted_events
WHERE
    company_id = '<tenant_company_id>'
    AND resolved = false -- Focus on unresolved issues
ORDER BY
    created_at DESC;

```

- `last_error`: Shows the error message from the final failed processing attempt.
- `original_payload`: Contains the raw JSON payload of the message that failed.
- `dlq_payload`: Contains the full DLQ message structure, including retry metadata.

### 2.2 Reprocessing / Resolution

Currently, there is no automated reprocessing mechanism for exhausted events. Manual intervention is required:

1.  **Analyze the `last_error` and `original_payload`**: Understand why the message failed processing repeatedly.
2.  **Fix Underlying Issue**: Address the root cause (e.g., data corruption, bug in handler logic, external dependency issue).
3.  **Manual Reprocessing (Optional)**: If the event is still relevant and the issue is fixed, the `original_payload` can be manually re-published to the appropriate NATS subject (e.g., using the `cmd/tester` tool or another NATS client). Ensure the idempotency keys/logic will handle this correctly.
4.  **Mark as Resolved**: Update the record in the `exhausted_events` table:
    ```sql
    UPDATE exhausted_events
    SET
        resolved = true,
        resolved_at = NOW(),
        notes = 'Manually reprocessed after fixing bug XYZ / Marked as resolved, data no longer needed.'
    WHERE
        id = <event_id>;
    ```

## 3. Configuration

The DLQ worker behavior is influenced by configuration values (typically set via environment variables or the `default.yaml` file):

*   `nats.dlqWorkers` (Env: `NATS_DLQWORKERS`): Number of concurrent goroutines in the worker pool processing DLQ messages. Default: `8`.
*   `nats.dlqBaseDelayMinutes` (Env: `NATS_DLQBASEDELAYMINUTES`): The base delay (in minutes) for the first retry by the worker using exponential backoff. Default: `5`.
*   `nats.dlqMaxDelayMinutes` (Env: `NATS_DLQMAXDELAYMINUTES`): The maximum delay (in minutes) applied during exponential backoff by the worker. Default: `5`.
*   `nats.dlqStream` (Env: `NATS_DLQSTREAM`): Name of the Dead Letter Queue stream. Default: `dlq_stream`.
*   `nats.dlqSubject` (Env: `NATS_DLQSUBJECT`): Base subject for DLQ messages (e.g., `v1.dlq`). Default: `v1.dlq`.
*   `nats.dlqMaxAgeDays` (Env: `NATS_DLQMAXAGEDAYS`): Retention period for DLQ messages (in days). Default: `7`.
*   `nats.dlqMaxDeliver` (Env: `NATS_DLQMAXDELIVER`): Max redelivery attempts configured on the NATS consumer for the DLQ worker. Default: `10`.
*   `nats.dlqAckWait` (Env: `NATS_DLQACKWAIT`): Ack wait timeout configured on the NATS consumer for the DLQ worker. Default: `30s`.
*   `nats.dlqMaxAckPending` (Env: `NATS_DLQMAXACKPENDING`): Max pending ACKs configured on the NATS consumer for the DLQ worker. Default: `1000`.

## 4. Troubleshooting

*   **High `dlq_queue_length`**: Worker pool might be too small (`NATS_DLQWORKERS`), or processing takes too long. Check `dlq_processing_duration_seconds` and worker logs for errors.
*   **High `dlq_fetch_errors_total`**: Check NATS server connectivity and the health of the `dlq_stream`.
*   **High `dlq_task_retries_total`**: Indicates frequent transient errors during reprocessing. Investigate the errors logged by the worker.
*   **Increasing `dlq_tasks_dropped_total`**: Messages are consistently failing even after retries. Investigate the `exhausted_events` table for root causes.
*   **Worker not processing messages**: Ensure the DLQ worker process is running, check its logs for startup errors or continuous fetch errors. Verify the DLQ stream (`dlq_stream`) and consumer (`dlq_worker_consumer`) exist and are healthy in NATS. 