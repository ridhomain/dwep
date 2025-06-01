# Company Daisi WA Events Processor

**Multi-tenant Event-Driven Message Microservice** for ingesting realtime and historical events related to `chats`, `messages`, and `contacts`.

Built with **Golang**, **NATS JetStream**, **PostgreSQL**, with full support for **contextual logging**, **tracing**, and **metrics**.

---

## Features

- [x] Event-Driven architecture via NATS JetStream
- [x] Multi-tenant: deploy per tenant with isolated DB schema/data
- [x] Contextual logging with `zap` (request ID, company ID, etc.)
- [x] Observability: Prometheus metrics
- [x] Helm chart & Kubernetes manifests for deployment
- [x] CLI tool to send mock events for testing/load generation
- [x] Dead-Letter Queue (DLQ) with retry worker for handling processing failures

---

## Tech Stack

| Purpose           | Tech/Lib                                |
|------------------|------------------------------------------|
| Language          | Golang 1.23+                             |
| Event Broker      | NATS JetStream                           |
| SQL Database      | PostgreSQL (via GORM)                    |
| Logging           | Uber Zap (context-aware)                 |
| Config Management | Viper                                    |
| Validation        | go-playground/validator, JSON Schema     |
| Observability     | Prometheus Metrics                       |
| Testing           | Go testing, Testcontainers-go            |
| Worker Pool       | ants (for DLQ & Onboarding)              |

---

## Folder Structure

```
daisi-wa-events-processor/
├── cmd/
│   ├── main/               
│   │   └── main.go          # main service entrypoint
│   └── tester/              # CLI untuk testing
│       └── main.go          # entrypoint CLI (e.g. `go run cmd/tester/main.go`)
├── internal/
│   ├── apperrors/           # Application-specific errors
│   ├── config/              # Configuration loading (Viper)
│   ├── dlqworker/           # Dead-Letter Queue processing logic
│   ├── healthcheck/         # HTTP health/readiness/metrics endpoints
│   ├── usecase/             # Core business logic services
│   │   ├── agent_service.go
│   │   ├── agent_service_test.go
│   │   ├── chat_service.go
│   │   ├── chat_service_test.go
│   │   ├── contact_service.go
│   │   ├── contact_service_test.go
│   │   ├── exhausted_service.go
│   │   ├── message_service.go
│   │   ├── message_service_test.go
│   │   ├── onboarding_worker.go # Worker pool implementation
│   │   ├── onboarding_worker_test.go
│   │   ├── processor.go
│   │   ├── processor_test.go
│   │   ├── service.go
│   │   └── service_test.go
│   ├── ingestion/           # NATS message consuming logic
│   │   ├── handler/         # Specific event type handlers (realtime, historical)
│   │   │   ├── historical.go
│   │   │   ├── realtime.go
│   │   │   ├── interface.go
│   │   │   ├── historical_test.go
│   │   │   ├── realtime_test.go
│   │   │   └── mock/            # Mocks for handlers
│   │   ├── mock/            # Mocks for ingestion layer testing
│   │   ├── interface.go
│   │   ├── jetstream.go     # NATS consumer implementation details
│   │   └── router.go        # Event routing logic
│   ├── jetstream/           # NATS client wrapper and setup utilities
│   │   ├── mock/            # Mock for NATS client
│   │   └── interface.go
│   ├── model/               # Data structures (payloads, entities, events)
│   │   ├── jsonschema/      # JSON schema definitions for validation
│   │   ├── agent.go
│   │   ├── chat.go
│   │   ├── contact.go
│   │   ├── event.go
│   │   ├── event_test.go
│   │   ├── exhausted_event.go
│   │   ├── factories.go     # Test data factories
│   │   ├── message.go
│   │   ├── onboarding_log.go
│   │   ├── payload.go
│   │   └── payload_factories.go
│   ├── storage/             # Database interaction layer (PostgreSQL)
│   │   ├── adapters.go
│   │   ├── postgres.go
│   │   ├── postgres_test.go
│   │   ├── postgres_agent.go
│   │   ├── postgres_agent_test.go
│   │   ├── postgres_chat.go
│   │   ├── postgres_chat_test.go
│   │   ├── postgres_contact.go
│   │   ├── postgres_contact_test.go
│   │   ├── postgres_exhausted_event.go
│   │   ├── postgres_exhausted_event_test.go
│   │   ├── postgres_message.go
│   │   ├── postgres_message_test.go
│   │   ├── postgres_onboarding_log.go
│   │   ├── postgres_onboarding_log_test.go
│   │   ├── repository.go
│   │   └── mock/            # Mock for storage repository
│   ├── tenant/              # Tenant context handling
│   └── observer/            # Observability components
│       └── metrics.go       # Prometheus metrics definitions & helpers
│   └── validator/           # JSON schema validation logic
│       └── validator.go
├── pkg/
│   ├── utils/               # global helper functions
│   ├── logger/
│   │   └── logger.go        # Zap config + global logger init
│   └── middleware/          # (opsional) for inject logger to context, etc
├── scripts/                 # CLI, event sender, etc (planned)
├── integration_test/        # integration test for this service
│   ├── main_test.go         # Main test file with core test cases
│   ├── database_test.go     # Database container setup and verification
│   ├── nats_test.go         # NATS container setup and messaging
│   ├── app_test.go          # Application container setup
│   ├── payload/             # JSON test payloads
│   └── README.md            # Integration test documentation
├── proto/                   # (Optional) Protobuf for NATS message contract
├── docs/                    # documentation related to this project
├── deploy/    
│   ├── manifest/      # (opsional) k8s manifest for deployment of this service
│   └── helm/          # (opsional) helm chart for deployment of this service
├── Dockerfile
├── docker-compose.yaml     # for local development env
├── docker-compose.dev.yaml # for enhanced local dev with service build
├── docker-compose.loadtest.yaml # for load testing environment
├── go.mod
└── README.md
```

---

## Config

Configuration is handled via Viper, loading from (in order of priority):
1.  Environment Variables (prefixed with ``, `.` replaced by `_`, e.g., `NATS_URL`)
2.  `.env` file (if present)
3.  `internal/config/default.yaml`

See `docs/setup.md` for a full list of environment variables.

### Example `.env`
```env
POSTGRES_DSN=postgres://postgres:postgres@localhost:54321/message_service
NATS_URL=nats://localhost:4222
COMPANY_ID=CompanyDev01
LOG_LEVEL=debug
```

---

## Run Locally

### Using Docker Compose (Recommended)

```bash
# Initialize data directories (first time only)
make init

# Start NATS & PostgreSQL
make up

# Build and run the service (watches for changes)
make dev

# View logs
make logs

# Stop services
make down
```

### Without Docker

Ensure PostgreSQL and NATS are running accessible at the configured DSN/URL.

```bash
go mod tidy
export POSTGRES_DSN="postgres://postgres:postgres@localhost:54321/message_service"
export NATS_URL="nats://localhost:4222"
export COMPANY_ID="CompanyDev01"
go run cmd/main/main.go
```

Metrics available at: [http://localhost:8080/metrics](http://localhost:8080/metrics) (if enabled, default)
Health/Readiness at: [http://localhost:8080/health](http://localhost:8080/health), [http://localhost:8080/ready](http://localhost:8080/ready)

---

## Event Tester Tool (Load Generator)

The project includes a tester CLI tool (`cmd/tester/main.go`) for generating and publishing batches of mock events to NATS JetStream, useful for functional and load testing.

### Usage

Use the `make run-tester` target. Pass arguments after `--`.

```bash
make run-tester -- [options]
```

### Key Options

*   `--subjects`: Comma-separated list of base subjects (e.g., `v1.chats.upsert,v1.messages.upsert`)
*   `--rate`: Target total messages per second (default: 100)
*   `--duration`: Load test duration (default: 1m)
*   `--concurrency`: Number of concurrent publisher workers (default: 10)
*   `--company_ids`: Comma-separated list of Company IDs (default: from config/env)
*   `--batch-size`: Number of messages per worker batch (default: 50)
*   `--history-count`: Number of items per historical payload (default: 10)
*   `--metrics-port`: Port for the tester's own Prometheus metrics (default: 9091)
*   `--log-level`: Log level for the tester (default: from config/env)

See `docs/setup.md` for more examples.

### Examples

```bash
# Publish 5 historical chat events for CompanyDev01
make run-tester -- --subjects v1.history.chats --company_ids CompanyDev01 --count 5

# Run a small load test for 30s for two companies
make run-tester -- --subjects v1.chats.upsert,v1.messages.upsert --company_ids CompanyAAA01,CompanyBBB02 --rate 50 --duration 30s
```

---

## Integration Tests

The project includes comprehensive integration tests using [testcontainers-go](https://golang.testcontainers.org/) to verify end-to-end functionality.

### Test Components

- PostgreSQL
- NATS JetStream
- The service application itself

### Running Integration Tests

```bash
cd integration_test
go test -v
```

### Running Specific Tests

```bash
cd integration_test
go test -v -run TestIntegrationMessageEventService/TestChatUpsert
```

### Test Coverage

1.  **Core Event Processing**
    - Chat creation and updates
    - Message creation and updates
    - Contact creation and updates
    - Agent status updates
2.  **Historical Data Processing**
    - Historical chat, message, and contact data import
3.  **Database Verification**
    - Confirm events are properly stored
    - Verify data integrity
4.  **DLQ Handling** 
    - Verification of messages sent to DLQ

---

## Logger Usage

Use the logger obtained via `logger.Log` (initialized in `main.go`). For context propagation (planned):

```go
// Placeholder: Example if context logger is implemented
// logger := tenant.LoggerFromContext(ctx)
// logger.Info("handling event", zap.String("company_id", companyID))

// Current usage (global logger):
logger.Log.Info("Processing event", zap.String("company_id", companyID), zap.String("event_type", eventType))
```

---

## Observability

- **Prometheus metrics**: Exposed at `/metrics` on the main server port (default 8080). See [Monitoring Guide](docs/monitoring.md).
- **Structured Logging**: Via Uber Zap, includes `company_id` where available.

---

## Roadmap

- [x] NATS JetStream Consumer skeleton
- [x] Event validation layer (JSON Schema + Go Validator)
- [x] Full DB ops for chat, message, contact, agent
- [x] Onboarding Log worker pool
- [x] CLI tool for testing (batch mode)
- [x] Integration test suite
- [x] Helm chart & K8s manifests
- [x] DLQ Worker implementation
- [x] Prometheus Metrics
- [ ] OpenTelemetry Tracing integration
- [ ] Enhanced DLQ handling/reprocessing options

---

## 📚 Documentation Index

- [Overview](docs/overview.md)
- [Architecture](docs/architecture.md)
- [Setup Guide](docs/setup.md)
- [Consumer Reference](docs/consumer.md)
- [Event Versioning Guide](docs/event_versioning.md)
- [JSON Schema Reference](docs/schema.md)
- [Testing Guide](docs/testing.md)
- [Troubleshooting Guide](docs/troubleshooting.md)
- [Monitoring Guide (Prometheus/Grafana)](docs/monitoring.md)
- [Operator Guide (DLQ Management)](docs/operator-guide.md)
