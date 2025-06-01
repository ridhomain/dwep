# Testing Guide

This guide covers the testing approach for the Daisi WA Events Processor, including unit tests, integration tests, and best practices.

## 1. Types of Tests

### Unit Tests (Package-Level)

Package-level unit tests verify individual components in isolation using mocks for external dependencies.

- **Location**: Co-located with source files (`*_test.go`)
- **Coverage**: Aim for 70%+ code coverage (critical paths)

**Key Unit Test Packages**:

- `/internal/usecase`: Business logic tests (`message_service_test.go`, `chat_service_test.go`, etc.)
- `/internal/ingestion`: Event routing, handler tests (`router_test.go`, `jetstream_test.go`)
- `/internal/storage`: Repository tests for Postgres
- `/pkg/utils`: Utility function tests

Example unit test command:
```bash
go test ./internal/usecase -v
```

### Benchmark Tests (Handler Performance)

Benchmark tests measure the performance of specific handlers, particularly the `RealtimeHandler` and `HistoricalHandler` in the `internal/ingestion/handler` package. These tests mock the repository layer to isolate handler and usecase logic performance.

**Location**: `internal/ingestion/handler/*_benchmark_test.go`

**How to Run**:

Using the Makefile target:
```bash
make benchmark-handlers
```

Directly using go test:
```bash
# Run all benchmarks in the handler package
go test -bench=. -benchmem -run=^$ ./internal/ingestion/handler/...

# Run a specific benchmark function (e.g., BenchmarkRealtimeHandler_ChatUpsert)
go test -bench=BenchmarkRealtimeHandler_ChatUpsert -benchmem -run=^$ ./internal/ingestion/handler/...
```

These commands will output performance statistics, including time per operation (`ns/op`), memory allocated per operation (`B/op`), and number of allocations per operation (`allocs/op`).

### Integration Tests (End-to-End)

Integration tests verify the entire system using real dependencies via [testcontainers-go](https://golang.testcontainers.org/).

- **Location**: `/integration_test` directory
- **Key Files**: 
  - `main_test.go` - Core test suites and environment setup.
  - `database_test.go` - PostgreSQL container setup and DB helpers.
  - `nats_test.go` - NATS container setup and publishing helpers.
  - `app_test.go` - Service application container setup.
  - `fixtures_test.go` - Test data and payload generation.
  - Specific test files for different categories, e.g.:
    - `invalid_input_test.go`
    - `semantic_validation_test.go`
    - `db_connection_test.go`
    - `nats_server_connection_test.go`, `nats_stream_lifecycle_test.go`, `nats_consumer_lifecycle_test.go`
    - `partial_data_test.go`
    - `data_integrity_test.go`
    - `tenant_isolation_test.go`
    - `recovery_resilience_test.go`
    - `onboarding_log_test.go`
    - `agent_events_test.go`
    - Repository specific tests (`*_repository_test.go`)
  - `test-case.md` - List of implemented test cases.
  - `summary.md` - Detailed summary of test implementations.

### Schema Validation Tests

Tests to ensure event payloads conform to our expected schemas.

- Validate event payloads against JSON schemas (primarily in `fixtures_test.go` and `helper_test.go` through `validatePayload`)
- Protect against breaking API changes
- Useful for service contract validation

## 2. How to Run Tests

### Unit Tests

```bash
# Run all unit tests
go test -v ./...

# Run specific package tests
go test -v ./internal/usecase
go test -v ./internal/storage
go test -v ./pkg/utils

# Run with race detection
go test -race ./...

# Run with coverage
go test -cover ./...
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out  # View coverage report in browser
```

### Integration Tests

```bash
# Run all integration tests
cd integration_test
go test -v

# Run specific integration test suite or test
# (Example: running the E2EIntegrationTestSuite, or a specific test within it if named accordingly)
go test -v -run E2EIntegrationTestSuite # Or a more specific test name like E2EIntegrationTestSuite/TestChatUpsertEvent

# Example for running a specific test file's suite (e.g., InvalidInputTestSuite)
cd integration_test
go test -v -run TestInvalidInputSuite # Or a specific subtest like TestInvalidInputSuite/IV-01_MalformedJSON
```

### Watch Mode (Using third-party tools)

For development with continuous testing:

```bash
# Using goconvey (install first: go get github.com/smartystreets/goconvey)
goconvey

# Using reflex (install first: go get github.com/cespare/reflex)
reflex -r '\.go$' -s -- sh -c 'go test ./...'
```

## 3. Integration Test Requirements

### Prerequisites

- Docker installed and running
- No services running on the following ports (or configure dynamic mapping):
  - PostgreSQL: 5432
  - NATS: 4222

### Required Files

- `integration_test/init.sql` - Postgres schema initialization for the default test tenant.
- `integration_test/payload/` - Directory for any static JSON test payloads (though dynamic generation is preferred).
- `integration_test/main_test.go` - Defines base test suites and orchestrates test runs.
- `integration_test/test-case.md` - Lists implemented test cases.
- `integration_test/summary.md` - Details all implemented tests and suites.
- Various `*_test.go` files containing the actual test logic.

### Container Setup

The integration tests automatically:

1. Launch PostgreSQL and NATS containers using `testcontainers-go`.
2. Initialize schemas (e.g., for the default tenant using `init.sql` and on-the-fly for other tenants) and truncate tables before tests.
3. Start the application container (built from the project's Dockerfile).
4. Run test cases by publishing events to NATS and verifying outcomes in PostgreSQL.
5. Clean up all containers on completion.

## 4. Test Data

### JSON Test Payloads

Test payloads are primarily generated dynamically using helper functions in `integration_test/fixtures_test.go`. 
The `integration_test/payload/` directory is available for any static JSON samples if needed, and includes:

- Examples for chat events
- Examples for message events 
- Both valid and potentially invalid samples to test error handling (though most invalid cases are constructed in code).
- JSON schemas (located in `internal/model/jsonschema/`) are used for validation.

### Mock Event Generation

The CLI tester tool can generate mock events for testing against a running service instance:

```bash
# Generate chat upsert event
go run cmd/tester/main.go -type chats.upsert -tenant test_tenant

# Generate message update event
go run cmd/tester/main.go -type messages.update
```

### Schema Validation

Payloads generated or published via `E2EIntegrationTestSuite.PublishEvent` are typically validated against their JSON schemas (defined in `internal/model/jsonschema/`). The validation logic is often invoked via `validatePayload` helper from `integration_test/helper_test.go`.

For manual schema validation against JSON payloads:

```bash
# Using ajv (Node.js, install with: npm install -g ajv-cli)
# Example: ajv validate -s internal/model/jsonschema/chat.upsert.schema.json -d path/to/payload.json

# Using go-jsonschema-validator (if a similar tool is in tools/)
# Example: go run tools/validate.go -schema internal/model/jsonschema/chat.upsert.schema.json -json path/to/payload.json
```

## 4.5 Load Testing with Mock Events

The project includes a dedicated CLI tool (`cmd/tester/main.go`) specifically designed for generating load against the NATS JetStream instance, primarily for stress and performance testing. 

**Integration with Load Test Environment:**

The load generator is typically run against the environment set up by `docker-compose.loadtest.yaml`. You use the `make run-tester` target, which builds and runs the tester CLI, passing additional flags after `--`.

**Example Load Generation Scenarios:**

```sh
# Scenario 1: Simulate average load (50 msg/sec per tenant)
make run-tester -- --subjects v1.chats.upsert,v1.messages.upsert \
                   --company_ids CompanyAAA01,CompanyBBB02 \
                   --rate 100 --duration 5m # 100 total = 50 per tenant

# Scenario 2: Simulate stress load (100 msg/sec per tenant)
make run-tester -- --subjects v1.chats.upsert,v1.messages.upsert \
                   --company_ids CompanyAAA01,CompanyBBB02 \
                   --rate 200 --duration 3m

# Scenario 3: Test historical data ingestion performance
make run-tester -- --subjects v1.history.messages \
                   --company_ids CompanyAAA01 \
                   --rate 20 --history-count 50 --duration 2m

# Scenario 4: High concurrency scenario
make run-tester -- --subjects v1.chats.upsert,v1.messages.upsert \
                   --company_ids CompanyAAA01,CompanyBBB02 \
                   --rate 1000 --concurrency 40 --batch-size 150 --duration 1m
```

Refer to the `docs/setup.md` file under "Load Testing Scenarios" for more examples and explanations of the available flags (`--subjects`, `--rate`, `--duration`, `--concurrency`, `--company_ids`, `--batch-size`, `--history-count`, etc.).

Monitoring the system using Prometheus and Grafana (available in the load test environment) during these tests is essential to identify bottlenecks and verify performance targets.

## 5. Expected Logs

### Success Logs

```
[INFO] [system] Service starting ┃ version=0.1.0
[INFO] [db] Connected to PostgreSQL database
[INFO] [queue] Connected to NATS JetStream
[INFO] [queue] Created stream message_events (example, actual stream names might vary or be tenant-specific)
[INFO] [queue] Created consumer tenant-event-consumer (example, actual consumer names might vary)
[INFO] [api] Chat processor received event ┃ tenant=tenant_abc • event_id=12345 (example format)
[INFO] [api] Successfully processed message event ┃ tenant=tenant_abc • msg_id=123 • type=messages.upsert
```

### Failure Logs

```
[ERROR] [db] Failed to connect to PostgreSQL ┃ error=connection refused
[ERROR] [queue] Failed to create consumer ┃ error=stream not found • stream=some_stream_name
[ERROR] [api] Invalid event payload ┃ tenant=tenant_abc • error=missing required field • field=jid
[WARN] [api] Duplicate message received ┃ tenant=tenant_abc • msg_id=123 • action=skipping
```

### Log Analysis

Logs can be analyzed:

- During test execution with `-v` flag
- In CI environment logs
- Using structured log queries when exported to a log aggregator
- Filtering for specific contexts like `[api]` or `[db]`

## 6. Best Practices

1. **Test Isolation**: Each test should be independent and not rely on others (achieved via suite setups and DB truncation).
2. **Clean Fixtures**: Use code-generated fixtures (from `integration_test/fixtures_test.go`) over static JSON where feasible.
3. **Clear Failures**: Tests should fail with clear messages indicating the issue, leveraging `testify/assert` and `testify/require`.
4. **Parallel Safety**: Tests within suites can be marked with `t.Parallel()` if they are independent, but entire suites run sequentially by default.
5. **External Config**: Use environment variables for test configuration where appropriate (e.g., `TEST_COMPANY_ID`).
6. **Realistic Data**: Test with realistic data volumes and edge cases as defined in `integration_test/test-case.md`.

## 7. CI Integration (Plan)
Need to automatically run in the CI pipeline:

- On pull requests
- On merges to main branch
- Nightly for regression testing

## 8. Troubleshooting

### Common Test Failures

1. **Port Conflicts**: Ensure no local services are using required ports
   ```bash
   # Check for processes using ports
   lsof -i :5432
   lsof -i :4222
   ```

2. **Docker Issues**: Verify Docker is running and has sufficient resources
   ```bash
   docker info
   ```

3. **Test Timeouts**: Increase timeouts for slower environments if necessary.
   ```go
   // Example of increasing timeout in tests
   // (though testcontainers often manages its own startup timeouts)
   ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second) // Adjusted example
   defer cancel()
   ```

4. **Container Logs**: View logs for more details on failures. Test output usually includes logs from failed test runs.
   ```bash
   # In another terminal during test run (if needed, less common now with testcontainer log capture):
   docker logs <container_id>
   ``` 