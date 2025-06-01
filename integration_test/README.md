# Integration Tests for Daisi WA Events Processor

This directory contains integration tests for the Daisi WA Events Processor, which verify the end-to-end functionality of the service with its dependencies.

## Overview

The integration tests use [testcontainers-go](https://golang.testcontainers.org/) to set up the following components:

- PostgreSQL database (for storing messages and chats)
- NATS JetStream (message broker)
- The Daisi WA Events Processor application itself (built from the Dockerfile)

The tests publish messages to NATS and verify that they are correctly processed and stored in the PostgreSQL database.

## Requirements

- Go 1.23 or higher
- Docker runtime
- Docker Compose (optional, for manual testing)

## Running the Tests

### Running All Tests

To run all integration tests:

```bash
cd integration_test
go mod download
go test -v
```

### Running Specific Tests

To run a specific test:

```bash
cd integration_test
go test -v -run TestIntegrationMessageEventService/TestChatUpsert
```

To run invalid input tests:

```bash
cd integration_test
go test -v -run TestInvalidInputSuite
```

To run a specific invalid input test (example assuming subtest naming based on ID):

```bash
cd integration_test
go test -v -run TestInvalidInputSuite/IV-01_MalformedJSON
```

### Running in Short Mode

To skip integration tests in short mode:

```bash
cd integration_test
go test -short
```

## Test Structure

- `main_test.go`: Main test file that sets up the test environment and contains overarching test suites.
- `database_test.go`: Database container setup, verification logic, and cleanup functions.
- `nats_test.go`: NATS container setup and message publishing functions.
- `nats_connection_test.go`: Helper functions for NATS stream/consumer management.
- `nats_server_connection_test.go`: Tests for NATS server connection issues.
- `nats_stream_lifecycle_test.go`: Tests for NATS stream lifecycle management.
- `nats_consumer_lifecycle_test.go`: Tests for NATS consumer lifecycle management.
- `app_test.go`: Application container setup.
- `fixtures_test.go`: Test payload generation helpers.
- `invalid_input_test.go`: Test cases for invalid inputs and schema validation.
- `semantic_validation_test.go`: Test cases for semantic data validation.
- `db_connection_test.go`: Test cases for database connection issues.
- `partial_data_test.go`: Test cases for handling partial data.
- `data_integrity_test.go`: Test cases for data integrity.
- `tenant_isolation_test.go`: Test cases for tenant isolation.
- `recovery_resilience_test.go`: Test cases for service recovery and resilience.
- `onboarding_log_test.go`: Test cases for onboarding log creation.
- `postgres_repo_init_test.go`: Tests for PostgreSQL repository initialization.
- `message_repository_test.go`, `chat_repository_test.go`, `contact_repository_test.go`, `agent_repository_test.go`: Repository layer tests.
- `agent_events_test.go`: Tests for agent-related NATS events.
- `payload/`: Directory for any static JSON payloads (though most are generated dynamically).
- `test-case.md`: Document listing detailed implemented integration test cases.
- `summary.md`: Detailed summary of all test files, suites, and test cases implemented.

## Test Coverage

The integration tests cover:

1.  **Core Event Processing**:
    -   Chat creation and updates
    -   Message creation and updates
    -   Agent event processing

2.  **Repository Layer Functionality**:
    -   Direct testing of database interactions for messages, chats, contacts, and agents.
    -   PostgreSQL repository initialization, including schema and partition creation.

3.  **Database Verification and Integrity**:
    -   Confirming events are properly stored in PostgreSQL.
    -   Verifying data integrity, consistency, and handling of duplicates or conflicts.
    -   Ensuring tenant isolation at the database and event processing level.

4.  **Edge Cases and Error Scenarios**:
    -   Invalid input validation (implemented)
        -   Malformed JSON payloads
        -   Missing required fields
        -   Invalid field types (schema and semantic)
        -   Oversized payloads
        -   Empty payloads
        -   Schema version mismatches
        -   Invalid/future timestamps
        -   Invalid entity references
    -   Resource errors (DB down, NATS server/connection/stream/consumer issues) (implemented)
    -   Partial and incomplete data handling (implemented)
    -   Recovery and resilience scenarios (e.g., service restart, JetStream redelivery, poison messages, transaction rollbacks) (implemented)
    -   Onboarding log creation for new contacts (implemented)

-   See `test-case.md` for a detailed list of implemented test cases.
-   See `summary.md` for a comprehensive description of each test suite and test case.

## Test Case Plan

The `test-case.md` file now serves as a detailed inventory of implemented integration test cases, categorized by the area they target. This includes:

1.  **Input Validation Test Cases**: Testing invalid payloads, schema validation, and semantic validation.
2.  **Resource Error Test Cases**: Simulating database and NATS connection/lifecycle issues.
3.  **Data Consistency Tests**: Ensuring proper handling of partial data, data integrity, and tenant isolation.
4.  **Recovery and Resilience Tests**: Verifying service behavior during outages and restarts.

While `test-case.md` lists what is currently implemented, future work could expand into areas like more advanced resource limitation, security, and performance testing.

## Implementation Status

Currently implemented test categories (see `test-case.md` for specific test IDs and `summary.md` for detailed descriptions of each test):

-   ✅ **Input Validation Tests**
    -   Invalid Payload Format (IV-01 to IV-05 in `invalid_input_test.go`)
    -   Schema Validation (SV-01 to SV-04 in `invalid_input_test.go`)
    -   Semantic Validation (SM-01, SM-02, SM-04 in `semantic_validation_test.go`)
-   ✅ **Resource Error Tests**
    -   Database Connection Issues (DB-01, DB-03, DB-04, DB-05 in `db_connection_test.go`)
    -   NATS Connection & Lifecycle Issues (MQ-01, MQ-02 in `nats_server_connection_test.go`; MQ-03 in `nats_stream_lifecycle_test.go`; MQ-04 in `nats_consumer_lifecycle_test.go`)
-   ✅ **Data Consistency Tests**
    -   Partial/Incomplete Data (PD-01, PD-02, PD-03, PD-05 in `partial_data_test.go`)
    -   Data Integrity (DI-01, DI-03, DI-04, DI-05 in `data_integrity_test.go`)
    -   Company (Tenant) Isolation (TI-01 to TI-03 in `tenant_isolation_test.go`)
-   ✅ **Recovery and Resilience Tests**
    -   Service Restart, JetStream Retry, Poison Messages, Transaction Rollback (RC-01 to RC-04 in `recovery_resilience_test.go`)
-   ✅ **Other Core Functionality**
    -   Onboarding Log Creation (`onboarding_log_test.go`)
    -   Agent Event Processing (`agent_events_test.go`)
    -   Repository Layer Tests (`message_repository_test.go`, `chat_repository_test.go`, `contact_repository_test.go`, `agent_repository_test.go`)
    -   PostgreSQL Repository Initialization (`postgres_repo_init_test.go`)

Refer to `test-case.md` for the granular list of test cases and `summary.md` for their detailed descriptions and corresponding Go test functions.

## Adding New Tests

1.  Create a new test function within an existing `_test.go` file or create a new one if testing a new component/category.
2.  If needed, add new JSON payloads in the `payload/` directory (though dynamic generation in `fixtures_test.go` is preferred).
3.  Implement the test logic, leveraging helper functions from `main_test.go`, `database_test.go`, `nats_test.go`, etc.
4.  Run the tests with `go test -v`.
5.  Add a corresponding entry to `test-case.md` with a unique ID and description.
6.  Update `summary.md` by re-generating or manually adding the summary for the new test/file.

## Troubleshooting

### Container Logs

When a test fails, the logs from all containers are captured and printed to the test output to help with troubleshooting. You can also stream logs during test execution by calling methods like `s.StreamLogs(t)` from the `E2EIntegrationTestSuite`.

### Common Issues

-   **Port Conflicts**: If you have local services running on the same ports, the tests may fail.
-   **Resource Constraints**: These tests require Docker resources. Ensure your Docker daemon has enough memory and CPU allocated.
-   **Network Issues**: If tests fail with network errors, ensure Docker networking is working correctly.

### Database Reset

Between test runs, the test framework automatically truncates all relevant PostgreSQL tables within the active tenant's schema to ensure a clean state. This is typically handled in the `SetupTest` method of the test suites.

## Contributing

When adding new tests or modifying existing ones, please:

1.  Follow the existing code structure and naming conventions.
2.  Add appropriate documentation within the Go files and update `summary.md` and `test-case.md`.
3.  Test both success and failure scenarios where applicable.
4.  Ensure all resources are cleaned up properly by the testcontainer framework or suite `TearDown` methods. 