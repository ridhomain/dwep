# Integration Test Suite Summary

This document summarizes the Go test files within the `integration_test` directory, detailing their test suites, helper functions, and individual test cases.

---

# File: integration_test/db_connection_test.go

## Test Suite

### `DBConnectionTestSuite`
-   **Description:** Tests database connection issues and recovery scenarios. It embeds `E2EIntegrationTestSuite` to inherit common testing infrastructure (App, DB, NATS, etc.).
-   **Setup Function:** `TestDBConnectionSuite(t *testing.T)` is used to run the suite.

## Helper Functions
*Based on the content, there are no standalone local helper functions explicitly defined in this file outside of the test suite methods or standard library/third-party library calls. The suite itself (`DBConnectionTestSuite`) and its embedded `E2EIntegrationTestSuite` provide helper methods like `s.StopService`, `s.StartService`, `s.PublishEvent`, `s.GenerateNatsPayload`, `verifyPostgresData`, `verifyPostgresDataWithSchema`, `executeQuery`, and `changeUriFromLocalhostToDocker`. These are part of the testing framework rather than general-purpose helpers local to this specific file.*

## Test Cases (Methods of `DBConnectionTestSuite`)

### 1. `TestDB01_ConnectionRefused()`
    -   **Description:** Tests how the service handles scenarios where database connections are refused.
    -   **Steps:**
        1.  Stops the PostgreSQL container to simulate connection refusal.
        2.  Sends a test message (chat upsert) while the database is down.
        3.  Restarts the PostgreSQL container.
        4.  Updates PostgreSQL connection details if they changed after restart.
        5.  Checks service logs for error handling, retry attempts, or circuit breaker patterns (implicitly, by expecting the test operator to check logs).
        6.  Verifies if the service implements a retry or queuing mechanism by checking if the chat message was eventually stored after database recovery. The test logs the outcome but doesn't fail if the message isn't stored, as this depends on the specific recovery implementation of the service.

### 2. `TestDB02_ConnectionTimeout()`
    -   **Description:** Tests how the service handles database connection timeouts.
    -   **Steps:**
        1.  Simulates database slowness/timeout by generating load on PostgreSQL (executing `pg_sleep(5)` repeatedly in a goroutine).
        2.  Sends a test message (chat upsert) during this high database load.
        3.  Stops the load generation.
        4.  Checks service logs for timeout errors, retry behavior, and circuit breaker patterns (implicitly).
        5.  Waits for the database to recover and then checks if the message sent during the load was eventually processed and stored. The test logs the outcome but doesn't fail if the message isn't stored.

### 3. `TestDB03_ConnectionRecovery()`
    -   **Description:** Tests how the service recovers its database connection after an outage and processes messages.
    -   **Steps:**
        1.  Stores a control message (pre-outage chat) successfully before simulating an outage.
        2.  Stops the PostgreSQL container.
        3.  Sends multiple messages (during-outage chat) while the database is down.
        4.  Restarts the PostgreSQL container and updates connection details.
        5.  Sends a post-recovery message.
        6.  Verifies that the post-recovery message was stored successfully.
        7.  Checks if the messages sent during the outage were eventually processed and stored after recovery. The test logs this outcome but doesn't fail based on it.

### 4. `TestDB04_QueryTimeout()`
    -   **Description:** Tests how the service handles database query timeouts, specifically when a table is locked.
    -   **Steps:**
        1.  Creates a blocking session in PostgreSQL by acquiring an `ACCESS EXCLUSIVE MODE` lock on the `chats` table within a transaction and holding it with `pg_sleep(30)`.
        2.  Sends a test message (chat upsert) that would need to access the locked `chats` table.
        3.  Waits for the service to attempt processing, expecting it to encounter a query timeout due to the lock.
        4.  Releases the lock by canceling the context of the goroutine holding the lock.
        5.  Gives the service time to retry the operation.
        6.  Checks if the message sent while the table was locked was eventually processed and stored. The test logs the outcome.
        7.  Sends a new "post-timeout" message to confirm the service is back to normal operations and verifies it's processed successfully.

---

# File: integration_test/nats_stream_lifecycle_test.go

## Test Suite

### `NATSStreamLifecycleTestSuite`
-   **Description:** Defines the suite for testing NATS stream lifecycle events, particularly how the application handles missing streams and their recreation. It embeds `E2EIntegrationTestSuite`.
-   **Setup Function:** `TestNATSStreamLifecycleSuite(t *testing.T)` is used to run the suite.
-   **Per-Test Setup:** `SetupTest()`:
    -   Calls the base `E2EIntegrationTestSuite.SetupTest()` (which includes DB truncation).
    -   Initializes a `zaptest` logger named "NATSStreamLifecycleTestSuite".
    -   Includes a short delay (1 second) before each test, logging that it's waiting for the service to process any remnants from previous tests.

## Helper Functions
*This file primarily uses helper functions from the embedded `E2EIntegrationTestSuite` (defined in `integration_test/main_test.go`) (e.g., `s.StopService`, `s.StartService`, `s.PublishEvent`, `s.GenerateNatsPayload`, `s.StreamLogs`) and shared utilities like `WaitUntilNatsReady` (defined in `integration_test/nats_connection_test.go`) and `verifyPostgresDataWithSchema` (defined in `integration_test/database_test.go`). `SubjectMapping` (global in `integration_test/main_test.go`) and `model.UpsertChatPayload` are also used from other packages/files.*

## Test Cases (Methods of `NATSStreamLifecycleTestSuite`)

### 1. `TestNATSStreamMissing()`
    -   **Description:** Tests the application's behavior when a NATS stream (`wa_events_stream`) is missing, specifically checking if the service can auto-create it upon restart and then process messages. This corresponds to test case MQ-03.
    -   **Steps:**
        1.  Logs the start of the test.
        2.  Ensures NATS server is ready.
        3.  Deletes the `wa_events_stream` if it exists.
        4.  Stops the application container.
        5.  Restarts the application container (using `setupTestApp`), expecting it to auto-create necessary streams.
        6.  Streams logs from the newly started application container.
        7.  Generates a test chat message payload for a specific tenant.
        8.  Publishes the message to the NATS subject associated with chat creation for that tenant. This publish is expected to succeed because the service restart should have recreated the stream.
        9.  Waits for the service to process the message.
        10. Verifies that the message was correctly processed and stored in the database for the correct tenant schema.
        11. Verifies that the `wa_events_stream` now exists.

---

# File: integration_test/nats_connection_test.go

## Test Suite
*This file does not define any `testify/suite` structures or `Test...` functions. It appears to contain only helper functions related to NATS JetStream management.*

## Helper Functions

### 1. `deleteStream(ctx context.Context, natsURL string, streamName string) error`
    -   **Description:** Deletes a NATS JetStream stream by its name.
    -   **Details:** Connects to NATS, gets a JetStream context, and then attempts to delete the specified stream. It handles the `jetstream.ErrStreamNotFound` error gracefully, meaning it's not an error if the stream doesn't exist. Logs the action.

### 2. `createStream(ctx context.Context, natsURL string, streamName string, subjects string) error`
    -   **Description:** Creates a NATS JetStream stream with standard configuration.
    -   **Details:** Connects to NATS, gets a JetStream context, and defines a `jetstream.StreamConfig` with `LimitsPolicy` retention, `FileStorage`, and a `MaxAge` of 24 hours. Attempts to create the stream and handles `jetstream.ErrStreamNameAlreadyInUse` gracefully. Logs the action.

### 3. `createStreamWithFlowControl(ctx context.Context, natsURL string, streamName string, subjects string, maxBytes int64) error`
    -   **Description:** Creates a NATS JetStream stream with a specific `MaxBytes` setting, intended for flow control testing.
    -   **Details:** Similar to `createStream` but adds the `MaxBytes` field to the `jetstream.StreamConfig`. Handles `jetstream.ErrStreamNameAlreadyInUse` gracefully. Logs the action.

### 4. `createStreamIfNotExists(ctx context.Context, natsURL string, streamName string, subjects string) error`
    -   **Description:** Checks if a NATS JetStream stream exists, and if not, creates it with standard configuration.
    -   **Details:** Connects to NATS, gets a JetStream context. First, it tries to get the stream info. If it exists, it logs and returns. If `jetstream.ErrStreamNotFound` is returned, it proceeds to create the stream using a standard configuration (similar to `createStream`). Logs its actions.

### 5. `deleteAllConsumers(ctx context.Context, natsURL string, streamName string) error`
    -   **Description:** Deletes all consumers associated with a given NATS JetStream stream.
    -   **Details:** Connects to NATS, gets JetStream context, and retrieves the stream. It then iterates through all consumers of the stream and deletes them one by one. Logs errors during individual consumer deletion but continues. Handles `jetstream.ErrStreamNotFound` for the stream itself or during consumer listing. Logs the number of deleted consumers.

### 6. `listConsumers(ctx context.Context, natsURL string, streamName string) ([]string, error)`
    -   **Description:** Lists the names of all consumers for a given NATS JetStream stream.
    -   **Details:** Connects to NATS, gets JetStream context, retrieves the stream. It iterates through consumer information and collects their names. Returns an empty list if the stream is not found.

### 7. `checkStreamExists(ctx context.Context, natsURL string, streamName string) (bool, error)`
    -   **Description:** Checks if a NATS JetStream stream exists.
    -   **Details:** Connects to NATS, gets JetStream context. It attempts to retrieve the stream information. Returns `true` if successful (stream exists), `false` if `jetstream.ErrStreamNotFound` occurs, and an error for other issues.

### 8. `WaitUntilNatsReady(ctx context.Context, natsURL string) error`
    -   **Description:** Waits for the NATS server at the given URL to become connectable.
    -   **Details:** It tries to connect to the NATS server in a loop with a 1-second ticker and a 30-second overall timeout. If a connection is successful, it closes the connection and returns `nil`. If it times out, it returns an error. Logs failed ping attempts.

---

# File: integration_test/nats_server_connection_test.go

## Test Suite

### `NATSServerAndConnectionTestSuite`
-   **Description:** Defines the suite for testing application behavior related to NATS server availability and connection stability. It embeds `E2EIntegrationTestSuite`.
-   **Setup Function:** `TestNATSServerAndConnectionSuite(t *testing.T)` runs the suite.
-   **Per-Test Setup:** `SetupTest()`:
    -   Calls the base `E2EIntegrationTestSuite.SetupTest()` (which includes DB truncation).
    -   Initializes a `zaptest` logger named "NATSServerAndConnectionTestSuite".
    -   Includes a short delay (1 second) before each test.

## Helper Functions
*This file uses helper functions from the embedded `E2EIntegrationTestSuite` (defined in `integration_test/main_test.go`) or other shared test utility files (e.g., `WaitUntilNatsReady` from `integration_test/nats_connection_test.go`, `createStreamIfNotExists` from `integration_test/nats_connection_test.go`, `deleteAllConsumers` from `integration_test/nats_connection_test.go`, `s.GenerateNatsPayload`, `s.PublishEvent`, `verifyPostgresDataWithSchema` from `integration_test/database_test.go`). `SubjectMapping` (global in `integration_test/main_test.go`) and `model.UpsertChatPayload` are also used from other packages/files.*

## Test Cases (Methods of `NATSServerAndConnectionTestSuite`)

### 1. `TestNATSServerDown()`
    -   **Description:** Tests the application's behavior when the NATS server is completely unavailable. This corresponds to test case MQ-01.
    -   **Steps:**
        1.  Generates a test chat message payload.
        2.  Stops the NATS container to simulate server unavailability.
        3.  Attempts to connect to NATS directly using `natsgo.Connect`, asserting that this connection fails.
        4.  Observes application logs for a period (10 seconds) to check for reconnection attempts by the service (implicitly).
        5.  Restarts the NATS container.
        6.  Waits for the NATS server to become ready.
        7.  Publishes the previously generated test message, expecting it to succeed now.
        8.  Streams application logs.
        9.  Waits for message processing.
        10. Verifies that the message was correctly processed and stored in the database.

### 2. `TestNATSConnectionDrop()`
    -   **Description:** Tests the application's behavior when an established NATS connection is dropped and then re-established (e.g., due to NATS server restart). This corresponds to test case MQ-02.
    -   **Steps:**
        1.  Publishes an initial test message (chat 1) and verifies it's processed successfully.
        2.  Restarts the NATS container (stop, wait, start) to simulate a connection drop and recovery.
        3.  Waits for the NATS server to become ready again.
        4.  Publishes a second test message (chat 2).
        5.  Waits for processing.
        6.  Verifies that the second message was processed correctly, indicating the service reconnected and resumed operations.

---

# File: integration_test/nats_consumer_lifecycle_test.go

## Test Suite

### `NATSConsumerLifecycleTestSuite`
-   **Description:** Defines the suite for testing NATS consumer lifecycle events, particularly how the application behaves when consumers are missing. It embeds `E2EIntegrationTestSuite`.
-   **Setup Function:** `TestNATSConsumerLifecycleSuite(t *testing.T)` runs the suite.
-   **Per-Test Setup:** `SetupTest()`:
    -   Calls the base `E2EIntegrationTestSuite.SetupTest()` (which includes DB truncation).
    -   Initializes a `zaptest` logger named "NATSConsumerLifecycleTestSuite".
    -   Includes a short delay (1 second) before each test.

## Helper Functions
*This file uses helper functions from the embedded `E2EIntegrationTestSuite` (defined in `integration_test/main_test.go`) or other shared test utility files (e.g., `WaitUntilNatsReady` from `integration_test/nats_connection_test.go`, `createStreamIfNotExists` from `integration_test/nats_connection_test.go`, `deleteAllConsumers` from `integration_test/nats_connection_test.go`, `s.GenerateNatsPayload`, `s.PublishEvent`, `verifyPostgresDataWithSchema` from `integration_test/database_test.go`). `SubjectMapping` (global in `integration_test/main_test.go`) and `model.UpsertChatPayload` are also used from other packages/files.*

## Test Cases (Methods of `NATSConsumerLifecycleTestSuite`)

### 1. `TestNATSConsumerMissing()`
    -   **Description:** Tests the application's behavior when a NATS consumer for a specific stream (`wa_events_stream`) is missing. This corresponds to test case MQ-04. The test *expects* the message *not* to be processed if the consumer is missing and the service isn't restarted (implying the service doesn't auto-recreate consumers on the fly without a restart for this specific test's assertion).
    -   **Steps:**
        1.  Ensures NATS server is ready.
        2.  Ensures the target stream (`wa_events_stream`) exists (using `createStreamIfNotExists`). The comment in the original code indicates some evolution here, but the intent is to have a stream.
        3.  Deletes all consumers from this stream to simulate a missing consumer scenario.
        4.  Generates a test chat message payload.
        5.  Publishes the message to a NATS subject that routes to the stream.
        6.  Verifies that the message was **not** processed by checking the database (expects count 0). This assertion implies that for this test's scope, simply publishing to a stream without a consumer (and without an app restart in *this specific test*) means the message won't be picked up.

---

# File: integration_test/database_test.go

## Test Suite
*This file does not define any `testify/suite` structures or `Test...` functions. It consists of helper functions for PostgreSQL database operations within the integration testing environment.*

## Helper Functions

### 1. `startPostgres(ctx context.Context, networkName string, network *testcontainers.DockerNetwork) (testcontainers.Container, string, error)`
    -   **Description:** Starts a PostgreSQL testcontainer.
    -   **Details:** Uses `testcontainers-go` to run a `postgres:17-bookworm` image. It configures the database name (`message_service`), user/password, associates it with a given Docker network, and sets up a wait strategy for the database to be ready. It returns the container instance and its connection DSN. If fetching the DSN via the module function fails, it manually constructs the DSN.

### 2. `verifyPostgresData(ctx context.Context, dsn string, query string, expectedResults int) (bool, error)`
    -   **Description:** Connects to a PostgreSQL database using the provided DSN, executes a given query, and verifies if the number of rows returned meets the `expectedResults` count.
    -   **Details:** Opens a SQL connection, pings the database, executes the query, and iterates through rows to count them. Returns `true` if `count >= expectedResults`.

### 3. `verifyPostgresDataWithSchema(ctx context.Context, dsn, tenantSchema, query string, expectedResults int, args ...interface{}) (bool, error)`
    -   **Description:** Similar to `verifyPostgresData`, but sets the `search_path` to a specific `tenantSchema` before executing the query. This allows querying tables within a specific tenant's schema.
    -   **Details:** Connects to the database, sets `search_path` for the session using `SET search_path TO <tenantSchema>`. Then, it executes the provided query with given arguments. It handles `COUNT(*)` queries specifically by scanning the count directly. For other queries, it counts the rows. If `expectedResults` is negative (e.g., -1), it checks for existence (count > 0). Otherwise, it checks if `count == expectedResults`.

### 4. `truncatePostgresTables(ctx context.Context, dsn, schemaName string) error`
    -   **Description:** Truncates a predefined list of tables within a specific schema in the PostgreSQL database.
    -   **Details:** Connects to the database. For each table in `tableNames` (messages, chats, agents, contacts, exhausted_events, onboarding_log), it first checks if the table exists in the given `schemaName` using `tableExists` (defined in `integration_test/postgres_repo_init_test.go`). If it exists, it executes `TRUNCATE TABLE <schema>.<tableName> CASCADE`. Logs warnings if table existence check fails or info if a table doesn't exist.

### 5. `executeQueryWIthSchema(ctx context.Context, dsn, tenantSchema, query string, args ...interface{}) (*sql.Rows, error)`
    -   **Description:** Executes a query within a specific tenant schema and returns the `*sql.Rows`.
    -   **Details:** It modifies the DSN to include `options=-c search_path=<tenantSchema>` to set the schema for the connection. Connects to the database using `connectDB` (defined in `integration_test/postgres_repo_init_test.go` or the suite's own method). Executes the query and returns the rows. The caller is responsible for closing the rows. It notes a potential for leaking DB connections if not managed carefully by the caller.

### 6. `executeQueryWIthSchemaRowScan(ctx context.Context, dsn, tenantSchema, query string, dest []interface{}, args ...interface{}) error`
    -   **Description:** Executes a query expected to return a single row within a specific tenant schema and scans the results into `dest` arguments.
    -   **Details:** Connects to the database using `connectDB` (defined in `integration_test/postgres_repo_init_test.go` or the suite's own method). Sets the `search_path` for the current session. Executes `QueryRowContext` and scans the results.

### 7. `executeNonQuerySQLWithSchema(ctx context.Context, dsn, tenantSchema, query string, args ...interface{}) error`
    -   **Description:** Executes a SQL statement (non-query, e.g., INSERT, UPDATE, DELETE) within a specific tenant schema that doesn't return rows.
    -   **Details:** Connects to the database using `connectDB` (defined in `integration_test/postgres_repo_init_test.go` or the suite's own method). Sets the `search_path` for the current session. Executes the statement using `ExecContext`.

*(Note: The functions `tableExists` and `connectDB` are used by helpers in this file. `tableExists` is defined in `integration_test/postgres_repo_init_test.go`. `connectDB` refers to a local helper in `integration_test/postgres_repo_init_test.go` or the suite's own method, which should be clarified or unified if different.)*

---

# File: integration_test/main_test.go

## Global Variables and Constants
-   `DefaultCompanyID`: Constant string `"defaultcompanyid"`.
-   `PostgresServiceName`: Constant string `"postgres"`.
-   `NatsServiceName`: Constant string `"nats"`.
-   `SubjectToSchemaMap`: A map associating NATS base subjects (e.g., "v1.chats.upsert") to their corresponding JSON schema file names (e.g., "chat.upsert.schema"). Used for payload validation.
-   `SubjectMapping`: A map associating logical event type names (e.g., "chat_created") to their NATS base subject strings (e.g., "v1.chats.upsert"). Used by helper functions to construct full NATS subjects.

## Test Suites

### 1. `BaseIntegrationSuite`
    -   **Description:** Provides the core infrastructure setup for integration tests, including a Docker network, PostgreSQL container, and NATS container. Tests that only require DB and NATS can embed this suite.
    -   **Fields:**
        -   `suite.Suite`: Embedded testify suite.
        -   `Network`: `*testcontainers.DockerNetwork`.
        -   `Postgres`: `testcontainers.Container` for PostgreSQL.
        -   `PostgresDSN`: Connection string for PostgreSQL (host accessible).
        -   `PostgresDSNNetwork`: DSN for PostgreSQL (app container accessible via network alias).
        -   `NATS`: `testcontainers.Container` for NATS.
        -   `NATSURL`: Connection URL for NATS (host accessible).
        -   `NATSURLNetwork`: URL for NATS (app container accessible via network alias).
        -   `CompanyID`: The ID of the company for tenancy testing.
        -   `CompanySchemaName`: The derived PostgreSQL schema name (e.g., "daisi_defaultcompanyid").
        -   `Ctx`: `context.Context` for the suite.
        -   `cancel`: `context.CancelFunc` for the suite's context.
    -   **`SetupSuite()`:**
        1.  Initializes context and logger.
        2.  Determines `CompanyID` (from `TEST_COMPANY_ID` env var or `DefaultCompanyID`).
        3.  Creates a Docker network using `createNetwork()` (defined in this file).
        4.  Starts PostgreSQL container using `startPostgres()` (defined in `integration_test/database_test.go`) and sets up DSNs.
        5.  Starts NATS container using `startNATSContainer()` (defined in `integration_test/nats_test.go`) and sets up URLs.
        6.  Creates the company-specific schema in PostgreSQL (e.g., `CREATE SCHEMA IF NOT EXISTS daisi_defaultcompanyid`).
    -   **`TearDownSuite()`:**
        1.  Terminates NATS container.
        2.  Terminates PostgreSQL container.
        3.  Removes the Docker network.
        4.  Cancels the suite context.
    -   **`SetupTest()`:**
        -   Truncates PostgreSQL tables within the `CompanySchemaName` using `truncatePostgresTables()` (defined in `integration_test/database_test.go`) to ensure a clean state before each test.

### 2. `E2EIntegrationTestSuite`
    -   **Description:** Embeds `BaseIntegrationSuite` and adds the application container to the setup, allowing for end-to-end tests.
    -   **Fields:**
        -   `BaseIntegrationSuite`: Embedded base suite.
        -   `Application`: `testcontainers.Container` for the application being tested.
        -   `AppAPIURL`: Base URL for the application's API (if applicable).
    -   **`SetupSuite()`:**
        1.  Calls `s.BaseIntegrationSuite.SetupSuite()` to set up network, DB, and NATS.
        2.  Waits for a short duration.
        3.  Starts the application container using `setupTestApp()` (defined in `integration_test/app_test.go`), providing necessary environment details.
        4.  Streams logs from the application container.
    -   **`TearDownSuite()`:**
        1.  Terminates the application container.
        2.  Calls `s.BaseIntegrationSuite.TearDownSuite()` to clean up base infrastructure.

## Helper Functions and Methods

### Methods of `BaseIntegrationSuite`:
-   **`connectDB(tenantDSN string) (*sql.DB, error)`:**
    -   Opens and pings a PostgreSQL database connection using the provided DSN.
    -   Intended for internal use by other DB helper methods within the suite.
-   **`VerifyPostgresData(ctx context.Context, query string, expectedResults int, args ...interface{}) (bool, error)`:**
    -   Connects to the tenant's schema and checks if a given SQL query returns the `expectedResults`.
    -   If `expectedResults < 0`, it checks for data existence (count > 0).
    -   Handles `COUNT(*)` queries specifically.
-   **`QueryRowScan(ctx context.Context, query string, dest []interface{}, args ...interface{}) error`:**
    -   Executes a query expected to return a single row in the tenant's schema and scans the result into `dest`.
-   **`ExecuteNonQuery(ctx context.Context, query string, args ...interface{}) error`:**
    -   Executes a SQL statement (e.g., INSERT, UPDATE) that doesn't return rows in the tenant's schema.
-   **`StopService(ctx context.Context, serviceName string) error`:**
    -   Stops a specified service container (`PostgresServiceName` or `NatsServiceName`).
-   **`StartService(ctx context.Context, serviceName string) error`:**
    -   Starts a specified service container and updates its DSN/URL in the suite.
-   **`UpdatePostgresDSN(ctx context.Context) error`:**
    -   Refreshes `s.PostgresDSN` from the PostgreSQL container, typically after a restart.
-   **`UpdateNatsUrl(ctx context.Context) error`:**
    -   Refreshes `s.NATSURL` from the NATS container, typically after a restart.

### Methods of `E2EIntegrationTestSuite`:
-   **`StreamLogs(t *testing.T)`:**
    -   Streams logs from the application container to the test output (`t.Logf`).
-   **`PublishEvent(ctx context.Context, subject string, payload []byte) error`:**
    -   Connects to NATS, validates the payload against a JSON schema (derived from `SubjectToSchemaMap` and the base subject), and publishes the event to the specified NATS subject using JetStream.
-   **`PublishEventWithoutValidation(ctx context.Context, subject string, payload []byte) error`:**
    -   Similar to `PublishEvent` but skips the JSON schema validation step.
-   **`GenerateNatsPayload(subject string, overrides ...interface{}) ([]byte, error)`:**
    -   A sophisticated helper to generate NATS message payloads.
    -   Determines the base NATS subject and payload type from the full subject string using `subjectToPayloadMap` (global in this file).
    -   Extracts `companyID` from the subject if present.
    -   Uses `generatePayloadStruct()` (from `integration_test/fixtures_test.go`) to create a struct instance of the correct payload type, applying default values and then user-provided `overrides`.
    -   Overrides can be a `map[string]interface{}` or a struct.
    -   Handles explicit `nil` values in overrides to ensure they are marshaled as `null` in JSON.
    -   Marshals the struct to JSON.
    -   Validates the generated JSON payload against the schema defined in `SubjectToSchemaMap` (global in this file), using `validatePayload` (from `integration_test/helper_test.go`).

### Standalone Helper Functions:
-   **`getMapKeys(m map[string]string) []string`:**
    -   Returns a slice of keys from the input map. Used for error reporting in `GenerateNatsPayload`.
-   **`createNetwork(ctx context.Context) (*testcontainers.DockerNetwork, error)`:**
    -   Creates a new attachable Docker network using `testcontainers-go`.
-   **`changeUriFromLocalhostToDocker(uri, networkAlias string) string`:**
    -   Converts a localhost URI (e.g., from `container.MappedPort()`) to a URI that uses the Docker network alias and default service port. This is for inter-container communication.
    -   Handles "postgres" and "nats" URI schemes.
-   **`replacePort(uri, newPort string) string`:**
    -   A utility function to replace the port number in a URI string.

### Structs:
-   **`TestEnvironment` struct:**
    -   Holds DSNs, URLs, and network information. Primarily used internally by `setupTestApp`.

## Test Runner Functions
-   **`TestRunE2ESuite(t *testing.T)`:**
    -   The main entry point to run all tests defined within `E2EIntegrationTestSuite`.

*(Note: `setupTestApp` is defined in `integration_test/app_test.go`, `startPostgres` in `integration_test/database_test.go`, `startNATSContainer` in `integration_test/nats_test.go`, `validatePayload` in `integration_test/helper_test.go`, and `subjectToPayloadMap`/`generatePayloadStruct` are from `integration_test/fixtures_test.go`.)*

---

# File: integration_test/recovery_resilience_test.go

## Helper Functions (Top-Level)

### 1. `WaitUntilDBReady(ctx context.Context, dsn string) error`
    -   **Description:** Waits for the PostgreSQL database at the given DSN to become connectable and responsive to pings.
    -   **Details:** It uses a ticker (2 seconds) and a timeout (30 seconds). In each tick, it attempts to open a DB connection and ping it. Returns `nil` on successful ping, or an error if timed out or connection fails. (Note: The code uses `sql.Open("pgx", dsn)` which is a valid way to use the `pgx` driver with the `database/sql` package).

## Test Suites

### 1. `MainRecoveryResilienceTestSuite`
    -   **Description:** A suite for general recovery and resilience tests of the service. Embeds `E2EIntegrationTestSuite`.
    -   **Setup Function (Per-Test):** `SetupTest()`:
        -   Calls the base `E2EIntegrationTestSuite.SetupTest()` (DB truncation).
        -   Initializes a `zaptest` logger named "MainRecoveryResilienceTestSuite".
        -   Includes a 1-second delay.
    -   **Test Cases:**
        -   **`TestServiceRestartDuringProcessing()` (Test ID: RC-01)**
            -   **Description:** Tests if the service recovers and continues processing a large batch of messages after an unexpected restart.
            -   **Steps:**
                1.  Publishes a large batch (50) of chat history messages.
                2.  Waits briefly, then stops and restarts the application container.
                3.  Waits for application recovery.
                4.  Verifies that a significant portion (at least 75%) of the batch messages were processed.
                5.  Publishes a new single message and verifies it's processed to confirm normal operation.
        -   **`TestTransactionRollback()` (Test ID: RC-04)**
            -   **Description:** Tests if the service correctly handles database transaction failures by rolling back changes cleanly.
            -   **Steps:**
                1.  Successfully publishes and verifies a chat message.
                2.  Connects to the DB and alters the `messages` table to add a `CHECK` constraint that will cause an INSERT to fail if `status` is 'will_fail'.
                3.  Publishes a message with `status: "will_fail"` (using `PublishEventWithoutValidation` as it would fail schema validation due to the status).
                4.  Verifies that this "failing" message was *not* stored in the `messages` table.
                5.  Verifies that the original chat message (from step 1) still exists (i.e., was not rolled back).
                6.  Removes the temporary `CHECK` constraint.
                7.  Publishes a new valid message and verifies it's stored successfully.
        -   **`TestJetStreamDeliveryRetry()` (Test ID: RC-02)**
            -   **Description:** Tests JetStream's message redelivery mechanism when the service (or its dependency like DB) is temporarily unavailable.
            -   **Steps:**
                1.  Generates a chat payload.
                2.  Stops the PostgreSQL container.
                3.  Publishes the chat message (expected to be queued by JetStream as the service can't process it without DB).
                4.  Restarts the PostgreSQL container and waits for it to be ready.
                5.  Waits for JetStream to redeliver the message and the service to process it.
                6.  Uses `s.Require().Eventually` to repeatedly check (for up to 60 seconds) if the message was processed and stored in the database.

### 2. `PoisonMessageHandlingSuite`
    -   **Description:** A suite specifically for testing how the service handles "poison messages" (messages that persistently fail processing), expecting them to be moved to a Dead Letter Queue (DLQ) or similar mechanism. Embeds `E2EIntegrationTestSuite`.
    -   **Setup Function (Per-Test):** `SetupTest()`:
        -   Calls base setup and initializes a logger named "PoisonMessageHandlingSuite".
    -   **`BeforeTest(suiteName, testName string)`:**
        -   This hook runs before the `TestPoisonMessageHandling` test.
        -   It terminates the existing application container.
        -   Restarts the application container with specific environment variable overrides to configure short DLQ retry/delay parameters (`NATS_DLQBASEDELAYMINUTES`, `NATS_DLQMAXDELAYMINUTES`, `NATS_DLQMAXDELIVER`, `NATS_DLQACKWAIT`) for faster testing of DLQ behavior.
        -   Sets up a `t.Cleanup` function to restore the application container to its default environment after the test.
    -   **Test Cases:**
        -   **`TestPoisonMessageHandling()` (Test ID: RC-03)**
            -   **Description:** Tests the service's ability to isolate a poison message and continue processing valid messages, and that the poison message ends up in the `exhausted_events` table (acting as a DLQ).
            -   **Steps:**
                1.  Defines a "poison" chat payload with deliberately invalid data types/values that would cause processing errors (e.g., a channel for a string field, non-timestamp for timestamp field).
                2.  Attempts to marshal this payload, expecting an error (this part tests the test's own setup of a bad payload).
                3.  Creates a *second* poison payload (`poisonPayloadValidJSON`) that is valid JSON but contains data likely to be rejected by application logic or schema validation (e.g., very large timestamp, negative unread count, but these might still pass basic JSON schema if not constrained). This is the one actually published.
                4.  Publishes this `poisonJSONValid` message using `PublishEventWithoutValidation`.
                5.  Publishes a subsequent "normal" valid chat message.
                6.  Waits, then verifies that the poison message was *not* processed/stored in the `chats` table.
                7.  Verifies that the normal message *was* processed successfully.
                8.  Uses `s.Require().Eventually` to check if an entry for the poison message appears in the `exhausted_events` table (for up to 150 seconds).
                9.  Fetches the details of the DLQ entry from `exhausted_events` and asserts various fields: `company_id`, `source_subject`, `last_error` (should be non-empty), `retry_count` (should be >0), `resolved` (should be false), and that `dlq_payload` matches the `poisonJSONValid`.

## Test Runner Functions (Global)
-   **`TestAllRecoveryAndResilienceScenarios(t *testing.T)`:**
    -   A test runner function that executes the `MainRecoveryResilienceTestSuite`.
    -   It comments out running `PoisonMessageHandlingSuite`, noting it's not applicable for this combined runner "because long wait," likely due to the `Eventually` block in `TestPoisonMessageHandling`.

*(Note: Helper functions like `executeQueryWIthSchemaRowScan` (from `integration_test/database_test.go`), `connectDB` (suite's own or from `integration_test/postgres_repo_init_test.go`), `verifyPostgresDataWithSchema` (from `integration_test/database_test.go`), and methods from `E2EIntegrationTestSuite` like `s.PublishEvent`, `s.GenerateNatsPayload`, `s.StopService`, `s.StartService`, `s.StreamLogs` are used throughout.)*

---

# File: integration_test/app_test.go

## Test Suite
*This file does not define any `testify/suite` structures or `Test...` functions. It contains a helper function for setting up the application under test within a Docker container.*

## Helper Functions

### 1. `setupTestApp(ctx context.Context, networkName string, env *TestEnvironment, envOverrides map[string]string) (testcontainers.Container, string, error)`
    -   **Description:** Creates and starts a new Docker container instance of the main application service for integration testing.
    -   **Parameters:**
        -   `ctx`: Context for the operation.
        -   `networkName`: The name of the Docker network to connect the app container to.
        -   `env`: A `*TestEnvironment` struct containing base DSNs, URLs, and company ID for the app.
        -   `envOverrides`: A map of environment variables to override or add to the base environment for the app container.
    -   **Details:**
        1.  Determines the project root directory (using `getProjectRoot()`, defined in `integration_test/helper_test.go`).
        2.  Defines a base set of environment variables for the application container:
            -   `POSTGRES_DSN`: Set from `env.PostgresDSNNetwork`.
            -   `NATS_URL`: Set from `env.NATSURLNetwork`.
            -   `COMPANY_ID`: Set from `env.CompanyID`.
            -   `LOG_LEVEL`: "debug".
            -   `SERVER_PORT`: "8080".
            -   `ENVIRONMENT`: "test".
        3.  Merges any provided `envOverrides` into the base environment variables, with overrides taking precedence.
        4.  Configures a `testcontainers.ContainerRequest`:
            -   Builds from the `Dockerfile` located at the project root.
            -   Attaches to the specified `networkName`.
            -   Sets a network alias "message-service" for the app container within that network.
            -   Exposes port "8080/tcp".
            -   Passes the final merged environment variables.
            -   Sets a `WaitingFor` strategy: `wait.ForLog("Both consumers started successfully")`, indicating the app is ready when this log message appears.
        5.  Starts the application container as a generic container.
        6.  Retrieves the host and mapped port (for "8080") of the started application container.
        7.  Constructs and returns the application container instance, its accessible API address (e.g., `http://localhost:mappedPort`), and any error.

*(Note: `getProjectRoot()` is defined in `integration_test/helper_test.go`. The `TestEnvironment` struct is defined in `integration_test/main_test.go`.)*

---

# File: integration_test/semantic_validation_test.go

## Test Suite

### `SemanticValidationTestSuite`
-   **Description:** Defines a suite for testing semantic data validation by the service. This includes checks beyond basic schema validation, such as logical consistency of data (e.g., valid timestamps, existing references). It embeds `E2EIntegrationTestSuite` as it needs the application container running.
-   **Setup Function (Global):** `TestSemanticValidationSuite(t *testing.T)` runs the suite.
-   **Setup Function (Per-Test):** `SetupTest()`:
    -   Calls the base `E2EIntegrationTestSuite.SetupTest()` (which includes DB truncation).
    -   Initializes a `zaptest` logger named "SemanticValidationTestSuite".
    -   Includes a 1-second delay.

## Helper Functions
*This file primarily uses helper functions from the embedded `E2EIntegrationTestSuite` (defined in `integration_test/main_test.go`) (e.g., `s.StreamLogs`, `s.PublishEventWithoutValidation`) and shared utilities like `verifyPostgresDataWithSchema` (defined in `integration_test/database_test.go`). `SubjectMapping` (global in `integration_test/main_test.go`) is also used.*

## Test Cases (Methods of `SemanticValidationTestSuite`)

### 1. `TestInvalidTimestampFormat()` (Test ID: SM-01)
    -   **Description:** Tests how the service handles NATS events containing timestamps in an invalid format (e.g., a non-numeric string where a Unix timestamp is expected).
    -   **Steps:**
        1.  Streams application logs.
        2.  Creates a chat upsert payload as a `map[string]interface{}` to manually set an invalid `conversationTimestamp` (e.g., "not-a-unix-timestamp").
        3.  Marshals this map to JSON.
        4.  Publishes this payload using `PublishEventWithoutValidation` (as it would likely fail schema validation if one was strictly enforcing numeric timestamps, though the test implies NATS itself accepts any bytes).
        5.  Waits for processing.
        6.  Verifies that the chat message was **not** stored in the PostgreSQL database, asserting that the service rejected the semantically invalid payload.

### 2. `TestFutureTimestamp()` (Test ID: SM-02)
    -   **Description:** Tests how the service handles events with timestamps that are unreasonably far in the future.
    -   **Steps:**
        1.  Streams application logs.
        2.  Calculates a timestamp that is one year in the future.
        3.  Creates a chat upsert payload as a `map[string]interface{}` with this future `conversationTimestamp`.
        4.  Marshals this map to JSON.
        5.  Publishes the payload using `PublishEventWithoutValidation`.
        6.  Waits for processing.
        7.  Verifies that the chat message was **not** stored. The test notes that rejection is the stricter and expected behavior, but logs a warning if the service *does* store it (implying processing with a warning might be acceptable in some designs).

### 3. `TestInvalidReferences()` (Test ID: SM-04)
    -   **Description:** Tests how the service handles messages that reference non-existent parent entities (e.g., a message event for a `chatID` that does not exist in the `chats` table).
    -   **Steps:**
        1.  Streams application logs.
        2.  Generates a unique `nonExistentChatID`.
        3.  Verifies that this chat ID indeed does not exist in the database.
        4.  Creates a message upsert payload as a `map[string]interface{}` that references this `nonExistentChatID`.
        5.  Marshals this map to JSON.
        6.  Publishes the payload using `PublishEventWithoutValidation`.
        7.  Waits for processing.
        8.  Verifies that the message was **not** stored in the `messages` table.
        9.  Re-verifies that the `nonExistentChatID` was **not** auto-created in the `chats` table.
        10. Logs a warning if the message *was* stored, as rejection is preferred.

---

# File: integration_test/tenant_isolation_test.go

## Test Suite

### `TenantIsolationTestSuite`
-   **Description:** Defines a suite for testing tenant isolation capabilities of the service. This ensures that data for one tenant is not accessible or modifiable by another, and that events with invalid or missing tenant information are handled correctly. It embeds `E2EIntegrationTestSuite`.
-   **Setup Function (Global):** `TestTenantIsolationSuite(t *testing.T)` runs the suite.
-   **Setup Function (Per-Test):** `SetupTest()`:
    -   Calls the base `E2EIntegrationTestSuite.SetupTest()` (DB truncation).
    -   Initializes a `zaptest` logger named "TenantIsolationTestSuite".
    -   Includes a 1-second delay.

## Helper Functions
*This file uses helper functions from the embedded `E2EIntegrationTestSuite` (defined in `integration_test/main_test.go`) (e.g., `s.StreamLogs`, `s.GenerateNatsPayload`, `s.PublishEvent`) and shared utilities like `verifyPostgresDataWithSchema`. `SubjectMapping`, `DefaultCompanyID`, and `model.UpsertChatPayload`/`model.UpsertMessagePayload` are also used from other parts of the test package or internal models.*

## Test Cases (Methods of `TenantIsolationTestSuite`)

### 1. `TestCrossTenantDataAccess()` (Test ID: TI-01)
    -   **Description:** Tests that the service prevents data from one tenant (companyID1) being affected by an operation intended for, or accidentally specifying, another tenant (companyID2).
    -   **Steps:**
        1.  Defines two tenant IDs: `companyID1` (from the suite) and a hardcoded `companyID2` ("tenant_two").
        2.  Creates a chat and a message within that chat for `companyID1`. Payloads are generated as `map[string]interface{}`.
        3.  Verifies that both the chat and message exist in `companyID1`'s schema.
        4.  Attempts to update the chat (created for `companyID1`) by publishing a chat update event. The NATS subject targets `companyID1`'s stream/subject, but the `company_id` field *within the payload* is set to `companyID2`.
        5.  Verifies that the original chat data in `companyID1`'s schema (specifically `unread_count`) remains unchanged, demonstrating that the cross-tenant update attempt from the payload was ignored or rejected, thus preserving tenant isolation.

### 2. `TestMissingTenantID()` (Test ID: TI-02)
    -   **Description:** Tests that the service rejects events where the `company_id` (tenant ID) is missing from the payload.
    -   **Steps:**
        1.  Generates a chat upsert payload (`model.UpsertChatPayload`) where `CompanyID` is omitted (or effectively empty). The NATS subject uses `DefaultCompanyID` for routing purposes.
        2.  Publishes this payload.
        3.  Verifies that the chat was **not** stored in the database schema corresponding to `DefaultCompanyID`.
        4.  Repeats the process for a message upsert payload, also omitting `CompanyID`.
        5.  Verifies that this message was also **not** stored.

### 3. `TestInvalidTenantID()` (Test ID: TI-03)
    -   **Description:** Tests that the service rejects events where the `company_id` in the payload has an invalid format (e.g., empty, whitespace, contains special characters that would be invalid for schema names).
    -   **Steps:**
        1.  Defines a list of `invalidTenantIDs` (e.g., "", " ", "tenant*invalid", "tenant with spaces", "12345").
        2.  Iterates through each `invalidTenantID`, running a subtest (`t.Run`):
            a.  Generates a chat upsert payload (`model.UpsertChatPayload`) with the current `invalidTenantID`. The NATS subject uses `DefaultCompanyID` for routing.
            b.  Publishes this payload.
            c.  Verifies that the chat was **not** stored in the database (checks against the `DefaultCompanyID` schema as a proxy, assuming an invalid tenant ID wouldn't lead to successful storage anywhere).
            d.  Repeats this for a message upsert payload using the same `invalidTenantID`.
            e.  Verifies the message was also **not** stored.

---

# File: integration_test/invalid_input_test.go

## Test Suite

### `InvalidInputTestSuite`
-   **Description:** Defines a suite for testing how the service handles various types of invalid input, including malformed JSON, missing fields, incorrect data types, oversized payloads, and schema violations. It embeds `E2EIntegrationTestSuite`.
-   **Setup Function (Global):** `TestInvalidInputSuite(t *testing.T)` runs the suite.
-   **Setup Function (Per-Test):** `SetupTest()`:
    -   Calls the base `E2EIntegrationTestSuite.SetupTest()` (DB truncation).
    -   Initializes a `zaptest` logger named "InvalidInputTestSuite".
    -   Includes a 1-second delay.

## Helper Functions

### 1. `generateLargeString(size int) string`
    -   **Description:** Generates a string of a specified `size` consisting of repeating uppercase letters (A-Z).
    -   **Used By:** `TestOversizedPayload()`

### 2. `countPostgresRows(ctx context.Context, dsn string, query string, args ...interface{}) (int, error)`
    -   **Description:** Connects to PostgreSQL using `connectDB` (from `BaseIntegrationSuite` in `integration_test/main_test.go` or a local helper like in `integration_test/postgres_repo_init_test.go`), executes a given SQL query (expected to be a `COUNT(*)` or similar query returning a single integer), and scans the result into an `int`.
    -   **Details:** Handles `sql.ErrNoRows` by returning 0 (as no rows means count is zero).
    -   *(Note: This helper is useful. `verifyPostgresDataWithSchema` (from `integration_test/database_test.go`) is used more commonly for verifications that involve schema context and specific counts, while this is a more direct count query.)*

## Test Cases (Methods of `InvalidInputTestSuite`)

### 1. `TestMalformedJSON()` (Test ID: IV-01)
    -   **Description:** Tests service handling of payloads that are not valid JSON.
    -   **Steps:**
        1.  Constructs a byte slice containing deliberately malformed JSON (syntax error).
        2.  Verifies the target chat does not exist in the DB initially.
        3.  Publishes this malformed payload using `PublishEventWithoutValidation`.
        4.  Waits for processing.
        5.  Verifies that the chat was **not** stored, implying the service rejected the malformed JSON. Logs are expected to show details.

### 2. `TestMissingRequiredFields()` (Test ID: IV-02)
    -   **Description:** Tests service handling of payloads that are valid JSON but are missing fields required by the application's logic or schema (e.g., missing `chat_id`).
    -   **Steps:**
        1.  Creates a chat upsert payload as `map[string]interface{}` where `chat_id` is omitted.
        2.  Marshals to JSON.
        3.  Publishes using `PublishEventWithoutValidation`.
        4.  Waits. Asserts that the service should reject this and relies on logs for error details (no DB check for non-storage is explicitly performed in this test for the missing `chat_id` itself, but the expectation is rejection).

### 3. `TestInvalidFieldTypes()` (Test ID: IV-03)
    -   **Description:** Tests service handling of payloads with fields of incorrect data types (e.g., string where number is expected).
    -   **Steps:**
        1.  Creates a chat upsert payload as `map[string]interface{}` with `conversationTimestamp` as a string ("not-a-timestamp"), `unread_count` as a string ("two"), and `is_group` as a string ("maybe").
        2.  Marshals to JSON.
        3.  Publishes using `PublishEventWithoutValidation`.
        4.  Waits.
        5.  Verifies that the chat was **not** stored in the database.

### 4. `TestOversizedPayload()` (Test ID: IV-04)
    -   **Description:** Tests service (and NATS) handling of payloads that exceed size limits.
    -   **Steps:**
        1.  Generates a very large string (e.g., 2MB) using `generateLargeString()`.
        2.  Creates a chat upsert payload as `map[string]interface{}` including this large string in a field.
        3.  Marshals to JSON.
        4.  Attempts to publish using `PublishEventWithoutValidation`.
        5.  Asserts that the publish operation itself might fail with a NATS error related to "maximum payload exceeded" or "message too large".
        6.  If NATS *does* accept it (logged as a warning in the test), the test still expects the service to reject it.
        7.  Verifies that the chat was **not** stored in the database, regardless of NATS's behavior.

### 5. `TestEmptyPayload()` (Test ID: IV-05)
    -   **Description:** Tests service handling of empty (zero-byte) payloads.
    -   **Steps:**
        1.  Creates an empty byte slice `[]byte{}`.
        2.  Publishes this empty payload using `PublishEventWithoutValidation`.
        3.  Waits. Expects rejection by the service (validation/unmarshalling error) and relies on logs for details.

### 6. `TestInvalidChatSchema()` (Test ID: SV-01)
    -   **Description:** Tests service handling of chat upsert payloads that violate the defined JSON schema (e.g., wrong data types for fields, extra fields not in schema).
    -   **Steps:**
        1.  Creates a chat upsert payload as `map[string]interface{}` with multiple schema violations: `jid` as int, `push_name` as map, `conversation_timestamp` as string "tomorrow", `unread_count` as -10, `is_group` as string "yes", and an `additional_field`.
        2.  Marshals to JSON.
        3.  Publishes using `PublishEventWithoutValidation`.
        4.  Waits.
        5.  Verifies that the chat was **not** stored.

### 7. `TestInvalidMessageSchema()` (Test ID: SV-02)
    -   **Description:** Tests service handling of message upsert payloads that violate the JSON schema.
    -   **Steps:**
        1.  Creates a message upsert payload as `map[string]interface{}` with schema violations: `message_obj` as string, `message_timestamp` as negative int, `status` as int, `flow` as "sideways", and an `extra_data` field.
        2.  Marshals to JSON.
        3.  Publishes using `PublishEventWithoutValidation`.
        4.  Waits.
        5.  Verifies that the message was **not** stored.

### 8. `TestInvalidHistorySchema()` (Test ID: SV-03)
    -   **Description:** Tests service handling of chat history payloads that violate the schema (e.g., `chats` field is a string instead of an array of chat objects).
    -   **Steps:**
        1.  Creates a history payload map where `chats` is a string.
        2.  Attempts to marshal this. The test logic here is a bit different: it comments that `s.GenerateNatsPayload` (which does internal validation) *should* fail for this. If it does, it asserts the error. If `GenerateNatsPayload` *doesn't* error (logged as WARN), it proceeds to publish the marshaled map using `PublishEventWithoutValidation`.
        3.  Regardless of how it was published (or if publish was skipped due to generation error), it verifies that no chats were created/updated, expecting the invalid history payload to be rejected.

### 9. `TestSchemaVersionMismatch()` (Test ID: SV-04)
    -   **Description:** Tests service handling of payloads sent to an unsupported or non-existent NATS subject version (e.g., "v999.chats.upsert...").
    -   **Steps:**
        1.  Creates a valid-looking chat upsert payload map.
        2.  Marshals to JSON.
        3.  Publishes this payload to an unsupported subject like `v999.chats.upsert.<tenantID>` using `PublishEventWithoutValidation`.
        4.  Notes that NATS publish might succeed (if stream allows any subject) or fail (if stream is strict). The test logs the outcome but doesn't fail on publish error itself.
        5.  Waits.
        6.  Verifies that the chat was **not** stored, as the service's v1 handlers should not process messages from a v999 subject.

---

# File: integration_test/partial_data_test.go

## Test Suite

### `PartialDataTestSuite`
-   **Description:** Defines a suite for testing how the service handles partial data in NATS events. This includes partial updates (where only some fields are provided for an existing record) and payloads with intentionally empty fields. It embeds `E2EIntegrationTestSuite`.
-   **Setup Function (Global):** `TestPartialDataSuite(t *testing.T)` runs the suite.
-   **Setup Function (Per-Test):** `SetupTest()`:
    -   Calls the base `E2EIntegrationTestSuite.SetupTest()` (DB truncation).
    -   Initializes a `zaptest` logger named "PartialDataTestSuite".
    -   Includes a 1-second delay.

## Helper Functions
*This file primarily uses helper functions from the embedded `E2EIntegrationTestSuite` (defined in `integration_test/main_test.go`) (e.g., `s.GenerateNatsPayload`, `s.PublishEvent`) and shared utilities like `verifyPostgresDataWithSchema`, `executeNonQuerySQLWithSchema`, `executeQueryWIthSchemaRowScan` (all from `integration_test/database_test.go`). The `generateUUID()` function (from `integration_test/helper_test.go`) and `SubjectMapping` (global in `integration_test/main_test.go`) are also used.*

## Test Cases (Methods of `PartialDataTestSuite`)

### 1. `TestPartialChatUpdate()` (Test ID: PD-01)
    -   **Description:** Tests if the service correctly applies partial updates to an existing chat record. Only a subset of fields are sent in the update payload, and the service should update these fields while preserving the others.
    -   **Steps:**
        1.  Creates and publishes an initial, complete chat payload (`model.UpsertChatPayload`) with specific values for `push_name`, `unread_count`, etc.
        2.  Verifies this initial chat is stored correctly.
        3.  Creates a partial update payload as `map[string]interface{}` containing only `id` (matching `chatID`), `company_id`, `unread_count` (new value), and `conversation_timestamp`.
        4.  Publishes this partial update payload to the chat update subject.
        5.  Verifies that in the database:
            -   The `unread_count` is updated to the new value (e.g., 1 in the test, but the DB check looks for 11, which seems like a typo in the test's verification query if the payload sent 1. *Self-correction: The verification query `unread_count = 11` is likely a typo and should probably be `unread_count = 1` based on the payload. The test's intent is clear: updated fields change, others are preserved.*).
            -   Other fields from the initial creation (`push_name`, `is_group`) are preserved and have their original values.

### 2. `TestPartialMessageUpdate()` (Test ID: PD-02)
    -   **Description:** Tests if the service correctly applies partial updates to an existing message record.
    -   **Steps:**
        1.  Creates a prerequisite chat.
        2.  Creates and publishes an initial, complete message payload (`model.UpsertMessagePayload`) with a specific `status` ("sent") and `message_obj.conversation` content.
        3.  Verifies this initial message is stored correctly.
        4.  Creates a partial message update payload as `map[string]interface{}` containing only `id` (matching `messageID`), `company_id`, and `status` (new value: "read").
        5.  Publishes this partial update payload to the message update subject.
        6.  Verifies that in the database:
            -   The `status` is updated to "read".
            -   The original `message_obj.conversation` content is preserved.

### 3. `TestEmptyFields()` (Test ID: PD-03)
    -   **Description:** Tests how the service handles payloads where optional string fields are provided as empty strings (e.g., `push_name: ""`).
    -   **Steps:**
        1.  Creates a chat upsert payload as `map[string]interface{}` where `push_name` and `group_name` are explicitly set to `""`.
        2.  Publishes this payload.
        3.  Verifies that the chat was stored and that `push_name` and `group_name` in the database are either empty strings or NULL (the query `(push_name = '' OR push_name IS NULL)` accommodates both).
        4.  Creates a message upsert payload as `map[string]interface{}` where `message_obj.conversation` is `""`.
        5.  Publishes this payload.
        6.  Verifies that the message was stored and `message_obj.conversation` is either an empty string or NULL in the database.

### 4. `TestIncompleteBatch()` (Test ID: PD-05)
    -   **Description:** Tests how the service handles a batch of chat history events where some entries in the batch are valid and others are invalid (e.g., missing required fields). The expectation in this test appears to be that if *any* part of the batch is invalid, *none* of the chats (even the valid ones in that batch) should be processed.
    -   **Steps:**
        1.  Manually constructs a JSON string for a chat history payload. This batch contains:
            -   One valid chat object (`validChat1ID`).
            -   One invalid chat object (`"invalid-missing-required-fields"`, missing `jid`).
            -   Another valid chat object (`validChat2ID`).
        2.  Publishes this manually constructed batch JSON using `PublishEventWithoutValidation`.
        3.  Streams application logs.
        4.  Verifies that **none** of the chats from the batch were stored in the database:
            -   Checks that `validChat1ID` was **not** stored.
            -   Checks that `validChat2ID` was **not** stored.
            -   Checks that the chat with ID `"invalid-missing-required-fields"` was **not** stored.
        *(Note: The assertion is that if the batch processing encounters an error with one item, the entire batch or at least subsequent valid items might be skipped or the transaction rolled back, leading to none of them being persisted. The test asserts that the valid items are *not* processed).*

---

# File: integration_test/data_integrity_test.go

## Test Suite

### `DataIntegrityTestSuite`
-   **Description:** Defines a suite for testing data integrity aspects of the service. This includes handling duplicate records, conflicting updates, orphaned references, and general data consistency across operations. It embeds `E2EIntegrationTestSuite`.
-   **Setup Function (Global):** `TestDataIntegritySuite(t *testing.T)` runs the suite.
-   **Setup Function (Per-Test):** `SetupTest()`:
    -   Initializes a `zaptest` logger named "DataIntegrityTestSuite".
    -   Calls the base `E2EIntegrationTestSuite.SetupTest()` (DB truncation).
    -   Includes a 1-second delay.

## Helper Functions
*This file primarily uses helper functions from the embedded `E2EIntegrationTestSuite` (defined in `integration_test/main_test.go`) (e.g., `s.GenerateNatsPayload`, `s.PublishEvent`) and shared utilities like `verifyPostgresDataWithSchema`, `executeNonQuerySQLWithSchema`, `executeQueryWIthSchemaRowScan`. The `generateUUID()` function (not defined in snippets, assumed to be a utility) and `SubjectMapping` are also used.*

## Test Cases (Methods of `DataIntegrityTestSuite`)

### 1. `TestDuplicateMessageIDs()` (Test ID: DI-01)
    -   **Description:** Tests how the service handles duplicate message IDs, expecting an upsert-like behavior where subsequent messages with the same ID update the existing record.
    -   **Steps:**
        1.  Creates a prerequisite chat.
        2.  Publishes an initial message with a specific `messageID` and content ("Original message content").
        3.  Verifies the message is stored.
        4.  Publishes a "duplicate" message using the same `messageID` but different content ("Duplicate message content") and a newer timestamp, to the *same upsert subject*.
        5.  Verifies that only one message record exists for `messageID`.
        6.  Verifies that the message content stored is now "Duplicate message content" (assuming last write wins or newer timestamp wins).
        7.  Publishes another update for the same `messageID`, this time to the *update subject* (e.g., `v1.messages.update...`), changing only the `status` to "delivered".
        8.  Verifies only one message record still exists.
        9.  Verifies the status is "delivered" and the content ("Duplicate message content") is preserved.

### 2. `TestConflictingUpdates()` (Test ID: DI-04)
    -   **Description:** Tests how the service handles multiple, potentially out-of-order updates to the same record, expecting the update with the latest `conversation_timestamp` to prevail.
    -   **Steps:**
        1.  Creates an initial chat.
        2.  Prepares three update payloads for this chat:
            -   `firstUpdate`: `unread_count: 1`, timestamp `T1`.
            -   `secondUpdate`: `unread_count: 0` (test comment says 2, payload says 0, verification query looks for 2, which implies the text description/verification is the source of truth for intent: `unread_count` becomes 2), timestamp `T2` (T1 + 50ms). This is the intended final state.
            -   `thirdUpdate`: `unread_count: 1`, timestamp `T3` (T2 - 10ms, so T1 < T3 < T2).
        3.  Publishes these updates in the order: `firstUpdate`, then `thirdUpdate`, then `secondUpdate` (simulating out-of-order arrival but with `secondUpdate` having the latest timestamp).
        4.  Waits for processing.
        5.  Verifies that the chat's final state (`unread_count` and `conversation_timestamp`) matches that of `secondUpdate` (the one with the latest timestamp). The verification query checks for `unread_count = 2` and timestamp of `secondUpdate`.

### 3. `TestOrphanedReferences()` (Test ID: DI-04, duplicate ID, likely meant DI-02 or similar)
    -   **Description:** Tests two scenarios:
        1.  Creating a message that references a chat ID that does not exist.
        2.  Creating a message linked to an existing chat, then deleting the chat directly from the DB, and observing the message's state.
    -   **Steps (Scenario 1 - Message with non-existent chat ref):**
        1.  Publishes a message payload where `chat_id` refers to a non-existent chat.
        2.  Verifies the message *is* stored (implying the service handles this gracefully, perhaps allowing foreign key to be null or not enforcing it strictly at this stage for messages).
        3.  Optionally logs if the non-existent chat was auto-created (behavior depends on application logic).
    -   **Steps (Scenario 2 - Deleting parent chat):**
        1.  Creates a chat (`chatToDeleteID`).
        2.  Creates a message (`messageToOrphanID`) linked to `chatToDeleteID`.
        3.  Verifies both exist.
        4.  Deletes `chatToDeleteID` directly from the database using a SQL query.
        5.  Verifies the chat is gone.
        6.  Queries the message (`messageToOrphanID`) and logs its `chat_id` (which would be `NULL` if `ON DELETE SET NULL` is used, or the message would be gone if `ON DELETE CASCADE`, or the delete would have failed if `RESTRICT`). The test logs the state for observation.

### 4. `TestDataConsistency()` (Test ID: DI-05)
    -   **Description:** Tests general data consistency after a sequence of create and update operations for both chats and messages.
    -   **Steps:**
        1.  Creates a chat.
        2.  Creates a message linked to this chat.
        3.  Updates the chat (e.g., `unread_count` to 1).
        4.  Updates the message (e.g., `status` to "read").
        5.  Verifies that the chat in PostgreSQL has `unread_count = 1`.
        6.  Verifies that the message in PostgreSQL has `status = 'read'`.
        7.  Makes a final update to the message (e.g., `status` to "delivered").
        8.  Verifies this final message state in PostgreSQL.

### 5. `TestMismatchedTenantData()` (Test ID: DI-03)
    -   **Description:** Tests scenarios where the `company_id` in the NATS subject might differ from the `company_id` in the message payload, to ensure data isn't processed for the wrong tenant or cross-contaminates schemas.
    -   **Steps (Scenario 1):**
        1.  Defines `primaryTenantID` (from suite) and `secondaryTenantID`.
        2.  Creates a chat payload where `company_id` field is `secondaryTenantID`.
        3.  Publishes this payload to a NATS subject belonging to `primaryTenantID`.
        4.  Verifies that the data is **not** stored in `primaryTenantID`'s schema (neither under `secondaryTenantID` nor `primaryTenantID` within that schema). The focus is on non-contamination of the primary schema.
    -   **Steps (Scenario 2):**
        1.  Creates a chat payload where `company_id` field is `primaryTenantID`.
        2.  Publishes this payload to a NATS subject belonging to `secondaryTenantID`.
        3.  Verifies that the data is **not** stored in `primaryTenantID`'s schema, assuming consumer routing is strict by subject or the service correctly routes to the (hypothetical) secondary tenant's data store.

---

# File: integration_test/fixtures_test.go

## Structs

### 1. `TestPayloads`
    -   **Description:** A struct designed to hold pre-generated and marshaled JSON byte slices for various event types (Chat Upsert/Update/History, Message Upsert/Update/History, etc.).
    -   **Note:** The comment indicates this might be refactored, and payloads might be generated directly in tests instead. The `generateTestPayloads` function using this struct is also largely a placeholder.

## Helper Functions

### 1. `generateTestPayloads() (*TestPayloads, error)`
    -   **Description:** Intended to use model factories (from `internal/model/factories.go`, though these factories like `model.NewChat` are directly used in `generateModelStruct`) to create test data and marshal them into the `TestPayloads` struct.
    -   **Status:** Mostly a placeholder, as the main payload generation logic has shifted to `generateModelStruct` and `generatePayloadStruct` which are then used by `E2EIntegrationTestSuite.GenerateNatsPayload`.

### 2. `generateModelStruct(modelName string, overrides ...interface{}) (interface{}, error)`
    -   **Description:** Creates an instance of a base GORM model struct (e.g., `model.Chat`, `model.Message`) using its respective factory function (e.g., `model.NewChat()`).
    -   **Parameters:**
        -   `modelName`: A string representing the model to create (e.g., "Chat", "Message").
        -   `overrides`: An optional argument. If provided, it's expected to be a pointer to a struct of the same model type (e.g., `*model.Chat`) with specific fields set. These fields will override the defaults from the factory.
    -   **Details:** Uses a switch statement on `modelName` to call the appropriate factory (e.g., `model.NewAgent(agentOverride)`).

### 3. `unmarshalJSONDataHookFunc() mapstructure.DecodeHookFunc`
    -   **Description:** A `mapstructure.DecodeHookFunc` used during the mapping from base model structs to NATS payload structs (in `generatePayloadStruct`).
    -   **Details:** This hook is designed to handle fields that might be `[]byte` (like `datatypes.JSON` from GORM models) in the source model and need to be unmarshaled into `map[string]interface{}` or a pointer to a specific struct (e.g., `*model.KeyPayload`) in the target payload struct. It checks if the source is `[]byte`, and if so, attempts JSON unmarshaling into the target type if it's a map or a pointer to a struct. Handles empty/null JSON byte slices by returning an empty map or nil pointer.

### 4. `generatePayloadStruct(payloadName string, overrides ...interface{}) (interface{}, error)`
    -   **Description:** This is a core function for creating instances of specific NATS event payload structs (e.g., `model.UpsertChatPayload`, `model.UpdateMessagePayload`).
    -   **Parameters:**
        -   `payloadName`: A string identifying the payload struct to create (e.g., "UpsertChatPayload").
        -   `overrides`: Optional. Can be a struct of the same type as the target payload (for direct field overrides) or a `map[string]interface{}` for more flexible overrides.
    -   **Details:**
        1.  Determines the base GORM model type (e.g., `model.Chat` for `UpsertChatPayload`) and the target payload struct type based on `payloadName`.
        2.  Calls `generateModelStruct` to get a base model instance (with its defaults).
        3.  Uses `mapstructure` (with the `unmarshalJSONDataHookFunc`) to decode/map fields from the `baseModel` instance to a new instance of the `targetPayload` struct. It uses "json" tags for field name matching.
        4.  Includes a fallback manual mapping section if `mapstructure.Decode` fails (logs a warning). This section manually copies fields for `UpsertChatPayload` and `UpsertMessagePayload`, including best-effort unmarshaling for JSON-like fields (`LastMessageObj`, `MessageObj`, `Key`).
        5.  Handles special cases for history payloads (e.g., `HistoryChatPayload`) by wrapping the single generated payload item into the correct list structure (e.g., `Chats: []model.UpsertChatPayload{...}`).
        6.  Applies the provided `overrides`:
            -   If `overrides[0]` is the same type as `targetPayload`, it merges non-zero fields from the override struct onto the target.
            -   If `overrides[0]` is a map, it uses `mapstructure` again to decode this map onto the `targetPayload`.
        7.  Returns the fully constructed and overridden payload struct instance.

### 5. `generateNatsPayload(subject string, overrides ...interface{}) ([]byte, error)`
    -   **Description:** This function is defined in `integration_test/main_test.go` but is highly related to the fixture generation process. It takes a full NATS subject, determines the base subject and payload type using `subjectToPayloadMap` (global in this file), generates the appropriate payload struct using `generatePayloadStruct` (from this file), applies overrides, marshals the struct to JSON, and finally validates the JSON against a schema (using `SubjectToSchemaMap` from `integration_test/main_test.go` and `validatePayload` from `integration_test/helper_test.go`).
    -   *(Note: Its definition is in `integration_test/main_test.go`, but its core dependency `generatePayloadStruct` is in this file, `fixtures_test.go`.)*

## Global Variables
-   `subjectToPayloadMap`: A map associating base NATS subjects (e.g., "v1.chats.upsert") to their corresponding NATS payload struct names (e.g., "UpsertChatPayload"). This is used by `generateNatsPayload` (defined in `integration_test/main_test.go`) to determine which payload struct to generate via `generatePayloadStruct` (defined in this file).

*(Note: `validatePayload` (defined in `integration_test/helper_test.go`) is used by `generateNatsPayload` (defined in `integration_test/main_test.go`), which in turn relies on helpers from this file. `SubjectToSchemaMap` is a global in `integration_test/main_test.go`.)*

---

# File: integration_test/onboarding_log_test.go

## Test Suite

### `OnboardingLogTestSuite`
-   **Description:** Defines a suite for testing the creation and handling of onboarding log entries. These logs are typically generated for incoming messages from phone numbers not yet registered as contacts. It embeds `E2EIntegrationTestSuite`.
-   **Setup Function (Global):** `TestOnboardingLogSuite(t *testing.T)` runs the suite.
-   **Setup Function (Per-Test):** `SetupTest()`:
    -   Calls the base `E2EIntegrationTestSuite.SetupTest()`.
    -   Initializes a `zaptest` logger named "OnboardingLogTestSuite".
    -   Includes a 500ms delay.

## Helper Functions (Methods of `OnboardingLogTestSuite`)

### 1. `generateOnboardingMessagePayload(t require.TestingT, tenantID, phoneNumber, messageID string) []byte`
    -   **Description:** Generates a JSON byte slice for an `UpsertMessagePayload` specifically tailored for onboarding tests.
    -   **Details:**
        -   Creates an `model.UpsertMessagePayload` struct.
        -   Sets `Flow: "IN"` (critical for onboarding logic).
        -   Sets `From` to the `phoneNumber` (also key for identifying new senders).
        -   Includes other necessary fields like `MessageID`, `CompanyID`, `ChatID` (generated), `Jid`, `AgentID`, `Key`, `MessageObj`, `Status`, `MessageTimestamp`.
        -   Marshals the struct to JSON and returns the bytes. Uses `require.NoError` for marshaling.

## Test Cases (Methods of `OnboardingLogTestSuite`)

### 1. `TestOnboardingLogCreation()`
    -   **Description:** Verifies that an `onboarding_log` entry is created when an incoming message (`Flow: "IN"`) is received from a phone number that does not yet exist in the `contacts` table for the tenant.
    -   **Steps:**
        1.  Generates a unique `phoneNumber` and `messageID`.
        2.  Optionally (good practice, as noted in comments) verifies that a contact with this `phoneNumber` does not already exist.
        3.  Generates an "IN" flow message payload using `generateOnboardingMessagePayload` for this `phoneNumber`.
        4.  Publishes the message to the `v1.messages.upsert` subject for the tenant.
        5.  Waits for processing.
        6.  Verifies that an entry now exists in the `onboarding_log` table matching the `messageID`, `company_id`, and `phoneNumber`.

### 2. `TestOnboardingLogDuplicatePrevention()`
    -   **Description:** Verifies that sending the exact same message (which would trigger onboarding if the contact is new) multiple times does not create duplicate entries in the `onboarding_log` table. It expects the log entry to be created on the first message, and subsequent identical messages to not add more logs for that same original message.
    -   **Steps:**
        1.  Generates a unique `phoneNumber` and `messageID`.
        2.  Generates and publishes an "IN" flow message payload for this `phoneNumber`.
        3.  Waits and verifies that one `onboarding_log` entry exists for this `messageID`.
        4.  Publishes the *exact same* message payload again.
        5.  Waits and verifies that there is *still only one* `onboarding_log` entry for that `messageID`.

### 3. `TestOnboardingLogSkipExistingContact()`
    -   **Description:** Verifies that no `onboarding_log` entry is created if an incoming message is from a phone number that already exists as a contact for the tenant.
    -   **Steps:**
        1.  Generates a unique `phoneNumber` and `messageID`.
        2.  Manually inserts a contact record directly into the database for this `phoneNumber` and `tenantID` using `s.ExecuteNonQuery()`.
        3.  Generates and publishes an "IN" flow message payload for this (now existing) `phoneNumber`.
        4.  Waits for processing.
        5.  Verifies that **no** `onboarding_log` entry was created for this `messageID` (expects count 0).

---

# File: integration_test/postgres_repo_init_test.go

## Test Suite

### `PostgresInitTestSuite`
-   **Description:** Defines a suite for testing the initialization logic of the PostgreSQL repository (`storage.NewPostgresRepo`). This specifically focuses on schema creation, execution of manual DDL (Data Definition Language) scripts, GORM auto-migration, and creation of message table partitions. It embeds `BaseIntegrationTestSuite` to get the PostgreSQL container and DSN.
-   **Setup Function (Global):** `TestPostgresInitSuite(t *testing.T)` runs the suite.
-   **Setup/TearDown (Suite/Test):** Inherited from `BaseIntegrationSuite`. No specific per-test setup/teardown is defined within this suite itself.

## Helper Functions (Top-Level in this file)

### 1. `connectDB(dsn string) (*sql.DB, error)`
    -   **Description:** A simple helper to open a new database connection using the `pgx` driver (via `sql.Open("pgx", dsn)`) and ping it.
    -   *(Note: This is a local version. `BaseIntegrationSuite` also has a `connectDB` method. Usage should be consistent or one should be preferred.)*

### 2. `schemaExists(db *sql.DB, schemaName string) (bool, error)`
    -   **Description:** Checks if a PostgreSQL schema with the given `schemaName` exists by querying `information_schema.schemata`.
    -   **Parameters:** Takes an active `*sql.DB` connection.

### 3. `tableExists(db *sql.DB, schemaName, tableName string) (bool, error)`
    -   **Description:** Checks if a table with the given `tableName` exists within the specified `schemaName` by querying `information_schema.tables`.
    -   **Parameters:** Takes an active `*sql.DB` connection.

## Test Cases (Methods of `PostgresInitTestSuite`)

### 1. `TestSchemaCreation()`
    -   **Description:** Verifies that calling `storage.NewPostgresRepo` for a new `tenantID` results in the creation of the corresponding tenant-specific schema (e.g., "daisi_some_tenant_id").
    -   **Steps:**
        1.  Generates a unique `testTenant` ID.
        2.  Calls `storage.NewPostgresRepo` with this `testTenant`.
        3.  Connects directly to the PostgreSQL database.
        4.  Uses `schemaExists()` helper to verify that the expected schema (e.g., "daisi_init_test_schema_uuid") now exists.
        5.  Cleans up by dropping the created schema.

### 2. `TestManualDDLExecution()`
    -   **Description:** Verifies that `storage.NewPostgresRepo` executes any manual DDL statements correctly, specifically checking for the creation of tables like `messages` and `messages_default` within the tenant's schema.
    -   **Steps:**
        1.  Generates a unique `testTenant` ID.
        2.  Calls `storage.NewPostgresRepo`.
        3.  Connects directly to the database.
        4.  Uses `tableExists()` helper to verify that tables defined by manual DDL (e.g., "messages", "messages_default") exist in the tenant's schema.
        5.  Includes commented-out code for optional index verification, noting its complexity.
        6.  Cleans up by dropping the schema.

### 3. `TestAutoMigrateTrue()`
    -   **Description:** Verifies that when `storage.NewPostgresRepo` is called with `autoMigrate=true`, GORM's AutoMigrate functionality creates or updates the necessary tables (chats, agents, contacts, etc.) within the tenant's schema.
    -   **Steps:**
        1.  Generates a unique `testTenant` ID.
        2.  Calls `storage.NewPostgresRepo` with `autoMigrate=true`.
        3.  Connects directly to the database.
        4.  Uses `tableExists()` to verify that tables expected to be managed by GORM AutoMigrate (e.g., "chats", "agents", "contacts", "onboarding_log", "exhausted_events", "messages") exist.
        5.  Also verifies that tables created by manual DDL (e.g., "messages_default") still exist.
        6.  Cleans up by dropping the schema.

### 4. `TestPartitionCreation()`
    -   **Description:** Verifies that `storage.NewPostgresRepo` correctly creates the necessary time-based partitions for the `messages` table (e.g., for the current, previous, and next month, plus a default partition).
    -   **Steps:**
        1.  Generates a unique `testTenant` ID.
        2.  Calls `storage.NewPostgresRepo`.
        3.  Connects directly to the database.
        4.  Calculates the expected partition table names based on the current date (e.g., "messages_y2023m05", "messages_y2023m04", "messages_y2023m06", "messages_default").
        5.  Uses `tableExists()` to verify that each of these expected partition tables exists in the tenant's schema.
        6.  Cleans up by dropping the schema.

---

# File: integration_test/message_repository_test.go

## Test Suite

### `MessageRepoTestSuite`
-   **Description:** Defines a suite for testing the `Message` repository functionalities, specifically `SaveMessage` for creating and updating messages, and ensuring tenant isolation. It embeds `BaseIntegrationTestSuite` to inherit the NATS/Postgres container setup but does *not* include the application container, as it tests the repository layer directly.
-   **Fields:**
    -   `BaseIntegrationTestSuite`: Embedded.
    -   `repo`: `*storage.PostgresRepo`, an instance of the repository initialized for the suite's default `CompanyID`.
    -   `TenantSchemaName`: String storing the schema name for the default tenant (e.g., "daisi_defaultcompanyid").
-   **Setup Function (Global):** `TestMessageRepoSuite(t *testing.T)` runs the suite.
-   **Setup Function (Per-Test):** `SetupTest()`:
    -   Initializes a `zaptest` logger named "MessageRepoTestSuite".
    -   Creates a new `storage.PostgresRepo` instance (`s.repo`) using the suite's `s.PostgresDSN` and `s.CompanyID`.
    -   Sets `s.TenantSchemaName`.
    -   Calls `s.BaseIntegrationSuite.SetupTest()` which handles schema creation and table truncation for `s.TenantSchemaName`.
-   **TearDown Function (Per-Test):** `TearDownTest()`:
    -   Closes the `s.repo` database connection if it's not nil.

## Helper Functions
*This suite uses `generateModelStruct` (from `fixtures_test.go`) and `connectDB` (defined locally in `postgres_repo_init_test.go` but a similar one might exist or be intended for general use). It also uses `tenant.WithCompanyID` for context manipulation and `utils.MustMarshalJSON`.*

## Test Cases (Methods of `MessageRepoTestSuite`)

### 1. `TestSaveMessage()`
    -   **Description:** Tests the `SaveMessage` repository method for both creating a new message and updating an existing one. It also verifies that the `message_date` partition key is correctly set and maintained.
    -   **Steps:**
        1.  Generates a new `model.Message` struct instance with initial data (status "DELIVERED", specific content) using `generateModelStruct` and overrides. Notes the current date for partition key verification.
        2.  Calls `s.repo.SaveMessage()` to create the message.
        3.  Verifies creation by directly querying the database (using `connectDB` and checking the specific tenant schema) for the message ID, status, timestamp, and the `message_date` column.
        4.  Updates fields in the local `msg` struct instance (e.g., `Status` to "READ", different `MessageObj` content).
        5.  Calls `s.repo.SaveMessage()` again to update the message.
        6.  Verifies the update by directly querying the database, checking the new status, updated message content (JSON comparison), and ensuring the `message_date` partition key remained unchanged (as the `MessageTimestamp` wasn't changed in the update step).

### 2. `TestMessageTenantIsolation()`
    -   **Description:** Verifies that message operations are correctly isolated between different tenants. Data for one tenant should not be visible or accessible when operating in the context of another tenant.
    -   **Steps:**
        1.  Defines `tenantA_ID` (from suite's `s.CompanyID`) and `tenantB_ID` (a new unique ID).
        2.  Creates `msgA` for `tenantA_ID`.
        3.  Creates `repoB` (a new `PostgresRepo` instance specifically for `tenantB_ID`).
        4.  Creates `msgB` for `tenantB_ID`.
        5.  Saves `msgA` using `s.repo` (configured for Tenant A).
        6.  Saves `msgB` using `repoB` (configured for Tenant B).
        7.  Directly queries Tenant A's schema:
            -   Verifies `msgA` exists.
            -   Verifies `msgB` does **not** exist.
        8.  Directly queries Tenant B's schema:
            -   Verifies `msgB` exists.
            -   Verifies `msgA` does **not** exist.
        9.  Cleans up by dropping Tenant B's schema.

---

# File: integration_test/contact_repository_test.go

## Test Suite

### `ContactRepoTestSuite`
-   **Description:** Defines a suite for testing the `Contact` repository functionalities. This includes saving (create/update), handling nullable fields, ensuring tenant isolation, and finding contacts by specific criteria. It embeds `BaseIntegrationSuite` for DB/NATS setup, but like `MessageRepoTestSuite`, it tests the repository layer directly without the application container.
-   **Fields:**
    -   `BaseIntegrationTestSuite`: Embedded.
    -   `repo`: `*storage.PostgresRepo`, an instance of the repository initialized for the suite's default `CompanyID`.
    -   `TenantSchemaName`: String storing the schema name for the default tenant.
-   **Setup Function (Global):** `TestContactRepoSuite(t *testing.T)` runs the suite.
-   **Setup Function (Per-Test):** `SetupTest()`:
    -   Initializes a `zaptest` logger named "ContactRepoTestSuite".
    -   Creates a new `storage.PostgresRepo` instance (`s.repo`) for the suite's default tenant.
    -   Sets `s.TenantSchemaName`.
    -   Calls `s.BaseIntegrationSuite.SetupTest()` for schema setup and table truncation.
-   **TearDown Function (Per-Test):** `TearDownTest()`:
    -   Closes the `s.repo` database connection.

## Helper Functions
*This suite uses `generateModelStruct` (from `fixtures_test.go`) and the local `connectDB` helper (similar to the one in `postgres_repo_init_test.go`). It also uses `tenant.WithCompanyID` for context.*

## Test Cases (Methods of `ContactRepoTestSuite`)

### 1. `TestSaveContact()`
    -   **Description:** Tests the `SaveContact` repository method for both creating a new contact and updating an existing one.
    -   **Steps:**
        1.  Generates a new `model.Contact` struct with initial data (CustomName, Status, etc.) using `generateModelStruct`.
        2.  Calls `s.repo.SaveContact()` to create the contact.
        3.  Verifies creation by directly querying the database (using `connectDB` and checking the specific tenant schema) for the contact's ID, name, phone, and status.
        4.  Updates fields in the local `contact` struct (e.g., `CustomName`, `Status`, `Notes`).
        5.  Calls `s.repo.SaveContact()` again to update the contact.
        6.  Verifies the update by directly querying the database, checking the new name, status, notes, and ensuring the phone number (which wasn't part of the update payload) remained unchanged.

### 2. `TestContactNullableFields()`
    -   **Description:** Tests the handling of nullable fields in the `Contact` model, specifically `Dob` (Date of Birth) and `Gender`.
    -   **Steps:**
        1.  Creates a `model.Contact` with `Dob` set to a specific date (as `*time.Time`) and `Gender` set to "FEMALE".
        2.  Saves this contact using `s.repo.SaveContact()`.
        3.  Retrieves the contact directly from the database and verifies:
            -   `CompanyID` is correctly set.
            -   `Dob` is not nil and matches the set date.
            -   `Gender` matches "FEMALE".
            -   `Pob` (Place of Birth, another nullable field not explicitly set) is an empty string.
        4.  Includes commented-out placeholder logic for a future test step: updating the contact to set `Dob` to `nil` and `Gender` to an empty string, then re-verifying. This step notes it would require a specific `UpdateContact` method that can handle partial updates with nulls (e.g., using `map[string]interface{}` or GORM's `Select`/`Omit`).

### 3. `TestContactTenantIsolation()`
    -   **Description:** Verifies that contact operations are correctly isolated between different tenants.
    -   **Steps:** (Similar structure to `TestMessageTenantIsolation`)
        1.  Defines `tenantA_ID` (suite default) and `tenantB_ID` (new unique ID).
        2.  Creates `contactA` for `tenantA_ID`.
        3.  Creates `repoB` (new `PostgresRepo` for `tenantB_ID`).
        4.  Creates `contactB` for `tenantB_ID`.
        5.  Saves `contactA` using `s.repo` (for Tenant A).
        6.  Saves `contactB` using `repoB`.
        7.  Directly queries Tenant A's schema: verifies `contactA` exists and `contactB` does not.
        8.  Directly queries Tenant B's schema: verifies `contactB` exists and `contactA` does not.
        9.  Cleans up by dropping Tenant B's schema.

### 4. `TestFindContactByPhoneAndAgentID()`
    -   **Description:** Tests the `FindContactByPhoneAndAgentID` repository method.
    -   **Steps:**
        1.  Creates and saves a test contact with a specific `PhoneNumber` and `AgentID`.
        2.  Calls `s.repo.FindContactByPhoneAndAgentID()` with the correct phone and agent ID. Verifies the contact is found and its details match.
        3.  Calls the find method with the correct phone but a wrong agent ID. Verifies an `apperrors.ErrNotFound` is returned.
        4.  Calls the find method with a wrong phone but the correct agent ID. Verifies an `apperrors.ErrNotFound` is returned.
        5.  Calls the find method with an empty string for `agentID`. Verifies an `apperrors.ErrBadRequest` is returned (as per expected implementation).
        6.  Calls the find method with a non-existent phone and agent ID combination. Verifies an `apperrors.ErrNotFound` is returned.

---

# File: integration_test/chat_repository_test.go

## Test Suite

### `ChatRepoTestSuite`
-   **Description:** Defines a suite for testing the `Chat` repository functionalities, specifically `SaveChat` for creating and updating chat records, and ensuring tenant isolation. Similar to other repository test suites, it embeds `BaseIntegrationTestSuite` for DB/NATS setup but tests the repository layer directly.
-   **Fields:**
    -   `BaseIntegrationTestSuite`: Embedded.
    -   `repo`: `*storage.PostgresRepo`, an instance of the repository initialized for the suite's default `CompanyID`.
    -   `TenantSchemaName`: String storing the schema name for the default tenant.
-   **Setup Function (Global):** `TestChatRepoSuite(t *testing.T)` runs the suite.
-   **Setup Function (Per-Test):** `SetupTest()`:
    -   Initializes a `zaptest` logger named "ChatRepoTestSuite".
    -   Creates a new `storage.PostgresRepo` instance (`s.repo`) for the suite's default tenant.
    -   Sets `s.TenantSchemaName`.
    -   Calls `s.BaseIntegrationSuite.SetupTest()` for schema setup and table truncation.
-   **TearDown Function (Per-Test):** `TearDownTest()`:
    -   Closes the `s.repo` database connection.

## Helper Functions
*This suite uses `generateModelStruct` (from `fixtures_test.go`), the local `connectDB` helper, `tenant.WithCompanyID` for context, and `utils.MustMarshalJSON`.*

## Test Cases (Methods of `ChatRepoTestSuite`)

### 1. `TestSaveChat()`
    -   **Description:** Tests the `SaveChat` repository method for both creating a new chat and updating an existing one.
    -   **Steps:**
        1.  Generates a new `model.Chat` struct with initial data (CustomName, UnreadCount, LastMessageObj, etc.) using `generateModelStruct`.
        2.  Calls `s.repo.SaveChat()` to create the chat.
        3.  Verifies creation by directly querying the database for the chat ID, custom name, unread count, and conversation timestamp.
        4.  Updates fields in the local `chat` struct instance (e.g., `CustomName`, `UnreadCount`, `LastMessageObj`, `ConversationTimestamp`).
        5.  Calls `s.repo.SaveChat()` again to update the chat.
        6.  Verifies the update by directly querying the database, checking the new custom name, unread count, conversation timestamp, and the updated `last_message` JSON content.

### 2. `TestChatTenantIsolation()`
    -   **Description:** Verifies that chat operations are correctly isolated between different tenants.
    -   **Steps:** (Similar structure to `TestMessageTenantIsolation` and `TestContactTenantIsolation`)
        1.  Defines `tenantA_ID` (suite default) and `tenantB_ID` (new unique ID).
        2.  Creates `chatA` for `tenantA_ID`.
        3.  Creates `repoB` (new `PostgresRepo` for `tenantB_ID`).
        4.  Creates `chatB` for `tenantB_ID`.
        5.  Saves `chatA` using `s.repo` (for Tenant A).
        6.  Saves `chatB` using `repoB`.
        7.  Directly queries Tenant A's schema: verifies `chatA` exists and `chatB` does not.
        8.  Directly queries Tenant B's schema: verifies `chatB` exists and `chatA` does not.
        9.  Cleans up by dropping Tenant B's schema.

---

# File: integration_test/agent_repository_test.go

## Test Suite

### `AgentRepoTestSuite`
-   **Description:** Defines a suite for testing the `Agent` repository functionalities, primarily `SaveAgent` for creating/updating agents and ensuring tenant isolation. It embeds `BaseIntegrationSuite` for DB/NATS setup but tests the repository layer directly.
-   **Fields:**
    -   `BaseIntegrationTestSuite`: Embedded.
    -   `repo`: `*storage.PostgresRepo`, an instance of the repository for the suite's default `CompanyID`.
    -   `TenantSchemaName`: String storing the schema name for the default tenant.
-   **Setup Function (Global):** `TestAgentRepoSuite(t *testing.T)` runs the suite.
-   **Setup Function (Per-Test):** `SetupTest()`:
    -   Initializes a `zaptest` logger named "AgentRepoTestSuite".
    -   Creates a new `storage.PostgresRepo` instance (`s.repo`) for the suite's default tenant.
    -   Sets `s.TenantSchemaName`.
    -   Calls `s.BaseIntegrationSuite.SetupTest()` for schema setup and table truncation. (The comment mentions explicit truncation was removed, relying on the base suite's setup.)
-   **TearDown Function (Per-Test):** `TearDownTest()`:
    -   Closes the `s.repo` database connection.

## Helper Functions
*This suite uses `generateModelStruct` (from `fixtures_test.go`), the local `connectDB` helper, and `tenant.WithCompanyID` for context.*

## Test Cases (Methods of `AgentRepoTestSuite`)

### 1. `TestSaveAgent()`
    -   **Description:** Tests the `SaveAgent` repository method for both creating a new agent and updating an existing one.
    -   **Steps:**
        1.  Generates a new `model.Agent` struct with initial data (AgentName "Initial Agent Name", Status "disconnected") using `generateModelStruct`.
        2.  Calls `s.repo.SaveAgent()` to create the agent.
        3.  Verifies creation by directly querying the database (using `connectDB` and checking the tenant schema) for the agent's ID, name, and status.
        4.  Updates fields in the local `agent` struct (e.g., `AgentName` to "Updated Agent Name", `Status` to "connected", `Version` to "1.1.0").
        5.  Calls `s.repo.SaveAgent()` again to update the agent.
        6.  Verifies the update by directly querying the database, checking the new name, status, and version.

### 2. `TestAgentTenantIsolation()`
    -   **Description:** Verifies that agent operations are correctly isolated between different tenants.
    -   **Steps:** (Similar structure to `TestMessageTenantIsolation` and `TestContactTenantIsolation`)
        1.  Defines `tenantA_ID` (suite default) and `tenantB_ID` (new unique ID).
        2.  Creates `agentA` for `tenantA_ID`.
        3.  Creates `repoB` (new `PostgresRepo` for `tenantB_ID`, which also creates `schemaB`).
        4.  Creates `agentB` for `tenantB_ID`.
        5.  Saves `agentA` using `s.repo` (for Tenant A).
        6.  Saves `agentB` using `repoB`.
        7.  Directly queries Tenant A's schema (`s.TenantSchemaName`): verifies `agentA` exists and `agentB` does not.
        8.  Directly queries Tenant B's schema (`schemaB`): verifies `agentB` exists and `agentA` does not.
        9.  Cleans up by dropping Tenant B's schema.

---

# File: integration_test/agent_events_test.go

## Test Suite

### `AgentEventsTestSuite`
-   **Description:** Defines a suite for testing the handling of NATS events related to agents, specifically upsert (create/update) operations. It ensures that events are processed correctly and result in the expected database state, including tenant isolation and handling of optional fields. It embeds `E2EIntegrationTestSuite` because it needs the application container running to process these NATS events.
-   **Setup Function (Global):** `TestAgentEventsSuite(t *testing.T)` runs the suite.
-   **Setup Function (Per-Test):** `SetupTest()`:
    -   Initializes a `zaptest` logger named "AgentEventsTestSuite".
    -   Calls the base `E2EIntegrationTestSuite.SetupTest()` (which includes DB truncation).
    -   Includes a 500ms delay.

## Helper Functions (Methods of `AgentEventsTestSuite`)

### 1. `GetAgentByAgentID(ctx context.Context, agentID, schema string) (*model.Agent, error)`
    -   **Description:** Fetches a single agent record from the database based on the `agentID` and the specified `schema`.
    -   **Details:**
        -   Connects to the database using `s.connectDB()` (from `BaseIntegrationSuite`).
        -   Executes a SQL `SELECT` query against the `agents` table within the provided `schema`.
        -   Scans the result into an `model.Agent` struct.
        -   Returns `(nil, nil)` if `sql.ErrNoRows` occurs (agent not found).
        -   Returns the `*model.Agent` and `nil` error on success, or `nil` agent and an error on other query/scan failures.
        -   The query selects all relevant fields, including `last_metadata` (JSONB).

## Test Cases (Methods of `AgentEventsTestSuite`)

### 1. `TestAgentUpsertEvent()`
    -   **Description:** Tests the creation of a new agent record via a NATS upsert event (`v1.agents.<tenantID>`).
    -   **Steps:**
        1.  Generates an `model.UpsertAgentPayload` with initial data (QRCode, Status, AgentName, etc.) for the suite's `tenantID`.
        2.  Marshals the payload to JSON.
        3.  Publishes the JSON payload to the agent upsert NATS subject.
        4.  Waits for processing.
        5.  Verifies (using `s.VerifyPostgresData`) that an agent record now exists in the database for the correct tenant schema with all the initial data accurately stored.

### 2. `TestAgentUpdateEvent()`
    -   **Description:** Tests updating an existing agent record via a NATS upsert event, ensuring fields are changed and timestamps (`created_at`, `updated_at`) are handled correctly.
    -   **Steps:**
        1.  Publishes an initial agent upsert event to create an agent record. Waits and verifies its existence.
        2.  Fetches the initially created agent using `s.GetAgentByAgentID()` to get its `CreatedAt` and `UpdatedAt` timestamps.
        3.  Generates a new `model.UpsertAgentPayload` with the *same* `AgentID` but with updated values for fields like QRCode, Status, AgentName, Version.
        4.  Publishes this update payload to the same agent upsert NATS subject.
        5.  Waits for processing.
        6.  Verifies (using `s.VerifyPostgresData`) that the agent record in the database reflects all the updated values.
        7.  Fetches the updated agent again using `s.GetAgentByAgentID()`.
        8.  Asserts that `CreatedAt` timestamp has *not* changed.
        9.  Asserts that `UpdatedAt` timestamp *has* been updated (is after the initial `UpdatedAt`).

### 3. `TestAgentEventDifferentTenant()`
    -   **Description:** Tests tenant isolation for agent events. An event published for one tenant (`otherTenantID`) should not create or affect agent data in a different tenant's schema (`primaryTenantID`).
    -   **Steps:**
        1.  Defines `primaryTenantID` (from suite) and `otherTenantID` (a new unique ID).
        2.  Generates an `model.UpsertAgentPayload` where `CompanyID` is set to `otherTenantID`.
        3.  Publishes this payload to a NATS subject specifically for `otherTenantID` (e.g., `v1.agents.other-test-tenant-uuid`).
        4.  Waits for processing.
        5.  Verifies (using `s.VerifyPostgresData` against the `primaryTenantID`'s schema, which is `s.CompanySchemaName`) that **no** agent record with the published `agentID` exists in the `primaryTenantID`'s schema.

### 4. `TestAgentUpsertEvent_OptionalFieldsNull()`
    -   **Description:** Tests the upserting of an agent via a NATS event where optional string fields in the payload are provided as empty strings. It verifies that these are stored correctly (likely as empty strings or NULLs in the DB, which the test asserts as empty strings in the model after fetching).
    -   **Steps:**
        1.  Creates an agent upsert payload as `map[string]interface{}` where optional fields like `qr_code`, `agent_name`, `host_name`, `version` are set to `""` (empty string). `status` (required) is set to "pending".
        2.  Uses `s.GenerateNatsPayload` to create the final JSON bytes (this helper also does schema validation).
        3.  Publishes this payload.
        4.  Uses `s.Require().Eventually` to repeatedly try fetching the agent using `s.GetAgentByAgentID()` until it's found or times out (10 seconds).
        5.  Once fetched, asserts:
            -   `AgentID`, `CompanyID`, and `Status` match the input.
            -   Optional string fields (`QRCode`, `AgentName`, `HostName`, `Version`) are indeed empty strings in the fetched `model.Agent` struct.

---

# File: integration_test/nats_test.go

## Test Suite
*This file does not define any `testify/suite` structures or `Test...` functions. It contains helper functions for NATS container setup and message publishing for integration tests.*

## Helper Functions

### 1. `startNATSContainer(ctx context.Context, networkName string, nwr *testcontainers.DockerNetwork) (testcontainers.Container, string, error)`
    -   **Description:** Starts a NATS testcontainer with JetStream enabled.
    -   **Details:**
        -   Uses `testcontainers-go/modules/nats` to run a `nats:2.11-alpine` image.
        -   Configures arguments like container name ("test-nats-server"), HTTP port (8222 for monitoring), and store directory.
        -   Associates the container with the provided Docker `networkName` and `*testcontainers.DockerNetwork` instance, aliasing it as "nats".
        -   Retrieves the host-accessible connection string (URL) for the NATS container.
        -   Calls `setupJetStreamNew()` (though this function is currently a no-op as stream/consumer creation is expected from the application).
        -   Returns the container instance, its connection URL, and any error.

### 2. `setupJetStreamNew(ctx context.Context, natsContainer testcontainers.Container) error`
    -   **Description:** Intended to create required NATS JetStream streams and consumers for tests using `natsContainer.Exec()` to run `nats stream add` and `nats consumer add` commands.
    -   **Status:** Currently, the actual commands to create streams and consumers are commented out. The function returns `nil` (no-op). The comment indicates that stream and consumer creation is now expected to be handled by the application itself.

### 3. `publishTestMessageNew(ctx context.Context, natsURL string, subject string, payload []byte) error`
    -   **Description:** Publishes a test message to a NATS JetStream subject after validating its payload against a JSON schema. This function is designed to be used by test cases.
    -   **Details:**
        1.  Connects to the NATS server using `natsURL`.
        2.  Determines the `baseSubject` by attempting to remove a tenant suffix (e.g., "_tenant_dev", "_company_") from the full `subject`.
        3.  Looks up the `schemaName` from the global `SubjectToSchemaMap` using the `baseSubject`. Returns an error if no mapping is found.
        4.  Calls `validatePayload()` (expected to be defined elsewhere, likely `semantic_validation_test.go`) to validate the `payload` against the `schemaName`.
        5.  Accesses the JetStream context.
        6.  Publishes the `payload` to the full `subject`.
        7.  Logs the publish details (subject and sequence number).
        8.  Includes a short `time.Sleep(200 * time.Millisecond)` after publishing.

### 4. `publishTestHistoricalPayload(ctx context.Context, natsURL, payloadType, companyID string, payloads *TestPayloads) error`
    -   **Description:** A helper to publish specific historical data payloads (chat, message, contact) from a `TestPayloads` struct.
    -   **Details:**
        -   Uses a `switch` statement on `payloadType` ("history_chat", "history_message", "history_contact").
        -   Constructs the full NATS subject using `SubjectMapping` and appending the `companyID` if provided.
        -   Calls `publishTestMessageNew()` to publish the corresponding historical payload from the `payloads` struct (e.g., `payloads.ChatHistory`).

### 5. `publishTestPayloadsNew(ctx context.Context, natsURL string, payloads *TestPayloads) error`
    -   **Description:** Publishes a set of different test payloads (chat upsert/update/history, message upsert/update/history) from a `TestPayloads` struct to their respective NATS subjects.
    -   **Details:**
        -   For each payload type in the `payloads` struct (if the payload byte slice is not empty):
            -   Gets the base subject from `SubjectMapping`.
            -   Calls `publishTestMessageNew()` to publish the payload.
        -   *(Note: This function does not seem to append a company/tenant ID to the subjects, unlike `publishTestHistoricalPayload`. This might imply it's for general, non-tenant-specific subjects or that the tenant ID is expected to be part of the subject in `SubjectMapping` or handled by the `payloads` content if `TestPayloads` were fully implemented with tenant context.)*

*(Note: `validatePayload()` and `SubjectToSchemaMap`/`SubjectMapping` are used but defined elsewhere. `TestPayloads` struct is defined in `fixtures_test.go` but its generation function `generateTestPayloads` is largely a placeholder in that file.)*

---

# File: integration_test/helper_test.go

## Test Suite
*This file does not define any `testify/suite` structures or `Test...` functions. It contains general-purpose helper functions used across various integration tests.*

## Helper Functions

### 1. `executeQuery(ctx context.Context, dsn string, query string) (interface{}, error)`
    -   **Description:** Executes a SQL query (expected to return a single row and single column) on the PostgreSQL database and returns the scanned result as `interface{}`.
    -   **Details:** Opens a new DB connection, executes `QueryRowContext().Scan()`, and closes the connection.
    -   *(Note: This is a general query helper. Other files also define more specific query/verification helpers like `verifyPostgresDataWithSchema` which might be preferred for structured checks.)*

### 2. `executeNonQuerySQL(ctx context.Context, dsn string, statement string) error`
    -   **Description:** Executes a SQL statement that doesn't return rows (e.g., INSERT, UPDATE, DELETE, DDL) on the PostgreSQL database.
    -   **Details:** Opens a new DB connection, executes `ExecContext()`, and closes the connection.

### 3. `verifyRecordExists(ctx context.Context, dsn string, table string, whereClause string) (bool, error)`
    -   **Description:** Checks if at least one record exists in a specified `table` that matches the given `whereClause`.
    -   **Details:** Opens a new DB connection. Constructs a query like `SELECT 1 FROM <table> WHERE <whereClause> LIMIT 1`. Tries to scan the result. Returns `true` if a row is found, `false` if `sql.ErrNoRows` occurs, and an error for other issues.

### 4. `validatePayload(payloadBytes []byte, schemaName string) error`
    -   **Description:** Validates a JSON payload (provided as `[]byte`) against a specified JSON schema file. The `schemaName` should correspond to a `.json` file located in the `internal/model/jsonschema/` directory (e.g., "message.upsert.schema" maps to "message.upsert.schema.json").
    -   **Details:**
        1.  Calls `getProjectRoot()` to find the project's root directory.
        2.  Constructs the absolute path to the schema file (e.g., `<projectRoot>/internal/model/jsonschema/<schemaName>.json`).
        3.  Checks if the schema file exists.
        4.  Uses `github.com/xeipuuv/gojsonschema` library to load the schema file and the payload bytes.
        5.  Performs validation.
        6.  If validation fails, it formats an error message including all validation errors and a truncated version of the payload for debugging. Returns `nil` if valid.

### 5. `getProjectRoot() (string, error)`
    -   **Description:** Attempts to determine the project's root directory by traversing upwards from the current working directory until a `go.mod` file is found.
    -   **Details:** Starts from `os.Getwd()`. In a loop, it checks for `go.mod` in the current directory. If not found, it moves to the parent directory. If the root of the filesystem is reached without finding `go.mod`, it has a fallback to check if the current directory is "integration_test" and, if so, assumes the parent is the project root. Returns an error if `go.mod` cannot be located.

### 6. `generateUUID() string`
    -   **Description:** A simple wrapper around `uuid.New().String()` to generate a new UUID string.

---

# File: integration_test/init.sql

## Purpose
This SQL script is designed to initialize the database schema for the `daisi-wa-events-processor` specifically for a **default tenant** (`daisi_tenant_dev`) used during integration testing. It sets up all necessary tables, indexes, and comments for this tenant. It is referenced by the test setup in `integration_test/main_test.go` and applied via helper functions in `integration_test/database_test.go`.

## Script Breakdown:

1.  **Timezone Setting:**
    -   `SET timezone = 'UTC';`
        -   Ensures all timestamp operations within the session use UTC.

2.  **Schema Creation & Path Setting:**
    -   `CREATE SCHEMA IF NOT EXISTS daisi_tenant_dev;`
        -   Creates the schema named `daisi_tenant_dev` if it doesn't already exist. This schema will contain all tables for this specific tenant.
    -   `SET search_path TO daisi_tenant_dev;`
        -   Sets the default schema for subsequent DDL/DML commands in the script, so table names don't need to be schema-qualified.

3.  **Table: `messages` (Partitioned Table)**
    -   **DDL:** Creates the main `messages` table, which is partitioned by range on the `message_date` (DATE) column.
        -   Includes columns like `id` (BIGSERIAL), `message_id` (TEXT), `from` (TEXT, quoted), `to` (TEXT, quoted), `chat_id` (TEXT), `jid` (TEXT), `company_id` (VARCHAR), `key` (JSONB), `message_obj` (JSONB), `flow` (TEXT), `agent_id` (TEXT), `status` (TEXT), `is_deleted` (BOOLEAN), `message_timestamp` (BIGINT), `message_date` (DATE, NOT NULL, partition key), `last_metadata` (JSONB), `created_at` (TIMESTAMPTZ), `updated_at` (TIMESTAMPTZ).
        -   The primary key is a composite key `(id, message_date)` to include the partition key.
    -   **Indexes:** Creates several indexes on `messages` for common query patterns:
        -   Unique index on `(message_id, message_date)`.
        -   Indexes on `from`, `to`, `chat_id`, `jid`, `agent_id`, `message_timestamp`, and `message_date`.
    -   **Default Partition:**
        -   `CREATE TABLE IF NOT EXISTS messages_default PARTITION OF messages DEFAULT;`
            -   Creates a default partition to catch any messages whose `message_date` does not fall into any explicitly defined date range partitions.

4.  **Table: `contacts`**
    -   **DDL:** Creates the `contacts` table.
        -   Includes columns: `id` (TEXT, PRIMARY KEY), `phone_number` (TEXT, NOT NULL), `type` (TEXT), `custom_name` (TEXT), `notes` (TEXT), `tags` (TEXT), `company_id` (VARCHAR), `avatar` (TEXT), `assigned_to` (TEXT), `pob` (TEXT), `dob` (DATE), `gender` (TEXT, DEFAULT 'MALE'), `origin` (TEXT), `push_name` (TEXT), `status` (TEXT, DEFAULT 'ACTIVE'), `agent_id` (TEXT), `first_message_id` (TEXT), `first_message_timestamp` (BIGINT), `created_at` (TIMESTAMPTZ), `updated_at` (TIMESTAMPTZ), `last_metadata` (JSONB).
    -   **Indexes:**
        -   Unique index on `phone_number`.
        -   Indexes on `assigned_to` and `agent_id`.

5.  **Table: `agents`**
    -   **DDL:** Creates the `agents` table.
        -   Includes columns: `id` (BIGSERIAL, PRIMARY KEY), `agent_id` (TEXT, UNIQUE), `qr_code` (TEXT), `status` (TEXT), `agent_name` (TEXT), `host_name` (TEXT), `version` (TEXT), `company_id` (TEXT), `created_at` (TIMESTAMPTZ), `updated_at` (TIMESTAMPTZ), `last_metadata` (JSONB).

6.  **Table: `chats`**
    -   **DDL:** Creates the `chats` table.
        -   Includes columns: `id` (BIGSERIAL, PRIMARY KEY), `chat_id` (TEXT, UNIQUE), `jid` (TEXT), `custom_name` (TEXT), `push_name` (TEXT), `is_group` (BOOLEAN), `group_name` (TEXT), `unread_count` (INTEGER), `assigned_to` (TEXT), `last_message` (JSONB), `conversation_timestamp` (BIGINT), `not_spam` (BOOLEAN), `agent_id` (TEXT), `company_id` (TEXT), `phone_number` (TEXT), `last_metadata` (JSONB), `created_at` (TIMESTAMPTZ), `updated_at` (TIMESTAMPTZ).
    -   **Indexes:** Indexes on `jid`, `assigned_to`, and `agent_id`.

7.  **Table: `onboarding_log`**
    -   **DDL:** Creates the `onboarding_log` table.
        -   Includes columns: `id` (BIGSERIAL, PRIMARY KEY), `message_id` (TEXT), `agent_id` (TEXT), `company_id` (TEXT), `phone_number` (TEXT), `timestamp` (BIGINT), `created_at` (TIMESTAMPTZ), `last_metadata` (JSONB).
    -   **Indexes:** Indexes on `message_id`, `agent_id`, and `phone_number`.

8.  **Table: `exhausted_events` (Dead Letter Queue Table)**
    -   **DDL:** Creates the `exhausted_events` table, likely for storing messages that have failed processing multiple times (DLQ).
        -   Includes columns: `id` (BIGSERIAL, PRIMARY KEY), `created_at` (TIMESTAMPTZ), `company_id` (TEXT, NOT NULL), `source_subject` (TEXT, NOT NULL), `last_error` (TEXT), `retry_count` (INTEGER), `event_timestamp` (TIMESTAMPTZ), `dlq_payload` (JSONB, NOT NULL), `original_payload` (JSONB), `resolved` (BOOLEAN, DEFAULT false), `resolved_at` (TIMESTAMPTZ), `notes` (TEXT).
    -   **Indexes:** Indexes on `source_subject`, `event_timestamp`, `resolved`, and `resolved_at`.

## Overall
This script provides a comprehensive DDL setup for one tenant (`daisi_tenant_dev`). It's intended to be run before integration tests that require a pre-existing schema structure. The use of `IF NOT EXISTS` ensures idempotency for table and schema creation, while index creation also uses `IF NOT EXISTS`. The `messages` table partitioning setup is a key feature.