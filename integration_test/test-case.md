# Daisi WA Events Processor Integration Test Cases

This document outlines the test cases for the integration testing of the Daisi WA Events Processor, focusing particularly on edge cases, error conditions, and data validation scenarios.

## 1. Input Validation Test Cases

### 1.1 Invalid Payload Format Tests (Implemented in `invalid_input_test.go`)

| ID    | Test Case               | Description                                                                 | Expected Result                                                              | Status         |
|-------|-------------------------|-----------------------------------------------------------------------------|------------------------------------------------------------------------------|----------------|
| IV-01 | Malformed JSON          | Send a payload with malformed JSON structure                                | Service should reject the message and log appropriate error                  | ✅ Implemented |
| IV-02 | Missing Required Fields | Send payloads missing required fields (e.g., no `id` in chat/message)         | Service should reject the message and log validation errors                  | ✅ Implemented |
| IV-03 | Invalid Field Types     | Send payloads with incorrect data types (e.g., numeric string where integer expected) | Service should reject the message and log type validation errors             | ✅ Implemented |
| IV-04 | Oversized Payload       | Send a payload exceeding the maximum allowed size (e.g., >1MB)                | Either NATS or the service should reject the oversized payload with appropriate error | ✅ Implemented |
| IV-05 | Empty Payload           | Send an empty payload or one with only whitespace                             | Service should reject the message and log appropriate error                  | ✅ Implemented |

### 1.2 Schema Validation Tests (Implemented in `invalid_input_test.go`)

| ID    | Test Case                 | Description                                                              | Expected Result                                         | Status         |
|-------|---------------------------|--------------------------------------------------------------------------|---------------------------------------------------------|----------------|
| SV-01 | Invalid Chat Schema       | Send chat payload violating JSON schema (invalid enum values, format)    | Service should reject and log schema validation error   | ✅ Implemented |
| SV-02 | Invalid Message Schema    | Send message payload violating JSON schema                               | Service should reject and log schema validation error   | ✅ Implemented |
| SV-03 | Invalid History Schema    | Send history payload violating JSON schema                               | Service should reject and log schema validation error   | ✅ Implemented |
| SV-04 | Schema Version Mismatch   | Send payload with unsupported schema version                             | Service should handle gracefully with appropriate error | ✅ Implemented |

### 1.3 Semantic Validation Tests

| ID    | Test Case                | Description                                                      | Expected Result                                              | Status         |
|-------|--------------------------|------------------------------------------------------------------|--------------------------------------------------------------|----------------|
| SM-01 | Invalid Timestamp Format | Send events with invalid timestamp formats                       | Service should reject with timestamp validation error        | ✅ Implemented |
| SM-02 | Future Timestamp         | Send events with timestamps in the future                        | Service should either reject or handle with warning          | ✅ Implemented |
| SM-04 | Invalid References       | Message referencing non-existent chat                            | Service should handle gracefully (create chat or queue message) | ✅ Implemented |

## 2. Resource Error Test Cases

### 2.1 Database Connection Issues

| ID    | Test Case                 | Description                                                                 | Expected Result                                                                    | Status         |
|-------|---------------------------|-----------------------------------------------------------------------------|------------------------------------------------------------------------------------|----------------|
| DB-01 | PostgreSQL Down           | Simulate PostgreSQL database being unavailable                                | Service should retry connections with backoff, log errors, and not lose messages   | ✅ Implemented |
| DB-03 | DB Connection Timeout     | Simulate database connection timeouts                                       | Service should implement retry with appropriate backoff                              | ✅ Implemented |
| DB-04 | DB Query Timeout          | Simulate slow database responses causing query timeouts                       | Service should handle with appropriate timeout errors                                | ✅ Implemented |
| DB-05 | DB Connection Recovery    | Bring database back online after being down                                   | Service should reconnect and resume processing queued messages                     | ✅ Implemented |

### 2.2 NATS Connection Issues

| ID    | Test Case               | Description                                                                 | Expected Result                                                              | Status         |
|-------|-------------------------|-----------------------------------------------------------------------------|------------------------------------------------------------------------------|----------------|
| MQ-01 | NATS Server Down        | Simulate NATS server being unavailable                                      | Service should attempt reconnection with backoff strategy                    | ✅ Implemented |
| MQ-02 | NATS Connection Drop    | Simulate network issues causing connection drops                            | Service should recover and resubscribe to subjects                           | ✅ Implemented |
| MQ-03 | NATS Stream Missing     | Attempt to use non-existent stream                                          | Service should create missing stream or report clear error                   | ✅ Implemented |
| MQ-04 | NATS Consumer Missing   | Attempt to use non-existent consumer                                        | Service should create missing consumer or report clear error                 | ✅ Implemented |

## 3. Data Consistency Tests

### 3.1 Partial/Incomplete Data Tests

| ID    | Test Case            | Description                                                              | Expected Result                                                | Status         |
|-------|----------------------|--------------------------------------------------------------------------|----------------------------------------------------------------|----------------|
| PD-01 | Partial Chat Update  | Send chat update with only some fields                                   | Service should update only provided fields, preserve others    | ✅ Implemented |
| PD-02 | Partial Message Update | Send message update with only some fields                                  | Service should update only provided fields, preserve others    | ✅ Implemented |
| PD-03 | Empty Fields         | Send payloads with empty string values for non-required fields           | Service should handle empty strings appropriately              | ✅ Implemented |
| PD-05 | Incomplete Batch     | Send a history batch with some invalid entries                           | Service should process valid entries, reject invalid ones      | ✅ Implemented |

### 3.2 Data Integrity Tests

| ID    | Test Case                 | Description                                                              | Expected Result                                                              | Status         |
|-------|---------------------------|--------------------------------------------------------------------------|------------------------------------------------------------------------------|----------------|
| DI-01 | Duplicate Message IDs     | Send multiple messages with same ID                                      | Service should be idempotent (no duplicates)                                 | ✅ Implemented |
| DI-03 | Conflicting Updates       | Send conflicting updates to same entity                                  | Service should resolve conflicts with last-writer-wins or similar            | ✅ Implemented |
| DI-04 | Orphaned References       | Create references to entities that are later deleted                     | Service should handle orphaned references gracefully                         | ✅ Implemented |
| DI-05 | Data Consistency          | Verify data consistency across databases after complex operations        | All databases should have consistent representation                          | ✅ Implemented |

### 3.3 Company Isolation Tests

| ID    | Test Case                   | Description                                                              | Expected Result                                         | Status         |
|-------|-----------------------------|--------------------------------------------------------------------------|---------------------------------------------------------|----------------|
| TI-01 | Cross-Company Data Access | Attempt to access data from wrong tenant                                 | Service should enforce strict tenant isolation          | ✅ Implemented |
| TI-02 | Missing Company ID          | Send events without tenant ID                                            | Service should reject or assign default tenant          | ✅ Implemented |
| TI-03 | Invalid Company ID          | Send events with invalid tenant ID format                                | Service should reject with clear error                  | ✅ Implemented |

## 4. Recovery and Resilience Tests

| ID    | Test Case                           | Description                                                              | Expected Result                                                              | Status         |
|-------|-------------------------------------|--------------------------------------------------------------------------|------------------------------------------------------------------------------|----------------|
| RC-01 | Service Restart During Processing   | Restart service while processing messages                                | Service should recover and continue processing                               | ✅ Implemented |
| RC-02 | JetStream Delivery Retry          | Force message processing failure to trigger redelivery                   | Message should be redelivered and eventually processed                       | ✅ Implemented |
| RC-03 | Poison Message Handling           | Send persistently failing message                                        | Service should move to dead-letter queue after retry limit                   | ✅ Implemented |
| RC-04 | Transaction Rollback              | Force database transaction failure                                       | Service should rollback cleanly without side effects                         | ✅ Implemented |

## Implementation Progress

| Section                          | Status    | File                           |
|----------------------------------|-----------|--------------------------------|
| 1.1 Invalid Payload Format Tests | ✅ Complete | `invalid_input_test.go`        |
| 1.2 Schema Validation Tests      | ✅ Complete | `invalid_input_test.go`        |
| 1.3 Semantic Validation Tests    | ✅ Complete | `semantic_validation_test.go`  |
| 2.1 Database Connection Issues   | ✅ Complete | `db_connection_test.go`        |
| 2.2 NATS Connection Issues       | ✅ Complete | `nats_server_connection_test.go` (also covers stream/consumer aspects from `nats_stream_lifecycle_test.go` & `nats_consumer_lifecycle_test.go`) |
| 3.1 Partial/Incomplete Data Tests| ✅ Complete | `partial_data_test.go`         |
| 3.2 Data Integrity Tests         | ✅ Complete | `data_integrity_test.go`       |
| 3.3 Company Isolation Tests      | ✅ Complete | `tenant_isolation_test.go`     |
| 4. Recovery and Resilience Tests   | ✅ Complete | `recovery_resilience_test.go`  |

## Notes for Test Implementation

- Use Docker network controls to simulate network issues
- Use `tc` network emulation to simulate latency and packet loss
- Use `kill -STOP` and `kill -CONT` to freeze and resume database processes
- Instrument code with additional logging during tests
- Consider using fault injection tools like Chaos Monkey for some scenarios
- Ensure proper cleanup between test cases to maintain isolation 