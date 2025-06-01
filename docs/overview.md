# Project Overview

## Introduction

The Daisi WA Events Processor is a multi-tenant, event-driven microservice designed to ingest and process messages, chats, contacts, and agent status events in real-time. It serves as a central component in a distributed messaging system, enabling applications to publish and react to events without direct coupling between services.

This service is built with scalability, reliability, and observability at its core, using technologies like NATS JetStream for event streaming, PostgreSQL for persistence, and modern observability tools for monitoring and debugging.

## Key Features

- **Event-Driven Architecture**: Uses NATS JetStream for reliable, persistent event processing
- **Multi-Company Support**: Each tenant has isolated data storage and processing
- **Historical Data Import**: Batch import capabilities for chat history and message archives
- **Real-time Event Processing**: Immediate handling of chat, message, and contact events
- **Schema Validation**: Strict JSON schema validation for all incoming events
- **Event Versioning**: Support for versioned event types for backward compatibility
- **Retry & DLQ Handling**: Configurable retries via JetStream for initial failures, followed by a dedicated DLQ worker implementing exponential backoff and persistence of terminally failed messages.
- **Contextual Logging**: Structured logging with tenant ID, request ID, and other context
- **Observability**: Comprehensive metrics, tracing, and health monitoring
- **Idempotent Processing**: Safe handling of duplicate events
- **Graceful Degradation**: Resilient to temporary failures in dependencies

## Architecture Summary

The Daisi WA Events Processor follows a clean, event-driven architecture with a clear separation of concerns:

1. **Event Ingestion Layer**: Consumes events from NATS JetStream and routes them to handlers
2. **Domain Logic Layer**: Processes events according to business rules
3. **Persistence Layer**: Stores data (chats, messages, contacts, agents, exhausted events) primarily in PostgreSQL using GORM.
4. **Observability Layer**: Monitors system health and provides insights

The architecture is designed to be horizontally scalable, with stateless processing workers that can be scaled independently based on load.

## Components & Responsibilities

### Core Components

1. **NATS JetStream Consumer**
   - Subscribes to event subjects
   - Manages acknowledgment and retry logic
   - Extracts metadata and routes events

2. **Event Router**
   - Directs events to appropriate handlers based on type
   - Supports versioned event routing
   - Implements default handling for unknown events

3. **DLQ Worker**
   - Processes messages from the Dead-Letter Queue (`v1.dlq.<company_id>`)
   - Implements retry logic with exponential backoff using a worker pool (e.g., `panjf2000/ants`)
   - Persists terminally failing messages to the `exhausted_events` PostgreSQL table.

4. **Event Handlers**
   - **Historical Handler**: Processes bulk history imports
   - **Realtime Handler**: Processes individual real-time events

5. **Service Layer**
   - Implements business logic for chats, messages, and contacts
   - Validates event payloads against schemas
   - Transforms events to domain models

6. **Repository Layer**
   - Abstracts database operations for PostgreSQL.
   - Implements multi-tenant data storage (schema-per-tenant or row-level security based on `company_id`).
   - Manages PostgreSQL interactions via GORM.

7. **Observability Components**
   - Structured logging with Zap
   - Prometheus metrics collection
   - Health check HTTP endpoints

## Data Flow Summary

### Event Ingestion Flow

1. External systems publish events to NATS JetStream
2. Daisi WA Events Processor subscribes to relevant subjects (e.g., `v1.messages.upsert.<company_id>`)
3. Events are validated against JSON schemas
4. Router directs events to appropriate handlers
5. Handlers process events through the service layer
6. Data is persisted to PostgreSQL.
7. Event is acknowledged (ACK) to NATS JetStream. If processing fails repeatedly, it's NACKed until `max_deliver` is reached, then published to the DLQ.
8. Metrics are updated and spans are completed.

### Typical Event Flows

**Chat Upsert Flow**:
1. Client publishes `v1.chats.upsert` event to NATS
2. Event is consumed and validated
3. Chat is created or updated in PostgreSQL
4. Success is acknowledged back to NATS

**Historical Import Flow**:
1. Client publishes `v1.history.messages` event with batch data
2. Event is consumed and validated
3. Messages are bulk upserted to PostgreSQL
4. Success is acknowledged back to NATS

## Technology Stack

| Component         | Technology                               | Purpose                                    |
|-------------------|------------------------------------------|------------------------------------------- |
| Language          | Go 1.23+                                 | Core implementation language               |
| Event Broker      | NATS JetStream                           | Reliable event streaming                   |
| SQL Database      | PostgreSQL (via GORM)                    | Structured chat and message storage        |
| Persistence       | PostgreSQL (via GORM)                    | Stores chats, messages, contacts, agents, exhausted DLQ events, onboarding logs |
| Worker Pool       | panjf2000/ants                           | Manages goroutines for DLQ worker          |
| Logging           | Uber Zap                                 | Structured, context-aware logging          |
| Configuration     | Viper                                    | Configuration management                   |
| Validation        | go-playground/validator                  | Request validation                         |
| Metrics           | Prometheus                               | Performance and health metrics             |
| Tracing           | OpenTelemetry                            | Distributed tracing                        |
| Testing           | Testcontainers-go                        | Integration testing                        |
| Containerization  | Docker                                   | Application packaging                      |
| Orchestration     | Kubernetes (via Helm/manifests)          | Deployment and scaling                     |

## Use Cases / Scenarios

### Real-time Messaging

- Chat applications sending real-time message updates
- Support agents responding to customer inquiries
- System notifications and alerts

### Historical Data Processing

- Importing chat history from legacy systems
- Bulk loading archived messages during migration
- Importing contact lists from external systems

### Analytics and Monitoring

- Tracking message delivery status
- Monitoring conversation activity
- Analyzing messaging patterns across tenants

### Multi-tenant Deployment

- Supporting multiple customers with isolated data
- Service provider scenarios with per-customer deployment
- SaaS deployments with tenant-specific configuration

## Limitations & Assumptions

### Current Limitations

- Single-region deployment (no cross-region replication)
- Limited to NATS JetStream as the event source
- DLQ handling relies on a background worker pool; high DLQ volume might become a bottleneck without scaling the worker pool
- No built-in rate limiting for event processing

### Assumptions

- Events are published in the expected format and subjects
- NATS JetStream provides sufficient durability for event storage
- Tenants have unique identifiers that don't change
- PostgreSQL can handle the expected message volume
- Network latency between service and databases is low

## Future Improvements

### Near-term Improvements

- Add support for event schema evolution
- Enhance performance monitoring and alerting around consumer lag and processing times
- Implement API endpoints for querying/managing application state (e.g., agent status, basic stats)

### Long-term Roadmap

- Multi-region deployment support
- Add support for alternative event brokers (Kafka, RabbitMQ)
- Implement CQRS pattern with separate read/write models
- Add real-time analytics capabilities
- Implement data retention policies and archiving
- Enhance security with end-to-end encryption
- Implement advanced search capabilities across message history 