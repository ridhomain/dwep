# Project Setup Guide

This guide covers how to set up, run, and deploy the Daisi WA Events Processor in various environments.

## Prerequisites

- **Go 1.23+** (1.23.8 recommended)
- **Docker** and **Docker Compose** for local development
- **kubectl** and **helm** (if deploying to Kubernetes)
- **Make** for running common development tasks

## Environment Variables

The service uses the following environment variables:

| Variable                             | Description                                    | Default                                                 |
|--------------------------------------|------------------------------------------------|---------------------------------------------------------|
| `POSTGRES_DSN`                       | PostgreSQL connection string                   | `postgres://postgres:postgres@localhost:5432/message_service` |
| `NATS_URL`                           | NATS JetStream URL                             | `nats://localhost:4222`                                 |
| `COMPANY_ID`                         | Company identifier                             | `CompanyAAA01` (from default.yaml)                      |
| `LOG_LEVEL`                          | Logging level (`debug`, `info`, `warn`, `error`) | `info`                                                  |
| `SERVER_PORT`                    | HTTP server port                               | `8080`                                                  |
| `DATABASE_POSTGRESAUTOMIGRATE` | Enable/disable auto DB migration             | `true`                                                  |
| `METRICS_ENABLED`                | Enable/disable Prometheus metrics endpoint     | `true`                                                  |
| `METRICS_PORT`                   | Port for Prometheus metrics endpoint           | `2112`                                                  |
| **NATS Realtime Consumer**           |                                                |                                                         |
| `NATS_REALTIME_MAXDELIVER`       | Max delivery attempts before DLQ               | `5`                                                     |
| `NATS_REALTIME_NAKBASEDELAY`     | Base NAK delay for consumer retries            | `1s`                                                    |
| `NATS_REALTIME_NAKMAXDELAY`      | Max NAK delay for consumer retries             | `30s`                                                   |
| **NATS Historical Consumer**         |                                                |                                                         |
| `NATS_HISTORICAL_MAXDELIVER`     | Max delivery attempts before DLQ               | `3`                                                     |
| `NATS_HISTORICAL_NAKBASEDELAY`   | Base NAK delay for consumer retries            | `2s`                                                    |
| `NATS_HISTORICAL_NAKMAXDELAY`    | Max NAK delay for consumer retries             | `60s`                                                   |
| **NATS DLQ Config**                  |                                                |                                                         |
| `NATS_DLQSTREAM`                 | DLQ Stream name                                | `dlq_stream`                                            |
| `NATS_DLQSUBJECT`                | Base DLQ subject prefix                        | `v1.dlq`                                                |
| `NATS_DLQWORKERS`                | Number of DLQ worker goroutines                | `8`                                                     |
| `NATS_DLQBASEDELAYMINUTES`       | Base delay (minutes) for DLQ worker retry      | `5`                                                     |
| `NATS_DLQMAXDELAYMINUTES`        | Max delay (minutes) for DLQ worker retry      | `5`                                                     |
| `NATS_DLQMAXAGEDAYS`             | Retention period (days) for DLQ stream messages | `7`                                                     |
| `NATS_DLQMAXDELIVER`             | Max redelivery attempts for DLQ consumer       | `10`                                                    |
| `NATS_DLQACKWAIT`                | Ack wait for DLQ consumer                      | `30s`                                                   |
| `NATS_DLQMAXACKPENDING`          | Max pending ACKs for DLQ consumer              | `1000`                                                  |
| **Worker Pool (Onboarding)**         |                                                |                                                         |
| `WORKERPOOLS_ONBOARDING_POOLSIZE`| Size of the onboarding worker pool             | `10`                                                    |
| `WORKERPOOLS_ONBOARDING_QUEUESIZE`| Queue size for the onboarding worker pool      | `10000`                                                 |
| `WORKERPOOLS_ONBOARDING_MAXBLOCK` | Max block duration if queue is full            | `1s`                                                    |
| `WORKERPOOLS_ONBOARDING_EXPIRYTIME`| Idle worker expiry time                        | `1m`                                                    |

You can provide these variables using:

1. **Environment variables** directly
2. **`.env`** file in the project root
3. **Configuration file** at `internal/config/default.yaml`

Priority order: Environment variables > `.env` file > Configuration file

## Clone & Install

```sh
# Clone the repository
git clone https://gitlab.com/timkado/api/daisi-wa-events-processor.git
cd daisi-wa-events-processor

# Install dependencies
go mod download

# Build the application
make build
# or
go build -o bin/daisi-wa-events-processor ./cmd/main/main.go
```

## Start Services

### Using Docker Compose (Local Development)

Docker Compose is the easiest way to run all required services locally.

```sh
# Initialize directories (first time only)
make init
# or
mkdir -p ./.data/{nats,postgres,mongodb}

# Start all services (NATS, PostgreSQL, MongoDB)
make up
# or
docker-compose up -d

# View logs
make logs
# or
docker-compose logs -f

# Build and run the service with all dependencies
make dev
```

The docker-compose setup includes:

- **NATS JetStream** on port 4222 (management on 8222)
- **PostgreSQL** on port 54321
- **MongoDB** on port 27017
- **Daisi WA Events Processor** on port 8080

### Running Locally (Without Docker)

If you prefer to run the service outside of Docker:

```sh
# Ensure dependencies are running (if needed)
docker-compose up -d nats postgres mongodb

# Run the service
SERVER_PORT=8080 POSTGRES_DSN="postgres://postgres:postgres@localhost:54321/message_service" COMPANY_ID=tenant_dev go run cmd/main/main.go
```

## Kubernetes Deployment

### Using Raw Manifests

**Important:** Before applying the deployment, you **must** create the Kubernetes Secret that contains the `POSTGRES_DSN`. The deployment expects a secret (named `daisi-wa-events-processor-secret` by default in the manifest) with a key named `POSTGRES_DSN` holding the connection string.

You can create it like this (replace placeholders):
```sh
kubectl create secret generic daisi-wa-events-processor-secret \
  --from-literal=POSTGRES_DSN='postgres://YOUR_USER:YOUR_PASSWORD@YOUR_HOST:YOUR_PORT/YOUR_DB?sslmode=disable'
  # Adjust namespace if needed with -n your-namespace
```

Also, review and **edit `deploy/manifest/configmap.yaml`** to set the correct `COMPANY_ID` and any other non-sensitive overrides for your specific deployment before applying.

```sh
# Apply manifests (ensure namespace exists if not default)
kubectl apply -f deploy/manifest/namespace.yaml # If you have one
kubectl apply -f deploy/manifest/configmap.yaml
kubectl apply -f deploy/manifest/secret.yaml # Applies the Secret definition, but value must be set correctly beforehand or managed externally
kubectl apply -f deploy/manifest/deployment.yaml
kubectl apply -f deploy/manifest/service.yaml
```

### Using Helm Chart

**Important:** The Helm chart expects a Kubernetes Secret containing the `POSTGRES_DSN`. By default, it looks for a secret named `{{ .Release.Name }}-secret` with a data key `POSTGRES_DSN`. You must ensure this secret exists with the correct value before installing the chart, or configure the chart to use a different existing secret via `secret.name` and `secret.postgresDsnKey` values.

```sh
# Add the required dependencies (if not already done)
# helm repo add nats https://nats-io.github.io/k8s/helm/charts/
# helm repo update

# Install the chart with default values (ensure secret exists!)
helm upgrade --install daisi-wa-events-processor ./deploy/helm 
  --namespace messaging \
  --create-namespace

# Install with custom values (e.g., specific company ID)
# Ensure secret exists!
helm upgrade --install daisi-wa-events-processor ./deploy/helm 
  --namespace messaging \
  --create-namespace \
  --set config.companyId=tenant_abc \
  --set replicaCount=2
  # Add other --set flags as needed based on values.yaml
```

## Running Tests

### Unit Tests

```sh
# Run all unit tests
go test ./...

# Run with coverage
go test -cover ./...
```

### Integration Tests

Integration tests use testcontainers-go to spin up all required dependencies.

```sh
# Run all integration tests
cd integration_test
go test -v

# Run specific test
go test -v -run TestIntegrationMessageEventService/TestChatUpsert

# Skip integration tests (short mode)
go test -short ./...
```

## Testing with Mock Events

The project includes a CLI tool (`cmd/tester/main.go`) for generating and publishing batches of mock events to NATS JetStream, primarily intended for load testing.

Key flags for the tester CLI:

*   `--url`: NATS server URL (default: from config/env)
*   `--subjects`: Comma-separated list of base subjects (e.g., `v1.chats.upsert,v1.messages.upsert`)
*   `--rate`: Target total messages per second across all subjects/companies (default: 100)
*   `--duration`: Load test duration (default: 1m)
*   `--concurrency`: Number of concurrent publisher workers (default: 10)
*   `--company_ids`: Comma-separated list of Company IDs to publish for (default: from config/env)
*   `--batch-size`: Number of messages per worker batch (default: 50)
*   `--history-count`: Number of items per historical payload (default: 10)
*   `--metrics-port`: Port for the tester's own Prometheus metrics (default: 9091)
*   `--log-level`: Log level for the tester (default: from config/env)

```sh
# Build the tester application (if not already built by other targets)
make build-tester

# Run the tester to publish upsert events for chats and messages
# for CompanyAAA01 and CompanyBBB02 at 200 msg/sec for 30 seconds
make run-tester -- --subjects v1.chats.upsert,v1.messages.upsert --company_ids CompanyAAA01,CompanyBBB02 --rate 200 --duration 30s

# Generate and publish historical chat events (10 chats per payload)
make run-tester -- --subjects v1.history.chats --history-count 10 --rate 10 --duration 10s

# Generate events with higher concurrency and batch size
make run-tester -- --rate 500 --duration 1m --concurrency 20 --batch-size 100
```

### Load Testing Scenarios

Here are more specific examples using the `make run-tester` target, leveraging the `cmd/tester/main.go` flags:

**Scenario 1: Basic Upsert Load (Chats & Messages)**

Simulate a moderate load of chat and message upserts for the two tenants defined in the load test environment.

```sh
# Target 500 messages/sec total across both tenants for 2 minutes
make run-tester -- --subjects v1.chats.upsert,v1.messages.upsert \
                   --company_ids CompanyAAA01,CompanyBBB02 \
                   --rate 500 \
                   --duration 2m
```

**Scenario 2: Historical Data Ingestion Load**

Simulate the ingestion of historical chat data for a single tenant.

```sh
# Target 50 historical chat payloads/sec for 1 minute
# Each payload contains 20 chat items (--history-count)
make run-tester -- --subjects v1.history.chats \
                   --company_ids CompanyAAA01 \
                   --rate 50 \
                   --duration 1m \
                   --history-count 20
```

**Scenario 3: High Concurrency Stress Test**

Push the system with a higher rate, increased worker concurrency, and larger batch sizes.

```sh
# Target 1000 messages/sec total for 5 minutes
# Use 30 concurrent workers and a batch size of 100
make run-tester -- --subjects v1.chats.upsert,v1.messages.upsert,v1.contacts.upsert \
                   --company_ids CompanyAAA01,CompanyBBB02 \
                   --rate 1000 \
                   --duration 5m \
                   --concurrency 30 \
                   --batch-size 100
```

**Scenario 4: Specific Subject Load**

Isolate load testing to only message upserts for a single tenant.

```sh
# Target 250 message upserts/sec for CompanyBBB02 for 3 minutes
make run-tester -- --subjects v1.messages.upsert \
                   --company_ids CompanyBBB02 \
                   --rate 250 \
                   --duration 3m
```

Remember to adjust the `--rate`, `--duration`, `--concurrency`, `--batch-size`, and other flags based on your specific testing goals and the capacity of your test environment. Check the tester's Prometheus metrics (default port 9091) and the service's metrics (Prometheus at 9090) to monitor performance.

## Load Testing

A dedicated Docker Compose file (`docker-compose.loadtest.yaml`) and Makefile targets are provided for running load tests.

Key features of the load test environment:

*   Runs multiple replicas (default 3) of the daisi-wa-events-processor for different tenants (`tenant_app_A`, `tenant_app_B`).
*   Uses a separate PostgreSQL database (`message_service_loadtest`) on a different port (`54322`) to avoid interfering with the development DB.
*   Includes Prometheus and Grafana for monitoring the services under load.
*   Includes `nats-exporter` for detailed NATS metrics.

Use the following Makefile targets:

```sh
# Build the service image (required before starting)
make loadtest-build

# Start the full load test environment (multiple replicas, monitoring)
make loadtest-up

# Stop the load test environment
make loadtest-down

# Restart the load test environment
make loadtest-restart

# View logs from all load test services
make loadtest-logs

# To run the load generator against this environment:
# Ensure NATS_URL points to nats://localhost:4222 (default)
# Use company IDs CompanyAAA01 and CompanyBBB02
# Example:
# make run-tester -- -subjects v1.chats.upsert,v1.messages.upsert -company_ids CompanyAAA01,CompanyBBB02 -rate 500 -duration 2m
```

Access Prometheus at `http://localhost:9090` and Grafana at `http://localhost:3000`.

## Troubleshooting

### Common Issues

#### Docker Compose Issues

If services fail to start:

```sh
# Check container status
docker-compose ps

# Check container logs
docker-compose logs nats
docker-compose logs postgres
docker-compose logs mongodb
docker-compose logs message-service
```

#### Database Connection Issues

If the service fails to connect to databases:

1. Verify container health: `docker ps`
2. Check `POSTGRES_DSN` environment variable matches your setup (host, port, user, pass, dbname).
3. For external databases, ensure network accessibility
4. Verify credentials in environment variables

#### NATS Connection Issues

```sh
# Check if NATS is running
curl http://localhost:8222/varz

# Check streams
curl http://localhost:8222/jsz?streams=1
```

### Debugging

Enable debug logs by setting:

```sh
export LOG_LEVEL=debug
```

## Cleanup

```sh
# Stop and remove containers
make down
# or
docker-compose down

# Remove data volumes
docker-compose down -v

# Clean build artifacts
make clean
# or
rm -rf bin/
rm -rf ./.data
``` 