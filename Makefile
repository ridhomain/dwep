# Daisi WA Events Processor Makefile

# Variables
APP_NAME := daisi-wa-events-processor
VERSION := $(shell git describe --tags --always --dirty || echo "dev")
DOCKER_REGISTRY := 
DOCKER_IMAGE := $(DOCKER_REGISTRY)$(APP_NAME)
DOCKER_TAG := $(VERSION)

# Go related variables
GOBASE := $(shell pwd)
GOBIN := $(GOBASE)/bin
GO_FILES := $(shell find . -name "*.go" -type f -not -path "./vendor/*")

.PHONY: all build clean test docker-build docker-push up down restart logs help dev

# Default target
all: help

# Build the application binary
build:
	@echo "Building $(APP_NAME)..."
	@go build -o $(GOBIN)/$(APP_NAME) ./cmd/main/main.go

# Clean build artifacts
clean:
	@echo "Cleaning..."
	@rm -rf $(GOBIN)
	@rm -rf ./.data
	@echo "Done."

# Run tests
test:
	@echo "Running tests..."
	@go test -v ./...

# Run benchmark tests for handlers
.PHONY: benchmark-handlers
benchmark-handlers:
	@echo "Running handler benchmark tests..."
	@go test -bench=. -benchmem -run=^$ ./internal/ingestion/handler/...

# Build Docker image
docker-build:
	@echo "Building Docker image: $(DOCKER_IMAGE):$(DOCKER_TAG)"
	docker build -t $(DOCKER_IMAGE):$(DOCKER_TAG) .
	@echo "Tagging as latest"
	docker tag $(DOCKER_IMAGE):$(DOCKER_TAG) $(DOCKER_IMAGE):latest

# Push Docker image to registry
docker-push:
	@echo "Pushing Docker image to registry: $(DOCKER_IMAGE):$(DOCKER_TAG)"
	docker push $(DOCKER_IMAGE):$(DOCKER_TAG)
	docker push $(DOCKER_IMAGE):latest

# Start the local development environment
up:
	@echo "Starting development environment..."
	docker-compose up -d

# Stop the local development environment
down:
	@echo "Stopping development environment..."
	docker-compose down

# Restart the local development environment
restart: down up

# Show logs from docker-compose services
logs:
	docker-compose logs -f

# Run the application in a Docker container
run: docker-build
	@echo "Running $(APP_NAME) in Docker..."
	docker run --rm -p 8080:8080 --name $(APP_NAME) $(DOCKER_IMAGE):$(DOCKER_TAG)

# Initialize the necessary infrastructure
init:
	@echo "Creating data directories..."
	@mkdir -p ./.data/nats
	@mkdir -p ./.data/postgres
	@mkdir -p ./.data/mongodb
	@echo "Starting infrastructure services..."
	docker-compose up -d nats postgres mongodb

# Generate Go code (e.g., from protobuf)
generate:
	@echo "Generating code..."
	@go generate ./...

# Build for all supported platforms
build-all:
	GOOS=linux GOARCH=amd64 go build -o $(GOBIN)/$(APP_NAME)-linux-amd64 ./cmd/main/main.go
	GOOS=darwin GOARCH=amd64 go build -o $(GOBIN)/$(APP_NAME)-darwin-amd64 ./cmd/main/main.go
	GOOS=darwin GOARCH=arm64 go build -o $(GOBIN)/$(APP_NAME)-darwin-arm64 ./cmd/main/main.go

# Build and start the service with docker-compose
dev: docker-build
	@echo "Starting full development environment with application..."
	@docker compose -f docker-compose.yaml up -d

# Build the tester application
.PHONY: build-tester
build-tester:
	@echo "Building tester application..."
	@go build -o $(GOBIN)/tester ./cmd/tester/main.go

# Run the tester application (assumes it has been built)
# Pass arguments after --, e.g., make run-tester -- --rate=200 --duration=30s
.PHONY: run-tester
run-tester: build-tester
	@echo "Running tester application..."
	@$(GOBIN)/tester $(filter-out $@,$(MAKECMDGOALS))

# --- Load Test Targets ---

# Build image (needed for load test apps)
.PHONY: loadtest-build
loadtest-build: docker-build

# Start the load test environment
.PHONY: loadtest-up
loadtest-up: loadtest-build
	@echo "Starting load test environment with 3 replicas per tenant app..."
	@docker compose -f docker-compose.loadtest.yaml up -d --scale tenant_app_A=3 --scale tenant_app_B=3

# Stop the load test environment
.PHONY: loadtest-down
loadtest-down:
	@echo "Stopping load test environment..."
	@docker compose -f docker-compose.loadtest.yaml down

# Restart the load test environment
.PHONY: loadtest-restart
loadtest-restart: loadtest-down loadtest-up

# Show logs from load test services
.PHONY: loadtest-logs
loadtest-logs:
	@docker compose -f docker-compose.loadtest.yaml logs -f

# --- Help Target ---

# Show help
help:
	@echo "Daisi WA Events Processor - Makefile targets:"
	@echo ""
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@echo "  build         Build the application binary"
	@echo "  clean         Clean build artifacts"
	@echo "  test          Run tests"
	@echo "  docker-build  Build Docker image"
	@echo "  docker-push   Push Docker image to registry"
	@echo "  up            Start the local development environment"
	@echo "  down          Stop the local development environment"
	@echo "  restart       Restart the local development environment"
	@echo "  logs          Show logs from docker-compose services"
	@echo "  run           Run the application in a Docker container"
	@echo "  init          Initialize the necessary infrastructure"
	@echo "  generate      Generate Go code"
	@echo "  build-all     Build for all supported platforms"
	@echo "  dev           Build Docker image and start full environment with docker-compose.yaml"
	@echo "  loadtest-build Build Docker image (prerequisite for load test)"
	@echo "  loadtest-up   Start load test environment using docker-compose.loadtest.yaml (scales tenant apps to 3 replicas)"
	@echo "  loadtest-down Stop load test environment"
	@echo "  loadtest-restart Restart load test environment"
	@echo "  loadtest-logs Show logs from load test services"
	@echo "  build-tester  Build the tester application"
	@echo "  run-tester    Build and run the tester application (pass args after --)"
	@echo "  benchmark-handlers Run benchmark tests for ingestion handlers"
	@echo "  help          Show this help message" 