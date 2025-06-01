# Troubleshooting Guide

This document provides solutions for common issues encountered when working with the Daisi WA Events Processor.

## 1. Database Connection Issues

### 1.1 PostgreSQL Connection Refused

**Symptoms:**
- `connect: connection refused` error in logs
- `dial tcp: lookup postgres: no such host` error
- Service crashes during startup with PostgreSQL errors

**Possible Causes:**
- PostgreSQL container not started or healthy
- Incorrect port mapping
- Wrong hostname resolution

**Solutions:**
1. Check if PostgreSQL is fully started and healthy:
   ```bash
   docker ps | grep postgres
   docker logs postgres-container
   ```

2. Verify DSN format and port mapping:
   ```
   # Correct format for Docker Compose
   POSTGRES_DSN=postgres://postgres:postgres@postgres:5432/message_service
   
   # For local development with mapped port
   POSTGRES_DSN=postgres://postgres:postgres@localhost:54321/message_service
   ```

3. Implement connection retry with backoff in application code:
   ```go
   // Example from internal/storage/postgres.go
   for i := 0; i < maxRetries; i++ {
     db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
     if err == nil {
       return db, nil
     }
     time.Sleep(time.Second * time.Duration(i+1))
   }
   ```

4. Check network connectivity:
   ```bash
   telnet localhost 5432
   ```

## 2. Message Queue Issues

### 2.1 NATS Connection Error

**Symptoms:**
- `dial tcp connection refused` error for NATS
- `nats: no servers available for connection` error in logs
- Events not being processed

**Possible Causes:**
- NATS server not running
- Incorrect URL or port
- Network connectivity issues
- Wrong container name resolution

**Solutions:**
1. Ensure NATS container is running:
   ```bash
   docker ps | grep nats
   ```

2. Use the correct NATS URL format:
   ```
   # For Docker Compose
   NATS_URL=nats://nats:4222
   
   # For local development
   NATS_URL=nats://localhost:4222
   ```

3. Check NATS server logs:
   ```bash
   docker logs nats-container
   ```

4. Verify NATS server status via HTTP monitoring interface:
   ```bash
   curl http://localhost:8222/varz
   ```

5. Test connectivity with the NATS CLI tool:
   ```bash
   nats-sub -s nats://localhost:4222 ">"
   ```

### 2.2 NATS Stream Not Found

**Symptoms:**
- `stream not found` error
- Events not being processed
- Service starts but fails to process messages

**Possible Causes:**
- Stream not created
- Incorrect stream name in configuration
- JetStream not enabled on NATS server

**Solutions:**
1. Check if JetStream is enabled:
   ```bash
   curl http://localhost:8222/jsz
   ```

2. Verify stream setup:
   ```bash
   nats stream ls
   nats stream info message_events_stream
   ```

3. Create stream manually if needed:
   ```bash
   nats stream add message_events_stream --subjects="v1.>" --storage=file --retention=limits --max-msgs=100000 --max-age=24h --replicas=1
   ```

### 2.3 Event Not Being Processed

**Symptoms:**
- Messages published but not appearing in database
- No processing logs for certain events
- No error logs

**Possible Causes:**
- Subject mismatch
- Handler not registered for event type
- Invalid payload schema

**Solutions:**
1. Check if proper handlers are registered:
   ```go
   // In internal/usecase/processor.go
   p.eventRouter.Register(model.V1MessagesUpsert, p.realtimeHandler.HandleEvent)
   ```

2. Verify subject format includes version prefix:
   ```bash
   # Example with correct versioning
   go run cmd/tester/main.go -type messages.upsert -version v1 -tenant tenant_abc
   ```

3. Enable debug logging and check handler matching:
   ```
   LOG_LEVEL=debug go run cmd/main/main.go
   ```

## 3. Integration Test Issues

### 3.1 Integration Tests Not Showing Logs

**Symptoms:**
- Tests fail without useful error messages
- No container logs visible in test output

**Possible Causes:**
- Verbose flag not enabled
- Log capturing not implemented
- Test panic before logs are captured

**Solutions:**
1. Run tests with verbose flag:
   ```bash
   go test -v
   ```

2. Implement proper log capturing in tests:
   ```go
   t.Cleanup(func() {
     logs, err := container.Logs(ctx)
     if err == nil {
       defer logs.Close()
       content, _ := io.ReadAll(logs)
       t.Logf("Container logs: %s", content)
     }
   })
   ```

3. Use `env.StreamLogs(ctx, t)` to see logs in real-time during test execution.

### 3.2 TestContainers Not Cleaning Up

**Symptoms:**
- Docker containers remain after tests
- Port conflicts in subsequent test runs
- System resources exhausted over time

**Possible Causes:**
- Missing termination calls
- Test panic before cleanup
- Manual test interruption

**Solutions:**
1. Use `defer` to ensure cleanup:
   ```go
   container, err := postgres.RunContainer(ctx)
   if err != nil {
     t.Fatalf("Failed to start postgres: %s", err)
   }
   defer container.Terminate(ctx)
   ```

2. Use test cleanup function:
   ```go
   postgresContainer, err := postgres.RunContainer(ctx)
   t.Cleanup(func() {
     if err := postgresContainer.Terminate(ctx); err != nil {
       t.Logf("Failed to terminate container: %s", err)
     }
   })
   ```

3. Clean up containers manually if needed:
   ```bash
   docker ps -a | grep testcontainers | awk '{print $1}' | xargs docker rm -f
   ```

### 3.3 Common Port Conflicts

**Symptoms:**
- `bind: address already in use` errors
- Tests fail with port-related errors
- Multiple services trying to use the same port

**Possible Causes:**
- Hardcoded port numbers
- Previous test containers not cleaned up
- Local services using required ports

**Solutions:**
1. Use dynamic port allocation:
   ```go
   natsMappedPort, err := natsContainer.MappedPort(ctx, "4222/tcp")
   if err != nil {
     t.Fatalf("Failed to get mapped port: %s", err)
   }
   natsURL := fmt.Sprintf("nats://localhost:%s", natsMappedPort.Port())
   ```

2. Check for processes using required ports:
   ```bash
   lsof -i :4222
   lsof -i :5432
   lsof -i :27017
   ```

3. Kill processes if needed:
   ```bash
   kill -9 $(lsof -t -i:4222)
   ```

## 4. Event Schema and Validation Issues

### 4.1 Event Validation Failures

**Symptoms:**
- `validation error` messages in logs
- Events not processed due to schema errors
- Inconsistent validation behavior

**Possible Causes:**
- Incorrect JSON payload format
- Missing required fields
- Incompatible data types

**Solutions:**
1. Check JSON schema requirements in `internal/model/payload.go`:
   ```go
   type UpsertMessagePayload struct {
     ID               string                 `json:"id,omitempty" validate:"required"`
     // Other fields...
   }
   ```

2. Validate your payload against examples in documentation:
   ```json
   {
     "id": "uuid:message-test001",
     "jid": "6281234567890@s.whatsapp.net",
     "message_type": "conversation",
     // Other fields...
   }
   ```

3. Use the event tester tool to generate valid payloads:
   ```bash
   go run cmd/tester/main.go -type messages.upsert -tenant tenant_abc
   ```

### 4.2 Event Versioning Issues

**Symptoms:**
- `unhandled event type` errors
- Events not routing to correct handlers
- Inconsistent event processing

**Possible Causes:**
- Missing version prefix
- Handler not registered for version
- Incorrect version format

**Solutions:**
1. Ensure events include proper version prefix:
   ```bash
   # Using the tester tool with version
   go run cmd/tester/main.go -type messages.upsert -version v1
   ```

2. Check handler registration in `processor.Setup()`:
   ```go
   p.eventRouter.Register(model.V1MessagesUpsert, p.realtimeHandler.HandleEvent)
   ```

3. Use the eventType helper methods to ensure proper versioning:
   ```go
   // Get the version of an event
   version := eventType.GetVersion() // Returns "v1" for "v1.messages.upsert"
   
   // Get the base type without version
   baseType := eventType.GetBaseType() // Returns "messages.upsert" for "v1.messages.upsert"
   ```

## 5. General Troubleshooting Steps

### 5.1 Service Startup Issues

**Symptoms:**
- Service fails to start
- Unexpected panics during initialization
- Configuration-related errors

**Solutions:**
1. Check service logs:
   ```bash
   docker logs daisi-wa-events-processor
   ```

2. Verify environment variables:
   ```bash
   docker exec daisi-wa-events-processor env | grep POSTGRES
   docker exec daisi-wa-events-processor env | grep MONGO
   docker exec daisi-wa-events-processor env | grep NATS
   ```

3. Run service with debug logging:
   ```bash
   LOG_LEVEL=debug go run cmd/main/main.go
   ```

### 5.2 Debugging Tools

1. **Health Check Endpoint**:
   ```bash
   curl http://localhost:8080/health
   curl http://localhost:8080/ready
   ```

2. **Metrics Endpoint** (if enabled):
   ```bash
   curl http://localhost:8080/metrics
   ```

3. **NATS Monitoring**:
   ```bash
   # Get NATS server information
   curl http://localhost:8222/varz
   
   # Get JetStream info
   curl http://localhost:8222/jsz
   
   # Get stream information
   curl http://localhost:8222/jsz?streams=1
   ```

4. **Database Inspection**:
   ```bash
   # PostgreSQL
   docker exec -it postgres-container psql -U postgres -d message_service -c "SELECT COUNT(*) FROM messages;"
   ```

### 5.3 Graceful Shutdown Issues

**Symptoms:**
- Service doesn't shut down cleanly
- Resource leaks after service restart
- Stuck connections

**Solutions:**
1. Use proper signal handling:
   ```bash
   # Send SIGTERM for graceful shutdown
   kill -TERM $(pgrep message-event)
   ```

2. Increase shutdown timeout if needed (in `cmd/main/main.go`).

3. Check for stuck goroutines:
   ```go
   go func() {
     http.ListenAndServe(":6060", nil) // pprof debugging
   }()
   ```

## 6. Common Error Messages and Solutions

| Error Message | Possible Cause | Solution |
|---------------|----------------|----------|
| `connect: connection refused` | PostgreSQL not started or wrong port | Check if DB is fully started, correct port mapping |
| `dial tcp ... connection refused` | NATS server not available | Check if NATS container is running and correct alias/port |
| `stream not found` | JetStream stream not created | Create required stream (see NATS Stream Not Found section) |
| `validation error: field X is required` | Missing required fields in payload | Check schema in payload.go and fix the JSON payload |
| `unhandled event type` | Event router can't find handler | Check if handler is registered for event type |
| `no such host` | DNS resolution issue in Docker network | Ensure services are on same network, check container names |
| `context deadline exceeded` | Operation timeout | Increase timeout or check for performance issues |

## 7. Advanced Troubleshooting

### 7.1 Performance Issues

**Symptoms:**
- Slow message processing
- High CPU or memory usage
- Processing queue backlog

**Solutions:**
1. Enable Prometheus metrics and monitor resource usage:
   ```bash
   curl http://localhost:8080/metrics | grep events_
   ```

2. Check database query performance:
   ```sql
   EXPLAIN ANALYZE SELECT * FROM messages_tenant_abc WHERE message_id = 'msg-123';
   ```

3. Increase JetStream performance settings:
   ```bash
   nats stream edit message_events_stream --max-msgs-per-subject=10000 --max-bytes=1GB
   ```

### 7.2 Diagnostics Commands

Here are some useful commands for diagnosing issues:

1. **Check service status:**
   ```bash
   docker ps --format "table {{.Names}}\t{{.Status}}" | grep message
   ```

2. **List NATS streams and consumers:**
   ```bash
   nats stream ls
   nats consumer ls message_events_stream
   ```

3. **Monitor NATS messages in real-time:**
   ```bash
   nats sub "v1.>"
   ```

4. **View NATS consumer information:**
   ```bash
   nats consumer info message_events_stream tenant_abc_events_consumer
   ```

5. **Check PostgreSQL table sizes:**
   ```bash
   docker exec -it postgres-container psql -U postgres -d message_service -c "SELECT pg_size_pretty(pg_total_relation_size(relid)) AS size, relname FROM pg_catalog.pg_statio_user_tables ORDER BY pg_total_relation_size(relid) DESC;"
   ``` 