package integration_test

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"testing"
	"time"

	"gitlab.com/timkado/api/daisi-wa-events-processor/pkg/logger"

	_ "github.com/lib/pq"
	natsgo "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	tcnats "github.com/testcontainers/testcontainers-go/modules/nats"
	pgtc "github.com/testcontainers/testcontainers-go/modules/postgres"
	netlib "github.com/testcontainers/testcontainers-go/network"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
)

const DefaultCompanyID = "defaultcompanyid"
const (
	PostgresServiceName = "postgres"
	NatsServiceName     = "nats"
)

// SubjectToSchemaMap maps NATS subject patterns to JSON schema file names (without .json extension)
// Restored here for global accessibility within the package.
var SubjectToSchemaMap = map[string]string{
	"v1.chats.upsert":      "chat.upsert.schema",
	"v1.chats.update":      "chat.update.schema",
	"v1.history.chats":     "chat.history.schema",
	"v1.messages.upsert":   "message.upsert.schema",
	"v1.messages.update":   "message.update.schema",
	"v1.history.messages":  "message.history.schema",
	"v1.contacts.upsert":   "contact.upsert.schema",
	"v1.contacts.update":   "contact.update.schema",
	"v1.history.contacts":  "contact.history.schema",
	"v1.connection.update": "agent.upsert.schema", // Schema for agent upsert via v1.connection.update
	// Add other mappings as needed
}

// SubjectMapping maps logical event types (used in helper functions) to their corresponding NATS base subjects.
var SubjectMapping = map[string]string{
	"chat_created":    "v1.chats.upsert",
	"chat_updated":    "v1.chats.update",
	"history_chat":    "v1.history.chats",
	"message_created": "v1.messages.upsert",
	"message_updated": "v1.messages.update",
	"history_message": "v1.history.messages",
	"contact_created": "v1.contacts.upsert",
	"contact_updated": "v1.contacts.update",
	"history_contact": "v1.history.contacts",
	"agent_created":   "v1.connection.update", // Use base subject v1.connection.update for agent events
	// Add other mappings as needed
}

// --- Base Suite (Network, DB, NATS) ---

// BaseIntegrationSuite sets up the core infrastructure (Network, Postgres, NATS)
// Tests needing only DB/NATS can embed this suite.
type BaseIntegrationSuite struct {
	suite.Suite
	Network            *testcontainers.DockerNetwork
	Postgres           testcontainers.Container
	PostgresDSN        string
	PostgresDSNNetwork string // DSN for app container referencing network alias
	NATS               testcontainers.Container
	NATSURL            string
	NATSURLNetwork     string // URL for app container referencing network alias
	CompanyID          string
	CompanySchemaName  string
	Ctx                context.Context // Context for the suite
	cancel             context.CancelFunc
}

// SetupSuite runs once before the tests in the base suite are run.
func (s *BaseIntegrationSuite) SetupSuite() {
	s.Ctx, s.cancel = context.WithCancel(context.Background())
	log.Println("Setting up BaseIntegrationSuite...")
	logger.Log = zaptest.NewLogger(s.T()).Named("BaseIntegrationSuite")

	startTime := time.Now()
	var err error

	// Determine CompanyID
	s.CompanyID = os.Getenv("TEST_COMPANY_ID")
	if s.CompanyID == "" {
		s.CompanyID = DefaultCompanyID
		log.Println("TEST_COMPANY_ID not set, using default", zap.String("companyID", s.CompanyID))
	}

	// 1. Create Docker Network
	s.Network, err = createNetwork(s.Ctx)
	if err != nil {
		s.T().Fatalf("Failed to create network: %v", err)
	}
	networkName := s.Network.Name
	log.Printf("Docker network '%s' created.", networkName)

	// 2. Start PostgreSQL Container
	s.Postgres, s.PostgresDSN, err = startPostgres(s.Ctx, networkName, s.Network)
	if err != nil {
		s.T().Fatalf("Failed to start postgres: %v", err)
	}
	s.PostgresDSNNetwork = changeUriFromLocalhostToDocker(s.PostgresDSN, "pg")
	log.Println("PostgreSQL container started.")

	// 3. Start NATS Container
	s.NATS, s.NATSURL, err = startNATSContainer(s.Ctx, networkName, s.Network)
	if err != nil {
		s.T().Fatalf("Failed to start NATS: %v", err)
	}
	s.NATSURLNetwork = changeUriFromLocalhostToDocker(s.NATSURL, "nats")
	log.Println("NATS container started.")

	// 4. Set up PostgreSQL schema for the company
	s.CompanySchemaName = fmt.Sprintf("daisi_%s", s.CompanyID)
	schemaCreationQuery := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", s.CompanySchemaName)
	err = s.ExecuteNonQuery(s.Ctx, schemaCreationQuery)
	if err != nil {
		s.T().Fatalf("Failed to create schema %s: %v", s.CompanySchemaName, err)
	}

	log.Printf("BaseIntegrationSuite setup complete in %v", time.Since(startTime))
}

// TearDownSuite runs once after all tests in the base suite have finished.
func (s *BaseIntegrationSuite) TearDownSuite() {
	log.Println("Tearing down BaseIntegrationSuite...")
	startTime := time.Now()

	// Terminate containers in reverse order of creation
	if s.NATS != nil {
		log.Println("Terminating NATS container...")
		if err := s.NATS.Terminate(s.Ctx); err != nil {
			s.T().Logf("Error terminating NATS container: %v", err)
		}
	}

	if s.Postgres != nil {
		log.Println("Terminating PostgreSQL container...")
		if err := s.Postgres.Terminate(s.Ctx); err != nil {
			s.T().Logf("Error terminating PostgreSQL container: %v", err)
		}
	}

	if s.Network != nil {
		log.Println("Removing Docker network...")
		if err := s.Network.Remove(s.Ctx); err != nil {
			s.T().Logf("Error removing network: %v", err)
		}
	}

	if s.cancel != nil {
		s.cancel() // Cancel the context
	}

	log.Printf("BaseIntegrationSuite teardown complete in %v", time.Since(startTime))
}

// SetupTest runs before each test in the suite.
// It truncates PostgreSQL tables to ensure a clean state.
func (s *BaseIntegrationSuite) SetupTest() {
	log.Println("--- Running SetupTest: Truncating DB Tables --- ")
	err := truncatePostgresTables(s.Ctx, s.PostgresDSN, s.CompanySchemaName)
	s.Require().NoError(err, "Failed to truncate PostgreSQL tables")
	log.Println("--- SetupTest Complete --- ")
}

// --- E2E Suite (Includes Application Container) ---

// E2EIntegrationTestSuite embeds BaseIntegrationSuite and adds the application container.
type E2EIntegrationTestSuite struct {
	BaseIntegrationSuite
	Application testcontainers.Container
	AppAPIURL   string // Base URL for the application API
}

// SetupSuite runs once before the tests in the E2E suite are run.
func (s *E2EIntegrationTestSuite) SetupSuite() {
	log.Println("Setting up E2EIntegrationTestSuite...")
	startTime := time.Now()

	// 1. Setup Base Infrastructure (Network, DB, NATS)
	s.BaseIntegrationSuite.SetupSuite()

	// Sleep to ensure all containers are ready
	time.Sleep(2 * time.Second)

	// 2. Start Application Container
	var err error
	testEnvForApp := &TestEnvironment{ // Create a temporary struct matching setupTestApp's expectation
		Network:            s.Network,
		PostgresDSNNetwork: s.PostgresDSNNetwork,
		NATSURLNetwork:     s.NATSURLNetwork,
		CompanyID:          s.CompanyID,
	}
	s.Application, s.AppAPIURL, err = setupTestApp(s.Ctx, s.Network.Name, testEnvForApp, nil)
	if err != nil {
		s.StreamLogs(s.T())
		s.BaseIntegrationSuite.TearDownSuite() // Attempt base cleanup on failure
		s.T().Fatalf("Failed to start application container: %v", err)
	}
	// Start streaming app container logs
	s.StreamLogs(s.T())
	log.Println("Application container started.")

	log.Printf("E2EIntegrationTestSuite setup complete in %v", time.Since(startTime))
}

// TearDownSuite runs once after all tests in the E2E suite have finished.
func (s *E2EIntegrationTestSuite) TearDownSuite() {
	log.Println("Tearing down E2EIntegrationTestSuite...")
	startTime := time.Now()

	// 1. Terminate Application Container
	if s.Application != nil {
		log.Println("Terminating Application container...")
		// Optional: Stream logs just before termination
		// s.streamLogs(s.T())
		if err := s.Application.Terminate(s.Ctx); err != nil {
			s.T().Logf("Error terminating Application container: %v", err)
		}
	}

	// 2. Teardown Base Infrastructure (NATS, DB, Network)
	s.BaseIntegrationSuite.TearDownSuite()

	log.Printf("E2EIntegrationTestSuite teardown complete in %v", time.Since(startTime))
}

// StreamLogs streams the logs from the application container.
func (s *E2EIntegrationTestSuite) StreamLogs(t *testing.T) {
	if s.Application == nil {
		return
	}
	logs, err := s.Application.Logs(s.Ctx)
	if err != nil {
		t.Logf("Failed to stream logs: %v", err)
		return
	}
	go func() {
		defer logs.Close()
		scanner := bufio.NewScanner(logs)
		for scanner.Scan() {
			t.Logf("[app-log][%d] %s", time.Now().UnixMilli(), scanner.Text())
		}
	}()
}

// PublishEvent connects to NATS using the suite's NATSURL, validates the payload against
// the schema mapped from the subject, and publishes the event.
func (s *E2EIntegrationTestSuite) PublishEvent(ctx context.Context, subject string, payload []byte) error {
	// Use the NATS URL from the suite
	nc, err := natsgo.Connect(s.NATSURL, natsgo.Name(fmt.Sprintf("Integration Test Publisher - %s", s.T().Name())))
	if err != nil {
		return fmt.Errorf("suite failed to connect to NATS: %w", err)
	}
	defer nc.Close()

	// --- Payload Validation Start ---
	// Determine the base subject (remove tenant suffix if present)
	baseSubject := subject
	parts := strings.Split(subject, ".")
	if len(parts) > 1 {
		lastPart := parts[len(parts)-1]
		// Assuming tenant ID is the last part and might contain separators like tenantID or companyID
		if strings.Contains(lastPart, s.CompanyID) { // Check against suite's CompanyID
			baseSubject = strings.Join(parts[:len(parts)-1], ".")
		} else {
			// Fallback: check generic tenant/company pattern if direct match fails
			if strings.Contains(lastPart, "_tenant_") || strings.Contains(lastPart, "_company_") {
				baseSubject = strings.Join(parts[:len(parts)-1], ".")
			}
		}
	}

	// Re-enabled schema validation
	// s.T().Logf("WARN: Skipping schema validation for subject: %s (Base: %s)", subject, baseSubject)
	schemaName, ok := SubjectToSchemaMap[baseSubject]
	if !ok {
		// Using test logger for warnings/errors within tests is better
		s.T().Logf("Warning: No schema mapping found for base subject: %s (derived from %s). Skipping validation.", baseSubject, subject)
		// Decide if this should be a hard error or just a warning
		// return fmt.Errorf("no schema mapping found for base subject: %s (derived from %s)", baseSubject, subject)
	} else {
		// Assuming validatePayload exists in this package or is imported
		err = validatePayload(payload, schemaName)
		if err != nil {
			return fmt.Errorf("payload validation failed for subject %s (schema %s): %w", subject, schemaName, err)
		}
	}
	// --- Payload Validation End ---

	// Access JetStream context
	js, err := nc.JetStream()
	if err != nil {
		return fmt.Errorf("suite failed to access JetStream: %w", err)
	}

	// Publish message
	pubAck, err := js.Publish(subject, payload)
	if err != nil {
		return fmt.Errorf("suite failed to publish message to subject %s: %w", subject, err)
	}

	// Log successful publish details using test logger
	s.T().Logf("Published to %s (Seq: %d)", subject, pubAck.Sequence)

	// Removing the arbitrary sleep from here. Tests should manage their own waits.
	// time.Sleep(200 * time.Millisecond)

	return nil
}

// PublishEventWithoutValidation connects to NATS using the suite's NATSURL and publishes the event
// WITHOUT performing any payload validation against a schema.
func (s *E2EIntegrationTestSuite) PublishEventWithoutValidation(ctx context.Context, subject string, payload []byte) error {
	// Use the NATS URL from the suite
	nc, err := natsgo.Connect(s.NATSURL, natsgo.Name(fmt.Sprintf("Integration Test Publisher (No Validation) - %s", s.T().Name())))
	if err != nil {
		return fmt.Errorf("suite failed to connect to NATS: %w", err)
	}
	defer nc.Close()

	// --- Payload Validation SKIPPED ---
	s.T().Logf("Publishing to %s without schema validation.", subject)

	// Access JetStream context
	js, err := nc.JetStream()
	if err != nil {
		return fmt.Errorf("suite failed to access JetStream: %w", err)
	}

	// Publish message
	pubAck, err := js.Publish(subject, payload)
	if err != nil {
		return fmt.Errorf("suite failed to publish message to subject %s: %w", subject, err)
	}

	// Log successful publish details using test logger
	s.T().Logf("Published (no validation) to %s (Seq: %d)", subject, pubAck.Sequence)

	return nil
}

func (s *E2EIntegrationTestSuite) GenerateNatsPayload(subject string, overrides ...interface{}) ([]byte, error) {
	t := s.T()

	// 1. Determine Base Subject, Payload Type, and CompanyID from Subject
	var baseSubject string
	var companyIDFromSubject string

	// Iterate over known base subjects (e.g., "v1.chats.upsert") to find a prefix match.
	// subjectToPayloadMap keys serve this purpose well.
	for knownKey := range subjectToPayloadMap {
		if strings.HasPrefix(subject, knownKey+".") {
			baseSubject = knownKey
			companyIDFromSubject = strings.TrimPrefix(subject, knownKey+".")
			break
		} else if subject == knownKey { // Handles subjects without tenant IDs, if any
			baseSubject = knownKey
			break
		}
	}

	if baseSubject == "" {
		// Fallback: if no prefix match, check if the subject itself is a direct key (e.g. for non-tenant specific subjects)
		if _, ok := subjectToPayloadMap[subject]; ok {
			baseSubject = subject
		} else {
			return nil, fmt.Errorf("GenerateNatsPayload: could not determine base subject from: %s. Known prefixes: %v", subject, getMapKeys(subjectToPayloadMap))
		}
	}

	payloadName, ok := subjectToPayloadMap[baseSubject]
	if !ok {
		// This should ideally not be reached if baseSubject was determined correctly from map keys
		return nil, fmt.Errorf("GenerateNatsPayload: no payload mapping found for determined base subject: '%s' (derived from full subject: '%s')", baseSubject, subject)
	}

	initialOverrides := make(map[string]interface{})
	// companyIDFromSubject might be an actual ID like "defaultcompanyid" or "secondary-company-id-uuid"
	// The payload struct (e.g., UpsertChatPayload) has a CompanyID field.
	// We should ensure this is correctly passed if present in the subject and not overridden by user's map.
	if companyIDFromSubject != "" {
		// The key in overrides map should match the JSON tag or field name expected by generatePayloadStruct/mapstructure.
		// Assuming payload structs use `company_id` as json tag or `CompanyID` as field name.
		// Let's use "company_id" as it's common for JSON, and mapstructure uses `json` tag by default.
		initialOverrides["company_id"] = companyIDFromSubject
	}

	var finalOverrideArgument interface{}
	mergedMapOverrides := initialOverrides // Start with initial (e.g., company_id)

	var userNilOverrideKeys []string // Track keys explicitly set to nil by the user

	if len(overrides) > 0 && overrides[0] != nil {
		if overrideMap, okAsserted := overrides[0].(map[string]interface{}); okAsserted {
			// Merge user-provided map, and track nil values from it
			for k, v := range overrideMap {
				// Assuming user override keys are already snake_case from previous refactor
				mergedMapOverrides[k] = v
				if v == nil {
					userNilOverrideKeys = append(userNilOverrideKeys, k)
				}
			}
			finalOverrideArgument = mergedMapOverrides
		} else {
			// If override is not a map (e.g., a struct), pass it directly
			finalOverrideArgument = overrides[0]
		}
	} else {
		// No user overrides, just use initialOverrides (e.g., company_id)
		finalOverrideArgument = mergedMapOverrides
	}

	payloadStruct, err := generatePayloadStruct(payloadName, finalOverrideArgument)
	if err != nil {
		return nil, fmt.Errorf("failed to generate payload struct for %s: %w", payloadName, err)
	}

	payloadBytes, err := json.Marshal(payloadStruct)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal payload struct %s to JSON: %w", payloadName, err)
	}

	// Post-processing to ensure explicit null for user-specified nil overrides
	if len(userNilOverrideKeys) > 0 {
		var tempMap map[string]interface{}
		// Check if payloadBytes is "null" (e.g. if payloadStruct was nil)
		if string(payloadBytes) == "null" {
			tempMap = make(map[string]interface{})
		} else if err := json.Unmarshal(payloadBytes, &tempMap); err != nil {
			// This might happen if payloadBytes is not a valid JSON object (e.g. an empty string, or malformed)
			// For robustness, log or return error. Here, we return an error.
			return nil, fmt.Errorf("failed to unmarshal payload ('%s') for nil value post-processing: %w", string(payloadBytes), err)
		}

		modified := false
		for _, key := range userNilOverrideKeys {
			// Ensure the key is present in the map and its value is nil.
			// This handles cases where the key was omitted by `omitempty` or if it was present with a non-nil zero value.
			if currentValue, exists := tempMap[key]; !exists || currentValue != nil {
				tempMap[key] = nil
				modified = true
			}
		}

		if modified {
			payloadBytes, err = json.Marshal(tempMap)
			if err != nil {
				return nil, fmt.Errorf("failed to re-marshal payload after nil value post-processing: %w", err)
			}
		}
	}

	schemaName, schemaOk := SubjectToSchemaMap[baseSubject]
	if !schemaOk {
		t.Logf("Warning: No schema mapping found for base subject: %s (derived from %s). Skipping validation.", baseSubject, subject)
	} else {
		if err := validatePayload(payloadBytes, schemaName); err != nil {
			payloadStr := string(payloadBytes)
			if len(payloadStr) > 500 {
				payloadStr = payloadStr[:500] + "... (truncated)"
			}
			return payloadBytes, fmt.Errorf("payload validation failed for subject %s (schema %s): %w\nPayload: %s", subject, schemaName, err, payloadStr)
		}
	}

	return payloadBytes, nil
}

// Helper function to get keys from a map for error reporting
func getMapKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// --- Database Helper Methods (Moved from database_test.go) ---

// connectDB is a helper to open and ping a DB connection (internal use for helpers below).
// NOTE: This simple version opens/closes connections frequently. Consider managing
// a persistent *sql.DB handle within the suite (e.g., s.db) for better performance
// if these helpers are called very often.
func (s *BaseIntegrationSuite) connectDB(tenantDSN string) (*sql.DB, error) {
	db, err := sql.Open("postgres", tenantDSN)
	if err != nil {
		return nil, fmt.Errorf("connectDB: failed to open connection: %w", err)
	}

	ctx, cancel := context.WithTimeout(s.Ctx, 10*time.Second) // Use suite context
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		db.Close() // Close if ping fails
		return nil, fmt.Errorf("connectDB: failed to ping database: %w", err)
	}
	return db, nil
}

// VerifyPostgresData checks if a query returns the expected number of rows in the tenant's schema.
// expectedResults < 0 means check for existence (> 0 rows).
func (s *BaseIntegrationSuite) VerifyPostgresData(ctx context.Context, query string, expectedResults int, args ...interface{}) (bool, error) {
	db, err := s.connectDB(s.PostgresDSN) // Use internal helper
	if err != nil {
		return false, fmt.Errorf("VerifyPostgresData: %w", err)
	}
	defer db.Close()

	ctxQuery, cancel := context.WithTimeout(ctx, 15*time.Second) // Separate timeout for query
	defer cancel()

	rows, err := db.QueryContext(ctxQuery, query, args...)
	if err != nil {
		return false, fmt.Errorf("VerifyPostgresData: failed to execute query in %s: %w. Query: %s, Args: %v", s.CompanySchemaName, err, query, args)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		// If it's a COUNT query, scan directly into count
		if strings.Contains(strings.ToLower(query), "count(*)") {
			if scanErr := rows.Scan(&count); scanErr != nil {
				return false, fmt.Errorf("VerifyPostgresData: error scanning COUNT(*) result in %s: %w", s.CompanySchemaName, scanErr)
			}
			break // COUNT(*) returns only one row
		} else {
			// For other queries, just increment count for existence check
			var dummy interface{}
			if scanErr := rows.Scan(&dummy); scanErr != nil {
				// Log scan error but continue counting if just checking existence
				s.T().Logf("Warning: VerifyPostgresData: error scanning row in %s: %v", s.CompanySchemaName, scanErr)
			}
			count++
		}
	}

	if err = rows.Err(); err != nil {
		return false, fmt.Errorf("VerifyPostgresData: error iterating query results in %s: %w", s.CompanySchemaName, err)
	}

	if expectedResults < 0 {
		return count > 0, nil // Just check if any rows exist
	}

	return count == expectedResults, nil
}

// QueryRowScan executes a query expected to return one row in the tenant's schema and scans it.
func (s *BaseIntegrationSuite) QueryRowScan(ctx context.Context, query string, dest []interface{}, args ...interface{}) error {
	db, err := s.connectDB(s.PostgresDSN)
	if err != nil {
		return fmt.Errorf("QueryRowScan: %w", err)
	}
	defer db.Close()

	ctxQuery, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	err = db.QueryRowContext(ctxQuery, query, args...).Scan(dest...)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("QueryRowScan: no rows found in %s. Query: %s, Args: %v: %w", s.CompanySchemaName, query, args, err)
		}
		return fmt.Errorf("QueryRowScan: failed query/scan in %s: %w. Query: %s, Args: %v", s.CompanySchemaName, err, query, args)
	}
	return nil
}

// ExecuteNonQuery executes a SQL statement that doesn't return rows in the tenant's schema.
func (s *BaseIntegrationSuite) ExecuteNonQuery(ctx context.Context, query string, args ...interface{}) error {
	db, err := s.connectDB(s.PostgresDSN)
	if err != nil {
		return fmt.Errorf("ExecuteNonQuery: %w", err)
	}
	defer db.Close()

	ctxExec, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	_, err = db.ExecContext(ctxExec, query, args...)
	if err != nil {
		return fmt.Errorf("ExecuteNonQuery: failed query in %s: %w. Query: %s, Args: %v", s.CompanySchemaName, err, query, args)
	}
	return nil
}

// --- Test Runner Function ---

// TestRunE2ESuite is the entry point for running the E2E test suite.
func TestRunE2ESuite(t *testing.T) {
	suite.Run(t, new(E2EIntegrationTestSuite))
}

// You could add another function like this to run only the base suite if needed:
// func TestRunBaseSuite(t *testing.T) {
// 	suite.Run(t, new(BaseIntegrationSuite))
// }

// --- Helper Functions ---

func (s *BaseIntegrationSuite) StopService(ctx context.Context, serviceName string) error {
	stopDuration := 20 * time.Second
	switch serviceName {
	case PostgresServiceName:
		err := s.Postgres.Stop(ctx, &stopDuration)
		if err != nil {
			return err
		}
		return nil
	case NatsServiceName:
		err := s.NATS.Stop(ctx, &stopDuration)
		if err != nil {
			return err
		}
		return nil
	default:
		return fmt.Errorf("unknown service name to stop: %s", serviceName)
	}
}

// StartService starts a specific service in the test environment's docker-compose setup
func (s *BaseIntegrationSuite) StartService(ctx context.Context, serviceName string) error {
	switch serviceName {
	case PostgresServiceName:
		err := s.Postgres.Start(ctx)
		if err != nil {
			return err
		}
		err = s.UpdatePostgresDSN(ctx)
		if err != nil {
			return fmt.Errorf("failed to update Postgres DSN: %w", err)
		}
		return nil
	case NatsServiceName:
		err := s.NATS.Start(ctx)
		if err != nil {
			return err
		}
		err = s.UpdateNatsUrl(ctx)
		if err != nil {
			return fmt.Errorf("failed to update Nats Url: %w", err)
		}
		return nil
	default:
		return fmt.Errorf("unknown service name to start: %s", serviceName)
	}
}

func (s *BaseIntegrationSuite) UpdatePostgresDSN(ctx context.Context) error {
	if pg, ok := s.Postgres.(*pgtc.PostgresContainer); ok {
		// Get the new DSN from the container
		newDSN, err := pg.ConnectionString(ctx, "sslmode=disable")
		if err != nil {
			return fmt.Errorf("failed to get new Postgres DSN: %w", err)
		}
		s.PostgresDSN = newDSN
		return nil
	}
	return fmt.Errorf("Postgres container is not of type pgtc.PostgresContainer")
}

func (s *BaseIntegrationSuite) UpdateNatsUrl(ctx context.Context) error {
	if nts, ok := s.NATS.(*tcnats.NATSContainer); ok {
		// Get the new DSN from the container
		newDSN, err := nts.ConnectionString(ctx)
		if err != nil {
			return fmt.Errorf("failed to get new NATS Url: %w", err)
		}
		s.NATSURL = newDSN
		return nil
	}
	return fmt.Errorf("Postgres container is not of type pgtc.PostgresContainer")
}

// TestEnvironment holds the shared infrastructure for integration tests.
// Note: This is now mainly used internally by setupTestApp
type TestEnvironment struct {
	Network            *testcontainers.DockerNetwork
	PostgresDSN        string
	PostgresDSNNetwork string
	NATSURL            string
	NATSURLNetwork     string
	CompanyID          string
}

// createNetwork creates a Docker network for the test environment.
func createNetwork(ctx context.Context) (*testcontainers.DockerNetwork, error) {
	dockerNetwork, err := netlib.New(ctx, netlib.WithAttachable())
	if err != nil {
		return nil, err
	}

	return dockerNetwork, nil
}

// changeUriFromLocalhostToDocker converts localhost URIs to network alias URIs for containers.
func changeUriFromLocalhostToDocker(uri, networkAlias string) string {
	// remove localhost from the URI and replace it with the network alias
	// change the port to the default port for the service
	// Example: "postgres://postgres:postgres@localhost:51475/message_service?sslmode=disable" -> "postgres://postgres:postgres@pg:5432/message_service?sslmode=disable"
	// Example: "nats://localhost:51484" -> "nats://nats:4222"
	replacements := map[string]struct {
		defaultPort string
	}{
		"pg":   {defaultPort: "5432"},
		"nats": {defaultPort: "4222"},
	}

	if !strings.Contains(uri, "localhost") {
		return uri // Nothing to replace
	}

	var serviceType string
	if strings.HasPrefix(uri, "postgres://") {
		serviceType = "pg"
	} else if strings.HasPrefix(uri, "nats://") {
		serviceType = "nats"
	} else {
		return uri // Unknown URI scheme
	}

	if serviceType == networkAlias {
		cfg, ok := replacements[networkAlias]
		if !ok {
			return uri // Should not happen if serviceType is set correctly
		}
		uri = strings.Replace(uri, "localhost", networkAlias, 1) // Replace only host part
		uri = replacePort(uri, cfg.defaultPort)
	}

	return uri
}

// replacePort replaces the port in a URI string.
func replacePort(uri, newPort string) string {
	// Find the start of the host part (after //)
	hostStart := strings.Index(uri, "//")
	if hostStart == -1 {
		return uri // Invalid URI format
	}
	hostStart += 2 // Move past //

	// Find the end of the host/port part (before path / or query ?)
	pathStart := strings.Index(uri[hostStart:], "/")
	queryStart := strings.Index(uri[hostStart:], "?")
	hostEnd := len(uri)
	if pathStart != -1 {
		hostEnd = hostStart + pathStart
	}
	if queryStart != -1 && (pathStart == -1 || hostStart+queryStart < hostEnd) {
		hostEnd = hostStart + queryStart
	}

	// Find the last colon in the host/port part, which indicates the port separator
	portColon := strings.LastIndex(uri[hostStart:hostEnd], ":")
	if portColon == -1 {
		// No port found, cannot replace
		return uri
	}
	portColon += hostStart // Adjust index relative to the full URI

	// Construct the new URI
	return uri[:portColon+1] + newPort + uri[hostEnd:]
}

// --- Placeholder for Helper Functions ---
// (Will add validatePayload and other helpers here as needed based on the plan)
