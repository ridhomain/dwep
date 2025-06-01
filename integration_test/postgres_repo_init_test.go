package integration_test

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid" // Added for unique tenant names
	"github.com/stretchr/testify/suite"

	// Import necessary packages, including your storage package
	_ "github.com/jackc/pgx/v5/stdlib" // PostgreSQL driver
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/storage"
)

// PostgresInitTestSuite defines the suite for repository initialization tests
// It embeds BaseIntegrationSuite to get the DB connection.
type PostgresInitTestSuite struct {
	BaseIntegrationSuite // Embed the base suite
	// No repo needed here as we are testing the NewPostgresRepo function itself
}

// SetupSuite and TearDownSuite are inherited from BaseIntegrationSuite.
// We might not need specific SetupTest/TearDownTest here unless we want
// to ensure a specific state before/after *each* init test.

// Helper function to connect to the database using stdlib
func connectDB(dsn string) (*sql.DB, error) {
	db, err := sql.Open("pgx", dsn) // Use the stdlib compatible driver name
	if err != nil {
		return nil, fmt.Errorf("failed to open DB connection: %w", err)
	}
	if err = db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping DB: %w", err)
	}
	return db, nil
}

// Helper function to check if a schema exists
func schemaExists(db *sql.DB, schemaName string) (bool, error) {
	var exists bool
	query := "SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = $1)"
	err := db.QueryRow(query, schemaName).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("failed to check schema existence for %s: %w", schemaName, err)
	}
	return exists, nil
}

// Helper function to check if a table exists within a specific schema
func tableExists(db *sql.DB, schemaName, tableName string) (bool, error) {
	var exists bool
	// Query needs to account for the specific schema
	query := "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2)"
	err := db.QueryRow(query, schemaName, tableName).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("failed to check table existence for %s.%s: %w", schemaName, tableName, err)
	}
	return exists, nil
}

// TestRunner runs the test suite
func TestPostgresInitSuite(t *testing.T) {
	suite.Run(t, new(PostgresInitTestSuite))
}

// --- Test Cases ---

// TestSchemaCreation verifies that NewPostgresRepo creates the tenant schema.
func (s *PostgresInitTestSuite) TestSchemaCreation() {
	ctx := s.Ctx
	testTenant := "init_test_schema_" + uuid.New().String()
	expectedSchema := "daisi_" + testTenant
	defaultDbDSN := s.PostgresDSN

	// 1. Call NewPostgresRepo. This should create the schema.
	repo, err := storage.NewPostgresRepo(defaultDbDSN, true, testTenant) // autoMigrate can be false for this test
	s.Require().NoError(err, "NewPostgresRepo failed for schema creation test")
	s.Require().NotNil(repo, "Repo should not be nil")
	defer repo.Close(ctx)

	// 2. Connect directly to the default DSN to check schema existence.
	db, err := connectDB(defaultDbDSN)
	s.Require().NoError(err, "Failed to connect to default DB for schema verification")
	defer db.Close()

	// 3. Verify schema exists
	exists, err := schemaExists(db, expectedSchema)
	s.Require().NoError(err, "Failed to check schema existence for %s", expectedSchema)
	s.Require().True(exists, "Schema '%s' should exist after NewPostgresRepo is called", expectedSchema)

	// 4. Cleanup: Drop the schema
	_, err = db.Exec(fmt.Sprintf("DROP SCHEMA IF EXISTS %q CASCADE", expectedSchema))
	s.Require().NoError(err, "Cleanup: Failed to drop test schema %s", expectedSchema)
}

// TestManualDDLExecution verifies that messages and contacts tables are created by manual DDL.
func (s *PostgresInitTestSuite) TestManualDDLExecution() {
	ctx := s.Ctx
	testTenant := "init_test_manual_ddl_" + uuid.New().String()
	testSchema := "daisi_" + testTenant
	defaultDbDSN := s.PostgresDSN

	// 1. Call NewPostgresRepo. This should create the schema and manual DDL tables.
	// We can set autoMigrate to false as we are not testing GORM's AutoMigrate here,
	// but the manual DDL execution part of NewPostgresRepo.
	repo, err := storage.NewPostgresRepo(defaultDbDSN, true, testTenant)
	s.Require().NoError(err, "NewPostgresRepo(autoMigrate=false) failed for manual DDL test")
	s.Require().NotNil(repo, "Repo should not be nil")
	defer repo.Close(ctx)

	// 2. Connect directly to the default DSN. Queries will specify schema.
	db, err := connectDB(defaultDbDSN)
	s.Require().NoError(err, "Failed to connect to default DB for manual DDL verification")
	defer db.Close()

	// 3. Verify tables created by manual DDL exist
	manualDDLTables := []string{"messages", "messages_default"}
	for _, tableName := range manualDDLTables {
		exists, err := tableExists(db, testSchema, tableName)
		s.Require().NoError(err, "Failed to check %s table existence in schema %s", tableName, testSchema)
		s.Require().True(exists, "Table '%s' should exist in schema '%s' due to manual DDL execution", tableName, testSchema)
	}

	// Optional: Verify indexes for the messages table
	// Example for one index. You might want to check more.
	// Note: Index names might need to match exactly how they are created in postgres.go
	// This part can be complex due to how GORM/pg driver report index names or existence.
	// For simplicity, we'll skip detailed index verification here but it's a good extension.
	// var indexExists bool
	// indexQuery := "SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = $1 AND tablename = 'messages' AND indexname = 'idx_messages_message_id')"
	// err = db.QueryRow(indexQuery, testSchema).Scan(&indexExists)
	// s.Require().NoError(err, "Failed to check for index idx_messages_message_id on messages table in schema %s", testSchema)
	// s.Require().True(indexExists, "Index 'idx_messages_message_id' should exist on 'messages' table in schema '%s'", testSchema)

	// 4. Cleanup: Drop the schema
	_, err = db.Exec(fmt.Sprintf("DROP SCHEMA IF EXISTS %q CASCADE", testSchema))
	s.Require().NoError(err, "Cleanup: Failed to drop test schema %s", testSchema)
}

// TestAutoMigrateTrue verifies tables are created/updated when autoMigrate is true.
func (s *PostgresInitTestSuite) TestAutoMigrateTrue() {
	ctx := s.Ctx // Use suite context

	testTenant := "init_test_automig_true_" + uuid.New().String()
	testSchema := "daisi_" + testTenant
	defaultDbDSN := s.PostgresDSN // Use suite DSN
	// Removed tenantDbDSN with search_path

	// 1. Call NewPostgresRepo with autoMigrate=true
	// This will create testSchema and run migrations.
	repo, err := storage.NewPostgresRepo(defaultDbDSN, true, testTenant)
	s.Require().NoError(err, "NewPostgresRepo(autoMigrate=true) failed")
	s.Require().NotNil(repo, "Repo should not be nil")
	defer repo.Close(ctx) // Close repo connection when done

	// 2. Connect directly to the default DSN. Queries will specify schema.
	db, err := connectDB(defaultDbDSN)
	s.Require().NoError(err, "Failed to connect to default DB for autoMigrate=true verification")
	defer db.Close()

	// 3. Verify tables managed by AutoMigrate exist
	autoMigrateTables := []string{"chats", "agents", "contacts", "onboarding_log", "exhausted_events", "messages"} // "messages" is also touched by AutoMigrate
	for _, tableName := range autoMigrateTables {
		exists, err := tableExists(db, testSchema, tableName)
		s.Require().NoError(err, "Failed to check %s table existence", tableName)
		s.Require().True(exists, "Table '%s' should exist in schema '%s' when autoMigrate=true", tableName, testSchema)
	}

	// 4. Verify manual DDL tables also exist (messages and its default partition)
	// messages_default is purely manual, messages is both manual (base) and auto-migrated (columns)
	manualDDLTables := []string{"messages_default"}
	for _, tableName := range manualDDLTables {
		exists, err := tableExists(db, testSchema, tableName)
		s.Require().NoError(err, "Failed to check %s table existence", tableName)
		s.Require().True(exists, "Table '%s' should still exist in schema '%s' when autoMigrate=true", tableName, testSchema)
	}

	// Cleanup: Drop the schema
	defaultDb, err := connectDB(defaultDbDSN)
	s.Require().NoError(err, "Cleanup: Failed to connect to default DB")
	defer defaultDb.Close()
	// Use %q for schema name
	_, err = defaultDb.Exec(fmt.Sprintf("DROP SCHEMA IF EXISTS %q CASCADE", testSchema))
	s.Require().NoError(err, "Cleanup: Failed to drop test schema")
}

// TestPartitionCreation verifies that necessary message partitions are created.
func (s *PostgresInitTestSuite) TestPartitionCreation() {
	ctx := s.Ctx
	testTenant := "init_test_partition_" + uuid.New().String()
	testSchema := "daisi_" + testTenant
	defaultDbDSN := s.PostgresDSN

	// 1. Call NewPostgresRepo. This should create partitions.
	// autoMigrate can be true or false, partition creation is independent of GORM AutoMigrate for these.
	repo, err := storage.NewPostgresRepo(defaultDbDSN, true, testTenant)
	s.Require().NoError(err, "NewPostgresRepo failed for partition creation test")
	s.Require().NotNil(repo, "Repo should not be nil")
	defer repo.Close(ctx)

	// 2. Connect directly to the default DSN to check partition existence.
	db, err := connectDB(defaultDbDSN)
	s.Require().NoError(err, "Failed to connect to default DB for partition verification")
	defer db.Close()

	// 3. Verify expected partitions exist
	now := time.Now().UTC()
	expectedPartitions := []string{
		fmt.Sprintf("messages_y%dm%02d", now.Year(), now.Month()),                                     // Current month
		fmt.Sprintf("messages_y%dm%02d", now.AddDate(0, -1, 0).Year(), now.AddDate(0, -1, 0).Month()), // Previous month
		fmt.Sprintf("messages_y%dm%02d", now.AddDate(0, 1, 0).Year(), now.AddDate(0, 1, 0).Month()),   // Next month
		"messages_default", // Default partition should also exist
	}

	for _, partitionName := range expectedPartitions {
		exists, err := tableExists(db, testSchema, partitionName)
		s.Require().NoError(err, "Failed to check partition/table existence for %s in schema %s", partitionName, testSchema)
		s.Require().True(exists, "Partition/Table '%s' should exist in schema '%s'", partitionName, testSchema)
	}

	// 4. Cleanup: Drop the schema
	_, err = db.Exec(fmt.Sprintf("DROP SCHEMA IF EXISTS %q CASCADE", testSchema))
	s.Require().NoError(err, "Cleanup: Failed to drop test schema %s", testSchema)
}

// Add more tests here for:
// - Partition creation check
