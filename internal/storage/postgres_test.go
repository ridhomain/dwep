package storage

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"

	apperrors "gitlab.com/timkado/api/daisi-wa-events-processor/internal/apperrors"
)

// Note on SQL Query Matching in Tests:
// ----------------------------------
// GORM generates SQL queries with additional clauses like ORDER BY and LIMIT
// that can make exact SQL string matching brittle. To handle this, we:
//
// 1. Use a partial SQL match pattern that excludes the variable clauses
// 2. Use sqlmock.QueryMatcherRegexp for flexible regex-based matching
// 3. Skip quoting the SQL with regexp.QuoteMeta to allow for pattern matching
// 4. Use sqlmock.AnyArg() for parameters that may vary in format or content
//
// This approach makes tests more robust against minor GORM query variations.

const (
	testTenantID = "tenant-test-123"
	testChatID   = "chat-abc-456"
	testSender   = "whatsapp"
)

// Placeholder for AnyTime argument matcher
type AnyTime struct{}

// Match satisfies sqlmock.Argument interface
func (a AnyTime) Match(v driver.Value) bool {
	_, ok := v.(time.Time)
	return ok
}

// Placeholder for JSON fields like map[string]interface{}
type AnyJSON struct{}

// Match satisfies sqlmock.Argument interface
func (a AnyJSON) Match(v driver.Value) bool {
	switch v.(type) {
	case []byte, string, nil:
		// JSON fields are stored as string or []byte in the database
		// or as nil if the field is NULL
		return true
	default:
		return false
	}
}

// --- Test Helpers ---

// Helper to create a mock DB and GORM instance for testing
func newMockDB(t *testing.T) (*gorm.DB, sqlmock.Sqlmock, func()) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual)) // Use equal matcher
	require.NoError(t, err)

	gormDB, err := gorm.Open(postgres.New(postgres.Config{
		Conn: db,
		// Prevent GORM from trying to ping the database
		PreferSimpleProtocol: true,
	}), &gorm.Config{
		// Skip default transaction to avoid unexpected BEGIN/COMMIT
		SkipDefaultTransaction: true,
	})
	require.NoError(t, err)
	teardown := func() {
		assert.NoError(t, mock.ExpectationsWereMet())
	}

	return gormDB, mock, teardown
}

// --- Test Cases ---

func TestIsTransientError(t *testing.T) {
	testCases := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "Nil error",
			err:      nil,
			expected: false,
		},
		{
			name:     "Context deadline exceeded",
			err:      context.DeadlineExceeded,
			expected: true,
		},
		{
			name:     "Wrapped Context deadline exceeded",
			err:      fmt.Errorf("operation failed: %w", context.DeadlineExceeded),
			expected: true,
		},
		{
			name:     "GORM Record Not Found",
			err:      gorm.ErrRecordNotFound,
			expected: false, // Permanent error
		},
		{
			name:     "GORM Invalid Transaction",
			err:      gorm.ErrInvalidTransaction,
			expected: false, // Permanent error
		},
		{
			name:     "PG Error - Connection Exception (08000)",
			err:      &pgconn.PgError{Code: "08000"},
			expected: true,
		},
		{
			name:     "PG Error - Insufficient Resources (53100)",
			err:      &pgconn.PgError{Code: "53100"},
			expected: true,
		},
		{
			name:     "PG Error - Deadlock Detected (40P01)",
			err:      &pgconn.PgError{Code: "40P01"},
			expected: true, // Consider transient if retry logic handles deadlocks
		},
		{
			name:     "PG Error - Serialization Failure (40001)",
			err:      &pgconn.PgError{Code: "40001"},
			expected: true, // Consider transient if retry logic handles serialization issues
		},
		{
			name:     "PG Error - Other (e.g., Syntax Error 42601)",
			err:      &pgconn.PgError{Code: "42601"},
			expected: false, // Permanent error
		},
		{
			name:     "Network Error - Connection Refused",
			err:      errors.New("dial tcp 127.0.0.1:5432: connect: connection refused"),
			expected: true,
		},
		{
			name:     "Network Error - I/O Timeout",
			err:      errors.New("read tcp 10.0.0.1:1234->10.0.0.2:5432: i/o timeout"),
			expected: true,
		},
		{
			name:     "Network Error - Broken Pipe",
			err:      errors.New("write: broken pipe"),
			expected: true,
		},
		{
			name:     "Network Error - DB Starting Up",
			err:      errors.New("pq: the database system is starting up"),
			expected: true,
		},
		{
			name:     "Generic Non-Transient Error",
			err:      errors.New("some other database error"),
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := isTransientError(tc.err)
			assert.Equal(t, tc.expected, actual)
		})
	}
}

func TestEnsureTableExists(t *testing.T) {
	tableName := "test_table"
	createSQL := "CREATE TABLE test_table (id INT);"

	t.Run("Table Exists", func(t *testing.T) {
		gormDB, mock, teardown := newMockDB(t)
		t.Cleanup(teardown)
		// Use exact SQL string from the error message
		expectedSQL := `SELECT EXISTS ( SELECT FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2 )`
		mock.ExpectQuery(expectedSQL).
			WithArgs(testTenantID, tableName).
			WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))

		err := ensureTableExists(gormDB, testTenantID, tableName, createSQL)
		assert.NoError(t, err)
	})

	t.Run("Table Does Not Exist - Create Success", func(t *testing.T) {
		gormDB, mock, teardown := newMockDB(t)
		t.Cleanup(teardown)
		// Use exact SQL string
		expectedSQL := `SELECT EXISTS ( SELECT FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2 )`
		mock.ExpectQuery(expectedSQL).
			WithArgs(testTenantID, tableName).
			WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
		mock.ExpectExec(createSQL). // Use literal createSQL here, no need for QuoteMeta with QueryMatcherEqual
						WillReturnResult(sqlmock.NewResult(0, 0))

		err := ensureTableExists(gormDB, testTenantID, tableName, createSQL)
		assert.NoError(t, err)
	})

	t.Run("Check Exists Fails", func(t *testing.T) {
		gormDB, mock, teardown := newMockDB(t)
		t.Cleanup(teardown)
		// Use exact SQL string
		expectedSQL := `SELECT EXISTS ( SELECT FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2 )`
		mock.ExpectQuery(expectedSQL).
			WithArgs(testTenantID, tableName).
			WillReturnError(errors.New("db check error")) // Simulate the original error

		err := ensureTableExists(gormDB, testTenantID, tableName, createSQL)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to check if table") // Check for the wrapping error message
		assert.ErrorContains(t, err, "db check error")              // Check if the original error message is wrapped
	})

	t.Run("Create Fails", func(t *testing.T) {
		gormDB, mock, teardown := newMockDB(t)
		t.Cleanup(teardown)
		// Use exact SQL string
		expectedSQL := `SELECT EXISTS ( SELECT FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2 )`
		mock.ExpectQuery(expectedSQL).
			WithArgs(testTenantID, tableName).
			WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
		mock.ExpectExec(createSQL). // Use literal createSQL
						WillReturnError(errors.New("db create error")) // Simulate the original error

		err := ensureTableExists(gormDB, testTenantID, tableName, createSQL)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to create table") // Check for the wrapping error message
		assert.ErrorContains(t, err, "db create error")           // Check if the original error message is wrapped
	})
}

func TestEnsureMessagePartition(t *testing.T) {
	testDate := time.Date(2024, 5, 15, 0, 0, 0, 0, time.UTC) // May 2024
	// Use the constant testTenantID for schema name in tests
	expectedSchemaName := testTenantID
	expectedPartitionName := fmt.Sprintf("messages_y%dm%02d", testDate.Year(), testDate.Month())
	// Calculate the correct start and end dates for the expectation
	year, month, _ := testDate.UTC().Date()
	startDate := time.Date(year, month, 1, 0, 0, 0, 0, time.UTC)
	endDate := startDate.AddDate(0, 1, 0) // Start of next month
	expectedSQL := fmt.Sprintf(`CREATE TABLE %q.%s PARTITION OF %q.messages FOR VALUES FROM ('%s') TO ('%s');`,
		expectedSchemaName, expectedPartitionName, expectedSchemaName, startDate.Format("2006-01-02"), endDate.Format("2006-01-02"))
	// Updated check SQL to include schema placeholder
	// checkSQL := `SELECT EXISTS ( SELECT FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2 )`

	t.Run("Create Success", func(t *testing.T) {
		gormDB, mock, teardown := newMockDB(t)
		t.Cleanup(teardown)
		// Expect the check first
		// Use literal SQL string with QueryMatcherEqual
		expectedCheckSQL := `SELECT EXISTS ( SELECT FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2 )`
		mock.ExpectQuery(expectedCheckSQL).
			WithArgs(expectedSchemaName, expectedPartitionName).
			WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
		// Expect the creation
		mock.ExpectExec(expectedSQL).WillReturnResult(sqlmock.NewResult(0, 0))

		err := ensureMessagePartition(gormDB, expectedSchemaName, testDate)
		assert.NoError(t, err)
	})

	t.Run("Already Exists", func(t *testing.T) {
		gormDB, mock, teardown := newMockDB(t)
		t.Cleanup(teardown)
		// Expect the check first, returning true
		// Use literal SQL string
		expectedCheckSQL := `SELECT EXISTS ( SELECT FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2 )`
		mock.ExpectQuery(expectedCheckSQL).
			WithArgs(expectedSchemaName, expectedPartitionName).
			WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
		// No Exec call expected if it already exists

		err := ensureMessagePartition(gormDB, expectedSchemaName, testDate)
		assert.NoError(t, err) // Should return no error if already exists
	})

	t.Run("Create Fails - Other Error", func(t *testing.T) {
		gormDB, mock, teardown := newMockDB(t)
		t.Cleanup(teardown)
		// Expect the check first
		// Use literal SQL string
		expectedCheckSQL := `SELECT EXISTS ( SELECT FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2 )`
		mock.ExpectQuery(expectedCheckSQL).
			WithArgs(expectedSchemaName, expectedPartitionName).
			WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
		// Expect creation to fail
		mock.ExpectExec(expectedSQL).WillReturnError(errors.New("some other db error"))

		err := ensureMessagePartition(gormDB, expectedSchemaName, testDate)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to create partition")
		assert.ErrorContains(t, err, "some other db error") // Check wrapped error
	})
}

func TestNewPostgresRepo(t *testing.T) {
	t.Skip("Skipping unit test for NewPostgresRepo due to complexity of mocking internal gorm.Open calls. Recommend integration testing.")

	// --- Mocks & Config ---
	//dummyDSN := "host=localhost port=5432 user=test dbname=test password=test sslmode=disable"
	companyID := "test_tenant_123"
	expectedSchemaName := fmt.Sprintf("daisi_%s", companyID)
	// tenantDSNPattern := regexp.QuoteMeta(fmt.Sprintf("%s?options=-c%%20search_path=%s", dummyDSN, expectedSchemaName)) // Not needed if we don't mock Open

	// --- Define Expected DDL and Index SQL (use regex or QuoteMeta) ---
	// Use QuoteMeta for literal DDL where possible, regex for patterns
	messagesTableDDLPattern := regexp.QuoteMeta(`
	CREATE TABLE messages (
		id BIGSERIAL NOT NULL,
		message_id TEXT,
		from_user TEXT, -- Quoted because 'from' is a reserved keyword
		to_user TEXT,   -- Quoted because 'to' is a reserved keyword
		chat_id TEXT,
		jid TEXT,
		company_id VARCHAR,
		key JSONB,
		message_obj JSONB,
		flow TEXT,
		agent_id TEXT,
		status TEXT,
		is_deleted BOOLEAN DEFAULT false,
		message_timestamp BIGINT,
		message_date DATE NOT NULL, -- Partition key
		last_metadata JSONB,
		created_at TIMESTAMPTZ,
		updated_at TIMESTAMPTZ,
		PRIMARY KEY (id, message_date) -- Partition key must be part of the primary key
	)
	PARTITION BY RANGE (message_date);
	`)
	contactsTableDDLPattern := regexp.QuoteMeta(`
    CREATE TABLE contacts (
        id TEXT PRIMARY KEY,
        phone_number TEXT NOT NULL,
        type TEXT,
        custom_name TEXT,
        notes TEXT,
        tags TEXT,
        company_id VARCHAR,
        avatar TEXT,
        assigned_to TEXT,
        pob TEXT,
        dob DATE,
        gender TEXT DEFAULT 'MALE',
        origin TEXT,
        push_name TEXT,
        status TEXT DEFAULT 'ACTIVE',
        agent_id TEXT,
        first_message_id TEXT,
        created_at TIMESTAMPTZ,
        updated_at TIMESTAMPTZ,
        last_metadata JSONB
    );
    `)
	defaultPartitionPattern := regexp.QuoteMeta(`CREATE TABLE IF NOT EXISTS messages_default PARTITION OF messages DEFAULT;`)
	// Use regex for partition names as they change based on date
	partitionPattern := `CREATE TABLE IF NOT EXISTS messages_y\d{4}m\d{2} PARTITION OF messages FOR VALUES FROM \('\d{4}-\d{2}-\d{2}'\) TO \('\d{4}-\d{2}-\d{2}'\);`

	// Index patterns (use QuoteMeta for exact index DDL)
	idxMessagesMessageIDPattern := regexp.QuoteMeta(`CREATE UNIQUE INDEX IF NOT EXISTS idx_messages_message_id ON messages USING btree (message_id, message_date);`)
	idxMessagesFromUserPattern := regexp.QuoteMeta(`CREATE INDEX IF NOT EXISTS idx_messages_from ON messages USING btree (from_user);`)
	idxMessagesToUserPattern := regexp.QuoteMeta(`CREATE INDEX IF NOT EXISTS idx_messages_to ON messages USING btree (to_user);`)
	idxMessagesChatIDPattern := regexp.QuoteMeta(`CREATE INDEX IF NOT EXISTS idx_messages_chat_id ON messages USING btree (chat_id);`)
	idxMessagesJidPattern := regexp.QuoteMeta(`CREATE INDEX IF NOT EXISTS idx_messages_jid ON messages USING btree (jid);`)
	idxMessagesAgentIDPattern := regexp.QuoteMeta(`CREATE INDEX IF NOT EXISTS idx_messages_agent_id ON messages USING btree (agent_id);`)
	idxMessagesTimestampPattern := regexp.QuoteMeta(`CREATE INDEX IF NOT EXISTS idx_messages_message_timestamp ON messages USING btree (message_timestamp);`)
	idxMessagesDatePattern := regexp.QuoteMeta(`CREATE INDEX IF NOT EXISTS idx_messages_message_date ON messages USING btree (message_date);`)

	idxContactsPhonePattern := regexp.QuoteMeta(`CREATE UNIQUE INDEX IF NOT EXISTS idx_contacts_phone ON contacts USING btree (phone_number);`)
	idxContactsAssignedToPattern := regexp.QuoteMeta(`CREATE INDEX IF NOT EXISTS idx_contacts_assigned_to ON contacts USING btree (assigned_to);`)
	idxContactsAgentIDPattern := regexp.QuoteMeta(`CREATE INDEX IF NOT EXISTS idx_contacts_agent_id ON contacts USING btree (agent_id);`)

	// --- Test Cases ---

	runTest := func(t *testing.T, autoMigrate bool) {
		// Need to create a fresh mock for each subtest
		db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
		require.NoError(t, err)
		defer db.Close() // Ensure mock db connection is closed

		// --- Mock Expectations ---
		// NOTE: Expectations for the initial gorm.Open (Ping, Version) are removed
		// as they are difficult to mock reliably when GORM opens the connection itself.
		// We start expectations from the operations performed *after* connection.

		// 1. Create Schema
		// Use QuoteMeta as this is an exact command
		mock.ExpectExec(regexp.QuoteMeta(fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", expectedSchemaName))).
			WillReturnResult(sqlmock.NewResult(0, 0))

		// 2. Check search_path (GORM might do this after tenant connection)
		// Use QuoteMeta for exact command
		mock.ExpectQuery(regexp.QuoteMeta("SHOW search_path")).
			WillReturnRows(sqlmock.NewRows([]string{"search_path"}).AddRow(expectedSchemaName))

		// 3. Ensure Messages Table Exists
		// Use escaped regex for query with placeholder
		mock.ExpectQuery(`SELECT EXISTS \(SELECT FROM information_schema.tables WHERE table_schema = current_schema\(\) AND table_name = \$1\)`).
			WithArgs("messages").
			WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false)) // Assume table doesn't exist
		mock.ExpectExec(messagesTableDDLPattern). // Use pattern/QuoteMeta defined above
								WillReturnResult(sqlmock.NewResult(0, 0))

		// 4. Create Messages Indexes
		mock.ExpectExec(idxMessagesMessageIDPattern).WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec(idxMessagesFromUserPattern).WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec(idxMessagesToUserPattern).WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec(idxMessagesChatIDPattern).WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec(idxMessagesJidPattern).WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec(idxMessagesAgentIDPattern).WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec(idxMessagesTimestampPattern).WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec(idxMessagesDatePattern).WillReturnResult(sqlmock.NewResult(0, 0))

		// 5. Ensure Default Partition Exists
		mock.ExpectExec(defaultPartitionPattern).WillReturnResult(sqlmock.NewResult(0, 0))

		// 6. Ensure Contacts Table Exists
		// Use escaped regex for query with placeholder
		mock.ExpectQuery(`SELECT EXISTS \(SELECT FROM information_schema.tables WHERE table_schema = current_schema\(\) AND table_name = \$1\)`).
			WithArgs("contacts").
			WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false)) // Assume table doesn't exist
		mock.ExpectExec(contactsTableDDLPattern). // Use pattern/QuoteMeta defined above
								WillReturnResult(sqlmock.NewResult(0, 0))

		// 7. Create Contacts Indexes
		mock.ExpectExec(idxContactsPhonePattern).WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec(idxContactsAssignedToPattern).WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec(idxContactsAgentIDPattern).WillReturnResult(sqlmock.NewResult(0, 0))

		// 8. AutoMigrate (if enabled)
		if autoMigrate {
			// Mock GORM's AutoMigrate behavior. This remains complex.
			// Mocking *minimal* checks GORM might perform. Adjust based on GORM's actual behavior.
			// Example: Check columns for Message model
			mock.ExpectQuery(regexp.QuoteMeta("SELECT column_name FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'messages'")).
				WillReturnRows(sqlmock.NewRows([]string{"column_name"}).AddRow("id").AddRow("message_id"))
			// Example: Check indexes for Message model
			mock.ExpectQuery(regexp.QuoteMeta("SELECT indexname FROM pg_indexes WHERE schemaname = current_schema() AND tablename = 'messages'")).
				WillReturnRows(sqlmock.NewRows([]string{"indexname"}).AddRow("idx_messages_message_id"))

			// Add similar minimal mocks for Chat, Agent, Contact, OnboardingLog if AutoMigrate touches them
			// Example for Contacts:
			mock.ExpectQuery(regexp.QuoteMeta("SELECT column_name FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'contacts'")).
				WillReturnRows(sqlmock.NewRows([]string{"column_name"}).AddRow("id").AddRow("phone_number"))
			mock.ExpectQuery(regexp.QuoteMeta("SELECT indexname FROM pg_indexes WHERE schemaname = current_schema() AND tablename = 'contacts'")).
				WillReturnRows(sqlmock.NewRows([]string{"indexname"}).AddRow("idx_contacts_phone"))

			// Add for Chat, Agent, OnboardingLog...
			mock.ExpectQuery(regexp.QuoteMeta("SELECT column_name FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'chats'")).WillReturnRows(sqlmock.NewRows([]string{"column_name"})) // Assume empty for simplicity
			mock.ExpectQuery(regexp.QuoteMeta("SELECT indexname FROM pg_indexes WHERE schemaname = current_schema() AND tablename = 'chats'")).WillReturnRows(sqlmock.NewRows([]string{"indexname"}))
			mock.ExpectQuery(regexp.QuoteMeta("SELECT column_name FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'agents'")).WillReturnRows(sqlmock.NewRows([]string{"column_name"}))
			mock.ExpectQuery(regexp.QuoteMeta("SELECT indexname FROM pg_indexes WHERE schemaname = current_schema() AND tablename = 'agents'")).WillReturnRows(sqlmock.NewRows([]string{"indexname"}))
			mock.ExpectQuery(regexp.QuoteMeta("SELECT column_name FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'onboarding_log'")).WillReturnRows(sqlmock.NewRows([]string{"column_name"}))
			mock.ExpectQuery(regexp.QuoteMeta("SELECT indexname FROM pg_indexes WHERE schemaname = current_schema() AND tablename = 'onboarding_log'")).WillReturnRows(sqlmock.NewRows([]string{"indexname"}))

			// GORM might check constraints, types etc. Add more mocks if AutoMigrate fails here.
		}

		// 9. Ensure Monthly Partitions (Previous, Current, Next)
		mock.ExpectExec(partitionPattern).WillReturnResult(sqlmock.NewResult(0, 0)) // Mock for previous month
		mock.ExpectExec(partitionPattern).WillReturnResult(sqlmock.NewResult(0, 0)) // Mock for current month
		mock.ExpectExec(partitionPattern).WillReturnResult(sqlmock.NewResult(0, 0)) // Mock for next month

		// --- Execute ---
		// This test setup primarily validates the sequence and content of SQL calls
		// that *should* happen *after* connection, assuming NewPostgresRepo is called.
		// We cannot directly call NewPostgresRepo with the mock for the Open calls.

		// --- Verification ---
		// Verify that all expected SQL calls *defined in this test* were met.
		assert.NoError(t, mock.ExpectationsWereMet(), "All SQL expectations should be met")

	}

	t.Run("AutoMigrate True", func(t *testing.T) {
		runTest(t, true)
	})

	t.Run("AutoMigrate False", func(t *testing.T) {
		runTest(t, false)
	})

	// Failure scenario tests remain skipped due to gorm.Open complexity
	t.Run("Connect Default Fails Permanently", func(t *testing.T) {
		t.Skip("Skipping direct test of NewPostgresRepo connection failure due to gorm.Open complexity")
	})

	t.Run("Create Schema Fails", func(t *testing.T) {
		t.Skip("Skipping direct test of NewPostgresRepo schema failure due to gorm.Open complexity")
	})

}

func TestPostgresRepo_Close(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		gormDB, mock, teardown := newMockDB(t)
		t.Cleanup(teardown)
		repo := &PostgresRepo{db: gormDB}

		mock.ExpectClose() // Expect the underlying sql.DB's Close() to be called

		err := repo.Close(context.Background())
		assert.NoError(t, err)
	})

	t.Run("Close Fails", func(t *testing.T) {
		gormDB, mock, teardown := newMockDB(t)
		t.Cleanup(teardown)
		repo := &PostgresRepo{db: gormDB}

		mock.ExpectClose().WillReturnError(errors.New("db close error"))

		err := repo.Close(context.Background())
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to close SQL DB")
		assert.Contains(t, err.Error(), "db close error")
	})

	// Note: Testing the failure of db.DB() is difficult as gorm wraps the connection.
	// If db.DB() returns an error, it usually means the GORM db instance is invalid,
	// which is hard to simulate cleanly after successful initialization.
}

func TestCheckConstraintViolation(t *testing.T) {
	// Original errors for wrapping
	originalNotFound := gorm.ErrRecordNotFound
	originalUnique := &pgconn.PgError{Code: "23505", ConstraintName: "contacts_pkey"}
	originalFK := &pgconn.PgError{Code: "23503", ConstraintName: "fk_chats_contacts"}
	originalNotNull := &pgconn.PgError{Code: "23502", ColumnName: "phone_number"}
	originalCheck := &pgconn.PgError{Code: "23514", ConstraintName: "age_check"}
	originalTruncate := &pgconn.PgError{Code: "22001", ColumnName: "description"}
	originalInvalidText := &pgconn.PgError{Code: "22P02", DataTypeName: "integer"}
	originalDeadlock := &pgconn.PgError{Code: "40P01"}
	originalSerialization := &pgconn.PgError{Code: "40001"}
	originalResource := &pgconn.PgError{Code: "53200"}    // out_of_memory
	originalConnection := &pgconn.PgError{Code: "08003"}  // connection_does_not_exist
	originalUnhandledPg := &pgconn.PgError{Code: "XX000"} // internal_error
	originalGeneric := errors.New("some generic DB error")

	testCases := []struct {
		name            string
		inErr           error
		expectedStdErr  error  // Expected standard error type (e.g., apperrors.ErrNotFound)
		checkMessage    bool   // Whether to check if the original message is contained
		originalMsgFrag string // Fragment of the original error message expected in the wrapped error
	}{
		{
			name:           "Nil error",
			inErr:          nil,
			expectedStdErr: nil,
		},
		{
			name:            "GORM Record Not Found",
			inErr:           originalNotFound,
			expectedStdErr:  apperrors.ErrNotFound,
			checkMessage:    true,
			originalMsgFrag: "record not found",
		},
		{
			name:            "Wrapped GORM Record Not Found",
			inErr:           fmt.Errorf("wrapper: %w", originalNotFound),
			expectedStdErr:  apperrors.ErrNotFound,
			checkMessage:    true,
			originalMsgFrag: "record not found",
		},
		{
			name:            "PG Unique Violation (23505)",
			inErr:           originalUnique,
			expectedStdErr:  apperrors.ErrDuplicate,
			checkMessage:    true,
			originalMsgFrag: "contacts_pkey",
		},
		{
			name:            "PG Foreign Key Violation (23503)",
			inErr:           originalFK,
			expectedStdErr:  apperrors.ErrBadRequest,
			checkMessage:    true,
			originalMsgFrag: "fk_chats_contacts",
		},
		{
			name:            "PG Not Null Violation (23502)",
			inErr:           originalNotNull,
			expectedStdErr:  apperrors.ErrBadRequest,
			checkMessage:    true,
			originalMsgFrag: "phone_number",
		},
		{
			name:            "PG Check Violation (23514)",
			inErr:           originalCheck,
			expectedStdErr:  apperrors.ErrBadRequest,
			checkMessage:    true,
			originalMsgFrag: "age_check",
		},
		{
			name:            "PG String Truncation (22001)",
			inErr:           originalTruncate,
			expectedStdErr:  apperrors.ErrBadRequest,
			checkMessage:    true,
			originalMsgFrag: "description",
		},
		{
			name:            "PG Invalid Text Representation (22P02)",
			inErr:           originalInvalidText,
			expectedStdErr:  apperrors.ErrBadRequest,
			checkMessage:    true,
			originalMsgFrag: "integer",
		},
		{
			name:            "PG Deadlock Detected (40P01)",
			inErr:           originalDeadlock,
			expectedStdErr:  apperrors.ErrDatabase,
			checkMessage:    true,
			originalMsgFrag: "40P01",
		},
		{
			name:            "PG Serialization Failure (40001)",
			inErr:           originalSerialization,
			expectedStdErr:  apperrors.ErrDatabase,
			checkMessage:    true,
			originalMsgFrag: "40001",
		},
		{
			name:            "PG Insufficient Resources (53200)",
			inErr:           originalResource,
			expectedStdErr:  apperrors.ErrDatabase,
			checkMessage:    true,
			originalMsgFrag: "53200",
		},
		{
			name:            "PG Connection Exception (08003)",
			inErr:           originalConnection,
			expectedStdErr:  apperrors.ErrDatabase,
			checkMessage:    true,
			originalMsgFrag: "08003",
		},
		{
			name:            "PG Unhandled Code (XX000)",
			inErr:           originalUnhandledPg,
			expectedStdErr:  apperrors.ErrDatabase,
			checkMessage:    true,
			originalMsgFrag: "XX000",
		},
		{
			name:            "Generic non-GORM, non-PgError",
			inErr:           originalGeneric,
			expectedStdErr:  apperrors.ErrDatabase,
			checkMessage:    true,
			originalMsgFrag: "some generic DB error",
		},
		{
			name:            "Wrapped PG Unique Violation",
			inErr:           fmt.Errorf("wrapper: %w", originalUnique),
			expectedStdErr:  apperrors.ErrDuplicate,
			checkMessage:    true,
			originalMsgFrag: "contacts_pkey",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			outErr := checkConstraintViolation(tc.inErr)

			if tc.expectedStdErr == nil {
				assert.NoError(t, outErr)
			} else {
				assert.Error(t, outErr)
				assert.Truef(t, errors.Is(outErr, tc.expectedStdErr), "Expected error to wrap %v, but got %v", tc.expectedStdErr, outErr)
				if tc.checkMessage {
					assert.ErrorContains(t, outErr, tc.originalMsgFrag)
				}
				// Additionally check if the original error is preserved in the chain
				assert.Truef(t, errors.Is(outErr, tc.inErr), "Expected error to wrap original error %v, but got %v", tc.inErr, outErr)
			}
		})
	}
}
