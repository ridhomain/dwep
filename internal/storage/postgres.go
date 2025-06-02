package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/apperrors"

	"github.com/cenkalti/backoff/v4"
	"github.com/jackc/pgx/v5/pgconn"
	"go.uber.org/zap"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"

	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/model"
	"gitlab.com/timkado/api/daisi-wa-events-processor/pkg/logger"
)

// --- Retry Logic Configuration ---
const (
	defaultRetryInitialInterval = 50 * time.Millisecond
	defaultRetryMaxInterval     = 2 * time.Second
	defaultRetryMaxElapsedTime  = 10 * time.Second
	readRetryMaxElapsedTime     = 5 * time.Second  // More aggressive for reads
	commitRetryMaxElapsedTime   = 15 * time.Second // More tolerant for commits
)

// Setup publication + slot for company
func (r *PostgresRepo) setupCDCForCompany(ctx context.Context, schemaName string) error {
	logger.Log.Info("Setting up CDC for company schema", zap.String("schema", schemaName))

	// 1. Set REPLICA IDENTITY FULL for all tables in this schema
	replicaIdentitySQL := fmt.Sprintf(`
    DO $$
    DECLARE
        tbl_name TEXT;
        partition_rec RECORD;
    BEGIN
        -- Set REPLICA IDENTITY FULL for core tables
        FOREACH tbl_name IN ARRAY ARRAY['chats', 'agents', 'contacts', 'messages']
        LOOP
            IF EXISTS (
                SELECT 1 FROM information_schema.tables 
                WHERE table_schema = '%s' 
                AND table_name = tbl_name
            ) THEN
                EXECUTE format('ALTER TABLE %%I.%%I REPLICA IDENTITY FULL', '%s', tbl_name);
            END IF;
        END LOOP;
        
        -- Handle message partitions
        FOR partition_rec IN
            SELECT tablename 
            FROM pg_tables 
            WHERE schemaname = '%s' 
            AND tablename LIKE 'messages_%%'
        LOOP
            EXECUTE format('ALTER TABLE %%I.%%I REPLICA IDENTITY FULL', '%s', partition_rec.tablename);
        END LOOP;
    END $$;
    `, schemaName, schemaName, schemaName, schemaName)

	if err := r.db.WithContext(ctx).Exec(replicaIdentitySQL).Error; err != nil {
		logger.Log.Error("Failed to set replica identity",
			zap.String("schema", schemaName),
			zap.Error(err))
		return fmt.Errorf("failed to set replica identity: %w", err)
	}

	// 2. Create shared publication if it doesn't exist
	createPublicationSQL := `
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM pg_publication 
            WHERE pubname = 'daisi_pub'
        ) THEN
            CREATE PUBLICATION daisi_pub;
        END IF;
    END $$;
    `

	if err := r.db.WithContext(ctx).Exec(createPublicationSQL).Error; err != nil {
		logger.Log.Error("Failed to create shared publication", zap.Error(err))
		return fmt.Errorf("failed to create publication: %w", err)
	}

	// 3. Add this company's tables to the shared publication
	addTablesToPublicationSQL := fmt.Sprintf(`
    DO $$
    DECLARE
        tbl_name TEXT;
        full_table_name TEXT;
        partition_rec RECORD;
    BEGIN
        -- Add core tables to publication
        FOREACH tbl_name IN ARRAY ARRAY['chats', 'agents', 'contacts', 'messages']
        LOOP
            IF EXISTS (
                SELECT 1 FROM information_schema.tables 
                WHERE table_schema = '%s' 
                AND table_name = tbl_name
            ) THEN
                full_table_name := quote_ident('%s') || '.' || quote_ident(tbl_name);
                BEGIN
                    EXECUTE format('ALTER PUBLICATION daisi_pub ADD TABLE %%s', full_table_name);
                EXCEPTION WHEN duplicate_object THEN
                    NULL; -- Table already in publication, that's fine
                END;
            END IF;
        END LOOP;
        
        -- Add message partitions to publication
        FOR partition_rec IN
            SELECT tablename 
            FROM pg_tables 
            WHERE schemaname = '%s' 
            AND tablename LIKE 'messages_%%'
        LOOP
            full_table_name := quote_ident('%s') || '.' || quote_ident(partition_rec.tablename);
            BEGIN
                EXECUTE format('ALTER PUBLICATION daisi_pub ADD TABLE %%s', full_table_name);
            EXCEPTION WHEN duplicate_object THEN
                NULL; -- Partition already in publication
            END;
        END LOOP;
    END $$;
    `, schemaName, schemaName, schemaName, schemaName)

	if err := r.db.WithContext(ctx).Exec(addTablesToPublicationSQL).Error; err != nil {
		logger.Log.Error("Failed to add tables to publication",
			zap.String("schema", schemaName),
			zap.Error(err))
		return fmt.Errorf("failed to add tables to publication: %w", err)
	}

	logger.Log.Info("CDC setup completed for company",
		zap.String("schema", schemaName),
		zap.String("publication", "daisi_pub"))
	return nil
}

// func (r *PostgresRepo) setupCDCForPartition(ctx context.Context, schemaName string, partitionName string) error {
// 	// Set REPLICA IDENTITY FULL for the partition
// 	replicaSQL := fmt.Sprintf("ALTER TABLE %q.%s REPLICA IDENTITY FULL", schemaName, partitionName)
// 	if err := r.db.WithContext(ctx).Exec(replicaSQL).Error; err != nil {
// 		return fmt.Errorf("failed to set replica identity for partition: %w", err)
// 	}

// 	// Add partition to publication
// 	addToPublicationSQL := fmt.Sprintf(`
//         ALTER PUBLICATION daisi_pub ADD TABLE %q.%s
//     `, schemaName, partitionName)

// 	if err := r.db.WithContext(ctx).Exec(addToPublicationSQL).Error; err != nil {
// 		// Ignore duplicate errors
// 		if !strings.Contains(err.Error(), "duplicate") {
// 			return fmt.Errorf("failed to add partition to publication: %w", err)
// 		}
// 	}

// 	return nil
// }

// newRetryPolicy creates a new exponential backoff policy with context awareness.
func newRetryPolicy(ctx context.Context, maxElapsedTime time.Duration) backoff.BackOffContext {
	b := backoff.NewExponentialBackOff()
	b.InitialInterval = defaultRetryInitialInterval
	b.MaxInterval = defaultRetryMaxInterval
	b.MaxElapsedTime = maxElapsedTime
	b.Reset() // Important: Reset before first use
	return backoff.WithContext(b, ctx)
}

// retryableOperation wraps a database operation with retry logic.
func retryableOperation(ctx context.Context, policy backoff.BackOffContext, opName string, operation func() error) error {
	notify := func(err error, d time.Duration) {
		logger.FromContext(ctx).Warn("Retrying DB operation",
			zap.String("operation", opName),
			zap.Error(err),
			zap.Duration("after", d),
		)
	}

	err := backoff.RetryNotify(func() error {
		err := operation()
		if err != nil {
			// Check for non-retryable errors first
			if errors.Is(err, gorm.ErrRecordNotFound) ||
				errors.Is(err, gorm.ErrInvalidTransaction) ||
				errors.Is(err, gorm.ErrDuplicatedKey) || // Assuming TranslateError=true
				errors.Is(err, gorm.ErrForeignKeyViolated) { // Assuming TranslateError=true
				return backoff.Permanent(err) // Don't retry these GORM errors
			}
			// Check for potentially transient errors
			if isTransientError(err) {
				return err // Retry transient errors
			}
			// Treat other errors as permanent by default
			return backoff.Permanent(err)
		}
		return nil // Success
	}, policy, notify)

	return err
}

// isTransientError checks if the error suggests a temporary issue like a network problem.
func isTransientError(err error) bool {
	if err == nil {
		return false
	}

	// Check for context deadline exceeded, often indicates a timeout
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	// Use specific pg driver error checks if possible (example for pgx/v5)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		// Add specific PostgreSQL error codes that indicate transient issues
		// See https://www.postgresql.org/docs/current/errcodes-appendix.html
		// Class 08 — Connection Exception
		// Class 53 — Insufficient Resources
		if strings.HasPrefix(pgErr.Code, "08") ||
			strings.HasPrefix(pgErr.Code, "53") ||
			strings.HasPrefix(pgErr.Code, "40P01") ||
			strings.HasPrefix(pgErr.Code, "40001") {
			return true // Retry connection and resource errors
		}
		// Consider 40P01 (Deadlock) or 40001 (Serialization Failure) if tx logic handles retries appropriately
	}

	// Fallback to string matching for common network-related errors
	errStr := strings.ToLower(err.Error())
	transientIndicators := []string{
		"connection refused",
		"network is unreachable",
		"i/o timeout",
		"broken pipe",
		"connection reset by peer",
		"could not translate host name",
		"no route to host",
		"database system is starting up", // Might occur during failover/restart
		"connection timed out",
		"connection reset", // Generic reset indicator
		// Add other specific error messages from your pg driver if needed
	}
	for _, indicator := range transientIndicators {
		if strings.Contains(errStr, indicator) {
			return true
		}
	}

	return false
}

// PostgresRepo implements repositories for messages and chats
type PostgresRepo struct {
	db *gorm.DB
}

// ensureTableExists checks if a table exists and creates it using the provided SQL DDL if it doesn't.
func ensureTableExists(db *gorm.DB, schemaName string, tableName string, createTableSQL string) error {
	var exists bool
	// Explicitly check within the target schema
	checkSQL := `
		SELECT EXISTS (
			SELECT FROM information_schema.tables
			WHERE table_schema = ? AND table_name = ?
		)`
	err := db.Raw(checkSQL, schemaName, tableName).Scan(&exists).Error
	if err != nil {
		return fmt.Errorf("failed to check if table %s exists in schema %s: %w", tableName, schemaName, err)
	}

	if !exists {
		logger.Log.Info("Table does not exist, creating table", zap.String("tableName", tableName), zap.String("schema", schemaName))
		// Create table relies on search_path being set correctly for the session
		if err := db.Exec(createTableSQL).Error; err != nil {
			return fmt.Errorf("failed to create table %s in schema %s: %w", tableName, schemaName, err)
		}
		logger.Log.Info("Successfully created table", zap.String("tableName", tableName), zap.String("schema", schemaName))
	} else {
		logger.Log.Debug("Table already exists", zap.String("tableName", tableName), zap.String("schema", schemaName))
	}
	return nil
}

// ensureMessagePartition creates a specific monthly partition for the messages table if it doesn't exist.
func ensureMessagePartition(db *gorm.DB, schemaName string, date time.Time) error {
	year, month, _ := date.UTC().Date()
	partitionName := fmt.Sprintf("messages_y%dm%02d", year, month)

	// Calculate start and end dates for the partition range
	startDate := time.Date(year, month, 1, 0, 0, 0, 0, time.UTC)
	endDate := startDate.AddDate(0, 1, 0) // Start of next month

	// Check if partition exists in the correct schema
	var exists bool
	checkSQL := `
		SELECT EXISTS (
			SELECT FROM information_schema.tables
			WHERE table_schema = ? AND table_name = ?
		)`
	err := db.Raw(checkSQL, schemaName, partitionName).Scan(&exists).Error
	if err != nil {
		return fmt.Errorf("failed to check if partition %s exists in schema %s: %w", partitionName, schemaName, err)
	}

	if exists {
		logger.Log.Debug("Message partition already exists, skipping creation", zap.String("partitionName", partitionName), zap.String("schema", schemaName))
		return nil
	}

	// Create partition (relies on search_path for the base 'messages' table)
	// The partition name itself is unqualified as it will be created in the default schema set by search_path
	// Explicitly qualify partition name and base table name
	createPartitionSQL := fmt.Sprintf(`
        CREATE TABLE %q.%s PARTITION OF %q.messages
        FOR VALUES FROM ('%s') TO ('%s');
    `, schemaName, partitionName, schemaName, startDate.Format("2006-01-02"), endDate.Format("2006-01-02"))

	logger.Log.Info("Creating message partition",
		zap.String("partitionName", partitionName),
		zap.String("schema", schemaName), // Log the target schema
		zap.String("startDate", startDate.Format("2006-01-02")),
		zap.String("endDate", endDate.Format("2006-01-02")),
	)

	if err := db.Exec(createPartitionSQL).Error; err != nil {
		// Improved error check for concurrent creation (race condition)
		if pgErr, ok := err.(*pgconn.PgError); ok && pgErr.Code == "42P07" { // 42P07 = duplicate_table
			logger.Log.Warn("Message partition creation conflict (likely concurrent creation), assuming exists", zap.String("partitionName", partitionName), zap.String("schema", schemaName))
			return nil // Treat as success if it already exists due to race condition
		}
		// Fallback check for generic "already exists" messages
		if strings.Contains(err.Error(), "already exists") || strings.Contains(err.Error(), "duplicate table") {
			logger.Log.Warn("Message partition creation conflict (already exists string match), assuming exists", zap.String("partitionName", partitionName), zap.String("schema", schemaName))
			return nil // Treat as success if it already exists
		}
		logger.Log.Error("Failed to create message partition",
			zap.String("partitionName", partitionName),
			zap.String("schema", schemaName),
			zap.Error(err),
		)
		return fmt.Errorf("failed to create partition %s in schema %s: %w", partitionName, schemaName, err)
	}

	// Setup CDC for the new partition
	logger.Log.Info("Setting up CDC for new message partition",
		zap.String("partition", partitionName),
		zap.String("schema", schemaName))

	// Set REPLICA IDENTITY FULL
	cdcSQL := fmt.Sprintf(`
    DO $$
    BEGIN
        -- Set REPLICA IDENTITY FULL for the new partition
        ALTER TABLE %q.%s REPLICA IDENTITY FULL;
        
        -- Add to publication
        BEGIN
            ALTER PUBLICATION daisi_pub ADD TABLE %q.%s;
        EXCEPTION WHEN duplicate_object THEN
            NULL; -- Already in publication
        END;
    END $$;
    `, schemaName, partitionName, schemaName, partitionName)

	if err := db.Exec(cdcSQL).Error; err != nil {
		logger.Log.Warn("Failed to setup CDC for partition",
			zap.String("partition", partitionName),
			zap.Error(err))
		// Don't fail partition creation, just log the CDC setup failure
	}

	logger.Log.Info("Successfully created message partition with CDC", zap.String("partitionName", partitionName), zap.String("schema", schemaName))

	return nil
}

// tenantNamer implements gorm schema.Namer interface for multi-tenant schemas
// It embeds the default NamingStrategy and overrides TableName.
type tenantNamer struct {
	schema.NamingStrategy // Embed the default strategy
	schemaName            string
}

// TableName implements the schema.Namer interface, overriding the default.
func (tn tenantNamer) TableName(table string) string {
	// GORM models return the base table name (e.g., "users")
	// We prepend the specific schema name for this connection.
	// Use fmt.Sprintf with %q to handle potential special characters in schema/table names.
	return fmt.Sprintf("%q.%s", tn.schemaName, table) // Qualify with schema
}

// NewPostgresRepo creates a new Postgres repository and initializes the schema
func NewPostgresRepo(dsn string, autoMigrate bool, companyID string) (*PostgresRepo, error) {
	// Retry connecting to the default database
	operationConnectDefault := func() (*gorm.DB, error) {
		db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
		if err != nil {
			// Check if the error is potentially transient (e.g., network error)
			if isTransientError(err) { // You'll need to define isTransientError based on pg driver errors
				logger.Log.Warn("Failed to connect to default postgres (transient), retrying...", zap.Error(err))
				return nil, err // Return transient error to trigger retry
			}
			// Return permanent error wrapped
			return nil, backoff.Permanent(fmt.Errorf("failed to connect to default postgres db: %w", err))
		}
		return db, nil
	}

	notify := func(err error, d time.Duration) {
		logger.Log.Warn("Retrying default DB connection", zap.Error(err), zap.Duration("after", d))
	}

	// Configure exponential backoff
	// TODO: Make these configurable
	b := backoff.NewExponentialBackOff()
	b.InitialInterval = 1 * time.Second
	b.MaxInterval = 15 * time.Second
	b.MaxElapsedTime = 1 * time.Minute // Stop retrying after 1 minute

	dbDefault, err := backoff.RetryNotifyWithData(operationConnectDefault, b, notify)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to default postgres after retries: %w", err)
	}

	schemaName := fmt.Sprintf("daisi_%s", companyID)
	logger.Log.Info("Ensuring PostgreSQL schema exists", zap.String("schema", schemaName))

	// Create schema if it doesn't exist - Use %q to quote the identifier
	if err := dbDefault.Exec(fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %q", schemaName)).Error; err != nil {
		sqlDB, _ := dbDefault.DB()
		if sqlDB != nil {
			sqlDB.Close()
		}
		return nil, fmt.Errorf("failed to create schema %s: %w", schemaName, err)
	}

	// Close the initial connection
	sqlDB, err := dbDefault.DB()
	if err != nil {
		logger.Log.Warn("Failed to get underlying SQL DB handle for closing", zap.Error(err))
	} else {
		if err := sqlDB.Close(); err != nil {
			logger.Log.Warn("Failed to close initial DB connection", zap.Error(err))
		}
	}

	// Reconnect, explicitly setting the search_path to the tenant's schema
	// This ensures subsequent operations (like AutoMigrate) target the correct schema.
	tenantDSN := dsn

	// Retry connecting to the tenant schema
	operationConnectTenant := func() (*gorm.DB, error) {
		// Create the Namer for this specific tenant
		namer := tenantNamer{schemaName: schemaName}
		// Configure GORM to use the custom Namer
		db, err := gorm.Open(postgres.Open(tenantDSN), &gorm.Config{
			NamingStrategy: namer, // Use the custom namer
		})
		if err != nil {
			if isTransientError(err) {
				logger.Log.Warn("Failed to connect to tenant schema (transient), retrying...", zap.String("schema", schemaName), zap.Error(err))
				return nil, err
			}
			return nil, backoff.Permanent(fmt.Errorf("failed to connect to postgres tenant schema %s: %w", schemaName, err))
		}
		return db, nil
	}

	notifyTenant := func(err error, d time.Duration) {
		logger.Log.Warn("Retrying tenant schema DB connection", zap.String("schema", schemaName), zap.Error(err), zap.Duration("after", d))
	}

	bTenant := backoff.NewExponentialBackOff() // Use a separate backoff instance or Reset the previous one
	bTenant.InitialInterval = 1 * time.Second
	bTenant.MaxInterval = 15 * time.Second
	bTenant.MaxElapsedTime = 1 * time.Minute

	db, err := backoff.RetryNotifyWithData(operationConnectTenant, bTenant, notifyTenant)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to postgres tenant db %s after retries: %w", schemaName, err) // Updated error message
	}

	// ----> SET SEARCH_PATH HERE <----
	// setSearchPathSQL := fmt.Sprintf("SET search_path TO %q, public", schemaName)
	// if err := db.Exec(setSearchPathSQL).Error; err != nil {
	// 	sqlDB, _ := db.DB()
	// 	if sqlDB != nil {
	// 		sqlDB.Close() // Clean up the connection if setting search_path fails
	// 	}
	// 	return nil, fmt.Errorf("failed to set search_path for schema %s: %w", schemaName, err)
	// }
	// logger.Log.Info("Set search_path for connection", zap.String("schema", schemaName))
	// ----> END SET SEARCH_PATH <----

	repo := &PostgresRepo{db: db}

	// --- Define DDL for the partitioned messages table ---
	messagesTableDDL := fmt.Sprintf(`
	CREATE TABLE %q.messages (
		id BIGSERIAL NOT NULL,
		message_id TEXT,
		from_phone TEXT,
		to_phone TEXT,
		chat_id TEXT,
		jid TEXT,
		message_text TEXT,
		message_url TEXT,
		message_type TEXT,
		company_id VARCHAR,
		key JSONB,
		edited_message_obj JSONB,
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
	`, schemaName)

	// --- Ensure the main partitioned messages table exists ---
	// Pass schemaName to ensureTableExists
	if err := ensureTableExists(db, schemaName, "messages", messagesTableDDL); err != nil {
		sqlDBClose, _ := db.DB()
		if sqlDBClose != nil {
			sqlDBClose.Close()
		}
		return nil, err // Propagate error if table creation fails
	}

	// Add indexes separately after ensuring table exists. AutoMigrate might handle this later,
	// but adding manually ensures they exist even if AutoMigrate is off or fails partially.
	// Note: company_id is not included as indexing is within the tenant-specific schema.
	indexes := map[string]string{
		"idx_messages_message_id":        fmt.Sprintf("CREATE UNIQUE INDEX IF NOT EXISTS idx_messages_message_id ON %q.messages USING btree (message_id, message_date);", schemaName),
		"idx_messages_from_phone":        fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_messages_from ON %q.messages USING btree (from_phone);`, schemaName),
		"idx_messages_to_phone":          fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_messages_to ON %q.messages USING btree (to_phone);`, schemaName),
		"idx_messages_chat_id":           fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_messages_chat_id ON %q.messages USING btree (chat_id);", schemaName),
		"idx_messages_jid":               fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_messages_jid ON %q.messages USING btree (jid);", schemaName),
		"idx_messages_agent_id":          fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_messages_agent_id ON %q.messages USING btree (agent_id);", schemaName),
		"idx_messages_message_timestamp": fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_messages_message_timestamp ON %q.messages USING btree (message_timestamp);", schemaName),
		"idx_messages_message_date":      fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_messages_message_date ON %q.messages USING btree (message_date);", schemaName),
	}

	for indexName, indexSQL := range indexes {
		if err := db.Exec(indexSQL).Error; err != nil {
			// Log index creation errors but don't fail startup, AutoMigrate might fix it
			logger.Log.Warn("Failed to create index", zap.String("indexName", indexName), zap.Error(err))
		}
	}

	// --- Ensure the DEFAULT partition exists ---
	// This catches any messages that don't fall into a specific monthly partition.
	// We should check if this default partition exists in the correct schema too.
	defaultPartitionName := "messages_default"
	var defaultExists bool
	checkDefaultSQL := `SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = ? AND table_name = ?)`
	if err := db.Raw(checkDefaultSQL, schemaName, defaultPartitionName).Scan(&defaultExists).Error; err != nil {
		sqlDBClose, _ := db.DB()
		if sqlDBClose != nil {
			sqlDBClose.Close()
		}
		return nil, fmt.Errorf("failed to check if default partition %s exists in schema %s: %w", defaultPartitionName, schemaName, err)
	}

	if !defaultExists {
		logger.Log.Info("Creating default message partition", zap.String("schema", schemaName))
		// Explicitly qualify partition name and base table name
		defaultPartitionSQL := fmt.Sprintf(`CREATE TABLE %q.%s PARTITION OF %q.messages DEFAULT;`, schemaName, defaultPartitionName, schemaName)
		if err := db.Exec(defaultPartitionSQL).Error; err != nil {
			// Check for race condition/already exists
			if pgErr, ok := err.(*pgconn.PgError); ok && pgErr.Code == "42P07" {
				logger.Log.Warn("Default message partition creation conflict (likely concurrent creation), assuming exists", zap.String("schema", schemaName))
			} else if strings.Contains(err.Error(), "already exists") || strings.Contains(err.Error(), "duplicate table") {
				logger.Log.Warn("Default message partition creation conflict (already exists string match), assuming exists", zap.String("schema", schemaName))
			} else {
				logger.Log.Error("Failed to create default message partition", zap.String("schema", schemaName), zap.Error(err))
				sqlDB, _ := db.DB()
				if sqlDB != nil {
					sqlDB.Close()
				}
				return nil, fmt.Errorf("failed to create default partition in schema %s: %w", schemaName, err)
			}
		}
	} else {
		logger.Log.Debug("Default message partition already exists", zap.String("schema", schemaName))
	}

	// --- Run AutoMigrate for other tables and potentially update columns/indexes on 'messages' ---
	if autoMigrate {
		logger.Log.Info("Running auto-migration for schema", zap.String("schema", schemaName))
		// GORM's AutoMigrate should respect the session's search_path
		err = db.AutoMigrate(
			&model.Message{},
			&model.Chat{},
			&model.Agent{},
			&model.Contact{},
			&model.OnboardingLog{},
			&model.ExhaustedEvent{},
		)
		if err != nil {
			// Log migration errors but don't necessarily fail startup
			logger.Log.Error("Auto-migration failed or produced errors", zap.Error(err), zap.String("schema", schemaName))
		}
	} else {
		logger.Log.Info("Auto-migration disabled")
	}

	// ---> Verify crucial tables after AutoMigrate <---
	logger.Log.Debug("Verifying existence of 'chats' table post-migration", zap.String("schema", schemaName))
	var chatTableExists bool
	checkExistsSQL := `SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = ? AND table_name = ?)`
	if err := db.Raw(checkExistsSQL, schemaName, "chats").Scan(&chatTableExists).Error; err != nil {
		sqlDB, _ := db.DB()
		if sqlDB != nil {
			sqlDB.Close()
		}
		return nil, fmt.Errorf("failed to check for 'chats' table existence after migration in schema %s: %w", schemaName, err)
	}
	if !chatTableExists {
		sqlDB, _ := db.DB()
		if sqlDB != nil {
			sqlDB.Close()
		}
		return nil, fmt.Errorf("'chats' table still does not exist after auto-migration in schema %s", schemaName)
	}
	logger.Log.Debug("'chats' table verified post-migration", zap.String("schema", schemaName))

	var agentTableExists bool
	if err := db.Raw(checkExistsSQL, schemaName, "agents").Scan(&agentTableExists).Error; err != nil {
		sqlDB, _ := db.DB()
		if sqlDB != nil {
			sqlDB.Close()
		}
		return nil, fmt.Errorf("failed to check for 'agents' table existence after migration in schema %s: %w", schemaName, err)
	}
	if !agentTableExists {
		sqlDB, _ := db.DB()
		if sqlDB != nil {
			sqlDB.Close()
		}
		return nil, fmt.Errorf("'agents' table still does not exist after auto-migration in schema %s", schemaName)
	}
	logger.Log.Debug("'agents' table verified post-migration", zap.String("schema", schemaName))

	var contactTableExists bool
	if err := db.Raw(checkExistsSQL, schemaName, "contacts").Scan(&contactTableExists).Error; err != nil {
		sqlDB, _ := db.DB()
		if sqlDB != nil {
			sqlDB.Close()
		}
		return nil, fmt.Errorf("failed to check for 'contacts' table existence after migration in schema %s: %w", schemaName, err)
	}
	if !contactTableExists {
		sqlDB, _ := db.DB()
		if sqlDB != nil {
			sqlDB.Close()
		}
		return nil, fmt.Errorf("'contacts' table still does not exist after auto-migration in schema %s", schemaName)
	}
	logger.Log.Debug("'contacts' table verified post-migration", zap.String("schema", schemaName))

	var onboardingLogTableExists bool
	if err := db.Raw(checkExistsSQL, schemaName, "onboarding_log").Scan(&onboardingLogTableExists).Error; err != nil {
		sqlDB, _ := db.DB()
		if sqlDB != nil {
			sqlDB.Close()
		}
		return nil, fmt.Errorf("failed to check for 'onboarding_log' table existence after migration in schema %s: %w", schemaName, err)
	}
	if !onboardingLogTableExists {
		sqlDB, _ := db.DB()
		if sqlDB != nil {
			sqlDB.Close()
		}
		return nil, fmt.Errorf("'onboarding_log' table still does not exist after auto-migration in schema %s", schemaName)
	}
	logger.Log.Debug("'onboarding_log' table verified post-migration", zap.String("schema", schemaName))

	var exhEventTableExists bool
	if err := db.Raw(checkExistsSQL, schemaName, "exhausted_events").Scan(&exhEventTableExists).Error; err != nil {
		sqlDB, _ := db.DB()
		if sqlDB != nil {
			sqlDB.Close()
		}
		return nil, fmt.Errorf("failed to check for 'exhausted_events' table existence after migration in schema %s: %w", schemaName, err)
	}
	if !exhEventTableExists {
		sqlDB, _ := db.DB()
		if sqlDB != nil {
			sqlDB.Close()
		}
		return nil, fmt.Errorf("'exhausted_events' table still does not exist after auto-migration in schema %s", schemaName)
	}
	logger.Log.Debug("'exhausted_events' table verified post-migration", zap.String("schema", schemaName))
	// ---> End verification <---

	// Create indexes for contacts table
	contactsIndexes := map[string]string{
		"idx_contacts_phone":       fmt.Sprintf("CREATE UNIQUE INDEX IF NOT EXISTS idx_contacts_phone ON %q.contacts USING btree (phone_number);", schemaName),
		"idx_contacts_assigned_to": fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_contacts_assigned_to ON %q.contacts USING btree (assigned_to);", schemaName),
		"idx_contacts_agent_id":    fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_contacts_agent_id ON %q.contacts USING btree (agent_id);", schemaName),
		"idx_contacts_chat_id":     fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_contacts_chat_id ON %q.contacts USING btree (chat_id);", schemaName),
	}
	for indexName, indexSQL := range contactsIndexes {
		if err := db.Exec(indexSQL).Error; err != nil {
			logger.Log.Warn("Failed to create index", zap.String("indexName", indexName), zap.Error(err))
		}
	}

	// --- Ensure necessary monthly partitions exist on startup ---
	now := time.Now().UTC()
	for _, date := range []time.Time{
		now.AddDate(0, -1, 0), // Previous month
		now,                   // Current month
		now.AddDate(0, 1, 0),  // Next month
	} {
		// Pass schemaName to ensureMessagePartition
		if err := ensureMessagePartition(db, schemaName, date); err != nil {
			// Log partition creation errors but don't fail startup
			logger.Log.Warn("Failed to ensure message partition exists on startup", zap.Time("date", date), zap.String("schema", schemaName), zap.Error(err))
		}
	}

	// CDC auto setup
	if autoMigrate {
		logger.Log.Info("Setting up CDC for company schema",
			zap.String("company_id", companyID),
			zap.String("schema", schemaName))

		// Small delay to ensure all tables are ready
		time.Sleep(2 * time.Second)

		if err := repo.setupCDCForCompany(context.Background(), schemaName); err != nil {
			// Log error but don't fail the entire setup
			logger.Log.Error("Failed to setup CDC for company",
				zap.String("company_id", companyID),
				zap.String("schema", schemaName),
				zap.Error(err))

			// Retry in background
			go func() {
				retries := 5
				for i := 0; i < retries; i++ {
					time.Sleep(time.Duration(i+1) * 10 * time.Second)

					if err := repo.setupCDCForCompany(context.Background(), schemaName); err == nil {
						logger.Log.Info("CDC setup succeeded on retry",
							zap.String("schema", schemaName),
							zap.Int("attempt", i+1))
						return
					}
					logger.Log.Warn("CDC setup retry failed",
						zap.String("schema", schemaName),
						zap.Int("attempt", i+1),
						zap.Error(err))
				}
				logger.Log.Error("CDC setup failed after all retries",
					zap.String("schema", schemaName))
			}()
		} else {
			logger.Log.Info("CDC setup successful for company",
				zap.String("company_id", companyID),
				zap.String("schema", schemaName))
		}
	}

	return repo, nil
}

// Close closes the database connection
func (r *PostgresRepo) Close(ctx context.Context) error {
	// Attempt to get the underlying sql.DB connection
	sqlDB, err := r.db.DB()
	if err != nil {
		// Log if we can't get the DB instance, but don't necessarily fail Close
		logger.FromContext(ctx).Warn("Failed to get underlying SQL DB for closing", zap.Error(err))
		return nil // Or return the error if preferred: fmt.Errorf("failed to get SQL DB: %w", err)
	}

	// Close the connection
	closeErr := sqlDB.Close()
	if closeErr != nil {
		logger.FromContext(ctx).Error("Failed to close database connection", zap.Error(closeErr))
		return fmt.Errorf("failed to close SQL DB: %w", closeErr)
	}

	logger.FromContext(ctx).Info("Database connection closed successfully")
	return nil
}

// checkConstraintViolation inspects database errors and maps them to standard apperrors.
func checkConstraintViolation(err error) error {
	if err == nil {
		return nil
	}

	// Check for specific GORM errors first
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return fmt.Errorf("%w: %w", apperrors.ErrNotFound, err)
	}
	// Add other specific GORM errors if needed (e.g., gorm.ErrInvalidData)

	// Check for underlying pgconn errors
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		// Class 23 — Integrity Constraint Violation
		case "23505": // unique_violation
			return fmt.Errorf("%w: constraint %s: %w", apperrors.ErrDuplicate, pgErr.ConstraintName, err)
		case "23503": // foreign_key_violation
			return fmt.Errorf("%w: constraint %s: %w", apperrors.ErrBadRequest, pgErr.ConstraintName, err)
		case "23502": // not_null_violation
			return fmt.Errorf("%w: null value in column %s: %w", apperrors.ErrBadRequest, pgErr.ColumnName, err)
		case "23514": // check_violation
			return fmt.Errorf("%w: constraint %s: %w", apperrors.ErrBadRequest, pgErr.ConstraintName, err)

		// Class 22 — Data Exception
		case "22001": // string_data_right_truncation
			return fmt.Errorf("%w: value too long for column %s: %w", apperrors.ErrBadRequest, pgErr.ColumnName, err)
		case "22P02": // invalid_text_representation
			return fmt.Errorf("%w: invalid input syntax for type %s: %w", apperrors.ErrBadRequest, pgErr.DataTypeName, err)

		// Class 40 — Transaction Rollback
		case "40001": // serialization_failure
			fallthrough // Treat same as deadlock for now
		case "40P01": // deadlock_detected
			// Map to ErrDatabase, handler can decide if retryable
			return fmt.Errorf("%w: transaction rollback (%s): %w", apperrors.ErrDatabase, pgErr.Code, err)

		default:
			// Check error code prefixes for broader categories
			if strings.HasPrefix(pgErr.Code, "53") { // Class 53 — Insufficient Resources
				return fmt.Errorf("%w: insufficient resources (%s): %w", apperrors.ErrDatabase, pgErr.Code, err)
			}
			if strings.HasPrefix(pgErr.Code, "08") { // Class 08 — Connection Exception
				return fmt.Errorf("%w: connection error (%s): %w", apperrors.ErrDatabase, pgErr.Code, err)
			}
			// Wrap unhandled specific PgErrors as general database errors
			return fmt.Errorf("%w: unhandled pgcode %s: %w", apperrors.ErrDatabase, pgErr.Code, err)
		}
	}

	// Assume other GORM or generic errors are general database errors for now
	// This catches things like gorm.ErrInvalidTransaction, context deadline exceeded wrapped by GORM etc.
	return fmt.Errorf("%w: %w", apperrors.ErrDatabase, err)
}
