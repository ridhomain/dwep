package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/lib/pq"
	_ "github.com/lib/pq"
	"github.com/testcontainers/testcontainers-go"
	pgtc "github.com/testcontainers/testcontainers-go/modules/postgres"
	netlib "github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

// startPostgres starts a PostgreSQL container and returns it along with its connection string.
func startPostgres(ctx context.Context, networkName string, network *testcontainers.DockerNetwork) (testcontainers.Container, string, error) {
	// PostgreSQL container configuration with database name
	pgContainer, err := pgtc.Run(ctx,
		"postgres:17-bookworm",
		pgtc.WithDatabase("message_service"),
		pgtc.WithUsername("postgres"),
		pgtc.WithPassword("postgres"),
		//pgtc.WithInitScripts("./init.sql"),
		netlib.WithNetwork([]string{"pg", networkName}, network),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(5*time.Second)),
	)
	if err != nil {
		return nil, "", fmt.Errorf("failed to start PostgreSQL container: %w", err)
	}

	// Try to get connection string using the module function
	dsn, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		// If that fails, manually build the connection string
		host, err := pgContainer.Host(ctx)
		if err != nil {
			return pgContainer, "", fmt.Errorf("failed to get PostgreSQL host: %w", err)
		}

		mappedPort, err := pgContainer.MappedPort(ctx, "5432")
		if err != nil {
			return pgContainer, "", fmt.Errorf("failed to get PostgreSQL port: %w", err)
		}

		dsn = fmt.Sprintf("postgres://postgres:postgres@%s:%s/message_service?sslmode=disable",
			host, mappedPort.Port())
	}

	return pgContainer, dsn, nil
}

// verifyPostgresData connects to PostgreSQL and verifies that data was stored correctly.
func verifyPostgresData(ctx context.Context, dsn string, query string, expectedResults int) (bool, error) {
	// Open connection to PostgreSQL
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return false, fmt.Errorf("failed to connect to PostgreSQL: %w", err)
	}
	defer db.Close()

	// Set connection timeout
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	// Check connection
	if err := db.PingContext(ctx); err != nil {
		return false, fmt.Errorf("failed to ping PostgreSQL: %w", err)
	}

	// Execute query
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return false, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	// Count results
	count := 0
	for rows.Next() {
		count++
	}

	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("error scanning query results: %w", err)
	}

	// Verify results count
	return count >= expectedResults, nil
}

func verifyPostgresDataWithSchema(ctx context.Context, dsn, tenantSchema, query string, expectedResults int, args ...interface{}) (bool, error) {
	// Connect to PostgreSQL using the original DSN
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return false, fmt.Errorf("verifyPostgresData: failed to open connection to PostgreSQL for schema %s: %w", tenantSchema, err)
	}
	defer db.Close()

	// Context for initial connection, ping, and setting search_path
	ctxSetup, cancelSetup := context.WithTimeout(ctx, 10*time.Second)
	defer cancelSetup()

	// Check connection
	if err := db.PingContext(ctxSetup); err != nil {
		return false, fmt.Errorf("verifyPostgresData: failed to ping PostgreSQL for schema %s: %w", tenantSchema, err)
	}

	// Set the search_path for the current session
	// Use pq.QuoteIdentifier to ensure schema name is safely quoted
	setSchemaQuery := fmt.Sprintf("SET search_path TO %s", pq.QuoteIdentifier(tenantSchema))
	_, err = db.ExecContext(ctxSetup, setSchemaQuery)
	if err != nil {
		return false, fmt.Errorf("verifyPostgresData: failed to set search_path to '%s': %w", tenantSchema, err)
	}

	// Context for the main data query
	ctxQuery, cancelQuery := context.WithTimeout(ctx, 10*time.Second)
	defer cancelQuery()

	// Execute the user's query
	rows, err := db.QueryContext(ctxQuery, query, args...)
	if err != nil {
		return false, fmt.Errorf("verifyPostgresData: failed to execute query in schema '%s': %w. Query: %s, Args: %v", tenantSchema, err, query, args)
	}
	defer rows.Close()

	count := 0
	isCountQuery := strings.Contains(strings.ToLower(query), "count(*)")

	for rows.Next() {
		if isCountQuery {
			// If it's a COUNT(*) query, scan the result directly into count
			if err := rows.Scan(&count); err != nil {
				return false, fmt.Errorf("verifyPostgresData: error scanning COUNT(*) result in schema '%s': %w", tenantSchema, err)
			}
			break // COUNT(*) returns only one row
		} else {
			// For other queries, scan into a dummy variable and increment count
			var result interface{}
			if err := rows.Scan(&result); err != nil {
				// Log scan error but continue counting for simple existence checks or row counts
				fmt.Printf("Warning: verifyPostgresData: error scanning row in schema '%s': %v\n", tenantSchema, err)
			}
			count++
		}
	}

	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("verifyPostgresData: error iterating query results in schema '%s': %w", tenantSchema, err)
	}

	if expectedResults < 0 { // A negative value (e.g., -1) means "check for existence"
		return count > 0, nil
	}

	return count == expectedResults, nil
}

func truncatePostgresTables(ctx context.Context, dsn, schemaName string) error {
	// Open connection to PostgreSQL using the provided DSN
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return fmt.Errorf("failed to connect to PostgreSQL: %w", err)
	}
	defer db.Close()

	// Set connection timeout
	ctxConnect, cancelConnect := context.WithTimeout(ctx, 5*time.Second)
	defer cancelConnect()

	// Check connection
	if err := db.PingContext(ctxConnect); err != nil {
		return fmt.Errorf("failed to ping PostgreSQL: %w", err)
	}

	// Define simple table names
	tableNames := []string{
		"messages",
		"chats",
		"agents",
		"contacts",
		"exhausted_events",
		"onboarding_log",
	}

	for _, tableName := range tableNames {
		// Check if table exists in the given schema
		exists, errCheck := tableExists(db, schemaName, tableName)
		if errCheck != nil {
			fmt.Printf("Warning: failed to check existence of table %s.%s: %v\n", schemaName, tableName, errCheck)
			continue // Skip to next table if check fails
		}

		if exists {
			ctxExec, cancelExec := context.WithTimeout(ctx, 5*time.Second)
			// Use pq.QuoteIdentifier for schema and table name to handle special characters and prevent SQL injection
			qualifiedTableName := fmt.Sprintf("%s.%s", pq.QuoteIdentifier(schemaName), pq.QuoteIdentifier(tableName))
			truncateQuery := fmt.Sprintf("TRUNCATE TABLE %s CASCADE", qualifiedTableName)
			_, errExec := db.ExecContext(ctxExec, truncateQuery)
			cancelExec() // Call cancel regardless of error
			if errExec != nil {
				return fmt.Errorf("failed to truncate table %s: %w", qualifiedTableName, errExec)
			}
		} else {
			fmt.Printf("Info: table %s.%s does not exist, skipping truncation.\n", schemaName, tableName)
		}
	}

	return nil
}

// executeQuery executes a query and returns the rows.
// Updated to accept tenantSchema.
func executeQueryWIthSchema(ctx context.Context, dsn, tenantSchema, query string, args ...interface{}) (*sql.Rows, error) {
	var tenantDSN string
	if strings.Contains(dsn, "?") {
		tenantDSN = fmt.Sprintf("%s&options=-c%%20search_path=%s", dsn, pq.QuoteIdentifier(tenantSchema))
	} else {
		tenantDSN = fmt.Sprintf("%s?options=-c%%20search_path=%s", dsn, pq.QuoteIdentifier(tenantSchema))
	}
	db, err := connectDB(tenantDSN)
	if err != nil {
		return nil, fmt.Errorf("executeQuery: failed to connect to PostgreSQL (%s): %w", tenantSchema, err)
	}
	// Note: Closing the DB connection here would make the returned *sql.Rows unusable.
	// The caller must close the returned rows.

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		db.Close() // Close DB if query fails
		return nil, fmt.Errorf("executeQuery: failed query in %s: %w. Query: %s, Args: %v", tenantSchema, err, query, args)
	}

	// It's the caller's responsibility to close the returned rows
	// and eventually the db connection if reused outside this scope.
	// For simplicity here, we might leak db connections if caller doesn't manage.
	// Consider returning the db handle as well, or using a connection pool.
	return rows, nil // Caller must call rows.Close()
}

// executeQueryWIthSchemaRowScan executes a query expected to return one row and scans it.
// Updated to accept tenantSchema.
func executeQueryWIthSchemaRowScan(ctx context.Context, dsn, tenantSchema, query string, dest []interface{}, args ...interface{}) error {
	db, err := connectDB(dsn)
	if err != nil {
		return fmt.Errorf("executeQueryWIthSchemaRowScan: failed to connect to PostgreSQL (%s): %w", tenantSchema, err)
	}
	defer db.Close()

	// Set the search_path for the current session
	// Use pq.QuoteIdentifier to ensure schema name is safely quoted
	setSchemaQuery := fmt.Sprintf("SET search_path TO %s", pq.QuoteIdentifier(tenantSchema))
	_, err = db.ExecContext(ctx, setSchemaQuery)
	if err != nil {
		return fmt.Errorf("verifyPostgresData: failed to set search_path to '%s': %w", tenantSchema, err)
	}

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	err = db.QueryRowContext(ctx, query, args...).Scan(dest...)
	if err != nil {
		return fmt.Errorf("executeQueryWIthSchemaRowScan: failed query/scan in %s: %w. Query: %s, Args: %v", tenantSchema, err, query, args)
	}
	return nil
}

// executeNonQuerySQLWithSchema executes a SQL statement that doesn't return rows.
// Updated to accept tenantSchema.
func executeNonQuerySQLWithSchema(ctx context.Context, dsn, tenantSchema, query string, args ...interface{}) error {
	db, err := connectDB(dsn)
	if err != nil {
		return fmt.Errorf("executeNonQuerySQL: failed to connect to PostgreSQL (%s): %w", tenantSchema, err)
	}
	defer db.Close()

	// Set the search_path for the current session
	// Use pq.QuoteIdentifier to ensure schema name is safely quoted
	setSchemaQuery := fmt.Sprintf("SET search_path TO %s", pq.QuoteIdentifier(tenantSchema))
	_, err = db.ExecContext(ctx, setSchemaQuery)
	if err != nil {
		return fmt.Errorf("verifyPostgresData: failed to set search_path to '%s': %w", tenantSchema, err)
	}

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	_, err = db.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("executeNonQuerySQL: failed query in %s: %w. Query: %s, Args: %v", tenantSchema, err, query, args)
	}
	return nil
}
