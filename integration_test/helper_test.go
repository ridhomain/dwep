package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/google/uuid"
	"os"
	"path/filepath"

	"github.com/xeipuuv/gojsonschema"
)

// executeQuery executes a SQL query on the PostgreSQL database and returns the result
func executeQuery(ctx context.Context, dsn string, query string) (interface{}, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}
	defer db.Close()

	var result interface{}
	err = db.QueryRowContext(ctx, query).Scan(&result)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	return result, nil
}

// executeNonQuerySQL executes a SQL statement that doesn't return a result (like DELETE or UPDATE)
func executeNonQuerySQL(ctx context.Context, dsn string, statement string) error {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer db.Close()

	_, err = db.ExecContext(ctx, statement)
	if err != nil {
		return fmt.Errorf("failed to execute SQL statement: %w", err)
	}

	return nil
}

// verifyRecordExists checks if a record exists in the database without retrieving all its columns
func verifyRecordExists(ctx context.Context, dsn string, table string, whereClause string) (bool, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return false, fmt.Errorf("failed to connect to database: %w", err)
	}
	defer db.Close()

	query := fmt.Sprintf("SELECT 1 FROM %s WHERE %s LIMIT 1", table, whereClause)

	var result int
	err = db.QueryRowContext(ctx, query).Scan(&result)
	if err != nil {
		if err == sql.ErrNoRows {
			return false, nil // Record doesn't exist, but no error
		}
		return false, fmt.Errorf("failed to execute existence check: %w", err)
	}

	return result == 1, nil
}

// validatePayload validates a JSON payload (as byte slice) against a specified JSON schema file.
// schemaName should correspond to a file in internal/model/jsonschema/ (e.g., "message.upsert.schema").
func validatePayload(payloadBytes []byte, schemaName string) error {
	// Construct the absolute path to the schema file relative to the project root
	// Assumes tests are run from the project root or integration_test directory
	projectRoot, err := getProjectRoot()
	if err != nil {
		return fmt.Errorf("failed to get project root: %w", err)
	}
	schemaPath := filepath.Join(projectRoot, "internal", "model", "jsonschema", schemaName+".json")

	// Check if schema file exists
	if _, err := os.Stat(schemaPath); os.IsNotExist(err) {
		return fmt.Errorf("schema file not found at %s: %w", schemaPath, err)
	}

	schemaLoader := gojsonschema.NewReferenceLoader("file://" + schemaPath)
	documentLoader := gojsonschema.NewBytesLoader(payloadBytes)

	result, err := gojsonschema.Validate(schemaLoader, documentLoader)
	if err != nil {
		return fmt.Errorf("error during schema validation: %w", err)
	}

	if !result.Valid() {
		errMsg := "JSON payload failed schema validation:\n"
		for _, desc := range result.Errors() {
			errMsg += fmt.Sprintf("- %s\n", desc)
		}
		// Include the payload for easier debugging, truncated if too long
		payloadStr := string(payloadBytes)
		if len(payloadStr) > 500 {
			payloadStr = payloadStr[:500] + "... (truncated)"
		}
		errMsg += fmt.Sprintf("Payload: %s", payloadStr)
		return fmt.Errorf(errMsg)
	}

	return nil
}

// getProjectRoot attempts to find the project root directory based on the presence of go.mod.
// This is needed because tests might be run from different working directories.
func getProjectRoot() (string, error) {
	// Start from current directory
	cwd, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("failed to get current working directory: %w", err)
	}

	// Traverse up until go.mod is found or root is reached
	dir := cwd
	for {
		goModPath := filepath.Join(dir, "go.mod")
		if _, err := os.Stat(goModPath); err == nil {
			// go.mod found, this is the project root
			return dir, nil
		}

		// Move up one directory
		parentDir := filepath.Dir(dir)
		if parentDir == dir {
			// Reached the root directory without finding go.mod
			// Fallback: maybe running from within integration_test? Try going up one level.
			if filepath.Base(cwd) == "integration_test" {
				return filepath.Dir(cwd), nil
			}
			return "", fmt.Errorf("could not find project root (go.mod) starting from %s", cwd)
		}
		dir = parentDir
	}
}

func generateUUID() string {
	return uuid.New().String()
}
