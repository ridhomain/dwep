package storage

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zaptest"
	"gorm.io/datatypes"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"

	apperrors "gitlab.com/timkado/api/daisi-wa-events-processor/internal/apperrors"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/model"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/tenant"
	"gitlab.com/timkado/api/daisi-wa-events-processor/pkg/logger"
)

func TestSaveExhaustedEvent_Success(t *testing.T) {
	mockDB, mock, repo := setupMockDB(t)
	defer mockDB.Close()

	companyID := "test-company-success"
	ctx := tenant.WithCompanyID(context.Background(), companyID)
	ctx = logger.WithLogger(ctx, zaptest.NewLogger(t))

	dlqPayloadJSON, _ := json.Marshal(map[string]string{"error": "failed to process"})
	originalPayloadJSON, _ := json.Marshal(map[string]string{"data": "original data"})

	event := model.ExhaustedEvent{
		CompanyID:       companyID,
		SourceSubject:   "test.subject",
		LastError:       "some error",
		RetryCount:      5,
		EventTimestamp:  time.Now(),
		DLQPayload:      datatypes.JSON(dlqPayloadJSON),
		OriginalPayload: datatypes.JSON(originalPayloadJSON),
	}

	query := regexp.QuoteMeta(`INSERT INTO "exhausted_events" ("created_at","company_id","source_subject","last_error","retry_count","event_timestamp","dlq_payload","original_payload","resolved","resolved_at","notes") VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) RETURNING "id"`)

	mock.ExpectBegin()
	mock.ExpectQuery(query).
		WithArgs(sqlmock.AnyArg(), event.CompanyID, event.SourceSubject, event.LastError, event.RetryCount, event.EventTimestamp, event.DLQPayload, event.OriginalPayload, false, nil, "").
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))
	mock.ExpectCommit()

	err := repo.SaveExhaustedEvent(ctx, event)

	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestSaveExhaustedEvent_BeginError(t *testing.T) {
	mockDB, mock, repo := setupMockDB(t)
	defer mockDB.Close()

	companyID := "test-company-begin-err"
	ctx := tenant.WithCompanyID(context.Background(), companyID)
	ctx = logger.WithLogger(ctx, zaptest.NewLogger(t))

	dlqPayloadJSON, _ := json.Marshal(map[string]string{"error": "failed to process"})
	originalPayloadJSON, _ := json.Marshal(map[string]string{"data": "original data"})
	event := model.ExhaustedEvent{CompanyID: companyID, SourceSubject: "test.subject", DLQPayload: dlqPayloadJSON, OriginalPayload: originalPayloadJSON}

	expectedErr := errors.New("failed to begin")
	mock.ExpectBegin().WillReturnError(expectedErr)

	err := repo.SaveExhaustedEvent(ctx, event)

	assert.Error(t, err)
	assert.True(t, errors.Is(err, apperrors.ErrDatabase), "Expected ErrDatabase")
	assert.Contains(t, err.Error(), expectedErr.Error())
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestSaveExhaustedEvent_CreateError(t *testing.T) {
	mockDB, mock, repo := setupMockDB(t)
	defer mockDB.Close()

	companyID := "test-company-create-err"
	ctx := tenant.WithCompanyID(context.Background(), companyID)
	ctx = logger.WithLogger(ctx, zaptest.NewLogger(t))

	dlqPayloadJSON, _ := json.Marshal(map[string]string{"error": "failed to process"})
	originalPayloadJSON, _ := json.Marshal(map[string]string{"data": "original data"})
	event := model.ExhaustedEvent{CompanyID: companyID, SourceSubject: "test.subject", DLQPayload: dlqPayloadJSON, OriginalPayload: originalPayloadJSON}

	query := regexp.QuoteMeta(`INSERT INTO "exhausted_events"`) // Simplified query match
	expectedErr := errors.New("db connection lost")

	mock.ExpectBegin()
	mock.ExpectQuery(query).
		WithArgs(sqlmock.AnyArg(), event.CompanyID, event.SourceSubject, sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), event.DLQPayload, event.OriginalPayload, false, nil, "").
		WillReturnError(expectedErr)
	mock.ExpectRollback() // Expect rollback on error

	err := repo.SaveExhaustedEvent(ctx, event)

	assert.Error(t, err)
	assert.True(t, errors.Is(err, apperrors.ErrDatabase), "Expected ErrDatabase")
	assert.Contains(t, err.Error(), expectedErr.Error())
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestSaveExhaustedEvent_CommitError(t *testing.T) {
	mockDB, mock, repo := setupMockDB(t)
	defer mockDB.Close()

	companyID := "test-company-commit-err"
	ctx := tenant.WithCompanyID(context.Background(), companyID)
	ctx = logger.WithLogger(ctx, zaptest.NewLogger(t))

	dlqPayloadJSON, _ := json.Marshal(map[string]string{"error": "failed to process"})
	originalPayloadJSON, _ := json.Marshal(map[string]string{"data": "original data"})
	event := model.ExhaustedEvent{CompanyID: companyID, SourceSubject: "test.subject", DLQPayload: dlqPayloadJSON, OriginalPayload: originalPayloadJSON}

	query := regexp.QuoteMeta(`INSERT INTO "exhausted_events"`) // Simplified query match
	expectedErr := errors.New("commit failed")

	mock.ExpectBegin()
	mock.ExpectQuery(query).
		WithArgs(sqlmock.AnyArg(), event.CompanyID, event.SourceSubject, sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), event.DLQPayload, event.OriginalPayload, false, nil, "").
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))
	mock.ExpectCommit().WillReturnError(expectedErr)
	// Rollback is implicitly handled by defer in the original function when commit fails

	err := repo.SaveExhaustedEvent(ctx, event)

	assert.Error(t, err)
	assert.True(t, errors.Is(err, apperrors.ErrDatabase), "Expected ErrDatabase")
	assert.Contains(t, err.Error(), expectedErr.Error())
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Helper function to setup mock DB and repo (similar to postgres_test.go)
func setupMockDB(t *testing.T) (*sql.DB, sqlmock.Sqlmock, *PostgresRepo) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}

	dialector := postgres.New(postgres.Config{
		DSN:                  "sqlmock_db_0",
		DriverName:           "postgres",
		Conn:                 mockDB,
		PreferSimpleProtocol: true,
	})
	gormDB, err := gorm.Open(dialector, &gorm.Config{})
	if err != nil {
		t.Fatalf("Failed to open gorm database: %v", err)
	}

	repo := &PostgresRepo{db: gormDB}
	return mockDB, mock, repo
}

// AnyArgTime is a sqlmock argument matcher for time.Time
// (Copied from postgres_test.go if needed, or use sqlmock.AnyArg() for simplicity)
type AnyArgTime struct{}

// Match satisfies sqlmock.Argument interface
func (a AnyArgTime) Match(v driver.Value) bool {
	_, ok := v.(time.Time)
	return ok
}
