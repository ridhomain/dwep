package storage

import (
	"context"
	"testing"
	"time"

	"gitlab.com/timkado/api/daisi-wa-events-processor/pkg/logger"
	"go.uber.org/zap/zaptest"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	gormLogger "gorm.io/gorm/logger"

	apperrors "gitlab.com/timkado/api/daisi-wa-events-processor/internal/apperrors"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/model"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/tenant"
)

const testTenantIDOnboardingLog = "tenant-onboarding-test-111"

func newTestOnboardingLogRepo(t *testing.T) (*PostgresRepo, sqlmock.Sqlmock, func()) {
	logger.Log = zaptest.NewLogger(t).Named("test")
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	assert.NoError(t, err)
	gormDB, err := gorm.Open(postgres.New(postgres.Config{Conn: db}), &gorm.Config{
		Logger:                 gormLogger.Default.LogMode(gormLogger.Silent),
		SkipDefaultTransaction: true,
	})
	assert.NoError(t, err)
	repo := &PostgresRepo{db: gormDB}
	teardown := func() {
		assert.NoError(t, mock.ExpectationsWereMet())
	}
	return repo, mock, teardown
}

func newTestOnboardingLogRepoWithMatcher(t *testing.T, match sqlmock.QueryMatcher) (*PostgresRepo, sqlmock.Sqlmock, func()) {
	logger.Log = zaptest.NewLogger(t).Named("test")
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(match))
	assert.NoError(t, err)
	gormDB, err := gorm.Open(postgres.New(postgres.Config{Conn: db}), &gorm.Config{
		Logger:                 gormLogger.Default.LogMode(gormLogger.Silent),
		SkipDefaultTransaction: true,
	})
	assert.NoError(t, err)

	repo := &PostgresRepo{db: gormDB}
	teardown := func() {
		assert.NoError(t, mock.ExpectationsWereMet())
	}
	return repo, mock, teardown
}

func contextWithOnboardingLogTenant() context.Context {
	ctx := context.Background()
	ctx = tenant.WithCompanyID(ctx, testTenantIDOnboardingLog)
	return ctx
}

func TestPostgresRepo_SaveOnboardingLog_Success(t *testing.T) {
	repo, mock, teardown := newTestOnboardingLogRepoWithMatcher(t, sqlmock.QueryMatcherEqual)
	t.Cleanup(teardown)
	ctx := contextWithOnboardingLogTenant()
	now := time.Now()
	logEntry := model.OnboardingLog{
		MessageID:   "onboard-msg-1",
		AgentID:     "agent-onboard-1",
		CompanyID:   testTenantIDOnboardingLog,
		PhoneNumber: "08987654321",
		Timestamp:   now.Unix(),
	}

	expectedRows := sqlmock.NewRows([]string{"id"}).AddRow(1) // <<< Define expected return row

	insertPattern := `INSERT INTO "onboarding_logs" ("message_id","agent_id","company_id","phone_number","timestamp","created_at","last_metadata") VALUES ($1,$2,$3,$4,$5,$6,NULL) RETURNING "id"`
	mock.ExpectQuery(insertPattern).
		WithArgs(logEntry.MessageID, logEntry.AgentID, logEntry.CompanyID, logEntry.PhoneNumber, logEntry.Timestamp, sqlmock.AnyArg()).
		WillReturnRows(expectedRows)

	err := repo.SaveOnboardingLog(ctx, logEntry)
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestPostgresRepo_SaveOnboardingLog_TenantMismatch(t *testing.T) {
	repo, mock, teardown := newTestOnboardingLogRepo(t)
	t.Cleanup(teardown)
	ctx := contextWithOnboardingLogTenant()
	logEntry := model.OnboardingLog{MessageID: "onboard-mismatch", CompanyID: "wrong-tenant"}
	err := repo.SaveOnboardingLog(ctx, logEntry)
	assert.Error(t, err)
	assert.ErrorIs(t, err, apperrors.ErrBadRequest)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestPostgresRepo_FindOnboardingLogByMessageID_Found(t *testing.T) {
	repo, mock, teardown := newTestOnboardingLogRepoWithMatcher(t, sqlmock.QueryMatcherEqual)
	t.Cleanup(teardown)
	ctx := contextWithOnboardingLogTenant()
	now := time.Now()
	msgID := "find-onboard-msg-1"
	cols := []string{"id", "message_id", "company_id", "agent_id", "phone_number", "timestamp", "created_at"}
	rows := sqlmock.NewRows(cols).AddRow(1, msgID, testTenantIDOnboardingLog, "agent-find-1", "08111", now.Unix(), now)
	selectQuery := `SELECT * FROM "onboarding_logs" WHERE message_id = $1 AND company_id = $2 ORDER BY "onboarding_logs"."id" LIMIT $3`
	mock.ExpectQuery(selectQuery).WithArgs(msgID, testTenantIDOnboardingLog, 1).WillReturnRows(rows)
	found, err := repo.FindOnboardingLogByMessageID(ctx, msgID)
	assert.NoError(t, err)
	assert.NotNil(t, found)
	assert.Equal(t, msgID, found.MessageID)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestPostgresRepo_FindOnboardingLogByMessageID_NotFound(t *testing.T) {
	repo, mock, teardown := newTestOnboardingLogRepoWithMatcher(t, sqlmock.QueryMatcherEqual)
	t.Cleanup(teardown)
	ctx := contextWithOnboardingLogTenant()
	msgID := "find-onboard-msg-404"
	selectQuery := `SELECT * FROM "onboarding_logs" WHERE message_id = $1 AND company_id = $2 ORDER BY "onboarding_logs"."id" LIMIT $3`
	mock.ExpectQuery(selectQuery).WithArgs(msgID, testTenantIDOnboardingLog, 1).WillReturnError(gorm.ErrRecordNotFound)
	found, err := repo.FindOnboardingLogByMessageID(ctx, msgID)
	assert.ErrorIs(t, err, apperrors.ErrNotFound)
	assert.Nil(t, found)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestPostgresRepo_FindOnboardingLogsByPhoneNumber_Found(t *testing.T) {
	repo, mock, teardown := newTestOnboardingLogRepoWithMatcher(t, sqlmock.QueryMatcherEqual)
	t.Cleanup(teardown)
	ctx := contextWithOnboardingLogTenant()
	now := time.Now()
	phone := "082222"
	cols := []string{"id", "message_id", "company_id", "phone_number", "timestamp"}
	rows := sqlmock.NewRows(cols).
		AddRow(1, "msg-phone-1", testTenantIDOnboardingLog, phone, now.Unix()).
		AddRow(2, "msg-phone-2", testTenantIDOnboardingLog, phone, now.Unix()-3600)
	selectQuery := `SELECT * FROM "onboarding_logs" WHERE phone_number = $1 AND company_id = $2`
	mock.ExpectQuery(selectQuery).WithArgs(phone, testTenantIDOnboardingLog).WillReturnRows(rows)
	found, err := repo.FindOnboardingLogsByPhoneNumber(ctx, phone)
	assert.NoError(t, err)
	assert.NotNil(t, found)
	assert.Len(t, found, 2)
	assert.Equal(t, "msg-phone-1", found[0].MessageID)
	assert.Equal(t, "msg-phone-2", found[1].MessageID)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestPostgresRepo_FindOnboardingLogsByPhoneNumber_NotFound(t *testing.T) {
	repo, mock, teardown := newTestOnboardingLogRepoWithMatcher(t, sqlmock.QueryMatcherEqual)
	t.Cleanup(teardown)
	ctx := contextWithOnboardingLogTenant()
	phone := "083333"
	selectQuery := `SELECT * FROM "onboarding_logs" WHERE phone_number = $1 AND company_id = $2`
	mock.ExpectQuery(selectQuery).WithArgs(phone, testTenantIDOnboardingLog).WillReturnRows(sqlmock.NewRows([]string{"id"})) // Return empty rows
	found, err := repo.FindOnboardingLogsByPhoneNumber(ctx, phone)
	assert.NoError(t, err)
	assert.NotNil(t, found)
	assert.Len(t, found, 0)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestPostgresRepo_FindOnboardingLogsByAgentID_Found(t *testing.T) {
	repo, mock, teardown := newTestOnboardingLogRepoWithMatcher(t, sqlmock.QueryMatcherEqual)
	t.Cleanup(teardown)
	ctx := contextWithOnboardingLogTenant()
	now := time.Now()
	agentID := "agent-find-multi"
	cols := []string{"id", "message_id", "company_id", "agent_id", "timestamp"}
	rows := sqlmock.NewRows(cols).
		AddRow(1, "msg-agent-1", testTenantIDOnboardingLog, agentID, now.Unix()).
		AddRow(2, "msg-agent-2", testTenantIDOnboardingLog, agentID, now.Unix()-3600)
	selectQuery := `SELECT * FROM "onboarding_logs" WHERE agent_id = $1 AND company_id = $2`
	mock.ExpectQuery(selectQuery).WithArgs(agentID, testTenantIDOnboardingLog).WillReturnRows(rows)
	found, err := repo.FindOnboardingLogsByAgentID(ctx, agentID)
	assert.NoError(t, err)
	assert.NotNil(t, found)
	assert.Len(t, found, 2)
	assert.Equal(t, "msg-agent-1", found[0].MessageID)
	assert.Equal(t, "msg-agent-2", found[1].MessageID)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestPostgresRepo_FindOnboardingLogsByAgentID_NotFound(t *testing.T) {
	repo, mock, teardown := newTestOnboardingLogRepoWithMatcher(t, sqlmock.QueryMatcherEqual)
	t.Cleanup(teardown)
	ctx := contextWithOnboardingLogTenant()
	agentID := "agent-notfound"
	selectQuery := `SELECT * FROM "onboarding_logs" WHERE agent_id = $1 AND company_id = $2`
	mock.ExpectQuery(selectQuery).WithArgs(agentID, testTenantIDOnboardingLog).WillReturnRows(sqlmock.NewRows([]string{"id"}))
	found, err := repo.FindOnboardingLogsByAgentID(ctx, agentID)
	assert.NoError(t, err)
	assert.NotNil(t, found)
	assert.Len(t, found, 0)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestPostgresRepo_FindOnboardingLogsWithinTimeRange_Found(t *testing.T) {
	repo, mock, teardown := newTestOnboardingLogRepoWithMatcher(t, sqlmock.QueryMatcherEqual)
	t.Cleanup(teardown)
	ctx := contextWithOnboardingLogTenant()
	now := time.Now()
	startTime := now.Add(-2 * time.Hour).Unix()
	endTime := now.Unix()
	cols := []string{"id", "message_id", "company_id", "timestamp"}
	rows := sqlmock.NewRows(cols).
		AddRow(1, "msg-time-1", testTenantIDOnboardingLog, now.Add(-1*time.Hour).Unix()).   // Within range
		AddRow(2, "msg-time-2", testTenantIDOnboardingLog, now.Add(-30*time.Minute).Unix()) // Within range
	selectQuery := `SELECT * FROM "onboarding_logs" WHERE timestamp >= $1 AND timestamp <= $2 AND company_id = $3 ORDER BY timestamp ASC`
	mock.ExpectQuery(selectQuery).WithArgs(startTime, endTime, testTenantIDOnboardingLog).WillReturnRows(rows)
	found, err := repo.FindOnboardingLogsWithinTimeRange(ctx, startTime, endTime)
	assert.NoError(t, err)
	assert.NotNil(t, found)
	assert.Len(t, found, 2)
	assert.Equal(t, "msg-time-1", found[0].MessageID)
	assert.Equal(t, "msg-time-2", found[1].MessageID)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestPostgresRepo_FindOnboardingLogsWithinTimeRange_NotFound(t *testing.T) {
	repo, mock, teardown := newTestOnboardingLogRepoWithMatcher(t, sqlmock.QueryMatcherEqual)
	t.Cleanup(teardown)
	ctx := contextWithOnboardingLogTenant()
	startTime := time.Now().Add(-1 * time.Hour).Unix()
	endTime := time.Now().Unix()
	selectQuery := `SELECT * FROM "onboarding_logs" WHERE timestamp >= $1 AND timestamp <= $2 AND company_id = $3 ORDER BY timestamp ASC`
	mock.ExpectQuery(selectQuery).WithArgs(startTime, endTime, testTenantIDOnboardingLog).WillReturnRows(sqlmock.NewRows([]string{"id"}))
	found, err := repo.FindOnboardingLogsWithinTimeRange(ctx, startTime, endTime)
	assert.NoError(t, err)
	assert.NotNil(t, found)
	assert.Len(t, found, 0)
	assert.NoError(t, mock.ExpectationsWereMet())
}
