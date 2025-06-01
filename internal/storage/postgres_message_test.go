package storage

import (
	"regexp"
	"testing"
	"time"

	apperrors "gitlab.com/timkado/api/daisi-wa-events-processor/internal/apperrors"
	"gitlab.com/timkado/api/daisi-wa-events-processor/pkg/logger"
	"go.uber.org/zap/zaptest"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"gorm.io/datatypes"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	gormLogger "gorm.io/gorm/logger"

	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/model"
	"gitlab.com/timkado/api/daisi-wa-events-processor/pkg/utils"
)

func newTestMessageRepo(t *testing.T) (*PostgresRepo, sqlmock.Sqlmock, func()) {
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

func newTestMessageRepoWithMatcher(t *testing.T, match sqlmock.QueryMatcher) (*PostgresRepo, sqlmock.Sqlmock, func()) {
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

func TestPostgresRepo_SaveMessage_Upsert_New(t *testing.T) {
	repo, mock, teardown := newTestMessageRepoWithMatcher(t, sqlmock.QueryMatcherEqual)
	t.Cleanup(teardown)
	ctx := contextWithTestTenant()
	message := model.Message{
		MessageID:        "message-upsert-new-1",
		FromUser:         "sender1",
		ToUser:           "receiver1",
		ChatID:           testChatID,
		CompanyID:        testTenantIDChat,
		MessageTimestamp: time.Now().Unix(),
		MessageObj:       datatypes.JSON(utils.MustMarshalJSON(map[string]interface{}{"text": "Hello New"})),
	}

	insertQuery := `INSERT INTO "messages" ("message_id","from_user","to_user","chat_id","jid","flow","agent_id","company_id","message_obj","key","status","is_deleted","message_timestamp","message_date","created_at","updated_at","last_metadata") VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,NULL,$10,$11,$12,$13,$14,$15,NULL) ON CONFLICT ("message_id","message_date") DO UPDATE SET "message_id"="excluded"."message_id","from_user"="excluded"."from_user","to_user"="excluded"."to_user","chat_id"="excluded"."chat_id","jid"="excluded"."jid","flow"="excluded"."flow","agent_id"="excluded"."agent_id","company_id"="excluded"."company_id","message_obj"="excluded"."message_obj","key"="excluded"."key","status"="excluded"."status","is_deleted"="excluded"."is_deleted","message_timestamp"="excluded"."message_timestamp","message_date"="excluded"."message_date","updated_at"="excluded"."updated_at","last_metadata"="excluded"."last_metadata" RETURNING "id"`

	mock.ExpectQuery(insertQuery).
		WithArgs(
			message.MessageID, message.FromUser, message.ToUser, message.ChatID,
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), message.CompanyID,
			message.MessageObj, sqlmock.AnyArg(), false, message.MessageTimestamp, AnyTime{}, AnyTime{}, AnyTime{},
		).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))

	err := repo.SaveMessage(ctx, message)
	assert.NoError(t, err)
}

func TestPostgresRepo_SaveMessage_Upsert_Existing(t *testing.T) {
	repo, mock, teardown := newTestMessageRepoWithMatcher(t, sqlmock.QueryMatcherEqual)
	t.Cleanup(teardown)
	ctx := contextWithTestTenant()
	now := time.Now()

	message := model.Message{
		MessageID:        "message-upsert-existing-1",
		FromUser:         "sender1",
		ToUser:           "receiver1",
		ChatID:           testChatID,
		CompanyID:        testTenantIDChat,
		MessageTimestamp: now.Unix(),
		Status:           "updated",
		MessageObj:       datatypes.JSON(utils.MustMarshalJSON(map[string]interface{}{"text": "Hello Updated"})),
	}

	insertQuery := `INSERT INTO "messages" ("message_id","from_user","to_user","chat_id","jid","flow","agent_id","company_id","message_obj","key","status","is_deleted","message_timestamp","message_date","created_at","updated_at","last_metadata") VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,NULL,$10,$11,$12,$13,$14,$15,NULL) ON CONFLICT ("message_id","message_date") DO UPDATE SET "message_id"="excluded"."message_id","from_user"="excluded"."from_user","to_user"="excluded"."to_user","chat_id"="excluded"."chat_id","jid"="excluded"."jid","flow"="excluded"."flow","agent_id"="excluded"."agent_id","company_id"="excluded"."company_id","message_obj"="excluded"."message_obj","key"="excluded"."key","status"="excluded"."status","is_deleted"="excluded"."is_deleted","message_timestamp"="excluded"."message_timestamp","message_date"="excluded"."message_date","updated_at"="excluded"."updated_at","last_metadata"="excluded"."last_metadata" RETURNING "id"`

	mock.ExpectQuery(insertQuery).
		WithArgs(
			message.MessageID, message.FromUser, message.ToUser, message.ChatID,
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), message.CompanyID,
			message.MessageObj, message.Status, false, message.MessageTimestamp, AnyTime{}, AnyTime{}, AnyTime{},
		).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))

	err := repo.SaveMessage(ctx, message)
	assert.NoError(t, err)
}

func TestPostgresRepo_SaveMessage_TenantMismatch(t *testing.T) {
	repo, _, teardown := newTestMessageRepoWithMatcher(t, sqlmock.QueryMatcherEqual)
	t.Cleanup(teardown)
	ctx := contextWithTestTenant()
	message := model.Message{MessageID: "message-tenant-mismatch", CompanyID: "wrong-tenant"}
	err := repo.SaveMessage(ctx, message)
	assert.Error(t, err)
	assert.ErrorIs(t, err, apperrors.ErrBadRequest)
}

func TestPostgresRepo_UpdateMessage_Success(t *testing.T) {
	repo, mock, teardown := newTestMessageRepo(t)
	t.Cleanup(teardown)
	ctx := contextWithTestTenant()
	now := time.Now()
	existingCreatedAt := now.Add(-time.Hour)
	existingID := int64(1)

	message := model.Message{MessageID: "message-update-success", CompanyID: testTenantIDChat, Status: "updated"}
	existingCols := []string{"id", "message_id", "company_id", "created_at", "updated_at", "message_date"}
	existingRows := sqlmock.NewRows(existingCols).AddRow(existingID, message.MessageID, message.CompanyID, existingCreatedAt, now.Add(-time.Minute), now)

	mock.ExpectBegin()
	selectQuery := `SELECT * FROM "messages" WHERE message_id = $1 AND company_id = $2 ORDER BY "messages"."id" LIMIT $3 FOR UPDATE`
	mock.ExpectQuery(selectQuery).WithArgs(message.MessageID, message.CompanyID, 1).WillReturnRows(existingRows)

	updatePattern := `UPDATE "messages" SET "id"=$1,"message_id"=$2,"company_id"=$3,"status"=$4,"created_at"=$5,"updated_at"=$6 WHERE "id" = $7`
	mock.ExpectExec(updatePattern).
		WithArgs(
			existingID,        // $1 id (in SET)
			message.MessageID, // $2 message_id
			message.CompanyID, // $3 company_id
			message.Status,    // $4 status
			existingCreatedAt, // $5 created_at (in SET)
			AnyTime{},         // $6 updated_at
			existingID,        // $7 WHERE id
		).
		WillReturnResult(sqlmock.NewResult(0, 1))

	mock.ExpectCommit()
	err := repo.UpdateMessage(ctx, message)
	assert.NoError(t, err)
}

func TestPostgresRepo_UpdateMessage_NotFound(t *testing.T) {
	repo, mock, teardown := newTestMessageRepoWithMatcher(t, sqlmock.QueryMatcherRegexp)
	t.Cleanup(teardown)
	ctx := contextWithTestTenant()
	message := model.Message{MessageID: "message-update-notfound", CompanyID: testTenantIDChat, Status: "updated"}

	mock.ExpectBegin()
	selectQuery := `SELECT * FROM "messages" WHERE message_id = $1 AND company_id = $2 ORDER BY "messages"."id" LIMIT $3 FOR UPDATE`
	mock.ExpectQuery(regexp.QuoteMeta(selectQuery)).WithArgs(message.MessageID, message.CompanyID, 1).WillReturnError(gorm.ErrRecordNotFound)
	mock.ExpectRollback()

	err := repo.UpdateMessage(ctx, message)

	assert.Error(t, err)
	assert.ErrorIs(t, err, apperrors.ErrNotFound)
}

func TestPostgresRepo_UpdateMessage_TenantMismatch(t *testing.T) {
	repo, _, teardown := newTestMessageRepo(t)
	t.Cleanup(teardown)
	ctx := contextWithTestTenant()
	message := model.Message{MessageID: "message-tenant-mismatch", CompanyID: "wrong-tenant"}
	err := repo.UpdateMessage(ctx, message)
	assert.Error(t, err)
	assert.ErrorIs(t, err, apperrors.ErrBadRequest)
}

func TestPostgresRepo_FindMessageByMessageID_Found(t *testing.T) {
	repo, mock, teardown := newTestMessageRepo(t)
	t.Cleanup(teardown)
	ctx := contextWithTestTenant()
	now := time.Now()
	messageID := "message-find-found-100"
	expectedMessage := model.Message{
		ID:               100,
		MessageID:        messageID,
		CompanyID:        testTenantIDChat,
		FromUser:         "sender-find",
		ToUser:           "receiver-find",
		ChatID:           "chat-find",
		Jid:              "jid-find",
		MessageTimestamp: now.Unix(),
		CreatedAt:        now.Add(-time.Hour),
		UpdatedAt:        now.Add(-time.Minute),
		MessageDate:      now.Truncate(24 * time.Hour),
		MessageObj:       datatypes.JSON(`{"key": "value"}`),
	}

	cols := []string{"id", "message_id", "company_id", "from_user", "to_user", "chat_id", "jid", "message_timestamp", "created_at", "updated_at", "message_date", "message_obj"}
	rows := sqlmock.NewRows(cols).
		AddRow(expectedMessage.ID, expectedMessage.MessageID, expectedMessage.CompanyID, expectedMessage.FromUser, expectedMessage.ToUser, expectedMessage.ChatID, expectedMessage.Jid, expectedMessage.MessageTimestamp, expectedMessage.CreatedAt, expectedMessage.UpdatedAt, expectedMessage.MessageDate, expectedMessage.MessageObj)

	selectQuery := `SELECT * FROM "messages" WHERE message_id = $1 AND company_id = $2 ORDER BY "messages"."id" LIMIT $3`
	mock.ExpectQuery(selectQuery).WithArgs(messageID, testTenantIDChat, 1).WillReturnRows(rows)

	foundMessage, err := repo.FindMessageByMessageID(ctx, messageID)

	assert.NoError(t, err)
	assert.NotNil(t, foundMessage)
	assert.Equal(t, expectedMessage.ID, foundMessage.ID)
	assert.Equal(t, expectedMessage.MessageID, foundMessage.MessageID)
	assert.Equal(t, expectedMessage.CompanyID, foundMessage.CompanyID)
	assert.Equal(t, expectedMessage.FromUser, foundMessage.FromUser)
	assert.Equal(t, expectedMessage.ToUser, foundMessage.ToUser)
	assert.Equal(t, expectedMessage.Jid, foundMessage.Jid)
	assert.Equal(t, expectedMessage.MessageTimestamp, foundMessage.MessageTimestamp)
	assert.JSONEq(t, string(expectedMessage.MessageObj), string(foundMessage.MessageObj))
}

func TestPostgresRepo_FindMessageByMessageID_NotFound(t *testing.T) {
	repo, mock, teardown := newTestMessageRepo(t)
	t.Cleanup(teardown)
	ctx := contextWithTestTenant()
	selectQuery := `SELECT * FROM "messages" WHERE message_id = $1 AND company_id = $2 ORDER BY "messages"."id" LIMIT $3`
	mock.ExpectQuery(selectQuery).WithArgs("message-id-404", testTenantIDChat, 1).WillReturnError(gorm.ErrRecordNotFound)
	found, err := repo.FindMessageByMessageID(ctx, "message-id-404")
	assert.ErrorIs(t, err, apperrors.ErrNotFound)
	assert.Nil(t, found)
}

func TestPostgresRepo_BulkUpsertMessages_Success(t *testing.T) {
	repo, mock, teardown := newTestMessageRepo(t)
	t.Cleanup(teardown)
	ctx := contextWithTestTenant()
	now := time.Now()
	messages := []model.Message{
		{MessageID: "bulk-message-1", CompanyID: testTenantIDChat, FromUser: "sender1", MessageTimestamp: now.Unix(), MessageObj: datatypes.JSON(`{"a":1}`), LastMetadata: datatypes.JSON(`{"b":2}`), MessageDate: model.CreateTimeFromTimestamp(now.Unix())},
		{MessageID: "bulk-message-2", CompanyID: testTenantIDChat, ToUser: "sender2", MessageTimestamp: now.Unix(), MessageObj: datatypes.JSON(`{"a":1}`), LastMetadata: datatypes.JSON(`{"b":2}`), MessageDate: model.CreateTimeFromTimestamp(now.Unix())},
	}
	mock.ExpectBegin()

	insertQuery := `INSERT INTO "messages" ("message_id","from_user","to_user","chat_id","jid","flow","agent_id","company_id","message_obj","key","status","is_deleted","message_timestamp","message_date","created_at","updated_at","last_metadata") VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,NULL,$10,$11,$12,$13,$14,$15,$16),($17,$18,$19,$20,$21,$22,$23,$24,$25,NULL,$26,$27,$28,$29,$30,$31,$32) ON CONFLICT ("message_id","message_date") DO UPDATE SET "message_id"="excluded"."message_id","from_user"="excluded"."from_user","to_user"="excluded"."to_user","chat_id"="excluded"."chat_id","jid"="excluded"."jid","flow"="excluded"."flow","agent_id"="excluded"."agent_id","company_id"="excluded"."company_id","message_obj"="excluded"."message_obj","key"="excluded"."key","status"="excluded"."status","is_deleted"="excluded"."is_deleted","message_timestamp"="excluded"."message_timestamp","message_date"="excluded"."message_date","updated_at"="excluded"."updated_at","last_metadata"="excluded"."last_metadata" RETURNING "id"`

	mock.ExpectQuery(insertQuery).WithArgs(
		messages[0].MessageID,
		sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
		sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
		sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
		messages[1].MessageID,
		sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
		sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
		sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
	).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1).AddRow(2)) // Expect rows with IDs for each upserted record

	mock.ExpectCommit()
	err := repo.BulkUpsertMessages(ctx, messages)
	assert.NoError(t, err)
}

func TestPostgresRepo_BulkUpsertMessages_SkipMismatchedTenant(t *testing.T) {
	repo, mock, teardown := newTestMessageRepo(t)
	t.Cleanup(teardown)
	ctx := contextWithTestTenant()
	now := time.Now()
	messages := []model.Message{
		{MessageID: "bulk-message-ok-1", CompanyID: testTenantIDChat, FromUser: "sender1", MessageTimestamp: now.Unix()},
		{MessageID: "bulk-message-wrong", CompanyID: "wrong-tenant", FromUser: "sender2", MessageTimestamp: now.Unix()},
		{MessageID: "bulk-message-ok-2", CompanyID: testTenantIDChat, FromUser: "sender3", MessageTimestamp: now.Unix()},
	}

	mock.ExpectBegin()

	insertQuery := `INSERT INTO "messages" ("message_id","from_user","to_user","chat_id","jid","flow","agent_id","company_id","message_obj","key","status","is_deleted","message_timestamp","message_date","created_at","updated_at","last_metadata") VALUES ($1,$2,$3,$4,$5,$6,$7,$8,NULL,NULL,$9,$10,$11,$12,$13,$14,NULL),($15,$16,$17,$18,$19,$20,$21,$22,NULL,NULL,$23,$24,$25,$26,$27,$28,NULL) ON CONFLICT ("message_id","message_date") DO UPDATE SET "message_id"="excluded"."message_id","from_user"="excluded"."from_user","to_user"="excluded"."to_user","chat_id"="excluded"."chat_id","jid"="excluded"."jid","flow"="excluded"."flow","agent_id"="excluded"."agent_id","company_id"="excluded"."company_id","message_obj"="excluded"."message_obj","key"="excluded"."key","status"="excluded"."status","is_deleted"="excluded"."is_deleted","message_timestamp"="excluded"."message_timestamp","message_date"="excluded"."message_date","updated_at"="excluded"."updated_at","last_metadata"="excluded"."last_metadata" RETURNING "id"`

	mock.ExpectQuery(insertQuery).WithArgs(
		messages[0].MessageID,
		sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
		sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
		sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
		messages[2].MessageID,
		sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
		sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
		sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
	).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1).AddRow(2))

	mock.ExpectCommit()
	err := repo.BulkUpsertMessages(ctx, messages)
	assert.NoError(t, err)
}

func TestPostgresRepo_BulkUpsertMessages_EmptyList(t *testing.T) {
	repo, _, teardown := newTestMessageRepo(t)
	t.Cleanup(teardown)
	ctx := contextWithTestTenant()
	var messages []model.Message
	err := repo.BulkUpsertMessages(ctx, messages)
	assert.NoError(t, err)
}

func TestPostgresRepo_FindMessagesByJID_Found(t *testing.T) {
	repo, mock, teardown := newTestMessageRepo(t)
	t.Cleanup(teardown)
	ctx := contextWithTestTenant()
	now := time.Now()
	jid := "jid-test-1"
	startDate := now.AddDate(0, -1, 0)
	endDate := now.AddDate(0, 1, 0)
	limit := 10
	offset := 0
	cols := []string{"id", "message_id", "company_id", "jid", "created_at", "updated_at", "message_date"}
	rows := sqlmock.NewRows(cols).
		AddRow(1, "message-jid-1", testTenantIDChat, jid, now.Add(-time.Hour), now.Add(-time.Minute), now).
		AddRow(2, "message-jid-2", testTenantIDChat, jid, now.Add(-2*time.Hour), now.Add(-2*time.Minute), now)

	selectQuery := `SELECT * FROM "messages" WHERE jid = $1 AND company_id = $2 AND message_date >= $3 AND message_date <= $4 ORDER BY message_timestamp DESC LIMIT $5`
	mock.ExpectQuery(selectQuery).WithArgs(jid, testTenantIDChat, startDate.Format("2006-01-02"), endDate.Format("2006-01-02"), limit).WillReturnRows(rows)

	found, err := repo.FindMessagesByJID(ctx, jid, startDate, endDate, limit, offset)
	assert.NoError(t, err)
	assert.NotNil(t, found)
	assert.Len(t, found, 2)
	assert.Equal(t, "message-jid-1", found[0].MessageID)
	assert.Equal(t, "message-jid-2", found[1].MessageID)
}

func TestPostgresRepo_FindMessagesByJID_NotFound(t *testing.T) {
	repo, mock, teardown := newTestMessageRepoWithMatcher(t, sqlmock.QueryMatcherEqual)
	t.Cleanup(teardown)
	ctx := contextWithTestTenant()
	now := time.Now()
	jid := "jid-notfound"
	startDate := now.AddDate(0, -1, 0)
	endDate := now.AddDate(0, 1, 0)
	limit := 10
	offset := 0
	selectQuery := `SELECT * FROM "messages" WHERE jid = $1 AND company_id = $2 AND message_date >= $3 AND message_date <= $4 ORDER BY message_timestamp DESC LIMIT $5`
	mock.ExpectQuery(selectQuery).
		WithArgs(
			jid, testTenantIDChat, startDate.Format("2006-01-02"), endDate.Format("2006-01-02"), limit,
		).WillReturnRows(sqlmock.NewRows([]string{"id", "message_id", "company_id", "jid"}))
	found, err := repo.FindMessagesByJID(ctx, jid, startDate, endDate, limit, offset)
	assert.NoError(t, err)
	assert.NotNil(t, found)
	assert.Len(t, found, 0)
}

func TestPostgresRepo_FindMessagesBySender_Found(t *testing.T) {
	repo, mock, teardown := newTestMessageRepoWithMatcher(t, sqlmock.QueryMatcherEqual)
	t.Cleanup(teardown)
	ctx := contextWithTestTenant()
	now := time.Now()
	sender := testSender
	startDate := now.AddDate(0, -1, 0)
	endDate := now.AddDate(0, 1, 0)
	limit := 10
	offset := 0
	cols := []string{"id", "message_id", "company_id", "from_user", "created_at", "updated_at", "message_date"}
	rows := sqlmock.NewRows(cols).
		AddRow(1, "message-sender-1", testTenantIDChat, sender, now.Add(-time.Hour), now.Add(-time.Minute), now).
		AddRow(2, "message-sender-2", testTenantIDChat, sender, now.Add(-2*time.Hour), now.Add(-2*time.Minute), now)
	selectQuery := `SELECT * FROM "messages" WHERE "from_user" = $1 AND company_id = $2 AND message_date >= $3 AND message_date <= $4 ORDER BY message_timestamp DESC LIMIT $5`
	mock.ExpectQuery(selectQuery).WithArgs(sender, testTenantIDChat, startDate.Format("2006-01-02"), endDate.Format("2006-01-02"), limit).WillReturnRows(rows)
	found, err := repo.FindMessagesBySender(ctx, sender, startDate, endDate, limit, offset)
	assert.NoError(t, err)
	assert.NotNil(t, found)
	assert.Len(t, found, 2)
	assert.Equal(t, "message-sender-1", found[0].MessageID)
	assert.Equal(t, "message-sender-2", found[1].MessageID)
}

func TestPostgresRepo_FindMessagesBySender_NotFound(t *testing.T) {
	repo, mock, teardown := newTestMessageRepoWithMatcher(t, sqlmock.QueryMatcherEqual)
	t.Cleanup(teardown)
	ctx := contextWithTestTenant()
	now := time.Now()
	sender := "sender-notfound"
	startDate := now.AddDate(0, -1, 0)
	endDate := now.AddDate(0, 1, 0)
	limit := 10
	offset := 0
	selectQuery := `SELECT * FROM "messages" WHERE "from_user" = $1 AND company_id = $2 AND message_date >= $3 AND message_date <= $4 ORDER BY message_timestamp DESC LIMIT $5`
	mock.ExpectQuery(selectQuery).WithArgs(sender, testTenantIDChat, startDate.Format("2006-01-02"), endDate.Format("2006-01-02"), limit).WillReturnRows(sqlmock.NewRows([]string{"id", "message_id", "company_id", "from_user"}))
	found, err := repo.FindMessagesBySender(ctx, sender, startDate, endDate, limit, offset)
	assert.NoError(t, err)
	assert.NotNil(t, found)
	assert.Len(t, found, 0)
}

func TestPostgresRepo_FindMessagesByFromUser_Found(t *testing.T) {
	repo, mock, teardown := newTestMessageRepoWithMatcher(t, sqlmock.QueryMatcherEqual)
	t.Cleanup(teardown)
	ctx := contextWithTestTenant()
	now := time.Now()
	fromUser := "user-from_user-test"
	startDate := now.AddDate(0, -1, 0)
	endDate := now.AddDate(0, 1, 0)
	limit := 10
	offset := 0
	cols := []string{"id", "message_id", "company_id", "from_user", "created_at", "updated_at", "message_date"}
	rows := sqlmock.NewRows(cols).
		AddRow(1, "message-from_user-1", testTenantIDChat, fromUser, now.Add(-time.Hour), now.Add(-time.Minute), now).
		AddRow(2, "message-from_user-2", testTenantIDChat, fromUser, now.Add(-2*time.Hour), now.Add(-2*time.Minute), now)
	selectQuery := `SELECT * FROM "messages" WHERE "from_user" = $1 AND company_id = $2 AND message_date >= $3 AND message_date <= $4 ORDER BY message_timestamp DESC LIMIT $5`
	mock.ExpectQuery(selectQuery).WithArgs(fromUser, testTenantIDChat, startDate.Format("2006-01-02"), endDate.Format("2006-01-02"), limit).WillReturnRows(rows)
	found, err := repo.FindMessagesByFromUser(ctx, fromUser, startDate, endDate, limit, offset)
	assert.NoError(t, err)
	assert.NotNil(t, found)
	assert.Len(t, found, 2)
	assert.Equal(t, "message-from_user-1", found[0].MessageID)
	assert.Equal(t, "message-from_user-2", found[1].MessageID)
}

func TestPostgresRepo_FindMessagesByFromUser_NotFound(t *testing.T) {
	repo, mock, teardown := newTestMessageRepoWithMatcher(t, sqlmock.QueryMatcherEqual)
	t.Cleanup(teardown)
	ctx := contextWithTestTenant()
	now := time.Now()
	fromUser := "user-from_user-notfound"
	startDate := now.AddDate(0, -1, 0)
	endDate := now.AddDate(0, 1, 0)
	limit := 10
	offset := 0
	selectQuery := `SELECT * FROM "messages" WHERE "from_user" = $1 AND company_id = $2 AND message_date >= $3 AND message_date <= $4 ORDER BY message_timestamp DESC LIMIT $5`
	mock.ExpectQuery(selectQuery).WithArgs(fromUser, testTenantIDChat, startDate.Format("2006-01-02"), endDate.Format("2006-01-02"), limit).WillReturnRows(sqlmock.NewRows([]string{"id", "message_id", "company_id", "from_user"}))
	found, err := repo.FindMessagesByFromUser(ctx, fromUser, startDate, endDate, limit, offset)
	assert.NoError(t, err)
	assert.NotNil(t, found)
	assert.Len(t, found, 0)
}

func TestPostgresRepo_FindMessagesByToUser_Found(t *testing.T) {
	repo, mock, teardown := newTestMessageRepoWithMatcher(t, sqlmock.QueryMatcherEqual)
	t.Cleanup(teardown)
	ctx := contextWithTestTenant()
	now := time.Now()
	toUser := "user-to_user-test"
	startDate := now.AddDate(0, -1, 0)
	endDate := now.AddDate(0, 1, 0)
	limit := 10
	offset := 0
	cols := []string{"id", "message_id", "company_id", "to_user", "created_at", "updated_at", "message_date"}
	rows := sqlmock.NewRows(cols).
		AddRow(1, "message-to_user-1", testTenantIDChat, toUser, now.Add(-time.Hour), now.Add(-time.Minute), now).
		AddRow(2, "message-to_user-2", testTenantIDChat, toUser, now.Add(-2*time.Hour), now.Add(-2*time.Minute), now)
	selectQuery := `SELECT * FROM "messages" WHERE "to_user" = $1 AND company_id = $2 AND message_date >= $3 AND message_date <= $4 ORDER BY message_timestamp DESC LIMIT $5`
	mock.ExpectQuery(selectQuery).WithArgs(toUser, testTenantIDChat, startDate.Format("2006-01-02"), endDate.Format("2006-01-02"), limit).WillReturnRows(rows)
	found, err := repo.FindMessagesByToUser(ctx, toUser, startDate, endDate, limit, offset)
	assert.NoError(t, err)
	assert.NotNil(t, found)
	assert.Len(t, found, 2)
	assert.Equal(t, "message-to_user-1", found[0].MessageID)
	assert.Equal(t, "message-to_user-2", found[1].MessageID)
}

func TestPostgresRepo_FindMessagesByToUser_NotFound(t *testing.T) {
	repo, mock, teardown := newTestMessageRepoWithMatcher(t, sqlmock.QueryMatcherEqual)
	t.Cleanup(teardown)
	ctx := contextWithTestTenant()
	now := time.Now()
	toUser := "user-to_user-notfound"
	startDate := now.AddDate(0, -1, 0)
	endDate := now.AddDate(0, 1, 0)
	limit := 10
	offset := 0
	selectQuery := `SELECT * FROM "messages" WHERE "to_user" = $1 AND company_id = $2 AND message_date >= $3 AND message_date <= $4 ORDER BY message_timestamp DESC LIMIT $5`
	mock.ExpectQuery(selectQuery).WithArgs(toUser, testTenantIDChat, startDate.Format("2006-01-02"), endDate.Format("2006-01-02"), limit).WillReturnRows(sqlmock.NewRows([]string{"id", "message_id", "company_id", "to_user"}))
	found, err := repo.FindMessagesByToUser(ctx, toUser, startDate, endDate, limit, offset)
	assert.NoError(t, err)
	assert.NotNil(t, found)
	assert.Len(t, found, 0)
}
