package storage

import (
	"context"
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
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/tenant"
	"gitlab.com/timkado/api/daisi-wa-events-processor/pkg/utils"
)

const (
	testTenantIDChat = "tenant-chat-test-456"
	testChatIDValue  = "chat-test-123"
)

// Helper to create a mock DB and PostgresRepo instance for testing
func newTestChatRepo(t *testing.T) (*PostgresRepo, sqlmock.Sqlmock) {
	logger.Log = zaptest.NewLogger(t).Named("test")

	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	assert.NoError(t, err)

	gormDB, err := gorm.Open(postgres.New(postgres.Config{
		Conn: db,
	}), &gorm.Config{
		Logger:                 gormLogger.Default.LogMode(gormLogger.Silent),
		SkipDefaultTransaction: true,
	})
	assert.NoError(t, err)

	repo := &PostgresRepo{db: gormDB}
	return repo, mock
}

// Helper to create context with tenant ID
func contextWithTestTenant() context.Context {
	ctx := context.Background()
	ctx = tenant.WithCompanyID(ctx, testTenantIDChat)
	return ctx
}

// --- Chat Repository Tests ---

// TestPostgresRepo_SaveChat_Upsert_New tests saving a new chat record
func TestPostgresRepo_SaveChat_Upsert_New(t *testing.T) {
	repo, mock := newTestChatRepo(t)
	ctx := contextWithTestTenant()

	chat := model.Chat{
		ChatID:                "chat-upsert-new-1",
		Jid:                   "jid-insert",
		CompanyID:             testTenantIDChat,
		PushName:              "Test User",
		LastMessageObj:        datatypes.JSON(utils.MustMarshalJSON(map[string]interface{}{"text": "Hello!"})),
		ConversationTimestamp: time.Now().Unix(),
		AgentID:               "agent-123",
		LastMetadata:          datatypes.JSON(utils.MustMarshalJSON(map[string]interface{}{"platform": "whatsapp"})),
	}

	// GORM v1.25+ ON CONFLICT generates this type of query for chat upsert
	upsertQuery := `INSERT INTO "chats" ("chat_id","jid","custom_name","push_name","is_group","group_name","unread_count","assigned_to","last_message","conversation_timestamp","not_spam","agent_id","company_id","phone_number","last_metadata","created_at","updated_at") VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17) ON CONFLICT ("chat_id") DO UPDATE SET "jid"="excluded"."jid","custom_name"="excluded"."custom_name","push_name"="excluded"."push_name","is_group"="excluded"."is_group","group_name"="excluded"."group_name","unread_count"="excluded"."unread_count","assigned_to"="excluded"."assigned_to","last_message"="excluded"."last_message","conversation_timestamp"="excluded"."conversation_timestamp","not_spam"="excluded"."not_spam","agent_id"="excluded"."agent_id","phone_number"="excluded"."phone_number","last_metadata"="excluded"."last_metadata","updated_at"="excluded"."updated_at" RETURNING "id"`

	mock.ExpectQuery(upsertQuery).
		WithArgs(
			chat.ChatID, chat.Jid, sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), chat.CompanyID, sqlmock.AnyArg(), sqlmock.AnyArg(), AnyTime{}, AnyTime{},
		).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))

	err := repo.SaveChat(ctx, chat)

	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestPostgresRepo_SaveChat_Upsert_Existing tests updating an existing chat via SaveChat
func TestPostgresRepo_SaveChat_Upsert_Existing(t *testing.T) {
	repo, mock := newTestChatRepo(t)
	ctx := contextWithTestTenant()
	now := time.Now()

	chat := model.Chat{
		ChatID:                testChatIDValue,
		Jid:                   "jid-updated@example.com",
		CompanyID:             testTenantIDChat,
		PushName:              "Updated Push Name",
		ConversationTimestamp: now.Unix(),
		AgentID:               "agent-xyz",
	}

	// Expect the single ON CONFLICT query
	upsertQuery := `INSERT INTO "chats" ("chat_id","jid","custom_name","push_name","is_group","group_name","unread_count","assigned_to","last_message","conversation_timestamp","not_spam","agent_id","company_id","phone_number","last_metadata","created_at","updated_at") VALUES ($1,$2,$3,$4,$5,$6,$7,$8,NULL,$9,$10,$11,$12,$13,NULL,$14,$15) ON CONFLICT ("chat_id") DO UPDATE SET "jid"="excluded"."jid","custom_name"="excluded"."custom_name","push_name"="excluded"."push_name","is_group"="excluded"."is_group","group_name"="excluded"."group_name","unread_count"="excluded"."unread_count","assigned_to"="excluded"."assigned_to","last_message"="excluded"."last_message","conversation_timestamp"="excluded"."conversation_timestamp","not_spam"="excluded"."not_spam","agent_id"="excluded"."agent_id","phone_number"="excluded"."phone_number","last_metadata"="excluded"."last_metadata","updated_at"="excluded"."updated_at" RETURNING "id"`

	mock.ExpectQuery(upsertQuery).
		WithArgs(
			chat.ChatID, chat.Jid, chat.PushName, sqlmock.AnyArg(),
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), chat.ConversationTimestamp, sqlmock.AnyArg(),
			chat.AgentID, chat.CompanyID, sqlmock.AnyArg(), AnyTime{}, AnyTime{},
		).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(5)) // Return some existing ID

	err := repo.SaveChat(ctx, chat)

	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestPostgresRepo_SaveChat_TenantMismatch tests saving a chat with mismatched tenant ID
func TestPostgresRepo_SaveChat_TenantMismatch(t *testing.T) {
	repo, mock := newTestChatRepo(t)
	ctx := contextWithTestTenant()

	chat := model.Chat{
		ChatID:    testChatIDValue,
		CompanyID: "different-tenant-id",
	}

	err := repo.SaveChat(ctx, chat)

	assert.Error(t, err)
	assert.ErrorIs(t, err, apperrors.ErrBadRequest)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestPostgresRepo_UpdateChat_Success tests updating a chat
func TestPostgresRepo_UpdateChat_Success(t *testing.T) {
	repo, mock := newTestChatRepo(t)
	ctx := contextWithTestTenant()
	now := time.Now()
	existingCreatedAt := now.Add(-time.Hour)
	existingID := int64(10)

	chat := model.Chat{
		ChatID:      testChatIDValue,
		CompanyID:   testTenantIDChat,
		UnreadCount: 5,
		PhoneNumber: "1234567890",
	}

	// Existing record data
	existingChatCols := []string{"id", "chat_id", "company_id", "unread_count", "assigned_to", "phone_number", "created_at", "updated_at"}
	existingChatRows := sqlmock.NewRows(existingChatCols).
		AddRow(existingID, chat.ChatID, chat.CompanyID, 2, "agent-old", "0987654321", existingCreatedAt, now.Add(-time.Minute))

	mock.ExpectBegin()

	// Expect SELECT FOR UPDATE
	selectQuery := `SELECT * FROM "chats" WHERE chat_id = $1 AND company_id = $2 ORDER BY "chats"."id" LIMIT $3 FOR UPDATE`
	mock.ExpectQuery(selectQuery).
		WithArgs(chat.ChatID, chat.CompanyID, 1).
		WillReturnRows(existingChatRows)

	// Expect UPDATE query - Match actual query which includes id and created_at in SET
	updatePattern := `UPDATE "chats" SET "id"=$1,"chat_id"=$2,"unread_count"=$3,"assigned_to"=$4,"company_id"=$5,"phone_number"=$6,"created_at"=$7,"updated_at"=$8 WHERE "id" = $9`
	mock.ExpectExec(updatePattern).
		WithArgs(
			existingID,        // $1 id (in SET)
			chat.ChatID,       // $2 chat_id
			chat.UnreadCount,  // $3 unread_count
			chat.CompanyID,    // $5 company_id
			chat.PhoneNumber,  // $6 phone_number
			existingCreatedAt, // $7 created_at (in SET)
			AnyTime{},         // $8 updated_at
			existingID,        // $9 WHERE id
		).
		WillReturnResult(sqlmock.NewResult(0, 1))

	mock.ExpectCommit()

	err := repo.UpdateChat(ctx, chat)

	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestPostgresRepo_UpdateChat_NotFound tests updating a non-existent chat
func TestPostgresRepo_UpdateChat_NotFound(t *testing.T) {
	repo, mock := newTestChatRepo(t)
	ctx := contextWithTestTenant()

	chat := model.Chat{
		ChatID:    testChatIDValue,
		CompanyID: testTenantIDChat,
	}

	mock.ExpectBegin()

	selectQuery := `SELECT * FROM "chats" WHERE chat_id = $1 AND company_id = $2 ORDER BY "chats"."id" LIMIT $3 FOR UPDATE`
	mock.ExpectQuery(selectQuery).
		WithArgs(chat.ChatID, chat.CompanyID, 1).
		WillReturnError(gorm.ErrRecordNotFound)

	mock.ExpectRollback()

	err := repo.UpdateChat(ctx, chat)
	assert.Error(t, err)
	assert.ErrorIs(t, err, apperrors.ErrNotFound)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestPostgresRepo_UpdateChat_TenantMismatch tests updating a chat with mismatched tenant ID
func TestPostgresRepo_UpdateChat_TenantMismatch(t *testing.T) {
	repo, mock := newTestChatRepo(t)
	ctx := contextWithTestTenant()

	chat := model.Chat{
		ChatID:    testChatIDValue,
		CompanyID: "different-tenant-id",
	}

	err := repo.UpdateChat(ctx, chat)
	assert.Error(t, err)
	assert.ErrorIs(t, err, apperrors.ErrBadRequest)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestPostgresRepo_FindChatByChatID_Found tests finding a chat by ID
func TestPostgresRepo_FindChatByChatID_Found(t *testing.T) {
	repo, mock := newTestChatRepo(t)
	ctx := contextWithTestTenant()
	now := time.Now()

	// Expected data
	chatCols := []string{"id", "chat_id", "company_id", "jid", "custom_name", "created_at", "updated_at"}
	chatRows := sqlmock.NewRows(chatCols).
		AddRow(15, testChatIDValue, testTenantIDChat, "jid-123", "Test Chat", now.Add(-time.Hour), now.Add(-time.Minute))

	// Expect SELECT query using chat_id and company_id
	selectQuery := `SELECT * FROM "chats" WHERE chat_id = $1 AND company_id = $2 ORDER BY "chats"."id" LIMIT $3`
	mock.ExpectQuery(selectQuery).
		WithArgs(testChatIDValue, testTenantIDChat, 1).
		WillReturnRows(chatRows)

	foundChat, err := repo.FindChatByChatID(ctx, testChatIDValue)

	assert.NoError(t, err)
	assert.NotNil(t, foundChat)
	if foundChat != nil {
		assert.Equal(t, testChatIDValue, foundChat.ChatID)
		assert.Equal(t, testTenantIDChat, foundChat.CompanyID)
		assert.Equal(t, "jid-123", foundChat.Jid)
	}
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestPostgresRepo_FindChatByChatID_NotFound tests finding a non-existent chat by ID
func TestPostgresRepo_FindChatByChatID_NotFound(t *testing.T) {
	repo, mock := newTestChatRepo(t)
	ctx := contextWithTestTenant()

	selectQuery := `SELECT * FROM "chats" WHERE chat_id = $1 AND company_id = $2 ORDER BY "chats"."id" LIMIT $3`
	mock.ExpectQuery(selectQuery).
		WithArgs(testChatIDValue, testTenantIDChat, 1).
		WillReturnError(gorm.ErrRecordNotFound)

	foundChat, err := repo.FindChatByChatID(ctx, testChatIDValue)
	assert.ErrorIs(t, err, apperrors.ErrNotFound)
	assert.Nil(t, foundChat)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestPostgresRepo_FindChatsByJID_Found tests finding chats by JID
func TestPostgresRepo_FindChatsByJID_Found(t *testing.T) {
	repo, mock := newTestChatRepo(t)
	ctx := contextWithTestTenant()
	testJID := "test-jid@example.com"
	now := time.Now()

	// Expected data
	chatCols := []string{"id", "chat_id", "company_id", "jid", "custom_name", "created_at", "updated_at"}
	chatRows := sqlmock.NewRows(chatCols).
		AddRow(20, "chat-1", testTenantIDChat, testJID, "Chat One", now.Add(-2*time.Hour), now.Add(-10*time.Minute)).
		AddRow(21, "chat-2", testTenantIDChat, testJID, "Chat Two", now.Add(-3*time.Hour), now.Add(-5*time.Minute))

	// Expect SELECT query using jid and company_id
	selectQuery := `SELECT * FROM "chats" WHERE jid = $1 AND company_id = $2`
	mock.ExpectQuery(selectQuery).
		WithArgs(testJID, testTenantIDChat).
		WillReturnRows(chatRows)

	foundChats, err := repo.FindChatsByJID(ctx, testJID)

	assert.NoError(t, err)
	assert.NotNil(t, foundChats)
	assert.Len(t, foundChats, 2)
	assert.Equal(t, int64(20), foundChats[0].ID)
	assert.Equal(t, "chat-1", foundChats[0].ChatID)
	assert.Equal(t, testJID, foundChats[0].Jid)
	assert.Equal(t, int64(21), foundChats[1].ID)
	assert.Equal(t, "chat-2", foundChats[1].ChatID)
	assert.Equal(t, testJID, foundChats[1].Jid)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestPostgresRepo_FindChatsByJID_NotFound tests finding no chats by JID
func TestPostgresRepo_FindChatsByJID_NotFound(t *testing.T) {
	repo, mock := newTestChatRepo(t)
	ctx := contextWithTestTenant()
	testJID := "non-existent-jid@example.com"

	// Expect SELECT query
	selectQuery := `SELECT * FROM "chats" WHERE jid = $1 AND company_id = $2`
	mock.ExpectQuery(selectQuery).
		WithArgs(testJID, testTenantIDChat).
		WillReturnRows(sqlmock.NewRows([]string{"id", "chat_id", "company_id", "jid"})) // Return empty rows

	foundChats, err := repo.FindChatsByJID(ctx, testJID)

	// Expect empty slice, nil error
	assert.NoError(t, err)
	assert.NotNil(t, foundChats)
	assert.Len(t, foundChats, 0)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestPostgresRepo_FindChatsByAgentID_Found tests finding chats by Agent ID
func TestPostgresRepo_FindChatsByAgentID_Found(t *testing.T) {
	repo, mock := newTestChatRepo(t)
	ctx := contextWithTestTenant()
	testAgentID := "agent-abc-123"
	now := time.Now()

	// Expected data
	chatCols := []string{"id", "chat_id", "company_id", "jid", "agent_id", "created_at", "updated_at"}
	chatRows := sqlmock.NewRows(chatCols).
		AddRow(30, "chat-agent-1", testTenantIDChat, "jid1", testAgentID, now.Add(-time.Hour), now.Add(-1*time.Minute)).
		AddRow(31, "chat-agent-2", testTenantIDChat, "jid2", testAgentID, now.Add(-2*time.Hour), now.Add(-2*time.Minute))

	// Expect SELECT query using agent_id and company_id
	selectQuery := `SELECT * FROM "chats" WHERE agent_id = $1 AND company_id = $2`
	mock.ExpectQuery(selectQuery).
		WithArgs(testAgentID, testTenantIDChat).
		WillReturnRows(chatRows)

	foundChats, err := repo.FindChatsByAgentID(ctx, testAgentID)

	assert.NoError(t, err)
	assert.NotNil(t, foundChats)
	assert.Len(t, foundChats, 2)
	assert.Equal(t, int64(30), foundChats[0].ID)
	assert.Equal(t, "chat-agent-1", foundChats[0].ChatID)
	assert.Equal(t, testAgentID, foundChats[0].AgentID)
	assert.Equal(t, int64(31), foundChats[1].ID)
	assert.Equal(t, "chat-agent-2", foundChats[1].ChatID)
	assert.Equal(t, testAgentID, foundChats[1].AgentID)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestPostgresRepo_FindChatsByAgentID_NotFound tests finding no chats by Agent ID
func TestPostgresRepo_FindChatsByAgentID_NotFound(t *testing.T) {
	repo, mock := newTestChatRepo(t)
	ctx := contextWithTestTenant()
	testAgentID := "non-existent-agent"

	// Expect SELECT query
	selectQuery := `SELECT * FROM "chats" WHERE agent_id = $1 AND company_id = $2`
	mock.ExpectQuery(selectQuery).
		WithArgs(testAgentID, testTenantIDChat).
		WillReturnRows(sqlmock.NewRows([]string{"id", "chat_id", "company_id", "agent_id"})) // Empty rows

	foundChats, err := repo.FindChatsByAgentID(ctx, testAgentID)

	// Expect empty slice, nil error
	assert.NoError(t, err)
	assert.NotNil(t, foundChats)
	assert.Len(t, foundChats, 0)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestPostgresRepo_BulkUpsertChats_Success tests bulk upserting chats
func TestPostgresRepo_BulkUpsertChats_Success(t *testing.T) {
	// Use QueryMatcherEqual for bulk upsert as the argument order is fixed now
	repo, mock := newTestChatRepo(t)
	ctx := contextWithTestTenant()
	now := time.Now()

	chats := []model.Chat{
		{ChatID: "bulk-chat-1", Jid: "jid1@example.com", CompanyID: testTenantIDChat, UpdatedAt: now, AgentID: "agent-bulk", LastMessageObj: datatypes.JSON(`{"text":"hello 1"}`), LastMetadata: datatypes.JSON(`{"platform":"test2"}`)}, // Add JSON
		{ChatID: "bulk-chat-2", Jid: "jid2@example.com", CompanyID: testTenantIDChat, UpdatedAt: now, AgentID: "agent-bulk", LastMessageObj: datatypes.JSON(`{"text":"hello 2"}`), LastMetadata: datatypes.JSON(`{"platform":"test"}`)},  // Add JSON
	}

	mock.ExpectBegin()

	// Define the EXACT INSERT ... ON CONFLICT query string
	upsertQuery := `INSERT INTO "chats" ("chat_id","jid","custom_name","push_name","is_group","group_name","unread_count","assigned_to","last_message","conversation_timestamp","not_spam","agent_id","company_id","phone_number","last_metadata","created_at","updated_at") VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17),($18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34) ON CONFLICT ("chat_id") DO UPDATE SET "jid"="excluded"."jid","custom_name"="excluded"."custom_name","push_name"="excluded"."push_name","is_group"="excluded"."is_group","group_name"="excluded"."group_name","unread_count"="excluded"."unread_count","assigned_to"="excluded"."assigned_to","last_message"="excluded"."last_message","conversation_timestamp"="excluded"."conversation_timestamp","not_spam"="excluded"."not_spam","agent_id"="excluded"."agent_id","phone_number"="excluded"."phone_number","last_metadata"="excluded"."last_metadata","updated_at"="excluded"."updated_at" RETURNING "id"`

	// Expect a single Query call because of RETURNING "id"
	mock.ExpectQuery(upsertQuery).
		WithArgs(
			chats[0].ChatID,
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			chats[1].ChatID,
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
			sqlmock.AnyArg(),
		).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1).AddRow(2)) // Expect rows with IDs for each upserted record

	mock.ExpectCommit()

	err := repo.BulkUpsertChats(ctx, chats)

	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestPostgresRepo_BulkUpsertChats_SkipMismatchedTenant tests that chats with wrong company ID are skipped
func TestPostgresRepo_BulkUpsertChats_SkipMismatchedTenant(t *testing.T) {
	repo, mock := newTestChatRepo(t)
	ctx := contextWithTestTenant()
	now := time.Now()

	chats := []model.Chat{
		{ChatID: "bulk-chat-ok-1", CompanyID: testTenantIDChat, UpdatedAt: now, Jid: "jid-ok-1"},
		{ChatID: "bulk-chat-wrong", CompanyID: "wrong-tenant-id", UpdatedAt: now, Jid: "jid-wrong"},
		{ChatID: "bulk-chat-ok-2", CompanyID: testTenantIDChat, UpdatedAt: now, Jid: "jid-ok-2"},
	}

	mock.ExpectBegin()

	// Define the EXACT INSERT ... ON CONFLICT query string FOR VALID CHATS ONLY
	upsertQuery := `INSERT INTO "chats" ("chat_id","jid","custom_name","push_name","is_group","group_name","unread_count","assigned_to","last_message","conversation_timestamp","not_spam","agent_id","company_id","phone_number","last_metadata","created_at","updated_at") VALUES ($1,$2,$3,$4,$5,$6,$7,$8,NULL,$9,$10,$11,$12,$13,NULL,$14,$15),($16,$17,$18,$19,$20,$21,$22,$23,NULL,$24,$25,$26,$27,$28,NULL,$29,$30) ON CONFLICT ("chat_id") DO UPDATE SET "jid"="excluded"."jid","custom_name"="excluded"."custom_name","push_name"="excluded"."push_name","is_group"="excluded"."is_group","group_name"="excluded"."group_name","unread_count"="excluded"."unread_count","assigned_to"="excluded"."assigned_to","last_message"="excluded"."last_message","conversation_timestamp"="excluded"."conversation_timestamp","not_spam"="excluded"."not_spam","agent_id"="excluded"."agent_id","phone_number"="excluded"."phone_number","last_metadata"="excluded"."last_metadata","updated_at"="excluded"."updated_at" RETURNING "id"`

	// Expect a single Query call for the bulk upsert of valid chats
	mock.ExpectQuery(upsertQuery).
		WithArgs(
			chats[0].ChatID,
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
			chats[2].ChatID,
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
		).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1).AddRow(2)) // Expect rows with IDs

	mock.ExpectCommit()

	err := repo.BulkUpsertChats(ctx, chats) // Pass original list

	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestPostgresRepo_BulkUpsertChats_EmptyList tests bulk upserting with an empty list
func TestPostgresRepo_BulkUpsertChats_EmptyList(t *testing.T) {
	repo, mock := newTestChatRepo(t)
	ctx := contextWithTestTenant()

	var chats []model.Chat // Empty list

	// No DB calls should be expected
	err := repo.BulkUpsertChats(ctx, chats)

	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet()) // Ensures no Begin/Commit/Exec was called
}
