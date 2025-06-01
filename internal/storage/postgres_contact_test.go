package storage

import (
	"context"
	"encoding/json"
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

const testTenantIDContact = "tenant-contact-test-789"

func newTestContactRepo(t *testing.T) (*PostgresRepo, sqlmock.Sqlmock) {
	logger.Log = zaptest.NewLogger(t).Named("test")
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	assert.NoError(t, err)
	gormDB, err := gorm.Open(postgres.New(postgres.Config{Conn: db}), &gorm.Config{
		Logger:                 gormLogger.Default.LogMode(gormLogger.Silent),
		SkipDefaultTransaction: true,
	})
	assert.NoError(t, err)
	return &PostgresRepo{db: gormDB}, mock
}

func contextWithContactTenant() context.Context {
	ctx := context.Background()
	ctx = tenant.WithCompanyID(ctx, testTenantIDContact)
	return ctx
}

func TestPostgresRepo_SaveContact_Insert(t *testing.T) {
	repo, mock := newTestContactRepo(t)
	ctx := contextWithContactTenant()
	contact := model.Contact{
		ID:                    "contact-insert-1",
		PhoneNumber:           "08123456789",
		CompanyID:             testTenantIDContact,
		CustomName:            "Insert Contact",
		LastMetadata:          datatypes.JSON(utils.MustMarshalJSON(map[string]interface{}{"insert": true})),
		FirstMessageTimestamp: time.Now().Unix(),
	}
	mock.ExpectBegin()
	selectQuery := `SELECT * FROM "contacts" WHERE id = $1 AND company_id = $2 ORDER BY "contacts"."id" LIMIT $3 FOR UPDATE`
	mock.ExpectQuery(selectQuery).WithArgs(contact.ID, contact.CompanyID, 1).WillReturnError(gorm.ErrRecordNotFound)
	insertPattern := `INSERT INTO "contacts" ("id","phone_number","type","custom_name","notes","tags","company_id","avatar","assigned_to","pob","dob","gender","origin","push_name","status","agent_id","first_message_id","first_message_timestamp","created_at","updated_at","last_metadata") VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21)`
	mock.ExpectExec(insertPattern).
		WithArgs(
			contact.ID, contact.PhoneNumber, contact.Type, contact.CustomName, contact.Notes,
			contact.Tags, contact.CompanyID, contact.Avatar, contact.AssignedTo, contact.Pob,
			contact.Dob, "MALE", contact.Origin, contact.PushName, "ACTIVE",
			contact.AgentID, contact.FirstMessageID, contact.FirstMessageTimestamp,
			AnyTime{}, AnyTime{}, contact.LastMetadata,
		).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	err := repo.SaveContact(ctx, contact)
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestPostgresRepo_SaveContact_Update(t *testing.T) {
	repo, mock := newTestContactRepo(t)
	ctx := contextWithContactTenant()
	now := time.Now()
	contact := model.Contact{
		ID:          "contact-update-1",
		PhoneNumber: "08123456789",
		CompanyID:   testTenantIDContact,
		CustomName:  "Updated Contact",
	}
	existingCols := []string{"id", "company_id", "phone_number", "custom_name", "created_at", "updated_at"}
	existingRows := sqlmock.NewRows(existingCols).AddRow(contact.ID, contact.CompanyID, "08123456789", "Old Name", now.Add(-time.Hour), now.Add(-time.Minute))
	mock.ExpectBegin()
	selectQuery := `SELECT * FROM "contacts" WHERE id = $1 AND company_id = $2 ORDER BY "contacts"."id" LIMIT $3 FOR UPDATE`
	mock.ExpectQuery(selectQuery).WithArgs(contact.ID, contact.CompanyID, 1).WillReturnRows(existingRows)
	updatePattern := `UPDATE "contacts" SET "id"=$1,"phone_number"=$2,"custom_name"=$3,"company_id"=$4,"updated_at"=$5 WHERE "id" = $6`
	mock.ExpectExec(updatePattern).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	err := repo.SaveContact(ctx, contact)
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestPostgresRepo_SaveContact_TenantMismatch(t *testing.T) {
	repo, mock := newTestContactRepo(t)
	ctx := contextWithContactTenant()
	contact := model.Contact{ID: "contact-tenant-mismatch", CompanyID: "wrong-tenant"}
	err := repo.SaveContact(ctx, contact)
	assert.Error(t, err)
	assert.ErrorIs(t, err, apperrors.ErrBadRequest)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestPostgresRepo_UpdateContact_Success(t *testing.T) {
	repo, mock := newTestContactRepo(t)
	ctx := contextWithContactTenant()
	now := time.Now()
	contact := model.Contact{ID: "contact-update-success", CompanyID: testTenantIDContact, PhoneNumber: "08123456789", CustomName: "Update Success"}
	existingCols := []string{"id", "company_id", "phone_number", "custom_name", "created_at", "updated_at"}
	existingRows := sqlmock.NewRows(existingCols).AddRow(contact.ID, contact.CompanyID, "08123456789", "Old Name", now.Add(-time.Hour), now.Add(-time.Minute))
	mock.ExpectBegin()
	selectQuery := `SELECT * FROM "contacts" WHERE id = $1 AND company_id = $2 ORDER BY "contacts"."id" LIMIT $3 FOR UPDATE`
	mock.ExpectQuery(selectQuery).WithArgs(contact.ID, contact.CompanyID, 1).WillReturnRows(existingRows)
	updatePattern := `UPDATE "contacts" SET "id"=$1,"phone_number"=$2,"custom_name"=$3,"company_id"=$4,"updated_at"=$5 WHERE "id" = $6`
	mock.ExpectExec(updatePattern).
		WithArgs(contact.ID, contact.PhoneNumber, contact.CustomName, contact.CompanyID, AnyTime{}, contact.ID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	err := repo.UpdateContact(ctx, contact)
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestPostgresRepo_UpdateContact_NotFound(t *testing.T) {
	repo, mock := newTestContactRepo(t)
	ctx := contextWithContactTenant()
	contact := model.Contact{ID: "contact-not-found", CompanyID: testTenantIDContact}
	mock.ExpectBegin()
	selectQuery := `SELECT * FROM "contacts" WHERE id = $1 AND company_id = $2 ORDER BY "contacts"."id" LIMIT $3 FOR UPDATE`
	mock.ExpectQuery(selectQuery).WithArgs(contact.ID, contact.CompanyID, 1).WillReturnError(gorm.ErrRecordNotFound)
	mock.ExpectRollback()
	err := repo.UpdateContact(ctx, contact)
	assert.Error(t, err)
	assert.ErrorIs(t, err, apperrors.ErrNotFound)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestPostgresRepo_UpdateContact_TenantMismatch(t *testing.T) {
	repo, mock := newTestContactRepo(t)
	ctx := contextWithContactTenant()
	contact := model.Contact{ID: "contact-tenant-mismatch", CompanyID: "wrong-tenant"}
	err := repo.UpdateContact(ctx, contact)
	assert.Error(t, err)
	assert.ErrorIs(t, err, apperrors.ErrBadRequest)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestPostgresRepo_FindContactByID_Found(t *testing.T) {
	repo, mock := newTestContactRepo(t)
	ctx := contextWithContactTenant()
	now := time.Now()
	cols := []string{"id", "company_id", "phone_number", "custom_name", "created_at", "updated_at"}
	rows := sqlmock.NewRows(cols).AddRow("contact-id-1", testTenantIDContact, "08123456789", "Contact Name", now.Add(-time.Hour), now.Add(-time.Minute))
	selectQuery := `SELECT * FROM "contacts" WHERE id = $1 AND company_id = $2 ORDER BY "contacts"."id" LIMIT $3`
	mock.ExpectQuery(selectQuery).WithArgs("contact-id-1", testTenantIDContact, 1).WillReturnRows(rows)
	found, err := repo.FindContactByID(ctx, "contact-id-1")
	assert.NoError(t, err)
	assert.NotNil(t, found)
	assert.Equal(t, "contact-id-1", found.ID)
	assert.Equal(t, testTenantIDContact, found.CompanyID)
	assert.Equal(t, "Contact Name", found.CustomName)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestPostgresRepo_FindContactByID_NotFound(t *testing.T) {
	repo, mock := newTestContactRepo(t)
	ctx := contextWithContactTenant()
	selectQuery := `SELECT * FROM "contacts" WHERE id = $1 AND company_id = $2 ORDER BY "contacts"."id" LIMIT $3`
	mock.ExpectQuery(selectQuery).WithArgs("contact-id-404", testTenantIDContact, 1).WillReturnError(gorm.ErrRecordNotFound)
	found, err := repo.FindContactByID(ctx, "contact-id-404")
	assert.ErrorIs(t, err, apperrors.ErrNotFound)
	assert.Nil(t, found)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestPostgresRepo_FindContactByPhone_Found(t *testing.T) {
	repo, mock := newTestContactRepo(t)
	ctx := contextWithContactTenant()
	now := time.Now()
	cols := []string{"id", "company_id", "phone_number", "custom_name", "created_at", "updated_at"}
	rows := sqlmock.NewRows(cols).AddRow("contact-phone-1", testTenantIDContact, "08123456789", "Contact Name", now.Add(-time.Hour), now.Add(-time.Minute))
	selectQuery := `SELECT * FROM "contacts" WHERE phone_number = $1 AND company_id = $2 ORDER BY "contacts"."id" LIMIT $3`
	mock.ExpectQuery(selectQuery).WithArgs("08123456789", testTenantIDContact, 1).WillReturnRows(rows)
	found, err := repo.FindContactByPhone(ctx, "08123456789")
	assert.NoError(t, err)
	assert.NotNil(t, found)
	assert.Equal(t, "contact-phone-1", found.ID)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestPostgresRepo_FindContactByPhone_NotFound(t *testing.T) {
	repo, mock := newTestContactRepo(t)
	ctx := contextWithContactTenant()
	selectQuery := `SELECT * FROM "contacts" WHERE phone_number = $1 AND company_id = $2 ORDER BY "contacts"."id" LIMIT $3`
	mock.ExpectQuery(selectQuery).WithArgs("08123456789", testTenantIDContact, 1).WillReturnError(gorm.ErrRecordNotFound)
	found, err := repo.FindContactByPhone(ctx, "08123456789")
	assert.ErrorIs(t, err, apperrors.ErrNotFound)
	assert.Nil(t, found)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestPostgresRepo_BulkUpsertContacts_Success(t *testing.T) {
	repo, mock := newTestContactRepo(t)
	ctx := contextWithContactTenant()
	now := time.Now()
	meta := model.LastMetadata{
		ConsumerSequence: 0,
		StreamSequence:   0,
		Stream:           "",
		Consumer:         "",
		Domain:           "",
		MessageID:        "",
		MessageSubject:   "",
		CompanyID:        "",
	}
	metaBytes, _ := json.Marshal(meta)
	contacts := []model.Contact{
		{ID: "bulk-contact-1", CompanyID: testTenantIDContact, PhoneNumber: "0811", UpdatedAt: now, LastMetadata: datatypes.JSON(metaBytes), FirstMessageTimestamp: now.Unix()},
		{ID: "bulk-contact-2", CompanyID: testTenantIDContact, PhoneNumber: "0812", UpdatedAt: now, LastMetadata: datatypes.JSON(metaBytes), FirstMessageTimestamp: now.Unix()},
	}
	mock.ExpectBegin()
	upsertPattern := `INSERT INTO "contacts" ("id","phone_number","type","custom_name","notes","tags","company_id","avatar","assigned_to","pob","dob","gender","origin","push_name","status","agent_id","first_message_id","first_message_timestamp","created_at","updated_at","last_metadata") VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21),($22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,$36,$37,$38,$39,$40,$41,$42) ON CONFLICT ("id") DO UPDATE SET "phone_number"="excluded"."phone_number","type"="excluded"."type","custom_name"="excluded"."custom_name","notes"="excluded"."notes","tags"="excluded"."tags","company_id"="excluded"."company_id","avatar"="excluded"."avatar","assigned_to"="excluded"."assigned_to","pob"="excluded"."pob","dob"="excluded"."dob","gender"="excluded"."gender","origin"="excluded"."origin","push_name"="excluded"."push_name","status"="excluded"."status","agent_id"="excluded"."agent_id","first_message_id"="excluded"."first_message_id","first_message_timestamp"="excluded"."first_message_timestamp","updated_at"="excluded"."updated_at","last_metadata"="excluded"."last_metadata"`
	mock.ExpectExec(upsertPattern).
		WithArgs(
			contacts[0].ID, contacts[0].PhoneNumber, contacts[0].Type, contacts[0].CustomName, contacts[0].Notes,
			contacts[0].Tags, contacts[0].CompanyID, contacts[0].Avatar, contacts[0].AssignedTo, contacts[0].Pob,
			contacts[0].Dob, "MALE", contacts[0].Origin, contacts[0].PushName, "ACTIVE",
			contacts[0].AgentID, contacts[0].FirstMessageID, contacts[0].FirstMessageTimestamp,
			AnyTime{}, AnyTime{}, contacts[0].LastMetadata,
			contacts[1].ID, contacts[1].PhoneNumber, contacts[1].Type, contacts[1].CustomName, contacts[1].Notes,
			contacts[1].Tags, contacts[1].CompanyID, contacts[1].Avatar, contacts[1].AssignedTo, contacts[1].Pob,
			contacts[1].Dob, "MALE", contacts[1].Origin, contacts[1].PushName, "ACTIVE",
			contacts[1].AgentID, contacts[1].FirstMessageID, contacts[1].FirstMessageTimestamp,
			AnyTime{}, AnyTime{}, contacts[1].LastMetadata,
		).
		WillReturnResult(sqlmock.NewResult(0, int64(len(contacts))))
	mock.ExpectCommit()
	err := repo.BulkUpsertContacts(ctx, contacts)
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestPostgresRepo_BulkUpsertContacts_SkipMismatchedTenant(t *testing.T) {
	repo, mock := newTestAgentRepoWithMatcher(t, sqlmock.QueryMatcherEqual)
	ctx := contextWithContactTenant()
	now := time.Now()
	meta := model.LastMetadata{
		ConsumerSequence: 0,
		StreamSequence:   0,
		Stream:           "",
		Consumer:         "",
		Domain:           "",
		MessageID:        "",
		MessageSubject:   "",
		CompanyID:        "",
	}
	metaBytes, _ := json.Marshal(meta)
	contacts := []model.Contact{
		{ID: "bulk-contact-ok-1", CompanyID: testTenantIDContact, PhoneNumber: "0811", UpdatedAt: now, LastMetadata: datatypes.JSON(metaBytes), FirstMessageTimestamp: now.Unix()},
		{ID: "bulk-contact-wrong", CompanyID: "wrong-tenant", PhoneNumber: "0812", UpdatedAt: now},
		{ID: "bulk-contact-ok-2", CompanyID: testTenantIDContact, PhoneNumber: "0813", UpdatedAt: now, LastMetadata: datatypes.JSON(metaBytes), FirstMessageTimestamp: now.Unix()},
	}
	mock.ExpectBegin()
	upsertPattern := `INSERT INTO "contacts" ("id","phone_number","type","custom_name","notes","tags","company_id","avatar","assigned_to","pob","dob","gender","origin","push_name","status","agent_id","first_message_id","first_message_timestamp","created_at","updated_at","last_metadata") VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21),($22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,$36,$37,$38,$39,$40,$41,$42) ON CONFLICT ("id") DO UPDATE SET "phone_number"="excluded"."phone_number","type"="excluded"."type","custom_name"="excluded"."custom_name","notes"="excluded"."notes","tags"="excluded"."tags","company_id"="excluded"."company_id","avatar"="excluded"."avatar","assigned_to"="excluded"."assigned_to","pob"="excluded"."pob","dob"="excluded"."dob","gender"="excluded"."gender","origin"="excluded"."origin","push_name"="excluded"."push_name","status"="excluded"."status","agent_id"="excluded"."agent_id","first_message_id"="excluded"."first_message_id","first_message_timestamp"="excluded"."first_message_timestamp","updated_at"="excluded"."updated_at","last_metadata"="excluded"."last_metadata"`
	mock.ExpectExec(upsertPattern).
		WithArgs(
			contacts[0].ID, contacts[0].PhoneNumber, contacts[0].Type, contacts[0].CustomName, contacts[0].Notes,
			contacts[0].Tags, contacts[0].CompanyID, contacts[0].Avatar, contacts[0].AssignedTo, contacts[0].Pob,
			contacts[0].Dob, "MALE", contacts[0].Origin, contacts[0].PushName, "ACTIVE",
			contacts[0].AgentID, contacts[0].FirstMessageID, contacts[0].FirstMessageTimestamp,
			AnyTime{}, AnyTime{}, contacts[0].LastMetadata,
			contacts[2].ID, contacts[2].PhoneNumber, contacts[2].Type, contacts[2].CustomName, contacts[2].Notes,
			contacts[2].Tags, contacts[2].CompanyID, contacts[2].Avatar, contacts[2].AssignedTo, contacts[2].Pob,
			contacts[2].Dob, "MALE", contacts[2].Origin, contacts[2].PushName, "ACTIVE",
			contacts[2].AgentID, contacts[2].FirstMessageID, contacts[2].FirstMessageTimestamp,
			AnyTime{}, AnyTime{}, contacts[2].LastMetadata,
		).
		WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectCommit()
	err := repo.BulkUpsertContacts(ctx, contacts)
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestPostgresRepo_BulkUpsertContacts_EmptyList(t *testing.T) {
	repo, mock := newTestContactRepo(t)
	ctx := contextWithContactTenant()
	var contacts []model.Contact
	err := repo.BulkUpsertContacts(ctx, contacts)
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}
