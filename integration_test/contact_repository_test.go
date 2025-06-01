package integration_test

import (
	"fmt"
	"testing"
	"time"

	"gitlab.com/timkado/api/daisi-wa-events-processor/pkg/logger"
	"go.uber.org/zap/zaptest"

	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib" // PostgreSQL driver
	"github.com/stretchr/testify/suite"

	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/apperrors"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/model"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/storage"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/tenant"
)

// ContactRepoTestSuite defines the suite for Contact repository tests
// It embeds BaseIntegrationSuite to get DB/NATS but not the app container.
type ContactRepoTestSuite struct {
	BaseIntegrationSuite                       // Embed the base suite
	repo                 *storage.PostgresRepo // Repo instance for the suite's default CompanyID
	TenantSchemaName     string
}

// SetupSuite and TearDownSuite are inherited from BaseIntegrationSuite.
// No need to redefine them here.

// SetupTest runs before each test in this suite.
// It initializes the repository for the suite's CompanyID and prepares the schema.
func (s *ContactRepoTestSuite) SetupTest() {
	logger.Log = zaptest.NewLogger(s.T()).Named("ContactRepoTestSuite")
	// Use DSN and CompanyID from the embedded BaseIntegrationSuite
	repo, err := storage.NewPostgresRepo(s.PostgresDSN, true, s.CompanyID)
	s.Require().NoError(err, "SetupTest: Failed to create repo for default tenant")
	s.Require().NotNil(repo, "SetupTest: Repo should not be nil")
	s.repo = repo

	s.TenantSchemaName = fmt.Sprintf("daisi_%s", s.CompanyID)

	// BaseIntegrationSuite.SetupTest() is expected to handle schema creation (if needed)
	// and cleanup for s.TenantSchemaName. Explicit truncation removed.
	s.BaseIntegrationSuite.SetupTest()
}

// TearDownTest runs after each test in this suite.
func (s *ContactRepoTestSuite) TearDownTest() {
	if s.repo != nil {
		// Use the suite's context
		s.repo.Close(s.Ctx) // Close the connection used in the test
	}
}

// TestRunner runs the test suite
func TestContactRepoSuite(t *testing.T) {
	// Ensure suite is run with BaseIntegrationSuite setup
	suite.Run(t, new(ContactRepoTestSuite))
}

// --- Test Cases ---

func (s *ContactRepoTestSuite) TestSaveContact() {
	// Use suite context and company ID
	ctx := tenant.WithCompanyID(s.Ctx, s.CompanyID)

	// 1. Create a new contact using the fixture generator
	contactID := "test-contact-" + uuid.New().String()
	overrides := &model.Contact{
		ID:          contactID,
		CompanyID:   s.CompanyID, // Use suite CompanyID
		PhoneNumber: "+1" + uuid.New().String()[:10],
		CustomName:  "Initial Contact Name",
		Status:      "ACTIVE",
		AgentID:     "agent-" + uuid.New().String(),
	}
	contactInterface, err := generateModelStruct("Contact", overrides)
	s.Require().NoError(err, "Failed to generate contact model")
	contact := contactInterface.(*model.Contact) // Type assertion

	// 2. Save the contact (create)
	err = s.repo.SaveContact(ctx, *contact) // Use SaveContact
	s.Require().NoError(err, "SaveContact (create) failed")

	// 3. Retrieve and verify using direct query via connectDB
	// Connect directly using s.PostgresDSN, without search_path
	db, err := connectDB(s.PostgresDSN)
	s.Require().NoError(err, "Failed to connect to DB for verification")
	defer db.Close()

	var retrievedContact model.Contact
	query := fmt.Sprintf("SELECT id, phone_number, custom_name, status FROM %q.contacts WHERE id = $1 AND company_id = $2", s.TenantSchemaName)
	err = db.QueryRow(query, contact.ID, s.CompanyID). // Use suite CompanyID
								Scan(&retrievedContact.ID, &retrievedContact.PhoneNumber, &retrievedContact.CustomName, &retrievedContact.Status)
	s.Require().NoError(err, "Failed to retrieve created contact")
	s.Require().Equal(contact.CustomName, retrievedContact.CustomName)
	s.Require().Equal(contact.PhoneNumber, retrievedContact.PhoneNumber)
	s.Require().Equal(contact.Status, retrievedContact.Status)

	// 4. Update the contact
	contact.CustomName = "Updated Contact Name"
	contact.Status = "DISABLED"
	contact.Notes = "Some notes added"

	err = s.repo.SaveContact(ctx, *contact) // Save again (update)
	s.Require().NoError(err, "SaveContact (update) failed")

	// 5. Retrieve and verify update using direct query
	var updatedContact model.Contact
	queryUpdated := fmt.Sprintf("SELECT id, custom_name, status, notes, phone_number FROM %q.contacts WHERE id = $1 AND company_id = $2", s.TenantSchemaName)
	err = db.QueryRow(queryUpdated, contact.ID, s.CompanyID). // Use suite CompanyID
									Scan(&updatedContact.ID, &updatedContact.CustomName, &updatedContact.Status, &updatedContact.Notes, &updatedContact.PhoneNumber)
	s.Require().NoError(err, "Failed to retrieve updated contact")
	s.Require().Equal("Updated Contact Name", updatedContact.CustomName)
	s.Require().Equal("DISABLED", updatedContact.Status)
	s.Require().Equal("Some notes added", updatedContact.Notes)
	s.Require().Equal(contact.PhoneNumber, updatedContact.PhoneNumber) // Verify phone number wasn't changed
}

func (s *ContactRepoTestSuite) TestContactNullableFields() {
	t := s.T()
	// Use suite context and company ID
	ctx := tenant.WithCompanyID(s.Ctx, s.CompanyID)

	// 1. Create contact with some nullable fields set
	dob := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	contactID := "test-contact-null-" + uuid.New().String()
	overrides := &model.Contact{
		ID:          contactID,
		CompanyID:   s.CompanyID, // Use suite CompanyID
		PhoneNumber: "+2" + uuid.New().String()[:10],
		CustomName:  "Nullable Test",
		Dob:         &dob, // Assign pointer to time
		Gender:      "FEMALE",
		Status:      "ACTIVE",
		AgentID:     "agent-null-" + uuid.New().String(),
	}
	contactInterface, err := generateModelStruct("Contact", overrides)
	s.Require().NoError(err, "Failed to generate contact model for nullable test")
	contact := contactInterface.(*model.Contact) // Type assertion

	err = s.repo.SaveContact(ctx, *contact)
	s.Require().NoError(err, "SaveContact with nullable fields failed")

	// 2. Retrieve and verify using direct query via connectDB
	// Connect directly using s.PostgresDSN, without search_path
	db, err := connectDB(s.PostgresDSN)
	s.Require().NoError(err, "Failed to connect to DB for verification")
	defer db.Close()

	var retrievedContact model.Contact
	// Select company_id to ensure it's set correctly
	query := fmt.Sprintf("SELECT id, company_id, dob, gender, pob FROM %q.contacts WHERE id = $1", s.TenantSchemaName)
	err = db.QueryRow(query, contact.ID).Scan(&retrievedContact.ID, &retrievedContact.CompanyID, &retrievedContact.Dob, &retrievedContact.Gender, &retrievedContact.Pob)
	s.Require().NoError(err, "Failed to retrieve contact with nullable fields")
	s.Require().Equal(s.CompanyID, retrievedContact.CompanyID) // Verify company ID
	s.Require().NotNil(retrievedContact.Dob, "DoB should not be nil")
	s.Require().Equal(dob.Format("2006-01-02"), retrievedContact.Dob.Format("2006-01-02"), "DoB should match")
	s.Require().Equal("FEMALE", retrievedContact.Gender)
	s.Require().Empty(retrievedContact.Pob, "PoB should be empty string as it wasn't set")

	// 3. Test setting fields to null via UpdateContact (if method exists and handles it)
	// This requires a dedicated UpdateContact method that likely accepts a map[string]interface{}
	// or uses GORM's Select/Omit features.
	// Example placeholder:
	// contactUpdate := model.Contact{ ID: contact.ID, CompanyID: DefaultTenant, Dob: nil, Gender: ""} // Gender empty might not set to NULL
	// err = s.repo.UpdateContact(ctx, contactUpdate) // Assuming UpdateContact exists
	// require.NoError(t, err, "UpdateContact to set fields to null failed")
	//
	// // 4. Retrieve and verify nulls via direct query
	// var nulledContact model.Contact
	// queryNull := "SELECT id, dob, gender FROM contacts WHERE id = $1"
	// err = db.QueryRow(queryNull, contact.ID).Scan(&nulledContact.ID, &nulledContact.Dob, &nulledContact.Gender)
	// require.NoError(t, err, "Failed to retrieve contact after setting fields to null")
	// require.Nil(t, nulledContact.Dob, "DoB should be nil after update")
	// require.Empty(t, nulledContact.Gender, "Gender should be empty/null after update")
	t.Log("Test for setting null values needs specific UpdateContact method verification")
}

// TestContactTenantIsolation verifies that operations are isolated to the correct tenant.
func (s *ContactRepoTestSuite) TestContactTenantIsolation() {
	baseCtx := s.Ctx // Use suite context as base

	// Tenant A (uses the suite's default CompanyID and repo)
	tenantA_ID := s.CompanyID
	schemaA := s.TenantSchemaName // Use TenantSchemaName for tenant A
	ctxA := tenant.WithCompanyID(baseCtx, tenantA_ID)
	contactAInterface, err := generateModelStruct("Contact", &model.Contact{
		ID:          "tenant-iso-contact-A-" + uuid.New().String(),
		CompanyID:   tenantA_ID,
		PhoneNumber: "+A" + uuid.New().String()[:10],
		CustomName:  "Contact A",
		AgentID:     "agent-A",
	})
	s.Require().NoError(err)
	contactA := contactAInterface.(*model.Contact)

	// Tenant B
	tenantB_ID := "tenant_b_contact_" + uuid.New().String()
	schemaB := fmt.Sprintf("daisi_%s", tenantB_ID)
	ctxB := tenant.WithCompanyID(baseCtx, tenantB_ID)
	// Use the suite's PostgresDSN to create repo for tenant B
	repoB, err := storage.NewPostgresRepo(s.PostgresDSN, true, tenantB_ID)
	s.Require().NoError(err, "Failed to create repo for Tenant B")
	s.Require().NotNil(repoB)
	defer repoB.Close(baseCtx)

	contactBInterface, err := generateModelStruct("Contact", &model.Contact{
		ID:          "tenant-iso-contact-B-" + uuid.New().String(),
		CompanyID:   tenantB_ID,
		PhoneNumber: "+B" + uuid.New().String()[:10],
		CustomName:  "Contact B",
		AgentID:     "agent-B",
	})
	s.Require().NoError(err)
	contactB := contactBInterface.(*model.Contact)

	// 1. Save Contact A using Repo A (s.repo)
	err = s.repo.SaveContact(ctxA, *contactA)
	s.Require().NoError(err, "Failed to save Contact A")

	// 2. Save Contact B using Repo B
	err = repoB.SaveContact(ctxB, *contactB)
	s.Require().NoError(err, "Failed to save Contact B")

	// 3. Verify Contact A exists using direct connection to Schema A
	dbA, err := connectDB(s.PostgresDSN) // Connect directly
	s.Require().NoError(err, "Failed to connect to DB for Tenant A verification")
	defer dbA.Close()
	var retrievedA_dbA model.Contact
	queryA := fmt.Sprintf("SELECT id, custom_name FROM %q.contacts WHERE id = $1 AND company_id = $2", schemaA) // Use $ placeholder
	err = dbA.QueryRow(queryA, contactA.ID, tenantA_ID).Scan(&retrievedA_dbA.ID, &retrievedA_dbA.CustomName)
	s.Require().NoError(err, "Failed to retrieve Contact A via DB A")
	s.Require().Equal(contactA.CustomName, retrievedA_dbA.CustomName)

	// 4. Verify Contact B exists using direct connection to Schema B
	dbB, err := connectDB(s.PostgresDSN) // Connect directly
	s.Require().NoError(err, "Failed to connect to DB for Tenant B verification")
	defer dbB.Close()
	var retrievedB_dbB model.Contact
	queryB := fmt.Sprintf("SELECT id, custom_name FROM %q.contacts WHERE id = $1 AND company_id = $2", schemaB) // Use $ placeholder
	err = dbB.QueryRow(queryB, contactB.ID, tenantB_ID).Scan(&retrievedB_dbB.ID, &retrievedB_dbB.CustomName)
	s.Require().NoError(err, "Failed to retrieve Contact B via DB B")
	s.Require().Equal(contactB.CustomName, retrievedB_dbB.CustomName)

	// 5. Verify Contact B does NOT exist using direct connection to Schema A
	var countB_dbA int
	queryB_dbA := fmt.Sprintf("SELECT COUNT(*) FROM %q.contacts WHERE id = $1", schemaA) // Use $ placeholder
	err = dbA.QueryRow(queryB_dbA, contactB.ID).Scan(&countB_dbA)
	s.Require().NoError(err, "Query B via DB A failed")
	s.Require().Equal(0, countB_dbA, "Contact B should not be found via DB A")

	// 6. Verify Contact A does NOT exist using direct connection to Schema B
	var countA_dbB int
	queryA_dbB := fmt.Sprintf("SELECT COUNT(*) FROM %q.contacts WHERE id = $1", schemaB) // Use $ placeholder
	err = dbB.QueryRow(queryA_dbB, contactA.ID).Scan(&countA_dbB)
	s.Require().NoError(err, "Query A via DB B failed")
	s.Require().Equal(0, countA_dbB, "Contact A should not be found via DB B")

	// Cleanup: Drop Tenant B schema
	// Use the suite's default DSN which should have permissions
	defaultDbSQL, err := connectDB(s.PostgresDSN)
	s.Require().NoError(err, "Cleanup: Failed to connect to default DB")
	defer defaultDbSQL.Close()
	_, err = defaultDbSQL.Exec(fmt.Sprintf("DROP SCHEMA IF EXISTS %q CASCADE", schemaB)) // Use %q for schemaB
	s.Require().NoError(err, "Cleanup: Failed to drop Tenant B schema")
}

func (s *ContactRepoTestSuite) TestFindContactByPhoneAndAgentID() {
	// Use suite context and company ID
	ctx := tenant.WithCompanyID(s.Ctx, s.CompanyID)

	// 1. Create a test contact
	phone := "+PAGNT" + uuid.New().String()[:8]
	agentID := "agent-pagnt-" + uuid.New().String()
	contactID := "contact-pagnt-" + uuid.New().String()
	overrides := &model.Contact{
		ID:          contactID,
		CompanyID:   s.CompanyID,
		PhoneNumber: phone,
		AgentID:     agentID,
		CustomName:  "PhoneAgent Test",
	}
	contactInterface, err := generateModelStruct("Contact", overrides)
	s.Require().NoError(err, "Failed to generate contact model for phone/agent test")
	contact := contactInterface.(*model.Contact) // Type assertion

	err = s.repo.SaveContact(ctx, *contact)
	s.Require().NoError(err, "SaveContact for phone/agent test failed")

	// 2. Test successful find
	foundContact, err := s.repo.FindContactByPhoneAndAgentID(ctx, phone, agentID)
	s.Require().NoError(err, "FindContactByPhoneAndAgentID should succeed for existing contact")
	s.Require().NotNil(foundContact)
	s.Require().Equal(contact.ID, foundContact.ID)
	s.Require().Equal(contact.PhoneNumber, foundContact.PhoneNumber)
	s.Require().Equal(contact.AgentID, foundContact.AgentID)

	// 3. Test find with correct phone, wrong agent
	_, err = s.repo.FindContactByPhoneAndAgentID(ctx, phone, "wrong-agent-"+uuid.New().String())
	s.Require().Error(err, "FindContactByPhoneAndAgentID should fail for wrong agent")
	s.Require().ErrorIs(err, apperrors.ErrNotFound, "Expected ErrNotFound for wrong agent")

	// 4. Test find with wrong phone, correct agent
	_, err = s.repo.FindContactByPhoneAndAgentID(ctx, "+WRONG"+uuid.New().String()[:8], agentID)
	s.Require().Error(err, "FindContactByPhoneAndAgentID should fail for wrong phone")
	s.Require().ErrorIs(err, apperrors.ErrNotFound, "Expected ErrNotFound for wrong phone")

	// 5. Test find with empty agent ID (should return BadRequest as per implementation)
	_, err = s.repo.FindContactByPhoneAndAgentID(ctx, phone, "")
	s.Require().Error(err, "FindContactByPhoneAndAgentID should fail for empty agent ID")
	s.Require().ErrorIs(err, apperrors.ErrBadRequest, "Expected ErrBadRequest for empty agent ID")

	// 6. Test find with non-existent phone and agent
	_, err = s.repo.FindContactByPhoneAndAgentID(ctx, "+NONEXIST"+uuid.New().String()[:7], "nonexist-agent-"+uuid.New().String())
	s.Require().Error(err, "FindContactByPhoneAndAgentID should fail for non-existent combo")
	s.Require().ErrorIs(err, apperrors.ErrNotFound, "Expected ErrNotFound for non-existent combo")
}

// Add TestUpdateContact explicitly if it exists and handles partial updates / pointers
