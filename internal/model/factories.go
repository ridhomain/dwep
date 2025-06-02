package model

import (
	"encoding/json"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	"gorm.io/datatypes"

	"gitlab.com/timkado/api/daisi-wa-events-processor/pkg/utils"
)

// --- START: Stub Utils Functions (Replace with actual implementation from pkg/utils) ---

// RandomJSONB generates random JSON data for testing.
func RandomJSONB() datatypes.JSON {
	// Simple stub: returns a basic JSON object
	jsonData := map[string]interface{}{
		"stub_key": gofakeit.Word(),
		"stub_num": gofakeit.Number(1, 100),
	}
	bytes, _ := json.Marshal(jsonData)
	return datatypes.JSON(bytes)
}

// RandomJSONBMap generates JSON data from a map for testing.
func RandomJSONBMap(data map[string]interface{}) datatypes.JSON {
	bytes, _ := json.Marshal(data)
	return datatypes.JSON(bytes)
}

// --- END: Stub Utils Functions ---

// init ensures gofakeit is seeded.
func init() {
	gofakeit.Seed(time.Now().UnixNano())
}

// NewAgent creates a new Agent instance with default fake data.
func NewAgent(overrideDefaults ...*Agent) *Agent {
	base := &Agent{
		AgentID:   gofakeit.UUID(),
		QRCode:    gofakeit.Sentence(5),
		Status:    gofakeit.RandomString([]string{"connected", "disconnected", "pairing"}),
		AgentName: gofakeit.Username(),
		HostName:  gofakeit.DomainName(),
		Version:   gofakeit.AppVersion(),
		CompanyID: "tenant_" + gofakeit.LetterN(10),
		CreatedAt: utils.Now().Add(-time.Duration(gofakeit.Number(1, 100)) * time.Hour),
		UpdatedAt: utils.Now(),
		// CompanyID:  "", // Removed - Field does not exist on Agent model
	}

	if len(overrideDefaults) > 0 && overrideDefaults[0] != nil {
		ovr := overrideDefaults[0]
		// Allow overriding with empty string by direct assignment
		base.AgentID = ovr.AgentID
		base.QRCode = ovr.QRCode
		base.Status = ovr.Status
		base.AgentName = ovr.AgentName
		base.HostName = ovr.HostName
		base.Version = ovr.Version
		base.CompanyID = ovr.CompanyID

		if !ovr.CreatedAt.IsZero() {
			base.CreatedAt = ovr.CreatedAt
		}
		if !ovr.UpdatedAt.IsZero() {
			base.UpdatedAt = ovr.UpdatedAt
		}
	}
	return base
}

// NewOnboardingLog creates a new OnboardingLog instance with default fake data.
func NewOnboardingLog(overrideDefaults ...*OnboardingLog) *OnboardingLog {
	base := &OnboardingLog{
		MessageID:   gofakeit.UUID(),
		AgentID:     gofakeit.UUID(),
		CompanyID:   "tenant_" + gofakeit.LetterN(10),
		PhoneNumber: gofakeit.Phone(),
		Timestamp:   utils.Now().Add(-time.Duration(gofakeit.Number(1, 100)) * time.Minute).Unix(),
		CreatedAt:   utils.Now(),
		// CompanyID:    "", // Removed - Field does not exist on OnboardingLog model
	}

	if len(overrideDefaults) > 0 && overrideDefaults[0] != nil {
		ovr := overrideDefaults[0]
		// Allow overriding with empty string by direct assignment
		base.MessageID = ovr.MessageID
		base.AgentID = ovr.AgentID
		base.CompanyID = ovr.CompanyID
		base.PhoneNumber = ovr.PhoneNumber

		if ovr.Timestamp != 0 {
			base.Timestamp = ovr.Timestamp
		}
		if !ovr.CreatedAt.IsZero() {
			base.CreatedAt = ovr.CreatedAt
		}
	}
	return base
}

// NewContact creates a new Contact instance with default fake data.
func NewContact(overrideDefaults ...*Contact) *Contact {
	randDate := gofakeit.Date()
	base := &Contact{
		ID:                    gofakeit.UUID(),
		PhoneNumber:           gofakeit.Phone(),
		Type:                  gofakeit.RandomString([]string{"customer", "lead", "internal"}),
		CustomName:            gofakeit.Name(),
		Notes:                 gofakeit.Sentence(10),
		Tags:                  gofakeit.LoremIpsumSentence(3),
		CompanyID:             "tenant_" + gofakeit.LetterN(10),
		Avatar:                gofakeit.ImageURL(100, 100),
		AssignedTo:            gofakeit.UUID(),
		Pob:                   gofakeit.City(),
		Dob:                   &randDate, // Dob is time.Time
		Gender:                gofakeit.RandomString([]string{"MALE", "FEMALE", "OTHER"}),
		Origin:                gofakeit.RandomString([]string{"whatsapp", "web", "import"}),
		PushName:              gofakeit.Username(),
		Status:                gofakeit.RandomString([]string{"ACTIVE", "INACTIVE", "BLOCKED"}),
		AgentID:               gofakeit.UUID(),
		FirstMessageID:        gofakeit.UUID(),
		CreatedAt:             utils.Now().Add(-time.Duration(gofakeit.Number(1, 365)) * 24 * time.Hour),
		UpdatedAt:             utils.Now(),
		LastMetadata:          RandomJSONB(), // Use stubbed function
		FirstMessageTimestamp: utils.Now().Add(-time.Duration(gofakeit.Number(1, 100)) * time.Hour).Unix(),
	}

	if len(overrideDefaults) > 0 && overrideDefaults[0] != nil {
		ovr := overrideDefaults[0]
		// Allow overriding with empty string by direct assignment
		base.ID = ovr.ID
		base.PhoneNumber = ovr.PhoneNumber
		base.Type = ovr.Type
		base.CustomName = ovr.CustomName
		base.Notes = ovr.Notes
		base.Tags = ovr.Tags
		base.CompanyID = ovr.CompanyID
		base.Avatar = ovr.Avatar
		base.AssignedTo = ovr.AssignedTo
		base.Pob = ovr.Pob
		// Allow overriding with nil for pointer type *time.Time by direct assignment
		base.Dob = ovr.Dob
		// Allow overriding with empty string by direct assignment
		base.Gender = ovr.Gender
		base.Origin = ovr.Origin
		base.PushName = ovr.PushName
		base.Status = ovr.Status
		base.AgentID = ovr.AgentID
		base.FirstMessageID = ovr.FirstMessageID

		if !ovr.CreatedAt.IsZero() {
			base.CreatedAt = ovr.CreatedAt
		}
		if !ovr.UpdatedAt.IsZero() {
			base.UpdatedAt = ovr.UpdatedAt
		}
		// Allow overriding with nil for datatypes.JSON by direct assignment
		base.LastMetadata = ovr.LastMetadata
		// Add missing override for FirstMessageTimestamp, keeping existing logic for int64
		if ovr.FirstMessageTimestamp != 0 {
			base.FirstMessageTimestamp = ovr.FirstMessageTimestamp
		}
	}
	return base
}

// NewMessage creates a new Message instance with default fake data.
func NewMessage(overrideDefaults ...*Message) *Message {
	msgTimestamp := utils.Now().Add(-time.Duration(gofakeit.Number(1, 100)) * time.Hour).Unix()
	base := &Message{
		MessageID:        gofakeit.UUID(),
		FromPhone:        gofakeit.Phone(),
		ToPhone:          gofakeit.Phone(),
		ChatID:           gofakeit.UUID(),
		Jid:              gofakeit.UUID() + "@" + gofakeit.RandomString([]string{"s.whatsapp.net", "g.us"}),
		Flow:             gofakeit.RandomString([]string{MessageFlowOutgoing, MessageFlowIncoming}),
		AgentID:          gofakeit.UUID(),
		CompanyID:        "tenant_" + gofakeit.LetterN(10),
		MessageObj:       RandomJSONBMap(map[string]interface{}{"text": gofakeit.Sentence(8)}),                                  // Use stubbed function
		Key:              RandomJSONBMap(map[string]interface{}{"id": gofakeit.UUID(), "remoteJid": gofakeit.UUID() + "@g.us"}), // Use stubbed function
		Status:           gofakeit.RandomString([]string{"sent", "delivered", "read", "failed"}),
		IsDeleted:        gofakeit.Bool(),
		MessageTimestamp: msgTimestamp,
		MessageDate:      CreateTimeFromTimestamp(msgTimestamp),
		CreatedAt:        CreateTimeFromTimestamp(msgTimestamp),
		UpdatedAt:        utils.Now(),
		LastMetadata:     RandomJSONB(), // Use stubbed function
	}

	if len(overrideDefaults) > 0 && overrideDefaults[0] != nil {
		ovr := overrideDefaults[0]
		// Allow overriding with empty string by direct assignment
		base.MessageID = ovr.MessageID
		base.FromPhone = ovr.FromPhone
		base.ToPhone = ovr.ToPhone
		base.ChatID = ovr.ChatID
		base.Jid = ovr.Jid
		base.Flow = ovr.Flow
		base.AgentID = ovr.AgentID
		base.CompanyID = ovr.CompanyID
		// Allow overriding with nil for datatypes.JSON by direct assignment
		base.MessageObj = ovr.MessageObj
		base.Key = ovr.Key
		// Allow overriding with empty string by direct assignment
		base.Status = ovr.Status
		// We don't check for default bool value (false)
		base.IsDeleted = ovr.IsDeleted
		if ovr.MessageTimestamp != 0 {
			base.MessageTimestamp = ovr.MessageTimestamp
		}
		if !ovr.MessageDate.IsZero() {
			base.MessageDate = ovr.MessageDate
		}
		if !ovr.CreatedAt.IsZero() {
			base.CreatedAt = ovr.CreatedAt
		}
		if !ovr.UpdatedAt.IsZero() {
			base.UpdatedAt = ovr.UpdatedAt
		}
		// Allow overriding with nil for datatypes.JSON by direct assignment
		base.LastMetadata = ovr.LastMetadata
	}
	return base
}

// NewChat creates a new Chat instance with default fake data.
// Corrected based on actual model fields.
func NewChat(overrideDefaults ...*Chat) *Chat {
	convTimestamp := utils.Now().Add(-time.Duration(gofakeit.Number(1, 60)) * time.Minute).Unix()
	base := &Chat{
		ChatID:                gofakeit.UUID(),
		Jid:                   gofakeit.UUID() + "@" + gofakeit.RandomString([]string{"s.whatsapp.net", "g.us"}),
		PushName:              gofakeit.Username(),
		IsGroup:               gofakeit.Bool(),
		GroupName:             gofakeit.Company(),
		UnreadCount:           int32(gofakeit.Number(0, 50)), // Correct type int32
		LastMessageObj:        RandomJSONBMap(map[string]interface{}{"text": gofakeit.Sentence(5)}),
		ConversationTimestamp: convTimestamp,
		NotSpam:               gofakeit.Bool(),
		AgentID:               gofakeit.UUID(), // Explicit AgentID field
		CompanyID:             "tenant_" + gofakeit.LetterN(10),
		PhoneNumber:           gofakeit.Phone(), // Correct field name for phone number
		LastMetadata:          RandomJSONB(),
		CreatedAt:             utils.Now().Add(-time.Duration(gofakeit.Number(1, 30)) * 24 * time.Hour),
		UpdatedAt:             utils.Now(),
	}

	if len(overrideDefaults) > 0 && overrideDefaults[0] != nil {
		ovr := overrideDefaults[0]
		// Allow overriding with empty string by direct assignment
		base.ChatID = ovr.ChatID
		base.Jid = ovr.Jid
		base.PushName = ovr.PushName
		base.IsGroup = ovr.IsGroup
		// Allow overriding with empty string by direct assignment
		base.GroupName = ovr.GroupName
		if ovr.UnreadCount != 0 { // Keep existing logic for int32 unless specified otherwise
			base.UnreadCount = ovr.UnreadCount
		} // Allow overriding 0
		// Allow overriding with empty string by direct assignment
		// Allow overriding with nil for datatypes.JSON by direct assignment
		base.LastMessageObj = ovr.LastMessageObj
		if ovr.ConversationTimestamp != 0 {
			base.ConversationTimestamp = ovr.ConversationTimestamp
		}
		base.NotSpam = ovr.NotSpam
		// Allow overriding with empty string by direct assignment
		base.AgentID = ovr.AgentID
		base.CompanyID = ovr.CompanyID
		base.PhoneNumber = ovr.PhoneNumber
		// Allow overriding with nil for datatypes.JSON by direct assignment
		base.LastMetadata = ovr.LastMetadata
		if !ovr.CreatedAt.IsZero() {
			base.CreatedAt = ovr.CreatedAt
		}
		if !ovr.UpdatedAt.IsZero() {
			base.UpdatedAt = ovr.UpdatedAt
		}
	}
	return base
}
