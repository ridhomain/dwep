package model

import (
	"time"

	"github.com/brianvoe/gofakeit/v6"

	"gitlab.com/timkado/api/daisi-wa-events-processor/pkg/utils"
)

// Ensure gofakeit is seeded. It might already be seeded by factories.go's init,
// but adding it here ensures this file is self-sufficient if used independently.
func init() {
	gofakeit.Seed(time.Now().UnixNano())
}

// --- NATS Payload Factories ---

// NewUpsertChatPayload creates a new UpsertChatPayload instance with default fake data.
func NewUpsertChatPayload(overrideDefaults ...*UpsertChatPayload) *UpsertChatPayload {
	base := &UpsertChatPayload{
		ChatID:                gofakeit.UUID(),
		Jid:                   gofakeit.UUID() + "@" + gofakeit.RandomString([]string{"s.whatsapp.net", "g.us"}),
		CompanyID:             "tenant_" + gofakeit.LetterN(10),
		AgentID:               gofakeit.UUID(),
		PhoneNumber:           gofakeit.Phone(),
		PushName:              gofakeit.Username(),
		CustomName:            gofakeit.Name(),
		IsGroup:               gofakeit.Bool(),
		GroupName:             gofakeit.Company(),
		LastMessageObj:        map[string]interface{}{"text": gofakeit.Sentence(5), "ts": utils.Now().Unix()},
		ConversationTimestamp: utils.Now().Add(-time.Duration(gofakeit.Number(1, 60)) * time.Minute).Unix(),
		UnreadCount:           int32(gofakeit.Number(0, 50)),
		NotSpam:               gofakeit.Bool(),
	}

	if len(overrideDefaults) > 0 && overrideDefaults[0] != nil {
		ovr := overrideDefaults[0]
		if ovr.ChatID != "" {
			base.ChatID = ovr.ChatID
		}
		if ovr.Jid != "" {
			base.Jid = ovr.Jid
		}
		if ovr.CompanyID != "" {
			base.CompanyID = ovr.CompanyID
		}
		if ovr.AgentID != "" {
			base.AgentID = ovr.AgentID
		}
		if ovr.PhoneNumber != "" {
			base.PhoneNumber = ovr.PhoneNumber
		}
		if ovr.PushName != "" {
			base.PushName = ovr.PushName
		}
		if ovr.CustomName != "" {
			base.CustomName = ovr.CustomName
		}
		base.IsGroup = ovr.IsGroup // Override boolean
		if ovr.GroupName != "" {
			base.GroupName = ovr.GroupName
		}
		if ovr.LastMessageObj != nil {
			base.LastMessageObj = ovr.LastMessageObj
		}
		if ovr.ConversationTimestamp != 0 {
			base.ConversationTimestamp = ovr.ConversationTimestamp
		}
		if ovr.UnreadCount != 0 {
			base.UnreadCount = ovr.UnreadCount
		}
		base.NotSpam = ovr.NotSpam // Override boolean
	}
	return base
}

// NewKeyPayload creates a new KeyPayload instance with default fake data.
func NewKeyPayload(overrideDefaults ...*KeyPayload) *KeyPayload {
	base := &KeyPayload{
		ID:        gofakeit.UUID(),
		FromMe:    gofakeit.Bool(),
		RemoteJid: gofakeit.UUID() + "@" + gofakeit.RandomString([]string{"s.whatsapp.net", "g.us"}),
	}

	if len(overrideDefaults) > 0 && overrideDefaults[0] != nil {
		ovr := overrideDefaults[0]
		if ovr.ID != "" {
			base.ID = ovr.ID
		}
		base.FromMe = ovr.FromMe // Override boolean
		if ovr.RemoteJid != "" {
			base.RemoteJid = ovr.RemoteJid
		}
	}
	return base
}

// NewUpsertMessagePayload creates a new UpsertMessagePayload instance with default fake data.
func NewUpsertMessagePayload(overrideDefaults ...*UpsertMessagePayload) *UpsertMessagePayload {
	base := &UpsertMessagePayload{
		MessageID:        gofakeit.UUID(),
		ToPhone:          gofakeit.Phone(),
		FromPhone:        gofakeit.Phone(),
		ChatID:           gofakeit.UUID(),
		Jid:              gofakeit.UUID() + "@" + gofakeit.RandomString([]string{"s.whatsapp.net", "g.us"}),
		Flow:             gofakeit.RandomString([]string{MessageFlowIncoming, MessageFlowOutgoing}),
		CompanyID:        "tenant_" + gofakeit.LetterN(10),
		AgentID:          gofakeit.UUID(),
		Key:              NewKeyPayload(), // Use KeyPayload factory
		MessageObj:       map[string]interface{}{"text": gofakeit.Sentence(8), "ts": utils.Now().Unix()},
		Status:           gofakeit.RandomString([]string{"sent", "delivered", "read", "failed"}),
		MessageTimestamp: utils.Now().Add(-time.Duration(gofakeit.Number(1, 100)) * time.Hour).Unix(),
	}

	if len(overrideDefaults) > 0 && overrideDefaults[0] != nil {
		ovr := overrideDefaults[0]
		if ovr.MessageID != "" {
			base.MessageID = ovr.MessageID
		}
		if ovr.ToPhone != "" {
			base.ToPhone = ovr.ToPhone
		}
		if ovr.FromPhone != "" {
			base.FromPhone = ovr.FromPhone
		}
		if ovr.ChatID != "" {
			base.ChatID = ovr.ChatID
		}
		if ovr.Jid != "" {
			base.Jid = ovr.Jid
		}
		if ovr.Flow != "" {
			base.Flow = ovr.Flow
		}
		if ovr.CompanyID != "" {
			base.CompanyID = ovr.CompanyID
		}
		if ovr.AgentID != "" {
			base.AgentID = ovr.AgentID
		}
		if ovr.Key != nil {
			base.Key = ovr.Key // Allow overriding the key entirely
		}
		if ovr.MessageObj != nil {
			base.MessageObj = ovr.MessageObj
		}
		if ovr.Status != "" {
			base.Status = ovr.Status
		}
		if ovr.MessageTimestamp != 0 {
			base.MessageTimestamp = ovr.MessageTimestamp
		}
	}
	return base
}

// NewUpsertAgentPayload creates a new UpsertAgentPayload instance with default fake data.
func NewUpsertAgentPayload(overrideDefaults ...*UpsertAgentPayload) *UpsertAgentPayload {
	base := &UpsertAgentPayload{
		AgentID:   gofakeit.UUID(),
		CompanyID: "tenant_" + gofakeit.LetterN(10),
		QRCode:    gofakeit.Sentence(5),
		Status:    gofakeit.RandomString([]string{"connected", "disconnected", "pairing"}),
		AgentName: gofakeit.Username(),
		HostName:  gofakeit.DomainName(),
		Version:   gofakeit.AppVersion(),
	}

	if len(overrideDefaults) > 0 && overrideDefaults[0] != nil {
		ovr := overrideDefaults[0]
		if ovr.AgentID != "" {
			base.AgentID = ovr.AgentID
		}
		if ovr.CompanyID != "" {
			base.CompanyID = ovr.CompanyID
		}
		if ovr.QRCode != "" {
			base.QRCode = ovr.QRCode
		}
		if ovr.Status != "" {
			base.Status = ovr.Status
		}
		if ovr.AgentName != "" {
			base.AgentName = ovr.AgentName
		}
		if ovr.HostName != "" {
			base.HostName = ovr.HostName
		}
		if ovr.Version != "" {
			base.Version = ovr.Version
		}
	}
	return base
}

// NewUpsertContactPayload creates a new UpsertContactPayload instance with default fake data.
func NewUpsertContactPayload(overrideDefaults ...*UpsertContactPayload) *UpsertContactPayload {
	base := &UpsertContactPayload{
		PhoneNumber: gofakeit.Phone(),
		CompanyID:   "tenant_" + gofakeit.LetterN(10),
		PushName:    gofakeit.Username(),
		AgentID:     gofakeit.UUID(),
	}

	if len(overrideDefaults) > 0 && overrideDefaults[0] != nil {
		ovr := overrideDefaults[0]
		if ovr.PhoneNumber != "" {
			base.PhoneNumber = ovr.PhoneNumber
		}
		if ovr.CompanyID != "" {
			base.CompanyID = ovr.CompanyID
		}
		if ovr.PushName != "" {
			base.PushName = ovr.PushName
		}
		if ovr.AgentID != "" {
			base.AgentID = ovr.AgentID
		}
	}
	return base
}

// Note: Factories for Update*Payloads are not included here as they often require specific IDs
// and usually involve setting only a subset of fields (often using pointers).
// Generating a 'full' update payload isn't typically how they are used.
// If needed, they could be added, potentially accepting existing IDs as arguments.

const defaultHistoryCount = 10

// NewHistoryChatPayload creates a new HistoryChatPayload instance with a specified number of fake chats.
func NewHistoryChatPayload(count *int, overrideDefaults ...*UpsertChatPayload) *HistoryChatPayload {
	numChats := defaultHistoryCount
	if count != nil && *count >= 0 {
		numChats = *count
	}

	chats := make([]UpsertChatPayload, 0, numChats)
	for i := 0; i < numChats; i++ {
		// Pass down any override defaults provided to the history factory
		chats = append(chats, *NewUpsertChatPayload(overrideDefaults...))
	}

	return &HistoryChatPayload{
		Chats: chats,
	}
}

// NewHistoryMessagePayload creates a new HistoryMessagePayload instance with a specified number of fake messages.
func NewHistoryMessagePayload(count *int, overrideDefaults ...*UpsertMessagePayload) *HistoryMessagePayload {
	numMessages := defaultHistoryCount
	if count != nil && *count >= 0 {
		numMessages = *count
	}

	messages := make([]UpsertMessagePayload, 0, numMessages)
	for i := 0; i < numMessages; i++ {
		messages = append(messages, *NewUpsertMessagePayload(overrideDefaults...))
	}

	return &HistoryMessagePayload{
		Messages: messages,
	}
}

// NewHistoryContactPayload creates a new HistoryContactPayload instance with a specified number of fake contacts.
func NewHistoryContactPayload(count *int, overrideDefaults ...*UpsertContactPayload) *HistoryContactPayload {
	numContacts := defaultHistoryCount
	if count != nil && *count >= 0 {
		numContacts = *count
	}

	contacts := make([]UpsertContactPayload, 0, numContacts)
	for i := 0; i < numContacts; i++ {
		contacts = append(contacts, *NewUpsertContactPayload(overrideDefaults...))
	}

	return &HistoryContactPayload{
		Contacts: contacts,
	}
}
