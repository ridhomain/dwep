package model

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMapToBaseEventType(t *testing.T) {
	tests := []struct {
		name          string
		input         string
		expectedType  EventType
		expectedFound bool
	}{
		{"direct match realtime", string(V1ChatsUpdate), V1ChatsUpdate, true},
		{"direct match historical", string(V1HistoricalMessages), V1HistoricalMessages, true},
		{"strip tenant realtime", "v1.chats.update.tenant123", V1ChatsUpdate, true},
		{"strip tenant historical", "v1.history.contacts.tenantXYZ", V1HistoricalContacts, true},
		{"strip agent subject", "v1.connection.update.agentABC", V1Agents, true},
		{"no known base", "v1.unknown.event.tenant1", "", false},
		{"no dot to strip", "unknown", "", false},
		{"only dot", ".", "", false},
		{"leading dot", ".v1.chats.update", "", false},
		{"empty string", "", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actualType, actualFound := MapToBaseEventType(tt.input)
			assert.Equal(t, tt.expectedType, actualType)
			assert.Equal(t, tt.expectedFound, actualFound)
		})
	}
}

func TestMessageMetadata_ToLastMetadata(t *testing.T) {
	now := time.Now()
	input := MessageMetadata{
		ConsumerSequence: 10,
		StreamSequence:   100,
		NumDelivered:     1,
		NumPending:       5,
		Timestamp:        now,
		Stream:           "streamA",
		Consumer:         "consumerB",
		Domain:           "domainC",
		MessageID:        "msgD",
		MessageSubject:   "subjectE",
		CompanyID:        "tenantF",
	}

	expected := &LastMetadata{
		ConsumerSequence: 10,
		StreamSequence:   100,
		Stream:           "streamA",
		Consumer:         "consumerB",
		Domain:           "domainC",
		MessageID:        "msgD",
		MessageSubject:   "subjectE",
		CompanyID:        "tenantF",
	}

	actual := input.ToLastMetadata()
	assert.Equal(t, expected, actual)
}

func TestEventType_GetVersion(t *testing.T) {
	tests := []struct {
		name     string
		e        EventType
		expected string
	}{
		{"v1 event", V1ChatsUpsert, "v1"},
		{"historical v1 event", V1HistoricalChats, "v1"},
		{"no version prefix", EventType("chats.update"), ""},
		{"empty string", EventType(""), ""},
		{"malformed version", EventType("vx.chats.update"), "vx"},
		{"version only", EventType("v2"), ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.e.GetVersion())
		})
	}
}

func TestEventType_GetBaseType(t *testing.T) {
	tests := []struct {
		name     string
		e        EventType
		expected EventType
	}{
		{"v1 event", V1MessagesUpdate, EventType("messages.update")},
		{"historical v1 event", V1HistoricalContacts, EventType("history.contacts")},
		{"no version prefix", EventType("contacts.upsert"), EventType("contacts.upsert")},
		{"empty string", EventType(""), EventType("")},
		{"malformed version", EventType("vx.test.event"), EventType("test.event")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.e.GetBaseType())
		})
	}
}

func TestEventType_WithVersion(t *testing.T) {
	tests := []struct {
		name       string
		e          EventType
		newVersion string
		expected   EventType
	}{
		{"add v2 to base type", EventType("chats.upsert"), "v2", EventType("v2.chats.upsert")},
		{"change v1 to v2", V1ContactsUpdate, "v2", EventType("v2.contacts.update")},
		{"add v1 to historical base", EventType("history.messages"), "v1", V1HistoricalMessages},
		{"add empty version", V1Agents, "", EventType(".agents")}, // Adds dot prefix
		{"add version to empty type", EventType(""), "v3", EventType("v3.")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.e.WithVersion(tt.newVersion))
		})
	}
}
