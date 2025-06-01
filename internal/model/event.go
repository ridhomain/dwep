package model

import (
	"strings"
	"time"
)

// EventType represents different types of events
type EventType string

// Common event type constants (with versioning)
const (
	// Version 1 historical event types
	V1HistoricalChats    EventType = "v1.history.chats"
	V1HistoricalMessages EventType = "v1.history.messages"
	V1HistoricalContacts EventType = "v1.history.contacts"
	//v1.agents
	// Version 1 realtime event types
	V1Agents         EventType = "v1.connection.update"
	V1ChatsUpdate    EventType = "v1.chats.update"
	V1ChatsUpsert    EventType = "v1.chats.upsert"
	V1MessagesUpsert EventType = "v1.messages.upsert"
	V1MessagesUpdate EventType = "v1.messages.update"
	V1ContactsUpsert EventType = "v1.contacts.upsert"
	V1ContactsUpdate EventType = "v1.contacts.update"
)

// MapToBaseEventType attempts to map an input string (potentially with extra identifiers)
// back to a known base EventType constant.
// It returns the mapped EventType and true if successful, or an empty EventType ("")
// and false if no mapping is found.
func MapToBaseEventType(input string) (EventType, bool) {
	// First, check if the input string *directly* matches a known EventType value.
	// This handles cases where the input might already be the base type (e.g., "v1.agents").
	switch EventType(input) {
	case V1Agents, V1ChatsUpdate, V1ChatsUpsert, V1MessagesUpsert, V1MessagesUpdate, V1ContactsUpsert, V1ContactsUpdate, V1HistoricalChats, V1HistoricalMessages, V1HistoricalContacts:
		return EventType(input), true // Direct match found
	}

	// If no direct match, try stripping the last component after the final dot.
	lastDotIndex := strings.LastIndex(input, ".")

	// Ensure a dot exists and it's not the first character.
	if lastDotIndex <= 0 {
		// Cannot strip a component, and it wasn't a direct match.
		return "", false
	}

	// Extract the base part of the string before the last dot.
	base := input[:lastDotIndex]

	// Check if this extracted base matches any known EventType value.
	switch EventType(base) {
	case V1Agents:
		return V1Agents, true
	case V1ChatsUpdate:
		return V1ChatsUpdate, true
	case V1ChatsUpsert:
		return V1ChatsUpsert, true
	case V1MessagesUpsert:
		return V1MessagesUpsert, true
	case V1MessagesUpdate:
		return V1MessagesUpdate, true
	case V1ContactsUpsert:
		return V1ContactsUpsert, true
	case V1ContactsUpdate:
		return V1ContactsUpdate, true
	case V1HistoricalChats:
		return V1HistoricalChats, true
	case V1HistoricalMessages:
		return V1HistoricalMessages, true
	case V1HistoricalContacts:
		return V1HistoricalContacts, true
	default:
		// The extracted base doesn't match any known type.
		return "", false
	}
}

type MessageMetadata struct {
	ConsumerSequence uint64
	StreamSequence   uint64
	NumDelivered     uint64
	NumPending       uint64
	Timestamp        time.Time
	Stream           string
	Consumer         string
	Domain           string
	MessageID        string
	MessageSubject   string
	CompanyID        string
}

// ToLastMetadata converts MessageMetadata to LastMetadata
func (e MessageMetadata) ToLastMetadata() *LastMetadata {
	return &LastMetadata{
		ConsumerSequence: int64(e.ConsumerSequence),
		StreamSequence:   int64(e.StreamSequence),
		Stream:           e.Stream,
		Consumer:         e.Consumer,
		Domain:           e.Domain,
		MessageID:        e.MessageID,
		MessageSubject:   e.MessageSubject,
		CompanyID:        e.CompanyID,
	}
}

// GetVersion extracts the version from an event type
// Returns the version string (e.g., "v1") or an empty string if no version specified
func (e EventType) GetVersion() string {
	parts := strings.SplitN(string(e), ".", 2)
	if len(parts) < 2 {
		return ""
	}

	// Check if the first part starts with 'v' followed by a number
	if len(parts[0]) >= 2 && parts[0][0] == 'v' {
		return parts[0]
	}

	return ""
}

// GetBaseType returns the event type without the version prefix
// For example: "v1.messages.upsert" -> "messages.upsert"
func (e EventType) GetBaseType() EventType {
	version := e.GetVersion()
	if version == "" {
		return e
	}

	// Remove the version prefix and the following dot
	return EventType(strings.TrimPrefix(string(e), version+"."))
}

// WithVersion returns a new EventType with the specified version
// For example: "messages.upsert" with version "v2" -> "v2.messages.upsert"
func (e EventType) WithVersion(version string) EventType {
	// If the event already has a version, remove it first
	baseType := e.GetBaseType()

	// Add the new version
	return EventType(version + "." + string(baseType))
}

// LastMetadata represents a last message metadata from nats
type LastMetadata struct {
	ConsumerSequence int64  `json:"consumer_sequence"`
	StreamSequence   int64  `json:"stream_sequence"`
	Stream           string `json:"stream"`
	Consumer         string `json:"consumer"`
	Domain           string `json:"domain"`
	MessageID        string `json:"message_id"`
	MessageSubject   string `json:"message_subject"`
	CompanyID        string `json:"company_id"`
}
