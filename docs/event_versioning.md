# Event Versioning Guide

This document explains how to use versioned event types in the daisi-wa-events-processor.

## Overview

The daisi-wa-events-processor now supports versioned event types to allow for backward compatibility and future enhancements. Event types can now be prefixed with a version, for example:

- `v1.messages.upsert` instead of just `messages.upsert`
- `v2.chats.update` for a hypothetical future version

## How Versioning Works

### Event Type Format

Versioned event types follow this format:
```
<version>.<event_type>
```

For example:
- `v1.messages.upsert`
- `v1.history.chats`
- `v2.contacts.update`

### Backward Compatibility

The system still supports legacy (unversioned) event types, making the transition smooth. Both of these events will be properly routed:
- `messages.upsert` (legacy format)
- `v1.messages.upsert` (versioned format)

## Using Versioned Events

### Producing Events

When producing events, you can specify the version in the event type:

```json
{
  "id": "evt_12345",
  "type": "v1.messages.upsert",
  "timestamp": "2023-04-09T12:34:56Z",
  "company_id": "tenant_abc",
  "sender_id": "sender_123",
  "payload": {
    "message": {
      "id": "msg_123",
      "content": "Hello world"
    }
  }
}
```

### Testing with the Event Tester Tool

The event tester tool now supports versioning. Use the `-version` flag to add a version to your events:

```bash
go run cmd/tester/main.go -type messages.upsert -version v1 -tenant tenant_abc
```

This will produce an event with type `v1.messages.upsert`.

## Event Router Behavior

The event router follows these steps to find a handler for an event:

1. Exact match - Looks for a handler registered with the exact event type (including version)
2. Base type match - If no exact match, tries to find a handler registered for the base type (without version)
3. Prefix match - If still no match, tries to find a handler registered with a prefix pattern (e.g., "history.*")
4. Default handler - Falls back to the default handler if no specific handler is found

## Adding New Versions

When introducing a new version of an event:

1. Define the new event type constant:
   ```go
   V2MessagesUpsert EventType = "v2.messages.upsert"
   ```

2. Register a handler for the new event type:
   ```go
   router.Register(model.V2MessagesUpsert, v2MessageHandler.HandleEvent)
   ```

3. Implement the new handler with version-specific logic

## Accessing Version Information

The `EventType` type has methods to work with versioned events:

```go
// Get the version of an event
version := event.Type.GetVersion() // Returns "v1" for "v1.messages.upsert"

// Get the base type without version
baseType := event.Type.GetBaseType() // Returns "messages.upsert" for "v1.messages.upsert"

// Create a versioned event type
v2Type := model.MessagesUpsert.WithVersion("v2") // Returns "v2.messages.upsert"
```

## Best Practices

1. Always register handlers for all supported versions of an event
2. Use the helper methods (`GetVersion`, `GetBaseType`, `WithVersion`) when working with event types
3. When introducing breaking changes to an event schema, create a new version
4. Log both the full event type and its components (version, base type) for better debugging
5. Test all supported versions of events regularly 