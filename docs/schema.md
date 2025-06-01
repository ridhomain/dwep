# JSON Schema Reference

## Overview

The Daisi WA Events Processor uses JSON schema validation to ensure incoming events meet the required structure and data types. This validation occurs during event ingestion to:

- Prevent invalid data from entering the system
- Ensure consistency across all tenants
- Provide clear error messages for debugging
- Support version compatibility

Validation is primarily handled by comparing incoming JSON against the schemas defined in `internal/model/jsonschema/`. Additional validation might occur via struct tags in `internal/model/payload.go` using `go-playground/validator` during Go struct unmarshalling, although the JSON schema is the primary contract.

## JSON Payload Schemas

This section describes the expected JSON structure for event payloads published to NATS JetStream, based on the `.json` schema files and supplemented by the Go `payload.go` structs.

### UpsertMessagePayload ([`message.upsert.schema.json`](../internal/model/jsonschema/message.upsert.schema.json))

Used for creating or updating a message in the system.

#### Fields & Types

| Field             | Type    | Description                                      | Required (Schema) | Required (Go Struct) |
|-------------------|---------|--------------------------------------------------|-------------------|----------------------|
| `message_id`      | string  | Unique identifier for the message              | Yes               | Yes (`validate:"required"`) |
| `to`              | string  | Recipient identifier                             | No                | No                   |
| `from`            | string  | Sender identifier                                | No                | No                   |
| `chat_id`         | string  | Identifier of the chat this message belongs to   | Yes               | Yes (`validate:"required"`) |
| `jid`             | string  | JID associated with the message context          | Yes               | Yes (`validate:"required"`) |
| `flow`            | string  | Message direction (`IN` or `OUT`)                | Yes               | Yes (`validate:"required"`) |
| `company_id`      | string  | Identifier for the company tenant                | Yes               | Yes (`validate:"required"`) |
| `agent_id`        | string  | Identifier for the Daisi agent involved        | No                | No                   |
| `key`             | object  | Object containing message key details            | No                | No (`*KeyPayload`)   |
| `key.id`          | string  | Unique ID within the key                         | Yes (if key)      | Yes (if key, `validate:"required"`) |
| `key.from_me`     | boolean | Indicates if the message was sent by the agent   | No                | No                   |
| `key.remote_jid`  | string  | The JID of the other party in the chat           | No                | No                   |
| `message_obj`     | object  | Full structured message content                  | No                | No (`map[string]interface{}`) |
| `status`          | string  | Delivery status of the message                   | Yes               | Yes (`validate:"required"`) |
| `message_timestamp` | integer | Unix timestamp (seconds) when message was sent/received | No                | No (`validate:"omitempty,gte=0"`) |


#### Enums

- **flow**: `IN`, `OUT` (Defined in schema, not explicitly in struct tags but enforced by logic)
- **status**: Not explicitly defined as enum in schema/struct, represents various delivery states (e.g., `sent`, `delivered`, `read`, `failed`).

#### Sample JSON

```json
{
  "message_id": "uuid:message-test001",
  "to": "user_y",
  "from": "user_x",
  "chat_id": "uuid:chat-123",
  "jid": "6281234567890@s.whatsapp.net",
  "flow": "IN",
  "company_id": "tenant_dev",
  "agent_id": "agent_whatsapp_1",
  "key": {
    "id": "uuid:key-test001",
    "from_me": false,
    "remote_jid": "6281234567890@s.whatsapp.net"
  },
  "message_obj": {
    "conversation": "Hello, this is a test message"
  },
  "status": "sent",
  "message_timestamp": 1714567800
}
```

### UpdateMessagePayload ([`message.update.schema.json`](../internal/model/jsonschema/message.update.schema.json))

Used for updating the status of an existing message.

#### Fields & Types

| Field      | Type   | Description                          | Required (Schema) | Required (Go Struct) |
|------------|--------|--------------------------------------|-------------------|----------------------|
| `id`       | string | Unique message ID (`message_id`) to update | Yes               | Yes (`validate:"required"`) |
| `company_id`| string | Identifier for the company tenant    | Yes               | Yes (`validate:"required"`) |
| `status`   | string | The new delivery status              | Yes               | Yes (`validate:"required"`) |

#### Enums

- **status**: Represents various delivery states (e.g., `sent`, `delivered`, `read`, `failed`).

#### Sample JSON

```json
{
  "id": "uuid:message-test001",
  "company_id": "tenant_dev",
  "status": "delivered"
}
```

### HistoryMessagePayload ([`message.history.schema.json`](../internal/model/jsonschema/message.history.schema.json))

Wrapper for bulk importing historical messages. Contains an array of `UpsertMessagePayload` objects.

#### Fields & Types

| Field      | Type  | Description                                | Required (Schema) | Required (Go Struct) |
|------------|-------|--------------------------------------------|-------------------|----------------------|
| `messages` | array | Array of `UpsertMessagePayload` objects | Yes               | Yes (`validate:"required,dive,required"`) |

#### Sample JSON

```json
{
  "messages": [
    {
      "message_id": "uuid:historical-msg-001",
      "chat_id": "uuid:chat-123",
      "jid": "6281234567890@s.whatsapp.net",
      "flow": "IN",
      "company_id": "tenant_dev",
      "agent_id": "agent_import_1",
      "message_obj": {
        "conversation": "First historical message"
      },
      "status": "read",
      "message_timestamp": 1714567700
    },
    {
      "message_id": "uuid:historical-msg-002",
      "chat_id": "uuid:chat-123",
      "jid": "6281234567890@s.whatsapp.net",
      "flow": "OUT",
      "company_id": "tenant_dev",
      "agent_id": "agent_import_1",
      "message_obj": {
        "image": {
          "url": "https://example.com/image.jpg",
          "caption": "Second historical message (image)"
        }
      },
      "status": "sent",
      "message_timestamp": 1714567800
    }
  ]
}
```

## Chat Schema

### UpsertChatPayload ([`chat.upsert.schema.json`](../internal/model/jsonschema/chat.upsert.schema.json))

Used for creating or updating a chat conversation.

#### Fields & Types

| Field                   | Type    | Description                             | Required (Schema) | Required (Go Struct) |
|-------------------------|---------|-----------------------------------------|-------------------|----------------------|
| `chat_id`               | string  | Unique chat identifier                  | Yes               | Yes (`validate:"required"`) |
| `jid`                   | string  | WhatsApp JID for the chat               | Yes               | Yes (`validate:"required"`) |
| `company_id`            | string  | Identifier for the company tenant       | Yes               | Yes (`validate:"required"`) |
| `agent_id`              | string  | Identifier for the Daisi agent          | No                | No                   |
| `phone_number`          | string  | Phone number associated with the chat   | Yes               | Yes (`validate:"required"`) |
| `push_name`             | string  | Push name provided by the client        | No                | No                   |
| `custom_name`           | string  | Custom alias or name for the chat       | No                | No                   |
| `is_group`              | boolean | Flag indicating if the chat is a group chat | No                | No                   |
| `group_name`            | string  | Name of the group if `is_group` is true | No                | No                   |
| `assigned_to`           | string  | Identifier of the agent assigned        | No                | No                   |
| `last_message`          | object  | Object containing last message details  | No                | No (`LastMessageObj`) |
| `conversation_timestamp`| integer | Unix timestamp (seconds) of last activity | No                | No (`validate:"omitempty,gte=0"`) |
| `unread_count`          | integer | Number of unread messages               | No                | No (`validate:"omitempty,gte=0"`) |
| `not_spam`              | boolean | Flag indicating if chat is not spam     | No                | No                   |

#### Sample JSON

```json
{
  "chat_id": "uuid:chat-test001",
  "jid": "6281234567890@s.whatsapp.net",
  "company_id": "tenant_dev",
  "agent_id": "agent_whatsapp_1",
  "phone_number": "6281234567890",
  "push_name": "Test Customer",
  "custom_name": "Test Chat",
  "is_group": false,
  "group_name": "",
  "assigned_to": "agent_007",
  "last_message": {
    "id": "uuid:msg-last001",
    "content": "This is the last message in the chat"
  },
  "conversation_timestamp": 1714567890,
  "unread_count": 2,
  "not_spam": true
}
```

### UpdateChatPayload ([`chat.update.schema.json`](../internal/model/jsonschema/chat.update.schema.json))

Used for updating specific fields of an existing chat.

#### Fields & Types

| Field                   | Type    | Description                        | Required (Schema) | Required (Go Struct) |
|-------------------------|---------|------------------------------------|-------------------|----------------------|
| `id`                    | string  | Unique chat ID to update           | Yes               | Yes (`validate:"required"`) |
| `company_id`            | string  | Company identifier                 | Yes               | Yes (`validate:"required"`) |
| `unread_count`          | integer | Updated unread message count       | No                | No (`validate:"omitempty,gte=0"`) |
| `conversation_timestamp`| integer | Updated conversation timestamp     | No                | No (`validate:"omitempty,gte=0"`) |

#### Sample JSON

```json
{
  "id": "uuid:chat-test001",
  "company_id": "tenant_dev",
  "unread_count": 0,
  "conversation_timestamp": 1714568000
}
```

### HistoryChatPayload ([`chat.history.schema.json`](../internal/model/jsonschema/chat.history.schema.json))

Wrapper for bulk importing historical chats. Contains an array of `UpsertChatPayload` objects.

#### Fields & Types

| Field   | Type  | Description                             | Required (Schema) | Required (Go Struct) |
|---------|-------|-----------------------------------------|-------------------|----------------------|
| `chats` | array | Array of `UpsertChatPayload` objects | Yes               | Yes (`validate:"required,dive,required"`) |

#### Sample JSON

```json
{
  "chats": [
    {
      "chat_id": "uuid:historical-chat-001",
      "jid": "6281234567890@s.whatsapp.net",
      "company_id": "tenant_dev",
      "phone_number": "6281234567890",
      "agent_id": "agent_import_1",
      "unread_count": 0,
      "is_group": false,
      "conversation_timestamp": 1714567700
    },
    {
      "chat_id": "uuid:historical-chat-002",
      "jid": "group-123@g.us",
      "company_id": "tenant_dev",
      "phone_number": "", // Or null if not applicable for group
      "agent_id": "agent_import_1",
      "push_name": "Support Group",
      "group_name": "Support Group",
      "is_group": true,
      "conversation_timestamp": 1714567800
    }
  ]
}
```

## Contact Schema

### UpsertContactPayload ([`contact.upsert.schema.json`](../internal/model/jsonschema/contact.upsert.schema.json))

Used for creating or updating a contact.

#### Fields & Types

| Field                     | Type    | Description                                              | Required (Schema) | Required (Go Struct) |
|---------------------------|---------|----------------------------------------------------------|-------------------|----------------------|
| `id`                      | string  | Unique identifier for the contact                        | Yes               | Yes (`validate:"required"`) |
| `phone_number`            | string  | Normalized phone number of the contact                   | Yes               | Yes (`validate:"required"`) |
| `type`                    | string  | Type of the contact                                      | No                | No (`validate:"omitempty,oneof=PERSONAL AGENT OTHER"`) |
| `custom_name`             | string  | Custom alias or name for the contact                     | No                | No                   |
| `notes`                   | string  | Freeform notes associated with the contact               | No                | No                   |
| `tags`                    | string  | Tags associated with the contact (e.g., comma-separated) | No             | No                   |
| `company_id`              | string  | Identifier for the company tenant                        | Yes               | Yes (`validate:"required"`) |
| `avatar`                  | string  | URL or reference to the contact's profile picture        | No                | No                   |
| `assigned_to`             | string  | Identifier of the agent assigned to this contact         | No                | No                   |
| `pob`                     | string  | Place of birth of the contact                            | No                | No (`PlaceOfBirth`)  |
| `dob`                     | integer | Date of birth (Unix timestamp in seconds)                | No                | No (`DateOfBirth`)   |
| `gender`                  | string  | Gender of the contact                                    | No                | No (`validate:"omitempty,oneof=MALE FEMALE"`) |
| `origin`                  | string  | Source from where the contact originated                 | No                | No                   |
| `push_name`               | string  | Push name obtained from WhatsApp metadata                | No                | No                   |
| `status`                  | string  | Current status of the contact                            | No                | No (`validate:"omitempty,oneof=ACTIVE DISABLED"`) |
| `agent_id`                | string  | Identifier of the agent primarily responsible            | Yes               | Yes (`validate:"required"`) |
| `first_message_timestamp` | integer  | Identifier of the first message Unix timestamp (seconds) received       | No                | No                   |
| `first_message_id`        | string  | Identifier of the first message received                 | No                | No                   |

#### Enums

- **type**: `PERSONAL`, `AGENT`, `OTHER`
- **gender**: `MALE`, `FEMALE`
- **status**: `ACTIVE`, `DISABLED`

#### Sample JSON

```json
{
  "id": "uuid:contact-abc",
  "phone_number": "6281122334455",
  "type": "PERSONAL",
  "custom_name": "Wira",
  "notes": "Important lead",
  "tags": "lead,jakarta",
  "company_id": "tenant_dev",
  "avatar": "https://example.com/avatar.png",
  "assigned_to": "agent_007",
  "pob": "Jakarta",
  "dob": 946684800, // Unix timestamp for 2000-01-01
  "gender": "MALE",
  "origin": "whatsapp",
  "push_name": "Wira S",
  "status": "ACTIVE",
  "agent_id": "agent_whatsapp_1",
  "first_message_id": "uuid:message-first"
}
```

### UpdateContactPayload ([`contact.update.schema.json`](../internal/model/jsonschema/contact.update.schema.json))

Used for partially updating an existing contact. Fields are nullable in the schema (using `type: ["<type>", "null"]`) corresponding to pointers in the Go struct (`*<type>`).

#### Fields & Types

| Field          | Type             | Description                                  | Required (Schema) | Required (Go Struct) |
|----------------|------------------|----------------------------------------------|-------------------|----------------------|
| `id`           | string           | Unique contact ID to update                  | Yes               | Yes (`validate:"required"`) |
| `company_id`   | string           | Company identifier                           | Yes               | Yes (`validate:"required"`) |
| `custom_name`  | string or null   | Updated custom name                          | No                | No (`*string`)         |
| `notes`        | string or null   | Updated notes                                | No                | No (`*string`)         |
| `tags`         | string or null   | Updated tags                                 | No                | No (`*string`)         |
| `avatar`       | string or null   | Updated avatar URL                           | No                | No (`*string`)         |
| `assigned_to`  | string or null   | Updated assigned agent/user ID               | No                | No (`*string`)         |
| `pob`          | string or null   | Updated place of birth                       | No                | No (`*string`, `PlaceOfBirth`) |
| `dob`          | integer or null  | Updated date of birth (Unix timestamp)       | No                | No (`*int64`, `DateOfBirth`) |
| `gender`       | string or null   | Updated gender (`MALE` or `FEMALE`)          | No                | No (`*string`, `validate:"omitempty,oneof=MALE FEMALE"`) |
| `status`       | string or null   | Updated status (`ACTIVE` or `DISABLED`)      | No                | No (`*string`, `validate:"omitempty,oneof=ACTIVE DISABLED"`) |

#### Enums

- **gender**: `MALE`, `FEMALE` (nullable)
- **status**: `ACTIVE`, `DISABLED` (nullable)

#### Sample JSON

```json
{
  "id": "uuid:contact-abc",
  "company_id": "tenant_dev",
  "tags": "lead,jakarta,vip",
  "assigned_to": "agent_008",
  "status": "ACTIVE",
  "gender": null, // Example of explicitly setting a field to null
  "pob": "Bandung"
}
```

### HistoryContactPayload ([`contact.history.schema.json`](../internal/model/jsonschema/contact.history.schema.json))

Wrapper for bulk importing historical contacts. Contains an array of `UpsertContactPayload` objects.

#### Fields & Types

| Field      | Type  | Description                               | Required (Schema) | Required (Go Struct) |
|------------|-------|-------------------------------------------|-------------------|----------------------|
| `contacts` | array | Array of `UpsertContactPayload` objects | Yes               | Yes (`validate:"required,dive,required"`) |

#### Sample JSON

```json
{
  "contacts": [
    {
      "id": "uuid:hist-contact-01",
      "phone_number": "6289988776655",
      "company_id": "tenant_dev",
      "custom_name": "Historical Contact 1",
      "agent_id": "agent_import_1",
      "type": "PERSONAL",
      "status": "ACTIVE"
    },
    {
      "id": "uuid:hist-contact-02",
      "phone_number": "6285544332211",
      "company_id": "tenant_dev",
      "custom_name": "Historical Contact 2",
      "agent_id": "agent_import_1",
      "tags": "archived",
      "status": "DISABLED"
    }
  ]
}
```

## Agent Schema

### UpsertAgentPayload ([`agent.upsert.schema.json`](../internal/model/jsonschema/agent.upsert.schema.json))

Used for creating or updating agent status information.

#### Fields & Types

| Field        | Type   | Description                                      | Required (Schema) | Required (Go Struct) |
|--------------|--------|--------------------------------------------------|-------------------|----------------------|
| `agent_id`   | string | Unique identifier for the agent                  | Yes               | Yes (`validate:"required"`) |
| `company_id` | string | Identifier for the company tenant this agent belongs to | Yes            | Yes (`validate:"required"`) |
| `qr_code`    | string | QR code string for linking or pairing the agent    | No                | No                   |
| `status`     | string | Current connection status of the agent             | No                | No                   |
| `agent_name` | string | Custom label or name for the agent               | No                | No                   |
| `host_name`  | string | Hostname of the device or server running the agent | No                | No                   |
| `version`    | string | Version information of the agent software        | No                | No                   |

#### Sample JSON

```json
{
  "agent_id": "wa-agent-instance-01",
  "company_id": "tenant_dev",
  "qr_code": "BASE64_ENCODED_QR_STRING",
  "status": "connected",
  "agent_name": "Primary WhatsApp Agent",
  "host_name": "agent-host-1.internal",
  "version": "1.2.3"
}
```

## DLQ Schema

### DLQPayload (Defined in `payload.go`, no separate JSON schema provided)

Structure of messages published to the Dead-Letter Queue.

#### Fields & Types

| Field              | Type            | Description                                                           | Required (Go Struct) |
|--------------------|-----------------|-----------------------------------------------------------------------|----------------------|
| `source_subject`   | string          | The original NATS subject the message was published to                | Yes                  |
| `company`          | string          | The company ID associated with the failed message                     | Yes                  |
| `original_payload` | json.RawMessage | The raw JSON byte slice of the original message payload             | Yes                  |
| `error`            | string          | The error message encountered during the last processing attempt    | Yes                  |
| `error_type`       | string          | Classification of the error (e.g., `fatal`, `retryable`)              | Yes                  |
| `retry_count`      | uint64          | Number of delivery attempts made by JetStream before sending to DLQ   | Yes                  |
| `max_retry`        | int             | The configured `max_deliver` value for the original consumer          | Yes                  |
| `next_retry_at`    | *time.Time      | Timestamp indicating when the DLQ worker should next attempt processing | No                   |
| `ts`               | time.Time       | Timestamp when the message was published to the DLQ                   | Yes                  |

#### Sample JSON

```json
{
  "source_subject": "v1.messages.upsert.tenant_dev",
  "company": "tenant_dev",
  "original_payload": {
    "message_id": "uuid:failed-message-123",
    "jid": "6281234567890@s.whatsapp.net",
    "status": "sent",
    "message_obj": {"conversation": "This message failed processing"}
  },
  "error": "failed to connect to external service: timeout",
  "error_type": "retryable",
  "retry_count": 5,
  "max_retry": 5,
  "ts": "2023-10-27T10:30:00Z"
}
```

## Onboarding Log Schema

### onboarding_log Table

Stores a record when an incoming message potentially triggers a new contact creation.

#### Fields & Types

| Column        | Type        | Notes                                 |
|---------------|-------------|---------------------------------------|
| id            | BIGINT      | Internal auto-incrementing primary key|
| message_id    | TEXT        | Source message ID that triggered log  |
| agent_id      | TEXT        | Agent ID from the source message      |
| company_id    | TEXT        | Company ID from the source message    |
| phone_number  | TEXT        | Cleaned phone number from message `from` |
| timestamp     | BIGINT      | Unix timestamp of the source message  |
| created_at    | TIMESTAMPTZ | Timestamp when the log record was created |
| last_metadata | JSONB       | NATS metadata from the triggering event |


## Database Schemas (PostgreSQL)

This section details the structure of the PostgreSQL tables used by the service. Note that in the multi-tenant approach, these tables reside within a company-specific schema (e.g., `daisi_<company_id>`) or use implicit filtering based on the `company_id` column.

### Table: `agents`

Stores information about connected WhatsApp agent instances.

| Column        | Type        | Notes                                         |
|---------------|-------------|-----------------------------------------------|
| id            | BIGINT      | Internal auto-incrementing primary key        |
| agent_id      | TEXT        | Unique agent identifier (Indexed, Unique)     |
| qr_code       | TEXT        | QR code data for pairing (nullable)           |
| status        | TEXT        | Agent connection status (nullable)            |
| agent_name    | TEXT        | Custom name/label for the agent (nullable)    |
| host_name     | TEXT        | Hostname where the agent is running (nullable)|
| version       | TEXT        | Agent software version (nullable)             |
| company_id    | TEXT        | Owning company/tenant identifier (nullable)   |
| created_at    | TIMESTAMPTZ | Timestamp of record creation                  |
| updated_at    | TIMESTAMPTZ | Timestamp of last record update               |
| last_metadata | JSONB       | Metadata from the last event processed (nullable) |

### Table: `chats`

Represents chat sessions or conversations.

| Column                  | Type        | Notes                                                |
|-------------------------|-------------|------------------------------------------------------|
| id                      | BIGINT      | Internal auto-incrementing primary key               |
| chat_id                 | TEXT        | Unique chat identifier (Indexed, Unique)             |
| jid                     | TEXT        | WhatsApp JID (user or group) (Indexed, nullable)     |
| custom_name             | TEXT        | Custom name for the chat (nullable)                  |
| push_name               | TEXT        | Name provided by WhatsApp client (nullable)          |
| is_group                | BOOLEAN     | Flag indicating if it's a group chat (nullable)      |
| group_name              | TEXT        | Name of the group if `is_group` is true (nullable) |
| unread_count            | INTEGER     | Count of unread messages (nullable)                  |
| assigned_to             | TEXT        | Assigned agent ID (Indexed, nullable)                |
| last_message            | JSONB       | Snapshot of the last message object (nullable)       |
| conversation_timestamp  | BIGINT      | Unix timestamp of the last message (nullable)        |
| not_spam                | BOOLEAN     | Flag indicating if marked as not spam (nullable)     |
| agent_id                | TEXT        | Associated agent ID (Indexed, nullable)              |
| company_id              | TEXT        | Owning company/tenant identifier (nullable)          |
| phone_number            | TEXT        | Associated phone number (nullable)                   |
| last_metadata           | JSONB       | Metadata from the last event processed (nullable)    |
| created_at              | TIMESTAMPTZ | Timestamp of record creation                         |
| updated_at              | TIMESTAMPTZ | Timestamp of last record update                      |

### Table: `contacts`

Stores contact information.

| Column                   | Type        | Notes                                                     |
|--------------------------|-------------|-----------------------------------------------------------|
| id                       | TEXT        | Unique contact identifier (Primary Key)                   |
| phone_number             | TEXT        | Contact's phone number (Indexed, Required)                |
| type                     | TEXT        | Contact type (e.g., PERSONAL, AGENT) (nullable)           |
| custom_name              | TEXT        | Custom name assigned to the contact (nullable)            |
| notes                    | TEXT        | Notes about the contact (nullable)                        |
| tags                     | TEXT        | Comma-separated tags or keywords (nullable)               |
| company_id               | TEXT        | Owning company/tenant identifier (nullable)               |
| avatar                   | TEXT        | URL to the contact's avatar image (nullable)              |
| assigned_to              | TEXT        | ID of the assigned agent/user (Indexed, nullable)         |
| pob                      | TEXT        | Place of birth (nullable)                                 |
| dob                      | DATE        | Date of birth (nullable)                                  |
| gender                   | TEXT        | Gender (e.g., MALE, FEMALE) (Default: MALE, nullable)     |
| origin                   | TEXT        | Source or origin of the contact (nullable)                |
| push_name                | TEXT        | Name from messaging platform (nullable)                   |
| status                   | TEXT        | Contact status (e.g., ACTIVE, DISABLED) (Default: ACTIVE) |
| agent_id                 | TEXT        | Associated agent ID (Indexed, nullable)                   |
| first_message_id         | TEXT        | ID of the first message received (nullable)               |
| first_message_timestamp         | BIGINT        | Unix timestamp of the first message (nullable)            |
| created_at               | TIMESTAMPTZ | Timestamp of record creation                              |
| updated_at               | TIMESTAMPTZ | Timestamp of last record update                           |
| last_metadata            | JSONB       | Metadata from the last event processed (nullable)         |

### Table: `messages`

Stores individual chat messages.

| Column             | Type        | Notes                                                            |
|--------------------|-------------|------------------------------------------------------------------|
| id                 | BIGINT      | Internal auto-incrementing primary key                           |
| message_id         | TEXT        | Unique message identifier from source (Indexed)                  |
| from               | TEXT        | Sender identifier (Indexed, nullable)                            |
| to                 | TEXT        | Recipient identifier (Indexed, nullable)                         |
| chat_id            | TEXT        | Associated chat identifier (Indexed, nullable)                   |
| jid                | TEXT        | WhatsApp JID associated with the message (Indexed, nullable)   |
| flow               | TEXT        | Message direction (e.g., inbound, outbound) (nullable)         |
| agent_id           | TEXT        | Associated agent ID (Indexed, nullable)                          |
| company_id         | TEXT        | Owning company/tenant identifier (nullable)                      |
| message_obj        | JSONB       | Full structured message content (nullable)                       |
| key                | JSONB       | Message key metadata from source (nullable)                      |
| status             | TEXT        | Message delivery status (nullable)                               |
| is_deleted         | BOOLEAN     | Flag indicating if message is soft-deleted (Default: false)      |
| message_timestamp  | BIGINT      | Unix timestamp of the original message (Indexed, nullable)       |
| message_date       | DATE        | Date part of the message timestamp (Not Null)                    |
| created_at         | TIMESTAMPTZ | Timestamp of record creation                                     |
| updated_at         | TIMESTAMPTZ | Timestamp of last record update                                  |
| last_metadata      | JSONB       | Metadata from the last event processed (nullable)              |

### Table: `onboarding_log`

Records events where an incoming message potentially triggered contact onboarding.

| Column        | Type        | Notes                                                  |
|---------------|-------------|--------------------------------------------------------|
| id            | BIGINT      | Internal auto-incrementing primary key                 |
| message_id    | TEXT        | Source message ID that triggered the log (Indexed)     |
| agent_id      | TEXT        | Agent ID from the source message (Indexed, nullable)   |
| company_id    | TEXT        | Company ID from the source message (nullable)          |
| phone_number  | TEXT        | Cleaned phone number from message `from` (Indexed)     |
| timestamp     | BIGINT      | Unix timestamp of the source message                   |
| created_at    | TIMESTAMPTZ | Timestamp when the log record was created              |
| last_metadata | JSONB       | Metadata from the triggering event (nullable)        |

### Table: `exhausted_events`

Stores messages that failed processing after all DLQ retry attempts.

| Column             | Type        | Notes                                                            |
|--------------------|-------------|------------------------------------------------------------------|
| id                 | BIGINT      | Internal auto-incrementing primary key (Note: GORM uint maps to this) |
| created_at         | TIMESTAMPTZ | Timestamp when the exhausted record was created                  |
| company_id         | TEXT        | Owning company/tenant identifier (Indexed, Not Null)             |
| source_subject     | TEXT        | Original NATS subject of the failed message (Indexed, Not Null) |
| last_error         | TEXT        | Error message from the final failed attempt (nullable)           |
| retry_count        | INTEGER     | Final retry count when exhaustion occurred                       |
| event_timestamp    | TIMESTAMPTZ | Timestamp from the DLQ payload (`ts` field) (Indexed)          |
| dlq_payload        | JSONB       | The full JSON payload from the DLQ message (Not Null)          |
| original_payload   | JSONB       | The extracted original message payload (nullable)                |
| resolved           | BOOLEAN     | Flag indicating manual resolution (Indexed, Default: false)      |
| resolved_at        | TIMESTAMPTZ | Timestamp when marked as resolved (Indexed, nullable)            |
| notes              | TEXT        | Manual notes regarding resolution (nullable)                     |

## Notes

### Additional Properties

The JSON schemas enforce `additionalProperties: false`, meaning any extra fields not defined in the schema will cause validation to fail. This helps ensure consistency and prevents typos or incorrect fields.

### Validation Process

1. The service uses the `go-playground/validator` package for input validation
2. JSON payloads are unmarshalled into Go structs
3. Struct field tags like `validate:"required"` define validation rules
4. Custom error messages are returned for failed validation

### Versioning Plan

Event types follow a versioning convention:
- All events are prefixed with a version (e.g., `v1.messages.upsert`)
- The `EventType` struct includes methods to:
  - Extract version: `GetVersion()`
  - Get base type without version: `GetBaseType()`
  - Apply a different version: `WithVersion()`

This allows for backward compatibility and future schema evolution.

## Supported Schemas

This service validates the following event types:

| Event Type | Schema | Description |
|------------|--------|-------------|
| `v1.messages.upsert` | UpsertMessagePayload | Create or update a message |
| `v1.messages.update` | UpdateMessagePayload | Update message status |
| `v1.chats.upsert` | UpsertChatPayload | Create or update a chat |
| `v1.chats.update` | UpdateChatPayload | Update chat metadata |
| `v1.history.messages` | HistoryMessagePayload | Bulk import historical messages |
| `v1.history.chats` | HistoryChatPayload | Bulk import historical chats | 
| `v1.contacts.upsert` | UpsertContactPayload | Create or update a contact |
| `v1.contacts.update` | UpdateContactPayload | Update contact metadata |
| `v1.contacts.history` | HistoryContactPayload | Bulk import historical contacts |
| `v1.connection.update` | UpsertAgentPayload | Create or update agent status information |
| `v1.dlq` | DLQPayload | Dead-Letter Queue message structure | 