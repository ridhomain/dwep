-- Initialize database schema for daisi-wa-events-processor
-- This script creates the necessary tables, indexes, and constraints for a specific tenant schema.

-- Set timezone to UTC
SET timezone = 'UTC';

-- Define the target schema based on the default tenant ID
CREATE SCHEMA IF NOT EXISTS daisi_tenant_dev;

-- Set the search path to the target schema for subsequent operations
SET search_path TO daisi_tenant_dev;

-- --- Table: messages (Partitioned) ---
-- DDL extracted from internal/storage/postgres.go
CREATE TABLE IF NOT EXISTS messages (
    id BIGSERIAL NOT NULL,
    message_id TEXT,
    "from" TEXT, -- Quoted because 'from' is a reserved keyword
    "to" TEXT,   -- Quoted because 'to' is a reserved keyword
    chat_id TEXT,
    jid TEXT,
    company_id VARCHAR,
    key JSONB,
    message_obj JSONB,
    flow TEXT,
    agent_id TEXT,
    status TEXT,
    is_deleted BOOLEAN DEFAULT false,
    message_timestamp BIGINT,
    message_date DATE NOT NULL, -- Partition key
    last_metadata JSONB,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    PRIMARY KEY (id, message_date) -- Partition key must be part of the primary key
)
PARTITION BY RANGE (message_date);

COMMENT ON TABLE messages IS 'Stores individual chat messages, partitioned by message_date.';
COMMENT ON COLUMN messages.message_date IS 'Date component of message_timestamp, used for partitioning.';

-- Indexes for messages (from postgres.go)
CREATE UNIQUE INDEX IF NOT EXISTS idx_messages_message_id ON messages USING btree (message_id, message_date);
CREATE INDEX IF NOT EXISTS idx_messages_from ON messages USING btree ("from");
CREATE INDEX IF NOT EXISTS idx_messages_to ON messages USING btree ("to");
CREATE INDEX IF NOT EXISTS idx_messages_chat_id ON messages USING btree (chat_id);
CREATE INDEX IF NOT EXISTS idx_messages_jid ON messages USING btree (jid);
CREATE INDEX IF NOT EXISTS idx_messages_agent_id ON messages USING btree (agent_id);
CREATE INDEX IF NOT EXISTS idx_messages_message_timestamp ON messages USING btree (message_timestamp);
CREATE INDEX IF NOT EXISTS idx_messages_message_date ON messages USING btree (message_date);

-- Default partition for messages (from postgres.go)
CREATE TABLE IF NOT EXISTS messages_default PARTITION OF messages DEFAULT;
COMMENT ON TABLE messages_default IS 'Default partition for messages that do not fall into a specific date range.';

-- --- Table: contacts ---
-- DDL extracted from internal/storage/postgres.go
CREATE TABLE IF NOT EXISTS contacts (
    id TEXT PRIMARY KEY,
    phone_number TEXT NOT NULL,
    type TEXT,
    custom_name TEXT,
    notes TEXT,
    tags TEXT,
    company_id VARCHAR,
    avatar TEXT,
    assigned_to TEXT,
    pob TEXT,
    dob DATE,
    gender TEXT DEFAULT 'MALE',
    origin TEXT,
    push_name TEXT,
    status TEXT DEFAULT 'ACTIVE',
    agent_id TEXT,
    first_message_id TEXT,
    first_message_timestamp BIGINT,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    last_metadata JSONB
);

COMMENT ON TABLE contacts IS 'Stores contact information.';

-- Indexes for contacts
CREATE UNIQUE INDEX IF NOT EXISTS idx_contacts_phone ON contacts USING btree (phone_number);
CREATE INDEX IF NOT EXISTS idx_contacts_assigned_to ON contacts USING btree (assigned_to);
CREATE INDEX IF NOT EXISTS idx_contacts_agent_id ON contacts USING btree (agent_id);

-- --- Table: agents ---
-- Derived from internal/model/agent.go
CREATE TABLE IF NOT EXISTS agents (
    id BIGSERIAL PRIMARY KEY,
    agent_id TEXT UNIQUE, -- uniqueIndex tag
    qr_code TEXT,
    status TEXT,
    agent_name TEXT,
    host_name TEXT,
    version TEXT,
    company_id TEXT,
    created_at TIMESTAMPTZ, -- autoCreateTime implies TIMESTAMPTZ
    updated_at TIMESTAMPTZ, -- autoUpdateTime implies TIMESTAMPTZ
    last_metadata JSONB
);

COMMENT ON TABLE agents IS 'Stores information about connected WhatsApp agent instances.';

-- --- Table: chats ---
-- Derived from internal/model/chat.go
CREATE TABLE IF NOT EXISTS chats (
    id BIGSERIAL PRIMARY KEY,
    chat_id TEXT UNIQUE, -- uniqueIndex tag
    jid TEXT,
    custom_name TEXT,
    push_name TEXT,
    is_group BOOLEAN,
    group_name TEXT,
    unread_count INTEGER,
    assigned_to TEXT,
    last_message JSONB,
    conversation_timestamp BIGINT,
    not_spam BOOLEAN,
    agent_id TEXT,
    company_id TEXT,
    phone_number TEXT,
    last_metadata JSONB,
    created_at TIMESTAMPTZ, -- autoCreateTime
    updated_at TIMESTAMPTZ  -- autoUpdateTime
);

COMMENT ON TABLE chats IS 'Represents chat sessions or conversations.';

-- Indexes for chats (from GORM model)
CREATE INDEX IF NOT EXISTS idx_chats_jid ON chats USING btree (jid);
CREATE INDEX IF NOT EXISTS idx_chats_assigned_to ON chats USING btree (assigned_to);
CREATE INDEX IF NOT EXISTS idx_chats_agent_id ON chats USING btree (agent_id);


-- --- Table: onboarding_log ---
-- Derived from internal/model/onboarding_log.go
CREATE TABLE IF NOT EXISTS onboarding_log (
    id BIGSERIAL PRIMARY KEY,
    message_id TEXT,
    agent_id TEXT,
    company_id TEXT,
    phone_number TEXT,
    timestamp BIGINT,
    created_at TIMESTAMPTZ, -- autoCreateTime
    last_metadata JSONB
);

COMMENT ON TABLE onboarding_log IS 'Records events where an incoming message potentially triggered contact onboarding.';

-- Indexes for onboarding_log (from GORM model)
CREATE INDEX IF NOT EXISTS idx_onboarding_log_message_id ON onboarding_log USING btree (message_id);
CREATE INDEX IF NOT EXISTS idx_onboarding_log_agent_id ON onboarding_log USING btree (agent_id);
CREATE INDEX IF NOT EXISTS idx_onboarding_log_phone_number ON onboarding_log USING btree (phone_number);


-- --- Table: exhausted_events ---
-- Derived from internal/model/exhausted_event.go
CREATE TABLE IF NOT EXISTS exhausted_events (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ,
    company_id TEXT NOT NULL,
    source_subject TEXT NOT NULL,
    last_error TEXT,
    retry_count INTEGER, -- GORM int maps to INTEGER
    event_timestamp TIMESTAMPTZ,
    dlq_payload JSONB NOT NULL,
    original_payload JSONB,
    resolved BOOLEAN DEFAULT false,
    resolved_at TIMESTAMPTZ,
    notes TEXT
);

COMMENT ON TABLE exhausted_events IS 'Stores messages that failed processing after all DLQ retry attempts.';

-- Indexes for exhausted_events
CREATE INDEX IF NOT EXISTS idx_exhausted_events_source_subject ON exhausted_events USING btree (source_subject);
CREATE INDEX IF NOT EXISTS idx_exhausted_events_event_timestamp ON exhausted_events USING btree (event_timestamp);
CREATE INDEX IF NOT EXISTS idx_exhausted_events_resolved ON exhausted_events USING btree (resolved);
CREATE INDEX IF NOT EXISTS idx_exhausted_events_resolved_at ON exhausted_events USING btree (resolved_at);

