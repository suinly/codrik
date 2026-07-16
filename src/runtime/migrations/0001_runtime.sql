PRAGMA foreign_keys = ON;

CREATE TABLE actors (
    id TEXT PRIMARY KEY,
    enabled INTEGER NOT NULL CHECK (enabled IN (0, 1)),
    tools_json TEXT NOT NULL,
    next_mailbox_sequence INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL
) STRICT;

CREATE TABLE identities (
    provider TEXT NOT NULL,
    subject TEXT NOT NULL,
    actor_id TEXT NOT NULL REFERENCES actors(id) ON DELETE CASCADE,
    username TEXT,
    PRIMARY KEY (provider, subject)
) STRICT;

CREATE TABLE work_items (
    id TEXT PRIMARY KEY,
    actor_id TEXT NOT NULL REFERENCES actors(id) ON DELETE CASCADE,
    kind TEXT NOT NULL CHECK (kind IN ('interactive', 'external')),
    audience_kind TEXT NOT NULL CHECK (audience_kind IN ('actor_private', 'conversation_scoped', 'shareable')),
    audience_address TEXT,
    state TEXT NOT NULL CHECK (state IN ('ready', 'waiting', 'completed', 'cancelled', 'failed_terminal', 'blocked_unknown_outcome', 'blocked_malformed', 'waiting_for_decision')),
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    CHECK ((audience_kind = 'conversation_scoped') = (audience_address IS NOT NULL))
) STRICT;

CREATE TABLE events (
    id TEXT PRIMARY KEY,
    actor_id TEXT NOT NULL REFERENCES actors(id) ON DELETE CASCADE,
    work_item_id TEXT REFERENCES work_items(id),
    mailbox_sequence INTEGER NOT NULL,
    gateway TEXT NOT NULL,
    external_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    audience_kind TEXT NOT NULL,
    audience_address TEXT,
    payload_json TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('pending', 'processing', 'completed', 'cancelled', 'failed_terminal', 'blocked')),
    run_id TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    UNIQUE (actor_id, mailbox_sequence),
    UNIQUE (gateway, external_id),
    CHECK ((audience_kind = 'conversation_scoped') = (audience_address IS NOT NULL))
) STRICT;

CREATE TABLE actor_leases (
    actor_id TEXT PRIMARY KEY REFERENCES actors(id) ON DELETE CASCADE,
    generation INTEGER NOT NULL,
    owner_id TEXT NOT NULL,
    expires_at INTEGER NOT NULL
) STRICT;

CREATE TABLE runs (
    id TEXT PRIMARY KEY,
    actor_id TEXT NOT NULL REFERENCES actors(id) ON DELETE CASCADE,
    work_item_id TEXT NOT NULL REFERENCES work_items(id) ON DELETE CASCADE,
    state TEXT NOT NULL CHECK (state IN ('active', 'completed', 'cancelled', 'failed_terminal')),
    lease_generation INTEGER NOT NULL,
    observed_sequence INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
) STRICT;

CREATE UNIQUE INDEX one_active_run_per_work_item
ON runs(work_item_id) WHERE state = 'active';

CREATE TABLE run_events (
    run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    event_id TEXT NOT NULL UNIQUE REFERENCES events(id) ON DELETE CASCADE,
    incorporated INTEGER NOT NULL DEFAULT 0 CHECK (incorporated IN (0, 1)),
    PRIMARY KEY (run_id, event_id)
) STRICT;

CREATE TABLE recent_messages (
    id INTEGER PRIMARY KEY,
    actor_id TEXT NOT NULL REFERENCES actors(id) ON DELETE CASCADE,
    work_item_id TEXT NOT NULL REFERENCES work_items(id) ON DELETE CASCADE,
    run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    audience_kind TEXT NOT NULL,
    audience_address TEXT,
    message_json TEXT NOT NULL,
    created_at INTEGER NOT NULL
) STRICT;

CREATE TABLE tool_attempts (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    tool_call_id TEXT NOT NULL,
    tool_name TEXT NOT NULL,
    arguments_json TEXT NOT NULL,
    capabilities_json TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('prepared', 'running', 'succeeded', 'failed_known', 'outcome_unknown', 'cancelled_known', 'waiting_for_decision')),
    outcome_json TEXT,
    observation_checkpointed INTEGER NOT NULL DEFAULT 0 CHECK (observation_checkpointed IN (0, 1)),
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    UNIQUE (run_id, tool_call_id)
) STRICT;

CREATE TABLE outbox (
    id TEXT PRIMARY KEY,
    intent_key TEXT NOT NULL UNIQUE,
    actor_id TEXT NOT NULL REFERENCES actors(id) ON DELETE CASCADE,
    work_item_id TEXT NOT NULL REFERENCES work_items(id) ON DELETE CASCADE,
    run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    intent_class TEXT NOT NULL,
    audience_kind TEXT NOT NULL,
    audience_address TEXT,
    payload_json TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('pending', 'delivering', 'delivered', 'failed_retryable', 'failed_terminal', 'outcome_unknown', 'acknowledged_duplicate')),
    attempt_count INTEGER NOT NULL DEFAULT 0,
    claim_owner TEXT,
    claim_expires_at INTEGER,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
) STRICT;

CREATE INDEX ready_events ON events(actor_id, state, kind, mailbox_sequence);
CREATE INDEX ready_outbox ON outbox(state, created_at);
