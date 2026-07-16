ALTER TABLE events ADD COLUMN delivery_gateway TEXT;
ALTER TABLE events ADD COLUMN delivery_address TEXT;
ALTER TABLE events ADD COLUMN reply_to_external_id TEXT;
ALTER TABLE events ADD COLUMN delivery_max_text_chars INTEGER;
ALTER TABLE events ADD COLUMN delivery_max_caption_chars INTEGER;

ALTER TABLE runs ADD COLUMN delivery_gateway TEXT;
ALTER TABLE runs ADD COLUMN delivery_address TEXT;
ALTER TABLE runs ADD COLUMN reply_to_external_id TEXT;
ALTER TABLE runs ADD COLUMN delivery_max_text_chars INTEGER;
ALTER TABLE runs ADD COLUMN delivery_max_caption_chars INTEGER;

CREATE TABLE gateway_commands (
    gateway TEXT NOT NULL,
    external_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    outcome_json TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    PRIMARY KEY(gateway, external_id)
) STRICT;

CREATE TABLE gateway_deliveries (
    id TEXT PRIMARY KEY,
    intent_key TEXT NOT NULL UNIQUE,
    source_outbox_id TEXT REFERENCES outbox(id),
    gateway TEXT NOT NULL,
    address TEXT NOT NULL,
    reply_to_external_id TEXT,
    max_text_chars INTEGER NOT NULL CHECK(max_text_chars > 0),
    max_caption_chars INTEGER NOT NULL CHECK(max_caption_chars > 0),
    ordinal INTEGER NOT NULL CHECK(ordinal >= 0),
    payload_json TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN (
        'pending',
        'delivering',
        'delivered',
        'failed_retryable',
        'failed_terminal',
        'outcome_unknown'
    )),
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK(attempt_count >= 0),
    next_attempt_at INTEGER,
    claim_owner TEXT,
    claim_expires_at INTEGER,
    remote_message_id TEXT,
    error_class TEXT,
    last_error TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    UNIQUE(source_outbox_id, gateway, address, ordinal)
) STRICT;

CREATE INDEX ready_gateway_deliveries
ON gateway_deliveries(gateway, state, next_attempt_at, created_at);

CREATE TABLE gateway_streams (
    work_item_id TEXT NOT NULL REFERENCES work_items(id) ON DELETE CASCADE,
    gateway TEXT NOT NULL,
    address TEXT NOT NULL,
    remote_message_id TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('active', 'closed')),
    updated_at INTEGER NOT NULL,
    PRIMARY KEY(work_item_id, gateway, address)
) STRICT;
