ALTER TABLE events ADD COLUMN execution_policy TEXT NOT NULL DEFAULT 'actor_tools'
    CHECK(execution_policy IN ('actor_tools', 'skills_only'));
ALTER TABLE events ADD COLUMN ingress_source TEXT;
ALTER TABLE runs ADD COLUMN execution_policy TEXT NOT NULL DEFAULT 'actor_tools'
    CHECK(execution_policy IN ('actor_tools', 'skills_only'));
ALTER TABLE runs ADD COLUMN ingress_source TEXT;

CREATE TABLE webhook_receipts (
    endpoint TEXT NOT NULL,
    identity_kind TEXT NOT NULL CHECK(identity_kind IN ('explicit', 'automatic')),
    identity_hash BLOB NOT NULL CHECK(length(identity_hash) = 32),
    event_id TEXT NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    accepted_at INTEGER NOT NULL,
    PRIMARY KEY(endpoint, identity_kind, identity_hash, accepted_at)
) STRICT;

CREATE UNIQUE INDEX webhook_explicit_identity
ON webhook_receipts(endpoint, identity_hash) WHERE identity_kind = 'explicit';

CREATE INDEX webhook_automatic_lookup
ON webhook_receipts(endpoint, identity_hash, accepted_at)
WHERE identity_kind = 'automatic';

CREATE TABLE actor_latest_telegram_routes (
    actor_id TEXT PRIMARY KEY REFERENCES actors(id) ON DELETE CASCADE,
    gateway TEXT NOT NULL,
    address TEXT NOT NULL,
    max_text_chars INTEGER NOT NULL CHECK(max_text_chars > 0),
    max_caption_chars INTEGER NOT NULL CHECK(max_caption_chars > 0),
    mailbox_sequence INTEGER NOT NULL CHECK(mailbox_sequence > 0),
    updated_at INTEGER NOT NULL
) STRICT;

CREATE TABLE deferred_webhook_results (
    outbox_id TEXT PRIMARY KEY REFERENCES outbox(id) ON DELETE CASCADE,
    actor_id TEXT NOT NULL REFERENCES actors(id) ON DELETE CASCADE,
    event_sequence INTEGER NOT NULL CHECK(event_sequence > 0),
    state TEXT NOT NULL CHECK(state IN ('pending', 'released', 'superseded')),
    released_at INTEGER,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
) STRICT;

CREATE INDEX deferred_webhook_results_actor
ON deferred_webhook_results(actor_id, state, event_sequence DESC);
