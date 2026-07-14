ALTER TABLE work_items ADD COLUMN failure_count INTEGER NOT NULL DEFAULT 0 CHECK(failure_count >= 0);
ALTER TABLE work_items ADD COLUMN next_attempt_at INTEGER;
ALTER TABLE work_items ADD COLUMN last_error TEXT;
ALTER TABLE work_items ADD COLUMN cancellation_requested_at INTEGER;

CREATE TABLE legacy_outbox_archive (
    id TEXT PRIMARY KEY,
    intent_key TEXT NOT NULL UNIQUE,
    actor_id TEXT NOT NULL,
    work_item_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    intent_class TEXT NOT NULL,
    audience_kind TEXT NOT NULL,
    audience_address TEXT,
    payload_json TEXT NOT NULL,
    state TEXT NOT NULL,
    attempt_count INTEGER NOT NULL,
    claim_owner TEXT,
    claim_expires_at INTEGER,
    last_error TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    source_schema_version INTEGER NOT NULL DEFAULT 1 CHECK(source_schema_version = 1)
) STRICT;

INSERT INTO legacy_outbox_archive (
    id, intent_key, actor_id, work_item_id, run_id, intent_class,
    audience_kind, audience_address, payload_json, state, attempt_count,
    claim_owner, claim_expires_at, last_error, created_at, updated_at
)
SELECT id, intent_key, actor_id, work_item_id, run_id, intent_class,
       audience_kind, audience_address, payload_json, state, attempt_count,
       claim_owner, claim_expires_at,
       NULL, -- Authoritative v1 has no last_error column.
       created_at, updated_at
FROM outbox;

DROP TABLE outbox;

CREATE TABLE artifacts (
    id TEXT PRIMARY KEY,
    actor_id TEXT NOT NULL REFERENCES actors(id) ON DELETE CASCADE,
    attempt_id TEXT REFERENCES tool_attempts(id),
    state TEXT NOT NULL CHECK(state IN ('staging','referenced')),
    managed_path TEXT NOT NULL UNIQUE,
    display_name TEXT NOT NULL,
    media_type TEXT NOT NULL,
    size_bytes INTEGER CHECK(size_bytes BETWEEN 0 AND 268435456),
    sha256 TEXT CHECK(sha256 IS NULL OR length(sha256) = 64),
    staging_owner TEXT,
    staging_expires_at INTEGER,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    CHECK ((state = 'staging') = (staging_expires_at IS NOT NULL)),
    CHECK (state = 'staging' OR (size_bytes IS NOT NULL AND sha256 IS NOT NULL))
) STRICT;

CREATE TABLE local_requests (
    request_id TEXT PRIMARY KEY,
    actor_id TEXT NOT NULL REFERENCES actors(id),
    event_id TEXT NOT NULL UNIQUE REFERENCES events(id),
    work_item_id TEXT NOT NULL REFERENCES work_items(id),
    prompt_sha256 TEXT NOT NULL CHECK(length(prompt_sha256) = 64),
    state TEXT NOT NULL CHECK(state IN ('active','completed','cancelled','failed_terminal')),
    result_bundle_id TEXT UNIQUE REFERENCES result_bundles(id) DEFERRABLE INITIALLY DEFERRED,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    CHECK ((state = 'active') = (result_bundle_id IS NULL))
) STRICT;

CREATE TABLE result_bundles (
    id TEXT PRIMARY KEY,
    request_id TEXT NOT NULL UNIQUE REFERENCES local_requests(request_id),
    delivery_count INTEGER NOT NULL CHECK(delivery_count BETWEEN 1 AND 1024),
    manifest_sha256 TEXT NOT NULL CHECK(length(manifest_sha256) = 64),
    state TEXT NOT NULL CHECK(state IN ('pending','delivering','delivered','failed_retryable','failed_terminal')),
    attempt_count INTEGER NOT NULL DEFAULT 0,
    next_attempt_at INTEGER,
    claim_owner TEXT,
    claim_expires_at INTEGER,
    last_error TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
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
    created_at INTEGER NOT NULL
) STRICT;

CREATE TABLE outbox_deliveries (
    id TEXT PRIMARY KEY,
    outbox_id TEXT NOT NULL REFERENCES outbox(id),
    bundle_id TEXT NOT NULL REFERENCES result_bundles(id),
    ordinal INTEGER NOT NULL CHECK(ordinal >= 0),
    transport TEXT NOT NULL CHECK(transport = 'local_ipc'),
    address TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    UNIQUE(outbox_id, transport, address),
    UNIQUE(bundle_id, ordinal)
) STRICT;

CREATE TRIGGER outbox_deliveries_are_immutable_on_update
BEFORE UPDATE ON outbox_deliveries
BEGIN
    SELECT RAISE(ABORT, 'outbox_deliveries are immutable');
END;

CREATE TRIGGER outbox_deliveries_are_immutable_on_delete
BEFORE DELETE ON outbox_deliveries
BEGIN
    SELECT RAISE(ABORT, 'outbox_deliveries are append-only');
END;

CREATE TABLE cancel_targets (
    cancel_id TEXT NOT NULL,
    request_id TEXT NOT NULL REFERENCES local_requests(request_id),
    created_at INTEGER NOT NULL,
    PRIMARY KEY(cancel_id, request_id)
) STRICT;

CREATE TABLE legacy_runtime_quarantine (
    id INTEGER PRIMARY KEY,
    entity_type TEXT NOT NULL CHECK(entity_type IN ('work_item','run','event','tool_attempt')),
    entity_id TEXT NOT NULL,
    prior_state TEXT NOT NULL,
    snapshot_json TEXT NOT NULL,
    quarantined_at INTEGER NOT NULL,
    UNIQUE(entity_type, entity_id)
) STRICT;

INSERT INTO legacy_runtime_quarantine (entity_type, entity_id, prior_state, snapshot_json, quarantined_at)
SELECT 'work_item', id, state,
       json_object(
           'id', id,
           'actor_id', actor_id,
           'kind', kind,
           'audience_kind', audience_kind,
           'audience_address', audience_address,
           'state', state,
           'created_at', created_at,
           'updated_at', updated_at
       ), updated_at
FROM work_items
WHERE state IN ('ready', 'waiting', 'blocked_unknown_outcome', 'waiting_for_decision');

INSERT INTO legacy_runtime_quarantine (entity_type, entity_id, prior_state, snapshot_json, quarantined_at)
SELECT 'run', id, state,
       json_object(
           'id', id,
           'actor_id', actor_id,
           'work_item_id', work_item_id,
           'state', state,
           'lease_generation', lease_generation,
           'observed_sequence', observed_sequence,
           'created_at', created_at,
           'updated_at', updated_at
       ), updated_at
FROM runs
WHERE state = 'active';

INSERT INTO legacy_runtime_quarantine (entity_type, entity_id, prior_state, snapshot_json, quarantined_at)
SELECT 'event', id, state,
       json_object(
           'id', id,
           'actor_id', actor_id,
           'work_item_id', work_item_id,
           'mailbox_sequence', mailbox_sequence,
           'gateway', gateway,
           'external_id', external_id,
           'kind', kind,
           'audience_kind', audience_kind,
           'audience_address', audience_address,
           'payload_json', payload_json,
           'state', state,
           'run_id', run_id,
           'created_at', created_at,
           'updated_at', updated_at
       ), updated_at
FROM events
WHERE state IN ('pending', 'processing', 'blocked');

INSERT INTO legacy_runtime_quarantine (entity_type, entity_id, prior_state, snapshot_json, quarantined_at)
SELECT 'tool_attempt', id, state,
       json_object(
           'id', id,
           'run_id', run_id,
           'tool_call_id', tool_call_id,
           'tool_name', tool_name,
           'arguments_json', arguments_json,
           'capabilities_json', capabilities_json,
           'state', state,
           'outcome_json', outcome_json,
           'observation_checkpointed', observation_checkpointed,
           'created_at', created_at,
           'updated_at', updated_at
       ), updated_at
FROM tool_attempts
WHERE state IN ('prepared', 'running', 'outcome_unknown', 'waiting_for_decision');

UPDATE tool_attempts SET state = 'cancelled_known', updated_at = max(updated_at, 1)
WHERE state = 'prepared';
UPDATE tool_attempts SET state = 'outcome_unknown', updated_at = max(updated_at, 1)
WHERE state = 'running';
UPDATE events SET state = 'failed_terminal', updated_at = max(updated_at, 1)
WHERE state IN ('pending', 'processing', 'blocked');
UPDATE runs SET state = 'failed_terminal', updated_at = max(updated_at, 1)
WHERE state = 'active';
UPDATE work_items
SET state = 'failed_terminal',
    failure_count = failure_count + 1,
    last_error = 'quarantined during schema v2 migration',
    updated_at = max(updated_at, 1)
WHERE state IN ('ready', 'waiting', 'blocked_unknown_outcome', 'waiting_for_decision');
DELETE FROM actor_leases;

CREATE INDEX claimable_result_bundles ON result_bundles(state, next_attempt_at, created_at);
CREATE INDEX local_requests_by_work_item ON local_requests(work_item_id, state);
CREATE INDEX artifacts_by_actor_state ON artifacts(actor_id, state, staging_expires_at);
