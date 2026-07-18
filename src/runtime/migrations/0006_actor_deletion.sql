CREATE TABLE actor_deletions (
    actor_id TEXT PRIMARY KEY REFERENCES actors(id) ON DELETE CASCADE,
    requested_at INTEGER NOT NULL
) STRICT;

DROP TRIGGER outbox_deliveries_are_immutable_on_delete;
CREATE TRIGGER outbox_deliveries_are_immutable_on_delete
BEFORE DELETE ON outbox_deliveries
WHEN NOT EXISTS (
    SELECT 1
    FROM outbox
    JOIN actor_deletions ON actor_deletions.actor_id = outbox.actor_id
    WHERE outbox.id = OLD.outbox_id
)
BEGIN
    SELECT RAISE(ABORT, 'outbox_deliveries are append-only');
END;
