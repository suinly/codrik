ALTER TABLE gateway_deliveries
ADD COLUMN transport_retry_safe INTEGER NOT NULL DEFAULT 0
CHECK(transport_retry_safe IN (0, 1));

ALTER TABLE gateway_streams RENAME TO gateway_streams_v4;

CREATE TABLE gateway_streams (
    work_item_id TEXT NOT NULL REFERENCES work_items(id) ON DELETE CASCADE,
    gateway TEXT NOT NULL,
    address TEXT NOT NULL,
    remote_message_id TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('active', 'closed', 'finalizing')),
    updated_at INTEGER NOT NULL,
    PRIMARY KEY(work_item_id, gateway, address)
) STRICT;

INSERT INTO gateway_streams(
    work_item_id, gateway, address, remote_message_id, state, updated_at
)
SELECT work_item_id, gateway, address, remote_message_id, state, updated_at
FROM gateway_streams_v4;

DROP TABLE gateway_streams_v4;
