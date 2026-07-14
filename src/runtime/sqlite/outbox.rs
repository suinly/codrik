#[cfg(test)]
use anyhow::{Result, anyhow};

#[cfg(test)]
use crate::runtime::{model::OutboxId, sqlite::SqliteRuntimeStore, store::OutboxPayload};

#[cfg(test)]
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct StoredOutboxIntent {
    pub(crate) id: OutboxId,
    pub(crate) intent_key: String,
    pub(crate) payload: OutboxPayload,
}

#[cfg(test)]
impl SqliteRuntimeStore {
    pub(crate) async fn outbox_intents(&self) -> Result<Vec<StoredOutboxIntent>> {
        self.connection
            .call(|connection| -> Result<Vec<StoredOutboxIntent>> {
                let mut statement = connection.prepare(
                    "SELECT id, intent_key, payload_json FROM outbox ORDER BY created_at, id",
                )?;
                statement
                    .query_map([], |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                        ))
                    })?
                    .map(|row| {
                        let (id, intent_key, payload_json) = row?;
                        Ok(StoredOutboxIntent {
                            id: OutboxId::from_string(id),
                            intent_key,
                            payload: serde_json::from_str(&payload_json)?,
                        })
                    })
                    .collect()
            })
            .await
            .map_err(|error| anyhow!("failed to inspect outbox intents: {error}"))
    }
}

#[cfg(test)]
mod tests {
    use crate::runtime::{model::OutboxId, sqlite::SqliteRuntimeStore, store::OutboxPayload};

    #[tokio::test]
    async fn test_probe_reads_immutable_intent_fields() {
        let store = SqliteRuntimeStore::open_in_memory().await.unwrap();
        store
            .connection
            .call(|connection| {
                connection.execute_batch(
                    "INSERT INTO actors(id, enabled, tools_json, created_at)
                     VALUES ('actor-1', 1, '[]', 1);
                     INSERT INTO work_items(id, actor_id, kind, audience_kind, state, created_at, updated_at)
                     VALUES ('work-1', 'actor-1', 'interactive', 'actor_private', 'completed', 1, 1);
                     INSERT INTO runs(id, actor_id, work_item_id, state, lease_generation,
                        observed_sequence, created_at, updated_at)
                     VALUES ('run-1', 'actor-1', 'work-1', 'completed', 1, 0, 1, 1);
                     INSERT INTO outbox(id, intent_key, actor_id, work_item_id, run_id, intent_class,
                        audience_kind, payload_json, created_at)
                     VALUES ('outbox-1', 'intent-1', 'actor-1', 'work-1', 'run-1', 'reply',
                        'actor_private', '{\"type\":\"text\",\"text\":\"hello\"}', 1);",
                )
            })
            .await
            .unwrap();

        let intents = store.outbox_intents().await.unwrap();
        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].id, OutboxId::from_string("outbox-1"));
        assert_eq!(intents[0].intent_key, "intent-1");
        assert_eq!(
            intents[0].payload,
            OutboxPayload::Text {
                text: "hello".into()
            }
        );
    }
}
