use anyhow::{Result, anyhow, bail};
use async_trait::async_trait;
use tokio_rusqlite::params;

use crate::runtime::{
    model::{OutboxId, OutboxState, Timestamp},
    sqlite::{SqliteRuntimeStore, map_call_error},
    store::{OutboxRecord, OutboxStore},
};

#[async_trait]
impl OutboxStore for SqliteRuntimeStore {
    async fn pending_outbox(&self) -> Result<Vec<OutboxRecord>> {
        self.connection
            .call(|connection| -> Result<Vec<OutboxRecord>> {
                let mut statement = connection.prepare(
                    "SELECT id, intent_key, payload_json, state, attempt_count
                     FROM outbox WHERE state = 'pending' ORDER BY created_at, id",
                )?;
                statement
                    .query_map([], |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, String>(3)?,
                            row.get::<_, i64>(4)?,
                        ))
                    })?
                    .map(|row| {
                        let (id, intent_key, payload_json, state, attempt_count) = row?;
                        Ok(OutboxRecord {
                            id: OutboxId::from_string(id),
                            intent_key,
                            payload: serde_json::from_str(&payload_json)?,
                            state: decode_state(&state)?,
                            attempt_count,
                        })
                    })
                    .collect()
            })
            .await
            .map_err(map_call_error)
    }

    async fn mark_outbox_delivered(&self, id: &OutboxId, now: Timestamp) -> Result<()> {
        transition(self, id, "delivered", now).await
    }

    async fn mark_outbox_failed_terminal(
        &self,
        id: &OutboxId,
        _error: &str,
        now: Timestamp,
    ) -> Result<()> {
        transition(self, id, "failed_terminal", now).await
    }
}

async fn transition(
    store: &SqliteRuntimeStore,
    id: &OutboxId,
    next_state: &'static str,
    now: Timestamp,
) -> Result<()> {
    let id = id.to_string();
    store
        .connection
        .call(move |connection| -> Result<()> {
            let changed = connection.execute(
                "UPDATE outbox SET state = ?2, attempt_count = attempt_count + 1,
                    claim_owner = NULL, claim_expires_at = NULL, updated_at = ?3
                 WHERE id = ?1 AND state = 'pending'",
                params![id, next_state, now.0],
            )?;
            if changed != 1 {
                bail!("outbox record is not pending");
            }
            Ok(())
        })
        .await
        .map_err(map_call_error)
}

fn decode_state(state: &str) -> Result<OutboxState> {
    match state {
        "pending" => Ok(OutboxState::Pending),
        "delivering" => Ok(OutboxState::Delivering),
        "delivered" => Ok(OutboxState::Delivered),
        "failed_retryable" => Ok(OutboxState::FailedRetryable),
        "failed_terminal" => Ok(OutboxState::FailedTerminal),
        "outcome_unknown" => Ok(OutboxState::OutcomeUnknown),
        "acknowledged_duplicate" => Ok(OutboxState::AcknowledgedDuplicate),
        _ => Err(anyhow!("invalid outbox state: {state}")),
    }
}

#[cfg(test)]
impl SqliteRuntimeStore {
    async fn outbox_state(&self, id: &OutboxId) -> Result<(String, i64)> {
        let id = id.to_string();
        self.connection
            .call(move |connection| {
                connection.query_row(
                    "SELECT state, attempt_count FROM outbox WHERE id = ?1",
                    [id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
            })
            .await
            .map_err(|error| anyhow!("failed to inspect outbox state: {error}"))
    }

    async fn seed_pending_outbox(&self, id: &OutboxId) -> Result<()> {
        let id = id.to_string();
        self.connection
            .call(move |connection| -> tokio_rusqlite::rusqlite::Result<()> {
                let transaction = connection.transaction()?;
                transaction.execute(
                    "INSERT INTO actors(id, enabled, tools_json, created_at)
                     VALUES ('actor-1', 1, '[]', 1)",
                    [],
                )?;
                transaction.execute(
                    "INSERT INTO work_items(
                        id, actor_id, kind, audience_kind, state, created_at, updated_at
                     ) VALUES ('work-1', 'actor-1', 'interactive', 'actor_private', 'ready', 1, 1)",
                    [],
                )?;
                transaction.execute(
                    "INSERT INTO runs(
                        id, actor_id, work_item_id, state, lease_generation,
                        observed_sequence, created_at, updated_at
                     ) VALUES ('run-1', 'actor-1', 'work-1', 'active', 1, 0, 1, 1)",
                    [],
                )?;
                transaction.execute(
                    "INSERT INTO outbox(
                        id, intent_key, actor_id, work_item_id, run_id, intent_class,
                        audience_kind, payload_json, state, created_at, updated_at
                     ) VALUES (?1, 'intent-1', 'actor-1', 'work-1', 'run-1', 'reply',
                        'actor_private', '{\"type\":\"text\",\"text\":\"hello\"}', 'pending', 1, 1)",
                    [id],
                )?;
                transaction.commit()
            })
            .await
            .map_err(|error| anyhow!("failed to seed outbox: {error}"))
    }
}

#[cfg(test)]
mod tests {
    use crate::runtime::{
        model::{OutboxId, OutboxState, Timestamp},
        sqlite::SqliteRuntimeStore,
        store::OutboxStore,
    };

    #[tokio::test]
    async fn delivery_transition_requires_pending_and_increments_attempt_count() {
        let store = SqliteRuntimeStore::open_in_memory().await.unwrap();
        let id = OutboxId::from_string("outbox-1");
        store.seed_pending_outbox(&id).await.unwrap();

        let pending = store.pending_outbox().await.unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].state, OutboxState::Pending);
        assert_eq!(pending[0].attempt_count, 0);

        store
            .mark_outbox_delivered(&id, Timestamp(10))
            .await
            .unwrap();
        assert_eq!(
            store.outbox_state(&id).await.unwrap(),
            ("delivered".into(), 1)
        );
        assert!(
            store
                .mark_outbox_delivered(&id, Timestamp(11))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn terminal_failure_transition_increments_attempt_count() {
        let store = SqliteRuntimeStore::open_in_memory().await.unwrap();
        let id = OutboxId::from_string("outbox-1");
        store.seed_pending_outbox(&id).await.unwrap();

        store
            .mark_outbox_failed_terminal(&id, "recipient blocked bot", Timestamp(10))
            .await
            .unwrap();

        assert_eq!(
            store.outbox_state(&id).await.unwrap(),
            ("failed_terminal".into(), 1)
        );
    }
}
