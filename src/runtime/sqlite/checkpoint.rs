use std::collections::HashSet;

use anyhow::{Result, anyhow, bail};
use async_trait::async_trait;
use tokio_rusqlite::params;

use crate::{
    agent::message::Message,
    runtime::{
        model::{Audience, EventId, Timestamp},
        sqlite::{SqliteRuntimeStore, dispatch::ensure_current_lease, map_call_error},
        store::{
            AttachedRun, CheckpointRun, CheckpointStore, FinalizeOutcome, FinalizeRun,
            NewOutboxIntent,
        },
    },
};

#[async_trait]
impl CheckpointStore for SqliteRuntimeStore {
    async fn checkpoint_run(&self, command: CheckpointRun, now: Timestamp) -> Result<()> {
        self.connection
            .call(move |connection| -> Result<()> {
                let transaction = connection.transaction_with_behavior(
                    tokio_rusqlite::rusqlite::TransactionBehavior::Immediate,
                )?;
                validate_run(&transaction, &command.run, now)?;
                incorporate_events(&transaction, &command.run, &command.incorporated_event_ids)?;
                checkpoint_attempts(
                    &transaction,
                    &command.run,
                    &command.checkpointed_attempt_ids,
                )?;
                insert_messages(&transaction, &command.run, &command.messages, now)?;
                transaction.commit()?;
                Ok(())
            })
            .await
            .map_err(map_call_error)
    }

    async fn finalize_run(&self, command: FinalizeRun, now: Timestamp) -> Result<FinalizeOutcome> {
        self.connection
            .call(move |connection| -> Result<FinalizeOutcome> {
                let transaction = connection.transaction_with_behavior(
                    tokio_rusqlite::rusqlite::TransactionBehavior::Immediate,
                )?;
                validate_run(&transaction, &command.run, now)?;

                if let Some(newest_sequence) = newest_compatible_input(
                    &transaction,
                    &command.run,
                    command.run.observed_sequence,
                )? {
                    return Ok(FinalizeOutcome::Preempted { newest_sequence });
                }

                require_all_events_incorporated(
                    &transaction,
                    &command.run,
                    &command.incorporated_event_ids,
                )?;
                insert_messages(&transaction, &command.run, &command.final_messages, now)?;
                for intent in &command.outbox {
                    insert_outbox(&transaction, &command.run, intent, now)?;
                }
                transaction.execute(
                    "UPDATE events
                     SET state = 'completed', updated_at = ?2
                     WHERE id IN (
                        SELECT event_id FROM run_events
                        WHERE run_id = ?1 AND incorporated = 1
                     )",
                    params![command.run.run_id.as_str(), now.0],
                )?;
                transaction.execute(
                    "UPDATE runs SET state = 'completed', updated_at = ?2
                     WHERE id = ?1 AND state = 'active'",
                    params![command.run.run_id.as_str(), now.0],
                )?;
                transaction.execute(
                    "UPDATE work_items SET state = 'completed', updated_at = ?2 WHERE id = ?1",
                    params![command.run.work_item_id.as_str(), now.0],
                )?;
                transaction.commit()?;
                Ok(FinalizeOutcome::Completed)
            })
            .await
            .map_err(map_call_error)
    }
}

fn validate_run(
    transaction: &tokio_rusqlite::rusqlite::Transaction<'_>,
    run: &AttachedRun,
    now: Timestamp,
) -> Result<()> {
    ensure_current_lease(transaction, &run.lease, now)?;
    let (actor_id, work_item_id, lease_generation, audience_kind, audience_address) = transaction
        .query_row(
            "SELECT runs.actor_id, runs.work_item_id, runs.lease_generation,
                    work_items.audience_kind, work_items.audience_address
             FROM runs
             JOIN work_items ON work_items.id = runs.work_item_id
             WHERE runs.id = ?1 AND runs.state = 'active'",
            [run.run_id.as_str()],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, Option<String>>(4)?,
                ))
            },
        )
        .map_err(|_| anyhow!("run is not active"))?;
    let expected_audience = encode_audience(&run.audience)?;
    if actor_id != run.lease.actor_id.as_str()
        || work_item_id != run.work_item_id.as_str()
        || lease_generation != run.lease.generation
        || (audience_kind, audience_address) != expected_audience
    {
        bail!("attached run does not match durable run state");
    }
    Ok(())
}

fn incorporate_events(
    transaction: &tokio_rusqlite::rusqlite::Transaction<'_>,
    run: &AttachedRun,
    event_ids: &[EventId],
) -> Result<()> {
    require_unique_ids(event_ids)?;
    for event_id in event_ids {
        let changed = transaction.execute(
            "UPDATE run_events SET incorporated = 1 WHERE run_id = ?1 AND event_id = ?2",
            params![run.run_id.as_str(), event_id.as_str()],
        )?;
        if changed != 1 {
            bail!("event is not attached to run: {event_id}");
        }
    }
    Ok(())
}

fn checkpoint_attempts(
    transaction: &tokio_rusqlite::rusqlite::Transaction<'_>,
    run: &AttachedRun,
    attempt_ids: &[crate::runtime::model::AttemptId],
) -> Result<()> {
    for attempt_id in attempt_ids {
        let changed = transaction.execute(
            "UPDATE tool_attempts SET observation_checkpointed = 1
             WHERE id = ?1 AND run_id = ?2",
            params![attempt_id.as_str(), run.run_id.as_str()],
        )?;
        if changed != 1 {
            bail!("attempt is not attached to run: {attempt_id}");
        }
    }
    Ok(())
}

fn insert_messages(
    transaction: &tokio_rusqlite::rusqlite::Transaction<'_>,
    run: &AttachedRun,
    messages: &[Message],
    now: Timestamp,
) -> Result<()> {
    let (audience_kind, audience_address) = encode_audience(&run.audience)?;
    for message in messages {
        let message_json = serde_json::to_string(message)?;
        transaction.execute(
            "INSERT INTO recent_messages(
                actor_id, work_item_id, run_id, audience_kind, audience_address,
                message_json, created_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                run.lease.actor_id.as_str(),
                run.work_item_id.as_str(),
                run.run_id.as_str(),
                audience_kind,
                audience_address,
                message_json,
                now.0,
            ],
        )?;
    }
    Ok(())
}

fn newest_compatible_input(
    transaction: &tokio_rusqlite::rusqlite::Transaction<'_>,
    run: &AttachedRun,
    observed_sequence: i64,
) -> Result<Option<i64>> {
    let (audience_kind, audience_address) = encode_audience(&run.audience)?;
    Ok(transaction.query_row(
        "SELECT MAX(mailbox_sequence) FROM events
         WHERE actor_id = ?1 AND state = 'pending'
           AND mailbox_sequence > ?2
           AND kind IN ('cancel_requested', 'user_message')
           AND audience_kind = ?3 AND audience_address IS ?4",
        params![
            run.lease.actor_id.as_str(),
            observed_sequence,
            audience_kind,
            audience_address,
        ],
        |row| row.get(0),
    )?)
}

fn require_all_events_incorporated(
    transaction: &tokio_rusqlite::rusqlite::Transaction<'_>,
    run: &AttachedRun,
    event_ids: &[EventId],
) -> Result<()> {
    require_unique_ids(event_ids)?;
    let durable = {
        let mut statement = transaction
            .prepare("SELECT event_id, incorporated FROM run_events WHERE run_id = ?1")?;
        statement
            .query_map([run.run_id.as_str()], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, bool>(1)?))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?
    };
    let requested = event_ids
        .iter()
        .map(|id| id.as_str())
        .collect::<HashSet<_>>();
    if durable.len() != requested.len()
        || durable
            .iter()
            .any(|(id, incorporated)| !incorporated || !requested.contains(id.as_str()))
    {
        bail!("all attached source events must be incorporated before finalization");
    }
    Ok(())
}

fn insert_outbox(
    transaction: &tokio_rusqlite::rusqlite::Transaction<'_>,
    run: &AttachedRun,
    intent: &NewOutboxIntent,
    now: Timestamp,
) -> Result<()> {
    if intent.audience != run.audience {
        bail!("outbox audience must match the finalized run");
    }
    let (audience_kind, audience_address) = encode_audience(&intent.audience)?;
    let payload_json = serde_json::to_string(&intent.payload)?;
    transaction.execute(
        "INSERT INTO outbox(
            id, intent_key, actor_id, work_item_id, run_id, intent_class,
            audience_kind, audience_address, payload_json, state, created_at, updated_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 'pending', ?10, ?10)
         ON CONFLICT(intent_key) DO NOTHING",
        params![
            intent.id.as_str(),
            intent.intent_key,
            run.lease.actor_id.as_str(),
            run.work_item_id.as_str(),
            run.run_id.as_str(),
            intent.intent_class,
            audience_kind,
            audience_address,
            payload_json,
            now.0,
        ],
    )?;
    Ok(())
}

fn require_unique_ids(event_ids: &[EventId]) -> Result<()> {
    let mut seen = HashSet::new();
    if event_ids.iter().any(|id| !seen.insert(id.as_str())) {
        bail!("event ids must be unique");
    }
    Ok(())
}

fn encode_audience(audience: &Audience) -> Result<(String, Option<String>)> {
    match audience {
        Audience::ActorPrivate => Ok(("actor_private".into(), None)),
        Audience::Shareable => Ok(("shareable".into(), None)),
        Audience::ConversationScoped { address } if !address.trim().is_empty() => {
            Ok(("conversation_scoped".into(), Some(address.clone())))
        }
        Audience::ConversationScoped { .. } => {
            bail!("conversation-scoped audience requires an address")
        }
    }
}

#[cfg(test)]
impl SqliteRuntimeStore {
    async fn source_events_completed(&self, run: &AttachedRun) -> Result<bool> {
        let run_id = run.run_id.to_string();
        self.connection
            .call(
                move |connection| -> tokio_rusqlite::rusqlite::Result<bool> {
                    connection.query_row(
                        "SELECT COUNT(*) = 0 FROM events
                     JOIN run_events ON run_events.event_id = events.id
                     WHERE run_events.run_id = ?1 AND events.state != 'completed'",
                        [run_id],
                        |row| row.get(0),
                    )
                },
            )
            .await
            .map_err(|error| anyhow!("failed to inspect source events: {error}"))
    }

    async fn pending_event_count(&self) -> Result<i64> {
        self.connection
            .call(|connection| {
                connection.query_row(
                    "SELECT COUNT(*) FROM events WHERE state = 'pending'",
                    [],
                    |row| row.get(0),
                )
            })
            .await
            .map_err(|error| anyhow!("failed to count pending events: {error}"))
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        agent::message::Message,
        auth::{LegacyActor, LegacyAuthorizationSnapshot, LegacyIdentity},
        llm::client::LlmToolCall,
        runtime::{
            model::{Audience, OutboxId, Timestamp},
            sqlite::SqliteRuntimeStore,
            store::{
                CheckpointRun, CheckpointStore, DispatchStore, FinalizeOutcome, FinalizeRun,
                IngressStore, NewInboundEvent, NewOutboxIntent, OutboxPayload, OutboxStore,
                RuntimeAuthorizationStore, StaleLease,
            },
        },
    };

    async fn store_with_run() -> (SqliteRuntimeStore, crate::runtime::store::AttachedRun) {
        let store = SqliteRuntimeStore::open_in_memory().await.unwrap();
        store
            .import_legacy_authorization(
                LegacyAuthorizationSnapshot {
                    version: 1,
                    actors: vec![LegacyActor {
                        id: "actor:telegram:123".into(),
                        enabled: true,
                        tools: vec!["*".into()],
                        identities: vec![LegacyIdentity {
                            provider: "telegram".into(),
                            subject: "123".into(),
                            username: None,
                        }],
                    }],
                },
                Timestamp(1),
            )
            .await
            .unwrap();
        store
            .ingest(
                NewInboundEvent::text(
                    "local",
                    "event-1",
                    "telegram",
                    "123",
                    Audience::ActorPrivate,
                    "hello",
                )
                .unwrap(),
                Timestamp(2),
            )
            .await
            .unwrap();
        let lease = store
            .acquire_ready_actor("worker", Timestamp(100), Timestamp(500))
            .await
            .unwrap()
            .unwrap();
        let run = store
            .attach_next_run(&lease, 8, Timestamp(101))
            .await
            .unwrap()
            .unwrap();
        (store, run)
    }

    #[tokio::test]
    async fn finalization_preempts_for_newer_compatible_input_then_completes_to_outbox() {
        let (store, run) = store_with_run().await;
        store
            .checkpoint_run(
                CheckpointRun {
                    run: run.clone(),
                    incorporated_event_ids: run.source_event_ids.clone(),
                    checkpointed_attempt_ids: Vec::new(),
                    messages: vec![
                        Message::user("hello"),
                        Message::assistant_tool_calls(
                            "thinking",
                            vec![LlmToolCall {
                                id: "call-1".into(),
                                name: "datetime".into(),
                                arguments: "{}".into(),
                            }],
                        ),
                    ],
                },
                Timestamp(150),
            )
            .await
            .unwrap();
        store
            .ingest(
                NewInboundEvent::text(
                    "local",
                    "event-2",
                    "telegram",
                    "123",
                    Audience::ActorPrivate,
                    "more context",
                )
                .unwrap(),
                Timestamp(151),
            )
            .await
            .unwrap();

        assert_eq!(
            store
                .finalize_run(finalize(&run, "intent-1"), Timestamp(200))
                .await
                .unwrap(),
            FinalizeOutcome::Preempted { newest_sequence: 2 }
        );
        assert!(store.pending_outbox().await.unwrap().is_empty());

        let resumed = store
            .attach_next_run(&run.lease, 8, Timestamp(201))
            .await
            .unwrap()
            .unwrap();
        store
            .ingest(
                NewInboundEvent::text(
                    "local",
                    "event-group",
                    "telegram",
                    "123",
                    Audience::ConversationScoped {
                        address: "telegram-group:7".into(),
                    },
                    "unrelated group input",
                )
                .unwrap(),
                Timestamp(201),
            )
            .await
            .unwrap();
        store
            .checkpoint_run(
                CheckpointRun {
                    run: resumed.clone(),
                    incorporated_event_ids: resumed.source_event_ids.clone(),
                    checkpointed_attempt_ids: Vec::new(),
                    messages: Vec::new(),
                },
                Timestamp(202),
            )
            .await
            .unwrap();
        let mut command = finalize(&resumed, "intent-1");
        let mut duplicate = command.outbox[0].clone();
        duplicate.id = OutboxId::new();
        command.outbox.push(duplicate);
        assert_eq!(
            store.finalize_run(command, Timestamp(203)).await.unwrap(),
            FinalizeOutcome::Completed
        );
        let outbox = store.pending_outbox().await.unwrap();
        assert_eq!(outbox.len(), 1);
        assert_eq!(outbox[0].intent_key, "intent-1");
        assert!(store.source_events_completed(&resumed).await.unwrap());
        assert_eq!(store.pending_event_count().await.unwrap(), 1);
    }

    #[tokio::test]
    async fn stale_lease_finalization_leaves_rows_unchanged() {
        let (store, run) = store_with_run().await;
        let replacement = store
            .acquire_ready_actor("replacement", Timestamp(501), Timestamp(600))
            .await
            .unwrap()
            .unwrap();

        let error = store
            .finalize_run(finalize(&run, "stale-intent"), Timestamp(502))
            .await
            .unwrap_err();

        assert!(error.downcast_ref::<StaleLease>().is_some());
        assert!(store.pending_outbox().await.unwrap().is_empty());
        assert!(!store.source_events_completed(&run).await.unwrap());
        store.release_lease(&replacement).await.unwrap();
    }

    fn finalize(run: &crate::runtime::store::AttachedRun, intent_key: &str) -> FinalizeRun {
        FinalizeRun {
            run: run.clone(),
            incorporated_event_ids: run.source_event_ids.clone(),
            final_messages: vec![Message::assistant("done")],
            outbox: vec![NewOutboxIntent {
                id: OutboxId::new(),
                intent_key: intent_key.into(),
                intent_class: "reply".into(),
                audience: run.audience.clone(),
                payload: OutboxPayload::Text {
                    text: "done".into(),
                },
            }],
        }
    }
}
