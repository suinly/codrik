use anyhow::{Result, anyhow, bail};
use async_trait::async_trait;
use serde_json::json;
use tokio_rusqlite::{params, rusqlite::OptionalExtension};

use crate::runtime::{
    model::{
        ActorId, BundleId, CancelId, EventId, LocalRequestState, RequestId, Timestamp, WorkItemId,
    },
    sqlite::{SqliteRuntimeStore, map_call_error},
    store::{
        CancelOutcome, LocalCancel, LocalIngressStore, LocalRequestRecord, LocalSubmission,
        LocalSubmitOutcome, RuntimeActor,
    },
};

const LOCAL_SUBMIT_GATEWAY: &str = "local:submit";
const LOCAL_CANCEL_GATEWAY: &str = "local:cancel";

#[async_trait]
impl LocalIngressStore for SqliteRuntimeStore {
    async fn submit_for_actor(
        &self,
        actor: &ActorId,
        command: LocalSubmission,
        now: Timestamp,
    ) -> Result<LocalSubmitOutcome> {
        let actor = actor.clone();
        self.connection
            .call(move |connection| -> Result<LocalSubmitOutcome> {
                let transaction = connection.transaction_with_behavior(
                    tokio_rusqlite::rusqlite::TransactionBehavior::Immediate,
                )?;
                if !actor_is_enabled(&transaction, &actor)? {
                    return Ok(LocalSubmitOutcome::ActorUnavailable);
                }

                if let Some((stored_actor, prompt_sha256, event_id, work_item_id, sequence)) =
                    transaction
                        .query_row(
                            "SELECT local_requests.actor_id, local_requests.prompt_sha256,
                                    events.id, local_requests.work_item_id, events.mailbox_sequence
                             FROM local_requests
                             JOIN events ON events.id = local_requests.event_id
                             WHERE local_requests.request_id = ?1",
                            [command.request_id.as_str()],
                            |row| {
                                Ok((
                                    row.get::<_, String>(0)?,
                                    row.get::<_, String>(1)?,
                                    row.get::<_, String>(2)?,
                                    row.get::<_, String>(3)?,
                                    row.get::<_, i64>(4)?,
                                ))
                            },
                        )
                        .optional()?
                {
                    if stored_actor == actor.as_str() && prompt_sha256 == command.prompt_sha256 {
                        return Ok(LocalSubmitOutcome::Duplicate {
                            event_id: EventId::from_string(event_id),
                            work_item_id: WorkItemId::from_string(work_item_id),
                            sequence,
                        });
                    }
                    return Ok(LocalSubmitOutcome::Conflict);
                }

                let work_item_id = transaction
                    .query_row(
                        "SELECT id FROM work_items
                         WHERE actor_id = ?1 AND kind = 'interactive'
                           AND audience_kind = 'actor_private'
                           AND audience_address IS NULL
                           AND state IN ('ready', 'waiting')
                           AND cancellation_requested_at IS NULL
                         ORDER BY updated_at DESC, id ASC
                         LIMIT 1",
                        [actor.as_str()],
                        |row| row.get::<_, String>(0),
                    )
                    .optional()?
                    .unwrap_or_else(|| WorkItemId::new().to_string());
                transaction.execute(
                    "INSERT INTO work_items(
                        id, actor_id, kind, audience_kind, audience_address, state, created_at, updated_at
                     ) VALUES (?1, ?2, 'interactive', 'actor_private', NULL, 'ready', ?3, ?3)
                     ON CONFLICT(id) DO UPDATE SET
                        state = CASE WHEN work_items.state = 'waiting' THEN 'ready' ELSE work_items.state END,
                        updated_at = excluded.updated_at",
                    params![work_item_id, actor.as_str(), now.0],
                )?;

                let sequence = next_sequence(&transaction, &actor)?;
                let event_id = EventId::new();
                let payload_json = serde_json::to_string(&json!({
                    "type": "text",
                    "text": command.text,
                }))?;
                transaction.execute(
                    "INSERT INTO events(
                        id, actor_id, work_item_id, mailbox_sequence, gateway, external_id,
                        kind, audience_kind, audience_address, payload_json, state, created_at, updated_at
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'user_message',
                               'actor_private', NULL, ?7, 'pending', ?8, ?8)",
                    params![
                        event_id.as_str(),
                        actor.as_str(),
                        work_item_id,
                        sequence,
                        LOCAL_SUBMIT_GATEWAY,
                        command.request_id.as_str(),
                        payload_json,
                        now.0,
                    ],
                )?;
                transaction.execute(
                    "INSERT INTO local_requests(
                        request_id, actor_id, event_id, work_item_id, prompt_sha256,
                        state, result_bundle_id, created_at, updated_at
                     ) VALUES (?1, ?2, ?3, ?4, ?5, 'active', NULL, ?6, ?6)",
                    params![
                        command.request_id.as_str(),
                        actor.as_str(),
                        event_id.as_str(),
                        work_item_id,
                        command.prompt_sha256,
                        now.0,
                    ],
                )?;
                transaction.commit()?;
                Ok(LocalSubmitOutcome::Accepted {
                    event_id,
                    work_item_id: WorkItemId::from_string(work_item_id),
                    sequence,
                })
            })
            .await
            .map_err(map_call_error)
    }

    async fn cancel_for_actor(
        &self,
        actor: &ActorId,
        command: LocalCancel,
        now: Timestamp,
    ) -> Result<CancelOutcome> {
        let actor = actor.clone();
        self.connection
            .call(move |connection| -> Result<CancelOutcome> {
                let transaction = connection.transaction_with_behavior(
                    tokio_rusqlite::rusqlite::TransactionBehavior::Immediate,
                )?;
                if !actor_is_enabled(&transaction, &actor)? {
                    bail!("local actor is unavailable");
                }

                if let Some((stored_actor, payload_json)) = transaction
                    .query_row(
                        "SELECT actor_id, payload_json FROM events
                         WHERE gateway = ?1 AND external_id = ?2",
                        params![LOCAL_CANCEL_GATEWAY, command.cancel_id.as_str()],
                        |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
                    )
                    .optional()?
                {
                    if stored_actor != actor.as_str() {
                        bail!("cancel id belongs to another actor");
                    }
                    let payload: StoredCancelPayload = serde_json::from_str(&payload_json)?;
                    if payload.request_id != command.request_id {
                        bail!("cancel id was already used for another request");
                    }
                    let affected_request_ids = load_cancel_targets(&transaction, &command.cancel_id)?;
                    return Ok(CancelOutcome {
                        cancel_id: command.cancel_id,
                        affected_request_ids,
                        already_terminal: payload.already_terminal,
                    });
                }

                let (work_item_id, state) = transaction
                    .query_row(
                        "SELECT work_item_id, state FROM local_requests
                         WHERE request_id = ?1 AND actor_id = ?2",
                        params![command.request_id.as_str(), actor.as_str()],
                        |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
                    )
                    .optional()?
                    .ok_or_else(|| anyhow!("local request was not found for actor"))?;
                let already_terminal = state != "active";
                let affected_request_ids = if already_terminal {
                    Vec::new()
                } else {
                    transaction.execute(
                        "UPDATE work_items
                         SET cancellation_requested_at = COALESCE(cancellation_requested_at, ?2),
                             updated_at = max(updated_at, ?2)
                         WHERE id = ?1",
                        params![work_item_id, now.0],
                    )?;
                    let mut statement = transaction.prepare(
                        "SELECT request_id FROM local_requests
                         WHERE actor_id = ?1 AND work_item_id = ?2 AND state = 'active'
                         ORDER BY created_at, request_id",
                    )?;
                    statement
                        .query_map(params![actor.as_str(), work_item_id], |row| {
                            row.get::<_, String>(0)
                        })?
                        .map(|row| RequestId::parse(&row?).map_err(anyhow::Error::from))
                        .collect::<Result<Vec<_>>>()?
                };

                let sequence = next_sequence(&transaction, &actor)?;
                let event_id = EventId::new();
                let payload_json = serde_json::to_string(&StoredCancelPayload {
                    request_id: command.request_id.clone(),
                    already_terminal,
                })?;
                transaction.execute(
                    "INSERT INTO events(
                        id, actor_id, work_item_id, mailbox_sequence, gateway, external_id,
                        kind, audience_kind, audience_address, payload_json, state, created_at, updated_at
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'cancel_requested',
                               'actor_private', NULL, ?7, ?8, ?9, ?9)",
                    params![
                        event_id.as_str(),
                        actor.as_str(),
                        work_item_id,
                        sequence,
                        LOCAL_CANCEL_GATEWAY,
                        command.cancel_id.as_str(),
                        payload_json,
                        if already_terminal { "completed" } else { "pending" },
                        now.0,
                    ],
                )?;
                for request_id in &affected_request_ids {
                    transaction.execute(
                        "INSERT INTO cancel_targets(cancel_id, request_id, created_at)
                         VALUES (?1, ?2, ?3)",
                        params![command.cancel_id.as_str(), request_id.as_str(), now.0],
                    )?;
                }
                transaction.commit()?;
                Ok(CancelOutcome {
                    cancel_id: command.cancel_id,
                    affected_request_ids,
                    already_terminal,
                })
            })
            .await
            .map_err(map_call_error)
    }

    async fn resolve_local_request(&self, id: &RequestId) -> Result<Option<LocalRequestRecord>> {
        let id = id.clone();
        self.connection
            .call(move |connection| -> Result<Option<LocalRequestRecord>> {
                connection
                    .query_row(
                        "SELECT request_id, actor_id, work_item_id, state, result_bundle_id
                         FROM local_requests WHERE request_id = ?1",
                        [id.as_str()],
                        |row| {
                            Ok((
                                row.get::<_, String>(0)?,
                                row.get::<_, String>(1)?,
                                row.get::<_, String>(2)?,
                                row.get::<_, String>(3)?,
                                row.get::<_, Option<String>>(4)?,
                            ))
                        },
                    )
                    .optional()?
                    .map(|(request_id, actor_id, work_item_id, state, bundle_id)| {
                        Ok(LocalRequestRecord {
                            request_id: RequestId::parse(&request_id)?,
                            actor_id: ActorId::from_string(actor_id),
                            work_item_id: WorkItemId::from_string(work_item_id),
                            state: decode_local_request_state(&state)?,
                            result_bundle_id: bundle_id
                                .map(|id| BundleId::parse(&id))
                                .transpose()?,
                        })
                    })
                    .transpose()
            })
            .await
            .map_err(map_call_error)
    }

    async fn load_actor(&self, id: &ActorId) -> Result<Option<RuntimeActor>> {
        let id = id.clone();
        self.connection
            .call(move |connection| -> Result<Option<RuntimeActor>> {
                let actor = connection
                    .query_row(
                        "SELECT enabled, tools_json FROM actors WHERE id = ?1",
                        [id.as_str()],
                        |row| Ok((row.get::<_, bool>(0)?, row.get::<_, String>(1)?)),
                    )
                    .optional()?;
                actor
                    .map(|(enabled, tools_json)| {
                        Ok(RuntimeActor {
                            id,
                            enabled,
                            tools: serde_json::from_str(&tools_json)?,
                        })
                    })
                    .transpose()
            })
            .await
            .map_err(map_call_error)
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct StoredCancelPayload {
    request_id: RequestId,
    already_terminal: bool,
}

fn actor_is_enabled(
    transaction: &tokio_rusqlite::rusqlite::Transaction<'_>,
    actor: &ActorId,
) -> tokio_rusqlite::rusqlite::Result<bool> {
    Ok(transaction
        .query_row(
            "SELECT enabled FROM actors WHERE id = ?1",
            [actor.as_str()],
            |row| row.get::<_, bool>(0),
        )
        .optional()?
        .unwrap_or(false))
}

fn next_sequence(
    transaction: &tokio_rusqlite::rusqlite::Transaction<'_>,
    actor: &ActorId,
) -> tokio_rusqlite::rusqlite::Result<i64> {
    transaction.query_row(
        "UPDATE actors SET next_mailbox_sequence = next_mailbox_sequence + 1
         WHERE id = ?1 RETURNING next_mailbox_sequence",
        [actor.as_str()],
        |row| row.get(0),
    )
}

fn load_cancel_targets(
    transaction: &tokio_rusqlite::rusqlite::Transaction<'_>,
    cancel_id: &CancelId,
) -> Result<Vec<RequestId>> {
    let mut statement = transaction.prepare(
        "SELECT request_id FROM cancel_targets WHERE cancel_id = ?1 ORDER BY created_at, request_id",
    )?;
    statement
        .query_map([cancel_id.as_str()], |row| row.get::<_, String>(0))?
        .map(|row| RequestId::parse(&row?).map_err(anyhow::Error::from))
        .collect()
}

fn decode_local_request_state(value: &str) -> Result<LocalRequestState> {
    match value {
        "active" => Ok(LocalRequestState::Active),
        "completed" => Ok(LocalRequestState::Completed),
        "cancelled" => Ok(LocalRequestState::Cancelled),
        "failed_terminal" => Ok(LocalRequestState::FailedTerminal),
        other => bail!("unknown local request state: {other}"),
    }
}

#[cfg(test)]
impl SqliteRuntimeStore {
    async fn gateway_counts(&self) -> Result<(i64, i64)> {
        self.connection
            .call(
                |connection| -> tokio_rusqlite::rusqlite::Result<(i64, i64)> {
                    Ok((
                        connection.query_row(
                            "SELECT count(*) FROM events WHERE gateway = 'local:submit'",
                            [],
                            |row| row.get(0),
                        )?,
                        connection.query_row(
                            "SELECT count(*) FROM events WHERE gateway = 'local:cancel'",
                            [],
                            |row| row.get(0),
                        )?,
                    ))
                },
            )
            .await
            .map_err(|error| anyhow!("failed to count local gateway events: {error}"))
    }

    async fn mark_local_request_terminal_for_test(
        &self,
        request_id: &RequestId,
        bundle_id: &BundleId,
        now: Timestamp,
    ) -> Result<()> {
        let request_id = request_id.clone();
        let bundle_id = bundle_id.clone();
        self.connection
            .call(move |connection| -> Result<()> {
                let transaction = connection.transaction_with_behavior(
                    tokio_rusqlite::rusqlite::TransactionBehavior::Immediate,
                )?;
                transaction.execute(
                    "INSERT INTO result_bundles(
                        id, request_id, delivery_count, manifest_sha256, state,
                        attempt_count, created_at, updated_at
                     ) VALUES (?1, ?2, 1, ?3, 'delivered', 0, ?4, ?4)",
                    params![
                        bundle_id.as_str(),
                        request_id.as_str(),
                        "f".repeat(64),
                        now.0,
                    ],
                )?;
                transaction.execute(
                    "UPDATE local_requests
                     SET state = 'completed', result_bundle_id = ?2, updated_at = ?3
                     WHERE request_id = ?1",
                    params![request_id.as_str(), bundle_id.as_str(), now.0],
                )?;
                transaction.commit()?;
                Ok(())
            })
            .await
            .map_err(map_call_error)
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;

    use crate::{
        auth::{LegacyActor, LegacyAuthorizationSnapshot},
        runtime::{
            model::{ActorId, BundleId, CancelId, LocalRequestState, RequestId, Timestamp},
            sqlite::SqliteRuntimeStore,
            store::{
                LocalCancel, LocalIngressStore, LocalSubmission, LocalSubmitOutcome,
                RuntimeAuthorizationStore,
            },
        },
    };

    async fn store_with_actor(enabled: bool) -> Result<(SqliteRuntimeStore, ActorId)> {
        let store = SqliteRuntimeStore::open_in_memory().await?;
        let actor = ActorId::from_string("actor:local:owner");
        store
            .import_legacy_authorization(
                LegacyAuthorizationSnapshot {
                    version: 1,
                    actors: vec![LegacyActor {
                        id: actor.to_string(),
                        enabled,
                        tools: vec!["*".into()],
                        identities: vec![],
                    }],
                },
                Timestamp(0),
            )
            .await?;
        Ok((store, actor))
    }

    fn submission(request_id: RequestId, text: &str, hash_byte: char) -> LocalSubmission {
        LocalSubmission {
            request_id,
            text: text.into(),
            prompt_sha256: std::iter::repeat_n(hash_byte, 64).collect(),
        }
    }

    fn cancel(cancel_id: &str, request_id: RequestId) -> Result<LocalCancel> {
        Ok(LocalCancel {
            cancel_id: CancelId::parse(cancel_id)?,
            request_id,
        })
    }

    fn accepted(outcome: LocalSubmitOutcome) -> (String, String, i64) {
        match outcome {
            LocalSubmitOutcome::Accepted {
                event_id,
                work_item_id,
                sequence,
            } => (event_id.to_string(), work_item_id.to_string(), sequence),
            other => panic!("expected accepted outcome, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn same_request_and_hash_is_idempotent_but_different_hash_conflicts() -> Result<()> {
        let (store, actor) = store_with_actor(true).await?;
        let request = RequestId::parse("0190f2ef-0000-7000-8000-000000000001")?;
        let (event_id, work_item_id, sequence) = accepted(
            store
                .submit_for_actor(
                    &actor,
                    submission(request.clone(), "first", 'a'),
                    Timestamp(1),
                )
                .await?,
        );

        assert_eq!(
            store
                .submit_for_actor(
                    &actor,
                    submission(request.clone(), "first", 'a'),
                    Timestamp(2),
                )
                .await?,
            LocalSubmitOutcome::Duplicate {
                event_id: crate::runtime::model::EventId::from_string(event_id),
                work_item_id: crate::runtime::model::WorkItemId::from_string(work_item_id),
                sequence,
            }
        );
        assert_eq!(
            store
                .submit_for_actor(&actor, submission(request, "changed", 'b'), Timestamp(3))
                .await?,
            LocalSubmitOutcome::Conflict
        );
        Ok(())
    }

    #[tokio::test]
    async fn trusted_ingress_uses_actor_directly_and_rejects_disabled_actor() -> Result<()> {
        let (enabled_store, enabled_actor) = store_with_actor(true).await?;
        assert!(matches!(
            enabled_store
                .submit_for_actor(
                    &enabled_actor,
                    submission(RequestId::new(), "direct", 'c'),
                    Timestamp(1),
                )
                .await?,
            LocalSubmitOutcome::Accepted { .. }
        ));

        let (disabled_store, disabled_actor) = store_with_actor(false).await?;
        assert_eq!(
            disabled_store
                .submit_for_actor(
                    &disabled_actor,
                    submission(RequestId::new(), "blocked", 'd'),
                    Timestamp(1),
                )
                .await?,
            LocalSubmitOutcome::ActorUnavailable
        );
        assert!(disabled_store.load_actor(&disabled_actor).await?.is_some());
        Ok(())
    }

    #[tokio::test]
    async fn submit_and_cancel_ids_use_distinct_gateway_namespaces() -> Result<()> {
        let (store, actor) = store_with_actor(true).await?;
        let shared = "0190f2ef-0000-7000-8000-000000000011";
        let request = RequestId::parse(shared)?;
        store
            .submit_for_actor(
                &actor,
                submission(request.clone(), "first", 'e'),
                Timestamp(1),
            )
            .await?;
        let outcome = store
            .cancel_for_actor(&actor, cancel(shared, request.clone())?, Timestamp(2))
            .await?;

        assert_eq!(outcome.affected_request_ids, vec![request]);
        assert_eq!(store.gateway_counts().await?, (1, 1));
        Ok(())
    }

    #[tokio::test]
    async fn cancellation_snapshot_is_immutable_across_retries() -> Result<()> {
        let (store, actor) = store_with_actor(true).await?;
        let first = RequestId::parse("0190f2ef-0000-7000-8000-000000000021")?;
        let second = RequestId::parse("0190f2ef-0000-7000-8000-000000000022")?;
        for (request, text, hash) in [
            (first.clone(), "first", 'f'),
            (second.clone(), "second", '0'),
        ] {
            store
                .submit_for_actor(&actor, submission(request, text, hash), Timestamp(1))
                .await?;
        }
        let command = cancel("0190f2ef-0000-7000-8000-000000000023", first.clone())?;
        let original = store
            .cancel_for_actor(&actor, command.clone(), Timestamp(2))
            .await?;
        let retried = store
            .cancel_for_actor(&actor, command, Timestamp(3))
            .await?;

        assert_eq!(original.affected_request_ids, vec![first, second]);
        assert_eq!(retried, original);
        Ok(())
    }

    #[tokio::test]
    async fn cancel_freezes_targets_and_new_submit_uses_new_work_item() -> Result<()> {
        let (store, actor) = store_with_actor(true).await?;
        let request = RequestId::parse("0190f2ef-0000-7000-8000-000000000031")?;
        let (_, first_work_item, _) = accepted(
            store
                .submit_for_actor(
                    &actor,
                    submission(request.clone(), "first", '1'),
                    Timestamp(1),
                )
                .await?,
        );
        let cancelled = store
            .cancel_for_actor(
                &actor,
                cancel("0190f2ef-0000-7000-8000-000000000032", request.clone())?,
                Timestamp(2),
            )
            .await?;
        assert_eq!(cancelled.affected_request_ids, vec![request]);
        let (_, second_work_item, _) = accepted(
            store
                .submit_for_actor(
                    &actor,
                    submission(RequestId::new(), "second", '2'),
                    Timestamp(3),
                )
                .await?,
        );

        assert_ne!(first_work_item, second_work_item);
        Ok(())
    }

    #[tokio::test]
    async fn cancelling_terminal_request_is_idempotent_noop() -> Result<()> {
        let (store, actor) = store_with_actor(true).await?;
        let request = RequestId::parse("0190f2ef-0000-7000-8000-000000000041")?;
        store
            .submit_for_actor(
                &actor,
                submission(request.clone(), "done", '3'),
                Timestamp(1),
            )
            .await?;
        store
            .mark_local_request_terminal_for_test(
                &request,
                &BundleId::parse("0190f2ef-0000-7000-8000-000000000042")?,
                Timestamp(2),
            )
            .await?;
        let command = cancel("0190f2ef-0000-7000-8000-000000000043", request.clone())?;

        let first = store
            .cancel_for_actor(&actor, command.clone(), Timestamp(3))
            .await?;
        let second = store
            .cancel_for_actor(&actor, command, Timestamp(4))
            .await?;
        assert!(first.already_terminal);
        assert!(first.affected_request_ids.is_empty());
        assert_eq!(second, first);
        assert_eq!(
            store.resolve_local_request(&request).await?.unwrap().state,
            LocalRequestState::Completed
        );
        Ok(())
    }

    #[tokio::test]
    async fn cancel_retry_cannot_cross_actor_boundary() -> Result<()> {
        let store = SqliteRuntimeStore::open_in_memory().await?;
        let first_actor = ActorId::from_string("actor:local:first");
        let second_actor = ActorId::from_string("actor:local:second");
        store
            .import_legacy_authorization(
                LegacyAuthorizationSnapshot {
                    version: 1,
                    actors: [&first_actor, &second_actor]
                        .into_iter()
                        .map(|actor| LegacyActor {
                            id: actor.to_string(),
                            enabled: true,
                            tools: vec!["*".into()],
                            identities: vec![],
                        })
                        .collect(),
                },
                Timestamp(0),
            )
            .await?;
        let request = RequestId::parse("0190f2ef-0000-7000-8000-000000000061")?;
        store
            .submit_for_actor(
                &first_actor,
                submission(request.clone(), "private", '4'),
                Timestamp(1),
            )
            .await?;
        let command = cancel("0190f2ef-0000-7000-8000-000000000062", request.clone())?;
        store
            .cancel_for_actor(&first_actor, command.clone(), Timestamp(2))
            .await?;

        assert!(
            store
                .cancel_for_actor(&second_actor, command, Timestamp(3))
                .await
                .is_err()
        );
        Ok(())
    }
}
