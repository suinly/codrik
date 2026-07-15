use anyhow::{Result, anyhow};
use async_trait::async_trait;
use tokio_rusqlite::{params, rusqlite::OptionalExtension};

use crate::runtime::{
    model::{ActorId, Audience, LocalRequestState, OutboxId, RequestId, RunId, Timestamp},
    sqlite::{
        SqliteRuntimeStore,
        checkpoint::{TerminalBundleContext, create_terminal_bundles},
        map_call_error,
        retry::call_with_busy_retry,
    },
    store::{
        FailureDisposition, FailureFence, FailureStore, NewOutboxIntent, OutboxPayload, StaleLease,
    },
};

#[async_trait]
impl FailureStore for SqliteRuntimeStore {
    async fn record_failure(
        &self,
        fence: &FailureFence,
        error: &str,
        now: Timestamp,
    ) -> Result<FailureDisposition> {
        let store = self.clone();
        let fence = fence.clone();
        let error = error.to_owned();
        call_with_busy_retry(move || {
            let store = store.clone();
            let fence = fence.clone();
            let error = error.clone();
            async move {
                store.connection.call(move |connection| -> Result<FailureDisposition> {
                    let transaction = connection.transaction_with_behavior(
                        tokio_rusqlite::rusqlite::TransactionBehavior::Immediate,
                    )?;
                    ensure_failure_fence(&transaction, &fence, now, true)?;
                    let failure_count = transaction.query_row(
                        "SELECT failure_count FROM work_items WHERE id = ?1 AND state = 'ready'",
                        [fence.work_item_id.as_str()],
                        |row| row.get::<_, i64>(0),
                    ).optional()?.ok_or_else(|| anyhow!("work item does not exist"))? + 1;
                    if failure_count < 5 {
                        let delay = 1_i64 << (failure_count - 1);
                        let retry_at = now.plus_millis(delay * 1_000);
                        transaction.execute(
                            "UPDATE work_items SET failure_count = ?2, next_attempt_at = ?3, last_error = ?4, updated_at = ?5 WHERE id = ?1",
                            params![fence.work_item_id.as_str(), failure_count, retry_at.0, error, now.0],
                        )?;
                        transaction.commit()?;
                        return Ok(FailureDisposition::RetryAt(retry_at));
                    }

                    let active_run = transaction.query_row(
                        "SELECT runs.id, runs.actor_id, work_items.audience_kind, work_items.audience_address
                         FROM runs JOIN work_items ON work_items.id = runs.work_item_id
                         WHERE runs.work_item_id = ?1 AND runs.state = 'active'",
                        [fence.work_item_id.as_str()],
                        |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?, row.get::<_, String>(2)?, row.get::<_, Option<String>>(3)?)),
                    ).optional()?;
                    if let Some((run_id, actor_id, audience_kind, audience_address)) = active_run {
                        let audience = decode_audience(&audience_kind, audience_address)?;
                        let request_ids = {
                            let mut statement = transaction.prepare(
                                "SELECT local_requests.request_id FROM local_requests
                                 JOIN run_events ON run_events.event_id = local_requests.event_id
                                 WHERE run_events.run_id = ?1 AND run_events.incorporated = 1
                                   AND local_requests.state = 'active'
                                 ORDER BY local_requests.created_at, local_requests.request_id",
                            )?;
                            statement.query_map([run_id.as_str()], |row| row.get::<_, String>(0))?
                                .map(|row| RequestId::parse(&row?).map_err(|error| tokio_rusqlite::rusqlite::Error::ToSqlConversionFailure(Box::new(error))))
                                .collect::<std::result::Result<Vec<_>, _>>()?
                        };
                        let context = TerminalBundleContext {
                            actor_id: ActorId::from_string(actor_id),
                            work_item_id: fence.work_item_id.clone(),
                            run_id: RunId::from_string(run_id.clone()),
                            audience: audience.clone(),
                        };
                        create_terminal_bundles(
                            &transaction,
                            &context,
                            &request_ids,
                            vec![NewOutboxIntent {
                                id: OutboxId::new(),
                                intent_key: format!("run:{run_id}:dispatcher-failed"),
                                intent_class: "terminal_error".into(),
                                audience,
                                payload: OutboxPayload::TerminalError {
                                    code: "dispatcher_failure_limit".into(),
                                    message: error.clone(),
                                },
                            }],
                            LocalRequestState::FailedTerminal,
                            now,
                        )?;
                        transaction.execute(
                            "UPDATE events SET state = 'failed_terminal', updated_at = ?2
                             WHERE id IN (SELECT event_id FROM run_events WHERE run_id = ?1 AND incorporated = 1)",
                            params![run_id, now.0],
                        )?;
                        transaction.execute(
                            "UPDATE local_requests SET work_item_id = NULL, updated_at = ?2
                             WHERE event_id IN (SELECT event_id FROM run_events WHERE run_id = ?1 AND incorporated = 0)",
                            params![run_id, now.0],
                        )?;
                        transaction.execute(
                            "UPDATE events SET state = 'pending', run_id = NULL, updated_at = ?2
                             , work_item_id = NULL
                             WHERE id IN (SELECT event_id FROM run_events WHERE run_id = ?1 AND incorporated = 0)",
                            params![run_id, now.0],
                        )?;
                        transaction.execute("DELETE FROM run_events WHERE run_id = ?1 AND incorporated = 0", [run_id.as_str()])?;
                        transaction.execute(
                            "UPDATE tool_attempts SET state = 'outcome_unknown', updated_at = ?2
                             WHERE run_id = ?1 AND state = 'running'",
                            params![run_id, now.0],
                        )?;
                        transaction.execute(
                            "UPDATE tool_attempts SET state = 'cancelled_known', updated_at = ?2
                             WHERE run_id = ?1 AND state = 'prepared'",
                            params![run_id, now.0],
                        )?;
                        transaction.execute(
                            "UPDATE runs SET state = 'failed_terminal', updated_at = ?2 WHERE id = ?1",
                            params![run_id, now.0],
                        )?;
                    }
                    transaction.execute(
                        "UPDATE work_items SET state = 'failed_terminal', failure_count = 5,
                         next_attempt_at = NULL, last_error = ?2, updated_at = ?3 WHERE id = ?1",
                        params![fence.work_item_id.as_str(), error, now.0],
                    )?;
                    transaction.commit()?;
                    Ok(FailureDisposition::Terminalized)
                }).await.map_err(map_call_error)
            }
        }).await
    }

    async fn record_progress(&self, fence: &FailureFence, now: Timestamp) -> Result<()> {
        let store = self.clone();
        let fence = fence.clone();
        call_with_busy_retry(move || {
            let store = store.clone();
            let fence = fence.clone();
            async move {
                store.connection.call(move |connection| -> Result<()> {
                    let transaction = connection.transaction_with_behavior(
                        tokio_rusqlite::rusqlite::TransactionBehavior::Immediate,
                    )?;
                    ensure_failure_fence(&transaction, &fence, now, false)?;
                    transaction.execute(
                        "UPDATE work_items SET failure_count = 0, next_attempt_at = NULL, last_error = NULL, updated_at = ?2 WHERE id = ?1",
                        params![fence.work_item_id.as_str(), now.0],
                    )?;
                    transaction.commit()?;
                    Ok(())
                }).await.map_err(map_call_error)
            }
        }).await
    }
}

fn ensure_failure_fence(
    transaction: &tokio_rusqlite::rusqlite::Transaction<'_>,
    fence: &FailureFence,
    now: Timestamp,
    require_active: bool,
) -> Result<()> {
    let valid: bool = transaction.query_row(
        "SELECT EXISTS(
           SELECT 1 FROM actor_leases
           JOIN runs ON runs.actor_id = actor_leases.actor_id
           JOIN work_items ON work_items.id = runs.work_item_id
           WHERE actor_leases.actor_id = ?1 AND actor_leases.owner_id = ?2
             AND actor_leases.generation = ?3 AND actor_leases.expires_at > ?4
             AND runs.id = ?5 AND runs.work_item_id = ?6
             AND runs.lease_generation = ?3
             AND (?7 = 0 OR (runs.state = 'active' AND work_items.state = 'ready'))
         )",
        params![
            fence.lease.actor_id.as_str(),
            fence.lease.owner_id,
            fence.lease.generation,
            now.0,
            fence.run_id.as_str(),
            fence.work_item_id.as_str(),
            require_active,
        ],
        |row| row.get(0),
    )?;
    if !valid {
        return Err(StaleLease.into());
    }
    Ok(())
}

fn decode_audience(kind: &str, address: Option<String>) -> Result<Audience> {
    match (kind, address) {
        ("actor_private", None) => Ok(Audience::ActorPrivate),
        ("shareable", None) => Ok(Audience::Shareable),
        ("conversation_scoped", Some(address)) => Ok(Audience::ConversationScoped { address }),
        _ => anyhow::bail!("invalid stored audience"),
    }
}

#[cfg(test)]
impl SqliteRuntimeStore {
    async fn work_state_for_test(
        &self,
        work: &crate::runtime::model::WorkItemId,
    ) -> Result<String> {
        let work = work.clone();
        self.connection
            .call(move |connection| -> Result<String> {
                Ok(connection.query_row(
                    "SELECT state FROM work_items WHERE id = ?1",
                    [work.as_str()],
                    |row| row.get(0),
                )?)
            })
            .await
            .map_err(map_call_error)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        auth::{LegacyActor, LegacyAuthorizationSnapshot},
        runtime::{
            model::{ActorId, LocalRequestState, RequestId, Timestamp},
            sqlite::SqliteRuntimeStore,
            store::{
                BundleStore, CheckpointRun, CheckpointStore, DispatchStore, FailureDisposition,
                FailureFence, FailureStore, FinalizeRun, LocalIngressStore, LocalSubmission,
                NewOutboxIntent, OutboxPayload, RuntimeAuthorizationStore,
            },
        },
    };

    fn requires_failure_store<T: FailureStore>() {}

    #[test]
    fn sqlite_store_implements_failure_store() {
        requires_failure_store::<SqliteRuntimeStore>();
    }

    #[tokio::test]
    async fn failures_back_off_one_two_four_eight_then_terminalize_every_incorporated_request() {
        let store = SqliteRuntimeStore::open_in_memory().await.unwrap();
        let actor = ActorId::from_string("actor:failure-tests");
        store
            .import_legacy_authorization(
                LegacyAuthorizationSnapshot {
                    version: 1,
                    actors: vec![LegacyActor {
                        id: actor.to_string(),
                        enabled: true,
                        tools: vec!["*".into()],
                        identities: vec![],
                    }],
                },
                Timestamp(1),
            )
            .await
            .unwrap();
        let requests = [RequestId::new(), RequestId::new()];
        for (index, request_id) in requests.iter().enumerate() {
            store
                .submit_for_actor(
                    &actor,
                    LocalSubmission {
                        request_id: request_id.clone(),
                        text: format!("message {index}"),
                        prompt_sha256: format!("{index:064x}"),
                    },
                    Timestamp(2 + index as i64),
                )
                .await
                .unwrap();
        }
        let lease = store
            .acquire_ready_actor("worker", Timestamp(10), Timestamp(1_000))
            .await
            .unwrap()
            .unwrap();
        let run = store
            .attach_next_run(&lease, 8, Timestamp(11))
            .await
            .unwrap()
            .unwrap();
        store
            .checkpoint_run(
                CheckpointRun {
                    run: run.clone(),
                    incorporated_event_ids: run.source_event_ids.clone(),
                    checkpointed_attempt_ids: vec![],
                    messages: vec![],
                },
                Timestamp(12),
            )
            .await
            .unwrap();
        let fence = FailureFence::from(&run);

        for (index, delay) in [1_000, 2_000, 4_000, 8_000].into_iter().enumerate() {
            assert_eq!(
                store
                    .record_failure(&fence, "transient", Timestamp(100))
                    .await
                    .unwrap(),
                FailureDisposition::RetryAt(Timestamp(100 + delay)),
                "failure {}",
                index + 1,
            );
        }
        store.record_progress(&fence, Timestamp(150)).await.unwrap();
        assert_eq!(
            store
                .record_failure(&fence, "after-progress", Timestamp(160))
                .await
                .unwrap(),
            FailureDisposition::RetryAt(Timestamp(1_160)),
        );
        // Restore the fourth consecutive failure before exercising the fifth-failure policy.
        for now in [161, 162, 163] {
            let _ = store
                .record_failure(&fence, "transient", Timestamp(now))
                .await
                .unwrap();
        }
        assert_eq!(
            store
                .record_failure(&fence, "terminal", Timestamp(200))
                .await
                .unwrap(),
            FailureDisposition::Terminalized,
        );
        for request_id in requests {
            let request = store
                .resolve_local_request(&request_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(request.state, LocalRequestState::FailedTerminal);
            let bundle = store
                .load_bundle(request.result_bundle_id.as_ref().unwrap())
                .await
                .unwrap();
            assert_eq!(bundle.deliveries.len(), 1);
        }
    }

    #[tokio::test]
    async fn fifth_failure_rebinds_unincorporated_request_to_distinct_work() {
        let store = SqliteRuntimeStore::open_in_memory().await.unwrap();
        let actor = ActorId::from_string("actor:released-event");
        store
            .import_legacy_authorization(
                LegacyAuthorizationSnapshot {
                    version: 1,
                    actors: vec![LegacyActor {
                        id: actor.to_string(),
                        enabled: true,
                        tools: vec!["*".into()],
                        identities: vec![],
                    }],
                },
                Timestamp(1),
            )
            .await
            .unwrap();
        let first = RequestId::new();
        let second = RequestId::new();
        for (index, request_id) in [&first, &second].into_iter().enumerate() {
            store
                .submit_for_actor(
                    &actor,
                    LocalSubmission {
                        request_id: request_id.clone(),
                        text: format!("message-{index}"),
                        prompt_sha256: format!("{index:064x}"),
                    },
                    Timestamp(2 + index as i64),
                )
                .await
                .unwrap();
        }
        let lease = store
            .acquire_ready_actor("old", Timestamp(10), Timestamp(1_000))
            .await
            .unwrap()
            .unwrap();
        let run = store
            .attach_next_run(&lease, 8, Timestamp(11))
            .await
            .unwrap()
            .unwrap();
        let old_work = run.work_item_id.clone();
        store
            .checkpoint_run(
                CheckpointRun {
                    run: run.clone(),
                    incorporated_event_ids: vec![run.source_event_ids[0].clone()],
                    checkpointed_attempt_ids: vec![],
                    messages: vec![],
                },
                Timestamp(12),
            )
            .await
            .unwrap();
        let fence = FailureFence::from(&run);
        for attempt in 0..5 {
            let _ = store
                .record_failure(&fence, "failed", Timestamp(20 + attempt))
                .await
                .unwrap();
        }
        store.release_lease(&lease).await.unwrap();
        assert_eq!(
            store
                .resolve_local_request(&first)
                .await
                .unwrap()
                .unwrap()
                .state,
            LocalRequestState::FailedTerminal
        );
        assert!(
            store
                .resolve_local_request(&second)
                .await
                .unwrap()
                .unwrap()
                .work_item_id
                .is_none()
        );

        let replacement_lease = store
            .acquire_ready_actor("new", Timestamp(30), Timestamp(1_030))
            .await
            .unwrap()
            .unwrap();
        let replacement = store
            .attach_next_run(&replacement_lease, 8, Timestamp(31))
            .await
            .unwrap()
            .unwrap();
        assert_ne!(replacement.work_item_id, old_work);
        assert_eq!(
            store
                .resolve_local_request(&second)
                .await
                .unwrap()
                .unwrap()
                .work_item_id,
            Some(replacement.work_item_id.clone())
        );
        assert_eq!(
            store.work_state_for_test(&old_work).await.unwrap(),
            "failed_terminal"
        );
    }

    #[tokio::test]
    async fn stale_fence_cannot_reset_or_terminalize_replacement_success() {
        let store = SqliteRuntimeStore::open_in_memory().await.unwrap();
        let actor = ActorId::from_string("actor:stale-failure");
        store
            .import_legacy_authorization(
                LegacyAuthorizationSnapshot {
                    version: 1,
                    actors: vec![LegacyActor {
                        id: actor.to_string(),
                        enabled: true,
                        tools: vec!["*".into()],
                        identities: vec![],
                    }],
                },
                Timestamp(1),
            )
            .await
            .unwrap();
        let request = RequestId::new();
        store
            .submit_for_actor(
                &actor,
                LocalSubmission {
                    request_id: request.clone(),
                    text: "hello".into(),
                    prompt_sha256: "a".repeat(64),
                },
                Timestamp(2),
            )
            .await
            .unwrap();
        let old_lease = store
            .acquire_ready_actor("old", Timestamp(10), Timestamp(20))
            .await
            .unwrap()
            .unwrap();
        let old_run = store
            .attach_next_run(&old_lease, 8, Timestamp(11))
            .await
            .unwrap()
            .unwrap();
        store
            .checkpoint_run(
                CheckpointRun {
                    run: old_run.clone(),
                    incorporated_event_ids: old_run.source_event_ids.clone(),
                    checkpointed_attempt_ids: vec![],
                    messages: vec![],
                },
                Timestamp(12),
            )
            .await
            .unwrap();
        let old_fence = FailureFence::from(&old_run);
        for attempt in 0..4 {
            let _ = store
                .record_failure(&old_fence, "failed", Timestamp(13 + attempt))
                .await
                .unwrap();
        }

        let new_lease = store
            .acquire_ready_actor("new", Timestamp(9_000), Timestamp(10_000))
            .await
            .unwrap()
            .unwrap();
        let new_run = store
            .attach_next_run(&new_lease, 8, Timestamp(9_001))
            .await
            .unwrap()
            .unwrap();
        let new_fence = FailureFence::from(&new_run);
        store
            .record_progress(&new_fence, Timestamp(9_002))
            .await
            .unwrap();
        store
            .finalize_run(
                FinalizeRun {
                    run: new_run.clone(),
                    incorporated_event_ids: new_run.source_event_ids.clone(),
                    final_messages: vec![],
                    outbox: vec![NewOutboxIntent {
                        id: crate::runtime::model::OutboxId::new(),
                        intent_key: format!("run:{}:success", new_run.run_id),
                        intent_class: "interactive_reply".into(),
                        audience: new_run.audience.clone(),
                        payload: OutboxPayload::Text {
                            text: "done".into(),
                        },
                    }],
                },
                Timestamp(9_003),
            )
            .await
            .unwrap();
        assert!(
            store
                .record_progress(&old_fence, Timestamp(9_004))
                .await
                .is_err()
        );
        assert!(
            store
                .record_failure(&old_fence, "stale fifth", Timestamp(9_004))
                .await
                .is_err()
        );
        assert_eq!(
            store
                .work_state_for_test(&old_run.work_item_id)
                .await
                .unwrap(),
            "completed"
        );
        assert_eq!(
            store
                .resolve_local_request(&request)
                .await
                .unwrap()
                .unwrap()
                .state,
            LocalRequestState::Completed
        );
    }
}
