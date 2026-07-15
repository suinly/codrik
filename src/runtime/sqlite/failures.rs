use anyhow::{Result, anyhow};
use async_trait::async_trait;
use tokio_rusqlite::{params, rusqlite::OptionalExtension};

use crate::runtime::{
    model::{
        ActorId, Audience, LocalRequestState, OutboxId, RequestId, RunId, Timestamp, WorkItemId,
    },
    sqlite::{
        SqliteRuntimeStore,
        checkpoint::{TerminalBundleContext, create_terminal_bundles},
        map_call_error,
        retry::call_with_busy_retry,
    },
    store::{FailureDisposition, FailureStore, NewOutboxIntent, OutboxPayload},
};

#[async_trait]
impl FailureStore for SqliteRuntimeStore {
    async fn record_failure(
        &self,
        work: &WorkItemId,
        error: &str,
        now: Timestamp,
    ) -> Result<FailureDisposition> {
        let store = self.clone();
        let work = work.clone();
        let error = error.to_owned();
        call_with_busy_retry(move || {
            let store = store.clone();
            let work = work.clone();
            let error = error.clone();
            async move {
                store.connection.call(move |connection| -> Result<FailureDisposition> {
                    let transaction = connection.transaction_with_behavior(
                        tokio_rusqlite::rusqlite::TransactionBehavior::Immediate,
                    )?;
                    let failure_count = transaction.query_row(
                        "SELECT failure_count FROM work_items WHERE id = ?1",
                        [work.as_str()],
                        |row| row.get::<_, i64>(0),
                    ).optional()?.ok_or_else(|| anyhow!("work item does not exist"))? + 1;
                    if failure_count < 5 {
                        let delay = 1_i64 << (failure_count - 1);
                        let retry_at = now.plus_millis(delay * 1_000);
                        transaction.execute(
                            "UPDATE work_items SET failure_count = ?2, next_attempt_at = ?3, last_error = ?4, updated_at = ?5 WHERE id = ?1",
                            params![work.as_str(), failure_count, retry_at.0, error, now.0],
                        )?;
                        transaction.commit()?;
                        return Ok(FailureDisposition::RetryAt(retry_at));
                    }

                    let active_run = transaction.query_row(
                        "SELECT runs.id, runs.actor_id, work_items.audience_kind, work_items.audience_address
                         FROM runs JOIN work_items ON work_items.id = runs.work_item_id
                         WHERE runs.work_item_id = ?1 AND runs.state = 'active'",
                        [work.as_str()],
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
                            work_item_id: work.clone(),
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
                            "UPDATE events SET state = 'pending', run_id = NULL, updated_at = ?2
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
                        params![work.as_str(), error, now.0],
                    )?;
                    transaction.commit()?;
                    Ok(FailureDisposition::Terminalized)
                }).await.map_err(map_call_error)
            }
        }).await
    }

    async fn record_progress(&self, work: &WorkItemId, now: Timestamp) -> Result<()> {
        let store = self.clone();
        let work = work.clone();
        call_with_busy_retry(move || {
            let store = store.clone();
            let work = work.clone();
            async move {
                store.connection.call(move |connection| -> Result<()> {
                    let transaction = connection.transaction_with_behavior(
                        tokio_rusqlite::rusqlite::TransactionBehavior::Immediate,
                    )?;
                    transaction.execute(
                        "UPDATE work_items SET failure_count = 0, next_attempt_at = NULL, last_error = NULL, updated_at = ?2 WHERE id = ?1",
                        params![work.as_str(), now.0],
                    )?;
                    transaction.commit()?;
                    Ok(())
                }).await.map_err(map_call_error)
            }
        }).await
    }
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
mod tests {
    use crate::{
        auth::{LegacyActor, LegacyAuthorizationSnapshot},
        runtime::{
            model::{ActorId, LocalRequestState, RequestId, Timestamp},
            sqlite::SqliteRuntimeStore,
            store::{
                BundleStore, CheckpointRun, CheckpointStore, DispatchStore, FailureDisposition,
                FailureStore, LocalIngressStore, LocalSubmission, RuntimeAuthorizationStore,
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

        for (index, delay) in [1_000, 2_000, 4_000, 8_000].into_iter().enumerate() {
            assert_eq!(
                store
                    .record_failure(&run.work_item_id, "transient", Timestamp(100))
                    .await
                    .unwrap(),
                FailureDisposition::RetryAt(Timestamp(100 + delay)),
                "failure {}",
                index + 1,
            );
        }
        store
            .record_progress(&run.work_item_id, Timestamp(150))
            .await
            .unwrap();
        assert_eq!(
            store
                .record_failure(&run.work_item_id, "after-progress", Timestamp(160))
                .await
                .unwrap(),
            FailureDisposition::RetryAt(Timestamp(1_160)),
        );
        // Restore the fourth consecutive failure before exercising the fifth-failure policy.
        for now in [161, 162, 163] {
            let _ = store
                .record_failure(&run.work_item_id, "transient", Timestamp(now))
                .await
                .unwrap();
        }
        assert_eq!(
            store
                .record_failure(&run.work_item_id, "terminal", Timestamp(200))
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
}
