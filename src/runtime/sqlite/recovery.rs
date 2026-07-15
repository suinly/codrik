use anyhow::{Result, anyhow};
use tokio_rusqlite::params;

use crate::runtime::{
    model::{ActorId, AttemptId, RunId, Timestamp, WorkItemId},
    sqlite::{SqliteRuntimeStore, map_call_error},
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecoveredUnknownOutcome {
    pub actor_id: ActorId,
    pub work_item_id: WorkItemId,
    pub run_id: RunId,
    pub attempt_id: AttemptId,
    pub lease_generation: i64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StartupRecoveryReport {
    pub expired_actor_leases: u64,
    pub expired_bundle_claims: u64,
    pub orphaned_running_attempts: u64,
    pub unknown_outcomes: Vec<RecoveredUnknownOutcome>,
}

impl SqliteRuntimeStore {
    pub async fn recover_startup(&self, now: Timestamp) -> Result<StartupRecoveryReport> {
        self.connection
            .call(move |connection| -> Result<StartupRecoveryReport> {
                let transaction = connection.transaction_with_behavior(
                    tokio_rusqlite::rusqlite::TransactionBehavior::Immediate,
                )?;
                let candidates = {
                    let mut statement = transaction.prepare(
                        "SELECT runs.actor_id, runs.work_item_id, runs.id,
                                tool_attempts.id, runs.lease_generation
                         FROM tool_attempts
                         JOIN runs ON runs.id = tool_attempts.run_id
                         JOIN work_items ON work_items.id = runs.work_item_id
                         JOIN actors ON actors.id = runs.actor_id
                         LEFT JOIN actor_leases ON actor_leases.actor_id = runs.actor_id
                         WHERE tool_attempts.state = 'running'
                           AND runs.state = 'active'
                           AND work_items.actor_id = runs.actor_id
                           AND work_items.state IN ('ready', 'waiting')
                           AND (actor_leases.actor_id IS NULL
                                OR actor_leases.expires_at <= ?1
                                OR actor_leases.generation != runs.lease_generation)",
                    )?;
                    statement
                        .query_map([now.0], |row| {
                            Ok((
                                row.get::<_, String>(0)?,
                                row.get::<_, String>(1)?,
                                row.get::<_, String>(2)?,
                                row.get::<_, String>(3)?,
                                row.get::<_, i64>(4)?,
                            ))
                        })?
                        .collect::<std::result::Result<Vec<_>, _>>()?
                };
                let mut unknown_outcomes = Vec::new();
                for (actor, work, run, attempt, generation) in candidates {
                    if transaction.execute(
                        "UPDATE tool_attempts SET state = 'outcome_unknown', updated_at = ?1
                         WHERE id = ?2 AND state = 'running'",
                        params![now.0, attempt],
                    )? == 1
                    {
                        transaction.execute(
                            "UPDATE work_items SET state = 'waiting_for_decision', updated_at = ?1
                             WHERE id = ?2 AND state IN ('ready', 'waiting')",
                            params![now.0, work],
                        )?;
                        unknown_outcomes.push(RecoveredUnknownOutcome {
                            actor_id: ActorId::from_string(actor),
                            work_item_id: WorkItemId::from_string(work),
                            run_id: RunId::from_string(run),
                            attempt_id: AttemptId::from_string(attempt),
                            lease_generation: generation,
                        });
                    }
                }
                let expired_actor_leases = transaction
                    .execute("DELETE FROM actor_leases WHERE expires_at <= ?1", [now.0])?
                    as u64;
                let expired_bundle_claims = transaction.execute(
                    "UPDATE result_bundles
                     SET state = 'failed_retryable', claim_owner = NULL,
                         claim_expires_at = NULL, next_attempt_at = ?1,
                         last_error = 'interrupted_delivery', updated_at = ?1
                     WHERE state = 'delivering' AND claim_expires_at <= ?1",
                    [now.0],
                )? as u64;
                let orphaned_running_attempts = unknown_outcomes.len() as u64;
                transaction.commit()?;
                Ok(StartupRecoveryReport {
                    expired_actor_leases,
                    expired_bundle_claims,
                    orphaned_running_attempts,
                    unknown_outcomes,
                })
            })
            .await
            .map_err(map_call_error)
            .map_err(|error| anyhow!("failed startup recovery: {error:#}"))
    }

    pub async fn recover_shutdown(
        &self,
        actor_owner: &str,
        bundle_owner: &str,
        now: Timestamp,
    ) -> Result<()> {
        let actor_owner = actor_owner.to_owned();
        let bundle_owner = bundle_owner.to_owned();
        self.connection
            .call(move |connection| -> Result<()> {
                let transaction = connection.transaction_with_behavior(
                    tokio_rusqlite::rusqlite::TransactionBehavior::Immediate,
                )?;
                transaction.execute(
                    "UPDATE result_bundles
                     SET state = 'failed_retryable', claim_owner = NULL,
                         claim_expires_at = NULL, next_attempt_at = ?1,
                         last_error = 'shutdown_before_ack', updated_at = ?1
                     WHERE state = 'delivering' AND claim_owner = ?2",
                    params![now.0, bundle_owner],
                )?;
                transaction.execute(
                    "DELETE FROM actor_leases
                     WHERE owner_id = ?1 AND NOT EXISTS (
                         SELECT 1 FROM runs
                         JOIN tool_attempts ON tool_attempts.run_id = runs.id
                         WHERE runs.actor_id = actor_leases.actor_id
                           AND runs.state = 'active' AND tool_attempts.state = 'running'
                     )",
                    [actor_owner],
                )?;
                transaction.commit()?;
                Ok(())
            })
            .await
            .map_err(map_call_error)
            .map_err(|error| anyhow!("failed shutdown recovery: {error:#}"))
    }

    #[cfg(test)]
    pub(super) async fn seed_recovery_fixture_for_test(&self) -> Result<()> {
        self.seed_attempt_recovery_for_test(Some((1, 99))).await?;
        self.connection.call(|connection| -> Result<()> {
            let transaction = connection.transaction()?;
            transaction.execute("INSERT INTO events(id, actor_id, mailbox_sequence, gateway, external_id, kind, audience_kind, payload_json, state, created_at, updated_at) VALUES ('event', 'actor', 1, 'local', 'request', 'user_message', 'actor_private', '{}', 'completed', 0, 0)", [])?;
            let request = uuid::Uuid::new_v4().to_string();
            let bundle = uuid::Uuid::new_v4().to_string();
            transaction.execute("INSERT INTO local_requests(request_id, actor_id, event_id, work_item_id, prompt_sha256, state, result_bundle_id, created_at, updated_at) VALUES (?1, 'actor', 'event', 'work', ?2, 'completed', ?3, 0, 0)", params![request, "0".repeat(64), bundle])?;
            transaction.execute("INSERT INTO result_bundles(id, request_id, delivery_count, manifest_sha256, state, attempt_count, claim_owner, claim_expires_at, created_at, updated_at) VALUES (?1, ?2, 1, ?3, 'delivering', 1, 'old', 99, 0, 0)", params![bundle, request, "0".repeat(64)])?;
            transaction.commit()?;
            Ok(())
        }).await.map_err(map_call_error)
    }

    #[cfg(test)]
    pub(super) async fn seed_attempt_recovery_for_test(
        &self,
        lease: Option<(i64, i64)>,
    ) -> Result<()> {
        self.connection.call(move |connection| -> Result<()> {
            let transaction = connection.transaction()?;
            transaction.execute("INSERT INTO actors(id, enabled, tools_json, created_at) VALUES ('actor', 1, '[]', 0)", [])?;
            transaction.execute("INSERT INTO work_items(id, actor_id, kind, audience_kind, state, created_at, updated_at) VALUES ('work', 'actor', 'interactive', 'actor_private', 'ready', 0, 0)", [])?;
            transaction.execute("INSERT INTO runs(id, actor_id, work_item_id, state, lease_generation, observed_sequence, created_at, updated_at) VALUES ('run', 'actor', 'work', 'active', 1, 0, 0, 0)", [])?;
            transaction.execute("INSERT INTO tool_attempts(id, run_id, tool_call_id, tool_name, arguments_json, capabilities_json, state, created_at, updated_at) VALUES ('attempt', 'run', 'call', 'datetime', '{}', '{}', 'running', 0, 0)", [])?;
            if let Some((generation, expires)) = lease {
                transaction.execute("INSERT INTO actor_leases(actor_id, generation, owner_id, expires_at) VALUES ('actor', ?1, 'owner', ?2)", params![generation, expires])?;
            }
            transaction.commit()?;
            Ok(())
        }).await.map_err(map_call_error)
    }

    #[cfg(test)]
    pub(super) async fn attempt_state_for_test(&self) -> Result<String> {
        self.connection
            .call(|connection| -> Result<String> {
                Ok(connection.query_row(
                    "SELECT state FROM tool_attempts WHERE id = 'attempt'",
                    [],
                    |row| row.get(0),
                )?)
            })
            .await
            .map_err(map_call_error)
    }

    #[cfg(test)]
    pub(crate) async fn sole_work_failure_count_for_test(&self) -> Result<i64> {
        self.connection
            .call(|connection| -> Result<i64> {
                Ok(connection
                    .query_row("SELECT failure_count FROM work_items", [], |row| row.get(0))?)
            })
            .await
            .map_err(map_call_error)
    }

    #[cfg(test)]
    pub(super) async fn set_attempt_state_for_test(&self, state: &str) -> Result<()> {
        let state = state.to_owned();
        self.connection
            .call(move |connection| -> Result<()> {
                connection.execute("UPDATE tool_attempts SET state = ?1", [state])?;
                Ok(())
            })
            .await
            .map_err(map_call_error)
    }

    #[cfg(test)]
    pub(super) async fn lease_count_for_test(&self) -> Result<u64> {
        self.connection
            .call(|connection| -> Result<u64> {
                Ok(connection
                    .query_row("SELECT count(*) FROM actor_leases", [], |row| row.get(0))?)
            })
            .await
            .map_err(map_call_error)
    }

    #[cfg(test)]
    pub(super) async fn recovery_fixture_states_for_test(
        &self,
    ) -> Result<(u64, String, String, String)> {
        self.connection
            .call(|connection| -> Result<_> {
                Ok((
                    connection
                        .query_row("SELECT count(*) FROM actor_leases", [], |row| row.get(0))?,
                    connection
                        .query_row("SELECT state FROM result_bundles", [], |row| row.get(0))?,
                    connection
                        .query_row("SELECT state FROM tool_attempts", [], |row| row.get(0))?,
                    connection.query_row("SELECT state FROM work_items", [], |row| row.get(0))?,
                ))
            })
            .await
            .map_err(map_call_error)
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;

    use crate::runtime::{model::Timestamp, sqlite::SqliteRuntimeStore};

    #[tokio::test]
    async fn startup_recovery_expires_claims_and_marks_running_tools_unknown() -> Result<()> {
        let store = SqliteRuntimeStore::open_in_memory().await?;
        store.seed_recovery_fixture_for_test().await?;

        let report = store.recover_startup(Timestamp(100)).await?;

        assert_eq!(report.expired_actor_leases, 1);
        assert_eq!(report.expired_bundle_claims, 1);
        assert_eq!(report.orphaned_running_attempts, 1);
        assert_eq!(
            store.recovery_fixture_states_for_test().await?,
            (
                0,
                "failed_retryable".into(),
                "outcome_unknown".into(),
                "waiting_for_decision".into()
            )
        );
        Ok(())
    }

    #[tokio::test]
    async fn live_matching_fence_is_not_recovered_but_stale_generation_is() -> Result<()> {
        let live = SqliteRuntimeStore::open_in_memory().await?;
        live.seed_attempt_recovery_for_test(Some((1, 200))).await?;
        assert_eq!(
            live.recover_startup(Timestamp(100))
                .await?
                .orphaned_running_attempts,
            0
        );
        assert_eq!(live.attempt_state_for_test().await?, "running");

        let stale = SqliteRuntimeStore::open_in_memory().await?;
        stale.seed_attempt_recovery_for_test(Some((2, 200))).await?;
        assert_eq!(
            stale
                .recover_startup(Timestamp(100))
                .await?
                .orphaned_running_attempts,
            1
        );
        assert_eq!(stale.attempt_state_for_test().await?, "outcome_unknown");
        Ok(())
    }

    #[tokio::test]
    async fn absent_and_expired_leases_recover_once_idempotently() -> Result<()> {
        for lease in [None, Some((1, 99))] {
            let store = SqliteRuntimeStore::open_in_memory().await?;
            store.seed_attempt_recovery_for_test(lease).await?;
            assert_eq!(
                store
                    .recover_startup(Timestamp(100))
                    .await?
                    .orphaned_running_attempts,
                1
            );
            assert_eq!(
                store
                    .recover_startup(Timestamp(100))
                    .await?
                    .orphaned_running_attempts,
                0
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn shutdown_releases_only_safe_actor_lease_and_retries_sent_unacked_bundle() -> Result<()>
    {
        let safe = SqliteRuntimeStore::open_in_memory().await?;
        safe.seed_recovery_fixture_for_test().await?;
        safe.set_attempt_state_for_test("prepared").await?;
        safe.recover_shutdown("owner", "old", Timestamp(50)).await?;
        let states = safe.recovery_fixture_states_for_test().await?;
        assert_eq!(states.0, 0);
        assert_eq!(states.1, "failed_retryable");

        let running = SqliteRuntimeStore::open_in_memory().await?;
        running
            .seed_attempt_recovery_for_test(Some((1, 200)))
            .await?;
        running
            .recover_shutdown("owner", "outbox", Timestamp(50))
            .await?;
        assert_eq!(running.lease_count_for_test().await?, 1);
        assert_eq!(running.attempt_state_for_test().await?, "running");
        Ok(())
    }
}
