use anyhow::{Result, bail};
use async_trait::async_trait;
use tokio_rusqlite::rusqlite::{OptionalExtension, TransactionBehavior, params};

use crate::runtime::{
    model::{ArtifactId, Timestamp},
    store::{
        ArtifactLease, ArtifactStore, AttemptOutcome, BeginArtifact, DurableToolExecution,
        ExpiredArtifact,
    },
};

use super::{SqliteRuntimeStore, map_call_error};

const MAX_ACTOR_BYTES: i64 = 2 * 1024 * 1024 * 1024;

#[async_trait]
impl ArtifactStore for SqliteRuntimeStore {
    async fn begin_staging(&self, command: BeginArtifact, now: Timestamp) -> Result<ArtifactLease> {
        if command.size > 256 * 1024 * 1024 {
            bail!("artifact exceeds the 256 MiB per-file limit");
        }
        self.connection
            .call(move |connection| -> Result<ArtifactLease> {
                let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let retained: i64 = transaction.query_row(
                    "SELECT COALESCE(sum(size_bytes), 0) FROM artifacts WHERE actor_id = ?1",
                    [command.actor_id.as_str()],
                    |row| row.get(0),
                )?;
                let size = i64::try_from(command.size)?;
                if retained.saturating_add(size) > MAX_ACTOR_BYTES {
                    bail!("actor artifact quota exceeds 2 GiB");
                }
                transaction.execute(
                    "INSERT INTO artifacts(
                        id, actor_id, attempt_id, state, managed_path, display_name, media_type,
                        size_bytes, sha256, staging_owner, staging_expires_at, created_at, updated_at
                     ) VALUES (?1, ?2, ?3, 'staging', ?4, ?5, ?6, ?7, NULL, ?8, ?9, ?10, ?10)",
                    params![
                        command.id.as_str(), command.actor_id.as_str(), command.attempt_id.as_str(),
                        command.managed_path.to_string_lossy(), command.display_name,
                        command.media_type, size, command.owner, command.lease_until.0, now.0,
                    ],
                )?;
                transaction.commit()?;
                Ok(ArtifactLease {
                    id: command.id,
                    actor_id: command.actor_id,
                    attempt_id: command.attempt_id,
                    managed_path: command.managed_path,
                    owner: command.owner,
                    expires_at: command.lease_until,
                })
            })
            .await
            .map_err(map_call_error)
    }

    async fn renew_staging(
        &self,
        lease: &ArtifactLease,
        until: Timestamp,
    ) -> Result<ArtifactLease> {
        let current = lease.clone();
        self.connection
            .call(move |connection| -> Result<ArtifactLease> {
                let changed = connection.execute(
                    "UPDATE artifacts SET staging_expires_at = ?4, updated_at = ?4
                     WHERE id = ?1 AND state = 'staging' AND staging_owner = ?2
                       AND staging_expires_at = ?3",
                    params![
                        current.id.as_str(),
                        current.owner,
                        current.expires_at.0,
                        until.0
                    ],
                )?;
                if changed != 1 {
                    bail!("stale artifact staging lease");
                }
                Ok(ArtifactLease {
                    expires_at: until,
                    ..current
                })
            })
            .await
            .map_err(map_call_error)
    }

    async fn commit_staged_execution(
        &self,
        run: &crate::runtime::store::AttachedRun,
        attempt: &crate::runtime::model::AttemptId,
        mut execution: DurableToolExecution,
        leases: &[ArtifactLease],
        now: Timestamp,
    ) -> Result<()> {
        for artifact in &execution.artifacts {
            let metadata = std::fs::symlink_metadata(&artifact.managed_path)
                .map_err(|error| anyhow::anyhow!("managed artifact is not durable: {error}"))?;
            if !metadata.file_type().is_file() || metadata.len() != artifact.size {
                bail!("managed artifact is not a durable regular file");
            }
        }
        let run = run.clone();
        let attempt = attempt.clone();
        let leases = leases.to_vec();
        self.connection
            .call(move |connection| -> Result<()> {
                let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let valid_run: bool = transaction.query_row(
                    "SELECT EXISTS(
                       SELECT 1 FROM runs r JOIN actor_leases l ON l.actor_id = r.actor_id
                       WHERE r.id = ?1 AND r.work_item_id = ?2 AND r.state = 'active'
                         AND l.actor_id = ?3 AND l.owner_id = ?4 AND l.generation = ?5
                         AND l.expires_at >= ?6 AND r.lease_generation = ?5
                     )",
                    params![run.run_id.as_str(), run.work_item_id.as_str(), run.lease.actor_id.as_str(),
                        run.lease.owner_id, run.lease.generation, now.0],
                    |row| row.get(0),
                )?;
                if !valid_run { bail!("stale actor lease"); }

                for (artifact, lease) in execution.artifacts.iter_mut().zip(&leases) {
                    if artifact.id != lease.id || lease.actor_id != run.lease.actor_id
                        || lease.attempt_id != attempt || lease.expires_at < now
                    {
                        bail!("invalid artifact staging lease");
                    }
                    let valid: bool = transaction.query_row(
                        "SELECT EXISTS(SELECT 1 FROM artifacts
                         WHERE id = ?1 AND actor_id = ?2 AND attempt_id = ?3 AND state = 'staging'
                           AND staging_owner = ?4 AND staging_expires_at = ?5)",
                        params![lease.id.as_str(), lease.actor_id.as_str(), attempt.as_str(),
                            lease.owner, lease.expires_at.0],
                        |row| row.get(0),
                    )?;
                    if !valid { bail!("stale artifact staging lease"); }

                    let existing = transaction.query_row(
                        "SELECT id, managed_path FROM artifacts
                         WHERE actor_id = ?1 AND state = 'referenced' AND sha256 = ?2 AND size_bytes = ?3",
                        params![lease.actor_id.as_str(), artifact.sha256, i64::try_from(artifact.size)?],
                        |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
                    ).optional()?;
                    if let Some((id, path)) = existing {
                        transaction.execute("DELETE FROM artifacts WHERE id = ?1 AND state = 'staging'", [lease.id.as_str()])?;
                        artifact.id = ArtifactId::parse(&id)?;
                        artifact.managed_path = path.into();
                    } else {
                        let changed = transaction.execute(
                            "UPDATE artifacts SET state = 'referenced', attempt_id = NULL,
                               managed_path = ?2, size_bytes = ?3, sha256 = ?4,
                               staging_owner = NULL, staging_expires_at = NULL, updated_at = ?5
                             WHERE id = ?1 AND state = 'staging'",
                            params![lease.id.as_str(), artifact.managed_path.to_string_lossy(),
                                i64::try_from(artifact.size)?, artifact.sha256, now.0],
                        )?;
                        if changed != 1 { bail!("artifact changed during commit"); }
                    }
                }
                if execution.artifacts.len() != leases.len() {
                    bail!("artifact lease count does not match execution");
                }
                let outcome = AttemptOutcome::Succeeded { execution };
                let changed = transaction.execute(
                    "UPDATE tool_attempts SET state = 'succeeded', outcome_json = ?3, updated_at = ?4
                     WHERE id = ?1 AND run_id = ?2 AND state = 'running'",
                    params![attempt.as_str(), run.run_id.as_str(), serde_json::to_string(&outcome)?, now.0],
                )?;
                if changed != 1 { bail!("attempt is not running"); }
                transaction.commit()?;
                Ok(())
            })
            .await
            .map_err(map_call_error)
    }

    async fn claim_expired_staging(
        &self,
        now: Timestamp,
        limit: usize,
    ) -> Result<Vec<ExpiredArtifact>> {
        let owner = format!("gc:{}", uuid::Uuid::new_v4());
        self.connection.call(move |connection| -> Result<Vec<ExpiredArtifact>> {
            let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let mut statement = transaction.prepare(
                "SELECT id, managed_path FROM artifacts WHERE state = 'staging'
                 AND staging_expires_at < ?1 ORDER BY staging_expires_at, id LIMIT ?2")?;
            let rows = statement.query_map(params![now.0, i64::try_from(limit)?], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?.collect::<std::result::Result<Vec<_>, _>>()?;
            drop(statement);
            let mut claimed = Vec::new();
            for (id, path) in rows {
                let changed = transaction.execute(
                    "UPDATE artifacts SET staging_owner = ?2, staging_expires_at = ?3, updated_at = ?1
                     WHERE id = ?4 AND state = 'staging' AND staging_expires_at < ?1",
                    params![now.0, owner, now.plus_millis(30_000).0, id],
                )?;
                if changed == 1 { claimed.push(ExpiredArtifact { id: ArtifactId::parse(&id)?, managed_path: path.into(), owner: owner.clone() }); }
            }
            transaction.commit()?;
            Ok(claimed)
        }).await.map_err(map_call_error)
    }

    async fn remove_claimed_staging(&self, artifact: &ExpiredArtifact) -> Result<bool> {
        let artifact = artifact.clone();
        self.connection
            .call(move |connection| -> Result<bool> {
                Ok(connection.execute(
                "DELETE FROM artifacts WHERE id = ?1 AND state = 'staging' AND staging_owner = ?2",
                params![artifact.id.as_str(), artifact.owner],
            )? == 1)
            })
            .await
            .map_err(map_call_error)
    }

    async fn artifact_path_exists(&self, path: &std::path::Path) -> Result<bool> {
        let path = path.to_string_lossy().into_owned();
        self.connection
            .call(move |connection| -> Result<bool> {
                Ok(connection.query_row(
                    "SELECT EXISTS(SELECT 1 FROM artifacts WHERE managed_path = ?1)",
                    [path],
                    |row| row.get(0),
                )?)
            })
            .await
            .map_err(map_call_error)
    }
}

#[cfg(test)]
impl SqliteRuntimeStore {
    pub(crate) async fn artifact_row_probe(&self) -> Result<Vec<(String, Option<String>, String)>> {
        self.connection.call(|connection| -> Result<Vec<(String, Option<String>, String)>> {
            let mut statement = connection.prepare(
                "SELECT id, attempt_id, managed_path FROM artifacts WHERE state = 'referenced' ORDER BY id")?;
            Ok(statement.query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
                .collect::<std::result::Result<Vec<_>, _>>()?)
        }).await.map_err(map_call_error)
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;

    use crate::runtime::{
        model::{ActorId, ArtifactId, AttemptId, Timestamp},
        sqlite::SqliteRuntimeStore,
        store::{ArtifactStore, BeginArtifact},
    };

    #[test]
    fn sqlite_store_implements_artifact_store() {
        fn assert_store<T: ArtifactStore>() {}
        assert_store::<SqliteRuntimeStore>();
    }

    #[tokio::test]
    async fn on_disk_staging_transaction_enforces_actor_quota() -> Result<()> {
        let root =
            std::env::temp_dir().join(format!("codrik-sqlite-artifacts-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root)?;
        let store = SqliteRuntimeStore::open(root.join("runtime.sqlite3")).await?;
        store.connection.call(|connection| -> Result<()> {
            connection.execute("INSERT INTO actors(id, enabled, tools_json, created_at) VALUES ('actor-1', 1, '[]', 1)", [])?;
            connection.execute("INSERT INTO work_items(id, actor_id, kind, audience_kind, state, created_at, updated_at) VALUES ('work-1', 'actor-1', 'interactive', 'actor_private', 'ready', 1, 1)", [])?;
            connection.execute("INSERT INTO runs(id, actor_id, work_item_id, state, lease_generation, observed_sequence, created_at, updated_at) VALUES ('run-1', 'actor-1', 'work-1', 'active', 1, 0, 1, 1)", [])?;
            connection.execute("INSERT INTO tool_attempts(id, run_id, tool_call_id, tool_name, arguments_json, capabilities_json, state, created_at, updated_at) VALUES ('attempt-1', 'run-1', 'call-1', 'file', '{}', '{}', 'running', 1, 1)", [])?;
            Ok(())
        }).await.map_err(super::map_call_error)?;
        for index in 0..8 {
            store
                .begin_staging(
                    BeginArtifact {
                        id: ArtifactId::new(),
                        actor_id: ActorId::from_string("actor-1"),
                        attempt_id: AttemptId::from_string("attempt-1"),
                        managed_path: root.join(format!("stage-{index}")),
                        display_name: "file".into(),
                        media_type: "application/octet-stream".into(),
                        size: 256 * 1024 * 1024,
                        caption: None,
                        owner: format!("owner-{index}"),
                        lease_until: Timestamp(100),
                    },
                    Timestamp(1),
                )
                .await?;
        }
        let error = store
            .begin_staging(
                BeginArtifact {
                    id: ArtifactId::new(),
                    actor_id: ActorId::from_string("actor-1"),
                    attempt_id: AttemptId::from_string("attempt-1"),
                    managed_path: root.join("over"),
                    display_name: "file".into(),
                    media_type: "application/octet-stream".into(),
                    size: 1,
                    caption: None,
                    owner: "owner-over".into(),
                    lease_until: Timestamp(100),
                },
                Timestamp(1),
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("2 GiB"));
        drop(store);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }
}
