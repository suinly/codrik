use std::{
    path::{Path, PathBuf},
    time::UNIX_EPOCH,
};

use anyhow::{Context, Result, bail};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::{
    agent::tool::{ToolArtifact, ToolExecution},
    runtime::{
        model::{ArtifactId, AttemptId, Clock, Timestamp},
        store::{
            ArtifactLease, AttachedRun, AttemptOutcome, BeginArtifact, DurableToolExecution,
            ManagedArtifact, RuntimeStore,
        },
    },
};

pub const MAX_ARTIFACT_BYTES: u64 = 256 * 1024 * 1024;
pub const MAX_ACTOR_ARTIFACT_BYTES: u64 = 2 * 1024 * 1024 * 1024;

async fn validate_source(path: &Path) -> Result<u64> {
    let metadata = tokio::fs::symlink_metadata(path).await?;
    if !metadata.file_type().is_file() {
        bail!("artifact source must be a regular file");
    }
    if metadata.len() > MAX_ARTIFACT_BYTES {
        bail!("artifact exceeds the 256 MiB per-file limit");
    }
    Ok(metadata.len())
}

#[derive(Clone)]
pub struct ArtifactManager<S, C> {
    root: PathBuf,
    store: S,
    clock: C,
}

impl<S, C> ArtifactManager<S, C>
where
    S: RuntimeStore + Clone,
    C: Clock,
{
    pub fn new(root: impl Into<PathBuf>, store: S, clock: C) -> Self {
        Self {
            root: root.into(),
            store,
            clock,
        }
    }

    pub async fn stage_execution(
        &self,
        run: &AttachedRun,
        attempt: &AttemptId,
        execution: ToolExecution,
    ) -> Result<DurableToolExecution> {
        let mut artifacts = Vec::with_capacity(execution.artifacts.len());
        let mut leases = Vec::with_capacity(execution.artifacts.len());
        for raw in execution.artifacts {
            let ToolArtifact::File(file) = raw;
            let size = validate_source(&file.path).await?;
            let id = ArtifactId::new();
            let owner = uuid::Uuid::new_v4().to_string();
            let actor_dir = self.root.join(actor_directory(run.lease.actor_id.as_str()));
            let staging_dir = actor_dir.join(".staging");
            ensure_private_directory(&self.root).await?;
            ensure_private_directory(&actor_dir).await?;
            ensure_private_directory(&staging_dir).await?;
            let partial = staging_dir.join(format!("{}.partial", id.as_str()));
            let now = self.clock.now();
            let lease = self
                .store
                .begin_staging(
                    BeginArtifact {
                        id: id.clone(),
                        actor_id: run.lease.actor_id.clone(),
                        attempt_id: attempt.clone(),
                        managed_path: partial.clone(),
                        display_name: file.display_name.clone(),
                        media_type: file.media_type.clone(),
                        size,
                        caption: file.caption.clone(),
                        owner,
                        lease_until: now.plus_millis(30_000),
                    },
                    now,
                )
                .await?;
            let (sha256, lease) = self.copy_and_hash(&file.path, &partial, lease).await?;
            let final_path = actor_dir.join(&sha256);
            if tokio::fs::try_exists(&final_path).await? {
                let metadata = tokio::fs::symlink_metadata(&final_path).await?;
                if !metadata.file_type().is_file() || metadata.len() != size {
                    bail!("managed artifact collision for {sha256}");
                }
                tokio::fs::remove_file(&partial).await?;
            } else {
                tokio::fs::rename(&partial, &final_path).await?;
            }
            sync_directory(&actor_dir).await?;
            artifacts.push(ManagedArtifact {
                id,
                managed_path: final_path,
                display_name: file.display_name,
                media_type: file.media_type,
                size,
                sha256,
                caption: file.caption,
            });
            leases.push(lease);
        }
        let durable = DurableToolExecution {
            observation: execution.observation,
            artifacts,
        };
        self.store
            .commit_staged_execution(run, attempt, durable, &leases, self.clock.now())
            .await?;
        match self.store.recover_attempt(attempt).await? {
            crate::runtime::store::AttemptRecovery::Terminal(AttemptOutcome::Succeeded {
                execution,
            }) => Ok(execution),
            _ => bail!("committed artifact execution is not recoverable"),
        }
    }

    async fn copy_and_hash(
        &self,
        source: &Path,
        destination: &Path,
        mut lease: ArtifactLease,
    ) -> Result<(String, ArtifactLease)> {
        let mut input = open_source_no_follow(source).await?;
        let metadata = input.metadata().await?;
        if !metadata.is_file() || metadata.len() > MAX_ARTIFACT_BYTES {
            bail!("artifact source must be a regular file no larger than 256 MiB");
        }
        let mut options = tokio::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            options.mode(0o600);
        }
        let mut output = options.open(destination).await?;
        let mut hash = Sha256::new();
        let mut copied = 0_u64;
        let mut since_renewal = 0_u64;
        let mut buffer = vec![0_u8; 64 * 1024];
        loop {
            let count = input.read(&mut buffer).await?;
            if count == 0 {
                break;
            }
            copied = copied.saturating_add(count as u64);
            since_renewal += count as u64;
            if copied > MAX_ARTIFACT_BYTES || copied > metadata.len() {
                bail!("artifact changed or exceeded 256 MiB while copying");
            }
            output.write_all(&buffer[..count]).await?;
            hash.update(&buffer[..count]);
            if since_renewal >= 8 * 1024 * 1024 {
                lease = self
                    .store
                    .renew_staging(&lease, self.clock.now().plus_millis(30_000))
                    .await?;
                since_renewal = 0;
            }
        }
        if copied != metadata.len() {
            bail!("artifact changed while copying");
        }
        output.sync_all().await?;
        Ok((format!("{:x}", hash.finalize()), lease))
    }

    pub async fn collect_garbage(&self, now: Timestamp) -> Result<()> {
        for expired in self.store.claim_expired_staging(now, 256).await? {
            if self.store.remove_claimed_staging(&expired).await? {
                match tokio::fs::remove_file(&expired.managed_path).await {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error.into()),
                }
            }
        }
        let root = self.root.clone();
        let files = tokio::task::spawn_blocking(move || orphan_candidates(&root)).await??;
        for path in files {
            if self.store.artifact_path_exists(&path).await? {
                continue;
            }
            let metadata = tokio::fs::symlink_metadata(&path).await?;
            if !metadata.file_type().is_file() || modified_millis(&metadata)? > now.0 - 3_600_000 {
                continue;
            }
            if !self.store.artifact_path_exists(&path).await? {
                tokio::fs::remove_file(path).await?;
            }
        }
        Ok(())
    }
}

fn actor_directory(actor: &str) -> String {
    format!("{:x}", Sha256::digest(actor.as_bytes()))
}

async fn open_source_no_follow(path: &Path) -> Result<tokio::fs::File> {
    let mut options = tokio::fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        options.custom_flags(libc::O_NOFOLLOW);
    }
    options
        .open(path)
        .await
        .context("failed to open artifact source without following symlinks")
}

async fn sync_directory(path: &Path) -> Result<()> {
    let path = path.to_owned();
    tokio::task::spawn_blocking(move || std::fs::File::open(path)?.sync_all()).await??;
    Ok(())
}

async fn ensure_private_directory(path: &Path) -> Result<()> {
    tokio::fs::create_dir_all(path).await?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        tokio::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).await?;
    }
    Ok(())
}

fn orphan_candidates(root: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    if !root.exists() {
        return Ok(files);
    }
    for actor in std::fs::read_dir(root)? {
        let actor = actor?.path();
        if !actor.is_dir() {
            continue;
        }
        for entry in std::fs::read_dir(actor)? {
            let path = entry?.path();
            if path.is_file() {
                files.push(path);
            }
        }
    }
    Ok(files)
}

fn modified_millis(metadata: &std::fs::Metadata) -> Result<i64> {
    Ok(i64::try_from(
        metadata.modified()?.duration_since(UNIX_EPOCH)?.as_millis(),
    )?)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use anyhow::Result;

    use crate::{
        agent::tool::{FileArtifact, ToolArtifact, ToolCapabilities, ToolExecution},
        auth::{LegacyActor, LegacyAuthorizationSnapshot, LegacyIdentity},
        runtime::{
            model::{AttemptId, Audience, ManualClock, Timestamp},
            sqlite::SqliteRuntimeStore,
            store::{
                ArtifactStore, AttemptOutcome, AttemptRecovery, CheckpointRun, CheckpointStore,
                DispatchStore, IngressStore, NewInboundEvent, NewToolAttempt,
                RuntimeAuthorizationStore, ToolAttemptStore,
            },
        },
    };

    use super::{ArtifactManager, MAX_ARTIFACT_BYTES, validate_source};

    #[tokio::test]
    async fn regular_file_passes_preflight() -> Result<()> {
        let path = temp_path("regular");
        tokio::fs::write(&path, b"artifact").await?;

        assert_eq!(validate_source(&path).await?, 8);

        tokio::fs::remove_file(path).await?;
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn symlink_source_is_rejected() -> Result<()> {
        let source = temp_path("source");
        let link = temp_path("link");
        tokio::fs::write(&source, b"artifact").await?;
        std::os::unix::fs::symlink(&source, &link)?;

        let error = validate_source(&link).await.unwrap_err();

        assert!(error.to_string().contains("regular file"));
        tokio::fs::remove_file(link).await?;
        tokio::fs::remove_file(source).await?;
        Ok(())
    }

    #[tokio::test]
    async fn oversized_source_is_rejected_during_preflight() -> Result<()> {
        let path = temp_path("oversized");
        let file = std::fs::File::create(&path)?;
        file.set_len(MAX_ARTIFACT_BYTES + 1)?;

        let error = validate_source(&path).await.unwrap_err();

        assert!(error.to_string().contains("256 MiB"));
        tokio::fs::remove_file(path).await?;
        Ok(())
    }

    #[tokio::test]
    async fn stage_execution_copies_to_content_addressed_durable_file() -> Result<()> {
        let fixture = ArtifactFixture::new().await?;
        let source = fixture.root.join("source.txt");
        tokio::fs::write(&source, b"artifact").await?;
        let attempt = fixture.running_attempt("call-1").await?;

        let durable = fixture
            .manager
            .stage_execution(
                &fixture.run,
                &attempt,
                file_execution(&source, "report.txt"),
            )
            .await?;

        assert_eq!(
            tokio::fs::read(&durable.artifacts[0].managed_path).await?,
            b"artifact"
        );
        assert_eq!(
            durable.artifacts[0]
                .managed_path
                .file_name()
                .unwrap()
                .to_string_lossy(),
            durable.artifacts[0].sha256
        );
        assert!(
            durable.artifacts[0]
                .managed_path
                .starts_with(&fixture.managed)
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(durable.artifacts[0].managed_path.parent().unwrap())?
                    .permissions()
                    .mode()
                    & 0o777,
                0o700
            );
            assert_eq!(
                std::fs::metadata(&durable.artifacts[0].managed_path)?
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn identical_content_reuses_referenced_row_and_path_across_attempts() -> Result<()> {
        let fixture = ArtifactFixture::new().await?;
        let source = fixture.root.join("same.bin");
        tokio::fs::write(&source, b"same bytes").await?;
        let first = fixture.running_attempt("call-1").await?;
        let second = fixture.running_attempt("call-2").await?;

        let first_outcome = fixture
            .manager
            .stage_execution(&fixture.run, &first, file_execution(&source, "first.bin"))
            .await?;
        let second_outcome = fixture
            .manager
            .stage_execution(&fixture.run, &second, file_execution(&source, "second.bin"))
            .await?;

        assert_eq!(
            first_outcome.artifacts[0].id,
            second_outcome.artifacts[0].id
        );
        assert_eq!(
            first_outcome.artifacts[0].managed_path,
            second_outcome.artifacts[0].managed_path
        );
        assert_eq!(second_outcome.artifacts[0].display_name, "second.bin");
        let rows = fixture.store.artifact_row_probe().await?;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].1, None);
        assert!(matches!(
            fixture.store.recover_attempt(&second).await?,
            AttemptRecovery::Terminal(AttemptOutcome::Succeeded { .. })
        ));
        Ok(())
    }

    #[tokio::test]
    async fn gc_preserves_live_lease_and_removes_it_after_expiry() -> Result<()> {
        let fixture = ArtifactFixture::new().await?;
        let attempt = fixture.running_attempt("call-gc").await?;
        let partial = fixture.managed.join("active.partial");
        tokio::fs::create_dir_all(&fixture.managed).await?;
        tokio::fs::write(&partial, b"partial").await?;
        fixture
            .store
            .begin_staging(
                crate::runtime::store::BeginArtifact {
                    id: crate::runtime::model::ArtifactId::new(),
                    actor_id: fixture.run.lease.actor_id.clone(),
                    attempt_id: attempt,
                    managed_path: partial.clone(),
                    display_name: "partial".into(),
                    media_type: "application/octet-stream".into(),
                    size: 7,
                    caption: None,
                    owner: "stager".into(),
                    lease_until: Timestamp(100),
                },
                Timestamp(10),
            )
            .await?;

        fixture.manager.collect_garbage(Timestamp(99)).await?;
        assert!(partial.exists());
        fixture.manager.collect_garbage(Timestamp(101)).await?;
        assert!(!partial.exists());
        Ok(())
    }

    #[tokio::test]
    async fn actor_quota_rejects_more_than_two_gib_of_staging() -> Result<()> {
        let fixture = ArtifactFixture::new().await?;
        let attempt = fixture.running_attempt("call-quota").await?;
        for index in 0..8 {
            fixture
                .store
                .begin_staging(
                    crate::runtime::store::BeginArtifact {
                        id: crate::runtime::model::ArtifactId::new(),
                        actor_id: fixture.run.lease.actor_id.clone(),
                        attempt_id: attempt.clone(),
                        managed_path: fixture.managed.join(format!("quota-{index}")),
                        display_name: "quota".into(),
                        media_type: "application/octet-stream".into(),
                        size: MAX_ARTIFACT_BYTES,
                        caption: None,
                        owner: format!("owner-{index}"),
                        lease_until: Timestamp(100),
                    },
                    Timestamp(10),
                )
                .await?;
        }
        let error = fixture
            .store
            .begin_staging(
                crate::runtime::store::BeginArtifact {
                    id: crate::runtime::model::ArtifactId::new(),
                    actor_id: fixture.run.lease.actor_id.clone(),
                    attempt_id: attempt,
                    managed_path: fixture.managed.join("over"),
                    display_name: "over".into(),
                    media_type: "application/octet-stream".into(),
                    size: 1,
                    caption: None,
                    owner: "over".into(),
                    lease_until: Timestamp(100),
                },
                Timestamp(10),
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("2 GiB"));
        Ok(())
    }

    #[tokio::test]
    async fn commit_refuses_database_reference_before_managed_file_exists() -> Result<()> {
        let fixture = ArtifactFixture::new().await?;
        let attempt = fixture.running_attempt("call-missing").await?;
        let id = crate::runtime::model::ArtifactId::new();
        let missing = fixture.managed.join("missing-hash");
        let lease = fixture
            .store
            .begin_staging(
                crate::runtime::store::BeginArtifact {
                    id: id.clone(),
                    actor_id: fixture.run.lease.actor_id.clone(),
                    attempt_id: attempt.clone(),
                    managed_path: fixture.managed.join("missing.partial"),
                    display_name: "missing".into(),
                    media_type: "application/octet-stream".into(),
                    size: 1,
                    caption: None,
                    owner: "stager".into(),
                    lease_until: Timestamp(100),
                },
                Timestamp(10),
            )
            .await?;
        let error = fixture
            .store
            .commit_staged_execution(
                &fixture.run,
                &attempt,
                crate::runtime::store::DurableToolExecution {
                    observation: "created".into(),
                    artifacts: vec![crate::runtime::store::ManagedArtifact {
                        id,
                        managed_path: missing,
                        display_name: "missing".into(),
                        media_type: "application/octet-stream".into(),
                        size: 1,
                        sha256: "0".repeat(64),
                        caption: None,
                    }],
                },
                &[lease],
                Timestamp(11),
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("not durable"));
        assert_eq!(
            fixture.store.recover_attempt(&attempt).await?,
            AttemptRecovery::OutcomeUnknown
        );
        Ok(())
    }

    #[tokio::test]
    async fn gc_removes_old_orphan_after_two_database_checks() -> Result<()> {
        let fixture = ArtifactFixture::new().await?;
        let actor_dir = fixture.managed.join("orphan-actor");
        tokio::fs::create_dir_all(&actor_dir).await?;
        let orphan = actor_dir.join("orphan-hash");
        tokio::fs::write(&orphan, b"orphan").await?;
        let future = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis() as i64
            + 3_700_000;

        fixture.manager.collect_garbage(Timestamp(future)).await?;

        assert!(!orphan.exists());
        Ok(())
    }

    struct ArtifactFixture {
        root: PathBuf,
        managed: PathBuf,
        store: SqliteRuntimeStore,
        manager: ArtifactManager<SqliteRuntimeStore, ManualClock>,
        run: crate::runtime::store::AttachedRun,
    }

    impl ArtifactFixture {
        async fn new() -> Result<Self> {
            let root = temp_path("fixture");
            let managed = root.join("managed");
            tokio::fs::create_dir_all(&root).await?;
            let store = SqliteRuntimeStore::open(root.join("runtime.sqlite3")).await?;
            store
                .import_legacy_authorization(
                    LegacyAuthorizationSnapshot {
                        version: 1,
                        actors: vec![LegacyActor {
                            id: "actor:artifact:1".into(),
                            enabled: true,
                            tools: vec!["files".into()],
                            identities: vec![LegacyIdentity {
                                provider: "local".into(),
                                subject: "owner".into(),
                                username: None,
                            }],
                        }],
                    },
                    Timestamp(1),
                )
                .await?;
            store
                .ingest(
                    NewInboundEvent::text(
                        "local",
                        "event-1",
                        "local",
                        "owner",
                        Audience::ActorPrivate,
                        "make file",
                    )?,
                    Timestamp(2),
                )
                .await?;
            let lease = store
                .acquire_ready_actor("worker", Timestamp(3), Timestamp(10_000))
                .await?
                .unwrap();
            let run = store
                .attach_next_run(&lease, 8, Timestamp(4))
                .await?
                .unwrap();
            store
                .checkpoint_run(
                    CheckpointRun {
                        run: run.clone(),
                        incorporated_event_ids: run.source_event_ids.clone(),
                        checkpointed_attempt_ids: Vec::new(),
                        messages: run.messages.clone(),
                    },
                    Timestamp(5),
                )
                .await?;
            let manager = ArtifactManager::new(&managed, store.clone(), ManualClock::new(10));
            Ok(Self {
                root,
                managed,
                store,
                manager,
                run,
            })
        }

        async fn running_attempt(&self, call: &str) -> Result<AttemptId> {
            let attempt = self
                .store
                .prepare_attempt(
                    &self.run,
                    NewToolAttempt {
                        id: AttemptId::new(),
                        tool_call_id: call.into(),
                        tool_name: "files".into(),
                        arguments_json: "{}".into(),
                        capabilities: ToolCapabilities::read_only(),
                    },
                    Timestamp(6),
                )
                .await?;
            self.store
                .mark_attempt_running(&self.run, &attempt.id, Timestamp(7))
                .await?;
            Ok(attempt.id)
        }
    }

    impl Drop for ArtifactFixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.root);
        }
    }

    fn file_execution(path: &std::path::Path, display_name: &str) -> ToolExecution {
        ToolExecution {
            observation: "created".into(),
            artifacts: vec![ToolArtifact::File(FileArtifact {
                path: path.into(),
                display_name: display_name.into(),
                media_type: "application/octet-stream".into(),
                caption: None,
            })],
        }
    }

    fn temp_path(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "codrik-artifact-{label}-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4()
        ))
    }
}
