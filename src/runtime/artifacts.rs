use std::{
    collections::{BTreeSet, HashMap},
    path::{Path, PathBuf},
    sync::{Arc, Weak},
    time::UNIX_EPOCH,
};

use anyhow::{Context, Result, bail};
use sha2::{Digest, Sha256};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::{Mutex, OwnedMutexGuard},
};

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

pub async fn remove_deleted_artifacts(root: &Path, paths: &[PathBuf]) -> usize {
    if paths.is_empty() {
        return 0;
    }
    let root_is_real_directory = tokio::fs::symlink_metadata(root)
        .await
        .is_ok_and(|metadata| metadata.is_dir() && !metadata.file_type().is_symlink());
    let Ok(root) = tokio::fs::canonicalize(root).await else {
        eprintln!(r#"{{"component":"actor-admin","transition":"artifact_cleanup_failed"}}"#);
        return 0;
    };
    if !root_is_real_directory {
        eprintln!(r#"{{"component":"actor-admin","transition":"artifact_cleanup_failed"}}"#);
        return 0;
    }
    let mut removed = 0;
    for path in paths {
        let result = async {
            validate_confined_path(&root, path, true).await?;
            match tokio::fs::remove_file(path).await {
                Ok(()) => removed += 1,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
            Result::<()>::Ok(())
        }
        .await;
        if result.is_err() {
            eprintln!(r#"{{"component":"actor-admin","transition":"artifact_cleanup_failed"}}"#);
        }
    }
    removed
}

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
    path_locks: Arc<Mutex<HashMap<PathBuf, Weak<Mutex<()>>>>>,
    #[cfg(test)]
    test_copy_delay: Option<std::time::Duration>,
    #[cfg(test)]
    test_copy_failure: bool,
    #[cfg(test)]
    test_gc_pause: Option<Arc<TestPause>>,
    #[cfg(test)]
    test_claim_pause: Option<Arc<TestPause>>,
    #[cfg(test)]
    test_after_claim_sync_pause: Option<Arc<TestPause>>,
}

#[cfg(test)]
pub(crate) struct TestPause {
    entered: tokio::sync::Notify,
    resume: tokio::sync::Notify,
}

#[cfg(test)]
impl TestPause {
    pub(crate) fn new() -> Self {
        Self {
            entered: tokio::sync::Notify::new(),
            resume: tokio::sync::Notify::new(),
        }
    }

    pub(crate) async fn wait_until_entered(&self) {
        self.entered.notified().await;
    }

    pub(crate) fn resume(&self) {
        self.resume.notify_one();
    }
}

struct PreparedArtifact {
    artifact: ManagedArtifact,
    lease: ArtifactLease,
    partial: PathBuf,
}

impl<S, C> ArtifactManager<S, C>
where
    S: RuntimeStore + Clone + 'static,
    C: Clock,
{
    pub fn new(root: impl Into<PathBuf>, store: S, clock: C) -> Self {
        Self {
            root: root.into(),
            store,
            clock,
            path_locks: Arc::new(Mutex::new(HashMap::new())),
            #[cfg(test)]
            test_copy_delay: None,
            #[cfg(test)]
            test_copy_failure: false,
            #[cfg(test)]
            test_gc_pause: None,
            #[cfg(test)]
            test_claim_pause: None,
            #[cfg(test)]
            test_after_claim_sync_pause: None,
        }
    }

    pub(crate) fn store(&self) -> S {
        self.store.clone()
    }

    pub(crate) fn clock(&self) -> C {
        self.clock.clone()
    }

    pub async fn stage_execution(
        &self,
        run: &AttachedRun,
        attempt: &AttemptId,
        execution: ToolExecution,
    ) -> Result<DurableToolExecution> {
        let root = self.prepare_root().await?;
        let actor_dir =
            create_private_child(&root, &actor_directory(run.lease.actor_id.as_str())).await?;
        let staging_dir = create_private_child(&actor_dir, ".staging").await?;
        let mut prepared = Vec::with_capacity(execution.artifacts.len());
        for raw in execution.artifacts {
            let ToolArtifact::File(file) = raw;
            let size = validate_source(&file.path).await?;
            let id = ArtifactId::new();
            let owner = uuid::Uuid::new_v4().to_string();
            let partial = staging_dir.join(format!("{}.partial", id.as_str()));
            validate_confined_path(&root, &partial, true).await?;
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
            validate_confined_path(&root, &final_path, true).await?;
            prepared.push(PreparedArtifact {
                artifact: ManagedArtifact {
                    id,
                    managed_path: final_path,
                    display_name: file.display_name,
                    media_type: file.media_type,
                    size,
                    sha256,
                    caption: file.caption,
                },
                lease,
                partial,
            });
        }

        // The daemon's InstanceLock guarantees one process authority. This registry coordinates
        // all canonical-path mutations inside that process, from validation through DB commit.
        let paths = prepared
            .iter()
            .map(|item| item.artifact.managed_path.clone())
            .collect::<BTreeSet<_>>();
        let mut path_guards = Vec::with_capacity(paths.len());
        for path in paths {
            path_guards.push(self.lock_path(path).await);
        }
        for item in &prepared {
            if let Some(existing) = self
                .store
                .referenced_artifact(
                    &run.lease.actor_id,
                    &item.artifact.sha256,
                    item.artifact.size,
                )
                .await?
            {
                if existing.managed_path != item.artifact.managed_path {
                    bail!("referenced artifact is outside its canonical path");
                }
                validate_canonical_file(
                    &root,
                    &existing.managed_path,
                    item.artifact.size,
                    &item.artifact.sha256,
                )
                .await?;
                tokio::fs::remove_file(&item.partial).await?;
            } else if tokio::fs::try_exists(&item.artifact.managed_path).await? {
                validate_canonical_file(
                    &root,
                    &item.artifact.managed_path,
                    item.artifact.size,
                    &item.artifact.sha256,
                )
                .await?;
                tokio::fs::remove_file(&item.partial).await?;
            } else {
                tokio::fs::rename(&item.partial, &item.artifact.managed_path).await?;
            }
        }
        sync_directory(&actor_dir).await?;
        let artifacts = prepared.iter().map(|item| item.artifact.clone()).collect();
        let leases = prepared
            .iter()
            .map(|item| item.lease.clone())
            .collect::<Vec<_>>();
        let durable = DurableToolExecution {
            observation: execution.observation,
            artifacts,
        };
        self.store
            .commit_staged_execution(run, attempt, durable, &leases, self.clock.now())
            .await?;
        drop(path_guards);
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
        lease: ArtifactLease,
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
        let lease = Arc::new(Mutex::new(lease));
        let (stop_tx, mut stop_rx) = tokio::sync::oneshot::channel();
        let heartbeat_store = self.store.clone();
        let heartbeat_clock = self.clock.clone();
        let heartbeat_lease = lease.clone();
        let heartbeat = tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(10));
            interval.tick().await;
            loop {
                tokio::select! {
                    _ = &mut stop_rx => return Ok::<(), anyhow::Error>(()),
                    _ = interval.tick() => {
                        let current = heartbeat_lease.lock().await.clone();
                        let renewed = heartbeat_store.renew_staging(
                            &current, heartbeat_clock.now().plus_millis(30_000)).await?;
                        *heartbeat_lease.lock().await = renewed;
                    }
                }
            }
        });
        let copy_result: Result<String> = async {
            let mut hash = Sha256::new();
            let mut copied = 0_u64;
            let mut buffer = vec![0_u8; 64 * 1024];
            loop {
                let count = input.read(&mut buffer).await?;
                if count == 0 {
                    break;
                }
                copied = copied.saturating_add(count as u64);
                if copied > MAX_ARTIFACT_BYTES || copied > metadata.len() {
                    bail!("artifact changed or exceeded 256 MiB while copying");
                }
                output.write_all(&buffer[..count]).await?;
                hash.update(&buffer[..count]);
                #[cfg(test)]
                if self.test_copy_failure {
                    bail!("injected artifact copy failure");
                }
                #[cfg(test)]
                if let Some(delay) = self.test_copy_delay {
                    tokio::time::sleep(delay).await;
                }
            }
            if copied != metadata.len() {
                bail!("artifact changed while copying");
            }
            output.sync_all().await?;
            Ok(format!("{:x}", hash.finalize()))
        }
        .await;
        let _ = stop_tx.send(());
        let heartbeat_result = heartbeat.await?;
        let sha256 = copy_result?;
        heartbeat_result?;
        let lease = lease.lock().await.clone();
        Ok((sha256, lease))
    }

    pub async fn collect_garbage(&self, now: Timestamp) -> Result<()> {
        let root = self.prepare_root().await?;
        for expired in self.store.claim_expired_staging(now, 256).await? {
            validate_confined_path(&root, &expired.managed_path, true).await?;
            let _guard = self.lock_path(expired.managed_path.clone()).await;
            #[cfg(test)]
            if let Some(pause) = &self.test_claim_pause {
                pause.entered.notify_one();
                pause.resume.notified().await;
            }
            let claim_now = self.clock.now();
            if !self
                .store
                .renew_gc_claim(&expired, claim_now, claim_now.plus_millis(30_000))
                .await?
            {
                continue;
            }
            match tokio::fs::remove_file(&expired.managed_path).await {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
            sync_directory(expired.managed_path.parent().unwrap()).await?;
            #[cfg(test)]
            if let Some(pause) = &self.test_after_claim_sync_pause {
                pause.entered.notify_one();
                pause.resume.notified().await;
            }
            if !self.store.complete_claimed_staging(&expired).await? {
                bail!("lost artifact GC claim before completion");
            }
        }
        let scan_root = root.clone();
        let files = tokio::task::spawn_blocking(move || orphan_candidates(&scan_root)).await??;
        let cutoff = now.0.saturating_sub(3_600_000);
        for path in files {
            validate_confined_path(&root, &path, false).await?;
            if self.store.artifact_path_exists(&path).await? {
                continue;
            }
            let metadata = tokio::fs::symlink_metadata(&path).await?;
            if !metadata.file_type().is_file() || modified_millis(&metadata)? >= cutoff {
                continue;
            }
            #[cfg(test)]
            if let Some(pause) = &self.test_gc_pause {
                pause.entered.notify_one();
                pause.resume.notified().await;
            }
            let _guard = self.lock_path(path.clone()).await;
            if !self.store.artifact_path_exists(&path).await? {
                tokio::fs::remove_file(path).await?;
            }
        }
        Ok(())
    }

    async fn lock_path(&self, path: PathBuf) -> OwnedMutexGuard<()> {
        let lock = {
            let mut locks = self.path_locks.lock().await;
            locks.retain(|_, value| value.strong_count() > 0);
            if let Some(lock) = locks.get(&path).and_then(Weak::upgrade) {
                lock
            } else {
                let lock = Arc::new(Mutex::new(()));
                locks.insert(path, Arc::downgrade(&lock));
                lock
            }
        };
        lock.lock_owned().await
    }

    async fn prepare_root(&self) -> Result<PathBuf> {
        reject_parent_components(&self.root)?;
        tokio::fs::create_dir_all(&self.root).await?;
        let metadata = tokio::fs::symlink_metadata(&self.root).await?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            bail!("artifact root must be a real directory");
        }
        let root = tokio::fs::canonicalize(&self.root).await?;
        ensure_private_directory(&root).await?;
        Ok(root)
    }

    #[cfg(test)]
    fn with_test_copy_delay(mut self, delay: std::time::Duration) -> Self {
        self.test_copy_delay = Some(delay);
        self
    }

    #[cfg(test)]
    fn with_test_copy_failure(mut self) -> Self {
        self.test_copy_failure = true;
        self
    }

    #[cfg(test)]
    pub(crate) fn with_test_gc_pause(mut self, pause: Arc<TestPause>) -> Self {
        self.test_gc_pause = Some(pause);
        self
    }

    #[cfg(test)]
    fn with_test_claim_pause(mut self, pause: Arc<TestPause>) -> Self {
        self.test_claim_pause = Some(pause);
        self
    }

    #[cfg(test)]
    fn with_test_after_claim_sync_pause(mut self, pause: Arc<TestPause>) -> Self {
        self.test_after_claim_sync_pause = Some(pause);
        self
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
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        tokio::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).await?;
    }
    Ok(())
}

async fn create_private_child(parent: &Path, name: &str) -> Result<PathBuf> {
    let path = parent.join(name);
    match tokio::fs::symlink_metadata(&path).await {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            bail!("artifact path component is a symlink")
        }
        Ok(metadata) if !metadata.is_dir() => bail!("artifact path component is not a directory"),
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            tokio::fs::create_dir(&path).await?
        }
        Err(error) => return Err(error.into()),
    }
    ensure_private_directory(&path).await?;
    Ok(path)
}

fn reject_parent_components(path: &Path) -> Result<()> {
    if path
        .components()
        .any(|component| matches!(component, std::path::Component::ParentDir))
    {
        bail!("artifact path containing '..' is not confined");
    }
    Ok(())
}

async fn validate_confined_path(root: &Path, path: &Path, allow_missing_leaf: bool) -> Result<()> {
    reject_parent_components(path)?;
    if !path.is_absolute() || !path.starts_with(root) || path == root {
        bail!("managed path is not confined to artifact root");
    }
    let relative = path.strip_prefix(root)?;
    let components = relative.components().collect::<Vec<_>>();
    let mut current = root.to_owned();
    for (index, component) in components.iter().enumerate() {
        let std::path::Component::Normal(component) = component else {
            bail!("invalid managed path component");
        };
        current.push(component);
        match tokio::fs::symlink_metadata(&current).await {
            Ok(metadata) => {
                if metadata.file_type().is_symlink() {
                    bail!("managed path component is a symlink");
                }
                if index + 1 < components.len() && !metadata.is_dir() {
                    bail!("managed path parent is not a directory");
                }
            }
            Err(error)
                if error.kind() == std::io::ErrorKind::NotFound
                    && allow_missing_leaf
                    && index + 1 == components.len() => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

async fn validate_canonical_file(root: &Path, path: &Path, size: u64, sha256: &str) -> Result<()> {
    validate_confined_path(root, path, false).await?;
    let metadata = tokio::fs::symlink_metadata(path).await?;
    if !metadata.file_type().is_file() || metadata.len() != size {
        bail!("canonical artifact size mismatch");
    }
    let mut file = open_source_no_follow(path).await?;
    let mut hash = Sha256::new();
    let mut buffer = vec![0; 64 * 1024];
    loop {
        let count = file.read(&mut buffer).await?;
        if count == 0 {
            break;
        }
        hash.update(&buffer[..count]);
    }
    if format!("{:x}", hash.finalize()) != sha256 {
        bail!("canonical artifact hash mismatch");
    }
    Ok(())
}

fn orphan_candidates(root: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    if !root.exists() {
        return Ok(files);
    }
    for actor in std::fs::read_dir(root)? {
        let actor = actor?;
        let actor_type = actor.file_type()?;
        let actor = actor.path();
        if actor_type.is_symlink() || !actor_type.is_dir() {
            continue;
        }
        for entry in std::fs::read_dir(actor)? {
            let entry = entry?;
            let entry_type = entry.file_type()?;
            let path = entry.path();
            if entry_type.is_symlink() {
                continue;
            }
            if entry_type.is_file() {
                files.push(path);
            } else if entry_type.is_dir() && entry.file_name() == ".staging" {
                for partial in std::fs::read_dir(path)? {
                    let partial = partial?;
                    if partial.file_type()?.is_file() {
                        files.push(partial.path());
                    }
                }
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
    use std::{path::PathBuf, sync::Arc};

    use anyhow::Result;
    use sha2::Digest;

    use crate::{
        agent::tool::{FileArtifact, ToolArtifact, ToolCapabilities, ToolExecution},
        runtime::{
            model::{AttemptId, Audience, Clock, ManualClock, Timestamp},
            sqlite::SqliteRuntimeStore,
            store::{
                ArtifactStore, AttemptOutcome, AttemptRecovery, CheckpointRun, CheckpointStore,
                DispatchStore, IngressStore, NewInboundEvent, NewToolAttempt, ToolAttemptStore,
            },
        },
        test_fixtures::{ActorSeed, ActorSeedSet, IdentitySeed},
    };

    use super::{
        ArtifactManager, MAX_ARTIFACT_BYTES, TestPause, remove_deleted_artifacts, validate_source,
    };

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
    async fn actor_quota_does_not_count_unretained_staging() -> Result<()> {
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
        fixture
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
            .await?;
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

    #[cfg(unix)]
    #[tokio::test]
    async fn gc_does_not_follow_or_remove_symlinked_orphans() -> Result<()> {
        let fixture = ArtifactFixture::new().await?;
        let actor_dir = fixture.managed.join("orphan-actor");
        tokio::fs::create_dir_all(&actor_dir).await?;
        let outside = fixture.root.join("outside");
        tokio::fs::write(&outside, b"outside").await?;
        let link = actor_dir.join("orphan-link");
        std::os::unix::fs::symlink(&outside, &link)?;
        let future = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis() as i64
            + 3_700_000;

        fixture.manager.collect_garbage(Timestamp(future)).await?;

        assert!(link.is_symlink());
        assert_eq!(tokio::fs::read(outside).await?, b"outside");
        Ok(())
    }

    #[tokio::test]
    async fn deleted_actor_artifacts_are_removed_from_managed_root() -> Result<()> {
        let root = temp_path("deleted-actor");
        tokio::fs::create_dir_all(&root).await?;
        let root = tokio::fs::canonicalize(root).await?;
        let actor = root.join("actor");
        tokio::fs::create_dir(&actor).await?;
        let artifact = actor.join("hash");
        tokio::fs::write(&artifact, b"artifact").await?;

        assert_eq!(
            remove_deleted_artifacts(&root, &[artifact.clone()]).await,
            1
        );
        assert!(!artifact.exists());

        tokio::fs::remove_dir_all(root).await?;
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn stage_rejects_symlinked_actor_directory() -> Result<()> {
        let fixture = ArtifactFixture::new().await?;
        let source = fixture.root.join("source-symlink.txt");
        tokio::fs::write(&source, b"artifact").await?;
        let outside = fixture.root.join("outside");
        tokio::fs::create_dir_all(&outside).await?;
        tokio::fs::create_dir_all(&fixture.managed).await?;
        let actor = fixture
            .managed
            .join(super::actor_directory(fixture.run.lease.actor_id.as_str()));
        std::os::unix::fs::symlink(&outside, &actor)?;
        let attempt = fixture.running_attempt("call-symlink-dir").await?;

        let error = fixture
            .manager
            .stage_execution(
                &fixture.run,
                &attempt,
                file_execution(&source, "report.txt"),
            )
            .await
            .unwrap_err();

        assert!(error.to_string().contains("symlink") || error.to_string().contains("confined"));
        assert!(std::fs::read_dir(outside)?.next().is_none());
        Ok(())
    }

    #[tokio::test]
    async fn gc_rejects_db_crafted_path_outside_root_without_unlinking() -> Result<()> {
        let fixture = ArtifactFixture::new().await?;
        let attempt = fixture.running_attempt("call-crafted-gc").await?;
        let victim = fixture.root.join("victim.txt");
        tokio::fs::write(&victim, b"keep").await?;
        fixture
            .store
            .begin_staging(
                crate::runtime::store::BeginArtifact {
                    id: crate::runtime::model::ArtifactId::new(),
                    actor_id: fixture.run.lease.actor_id.clone(),
                    attempt_id: attempt,
                    managed_path: victim.clone(),
                    display_name: "victim".into(),
                    media_type: "text/plain".into(),
                    size: 4,
                    caption: None,
                    owner: "crafted".into(),
                    lease_until: Timestamp(10),
                },
                Timestamp(1),
            )
            .await?;

        let error = fixture
            .manager
            .collect_garbage(Timestamp(11))
            .await
            .unwrap_err();

        assert!(
            error.to_string().contains("confined") || error.to_string().contains("artifact root")
        );
        assert_eq!(tokio::fs::read(victim).await?, b"keep");
        Ok(())
    }

    #[tokio::test]
    async fn corrupt_existing_canonical_file_is_rejected() -> Result<()> {
        let fixture = ArtifactFixture::new().await?;
        let source = fixture.root.join("source-corrupt.txt");
        tokio::fs::write(&source, b"good").await?;
        let sha = format!("{:x}", sha2::Sha256::digest(b"good"));
        let actor = fixture
            .managed
            .join(super::actor_directory(fixture.run.lease.actor_id.as_str()));
        tokio::fs::create_dir_all(&actor).await?;
        tokio::fs::write(actor.join(sha), b"evil").await?;
        let attempt = fixture.running_attempt("call-corrupt").await?;

        let error = fixture
            .manager
            .stage_execution(
                &fixture.run,
                &attempt,
                file_execution(&source, "report.txt"),
            )
            .await
            .unwrap_err();

        assert!(error.to_string().contains("hash") || error.to_string().contains("collision"));
        Ok(())
    }

    #[tokio::test]
    async fn missing_or_wrong_root_canonical_database_row_cannot_checkpoint() -> Result<()> {
        for wrong_root in [false, true] {
            let fixture = ArtifactFixture::new().await?;
            let source = fixture
                .root
                .join(format!("source-missing-{wrong_root}.txt"));
            tokio::fs::write(&source, b"missing").await?;
            let sha = format!("{:x}", sha2::Sha256::digest(b"missing"));
            let expected = fixture
                .managed
                .join(super::actor_directory(fixture.run.lease.actor_id.as_str()))
                .join(&sha);
            let stored = if wrong_root {
                fixture.root.join("wrong-root").join(&sha)
            } else {
                expected
            };
            fixture
                .store
                .seed_referenced_artifact_probe(
                    fixture.run.lease.actor_id.as_str(),
                    &stored,
                    7,
                    &sha,
                )
                .await?;
            let attempt = fixture
                .running_attempt(&format!("call-missing-{wrong_root}"))
                .await?;

            let error = fixture
                .manager
                .stage_execution(
                    &fixture.run,
                    &attempt,
                    file_execution(&source, "report.txt"),
                )
                .await
                .unwrap_err();

            assert!(
                error.to_string().contains("canonical")
                    || error.to_string().contains("outside")
                    || error.to_string().contains("No such file")
            );
            assert_eq!(
                fixture.store.recover_attempt(&attempt).await?,
                AttemptRecovery::OutcomeUnknown
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn gc_scans_old_untracked_staging_partials() -> Result<()> {
        let fixture = ArtifactFixture::new().await?;
        let staging = fixture.managed.join("actor").join(".staging");
        tokio::fs::create_dir_all(&staging).await?;
        let orphan = staging.join("old.partial");
        tokio::fs::write(&orphan, b"old").await?;
        fixture.manager.collect_garbage(Timestamp(i64::MAX)).await?;
        assert!(!orphan.exists());
        Ok(())
    }

    #[tokio::test]
    async fn gc_timestamp_cutoff_saturates() -> Result<()> {
        let fixture = ArtifactFixture::new().await?;
        fixture.manager.collect_garbage(Timestamp(i64::MIN)).await?;
        Ok(())
    }

    #[tokio::test]
    async fn failed_unlink_keeps_claimed_staging_row_for_retry() -> Result<()> {
        let fixture = ArtifactFixture::new().await?;
        let attempt = fixture.running_attempt("call-unlink-fail").await?;
        let actor = fixture.managed.join("unlink-actor");
        tokio::fs::create_dir_all(&actor).await?;
        let directory = actor.join("cannot-unlink-as-file");
        tokio::fs::create_dir(&directory).await?;
        fixture
            .store
            .begin_staging(
                crate::runtime::store::BeginArtifact {
                    id: crate::runtime::model::ArtifactId::new(),
                    actor_id: fixture.run.lease.actor_id.clone(),
                    attempt_id: attempt,
                    managed_path: directory.clone(),
                    display_name: "dir".into(),
                    media_type: "x".into(),
                    size: 0,
                    caption: None,
                    owner: "stage".into(),
                    lease_until: Timestamp(10),
                },
                Timestamp(1),
            )
            .await?;
        assert!(
            fixture
                .manager
                .collect_garbage(Timestamp(11))
                .await
                .is_err()
        );
        assert!(fixture.store.artifact_path_exists(&directory).await?);
        Ok(())
    }

    #[tokio::test]
    async fn crash_after_unlink_before_complete_is_retryable() -> Result<()> {
        let fixture = ArtifactFixture::new().await?;
        let attempt = fixture.running_attempt("call-crash-cleanup").await?;
        let actor = fixture.managed.join("crash-actor");
        tokio::fs::create_dir_all(&actor).await?;
        let partial = actor.join("crash.partial");
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
                    media_type: "x".into(),
                    size: 7,
                    caption: None,
                    owner: "stage".into(),
                    lease_until: Timestamp(10),
                },
                Timestamp(1),
            )
            .await?;
        let claimed = fixture
            .store
            .claim_expired_staging(Timestamp(11), 1)
            .await?;
        tokio::fs::remove_file(&partial).await?;
        assert!(fixture.store.artifact_path_exists(&partial).await?);

        fixture.manager.collect_garbage(Timestamp(41_012)).await?;

        assert!(!fixture.store.artifact_path_exists(&partial).await?);
        assert_eq!(claimed.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn gc_does_not_unlink_after_its_claim_expires_and_is_reclaimed() -> Result<()> {
        let fixture = ArtifactFixture::new().await?;
        let attempt = fixture.running_attempt("call-claim-reclaimed").await?;
        let partial = fixture.managed.join("claim-reclaimed.partial");
        tokio::fs::write(&partial, b"still owned by the new claimant").await?;
        fixture
            .store
            .begin_staging(
                crate::runtime::store::BeginArtifact {
                    id: crate::runtime::model::ArtifactId::new(),
                    actor_id: fixture.run.lease.actor_id.clone(),
                    attempt_id: attempt,
                    managed_path: partial.clone(),
                    display_name: "partial".into(),
                    media_type: "x".into(),
                    size: 30,
                    caption: None,
                    owner: "stage".into(),
                    lease_until: Timestamp(10),
                },
                Timestamp(1),
            )
            .await?;
        let pause = Arc::new(TestPause::new());
        let manager = fixture.manager.clone().with_test_claim_pause(pause.clone());
        let gc = tokio::spawn(async move { manager.collect_garbage(Timestamp(11)).await });
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            pause.wait_until_entered(),
        )
        .await?;

        fixture.clock.advance(40_000);
        let reclaimed = fixture
            .store
            .claim_expired_staging(fixture.clock.now(), 1)
            .await?;
        assert_eq!(reclaimed.len(), 1);
        pause.resume();
        gc.await??;

        assert!(partial.exists());
        assert!(fixture.store.artifact_path_exists(&partial).await?);
        Ok(())
    }

    #[tokio::test]
    async fn missing_claimed_file_syncs_parent_before_database_completion() -> Result<()> {
        let fixture = ArtifactFixture::new().await?;
        let attempt = fixture.running_attempt("call-missing-claimed").await?;
        let partial = fixture.managed.join("already-missing.partial");
        fixture
            .store
            .begin_staging(
                crate::runtime::store::BeginArtifact {
                    id: crate::runtime::model::ArtifactId::new(),
                    actor_id: fixture.run.lease.actor_id.clone(),
                    attempt_id: attempt,
                    managed_path: partial.clone(),
                    display_name: "partial".into(),
                    media_type: "x".into(),
                    size: 1,
                    caption: None,
                    owner: "stage".into(),
                    lease_until: Timestamp(10),
                },
                Timestamp(1),
            )
            .await?;
        let pause = Arc::new(TestPause::new());
        let manager = fixture
            .manager
            .clone()
            .with_test_after_claim_sync_pause(pause.clone());
        let gc = tokio::spawn(async move { manager.collect_garbage(Timestamp(11)).await });
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            pause.wait_until_entered(),
        )
        .await?;

        assert!(fixture.store.artifact_path_exists(&partial).await?);
        pause.resume();
        gc.await??;
        assert!(!fixture.store.artifact_path_exists(&partial).await?);
        Ok(())
    }

    #[tokio::test]
    async fn orphan_modified_exactly_at_cutoff_is_retained() -> Result<()> {
        let fixture = ArtifactFixture::new().await?;
        let actor = fixture.managed.join("cutoff-actor");
        tokio::fs::create_dir_all(&actor).await?;
        let orphan = actor.join("cutoff.bin");
        tokio::fs::write(&orphan, b"boundary").await?;
        let modified = super::modified_millis(&std::fs::metadata(&orphan)?)?;

        fixture
            .manager
            .collect_garbage(Timestamp(modified.saturating_add(3_600_000)))
            .await?;

        assert!(orphan.exists());
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn slow_copy_renews_staging_lease_on_time_not_byte_progress() -> Result<()> {
        let fixture = ArtifactFixture::new().await?;
        let source = fixture.root.join("slow.txt");
        tokio::fs::write(&source, vec![7_u8; 128 * 1024]).await?;
        let attempt = fixture.running_attempt("call-slow").await?;
        let manager = fixture
            .manager
            .clone()
            .with_test_copy_delay(std::time::Duration::from_secs(31));
        let run = fixture.run.clone();
        let task = tokio::spawn(async move {
            manager
                .stage_execution(&run, &attempt, file_execution(&source, "slow.txt"))
                .await
        });
        tokio::task::yield_now().await;
        fixture.clock.advance(31_000);
        tokio::time::advance(std::time::Duration::from_secs(31)).await;
        tokio::task::yield_now().await;
        tokio::time::advance(std::time::Duration::from_secs(31)).await;

        assert!(task.await??.artifacts[0].managed_path.exists());
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn failed_copy_stops_lease_heartbeat() -> Result<()> {
        let fixture = ArtifactFixture::new().await?;
        let source = fixture.root.join("failed-copy.txt");
        tokio::fs::write(&source, b"fail").await?;
        let attempt = fixture.running_attempt("call-failed-copy").await?;
        let manager = fixture.manager.clone().with_test_copy_failure();
        assert!(
            manager
                .stage_execution(
                    &fixture.run,
                    &attempt,
                    file_execution(&source, "failed.txt")
                )
                .await
                .is_err()
        );
        fixture.clock.advance(40_000);
        tokio::time::advance(std::time::Duration::from_secs(40)).await;
        tokio::task::yield_now().await;
        assert_eq!(
            fixture.store.staging_expiry_probe(&attempt).await?,
            Some(Timestamp(30_010))
        );
        Ok(())
    }

    #[tokio::test]
    async fn orphan_gc_racing_successful_commit_preserves_referenced_bytes() -> Result<()> {
        let fixture = ArtifactFixture::new().await?;
        let source = fixture.root.join("race.txt");
        tokio::fs::write(&source, b"race bytes").await?;
        let sha = format!("{:x}", sha2::Sha256::digest(b"race bytes"));
        let actor = fixture
            .managed
            .join(super::actor_directory(fixture.run.lease.actor_id.as_str()));
        tokio::fs::create_dir_all(&actor).await?;
        let canonical = actor.join(sha);
        tokio::fs::write(&canonical, b"race bytes").await?;
        let pause = Arc::new(TestPause {
            entered: tokio::sync::Notify::new(),
            resume: tokio::sync::Notify::new(),
        });
        let gc_manager = fixture.manager.clone().with_test_gc_pause(pause.clone());
        let gc = tokio::spawn(async move { gc_manager.collect_garbage(Timestamp(i64::MAX)).await });
        pause.entered.notified().await;

        let attempt = fixture.running_attempt("call-race").await?;
        fixture
            .manager
            .stage_execution(&fixture.run, &attempt, file_execution(&source, "race.txt"))
            .await?;
        pause.resume.notify_one();
        gc.await??;

        assert_eq!(tokio::fs::read(canonical).await?, b"race bytes");
        Ok(())
    }

    struct ArtifactFixture {
        root: PathBuf,
        managed: PathBuf,
        store: SqliteRuntimeStore,
        manager: ArtifactManager<SqliteRuntimeStore, ManualClock>,
        clock: ManualClock,
        run: crate::runtime::store::AttachedRun,
    }

    impl ArtifactFixture {
        async fn new() -> Result<Self> {
            let root = temp_path("fixture");
            let managed = root.join("managed");
            tokio::fs::create_dir_all(&managed).await?;
            let managed = tokio::fs::canonicalize(managed).await?;
            let store = SqliteRuntimeStore::open(root.join("runtime.sqlite3")).await?;
            store
                .seed_actors_for_test(
                    ActorSeedSet {
                        actors: vec![ActorSeed {
                            id: "actor:artifact:1".into(),
                            enabled: true,
                            tools: vec!["files".into()],
                            identities: vec![IdentitySeed {
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
                .acquire_ready_actor("worker", Timestamp(3), Timestamp(100_000))
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
            let clock = ManualClock::new(10);
            let manager = ArtifactManager::new(&managed, store.clone(), clock.clone());
            Ok(Self {
                root,
                managed,
                store,
                manager,
                run,
                clock,
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
