#[cfg(test)]
mod tests {
    use std::{
        env, fs,
        os::unix::fs::PermissionsExt,
        process::Command,
        sync::{Arc, Barrier},
        time::{Duration, Instant},
    };

    use anyhow::Result;

    use super::{RequestMetadataState, RequestMetadataStore, recovery_command};
    use crate::runtime::RequestId;

    #[test]
    fn atomically_persists_only_recovery_metadata_with_private_modes() -> Result<()> {
        let root = temp_root("private");
        let request = RequestId::new();
        let store = RequestMetadataStore::new(root.clone());
        store.create(&request, 1234, "very secret prompt")?;

        let path = store.path(&request);
        let json = fs::read_to_string(&path)?;
        assert!(!json.contains("very secret prompt"));
        assert!(!json.contains("response"));
        let value: serde_json::Value = serde_json::from_str(&json)?;
        let mut keys: Vec<_> = value
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        keys.sort_unstable();
        assert_eq!(keys, ["created_at", "prompt_sha256", "request_id", "state"]);
        let metadata = store.load(&request)?.expect("metadata");
        assert_eq!(metadata.state, RequestMetadataState::Created);
        assert_eq!(metadata.prompt_sha256.len(), 64);
        assert_eq!(fs::metadata(&root)?.permissions().mode() & 0o7777, 0o700);
        assert_eq!(fs::metadata(path)?.permissions().mode() & 0o7777, 0o600);
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn recovers_from_stale_temp_and_repairs_directory_permissions() -> Result<()> {
        let root = temp_root("recovery");
        fs::create_dir_all(&root)?;
        fs::set_permissions(&root, fs::Permissions::from_mode(0o755))?;
        fs::write(
            root.join(".interrupted.tmp"),
            b"partial prompt must not win",
        )?;
        let request = RequestId::new();
        let store = RequestMetadataStore::new(root.clone());

        store.create(&request, 42, "secret")?;
        store.set_state(&request, RequestMetadataState::SentUnconfirmed)?;
        assert_eq!(
            store.load(&request)?.expect("metadata").state,
            RequestMetadataState::SentUnconfirmed
        );
        assert_eq!(fs::metadata(&root)?.permissions().mode() & 0o7777, 0o700);
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn formats_exact_recovery_command() {
        let request = RequestId::parse("0190f2ef-0000-7000-8000-000000000001").unwrap();
        assert_eq!(
            recovery_command(&request),
            format!("codrik resume {request}")
        );
    }

    #[test]
    fn concurrent_stale_writers_cannot_overwrite_terminal() -> Result<()> {
        let root = temp_root("concurrent");
        let request = RequestId::new();
        let store = RequestMetadataStore::new(root.clone());
        store.create(&request, 1, "secret")?;
        store.set_state(&request, RequestMetadataState::Accepted)?;
        let barrier = Arc::new(Barrier::new(17));
        let mut writers = Vec::new();
        for index in 0..16 {
            let store = RequestMetadataStore::new(root.clone());
            let request = request.clone();
            let barrier = barrier.clone();
            writers.push(std::thread::spawn(move || {
                barrier.wait();
                if index == 0 {
                    store.set_state(&request, RequestMetadataState::Terminal)
                } else {
                    let _ = store.set_state(&request, RequestMetadataState::Accepted);
                    Ok(())
                }
            }));
        }
        barrier.wait();
        for writer in writers {
            writer.join().unwrap()?;
        }
        assert_eq!(
            store.load(&request)?.unwrap().state,
            RequestMetadataState::Terminal
        );
        assert_eq!(
            fs::metadata(store.lock_path(&request))?
                .permissions()
                .mode()
                & 0o7777,
            0o600
        );
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn rejects_bad_lock_permissions_and_cleans_failed_temp_write() -> Result<()> {
        let root = temp_root("bad-permissions");
        let request = RequestId::new();
        let store = RequestMetadataStore::new(root.clone());
        fs::create_dir_all(&root)?;
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700))?;
        fs::write(store.lock_path(&request), b"")?;
        fs::set_permissions(store.lock_path(&request), fs::Permissions::from_mode(0o644))?;
        assert!(store.create(&request, 1, "secret").is_err());
        fs::remove_file(store.lock_path(&request))?;
        fs::create_dir(store.path(&request))?;
        assert!(store.create(&request, 1, "secret").is_err());
        assert!(
            fs::read_dir(&root)?
                .filter_map(|entry| entry.ok())
                .all(|entry| !entry.file_name().to_string_lossy().ends_with(".tmp"))
        );
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn rejects_backward_and_bad_file_permissions() -> Result<()> {
        let root = temp_root("backward");
        let request = RequestId::new();
        let store = RequestMetadataStore::new(root.clone());
        store.create(&request, 1, "secret")?;
        store.set_state(&request, RequestMetadataState::Terminal)?;
        assert!(
            store
                .set_state(&request, RequestMetadataState::Accepted)
                .is_err()
        );
        fs::set_permissions(store.path(&request), fs::Permissions::from_mode(0o644))?;
        assert!(store.load(&request).is_err());
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn pre_rename_write_failure_preserves_authoritative_file_and_cleans_temp() -> Result<()> {
        let root = temp_root("write-failure");
        let request = RequestId::new();
        let store = RequestMetadataStore::new(root.clone());
        store.create(&request, 1, "secret")?;
        assert!(
            store
                .simulate_pre_rename_failure(&request, RequestMetadataState::Accepted)
                .is_err()
        );
        assert_eq!(
            store.load(&request)?.unwrap().state,
            RequestMetadataState::Created
        );
        assert!(
            fs::read_dir(&root)?
                .filter_map(|entry| entry.ok())
                .all(|entry| !entry.file_name().to_string_lossy().ends_with(".tmp"))
        );
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn rejects_special_permission_bits_on_root_lock_and_metadata() -> Result<()> {
        let root = temp_root("special-bits");
        let request = RequestId::new();
        let store = RequestMetadataStore::new(root.clone());
        store.create(&request, 1, "secret")?;

        fs::set_permissions(store.path(&request), fs::Permissions::from_mode(0o4600))?;
        assert!(store.load(&request).is_err());
        fs::set_permissions(store.path(&request), fs::Permissions::from_mode(0o600))?;

        fs::set_permissions(
            store.lock_path(&request),
            fs::Permissions::from_mode(0o4600),
        )?;
        assert!(
            store
                .set_state(&request, RequestMetadataState::Accepted)
                .is_err()
        );
        fs::set_permissions(store.lock_path(&request), fs::Permissions::from_mode(0o600))?;

        fs::set_permissions(&root, fs::Permissions::from_mode(0o4700))?;
        assert!(
            store
                .set_state(&request, RequestMetadataState::Accepted)
                .is_err()
        );
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn cross_process_lock_prevents_stale_accepted_overwriting_terminal() -> Result<()> {
        let root = temp_root("process-lock");
        let request = RequestId::new();
        let store = RequestMetadataStore::new(root.clone());
        store.create(&request, 1, "secret")?;
        store.set_state(&request, RequestMetadataState::Accepted)?;
        let ready = root.join("accepted-ready");
        let release = root.join("accepted-release");
        let terminal_started = root.join("terminal-started");
        let executable = env::current_exe()?;

        let mut accepted = Command::new(&executable)
            .args([
                "--exact",
                "interfaces::request_metadata::tests::metadata_process_writer_helper",
                "--nocapture",
            ])
            .env("CODRIK_METADATA_HELPER_ROOT", &root)
            .env("CODRIK_METADATA_HELPER_REQUEST", request.to_string())
            .env("CODRIK_METADATA_HELPER_STATE", "accepted")
            .env("CODRIK_METADATA_HELPER_READY", &ready)
            .env("CODRIK_METADATA_HELPER_RELEASE", &release)
            .spawn()?;
        wait_for_path(&ready)?;

        let mut terminal = Command::new(&executable)
            .args([
                "--exact",
                "interfaces::request_metadata::tests::metadata_process_writer_helper",
                "--nocapture",
            ])
            .env("CODRIK_METADATA_HELPER_ROOT", &root)
            .env("CODRIK_METADATA_HELPER_REQUEST", request.to_string())
            .env("CODRIK_METADATA_HELPER_STATE", "terminal")
            .env("CODRIK_METADATA_HELPER_STARTED", &terminal_started)
            .spawn()?;
        wait_for_path(&terminal_started)?;
        assert!(
            terminal.try_wait()?.is_none(),
            "terminal writer did not block"
        );
        fs::write(&release, b"release")?;
        assert!(accepted.wait()?.success());
        assert!(terminal.wait()?.success());
        assert_eq!(
            store.load(&request)?.unwrap().state,
            RequestMetadataState::Terminal
        );
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn metadata_process_writer_helper() -> Result<()> {
        let Ok(root) = env::var("CODRIK_METADATA_HELPER_ROOT") else {
            return Ok(());
        };
        let request = RequestId::parse(&env::var("CODRIK_METADATA_HELPER_REQUEST")?)?;
        let state = match env::var("CODRIK_METADATA_HELPER_STATE")?.as_str() {
            "accepted" => RequestMetadataState::Accepted,
            "terminal" => RequestMetadataState::Terminal,
            other => anyhow::bail!("unknown helper state {other}"),
        };
        if let Ok(started) = env::var("CODRIK_METADATA_HELPER_STARTED") {
            fs::write(started, b"attempting-set-state")?;
        }
        RequestMetadataStore::new(root.into()).set_state(&request, state)
    }

    fn wait_for_path(path: &std::path::Path) -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(2);
        while !path.exists() {
            if Instant::now() >= deadline {
                anyhow::bail!("timed out waiting for {}", path.display())
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        Ok(())
    }

    fn temp_root(label: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("codrik-task11-{label}-{}", uuid::Uuid::new_v4()))
    }
}
use std::{
    fs::{self, DirBuilder, File, OpenOptions},
    io::Write,
    os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt},
    path::PathBuf,
};

use anyhow::{Context, Result, bail};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::runtime::RequestId;

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum RequestMetadataState {
    Created,
    SentUnconfirmed,
    Accepted,
    Terminal,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RequestMetadata {
    pub request_id: RequestId,
    pub created_at: i64,
    pub prompt_sha256: String,
    pub state: RequestMetadataState,
}

#[derive(Clone, Debug)]
pub struct RequestMetadataStore {
    root: PathBuf,
}

impl RequestMetadataStore {
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }

    pub fn path(&self, request: &RequestId) -> PathBuf {
        self.root.join(format!("{request}.json"))
    }

    pub fn lock_path(&self, request: &RequestId) -> PathBuf {
        self.root.join(format!("{request}.lock"))
    }

    pub fn create(&self, request: &RequestId, created_at: i64, prompt: &str) -> Result<()> {
        let metadata = RequestMetadata {
            request_id: request.clone(),
            created_at,
            prompt_sha256: format!("{:x}", Sha256::digest(prompt.as_bytes())),
            state: RequestMetadataState::Created,
        };
        self.prepare_root()?;
        let _lock = self.lock(request)?;
        if let Some(existing) = self.load(request)? {
            if existing.prompt_sha256 != metadata.prompt_sha256 {
                bail!("request recovery metadata already exists with a different prompt hash")
            }
            return Ok(());
        }
        self.write_atomic(&metadata)
    }

    pub fn set_state(&self, request: &RequestId, state: RequestMetadataState) -> Result<()> {
        self.prepare_root()?;
        let _lock = self.lock(request)?;
        let mut metadata = self
            .load(request)?
            .with_context(|| format!("request recovery metadata is missing for {request}"))?;
        if state < metadata.state {
            bail!(
                "request metadata state cannot move backward from {:?} to {:?}",
                metadata.state,
                state
            );
        }
        #[cfg(test)]
        self.pause_test_writer_after_load(request)?;
        metadata.state = state;
        self.write_atomic(&metadata)
    }

    pub fn set_state_if_present(
        &self,
        request: &RequestId,
        state: RequestMetadataState,
    ) -> Result<()> {
        self.prepare_root()?;
        let _lock = self.lock(request)?;
        let Some(mut metadata) = self.load(request)? else {
            return Ok(());
        };
        if state < metadata.state {
            bail!(
                "request metadata state cannot move backward from {:?} to {:?}",
                metadata.state,
                state
            );
        }
        #[cfg(test)]
        self.pause_test_writer_after_load(request)?;
        metadata.state = state;
        self.write_atomic(&metadata)?;
        Ok(())
    }

    pub fn load(&self, request: &RequestId) -> Result<Option<RequestMetadata>> {
        let path = self.path(request);
        let file = match OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&path)
        {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("failed to open metadata {}", path.display()));
            }
        };
        let metadata = file.metadata()?;
        if !metadata.is_file()
            || metadata.permissions().mode() & 0o7777 != 0o600
            || metadata.uid() != unsafe { libc::geteuid() }
        {
            bail!(
                "request metadata must be a regular mode-0600 file: {}",
                path.display()
            );
        }
        let decoded: RequestMetadata = serde_json::from_reader(file)
            .with_context(|| format!("failed to decode metadata {}", path.display()))?;
        if &decoded.request_id != request {
            bail!("request metadata ID does not match its file name")
        }
        Ok(Some(decoded))
    }

    fn write_atomic(&self, metadata: &RequestMetadata) -> Result<()> {
        self.write_atomic_before_rename(metadata, || Ok(()))
    }

    fn write_atomic_before_rename(
        &self,
        metadata: &RequestMetadata,
        before_rename: impl FnOnce() -> Result<()>,
    ) -> Result<()> {
        let destination = self.path(&metadata.request_id);
        let temporary = self.root.join(format!(
            ".{}.{}.tmp",
            metadata.request_id,
            uuid::Uuid::new_v4()
        ));
        let result = (|| -> Result<()> {
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&temporary)
                .with_context(|| {
                    format!("failed to create metadata temp {}", temporary.display())
                })?;
            file.set_permissions(fs::Permissions::from_mode(0o600))?;
            if file.metadata()?.permissions().mode() & 0o7777 != 0o600 {
                bail!("request metadata temp file mode is not exactly 0600")
            }
            serde_json::to_writer(&mut file, metadata)?;
            file.write_all(b"\n")?;
            file.sync_all()?;
            before_rename()?;
            fs::rename(&temporary, &destination).with_context(|| {
                format!(
                    "failed to atomically replace request metadata {}",
                    destination.display()
                )
            })?;
            File::open(&self.root)?.sync_all()?;
            Ok(())
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temporary);
        }
        result
    }

    #[cfg(test)]
    fn simulate_pre_rename_failure(
        &self,
        request: &RequestId,
        state: RequestMetadataState,
    ) -> Result<()> {
        self.prepare_root()?;
        let _lock = self.lock(request)?;
        let mut metadata = self.load(request)?.context("metadata missing")?;
        metadata.state = state;
        self.write_atomic_before_rename(&metadata, || bail!("injected pre-rename failure"))
    }

    fn prepare_root(&self) -> Result<()> {
        match fs::symlink_metadata(&self.root) {
            Ok(metadata) => {
                if metadata.file_type().is_symlink()
                    || !metadata.is_dir()
                    || metadata.uid() != unsafe { libc::geteuid() }
                {
                    bail!(
                        "request metadata root is not a directory: {}",
                        self.root.display()
                    );
                }
                if metadata.permissions().mode() & 0o7000 != 0 {
                    bail!(
                        "request metadata root must not have special permission bits: {}",
                        self.root.display()
                    )
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let mut builder = DirBuilder::new();
                builder.recursive(true).mode(0o700).create(&self.root)?;
            }
            Err(error) => return Err(error.into()),
        }
        fs::set_permissions(&self.root, fs::Permissions::from_mode(0o700))?;
        if fs::metadata(&self.root)?.permissions().mode() & 0o7777 != 0o700 {
            bail!("request metadata root mode is not exactly 0700")
        }
        Ok(())
    }

    fn lock(&self, request: &RequestId) -> Result<RequestLock> {
        let path = self.lock_path(request);
        let create = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&path);
        let (file, created) = match create {
            Ok(file) => (file, true),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => (
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .custom_flags(libc::O_NOFOLLOW)
                    .open(&path)
                    .with_context(|| {
                        format!("failed to open request metadata lock {}", path.display())
                    })?,
                false,
            ),
            Err(error) => {
                return Err(error).with_context(|| {
                    format!("failed to create request metadata lock {}", path.display())
                });
            }
        };
        if created {
            file.set_permissions(fs::Permissions::from_mode(0o600))?;
        }
        let metadata = file.metadata()?;
        if !metadata.is_file()
            || metadata.permissions().mode() & 0o7777 != 0o600
            || metadata.uid() != unsafe { libc::geteuid() }
        {
            bail!(
                "request metadata lock must be an owned mode-0600 file: {}",
                path.display()
            )
        }
        file.lock_exclusive()
            .with_context(|| format!("failed to lock request metadata {}", path.display()))?;
        Ok(RequestLock(file))
    }

    #[cfg(test)]
    fn pause_test_writer_after_load(&self, request: &RequestId) -> Result<()> {
        let Ok(ready) = std::env::var("CODRIK_METADATA_HELPER_READY") else {
            return Ok(());
        };
        let expected_request = request.to_string();
        if std::env::var("CODRIK_METADATA_HELPER_REQUEST")
            .ok()
            .as_deref()
            != Some(expected_request.as_str())
        {
            return Ok(());
        }
        let release = std::env::var("CODRIK_METADATA_HELPER_RELEASE")?;
        fs::write(&ready, b"locked")?;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !std::path::Path::new(&release).exists() {
            if std::time::Instant::now() >= deadline {
                bail!("timed out waiting to release metadata test writer")
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        Ok(())
    }
}

struct RequestLock(File);

impl Drop for RequestLock {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.0);
    }
}

pub fn recovery_command(request: &RequestId) -> String {
    format!("codrik resume {request}")
}
