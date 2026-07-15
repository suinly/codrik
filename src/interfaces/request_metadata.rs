#[cfg(test)]
mod tests {
    use std::{fs, os::unix::fs::PermissionsExt};

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
        let metadata = store.load(&request)?.expect("metadata");
        assert_eq!(metadata.state, RequestMetadataState::Created);
        assert_eq!(metadata.prompt_sha256.len(), 64);
        assert_eq!(fs::metadata(&root)?.permissions().mode() & 0o777, 0o700);
        assert_eq!(fs::metadata(path)?.permissions().mode() & 0o777, 0o600);
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
        assert_eq!(fs::metadata(&root)?.permissions().mode() & 0o777, 0o700);
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

    fn temp_root(label: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("codrik-task11-{label}-{}", uuid::Uuid::new_v4()))
    }
}
use std::{
    fs::{self, DirBuilder, File, OpenOptions},
    io::Write,
    os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt},
    path::PathBuf,
};

use anyhow::{Context, Result, bail};
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

    pub fn create(&self, request: &RequestId, created_at: i64, prompt: &str) -> Result<()> {
        let metadata = RequestMetadata {
            request_id: request.clone(),
            created_at,
            prompt_sha256: format!("{:x}", Sha256::digest(prompt.as_bytes())),
            state: RequestMetadataState::Created,
        };
        self.write_atomic(&metadata)
    }

    pub fn set_state(&self, request: &RequestId, state: RequestMetadataState) -> Result<()> {
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
        metadata.state = state;
        self.write_atomic(&metadata)
    }

    pub fn set_state_if_present(
        &self,
        request: &RequestId,
        state: RequestMetadataState,
    ) -> Result<()> {
        if self.load(request)?.is_some() {
            self.set_state(request, state)?;
        }
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
        if !metadata.is_file() || metadata.permissions().mode() & 0o777 != 0o600 {
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
        self.prepare_root()?;
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
            serde_json::to_writer(&mut file, metadata)?;
            file.write_all(b"\n")?;
            file.sync_all()?;
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

    fn prepare_root(&self) -> Result<()> {
        match fs::symlink_metadata(&self.root) {
            Ok(metadata) => {
                if metadata.file_type().is_symlink() || !metadata.is_dir() {
                    bail!(
                        "request metadata root is not a directory: {}",
                        self.root.display()
                    );
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let mut builder = DirBuilder::new();
                builder.recursive(true).mode(0o700).create(&self.root)?;
            }
            Err(error) => return Err(error.into()),
        }
        fs::set_permissions(&self.root, fs::Permissions::from_mode(0o700))?;
        Ok(())
    }
}

pub fn recovery_command(request: &RequestId) -> String {
    format!("codrik resume {request}")
}
