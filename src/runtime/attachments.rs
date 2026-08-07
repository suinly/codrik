use std::{
    fmt::Display,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use sha2::{Digest, Sha256};
use tokio::{
    fs::{self, OpenOptions},
    io::AsyncWriteExt,
};

use crate::{agent::message::Attachment, runtime::model::ActorId};

pub const TELEGRAM_MAX_DOWNLOAD_BYTES: u64 = 20_000_000;

static TEMP_FILE_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug)]
pub struct RuntimeAttachmentStore {
    root: PathBuf,
}

impl RuntimeAttachmentStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn actor_root(&self, actor: &ActorId) -> Result<PathBuf> {
        let actor = ActorId::parse_workspace_safe(actor.as_str())?;
        Ok(self.root.join(actor.as_str()))
    }

    pub async fn store_stream<S, E>(
        &self,
        actor: &ActorId,
        display_name: &str,
        stream: S,
    ) -> Result<Attachment>
    where
        S: Stream<Item = std::result::Result<Bytes, E>>,
        E: Display,
    {
        ensure_directory(&self.root).await?;
        let actor_root = self.actor_root(actor)?;
        ensure_directory(&actor_root).await?;
        let temp_path = actor_root.join(format!(
            ".upload-{}-{}",
            std::process::id(),
            TEMP_FILE_COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        let result = write_stream(display_name, stream, &actor_root, &temp_path).await;
        if result.is_err() {
            fs::remove_file(&temp_path).await.ok();
        }
        result
    }

    pub async fn remove_actor(&self, actor: &ActorId) -> Result<()> {
        let actor_root = self.actor_root(actor)?;
        let metadata = match fs::symlink_metadata(&actor_root).await {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error.into()),
        };
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            bail!("actor attachment path is not a safe directory")
        }
        fs::remove_dir_all(&actor_root)
            .await
            .with_context(|| format!("failed to remove actor attachments: {actor}"))
    }
}

async fn write_stream<S, E>(
    display_name: &str,
    stream: S,
    actor_root: &Path,
    temp_path: &Path,
) -> Result<Attachment>
where
    S: Stream<Item = std::result::Result<Bytes, E>>,
    E: Display,
{
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(temp_path)
        .await?;
    let mut size_bytes = 0_u64;
    let mut hasher = Sha256::new();
    let mut probe = Vec::with_capacity(8192);
    futures_util::pin_mut!(stream);

    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| anyhow::anyhow!("attachment stream failed: {error}"))?;
        size_bytes = size_bytes
            .checked_add(chunk.len() as u64)
            .context("attachment size overflow")?;
        if size_bytes > TELEGRAM_MAX_DOWNLOAD_BYTES {
            bail!("attachment exceeds the {TELEGRAM_MAX_DOWNLOAD_BYTES} byte limit")
        }
        let remaining = 8192_usize.saturating_sub(probe.len());
        probe.extend_from_slice(&chunk[..chunk.len().min(remaining)]);
        hasher.update(&chunk);
        file.write_all(&chunk).await?;
    }
    file.flush().await?;
    drop(file);

    let sha256 = format!("{:x}", hasher.finalize());
    let inferred = infer::get(&probe);
    let media_type = inferred
        .map(|kind| kind.mime_type())
        .unwrap_or("application/octet-stream")
        .to_string();
    let extension = inferred
        .map(|kind| kind.extension())
        .or_else(|| safe_extension(display_name))
        .unwrap_or("bin");
    let relative_path = PathBuf::from(format!("{sha256}.{extension}"));
    let final_path = actor_root.join(&relative_path);

    match fs::symlink_metadata(&final_path).await {
        Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => {
            fs::remove_file(temp_path).await?;
        }
        Ok(_) => bail!("attachment destination is not a safe regular file"),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            fs::rename(temp_path, &final_path).await?;
        }
        Err(error) => return Err(error.into()),
    }

    Ok(Attachment::new(
        sha256.clone(),
        relative_path,
        safe_display_name(display_name),
        media_type,
        size_bytes,
        sha256,
    ))
}

async fn ensure_directory(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path).await {
        Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => Ok(()),
        Ok(_) => bail!("attachment path is not a safe directory"),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => fs::create_dir(path)
            .await
            .with_context(|| format!("failed to create attachment directory: {}", path.display())),
        Err(error) => Err(error.into()),
    }
}

fn safe_display_name(display_name: &str) -> String {
    display_name
        .rsplit(['/', '\\'])
        .find(|name| !name.is_empty())
        .unwrap_or("attachment.bin")
        .to_string()
}

fn safe_extension(display_name: &str) -> Option<&str> {
    Path::new(display_name)
        .extension()
        .and_then(|extension| extension.to_str())
        .filter(|extension| {
            !extension.is_empty()
                && extension.len() <= 16
                && extension
                    .chars()
                    .all(|character| character.is_ascii_alphanumeric())
        })
}

#[cfg(test)]
mod tests {
    use std::{convert::Infallible, io, path::PathBuf};

    #[cfg(unix)]
    use std::os::unix::fs::symlink;

    use anyhow::Result;
    use bytes::Bytes;
    use futures_util::stream;
    use tokio::fs;

    use crate::runtime::model::ActorId;

    use super::{RuntimeAttachmentStore, TELEGRAM_MAX_DOWNLOAD_BYTES};

    fn temp_root(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "codrik-runtime-attachment-test-{}-{name}",
            std::process::id()
        ))
    }

    #[tokio::test]
    async fn accepts_exact_telegram_limit_and_rejects_one_more_byte() -> Result<()> {
        let root = temp_root("limit");
        fs::remove_dir_all(&root).await.ok();
        let store = RuntimeAttachmentStore::new(&root);
        let actor = ActorId::parse_workspace_safe("alice")?;

        let accepted = stream::iter([Ok::<_, Infallible>(Bytes::from(vec![
            b'x';
            TELEGRAM_MAX_DOWNLOAD_BYTES
                as usize
        ]))]);
        assert_eq!(
            store
                .store_stream(&actor, "exact.bin", accepted)
                .await?
                .size_bytes,
            TELEGRAM_MAX_DOWNLOAD_BYTES
        );

        let rejected = stream::iter([
            Ok::<_, Infallible>(Bytes::from(vec![
                b'x';
                TELEGRAM_MAX_DOWNLOAD_BYTES as usize
            ])),
            Ok(Bytes::from_static(b"x")),
        ]);
        assert!(
            store
                .store_stream(&actor, "large.bin", rejected)
                .await
                .is_err()
        );
        fs::remove_dir_all(root).await.ok();
        Ok(())
    }

    #[tokio::test]
    async fn stores_verified_actor_relative_content_addressed_file() -> Result<()> {
        let root = temp_root("store");
        fs::remove_dir_all(&root).await.ok();
        let store = RuntimeAttachmentStore::new(&root);
        let actor = ActorId::parse_workspace_safe("alice")?;
        let png = Bytes::from_static(b"\x89PNG\r\n\x1a\n\0\0\0\rIHDR");

        let first = store
            .store_stream(
                &actor,
                "../screen.png",
                stream::iter([Ok::<_, Infallible>(png.clone())]),
            )
            .await?;
        let second = store
            .store_stream(
                &actor,
                "screen.png",
                stream::iter([Ok::<_, Infallible>(png)]),
            )
            .await?;

        assert_eq!(first, second);
        assert_eq!(first.display_name, "screen.png");
        assert_eq!(first.media_type, "image/png");
        assert!(!first.relative_path.is_absolute());
        assert_eq!(first.relative_path.components().count(), 1);
        let mut entries = fs::read_dir(root.join("alice")).await?;
        assert!(entries.next_entry().await?.is_some());
        assert!(entries.next_entry().await?.is_none());
        fs::remove_dir_all(root).await.ok();
        Ok(())
    }

    #[tokio::test]
    async fn strips_windows_paths_from_display_name() -> Result<()> {
        let root = temp_root("windows-name");
        fs::remove_dir_all(&root).await.ok();
        let stored = RuntimeAttachmentStore::new(&root)
            .store_stream(
                &ActorId::parse_workspace_safe("alice")?,
                r"C:\Users\alice\secret.pdf",
                stream::iter([Ok::<_, Infallible>(Bytes::from_static(b"pdf"))]),
            )
            .await?;

        assert_eq!(stored.display_name, "secret.pdf");
        fs::remove_dir_all(root).await.ok();
        Ok(())
    }

    #[tokio::test]
    async fn removes_partial_file_after_stream_failure() -> Result<()> {
        let root = temp_root("partial");
        fs::remove_dir_all(&root).await.ok();
        let store = RuntimeAttachmentStore::new(&root);
        let actor = ActorId::parse_workspace_safe("alice")?;
        let chunks = stream::iter([
            Ok(Bytes::from_static(b"partial")),
            Err(io::Error::other("download failed")),
        ]);

        assert!(
            store
                .store_stream(&actor, "broken.bin", chunks)
                .await
                .is_err()
        );
        let mut entries = fs::read_dir(root.join("alice")).await?;
        assert!(entries.next_entry().await?.is_none());
        fs::remove_dir_all(root).await.ok();
        Ok(())
    }

    #[tokio::test]
    async fn removes_only_selected_actor_and_accepts_missing_directory() -> Result<()> {
        let root = temp_root("remove");
        fs::remove_dir_all(&root).await.ok();
        let store = RuntimeAttachmentStore::new(&root);
        let alice = ActorId::parse_workspace_safe("alice")?;
        let bob = ActorId::parse_workspace_safe("bob")?;
        store
            .store_stream(
                &alice,
                "a.bin",
                stream::iter([Ok::<_, Infallible>(Bytes::from_static(b"a"))]),
            )
            .await?;
        store
            .store_stream(
                &bob,
                "b.bin",
                stream::iter([Ok::<_, Infallible>(Bytes::from_static(b"b"))]),
            )
            .await?;

        store.remove_actor(&alice).await?;
        store.remove_actor(&alice).await?;

        assert!(!fs::try_exists(root.join("alice")).await?);
        assert!(fs::try_exists(root.join("bob")).await?);
        fs::remove_dir_all(root).await.ok();
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn rejects_symlinked_actor_directory_without_removing_target() -> Result<()> {
        let root = temp_root("symlink");
        let target = temp_root("symlink-target");
        fs::remove_dir_all(&root).await.ok();
        fs::remove_dir_all(&target).await.ok();
        fs::create_dir_all(&root).await?;
        fs::create_dir_all(&target).await?;
        fs::write(target.join("kept.bin"), b"kept").await?;
        symlink(&target, root.join("alice"))?;
        let store = RuntimeAttachmentStore::new(&root);
        let actor = ActorId::parse_workspace_safe("alice")?;

        assert!(
            store
                .store_stream(
                    &actor,
                    "blocked.bin",
                    stream::iter([Ok::<_, Infallible>(Bytes::from_static(b"blocked"))]),
                )
                .await
                .is_err()
        );
        assert!(store.remove_actor(&actor).await.is_err());
        assert!(fs::try_exists(target.join("kept.bin")).await?);
        assert!(!fs::try_exists(target.join("blocked.bin")).await?);
        fs::remove_file(root.join("alice")).await?;
        fs::remove_dir_all(root).await.ok();
        fs::remove_dir_all(target).await.ok();
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn rejects_symlinked_temporary_file_without_touching_target() -> Result<()> {
        let root = temp_root("temp-symlink");
        let target = temp_root("temp-symlink-target");
        fs::remove_dir_all(&root).await.ok();
        fs::remove_file(&target).await.ok();
        fs::create_dir_all(&root).await?;
        fs::write(&target, b"kept").await?;
        let temp_path = root.join(".upload");
        symlink(&target, &temp_path)?;

        assert!(
            super::write_stream(
                "file.bin",
                stream::iter([Ok::<_, Infallible>(Bytes::from_static(b"replaced"))]),
                &root,
                &temp_path,
            )
            .await
            .is_err()
        );
        assert_eq!(fs::read(&target).await?, b"kept");
        fs::remove_file(temp_path).await?;
        fs::remove_file(target).await?;
        fs::remove_dir_all(root).await.ok();
        Ok(())
    }
}
