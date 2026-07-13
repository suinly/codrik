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
    fs::{self, File},
    io::AsyncWriteExt,
};

use crate::agent::message::Attachment;

static TEMP_FILE_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug)]
pub struct AttachmentStore {
    session_dir: PathBuf,
    max_size_bytes: u64,
}

impl AttachmentStore {
    pub fn new(session_dir: impl AsRef<Path>, max_size_bytes: u64) -> Self {
        Self {
            session_dir: session_dir.as_ref().to_path_buf(),
            max_size_bytes,
        }
    }

    pub async fn store_stream<S, E>(
        &self,
        display_name: impl AsRef<str>,
        stream: S,
    ) -> Result<Attachment>
    where
        S: Stream<Item = std::result::Result<Bytes, E>>,
        E: Display,
    {
        let attachments_dir = self.session_dir.join("attachments");
        fs::create_dir_all(&attachments_dir)
            .await
            .with_context(|| {
                format!(
                    "failed to create attachments directory: {}",
                    attachments_dir.display()
                )
            })?;

        let temp_path = attachments_dir.join(format!(
            ".upload-{}-{}",
            std::process::id(),
            TEMP_FILE_COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        let result = self
            .write_stream(display_name.as_ref(), stream, &attachments_dir, &temp_path)
            .await;

        if result.is_err() {
            fs::remove_file(&temp_path).await.ok();
        }

        result
    }

    async fn write_stream<S, E>(
        &self,
        display_name: &str,
        stream: S,
        attachments_dir: &Path,
        temp_path: &Path,
    ) -> Result<Attachment>
    where
        S: Stream<Item = std::result::Result<Bytes, E>>,
        E: Display,
    {
        let mut file = File::create(temp_path)
            .await
            .with_context(|| format!("failed to create attachment: {}", temp_path.display()))?;
        let mut size_bytes = 0_u64;
        let mut hasher = Sha256::new();
        let mut probe = Vec::with_capacity(8192);
        futures_util::pin_mut!(stream);

        while let Some(chunk) = stream.next().await {
            let chunk =
                chunk.map_err(|error| anyhow::anyhow!("attachment stream failed: {error}"))?;
            size_bytes = size_bytes
                .checked_add(chunk.len() as u64)
                .context("attachment size overflow")?;
            if size_bytes > self.max_size_bytes {
                bail!("attachment exceeds the {} byte limit", self.max_size_bytes);
            }

            let remaining_probe = 8192_usize.saturating_sub(probe.len());
            probe.extend_from_slice(&chunk[..chunk.len().min(remaining_probe)]);
            hasher.update(&chunk);
            file.write_all(&chunk)
                .await
                .with_context(|| format!("failed to write attachment: {}", temp_path.display()))?;
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
        let file_name = format!("{sha256}.{extension}");
        let final_path = attachments_dir.join(&file_name);

        if fs::try_exists(&final_path).await? {
            fs::remove_file(temp_path).await?;
        } else {
            fs::rename(temp_path, &final_path).await.with_context(|| {
                format!("failed to finalize attachment: {}", final_path.display())
            })?;
        }

        Ok(Attachment::new(
            sha256.clone(),
            PathBuf::from("attachments").join(file_name),
            safe_display_name(display_name),
            media_type,
            size_bytes,
            sha256,
        ))
    }
}

fn safe_display_name(display_name: &str) -> String {
    Path::new(display_name)
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
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
                && extension.chars().all(|char| char.is_ascii_alphanumeric())
        })
}

#[cfg(test)]
mod tests {
    use std::{convert::Infallible, path::PathBuf};

    use anyhow::Result;
    use bytes::Bytes;
    use futures_util::stream;
    use tokio::fs;

    use super::AttachmentStore;

    fn temp_session_dir(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "codrik-attachment-test-{}-{name}",
            std::process::id()
        ))
    }

    #[tokio::test]
    async fn stores_stream_and_derives_attachment_metadata() -> Result<()> {
        let session_dir = temp_session_dir("store");
        fs::remove_dir_all(&session_dir).await.ok();
        let store = AttachmentStore::new(&session_dir, 1024);
        let chunks = stream::iter([
            Ok::<_, Infallible>(Bytes::from_static(b"hello ")),
            Ok(Bytes::from_static(b"world")),
        ]);

        let attachment = store.store_stream("notes.txt", chunks).await?;

        assert_eq!(attachment.display_name, "notes.txt");
        assert_eq!(attachment.size_bytes, 11);
        assert_eq!(attachment.media_type, "application/octet-stream");
        assert!(attachment.relative_path.starts_with("attachments"));
        assert_eq!(
            fs::read(session_dir.join(&attachment.relative_path)).await?,
            b"hello world"
        );

        fs::remove_dir_all(session_dir).await.ok();
        Ok(())
    }

    #[tokio::test]
    async fn rejects_stream_when_actual_size_exceeds_limit() -> Result<()> {
        let session_dir = temp_session_dir("limit");
        fs::remove_dir_all(&session_dir).await.ok();
        let store = AttachmentStore::new(&session_dir, 5);
        let chunks = stream::iter([Ok::<_, Infallible>(Bytes::from_static(b"123456"))]);

        let error = store
            .store_stream("oversized.bin", chunks)
            .await
            .expect_err("oversized stream should fail");

        assert!(error.to_string().contains("exceeds the 5 byte limit"));
        let attachments_dir = session_dir.join("attachments");
        if fs::try_exists(&attachments_dir).await? {
            assert!(
                fs::read_dir(attachments_dir)
                    .await?
                    .next_entry()
                    .await?
                    .is_none()
            );
        }

        fs::remove_dir_all(session_dir).await.ok();
        Ok(())
    }
}
