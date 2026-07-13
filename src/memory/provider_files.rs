use std::{path::Path, path::PathBuf, sync::Arc};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use tokio::{fs, sync::Mutex};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderUploadPurpose {
    Vision,
    UserData,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderFileKey {
    pub sha256: String,
    pub provider: String,
    pub purpose: ProviderUploadPurpose,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderFileRecord {
    pub key: ProviderFileKey,
    pub file_id: String,
}

#[derive(Clone, Debug)]
pub struct ProviderFileStore {
    path: PathBuf,
    write_lock: Arc<Mutex<()>>,
}

impl ProviderFileStore {
    pub fn new(session_dir: impl AsRef<Path>) -> Self {
        Self {
            path: session_dir.as_ref().join("provider_files.json"),
            write_lock: Arc::new(Mutex::new(())),
        }
    }

    pub async fn get(&self, key: &ProviderFileKey) -> Result<Option<ProviderFileRecord>> {
        Ok(self
            .read_entries()
            .await?
            .into_iter()
            .find(|record| &record.key == key))
    }

    pub async fn entries(&self) -> Result<Vec<ProviderFileRecord>> {
        self.read_entries().await
    }

    pub async fn put(&self, record: ProviderFileRecord) -> Result<()> {
        let _guard = self.write_lock.lock().await;
        let mut entries = self.read_entries().await?;
        entries.retain(|entry| entry.key != record.key);
        entries.push(record);
        self.write_entries(&entries).await
    }

    pub async fn remove(&self, key: &ProviderFileKey) -> Result<()> {
        let _guard = self.write_lock.lock().await;
        let mut entries = self.read_entries().await?;
        entries.retain(|entry| &entry.key != key);
        self.write_entries(&entries).await
    }

    async fn read_entries(&self) -> Result<Vec<ProviderFileRecord>> {
        if !fs::try_exists(&self.path).await? {
            return Ok(Vec::new());
        }

        let content = fs::read_to_string(&self.path).await.with_context(|| {
            format!(
                "failed to read provider file cache: {}",
                self.path.display()
            )
        })?;
        serde_json::from_str(&content).with_context(|| {
            format!(
                "failed to parse provider file cache: {}",
                self.path.display()
            )
        })
    }

    async fn write_entries(&self, entries: &[ProviderFileRecord]) -> Result<()> {
        let parent = self
            .path
            .parent()
            .context("provider file cache must have a parent directory")?;
        fs::create_dir_all(parent).await?;
        let content = serde_json::to_string_pretty(entries)?;
        let temp_path = self.path.with_extension("json.tmp");
        fs::write(&temp_path, content).await.with_context(|| {
            format!(
                "failed to write provider file cache: {}",
                temp_path.display()
            )
        })?;
        fs::rename(&temp_path, &self.path).await.with_context(|| {
            format!(
                "failed to replace provider file cache: {}",
                self.path.display()
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use anyhow::Result;
    use tokio::fs;

    use super::{ProviderFileKey, ProviderFileRecord, ProviderFileStore, ProviderUploadPurpose};

    fn temp_session_dir() -> PathBuf {
        std::env::temp_dir().join(format!("codrik-provider-file-test-{}", std::process::id()))
    }

    #[tokio::test]
    async fn provider_file_cache_round_trips_records_atomically() -> Result<()> {
        let session_dir = temp_session_dir();
        fs::remove_dir_all(&session_dir).await.ok();
        let store = ProviderFileStore::new(&session_dir);
        let key = ProviderFileKey {
            sha256: "abc123".to_string(),
            provider: "openai".to_string(),
            purpose: ProviderUploadPurpose::Vision,
        };
        let record = ProviderFileRecord {
            key: key.clone(),
            file_id: "file_123".to_string(),
        };

        store.put(record.clone()).await?;

        assert_eq!(store.get(&key).await?, Some(record.clone()));
        assert_eq!(store.entries().await?, vec![record]);
        assert!(!fs::try_exists(session_dir.join("provider_files.json.tmp")).await?);

        store.remove(&key).await?;
        assert_eq!(store.get(&key).await?, None);

        fs::remove_dir_all(session_dir).await.ok();
        Ok(())
    }
}
