use std::collections::HashSet;

use anyhow::Result;
use async_trait::async_trait;

use crate::{
    config::AppConfig,
    llm::openai::OpenAiClient,
    memory::{
        provider_files::ProviderFileStore,
        telegram_sessions::{BeginDeleteResult, TelegramSessionStore},
    },
};

#[async_trait]
trait ProviderFileDeleter: Send + Sync {
    async fn delete_file(&self, file_id: &str) -> Result<()>;
}

#[async_trait]
impl ProviderFileDeleter for OpenAiClient {
    async fn delete_file(&self, file_id: &str) -> Result<()> {
        OpenAiClient::delete_file(self, file_id).await
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SessionDeletionOutcome {
    NotFound,
    Active,
    Deleted { failed_remote_deletions: usize },
}

pub async fn delete_inactive_session(
    config: AppConfig,
    store: &TelegramSessionStore,
    chat_id: i64,
    session_id: &str,
) -> Result<SessionDeletionOutcome> {
    let client = OpenAiClient::new(config.model, config.api_key, config.base_url);
    delete_with(&client, store, chat_id, session_id).await
}

async fn delete_with(
    deleter: &dyn ProviderFileDeleter,
    store: &TelegramSessionStore,
    chat_id: i64,
    session_id: &str,
) -> Result<SessionDeletionOutcome> {
    let session_dir = match store.begin_delete(chat_id, session_id).await? {
        BeginDeleteResult::NotFound => return Ok(SessionDeletionOutcome::NotFound),
        BeginDeleteResult::Active => return Ok(SessionDeletionOutcome::Active),
        BeginDeleteResult::Ready { session_dir } => session_dir,
    };
    let (records, mut failed) = match ProviderFileStore::new(&session_dir).entries().await {
        Ok(records) => (records, 0),
        Err(_) => (Vec::new(), 1),
    };
    let mut seen = HashSet::new();
    for file_id in records.into_iter().map(|record| record.file_id) {
        if seen.insert(file_id.clone()) && deleter.delete_file(&file_id).await.is_err() {
            failed += 1;
        }
    }
    store.finish_delete(chat_id, session_id).await?;
    Ok(SessionDeletionOutcome::Deleted {
        failed_remote_deletions: failed,
    })
}

#[cfg(test)]
mod tests {
    use std::{path::PathBuf, sync::Mutex};

    use anyhow::{Result, bail};
    use tokio::fs;

    use crate::memory::provider_files::{
        ProviderFileKey, ProviderFileRecord, ProviderUploadPurpose,
    };

    use super::*;

    struct RecordingDeleter {
        calls: Mutex<Vec<String>>,
        failing: String,
    }

    #[async_trait]
    impl ProviderFileDeleter for RecordingDeleter {
        async fn delete_file(&self, file_id: &str) -> Result<()> {
            self.calls
                .lock()
                .expect("calls lock poisoned")
                .push(file_id.to_string());
            if file_id == self.failing {
                bail!("remote delete failed");
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn local_delete_continues_after_partial_remote_failure() -> Result<()> {
        let root = temp_root();
        fs::remove_dir_all(&root).await.ok();
        let store = TelegramSessionStore::new(&root);
        let inactive = store.active_session_id(123).await?;
        store.create_session(123).await?;
        let session_dir = store.session_root(123).join(&inactive);
        let cache = ProviderFileStore::new(&session_dir);
        for (sha, file_id) in [("one", "file-ok"), ("two", "file-fail")] {
            cache
                .put(ProviderFileRecord {
                    key: ProviderFileKey {
                        sha256: sha.to_string(),
                        provider: "openai".to_string(),
                        purpose: ProviderUploadPurpose::UserData,
                    },
                    file_id: file_id.to_string(),
                })
                .await?;
        }
        let deleter = RecordingDeleter {
            calls: Mutex::new(Vec::new()),
            failing: "file-fail".to_string(),
        };

        let outcome = delete_with(&deleter, &store, 123, &inactive).await?;

        assert_eq!(
            outcome,
            SessionDeletionOutcome::Deleted {
                failed_remote_deletions: 1
            }
        );
        assert!(!fs::try_exists(session_dir).await?);
        assert!(
            !store
                .list_sessions(123)
                .await?
                .iter()
                .any(|session| session.id == inactive)
        );
        fs::remove_dir_all(root).await.ok();
        Ok(())
    }

    fn temp_root() -> PathBuf {
        std::env::temp_dir().join(format!(
            "codrik-session-deletion-test-{}",
            std::process::id()
        ))
    }
}
