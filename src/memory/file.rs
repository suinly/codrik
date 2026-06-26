use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use tokio::fs;

use crate::{agent::message::Message, memory::store::MemoryStore};

#[derive(Clone, Debug)]
pub struct FileMemoryStore {
    path: PathBuf,
}

impl FileMemoryStore {
    pub fn new(root: impl AsRef<Path>, session_id: impl AsRef<str>) -> Result<Self> {
        let session_id = session_id.as_ref();

        if !is_safe_session_id(session_id) {
            bail!("unsafe session id: {session_id}");
        }

        Ok(Self {
            path: root.as_ref().join(format!("{session_id}.json")),
        })
    }

    async fn read_messages(&self) -> Result<Vec<Message>> {
        if !fs::try_exists(&self.path)
            .await
            .with_context(|| format!("failed to inspect session file: {}", self.path.display()))?
        {
            return Ok(Vec::new());
        }

        let content = fs::read_to_string(&self.path)
            .await
            .with_context(|| format!("failed to read session file: {}", self.path.display()))?;

        serde_json::from_str(&content)
            .with_context(|| format!("failed to parse session file: {}", self.path.display()))
    }

    async fn write_messages(&self, messages: &[Message]) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).await.with_context(|| {
                format!("failed to create session directory: {}", parent.display())
            })?;
        }

        let content =
            serde_json::to_string_pretty(messages).context("failed to serialize session")?;

        fs::write(&self.path, content)
            .await
            .with_context(|| format!("failed to write session file: {}", self.path.display()))
    }
}

#[async_trait]
impl MemoryStore for FileMemoryStore {
    async fn save(&self, message: Message) -> Result<()> {
        let mut messages = self.read_messages().await?;
        messages.push(message);
        self.write_messages(&messages).await
    }

    async fn load_context(&self) -> Result<Vec<Message>> {
        self.read_messages().await
    }
}

fn is_safe_session_id(session_id: &str) -> bool {
    !session_id.is_empty()
        && session_id
            .chars()
            .all(|char| char.is_ascii_alphanumeric() || matches!(char, '.' | '_' | '-'))
}

#[cfg(test)]
mod tests {
    use std::{
        path::PathBuf,
        sync::atomic::{AtomicU64, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };

    use anyhow::Result;
    use tokio::fs;

    use crate::{
        agent::message::Message,
        llm::client::LlmToolCall,
        memory::{file::FileMemoryStore, store::MemoryStore},
    };

    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_session_root() -> PathBuf {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after unix epoch")
            .as_nanos();
        let counter = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);

        std::env::temp_dir().join(format!(
            "codrik-rs-test-{}-{suffix}-{counter}",
            std::process::id()
        ))
    }

    #[tokio::test]
    async fn persists_messages_to_session_file() -> Result<()> {
        let root = temp_session_root();
        let memory = FileMemoryStore::new(&root, "work")?;
        let message = Message::user("hello");

        memory.save(message.clone()).await?;

        let restored = FileMemoryStore::new(&root, "work")?.load_context().await?;

        assert_eq!(restored, vec![message]);

        let content = fs::read_to_string(root.join("work.json")).await?;
        assert!(content.contains("\"role\": \"user\""));
        assert!(!content.contains("tool_calls"));
        assert!(!content.contains("tool_call_id"));

        fs::remove_dir_all(root).await.ok();

        Ok(())
    }

    #[tokio::test]
    async fn preserves_tool_call_fields() -> Result<()> {
        let root = temp_session_root();
        let memory = FileMemoryStore::new(&root, "tools")?;
        let message = Message::assistant_tool_calls(
            "",
            vec![LlmToolCall {
                id: "call_1".to_string(),
                name: "datetime".to_string(),
                arguments: "{}".to_string(),
            }],
        );
        let tool_result = Message::tool_result("call_1", "2026-06-26");

        memory.save(message.clone()).await?;
        memory.save(tool_result.clone()).await?;

        let restored = memory.load_context().await?;

        assert_eq!(restored, vec![message, tool_result]);

        fs::remove_dir_all(root).await.ok();

        Ok(())
    }

    #[test]
    fn rejects_unsafe_session_ids() {
        assert!(FileMemoryStore::new(std::env::temp_dir(), "../secret").is_err());
        assert!(FileMemoryStore::new(std::env::temp_dir(), "chat/id").is_err());
        assert!(FileMemoryStore::new(std::env::temp_dir(), "").is_err());
    }
}
