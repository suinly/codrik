use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::{fs, sync::Mutex};

use crate::{
    agent::message::{Message, Role},
    llm::client::LlmToolCall,
    memory::store::MemoryStore,
};

#[derive(Clone, Debug)]
pub struct FileMemoryStore {
    path: PathBuf,
    write_lock: Arc<Mutex<()>>,
}

impl FileMemoryStore {
    pub fn new(root: impl AsRef<Path>, session_id: impl AsRef<str>) -> Result<Self> {
        let session_id = session_id.as_ref();

        if !is_safe_session_id(session_id) {
            bail!("unsafe session id: {session_id}");
        }

        Ok(Self {
            path: root.as_ref().join(format!("{session_id}.json")),
            write_lock: Arc::new(Mutex::new(())),
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

        let messages = serde_json::from_str::<Vec<SessionMessage>>(&content)
            .with_context(|| format!("failed to parse session file: {}", self.path.display()))?;

        Ok(messages.into_iter().map(Message::from).collect())
    }

    async fn write_messages(&self, messages: &[Message]) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).await.with_context(|| {
                format!("failed to create session directory: {}", parent.display())
            })?;
        }

        let messages = messages
            .iter()
            .cloned()
            .map(SessionMessage::from)
            .collect::<Vec<_>>();
        let content =
            serde_json::to_string_pretty(&messages).context("failed to serialize session")?;

        fs::write(&self.path, content)
            .await
            .with_context(|| format!("failed to write session file: {}", self.path.display()))
    }
}

#[async_trait]
impl MemoryStore for FileMemoryStore {
    async fn append(&self, message: Message) -> Result<()> {
        let _guard = self.write_lock.lock().await;
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct SessionMessage {
    role: SessionRole,
    content: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    tool_calls: Vec<LlmToolCall>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    tool_call_id: Option<String>,
}

impl From<Message> for SessionMessage {
    fn from(message: Message) -> Self {
        Self {
            role: SessionRole::from(message.role),
            content: message.content,
            tool_calls: message.tool_calls,
            tool_call_id: message.tool_call_id,
        }
    }
}

impl From<SessionMessage> for Message {
    fn from(message: SessionMessage) -> Self {
        Self {
            role: Role::from(message.role),
            content: message.content,
            tool_calls: message.tool_calls,
            tool_call_id: message.tool_call_id,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum SessionRole {
    #[serde(alias = "User")]
    User,
    #[serde(alias = "Assistant")]
    Assistant,
    #[serde(alias = "System")]
    System,
    #[serde(alias = "Tool")]
    Tool,
}

impl From<Role> for SessionRole {
    fn from(role: Role) -> Self {
        match role {
            Role::User => Self::User,
            Role::Assistant => Self::Assistant,
            Role::System => Self::System,
            Role::Tool => Self::Tool,
        }
    }
}

impl From<SessionRole> for Role {
    fn from(role: SessionRole) -> Self {
        match role {
            SessionRole::User => Self::User,
            SessionRole::Assistant => Self::Assistant,
            SessionRole::System => Self::System,
            SessionRole::Tool => Self::Tool,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        path::PathBuf,
        sync::atomic::{AtomicU64, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };

    use anyhow::Result;
    use tokio::{fs, task::JoinSet};

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
            "codrik-test-{}-{suffix}-{counter}",
            std::process::id()
        ))
    }

    #[tokio::test]
    async fn persists_messages_to_session_file() -> Result<()> {
        let root = temp_session_root();
        let memory = FileMemoryStore::new(&root, "work")?;
        let message = Message::user("hello");

        memory.append(message.clone()).await?;

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

        memory.append(message.clone()).await?;
        memory.append(tool_result.clone()).await?;

        let restored = memory.load_context().await?;

        assert_eq!(restored, vec![message, tool_result]);

        fs::remove_dir_all(root).await.ok();

        Ok(())
    }

    #[tokio::test]
    async fn reads_legacy_capitalized_roles() -> Result<()> {
        let root = temp_session_root();
        fs::create_dir_all(&root).await?;
        fs::write(
            root.join("legacy.json"),
            r#"[
  {
    "role": "User",
    "content": "legacy hello"
  }
]"#,
        )
        .await?;

        let memory = FileMemoryStore::new(&root, "legacy")?;
        let context = memory.load_context().await?;

        assert_eq!(context, vec![Message::user("legacy hello")]);

        fs::remove_dir_all(root).await.ok();

        Ok(())
    }

    #[tokio::test]
    async fn concurrent_appends_keep_all_messages() -> Result<()> {
        let root = temp_session_root();
        let memory = FileMemoryStore::new(&root, "concurrent")?;
        let mut tasks = JoinSet::new();

        for index in 0..20 {
            let memory = memory.clone();
            tasks.spawn(async move {
                memory
                    .append(Message::user(format!("message {index}")))
                    .await
            });
        }

        while let Some(result) = tasks.join_next().await {
            result??;
        }

        let context = memory.load_context().await?;

        assert_eq!(context.len(), 20);
        for index in 0..20 {
            assert!(context.contains(&Message::user(format!("message {index}"))));
        }

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
