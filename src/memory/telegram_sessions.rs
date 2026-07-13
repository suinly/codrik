use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use tokio::{fs, sync::Mutex};

#[derive(Clone, Debug)]
pub struct TelegramSessionStore {
    root: PathBuf,
    write_lock: Arc<Mutex<()>>,
}

impl TelegramSessionStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            write_lock: Arc::new(Mutex::new(())),
        }
    }

    pub fn session_root(&self, chat_id: i64) -> PathBuf {
        self.chat_root(chat_id)
    }

    pub async fn active_session_id(&self, chat_id: i64) -> Result<String> {
        let _guard = self.write_lock.lock().await;
        let mut chat = self.read_chat(chat_id).await?;

        if !chat
            .sessions
            .iter()
            .any(|session| session.id == chat.active_session_id)
        {
            chat.sessions.push(ChatSessionRecord::new(
                chat.active_session_id.clone(),
                now_timestamp(),
            ));
        }

        let active_session_id = chat.active_session_id.clone();
        touch_session(&mut chat, &active_session_id, now_timestamp());
        self.write_chat(chat_id, &chat).await?;

        Ok(active_session_id)
    }

    pub async fn create_session(&self, chat_id: i64) -> Result<String> {
        let _guard = self.write_lock.lock().await;
        let mut chat = self.read_chat(chat_id).await?;

        let created_at = now_timestamp();
        let session_id = unique_session_id(chat_id, created_at, chat.sessions.len() + 1);
        chat.sessions
            .push(ChatSessionRecord::new(session_id.clone(), created_at));
        chat.active_session_id = session_id.clone();

        self.write_chat(chat_id, &chat).await?;

        Ok(session_id)
    }

    pub async fn switch_session(&self, chat_id: i64, session_id: &str) -> Result<bool> {
        if !is_safe_session_id(session_id) {
            bail!("unsafe telegram session id: {session_id}");
        }

        let _guard = self.write_lock.lock().await;
        let mut chat = self.read_chat(chat_id).await?;

        if !chat
            .sessions
            .iter()
            .any(|session| session.id == session_id && !session.deleting)
        {
            return Ok(false);
        }

        chat.active_session_id = session_id.to_string();
        touch_session(&mut chat, session_id, now_timestamp());
        self.write_chat(chat_id, &chat).await?;

        Ok(true)
    }

    pub async fn list_sessions(&self, chat_id: i64) -> Result<Vec<TelegramSession>> {
        let _guard = self.write_lock.lock().await;
        let chat = self.read_chat(chat_id).await?;

        let sessions = chat
            .sessions
            .iter()
            .filter(|session| !session.deleting)
            .map(|session| TelegramSession {
                id: session.id.clone(),
                is_active: session.id == chat.active_session_id,
                created_at: session.created_at,
                last_used_at: session.last_used_at,
            })
            .collect();

        self.write_chat(chat_id, &chat).await?;

        Ok(sessions)
    }

    pub async fn begin_delete(&self, chat_id: i64, session_id: &str) -> Result<BeginDeleteResult> {
        if !is_safe_session_id(session_id) {
            bail!("unsafe telegram session id: {session_id}");
        }
        let _guard = self.write_lock.lock().await;
        let mut chat = self.read_chat(chat_id).await?;
        if chat.active_session_id == session_id {
            return Ok(BeginDeleteResult::Active);
        }
        let Some(record) = chat
            .sessions
            .iter_mut()
            .find(|session| session.id == session_id && !session.deleting)
        else {
            return Ok(BeginDeleteResult::NotFound);
        };
        record.deleting = true;
        self.write_chat(chat_id, &chat).await?;
        Ok(BeginDeleteResult::Ready {
            session_dir: self.chat_root(chat_id).join(session_id),
        })
    }

    pub async fn finish_delete(&self, chat_id: i64, session_id: &str) -> Result<()> {
        let _guard = self.write_lock.lock().await;
        let mut chat = self.read_chat(chat_id).await?;
        let session_dir = self.chat_root(chat_id).join(session_id);
        if fs::try_exists(&session_dir).await? {
            fs::remove_dir_all(&session_dir).await.with_context(|| {
                format!(
                    "failed to remove session directory: {}",
                    session_dir.display()
                )
            })?;
        }
        chat.sessions
            .retain(|session| !(session.id == session_id && session.deleting));
        self.write_chat(chat_id, &chat).await
    }

    async fn read_chat(&self, chat_id: i64) -> Result<ChatSessionIndex> {
        let index_path = self.index_path(chat_id);
        if !fs::try_exists(&index_path).await.with_context(|| {
            format!(
                "failed to inspect telegram sessions index: {}",
                index_path.display()
            )
        })? {
            if let Some(chat) = self.read_legacy_chat(chat_id).await? {
                self.migrate_legacy_session_files(chat_id, &chat).await?;
                return Ok(chat);
            }

            return Ok(ChatSessionIndex::new(
                legacy_session_id(chat_id),
                now_timestamp(),
            ));
        }

        let content = fs::read_to_string(&index_path).await.with_context(|| {
            format!(
                "failed to read telegram sessions index: {}",
                index_path.display()
            )
        })?;

        serde_json::from_str(&content).with_context(|| {
            format!(
                "failed to parse telegram sessions index: {}",
                index_path.display()
            )
        })
    }

    async fn write_chat(&self, chat_id: i64, chat: &ChatSessionIndex) -> Result<()> {
        let chat_root = self.chat_root(chat_id);
        fs::create_dir_all(&chat_root).await.with_context(|| {
            format!(
                "failed to create telegram session directory: {}",
                chat_root.display()
            )
        })?;

        let index_path = chat_root.join("index.json");
        let content =
            serde_json::to_string_pretty(chat).context("failed to serialize telegram sessions")?;
        let temp_path = index_path.with_extension("json.tmp");
        fs::write(&temp_path, content).await.with_context(|| {
            format!(
                "failed to write telegram sessions index: {}",
                temp_path.display()
            )
        })?;
        fs::rename(&temp_path, &index_path).await.with_context(|| {
            format!(
                "failed to replace telegram sessions index: {}",
                index_path.display()
            )
        })
    }

    async fn read_legacy_chat(&self, chat_id: i64) -> Result<Option<ChatSessionIndex>> {
        let legacy_path = self.legacy_state_path();
        if !fs::try_exists(&legacy_path).await.with_context(|| {
            format!(
                "failed to inspect legacy telegram sessions file: {}",
                legacy_path.display()
            )
        })? {
            return Ok(None);
        }

        let content = fs::read_to_string(&legacy_path).await.with_context(|| {
            format!(
                "failed to read legacy telegram sessions file: {}",
                legacy_path.display()
            )
        })?;
        let state =
            serde_json::from_str::<LegacyTelegramSessions>(&content).with_context(|| {
                format!(
                    "failed to parse legacy telegram sessions file: {}",
                    legacy_path.display()
                )
            })?;

        Ok(state.chats.get(&chat_key(chat_id)).cloned())
    }

    async fn migrate_legacy_session_files(
        &self,
        chat_id: i64,
        chat: &ChatSessionIndex,
    ) -> Result<()> {
        let chat_root = self.chat_root(chat_id);
        fs::create_dir_all(&chat_root).await.with_context(|| {
            format!(
                "failed to create telegram session directory: {}",
                chat_root.display()
            )
        })?;

        for session in &chat.sessions {
            let source = self.root.join(format!("{}.json", session.id));
            let destination = chat_root.join(format!("{}.json", session.id));
            if fs::try_exists(&destination).await.with_context(|| {
                format!(
                    "failed to inspect migrated telegram session file: {}",
                    destination.display()
                )
            })? {
                continue;
            }

            if fs::try_exists(&source).await.with_context(|| {
                format!(
                    "failed to inspect legacy telegram session file: {}",
                    source.display()
                )
            })? {
                fs::copy(&source, &destination).await.with_context(|| {
                    format!(
                        "failed to migrate telegram session file from {} to {}",
                        source.display(),
                        destination.display()
                    )
                })?;
            }
        }

        Ok(())
    }

    fn chat_root(&self, chat_id: i64) -> PathBuf {
        self.root.join(legacy_session_id(chat_id))
    }

    fn index_path(&self, chat_id: i64) -> PathBuf {
        self.chat_root(chat_id).join("index.json")
    }

    fn legacy_state_path(&self) -> PathBuf {
        self.root
            .parent()
            .map(|parent| parent.join("telegram-sessions.json"))
            .unwrap_or_else(|| PathBuf::from("telegram-sessions.json"))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TelegramSession {
    pub id: String,
    pub is_active: bool,
    pub created_at: u64,
    pub last_used_at: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BeginDeleteResult {
    NotFound,
    Active,
    Ready { session_dir: PathBuf },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct LegacyTelegramSessions {
    version: u32,
    chats: BTreeMap<String, ChatSessionIndex>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct ChatSessionIndex {
    active_session_id: String,
    sessions: Vec<ChatSessionRecord>,
}

impl ChatSessionIndex {
    fn new(session_id: String, created_at: u64) -> Self {
        Self {
            active_session_id: session_id.clone(),
            sessions: vec![ChatSessionRecord::new(session_id, created_at)],
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct ChatSessionRecord {
    id: String,
    created_at: u64,
    last_used_at: u64,
    #[serde(default)]
    deleting: bool,
}

impl ChatSessionRecord {
    fn new(id: String, created_at: u64) -> Self {
        Self {
            id,
            created_at,
            last_used_at: created_at,
            deleting: false,
        }
    }
}

fn touch_session(chat: &mut ChatSessionIndex, session_id: &str, timestamp: u64) {
    if let Some(session) = chat
        .sessions
        .iter_mut()
        .find(|session| session.id == session_id)
    {
        session.last_used_at = timestamp;
    }
}

fn unique_session_id(chat_id: i64, created_at: u64, sequence: usize) -> String {
    format!("{}-{created_at}-{sequence}", legacy_session_id(chat_id))
}

fn legacy_session_id(chat_id: i64) -> String {
    format!("telegram-chat-{chat_id}")
}

fn chat_key(chat_id: i64) -> String {
    chat_id.to_string()
}

fn now_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after unix epoch")
        .as_secs()
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

    use super::{BeginDeleteResult, TelegramSessionStore};

    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_store_root() -> PathBuf {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after unix epoch")
            .as_nanos();
        let counter = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);

        std::env::temp_dir().join(format!(
            "codrik-telegram-sessions-test-{}-{suffix}-{counter}",
            std::process::id()
        ))
    }

    #[tokio::test]
    async fn defaults_to_legacy_session_for_new_chat() -> Result<()> {
        let root = temp_store_root();
        let store = TelegramSessionStore::new(&root);

        let session_id = store.active_session_id(123).await?;

        assert_eq!(session_id, "telegram-chat-123");

        let sessions = store.list_sessions(123).await?;
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].id, "telegram-chat-123");
        assert!(sessions[0].is_active);

        fs::remove_dir_all(root).await.ok();

        Ok(())
    }

    #[tokio::test]
    async fn refuses_to_delete_active_session() -> Result<()> {
        let root = temp_store_root();
        let store = TelegramSessionStore::new(&root);
        let active = store.active_session_id(123).await?;

        assert_eq!(
            store.begin_delete(123, &active).await?,
            BeginDeleteResult::Active
        );

        fs::remove_dir_all(root).await.ok();
        Ok(())
    }

    #[tokio::test]
    async fn creates_new_active_session() -> Result<()> {
        let root = temp_store_root();
        let store = TelegramSessionStore::new(&root);

        let session_id = store.create_session(123).await?;
        let active_session_id = store.active_session_id(123).await?;

        assert_eq!(active_session_id, session_id);
        assert!(session_id.starts_with("telegram-chat-123-"));

        let sessions = store.list_sessions(123).await?;
        assert_eq!(sessions.len(), 2);
        assert_eq!(sessions[1].id, session_id);
        assert!(sessions[1].is_active);

        fs::remove_dir_all(root).await.ok();

        Ok(())
    }

    #[tokio::test]
    async fn switches_only_to_known_chat_session() -> Result<()> {
        let root = temp_store_root();
        let store = TelegramSessionStore::new(&root);
        let session_id = store.create_session(123).await?;
        store.create_session(456).await?;

        assert!(store.switch_session(123, "telegram-chat-123").await?);
        assert_eq!(store.active_session_id(123).await?, "telegram-chat-123");
        assert!(!store.switch_session(123, "telegram-chat-456").await?);
        assert!(store.switch_session(123, &session_id).await?);

        fs::remove_dir_all(root).await.ok();

        Ok(())
    }

    #[tokio::test]
    async fn persists_sessions_to_chat_index_json() -> Result<()> {
        let root = temp_store_root();
        let store = TelegramSessionStore::new(&root);
        let session_id = store.create_session(-100).await?;

        let restored = TelegramSessionStore::new(&root);
        assert_eq!(restored.active_session_id(-100).await?, session_id);

        let index_path = root.join("telegram-chat--100").join("index.json");
        let content = fs::read_to_string(&index_path).await?;
        assert!(content.contains("\"active_session_id\""));
        assert_eq!(store.session_root(-100), root.join("telegram-chat--100"));

        fs::remove_dir_all(root).await.ok();

        Ok(())
    }

    #[tokio::test]
    async fn migrates_legacy_chat_index_and_session_file() -> Result<()> {
        let base = temp_store_root();
        let root = base.join("sessions");
        fs::create_dir_all(&root).await?;
        fs::write(
            base.join("telegram-sessions.json"),
            r#"{
  "version": 1,
  "chats": {
    "123": {
      "active_session_id": "telegram-chat-123",
      "sessions": [
        {
          "id": "telegram-chat-123",
          "created_at": 10,
          "last_used_at": 20
        }
      ]
    }
  }
}"#,
        )
        .await?;
        fs::write(root.join("telegram-chat-123.json"), "[]").await?;

        let store = TelegramSessionStore::new(&root);
        assert_eq!(store.active_session_id(123).await?, "telegram-chat-123");

        let chat_root = root.join("telegram-chat-123");
        assert!(fs::try_exists(chat_root.join("index.json")).await?);
        assert!(fs::try_exists(chat_root.join("telegram-chat-123.json")).await?);

        fs::remove_dir_all(base).await.ok();

        Ok(())
    }
}
