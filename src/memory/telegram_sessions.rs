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
    path: PathBuf,
    write_lock: Arc<Mutex<()>>,
}

impl TelegramSessionStore {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            write_lock: Arc::new(Mutex::new(())),
        }
    }

    pub async fn active_session_id(&self, chat_id: i64) -> Result<String> {
        let _guard = self.write_lock.lock().await;
        let mut state = self.read_state().await?;
        let chat = state
            .chats
            .entry(chat_key(chat_id))
            .or_insert_with(|| StoredChat::new(legacy_session_id(chat_id), now_timestamp()));

        if !chat
            .sessions
            .iter()
            .any(|session| session.id == chat.active_session_id)
        {
            chat.sessions.push(StoredSession::new(
                chat.active_session_id.clone(),
                now_timestamp(),
            ));
        }

        let active_session_id = chat.active_session_id.clone();
        touch_session(chat, &active_session_id, now_timestamp());
        self.write_state(&state).await?;

        Ok(active_session_id)
    }

    pub async fn create_session(&self, chat_id: i64) -> Result<String> {
        let _guard = self.write_lock.lock().await;
        let mut state = self.read_state().await?;
        let chat = state
            .chats
            .entry(chat_key(chat_id))
            .or_insert_with(|| StoredChat::new(legacy_session_id(chat_id), now_timestamp()));

        let created_at = now_timestamp();
        let session_id = unique_session_id(chat_id, created_at, chat.sessions.len() + 1);
        chat.sessions
            .push(StoredSession::new(session_id.clone(), created_at));
        chat.active_session_id = session_id.clone();

        self.write_state(&state).await?;

        Ok(session_id)
    }

    pub async fn switch_session(&self, chat_id: i64, session_id: &str) -> Result<bool> {
        if !is_safe_session_id(session_id) {
            bail!("unsafe telegram session id: {session_id}");
        }

        let _guard = self.write_lock.lock().await;
        let mut state = self.read_state().await?;
        let Some(chat) = state.chats.get_mut(&chat_key(chat_id)) else {
            return Ok(false);
        };

        if !chat.sessions.iter().any(|session| session.id == session_id) {
            return Ok(false);
        }

        chat.active_session_id = session_id.to_string();
        touch_session(chat, session_id, now_timestamp());
        self.write_state(&state).await?;

        Ok(true)
    }

    pub async fn list_sessions(&self, chat_id: i64) -> Result<Vec<TelegramSession>> {
        let _guard = self.write_lock.lock().await;
        let mut state = self.read_state().await?;
        let chat = state
            .chats
            .entry(chat_key(chat_id))
            .or_insert_with(|| StoredChat::new(legacy_session_id(chat_id), now_timestamp()));

        let sessions = chat
            .sessions
            .iter()
            .map(|session| TelegramSession {
                id: session.id.clone(),
                is_active: session.id == chat.active_session_id,
                created_at: session.created_at,
                last_used_at: session.last_used_at,
            })
            .collect();

        self.write_state(&state).await?;

        Ok(sessions)
    }

    async fn read_state(&self) -> Result<StoredTelegramSessions> {
        if !fs::try_exists(&self.path).await.with_context(|| {
            format!(
                "failed to inspect telegram sessions file: {}",
                self.path.display()
            )
        })? {
            return Ok(StoredTelegramSessions::default());
        }

        let content = fs::read_to_string(&self.path).await.with_context(|| {
            format!(
                "failed to read telegram sessions file: {}",
                self.path.display()
            )
        })?;

        serde_json::from_str(&content).with_context(|| {
            format!(
                "failed to parse telegram sessions file: {}",
                self.path.display()
            )
        })
    }

    async fn write_state(&self, state: &StoredTelegramSessions) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).await.with_context(|| {
                format!(
                    "failed to create telegram sessions directory: {}",
                    parent.display()
                )
            })?;
        }

        let content =
            serde_json::to_string_pretty(state).context("failed to serialize telegram sessions")?;
        fs::write(&self.path, content).await.with_context(|| {
            format!(
                "failed to write telegram sessions file: {}",
                self.path.display()
            )
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TelegramSession {
    pub id: String,
    pub is_active: bool,
    pub created_at: u64,
    pub last_used_at: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct StoredTelegramSessions {
    version: u32,
    chats: BTreeMap<String, StoredChat>,
}

impl Default for StoredTelegramSessions {
    fn default() -> Self {
        Self {
            version: 1,
            chats: BTreeMap::new(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct StoredChat {
    active_session_id: String,
    sessions: Vec<StoredSession>,
}

impl StoredChat {
    fn new(session_id: String, created_at: u64) -> Self {
        Self {
            active_session_id: session_id.clone(),
            sessions: vec![StoredSession::new(session_id, created_at)],
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct StoredSession {
    id: String,
    created_at: u64,
    last_used_at: u64,
}

impl StoredSession {
    fn new(id: String, created_at: u64) -> Self {
        Self {
            id,
            created_at,
            last_used_at: created_at,
        }
    }
}

fn touch_session(chat: &mut StoredChat, session_id: &str, timestamp: u64) {
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

    use super::TelegramSessionStore;

    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_store_path() -> PathBuf {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after unix epoch")
            .as_nanos();
        let counter = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);

        std::env::temp_dir().join(format!(
            "codrik-telegram-sessions-test-{}-{suffix}-{counter}.json",
            std::process::id()
        ))
    }

    #[tokio::test]
    async fn defaults_to_legacy_session_for_new_chat() -> Result<()> {
        let path = temp_store_path();
        let store = TelegramSessionStore::new(&path);

        let session_id = store.active_session_id(123).await?;

        assert_eq!(session_id, "telegram-chat-123");

        let sessions = store.list_sessions(123).await?;
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].id, "telegram-chat-123");
        assert!(sessions[0].is_active);

        fs::remove_file(path).await.ok();

        Ok(())
    }

    #[tokio::test]
    async fn creates_new_active_session() -> Result<()> {
        let path = temp_store_path();
        let store = TelegramSessionStore::new(&path);

        let session_id = store.create_session(123).await?;
        let active_session_id = store.active_session_id(123).await?;

        assert_eq!(active_session_id, session_id);
        assert!(session_id.starts_with("telegram-chat-123-"));

        let sessions = store.list_sessions(123).await?;
        assert_eq!(sessions.len(), 2);
        assert_eq!(sessions[1].id, session_id);
        assert!(sessions[1].is_active);

        fs::remove_file(path).await.ok();

        Ok(())
    }

    #[tokio::test]
    async fn switches_only_to_known_chat_session() -> Result<()> {
        let path = temp_store_path();
        let store = TelegramSessionStore::new(&path);
        let session_id = store.create_session(123).await?;
        store.create_session(456).await?;

        assert!(store.switch_session(123, "telegram-chat-123").await?);
        assert_eq!(store.active_session_id(123).await?, "telegram-chat-123");
        assert!(!store.switch_session(123, "telegram-chat-456").await?);
        assert!(store.switch_session(123, &session_id).await?);

        fs::remove_file(path).await.ok();

        Ok(())
    }

    #[tokio::test]
    async fn persists_sessions_to_json() -> Result<()> {
        let path = temp_store_path();
        let store = TelegramSessionStore::new(&path);
        let session_id = store.create_session(-100).await?;

        let restored = TelegramSessionStore::new(&path);
        assert_eq!(restored.active_session_id(-100).await?, session_id);

        let content = fs::read_to_string(&path).await?;
        assert!(content.contains("\"-100\""));
        assert!(content.contains("\"active_session_id\""));

        fs::remove_file(path).await.ok();

        Ok(())
    }
}
