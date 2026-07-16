use std::{
    collections::{BTreeMap, HashSet},
    path::PathBuf,
    sync::Arc,
};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use tokio::{fs, sync::Mutex};

const BOOTSTRAP_TOOLS: &[&str] = &["*"];

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GatewayIdentity {
    pub provider: String,
    pub subject: String,
    pub username: Option<String>,
}

impl GatewayIdentity {
    pub fn new(
        provider: impl Into<String>,
        subject: impl Into<String>,
        username: Option<String>,
    ) -> Self {
        Self {
            provider: provider.into(),
            subject: subject.into(),
            username,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthorizedActor {
    pub id: String,
    pub tools: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct LegacyAuthorizationSnapshot {
    pub version: u32,
    pub actors: Vec<LegacyActor>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct LegacyActor {
    pub id: String,
    pub enabled: bool,
    pub tools: Vec<String>,
    pub identities: Vec<LegacyIdentity>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct LegacyIdentity {
    pub provider: String,
    pub subject: String,
    pub username: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AuthDecision {
    Authorized(AuthorizedActor),
    Denied,
}

#[derive(Clone, Debug)]
pub struct AuthorizationStore {
    path: PathBuf,
    write_lock: Arc<Mutex<()>>,
}

impl AuthorizationStore {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            write_lock: Arc::new(Mutex::new(())),
        }
    }

    pub async fn start(&self, identity: GatewayIdentity) -> Result<AuthDecision> {
        let _guard = self.write_lock.lock().await;
        let mut users = self.read_users().await?;
        let is_first_actor = users.actors.is_empty();
        let actor_id = actor_id_for(&identity);

        users
            .actors
            .entry(actor_id.clone())
            .and_modify(|actor| actor.refresh_identity(&identity))
            .or_insert_with(|| StoredActor::new(&identity, is_first_actor));

        self.write_users(&users).await?;

        Ok(decision_for(actor_id, users))
    }

    pub async fn authorize(&self, identity: &GatewayIdentity) -> Result<AuthDecision> {
        let users = self.read_users().await?;
        Ok(decision_for(actor_id_for(identity), users))
    }

    pub(crate) async fn snapshot(&self) -> Result<LegacyAuthorizationSnapshot> {
        Ok(LegacyAuthorizationSnapshot::from(self.read_users().await?))
    }

    pub async fn has_actors(&self) -> Result<bool> {
        Ok(!self.read_users().await?.actors.is_empty())
    }

    pub async fn actor_is_enabled(&self, actor_id: &str) -> Result<bool> {
        Ok(self
            .read_users()
            .await?
            .actors
            .get(actor_id)
            .is_some_and(|actor| actor.enabled))
    }

    async fn read_users(&self) -> Result<StoredUsers> {
        if !fs::try_exists(&self.path)
            .await
            .with_context(|| format!("failed to inspect users file: {}", self.path.display()))?
        {
            return Ok(StoredUsers::default());
        }

        let content = fs::read_to_string(&self.path)
            .await
            .with_context(|| format!("failed to read users file: {}", self.path.display()))?;

        serde_json::from_str(&content)
            .with_context(|| format!("failed to parse users file: {}", self.path.display()))
    }

    async fn write_users(&self, users: &StoredUsers) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).await.with_context(|| {
                format!("failed to create users directory: {}", parent.display())
            })?;
        }

        let content = serde_json::to_string_pretty(users).context("failed to serialize users")?;
        fs::write(&self.path, content)
            .await
            .with_context(|| format!("failed to write users file: {}", self.path.display()))
    }

    #[cfg(test)]
    fn path(&self) -> &std::path::Path {
        &self.path
    }
}

fn actor_id_for(identity: &GatewayIdentity) -> String {
    format!("actor:{}:{}", identity.provider, identity.subject)
}

fn decision_for(actor_id: String, users: StoredUsers) -> AuthDecision {
    let Some(actor) = users.actors.get(&actor_id) else {
        return AuthDecision::Denied;
    };

    if !actor.enabled {
        return AuthDecision::Denied;
    }

    AuthDecision::Authorized(AuthorizedActor {
        id: actor_id,
        tools: actor.tools.clone(),
    })
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct StoredUsers {
    version: u32,
    actors: BTreeMap<String, StoredActor>,
}

impl Default for StoredUsers {
    fn default() -> Self {
        Self {
            version: 1,
            actors: BTreeMap::new(),
        }
    }
}

impl From<StoredUsers> for LegacyAuthorizationSnapshot {
    fn from(users: StoredUsers) -> Self {
        Self {
            version: users.version,
            actors: users
                .actors
                .into_iter()
                .map(|(id, actor)| LegacyActor {
                    id,
                    enabled: actor.enabled,
                    tools: actor.tools,
                    identities: actor
                        .identities
                        .into_iter()
                        .map(|identity| LegacyIdentity {
                            provider: identity.provider,
                            subject: identity.subject,
                            username: identity.username,
                        })
                        .collect(),
                })
                .collect(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct StoredActor {
    enabled: bool,
    display_name: Option<String>,
    identities: Vec<StoredIdentity>,
    tools: Vec<String>,
}

impl StoredActor {
    fn new(identity: &GatewayIdentity, enabled: bool) -> Self {
        Self {
            enabled,
            display_name: identity.username.clone(),
            identities: vec![StoredIdentity::from(identity)],
            tools: default_tools_for_enabled(enabled),
        }
    }

    fn refresh_identity(&mut self, identity: &GatewayIdentity) {
        self.display_name = identity
            .username
            .clone()
            .or_else(|| self.display_name.clone());

        let stored = StoredIdentity::from(identity);
        let existing = self
            .identities
            .iter()
            .position(|item| item.provider == stored.provider && item.subject == stored.subject);

        if let Some(index) = existing {
            self.identities[index] = stored;
        } else {
            self.identities.push(stored);
        }

        let mut seen = HashSet::new();
        self.tools.retain(|tool| seen.insert(tool.clone()));
    }
}

fn default_tools_for_enabled(enabled: bool) -> Vec<String> {
    if !enabled {
        return Vec::new();
    }

    BOOTSTRAP_TOOLS.iter().map(ToString::to_string).collect()
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct StoredIdentity {
    provider: String,
    subject: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    username: Option<String>,
}

impl From<&GatewayIdentity> for StoredIdentity {
    fn from(identity: &GatewayIdentity) -> Self {
        Self {
            provider: identity.provider.clone(),
            subject: identity.subject.clone(),
            username: identity.username.clone(),
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
    use tokio::fs;

    use super::*;

    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_users_path() -> PathBuf {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after unix epoch")
            .as_nanos();
        let counter = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);

        std::env::temp_dir()
            .join(format!(
                "codrik-auth-test-{}-{suffix}-{counter}",
                std::process::id()
            ))
            .join("users.json")
    }

    fn telegram_identity(subject: &str, username: &str) -> GatewayIdentity {
        GatewayIdentity::new("telegram", subject, Some(username.to_string()))
    }

    #[tokio::test]
    async fn first_start_bootstraps_enabled_actor() -> Result<()> {
        let store = AuthorizationStore::new(temp_users_path());

        let decision = store.start(telegram_identity("123", "SomeUser")).await?;

        assert_eq!(
            decision,
            AuthDecision::Authorized(AuthorizedActor {
                id: "actor:telegram:123".to_string(),
                tools: vec!["*".to_string()],
            })
        );

        let content = fs::read_to_string(store.path()).await?;
        assert!(content.contains("\"enabled\": true"));
        assert!(content.contains("\"provider\": \"telegram\""));

        fs::remove_dir_all(store.path().parent().unwrap())
            .await
            .ok();
        Ok(())
    }

    #[tokio::test]
    async fn later_start_creates_pending_actor() -> Result<()> {
        let store = AuthorizationStore::new(temp_users_path());
        store.start(telegram_identity("123", "Owner")).await?;

        let decision = store.start(telegram_identity("456", "Guest")).await?;

        assert_eq!(decision, AuthDecision::Denied);

        let content = fs::read_to_string(store.path()).await?;
        assert!(content.contains("\"actor:telegram:456\""));
        assert!(content.contains("\"enabled\": false"));
        assert!(content.contains("\"tools\": []"));

        fs::remove_dir_all(store.path().parent().unwrap())
            .await
            .ok();
        Ok(())
    }

    #[tokio::test]
    async fn start_refreshes_metadata_without_overwriting_manual_permissions() -> Result<()> {
        let store = AuthorizationStore::new(temp_users_path());
        store.start(telegram_identity("123", "Owner")).await?;

        let mut users = store.read_users().await?;
        let actor = users.actors.get_mut("actor:telegram:123").unwrap();
        actor.enabled = false;
        actor.tools = vec!["custom".to_string()];
        store.write_users(&users).await?;

        let decision = store.start(telegram_identity("123", "Renamed")).await?;

        assert_eq!(decision, AuthDecision::Denied);

        let users = store.read_users().await?;
        let actor = users.actors.get("actor:telegram:123").unwrap();
        assert_eq!(actor.display_name.as_deref(), Some("Renamed"));
        assert_eq!(actor.tools, vec!["custom"]);

        fs::remove_dir_all(store.path().parent().unwrap())
            .await
            .ok();
        Ok(())
    }

    #[tokio::test]
    async fn unknown_or_disabled_identity_is_denied() -> Result<()> {
        let store = AuthorizationStore::new(temp_users_path());

        assert_eq!(
            store
                .authorize(&telegram_identity("404", "Missing"))
                .await?,
            AuthDecision::Denied
        );

        store.start(telegram_identity("123", "Owner")).await?;
        let mut users = store.read_users().await?;
        users.actors.get_mut("actor:telegram:123").unwrap().enabled = false;
        store.write_users(&users).await?;

        assert_eq!(
            store.authorize(&telegram_identity("123", "Owner")).await?,
            AuthDecision::Denied
        );

        fs::remove_dir_all(store.path().parent().unwrap())
            .await
            .ok();
        Ok(())
    }

    #[tokio::test]
    async fn snapshot_preserves_actor_permissions_and_identities() -> Result<()> {
        let store = AuthorizationStore::new(temp_users_path());
        store.start(telegram_identity("123", "Owner")).await?;
        let mut users = store.read_users().await?;
        users.actors.get_mut("actor:telegram:123").unwrap().tools =
            vec!["*".to_string(), "bash".to_string()];
        store.write_users(&users).await?;

        let snapshot = store.snapshot().await?;

        assert_eq!(snapshot.version, 1);
        assert_eq!(snapshot.actors.len(), 1);
        assert_eq!(snapshot.actors[0].id, "actor:telegram:123");
        assert_eq!(snapshot.actors[0].tools, vec!["*", "bash"]);
        assert_eq!(snapshot.actors[0].identities[0].provider, "telegram");
        assert_eq!(snapshot.actors[0].identities[0].subject, "123");

        fs::remove_dir_all(store.path().parent().unwrap())
            .await
            .ok();
        Ok(())
    }
}
