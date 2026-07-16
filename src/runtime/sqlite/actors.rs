use anyhow::{Result, anyhow};
use async_trait::async_trait;
use tokio_rusqlite::{params, rusqlite::OptionalExtension};

use crate::runtime::{
    model::{ActorId, Timestamp},
    sqlite::{SqliteRuntimeStore, map_call_error},
    store::{ActorBootstrapOutcome, ActorStore, RuntimeActor},
};

#[async_trait]
impl ActorStore for SqliteRuntimeStore {
    async fn ensure_initial_actor(
        &self,
        id: &ActorId,
        tools: &[String],
        now: Timestamp,
    ) -> Result<ActorBootstrapOutcome> {
        let id = ActorId::parse_workspace_safe(id.as_str())?;
        let tools_json = serde_json::to_string(tools)?;
        let initialized = self
            .connection
            .call(|connection| {
                connection.query_row("SELECT EXISTS(SELECT 1 FROM actors)", [], |row| {
                    row.get::<_, bool>(0)
                })
            })
            .await
            .map_err(|error| anyhow!("failed to inspect runtime actors: {error}"))?;
        if initialized {
            return Ok(ActorBootstrapOutcome::AlreadyInitialized);
        }
        self.connection
            .call(move |connection| -> Result<ActorBootstrapOutcome> {
                let transaction = connection.transaction_with_behavior(
                    tokio_rusqlite::rusqlite::TransactionBehavior::Immediate,
                )?;
                let count = transaction.query_row("SELECT COUNT(*) FROM actors", [], |row| {
                    row.get::<_, i64>(0)
                })?;
                if count != 0 {
                    return Ok(ActorBootstrapOutcome::AlreadyInitialized);
                }
                transaction.execute(
                    "INSERT INTO actors(id, enabled, tools_json, created_at)
                         VALUES (?1, 1, ?2, ?3)",
                    params![id.as_str(), tools_json, now.0],
                )?;
                transaction.commit()?;
                Ok(ActorBootstrapOutcome::Created)
            })
            .await
            .map_err(map_call_error)
            .map_err(|error| anyhow!("failed to bootstrap runtime actor: {error}"))
    }

    async fn load_actor(&self, id: &ActorId) -> Result<Option<RuntimeActor>> {
        let id = id.clone();
        self.connection
            .call(move |connection| -> Result<Option<RuntimeActor>> {
                let actor = connection
                    .query_row(
                        "SELECT enabled, tools_json FROM actors WHERE id = ?1",
                        [id.as_str()],
                        |row| Ok((row.get::<_, bool>(0)?, row.get::<_, String>(1)?)),
                    )
                    .optional()?;
                actor
                    .map(|(enabled, tools_json)| {
                        Ok(RuntimeActor {
                            id,
                            enabled,
                            tools: serde_json::from_str(&tools_json)?,
                        })
                    })
                    .transpose()
            })
            .await
            .map_err(map_call_error)
    }

    async fn resolve_identity(
        &self,
        provider: &str,
        subject: &str,
    ) -> Result<Option<RuntimeActor>> {
        let provider = provider.to_string();
        let subject = subject.to_string();
        self.connection
            .call(move |connection| -> Result<Option<RuntimeActor>> {
                let row = connection
                    .query_row(
                        "SELECT actors.id, actors.enabled, actors.tools_json
                         FROM identities
                         JOIN actors ON actors.id = identities.actor_id
                         WHERE identities.provider = ?1 AND identities.subject = ?2",
                        params![provider, subject],
                        |row| {
                            Ok((
                                row.get::<_, String>(0)?,
                                row.get::<_, bool>(1)?,
                                row.get::<_, String>(2)?,
                            ))
                        },
                    )
                    .optional()?;
                let Some((actor_id, enabled, tools_json)) = row else {
                    return Ok(None);
                };
                Ok(Some(RuntimeActor {
                    id: ActorId::from_string(actor_id),
                    enabled,
                    tools: serde_json::from_str(&tools_json)?,
                }))
            })
            .await
            .map_err(|error| anyhow!("failed to resolve runtime identity: {error}"))
    }
}

#[cfg(test)]
mod tests {
    use anyhow::{Result, anyhow};

    use crate::runtime::{
        model::{ActorId, Timestamp},
        sqlite::SqliteRuntimeStore,
        store::{ActorBootstrapOutcome, ActorStore, RuntimeActor},
    };

    impl SqliteRuntimeStore {
        async fn actor_count_for_test(&self) -> Result<i64> {
            self.connection
                .call(|connection| {
                    connection.query_row("SELECT COUNT(*) FROM actors", [], |row| row.get(0))
                })
                .await
                .map_err(|error| anyhow!("failed to count actors: {error}"))
        }

        async fn actor_created_at_for_test(&self, id: &str) -> Result<i64> {
            let id = id.to_string();
            self.connection
                .call(move |connection| {
                    connection.query_row(
                        "SELECT created_at FROM actors WHERE id = ?1",
                        [id],
                        |row| row.get(0),
                    )
                })
                .await
                .map_err(|error| anyhow!("failed to load actor creation time: {error}"))
        }
    }

    #[tokio::test]
    async fn empty_store_bootstraps_enabled_actor_with_tools_and_timestamp() -> Result<()> {
        let store = SqliteRuntimeStore::open_in_memory().await?;
        let actor = ActorId::parse_workspace_safe(" actor:local:owner ")?;

        assert_eq!(
            store
                .ensure_initial_actor(&actor, &["*".to_string()], Timestamp(42))
                .await?,
            ActorBootstrapOutcome::Created
        );
        assert_eq!(
            store.load_actor(&actor).await?,
            Some(RuntimeActor {
                id: actor,
                enabled: true,
                tools: vec!["*".to_string()],
            })
        );
        assert_eq!(
            store.actor_created_at_for_test("actor:local:owner").await?,
            42
        );
        Ok(())
    }

    #[tokio::test]
    async fn bootstrap_is_idempotent_for_the_same_actor() -> Result<()> {
        let store = SqliteRuntimeStore::open_in_memory().await?;
        let actor = ActorId::parse_workspace_safe("actor:local:owner")?;

        assert_eq!(
            store
                .ensure_initial_actor(&actor, &["*".into()], Timestamp(1))
                .await?,
            ActorBootstrapOutcome::Created
        );
        assert_eq!(
            store
                .ensure_initial_actor(&actor, &["bash".into()], Timestamp(2))
                .await?,
            ActorBootstrapOutcome::AlreadyInitialized
        );
        assert_eq!(store.load_actor(&actor).await?.unwrap().tools, vec!["*"]);
        assert_eq!(store.actor_created_at_for_test(actor.as_str()).await?, 1);
        Ok(())
    }

    #[tokio::test]
    async fn nonempty_store_does_not_bootstrap_a_different_actor() -> Result<()> {
        let store = SqliteRuntimeStore::open_in_memory().await?;
        let owner = ActorId::parse_workspace_safe("actor:local:owner")?;
        let typo = ActorId::parse_workspace_safe("actor:local:typo")?;
        store
            .ensure_initial_actor(&owner, &["*".into()], Timestamp(1))
            .await?;

        assert_eq!(
            store
                .ensure_initial_actor(&typo, &["*".into()], Timestamp(2))
                .await?,
            ActorBootstrapOutcome::AlreadyInitialized
        );
        assert!(store.load_actor(&typo).await?.is_none());
        assert_eq!(store.actor_count_for_test().await?, 1);
        Ok(())
    }
}
