use std::collections::BTreeSet;

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use tokio_rusqlite::{params, rusqlite::OptionalExtension};

use crate::runtime::{
    model::{ActorId, Timestamp},
    sqlite::{SqliteRuntimeStore, map_call_error},
    store::{
        ActorAdminStore, ActorBootstrapOutcome, ActorCreateOutcome, ActorDetails,
        ActorMutationOutcome, ActorStore, LinkIdentity, RuntimeActor,
    },
};

fn runtime_actor(id: String, enabled: bool, tools_json: String) -> Result<RuntimeActor> {
    Ok(RuntimeActor {
        id: ActorId::from_string(id),
        enabled,
        tools: serde_json::from_str(&tools_json)?,
    })
}

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

#[async_trait]
impl ActorAdminStore for SqliteRuntimeStore {
    async fn list_actors(&self) -> Result<Vec<RuntimeActor>> {
        self.connection
            .call(|connection| -> Result<Vec<RuntimeActor>> {
                let mut statement =
                    connection.prepare("SELECT id, enabled, tools_json FROM actors ORDER BY id")?;
                statement
                    .query_map([], |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, bool>(1)?,
                            row.get::<_, String>(2)?,
                        ))
                    })?
                    .map(|row| {
                        let (id, enabled, tools_json) = row?;
                        runtime_actor(id, enabled, tools_json)
                    })
                    .collect()
            })
            .await
            .map_err(map_call_error)
    }

    async fn actor_details(&self, actor: &ActorId) -> Result<Option<ActorDetails>> {
        let actor = actor.clone();
        self.connection
            .call(move |connection| -> Result<Option<ActorDetails>> {
                let row = connection
                    .query_row(
                        "SELECT enabled, tools_json FROM actors WHERE id = ?1",
                        [actor.as_str()],
                        |row| Ok((row.get::<_, bool>(0)?, row.get::<_, String>(1)?)),
                    )
                    .optional()?;
                let Some((enabled, tools_json)) = row else {
                    return Ok(None);
                };
                let mut statement = connection.prepare(
                    "SELECT provider, subject, username
                     FROM identities WHERE actor_id = ?1
                     ORDER BY provider, subject",
                )?;
                let identities = statement
                    .query_map([actor.as_str()], |row| {
                        Ok(LinkIdentity {
                            provider: row.get(0)?,
                            subject: row.get(1)?,
                            username: row.get(2)?,
                        })
                    })?
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                let has_active_work = connection.query_row(
                    "SELECT EXISTS(
                        SELECT 1 FROM actor_leases WHERE actor_id = ?1
                        UNION ALL
                        SELECT 1 FROM runs WHERE actor_id = ?1 AND state = 'active'
                        UNION ALL
                        SELECT 1 FROM work_items
                        WHERE actor_id = ?1
                          AND state NOT IN (
                            'completed', 'cancelled', 'failed_terminal',
                            'blocked_unknown_outcome', 'blocked_malformed'
                          )
                     )",
                    [actor.as_str()],
                    |row| row.get(0),
                )?;
                Ok(Some(ActorDetails {
                    actor: runtime_actor(actor.to_string(), enabled, tools_json)?,
                    identities,
                    has_active_work,
                }))
            })
            .await
            .map_err(map_call_error)
    }

    async fn create_actor(&self, actor: &ActorId, now: Timestamp) -> Result<ActorCreateOutcome> {
        let actor = ActorId::parse_workspace_safe(actor.as_str())?;
        self.connection
            .call(move |connection| -> Result<ActorCreateOutcome> {
                let transaction = connection.transaction_with_behavior(
                    tokio_rusqlite::rusqlite::TransactionBehavior::Immediate,
                )?;
                let created = transaction.execute(
                    "INSERT OR IGNORE INTO actors(id, enabled, tools_json, created_at)
                     VALUES (?1, 1, '[]', ?2)",
                    params![actor.as_str(), now.0],
                )? == 1;
                let (enabled, tools_json) = transaction.query_row(
                    "SELECT enabled, tools_json FROM actors WHERE id = ?1",
                    [actor.as_str()],
                    |row| Ok((row.get::<_, bool>(0)?, row.get::<_, String>(1)?)),
                )?;
                let actor = runtime_actor(actor.to_string(), enabled, tools_json)?;
                transaction.commit()?;
                Ok(if created {
                    ActorCreateOutcome::Created(actor)
                } else {
                    ActorCreateOutcome::Existing(actor)
                })
            })
            .await
            .map_err(map_call_error)
    }

    async fn set_actor_enabled(
        &self,
        actor: &ActorId,
        enabled: bool,
    ) -> Result<Option<ActorMutationOutcome>> {
        let actor = actor.clone();
        self.connection
            .call(move |connection| -> Result<Option<ActorMutationOutcome>> {
                let transaction = connection.transaction_with_behavior(
                    tokio_rusqlite::rusqlite::TransactionBehavior::Immediate,
                )?;
                let row = transaction
                    .query_row(
                        "SELECT enabled, tools_json FROM actors WHERE id = ?1",
                        [actor.as_str()],
                        |row| Ok((row.get::<_, bool>(0)?, row.get::<_, String>(1)?)),
                    )
                    .optional()?;
                let Some((current, tools_json)) = row else {
                    return Ok(None);
                };
                let changed = current != enabled;
                if changed {
                    transaction.execute(
                        "UPDATE actors SET enabled = ?2 WHERE id = ?1",
                        params![actor.as_str(), enabled],
                    )?;
                }
                let outcome = ActorMutationOutcome {
                    actor: runtime_actor(actor.to_string(), enabled, tools_json)?,
                    changed,
                };
                transaction.commit()?;
                Ok(Some(outcome))
            })
            .await
            .map_err(map_call_error)
    }

    async fn grant_actor_tool(
        &self,
        actor: &ActorId,
        tool: &str,
    ) -> Result<Option<ActorMutationOutcome>> {
        mutate_actor_tools(&self.connection, actor.clone(), tool.to_owned(), true).await
    }

    async fn revoke_actor_tool(
        &self,
        actor: &ActorId,
        tool: &str,
    ) -> Result<Option<ActorMutationOutcome>> {
        mutate_actor_tools(&self.connection, actor.clone(), tool.to_owned(), false).await
    }
}

async fn mutate_actor_tools(
    connection: &tokio_rusqlite::Connection,
    actor: ActorId,
    tool: String,
    grant: bool,
) -> Result<Option<ActorMutationOutcome>> {
    connection
        .call(move |connection| -> Result<Option<ActorMutationOutcome>> {
            let transaction = connection.transaction_with_behavior(
                tokio_rusqlite::rusqlite::TransactionBehavior::Immediate,
            )?;
            let row = transaction
                .query_row(
                    "SELECT enabled, tools_json FROM actors WHERE id = ?1",
                    [actor.as_str()],
                    |row| Ok((row.get::<_, bool>(0)?, row.get::<_, String>(1)?)),
                )
                .optional()?;
            let Some((enabled, tools_json)) = row else {
                return Ok(None);
            };
            let mut tools = serde_json::from_str::<BTreeSet<String>>(&tools_json)?;
            let changed = if grant {
                tools.insert(tool)
            } else {
                tools.remove(&tool)
            };
            let tools_json = serde_json::to_string(&tools)?;
            if changed {
                transaction.execute(
                    "UPDATE actors SET tools_json = ?2 WHERE id = ?1",
                    params![actor.as_str(), tools_json],
                )?;
            }
            let outcome = ActorMutationOutcome {
                actor: runtime_actor(actor.to_string(), enabled, tools_json)?,
                changed,
            };
            transaction.commit()?;
            Ok(Some(outcome))
        })
        .await
        .map_err(map_call_error)
}

#[cfg(test)]
impl SqliteRuntimeStore {
    pub(crate) async fn seed_actors_for_test(
        &self,
        seed: crate::test_fixtures::ActorSeedSet,
        now: Timestamp,
    ) -> Result<()> {
        let actors = seed
            .actors
            .into_iter()
            .map(|actor| {
                let tools_json = serde_json::to_string(&actor.tools)?;
                Ok((actor.id, actor.enabled, tools_json, actor.identities))
            })
            .collect::<Result<Vec<_>>>()?;
        self.connection
            .call(move |connection| -> Result<()> {
                let transaction = connection.transaction_with_behavior(
                    tokio_rusqlite::rusqlite::TransactionBehavior::Immediate,
                )?;
                for (actor_id, enabled, tools_json, identities) in actors {
                    transaction.execute(
                        "INSERT INTO actors(id, enabled, tools_json, created_at)
                         VALUES (?1, ?2, ?3, ?4)",
                        params![actor_id, enabled, tools_json, now.0],
                    )?;
                    for identity in identities {
                        transaction.execute(
                            "INSERT INTO identities(provider, subject, actor_id, username)
                             VALUES (?1, ?2, ?3, ?4)",
                            params![
                                identity.provider,
                                identity.subject,
                                actor_id,
                                identity.username
                            ],
                        )?;
                    }
                }
                transaction.commit()?;
                Ok(())
            })
            .await
            .map_err(map_call_error)
            .map_err(|error| anyhow!("failed to seed runtime actors for test: {error}"))
    }
}

#[cfg(test)]
mod tests {
    use anyhow::{Result, anyhow};

    use crate::runtime::{
        model::{ActorId, Timestamp},
        sqlite::SqliteRuntimeStore,
        store::{
            ActorAdminStore, ActorBootstrapOutcome, ActorCreateOutcome, ActorStore, RuntimeActor,
        },
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

    #[tokio::test]
    async fn actor_admin_create_list_and_show_are_stable() -> Result<()> {
        let store = SqliteRuntimeStore::open_in_memory().await?;
        let bob = ActorId::parse_workspace_safe("bob")?;
        let alice = ActorId::parse_workspace_safe("alice")?;

        assert!(matches!(
            store.create_actor(&bob, Timestamp(10)).await?,
            ActorCreateOutcome::Created(RuntimeActor {
                enabled: true,
                ref tools,
                ..
            }) if tools.is_empty()
        ));
        assert!(matches!(
            store.create_actor(&bob, Timestamp(11)).await?,
            ActorCreateOutcome::Existing(_)
        ));
        store.create_actor(&alice, Timestamp(12)).await?;

        assert_eq!(
            store
                .list_actors()
                .await?
                .into_iter()
                .map(|actor| actor.id)
                .collect::<Vec<_>>(),
            vec![alice, bob.clone()]
        );
        let details = store.actor_details(&bob).await?.unwrap();
        assert_eq!(details.actor.id, bob);
        assert!(details.identities.is_empty());
        assert!(!details.has_active_work);
        Ok(())
    }

    #[tokio::test]
    async fn actor_admin_enable_and_tools_are_idempotent_and_sorted() -> Result<()> {
        let store = SqliteRuntimeStore::open_in_memory().await?;
        let actor = ActorId::parse_workspace_safe("alice")?;
        store.create_actor(&actor, Timestamp(10)).await?;

        assert!(
            store
                .set_actor_enabled(&actor, false)
                .await?
                .unwrap()
                .changed
        );
        assert!(
            !store
                .set_actor_enabled(&actor, false)
                .await?
                .unwrap()
                .changed
        );
        store.grant_actor_tool(&actor, "bash").await?;
        store.grant_actor_tool(&actor, "*").await?;
        assert_eq!(
            store.load_actor(&actor).await?.unwrap().tools,
            vec!["*", "bash"]
        );
        assert!(
            store
                .revoke_actor_tool(&actor, "bash")
                .await?
                .unwrap()
                .changed
        );
        assert!(
            !store
                .revoke_actor_tool(&actor, "bash")
                .await?
                .unwrap()
                .changed
        );
        Ok(())
    }

    #[tokio::test]
    async fn seeded_identity_resolves_to_its_actor() -> Result<()> {
        let store = SqliteRuntimeStore::open_in_memory().await?;
        store
            .seed_actors_for_test(
                crate::test_fixtures::ActorSeedSet {
                    actors: vec![crate::test_fixtures::ActorSeed {
                        id: "actor:telegram:123".into(),
                        enabled: true,
                        tools: vec!["*".into(), "bash".into()],
                        identities: vec![crate::test_fixtures::IdentitySeed {
                            provider: "telegram".into(),
                            subject: "123".into(),
                            username: Some("owner".into()),
                        }],
                    }],
                },
                Timestamp(10),
            )
            .await?;

        assert_eq!(
            store.resolve_identity("telegram", "123").await?,
            Some(RuntimeActor {
                id: ActorId::from_string("actor:telegram:123"),
                enabled: true,
                tools: vec!["*".into(), "bash".into()],
            })
        );
        Ok(())
    }
}
