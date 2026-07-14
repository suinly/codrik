use std::collections::HashSet;

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use tokio_rusqlite::{params, rusqlite::OptionalExtension};

use crate::{
    auth::LegacyAuthorizationSnapshot,
    runtime::{
        model::{ActorId, Timestamp},
        sqlite::SqliteRuntimeStore,
        store::{ImportOutcome, RuntimeActor, RuntimeAuthorizationStore},
    },
};

#[async_trait]
impl RuntimeAuthorizationStore for SqliteRuntimeStore {
    async fn import_legacy_authorization(
        &self,
        snapshot: LegacyAuthorizationSnapshot,
        now: Timestamp,
    ) -> Result<ImportOutcome> {
        let version = snapshot.version;
        let actors = snapshot
            .actors
            .into_iter()
            .map(|actor| {
                let mut seen = HashSet::new();
                let tools = actor
                    .tools
                    .into_iter()
                    .filter(|tool| seen.insert(tool.clone()))
                    .collect::<Vec<_>>();
                let tools_json = serde_json::to_string(&tools)?;
                Ok((actor.id, actor.enabled, tools_json, actor.identities))
            })
            .collect::<Result<Vec<_>>>()?;

        self.connection
            .call(move |connection| -> tokio_rusqlite::rusqlite::Result<ImportOutcome> {
                let transaction = connection
                    .transaction_with_behavior(tokio_rusqlite::rusqlite::TransactionBehavior::Immediate)?;
                let imported = transaction
                    .query_row(
                        "SELECT 1 FROM runtime_metadata WHERE key = 'legacy_auth_imported'",
                        [],
                        |_| Ok(()),
                    )
                    .optional()?
                    .is_some();
                if imported {
                    return Ok(ImportOutcome::AlreadyImported);
                }

                for (actor_id, enabled, tools_json, identities) in actors {
                    transaction.execute(
                        "INSERT INTO actors(id, enabled, tools_json, created_at) VALUES (?1, ?2, ?3, ?4)",
                        params![actor_id, enabled, tools_json, now.0],
                    )?;
                    for identity in identities {
                        transaction.execute(
                            "INSERT INTO identities(provider, subject, actor_id, username) VALUES (?1, ?2, ?3, ?4)",
                            params![identity.provider, identity.subject, actor_id, identity.username],
                        )?;
                    }
                }

                transaction.execute(
                    "INSERT INTO runtime_metadata(key, value) VALUES ('legacy_auth_imported', ?1)",
                    [version.to_string()],
                )?;
                transaction.commit()?;
                Ok(ImportOutcome::Imported)
            })
            .await
            .map_err(|error| anyhow!("failed to import legacy authorization: {error}"))
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
impl SqliteRuntimeStore {
    async fn legacy_import_marker_exists(&self) -> Result<bool> {
        self.connection
            .call(|connection| -> tokio_rusqlite::rusqlite::Result<bool> {
                Ok(connection
                    .query_row(
                        "SELECT 1 FROM runtime_metadata WHERE key = 'legacy_auth_imported'",
                        [],
                        |_| Ok(()),
                    )
                    .optional()?
                    .is_some())
            })
            .await
            .map_err(|error| anyhow!("failed to inspect legacy import marker: {error}"))
    }

    async fn actor_count(&self) -> Result<i64> {
        self.connection
            .call(|connection| {
                connection.query_row("SELECT COUNT(*) FROM actors", [], |row| row.get(0))
            })
            .await
            .map_err(|error| anyhow!("failed to count actors: {error}"))
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        auth::{LegacyActor, LegacyAuthorizationSnapshot, LegacyIdentity},
        runtime::{
            model::Timestamp,
            sqlite::SqliteRuntimeStore,
            store::{ImportOutcome, RuntimeAuthorizationStore},
        },
    };

    fn owner_snapshot() -> LegacyAuthorizationSnapshot {
        LegacyAuthorizationSnapshot {
            version: 1,
            actors: vec![LegacyActor {
                id: "actor:telegram:123".into(),
                enabled: true,
                tools: vec!["*".into(), "bash".into()],
                identities: vec![LegacyIdentity {
                    provider: "telegram".into(),
                    subject: "123".into(),
                    username: Some("owner".into()),
                }],
            }],
        }
    }

    #[tokio::test]
    async fn legacy_authorization_import_is_atomic_and_idempotent() {
        let store = SqliteRuntimeStore::open_in_memory().await.unwrap();
        let snapshot = owner_snapshot();

        assert_eq!(
            store
                .import_legacy_authorization(snapshot.clone(), Timestamp(10))
                .await
                .unwrap(),
            ImportOutcome::Imported
        );
        assert_eq!(
            store
                .import_legacy_authorization(snapshot, Timestamp(20))
                .await
                .unwrap(),
            ImportOutcome::AlreadyImported
        );

        let actor = store
            .resolve_identity("telegram", "123")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(actor.tools, vec!["*", "bash"]);
    }

    #[tokio::test]
    async fn conflicting_identity_rolls_back_entire_import() {
        let store = SqliteRuntimeStore::open_in_memory().await.unwrap();
        let identity = LegacyIdentity {
            provider: "telegram".into(),
            subject: "same".into(),
            username: None,
        };
        let snapshot = LegacyAuthorizationSnapshot {
            version: 1,
            actors: vec![
                LegacyActor {
                    id: "actor-a".into(),
                    enabled: true,
                    tools: vec!["*".into()],
                    identities: vec![identity.clone()],
                },
                LegacyActor {
                    id: "actor-b".into(),
                    enabled: true,
                    tools: vec!["*".into()],
                    identities: vec![identity],
                },
            ],
        };

        assert!(
            store
                .import_legacy_authorization(snapshot, Timestamp(10))
                .await
                .is_err()
        );
        assert!(!store.legacy_import_marker_exists().await.unwrap());
        assert_eq!(store.actor_count().await.unwrap(), 0);
    }
}
