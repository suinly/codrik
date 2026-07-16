use std::collections::HashSet;

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use tokio_rusqlite::{params, rusqlite::OptionalExtension};

use crate::{
    auth::LegacyAuthorizationSnapshot,
    runtime::{
        model::Timestamp,
        sqlite::SqliteRuntimeStore,
        store::{
            ImportOutcome, IngressOutcome, IngressStore, NewInboundEvent, RuntimeAuthorizationStore,
        },
    },
};

#[async_trait]
impl RuntimeAuthorizationStore for SqliteRuntimeStore {
    async fn legacy_authorization_imported(&self) -> Result<bool> {
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
            .map_err(|error| anyhow!("failed to inspect legacy authorization marker: {error}"))
    }

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
}

#[async_trait]
impl IngressStore for SqliteRuntimeStore {
    async fn ingest(&self, event: NewInboundEvent, now: Timestamp) -> Result<IngressOutcome> {
        let (audience_kind, audience_address) = encode_audience(&event.audience)?;
        let kind = encode_event_kind(event.kind);
        self.connection
            .call(move |connection| -> tokio_rusqlite::rusqlite::Result<IngressOutcome> {
                let transaction = connection
                    .transaction_with_behavior(tokio_rusqlite::rusqlite::TransactionBehavior::Immediate)?;

                if let Some((event_id, sequence)) = transaction
                    .query_row(
                        "SELECT id, mailbox_sequence FROM events WHERE gateway = ?1 AND external_id = ?2",
                        params![event.gateway, event.external_id],
                        |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
                    )
                    .optional()?
                {
                    return Ok(IngressOutcome::Duplicate {
                        event_id: crate::runtime::model::EventId::from_string(event_id),
                        sequence,
                    });
                }

                let actor = transaction
                    .query_row(
                        "SELECT actors.id, actors.enabled
                         FROM identities
                         JOIN actors ON actors.id = identities.actor_id
                         WHERE identities.provider = ?1 AND identities.subject = ?2",
                        params![event.identity_provider, event.identity_subject],
                        |row| Ok((row.get::<_, String>(0)?, row.get::<_, bool>(1)?)),
                    )
                    .optional()?;
                let Some((actor_id, true)) = actor else {
                    return Ok(IngressOutcome::Unauthorized);
                };

                let work_item_id = transaction
                    .query_row(
                        "SELECT id FROM work_items
                         WHERE actor_id = ?1 AND kind = 'interactive'
                           AND audience_kind = ?2
                           AND audience_address IS ?3
                           AND state IN ('ready', 'waiting')
                           AND cancellation_requested_at IS NULL
                         ORDER BY updated_at DESC, id ASC
                         LIMIT 1",
                        params![actor_id, audience_kind, audience_address],
                        |row| row.get::<_, String>(0),
                    )
                    .optional()?
                    .unwrap_or_else(|| crate::runtime::model::WorkItemId::new().to_string());

                transaction.execute(
                    "INSERT INTO work_items(
                        id, actor_id, kind, audience_kind, audience_address, state, created_at, updated_at
                     ) VALUES (?1, ?2, 'interactive', ?3, ?4, 'ready', ?5, ?5)
                     ON CONFLICT(id) DO UPDATE SET
                        state = CASE WHEN work_items.state = 'waiting' THEN 'ready' ELSE work_items.state END,
                        updated_at = excluded.updated_at",
                    params![work_item_id, actor_id, audience_kind, audience_address, now.0],
                )?;

                let sequence = transaction.query_row(
                    "UPDATE actors
                     SET next_mailbox_sequence = next_mailbox_sequence + 1
                     WHERE id = ?1
                     RETURNING next_mailbox_sequence",
                    [actor_id.as_str()],
                    |row| row.get::<_, i64>(0),
                )?;
                let event_id = crate::runtime::model::EventId::new();
                transaction.execute(
                    "INSERT INTO events(
                        id, actor_id, work_item_id, mailbox_sequence, gateway, external_id,
                        kind, audience_kind, audience_address, payload_json, state, created_at, updated_at
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, 'pending', ?11, ?11)",
                    params![
                        event_id.as_str(),
                        actor_id,
                        work_item_id,
                        sequence,
                        event.gateway,
                        event.external_id,
                        kind,
                        audience_kind,
                        audience_address,
                        event.payload_json,
                        now.0,
                    ],
                )?;
                transaction.commit()?;
                Ok(IngressOutcome::Accepted {
                    event_id,
                    work_item_id: crate::runtime::model::WorkItemId::from_string(work_item_id),
                    sequence,
                })
            })
            .await
            .map_err(|error| anyhow!("failed to persist inbound event: {error}"))
    }
}

fn encode_audience(audience: &crate::runtime::model::Audience) -> Result<(String, Option<String>)> {
    match audience {
        crate::runtime::model::Audience::ActorPrivate => Ok(("actor_private".into(), None)),
        crate::runtime::model::Audience::Shareable => Ok(("shareable".into(), None)),
        crate::runtime::model::Audience::ConversationScoped { address }
            if !address.trim().is_empty() =>
        {
            Ok(("conversation_scoped".into(), Some(address.clone())))
        }
        crate::runtime::model::Audience::ConversationScoped { .. } => {
            Err(anyhow!("conversation-scoped audience requires an address"))
        }
    }
}

fn encode_event_kind(kind: crate::runtime::model::EventKind) -> &'static str {
    match kind {
        crate::runtime::model::EventKind::UserMessage => "user_message",
        crate::runtime::model::EventKind::CancelRequested => "cancel_requested",
        crate::runtime::model::EventKind::ExternalCompletion => "external_completion",
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

    async fn next_mailbox_sequence(&self, actor_id: &str) -> Result<i64> {
        let actor_id = actor_id.to_string();
        self.connection
            .call(move |connection| {
                connection.query_row(
                    "SELECT next_mailbox_sequence FROM actors WHERE id = ?1",
                    [actor_id],
                    |row| row.get(0),
                )
            })
            .await
            .map_err(|error| anyhow!("failed to inspect actor sequence: {error}"))
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        auth::{LegacyActor, LegacyAuthorizationSnapshot, LegacyIdentity},
        runtime::{
            model::{Audience, Timestamp},
            sqlite::SqliteRuntimeStore,
            store::{
                ActorStore, ImportOutcome, IngressOutcome, IngressStore, NewInboundEvent,
                RuntimeAuthorizationStore,
            },
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
        assert!(store.legacy_authorization_imported().await.unwrap());
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

    #[tokio::test]
    async fn ingress_sequences_events_and_deduplicates_external_ids() {
        let store = SqliteRuntimeStore::open_in_memory().await.unwrap();
        store
            .import_legacy_authorization(owner_snapshot(), Timestamp(1))
            .await
            .unwrap();
        let first = NewInboundEvent::text(
            "local",
            "event-1",
            "telegram",
            "123",
            Audience::ActorPrivate,
            "first",
        )
        .unwrap();
        let second = NewInboundEvent::text(
            "local",
            "event-2",
            "telegram",
            "123",
            Audience::ActorPrivate,
            "second",
        )
        .unwrap();

        let accepted = store.ingest(first.clone(), Timestamp(100)).await.unwrap();
        let duplicate = store.ingest(first, Timestamp(101)).await.unwrap();
        let next = store.ingest(second, Timestamp(102)).await.unwrap();

        assert_eq!(accepted.sequence(), Some(1));
        assert!(matches!(
            duplicate,
            IngressOutcome::Duplicate { sequence: 1, .. }
        ));
        assert_eq!(next.sequence(), Some(2));
    }

    #[tokio::test]
    async fn unauthorized_ingress_does_not_consume_actor_sequence() {
        let store = SqliteRuntimeStore::open_in_memory().await.unwrap();
        store
            .import_legacy_authorization(owner_snapshot(), Timestamp(1))
            .await
            .unwrap();
        let unknown = NewInboundEvent::text(
            "local",
            "missing",
            "telegram",
            "404",
            Audience::ActorPrivate,
            "ignored",
        )
        .unwrap();

        assert_eq!(
            store.ingest(unknown, Timestamp(100)).await.unwrap(),
            IngressOutcome::Unauthorized
        );
        assert_eq!(
            store
                .next_mailbox_sequence("actor:telegram:123")
                .await
                .unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn conversation_scoped_ingress_uses_separate_work_item() {
        let store = SqliteRuntimeStore::open_in_memory().await.unwrap();
        store
            .import_legacy_authorization(owner_snapshot(), Timestamp(1))
            .await
            .unwrap();
        let private = NewInboundEvent::text(
            "local",
            "private",
            "telegram",
            "123",
            Audience::ActorPrivate,
            "private",
        )
        .unwrap();
        let group = NewInboundEvent::text(
            "local",
            "group",
            "telegram",
            "123",
            Audience::ConversationScoped {
                address: "telegram-group:7".into(),
            },
            "group",
        )
        .unwrap();

        let private = store.ingest(private, Timestamp(100)).await.unwrap();
        let group = store.ingest(group, Timestamp(101)).await.unwrap();

        assert_ne!(private.work_item_id(), group.work_item_id());
    }
}
