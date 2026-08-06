use anyhow::{Result, anyhow};
use async_trait::async_trait;
use tokio_rusqlite::{params, rusqlite::OptionalExtension};

use crate::runtime::{
    model::{EventId, Timestamp, WorkItemId},
    sqlite::SqliteRuntimeStore,
    store::{NewWebhookEvent, WebhookIdempotency, WebhookIngressOutcome, WebhookIngressStore},
};

const AUTOMATIC_DEDUP_WINDOW_MILLIS: i64 = 86_400_000;
const RECEIPT_GC_BATCH: i64 = 256;

#[async_trait]
impl WebhookIngressStore for SqliteRuntimeStore {
    async fn ingest_webhook(
        &self,
        event: NewWebhookEvent,
        now: Timestamp,
    ) -> Result<WebhookIngressOutcome> {
        self.connection
            .call(
                move |connection| -> tokio_rusqlite::rusqlite::Result<WebhookIngressOutcome> {
                    let transaction = connection.transaction_with_behavior(
                        tokio_rusqlite::rusqlite::TransactionBehavior::Immediate,
                    )?;
                    transaction.execute(
                        "DELETE FROM webhook_receipts WHERE rowid IN (
                        SELECT rowid FROM webhook_receipts
                        WHERE identity_kind = 'automatic' AND accepted_at < ?1
                        ORDER BY accepted_at LIMIT ?2
                    )",
                        params![now.0 - AUTOMATIC_DEDUP_WINDOW_MILLIS, RECEIPT_GC_BATCH],
                    )?;

                    let enabled = transaction
                        .query_row(
                            "SELECT enabled FROM actors WHERE id = ?1",
                            [event.actor_id.as_str()],
                            |row| row.get::<_, bool>(0),
                        )
                        .optional()?;
                    if enabled != Some(true) {
                        return Ok(WebhookIngressOutcome::ActorUnavailable);
                    }

                    let (identity_kind, identity_hash, cutoff) = match event.idempotency {
                        WebhookIdempotency::Explicit(hash) => ("explicit", hash, None),
                        WebhookIdempotency::Automatic(hash) => (
                            "automatic",
                            hash,
                            Some(now.0 - AUTOMATIC_DEDUP_WINDOW_MILLIS),
                        ),
                    };
                    let duplicate = transaction
                        .query_row(
                            "SELECT event_id FROM webhook_receipts
                         WHERE endpoint = ?1 AND identity_kind = ?2 AND identity_hash = ?3
                           AND (?4 IS NULL OR accepted_at >= ?4)
                         ORDER BY accepted_at DESC LIMIT 1",
                            params![
                                event.endpoint,
                                identity_kind,
                                identity_hash.as_slice(),
                                cutoff
                            ],
                            |row| row.get::<_, String>(0),
                        )
                        .optional()?;
                    if let Some(event_id) = duplicate {
                        return Ok(WebhookIngressOutcome::Duplicate {
                            event_id: EventId::from_string(event_id),
                        });
                    }

                    let work_item_id = WorkItemId::new();
                    transaction.execute(
                        "INSERT INTO work_items(
                        id, actor_id, kind, audience_kind, state, created_at, updated_at
                     ) VALUES (?1, ?2, 'external', 'actor_private', 'ready', ?3, ?3)",
                        params![work_item_id.as_str(), event.actor_id.as_str(), now.0],
                    )?;
                    let sequence = transaction.query_row(
                        "UPDATE actors SET next_mailbox_sequence = next_mailbox_sequence + 1
                     WHERE id = ?1 RETURNING next_mailbox_sequence",
                        [event.actor_id.as_str()],
                        |row| row.get::<_, i64>(0),
                    )?;
                    let route = transaction
                        .query_row(
                            "SELECT gateway, address, max_text_chars, max_caption_chars
                         FROM actor_latest_telegram_routes WHERE actor_id = ?1",
                            [event.actor_id.as_str()],
                            |row| {
                                Ok((
                                    row.get::<_, String>(0)?,
                                    row.get::<_, String>(1)?,
                                    row.get::<_, i64>(2)?,
                                    row.get::<_, i64>(3)?,
                                ))
                            },
                        )
                        .optional()?;
                    let event_id = EventId::new();
                    let external_id = event_id.to_string();
                    let (gateway, address, max_text, max_caption) = match route.as_ref() {
                        Some((gateway, address, max_text, max_caption)) => (
                            Some(gateway.as_str()),
                            Some(address.as_str()),
                            Some(*max_text),
                            Some(*max_caption),
                        ),
                        None => (None, None, None, None),
                    };
                    transaction.execute(
                        "INSERT INTO events(
                        id, actor_id, work_item_id, mailbox_sequence, gateway, external_id,
                        kind, audience_kind, delivery_gateway, delivery_address,
                        reply_to_external_id, delivery_max_text_chars,
                        delivery_max_caption_chars, execution_policy, ingress_source,
                        payload_json, state, created_at, updated_at
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'user_message', 'actor_private',
                               ?7, ?8, NULL, ?9, ?10, 'skills_only', ?11,
                               ?12, 'pending', ?13, ?13)",
                        params![
                            event_id.as_str(),
                            event.actor_id.as_str(),
                            work_item_id.as_str(),
                            sequence,
                            format!("webhook:{}", event.endpoint),
                            external_id,
                            gateway,
                            address,
                            max_text,
                            max_caption,
                            event.endpoint,
                            event.payload_json,
                            now.0,
                        ],
                    )?;
                    transaction.execute(
                        "INSERT INTO webhook_receipts(
                        endpoint, identity_kind, identity_hash, event_id, accepted_at
                     ) VALUES (?1, ?2, ?3, ?4, ?5)",
                        params![
                            event.endpoint,
                            identity_kind,
                            identity_hash.as_slice(),
                            event_id.as_str(),
                            now.0,
                        ],
                    )?;
                    transaction.commit()?;
                    Ok(WebhookIngressOutcome::Accepted {
                        event_id,
                        work_item_id,
                        sequence,
                        route_snapshotted: route.is_some(),
                    })
                },
            )
            .await
            .map_err(|error| anyhow!("failed to persist webhook event: {error}"))
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;

    use crate::runtime::{
        model::{ActorId, Timestamp},
        sqlite::SqliteRuntimeStore,
        store::{
            ActorAdminStore, ActorStore, NewWebhookEvent, WebhookIdempotency,
            WebhookIngressOutcome, WebhookIngressStore,
        },
    };

    async fn store() -> Result<SqliteRuntimeStore> {
        let store = SqliteRuntimeStore::open_in_memory().await?;
        store
            .ensure_initial_actor(&ActorId::from_string("owner"), &[], Timestamp(0))
            .await?;
        Ok(store)
    }

    fn event(idempotency: WebhookIdempotency) -> NewWebhookEvent {
        NewWebhookEvent {
            endpoint: "grafana".into(),
            actor_id: ActorId::from_string("owner"),
            idempotency,
            payload_json: r#"{"type":"webhook","source":"grafana","received_at":"1970-01-01T00:00:00.001Z","data":{"status":"firing"}}"#.into(),
        }
    }

    #[tokio::test]
    async fn accepts_for_enabled_actor_and_persists_skills_only_event() -> Result<()> {
        let store = store().await?;
        let outcome = store
            .ingest_webhook(event(WebhookIdempotency::Explicit([1; 32])), Timestamp(1))
            .await?;
        assert!(matches!(
            outcome,
            WebhookIngressOutcome::Accepted {
                sequence: 1,
                route_snapshotted: false,
                ..
            }
        ));
        let stored = store
            .connection
            .call(|connection| {
                connection.query_row(
                    "SELECT gateway, execution_policy, ingress_source, payload_json FROM events",
                    [],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, String>(3)?,
                        ))
                    },
                )
            })
            .await?;
        assert_eq!(stored.0, "webhook:grafana");
        assert_eq!(stored.1, "skills_only");
        assert_eq!(stored.2, "grafana");
        assert!(stored.3.contains("firing"));
        Ok(())
    }

    #[tokio::test]
    async fn explicit_identity_is_permanent_and_endpoint_scoped() -> Result<()> {
        let store = store().await?;
        let command = event(WebhookIdempotency::Explicit([2; 32]));
        assert!(matches!(
            store.ingest_webhook(command.clone(), Timestamp(1)).await?,
            WebhookIngressOutcome::Accepted { .. }
        ));
        assert!(matches!(
            store
                .ingest_webhook(command.clone(), Timestamp(100_000_000))
                .await?,
            WebhookIngressOutcome::Duplicate { .. }
        ));
        let mut other = command;
        other.endpoint = "other".into();
        assert!(matches!(
            store.ingest_webhook(other, Timestamp(100_000_000)).await?,
            WebhookIngressOutcome::Accepted { .. }
        ));
        Ok(())
    }

    #[tokio::test]
    async fn automatic_identity_expires_only_after_twenty_four_hours() -> Result<()> {
        let store = store().await?;
        let command = event(WebhookIdempotency::Automatic([3; 32]));
        assert!(matches!(
            store.ingest_webhook(command.clone(), Timestamp(1)).await?,
            WebhookIngressOutcome::Accepted { .. }
        ));
        assert!(matches!(
            store
                .ingest_webhook(command.clone(), Timestamp(86_400_001))
                .await?,
            WebhookIngressOutcome::Duplicate { .. }
        ));
        assert!(matches!(
            store.ingest_webhook(command, Timestamp(86_400_002)).await?,
            WebhookIngressOutcome::Accepted { .. }
        ));
        Ok(())
    }

    #[tokio::test]
    async fn unavailable_actor_does_not_consume_sequence() -> Result<()> {
        let store = store().await?;
        store
            .set_actor_enabled(&ActorId::from_string("owner"), false)
            .await?;
        assert_eq!(
            store
                .ingest_webhook(event(WebhookIdempotency::Explicit([8; 32])), Timestamp(1))
                .await?,
            WebhookIngressOutcome::ActorUnavailable
        );
        let sequence = store
            .connection
            .call(|connection| {
                connection.query_row(
                    "SELECT next_mailbox_sequence FROM actors WHERE id = 'owner'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
            })
            .await?;
        assert_eq!(sequence, 0);
        Ok(())
    }

    #[tokio::test]
    async fn concurrent_explicit_duplicates_create_one_event() -> Result<()> {
        let store = store().await?;
        let first_store = store.clone();
        let second_store = store.clone();
        let first = tokio::spawn(async move {
            first_store
                .ingest_webhook(event(WebhookIdempotency::Explicit([9; 32])), Timestamp(1))
                .await
        });
        let second = tokio::spawn(async move {
            second_store
                .ingest_webhook(event(WebhookIdempotency::Explicit([9; 32])), Timestamp(1))
                .await
        });
        let outcomes = [first.await??, second.await??];
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| matches!(outcome, WebhookIngressOutcome::Accepted { .. }))
                .count(),
            1
        );
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| matches!(outcome, WebhookIngressOutcome::Duplicate { .. }))
                .count(),
            1
        );
        Ok(())
    }

    #[tokio::test]
    async fn snapshots_latest_telegram_route() -> Result<()> {
        let store = store().await?;
        store.connection.call(|connection| -> tokio_rusqlite::rusqlite::Result<()> {
            connection.execute(
                "INSERT INTO actor_latest_telegram_routes(actor_id, gateway, address, max_text_chars, max_caption_chars, mailbox_sequence, updated_at) VALUES ('owner', 'telegram:900', '100', 4096, 1024, 1, 1)",
                [],
            )?;
            Ok(())
        }).await?;
        let outcome = store
            .ingest_webhook(event(WebhookIdempotency::Explicit([4; 32])), Timestamp(2))
            .await?;
        assert!(matches!(
            outcome,
            WebhookIngressOutcome::Accepted {
                route_snapshotted: true,
                ..
            }
        ));
        let route = store
            .connection
            .call(|connection| {
                connection.query_row(
                    "SELECT delivery_gateway, delivery_address, reply_to_external_id FROM events",
                    [],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, Option<String>>(2)?,
                        ))
                    },
                )
            })
            .await?;
        assert_eq!(route, ("telegram:900".into(), "100".into(), None));
        Ok(())
    }
}
