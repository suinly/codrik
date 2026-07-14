use anyhow::{Result, anyhow, bail};
use async_trait::async_trait;
use tokio_rusqlite::{params, rusqlite::OptionalExtension};

use crate::{
    agent::message::Message,
    runtime::{
        model::{ActorId, Audience, EventId, RunId, Timestamp, WorkItemId},
        sqlite::{SqliteRuntimeStore, map_call_error},
        store::{ActorLease, AttachedRun, ControlEvent, ControlStore, DispatchStore, StaleLease},
    },
};

#[async_trait]
impl DispatchStore for SqliteRuntimeStore {
    async fn acquire_ready_actor(
        &self,
        owner: &str,
        now: Timestamp,
        lease_until: Timestamp,
    ) -> Result<Option<ActorLease>> {
        if lease_until <= now {
            bail!("lease expiry must be after current time");
        }
        let owner = owner.to_string();
        self.connection
            .call(move |connection| -> tokio_rusqlite::rusqlite::Result<Option<ActorLease>> {
                let transaction = connection.transaction_with_behavior(
                    tokio_rusqlite::rusqlite::TransactionBehavior::Immediate,
                )?;
                let actor_id = transaction
                    .query_row(
                        "SELECT actors.id
                         FROM actors
                         LEFT JOIN actor_leases ON actor_leases.actor_id = actors.id
                         WHERE (
                            actor_leases.actor_id IS NULL
                            OR actor_leases.owner_id = ?1
                            OR actor_leases.expires_at <= ?2
                         ) AND (
                            EXISTS (SELECT 1 FROM events WHERE events.actor_id = actors.id AND events.state = 'pending')
                            OR EXISTS (SELECT 1 FROM runs WHERE runs.actor_id = actors.id AND runs.state = 'active')
                         )
                         ORDER BY COALESCE(
                            (SELECT MIN(mailbox_sequence) FROM events WHERE events.actor_id = actors.id AND events.state = 'pending'),
                            (SELECT MIN(events.mailbox_sequence) FROM events JOIN run_events ON run_events.event_id = events.id JOIN runs ON runs.id = run_events.run_id WHERE runs.actor_id = actors.id AND runs.state = 'active'),
                            9223372036854775807
                         ), actors.id
                         LIMIT 1",
                        params![owner, now.0],
                        |row| row.get::<_, String>(0),
                    )
                    .optional()?;
                let Some(actor_id) = actor_id else {
                    return Ok(None);
                };

                let current = transaction
                    .query_row(
                        "SELECT generation, owner_id, expires_at FROM actor_leases WHERE actor_id = ?1",
                        [actor_id.as_str()],
                        |row| {
                            Ok((
                                row.get::<_, i64>(0)?,
                                row.get::<_, String>(1)?,
                                row.get::<_, i64>(2)?,
                            ))
                        },
                    )
                    .optional()?;
                let generation = match current {
                    None => {
                        transaction.execute(
                            "INSERT INTO actor_leases(actor_id, generation, owner_id, expires_at) VALUES (?1, 1, ?2, ?3)",
                            params![actor_id, owner, lease_until.0],
                        )?;
                        1
                    }
                    Some((generation, current_owner, expires_at))
                        if current_owner == owner && expires_at > now.0 =>
                    {
                        transaction.execute(
                            "UPDATE actor_leases SET expires_at = ?2 WHERE actor_id = ?1",
                            params![actor_id, lease_until.0],
                        )?;
                        generation
                    }
                    Some((generation, _, expires_at)) if expires_at <= now.0 => {
                        let next = generation + 1;
                        transaction.execute(
                            "UPDATE actor_leases SET generation = ?2, owner_id = ?3, expires_at = ?4 WHERE actor_id = ?1",
                            params![actor_id, next, owner, lease_until.0],
                        )?;
                        next
                    }
                    Some(_) => return Ok(None),
                };
                transaction.commit()?;
                Ok(Some(ActorLease {
                    actor_id: ActorId::from_string(actor_id),
                    owner_id: owner,
                    generation,
                    expires_at: lease_until,
                }))
            })
            .await
            .map_err(|error| anyhow!("failed to acquire actor lease: {error}"))
    }

    async fn renew_lease(
        &self,
        lease: &ActorLease,
        now: Timestamp,
        lease_until: Timestamp,
    ) -> Result<ActorLease> {
        if lease_until <= now {
            bail!("lease expiry must be after current time");
        }
        let lease = lease.clone();
        self.connection
            .call(move |connection| -> Result<ActorLease> {
                let changed = connection.execute(
                    "UPDATE actor_leases SET expires_at = ?4
                     WHERE actor_id = ?1 AND owner_id = ?2 AND generation = ?3 AND expires_at > ?5",
                    params![
                        lease.actor_id.as_str(),
                        lease.owner_id,
                        lease.generation,
                        lease_until.0,
                        now.0,
                    ],
                )?;
                if changed != 1 {
                    return Err(StaleLease.into());
                }
                Ok(ActorLease {
                    expires_at: lease_until,
                    ..lease
                })
            })
            .await
            .map_err(map_call_error)
    }

    async fn attach_next_run(
        &self,
        lease: &ActorLease,
        max_events: usize,
        now: Timestamp,
    ) -> Result<Option<AttachedRun>> {
        if max_events == 0 {
            bail!("max_events must be greater than zero");
        }
        let lease = lease.clone();
        self.connection
            .call(move |connection| -> Result<Option<AttachedRun>> {
                let transaction = connection.transaction_with_behavior(
                    tokio_rusqlite::rusqlite::TransactionBehavior::Immediate,
                )?;
                ensure_current_lease(&transaction, &lease, now)?;

                let active = transaction
                    .query_row(
                        "SELECT runs.id, runs.work_item_id, runs.observed_sequence,
                                work_items.audience_kind, work_items.audience_address
                         FROM runs
                         JOIN work_items ON work_items.id = runs.work_item_id
                         WHERE runs.actor_id = ?1 AND runs.state = 'active'
                         ORDER BY runs.updated_at, runs.id
                         LIMIT 1",
                        [lease.actor_id.as_str()],
                        |row| {
                            Ok((
                                row.get::<_, String>(0)?,
                                row.get::<_, String>(1)?,
                                row.get::<_, i64>(2)?,
                                row.get::<_, String>(3)?,
                                row.get::<_, Option<String>>(4)?,
                            ))
                        },
                    )
                    .optional()?;

                let (run_id, work_item_id, previous_observed, audience_kind, audience_address) =
                    if let Some(active) = active {
                        active
                    } else {
                        let selected = transaction
                            .query_row(
                                "SELECT work_items.id, work_items.audience_kind, work_items.audience_address
                                 FROM events
                                 JOIN work_items ON work_items.id = events.work_item_id
                                 WHERE events.actor_id = ?1 AND events.state = 'pending'
                                 ORDER BY CASE events.kind
                                    WHEN 'cancel_requested' THEN 0
                                    WHEN 'user_message' THEN 1
                                    ELSE 2 END,
                                    events.mailbox_sequence
                                 LIMIT 1",
                                [lease.actor_id.as_str()],
                                |row| {
                                    Ok((
                                        row.get::<_, String>(0)?,
                                        row.get::<_, String>(1)?,
                                        row.get::<_, Option<String>>(2)?,
                                    ))
                                },
                            )
                            .optional()?;
                        let Some((work_item_id, audience_kind, audience_address)) = selected else {
                            return Ok(None);
                        };
                        let run_id = RunId::new().to_string();
                        transaction.execute(
                            "INSERT INTO runs(
                                id, actor_id, work_item_id, state, lease_generation,
                                observed_sequence, created_at, updated_at
                             ) VALUES (?1, ?2, ?3, 'active', ?4, 0, ?5, ?5)",
                            params![
                                run_id,
                                lease.actor_id.as_str(),
                                work_item_id,
                                lease.generation,
                                now.0,
                            ],
                        )?;
                        (run_id, work_item_id, 0, audience_kind, audience_address)
                    };

                let remaining = max_events.saturating_sub(run_event_count(&transaction, &run_id)?);
                if remaining > 0 {
                    let pending = {
                        let mut statement = transaction.prepare(
                            "SELECT id FROM events
                             WHERE actor_id = ?1 AND work_item_id = ?2 AND state = 'pending'
                               AND kind != 'cancel_requested'
                               AND mailbox_sequence < COALESCE((
                                  SELECT MIN(control.mailbox_sequence) FROM events AS control
                                  WHERE control.actor_id = events.actor_id
                                    AND control.state = 'pending'
                                    AND control.kind = 'cancel_requested'
                               ), 9223372036854775807)
                             ORDER BY CASE kind
                                WHEN 'cancel_requested' THEN 0
                                WHEN 'user_message' THEN 1
                                ELSE 2 END,
                                mailbox_sequence
                             LIMIT ?3",
                        )?;
                        statement
                            .query_map(
                                params![lease.actor_id.as_str(), work_item_id, remaining as i64],
                                |row| row.get::<_, String>(0),
                            )?
                            .collect::<std::result::Result<Vec<_>, _>>()?
                    };
                    for event_id in pending {
                        transaction.execute(
                            "INSERT INTO run_events(run_id, event_id) VALUES (?1, ?2)",
                            params![run_id, event_id],
                        )?;
                        transaction.execute(
                            "UPDATE events SET state = 'processing', run_id = ?2, updated_at = ?3 WHERE id = ?1 AND state = 'pending'",
                            params![event_id, run_id, now.0],
                        )?;
                    }
                }

                let event_rows = load_run_events(&transaction, &run_id)?;
                let observed_sequence = event_rows
                    .iter()
                    .map(|event| event.sequence)
                    .max()
                    .unwrap_or(previous_observed);
                transaction.execute(
                    "UPDATE runs SET lease_generation = ?2, observed_sequence = ?3, updated_at = ?4 WHERE id = ?1",
                    params![run_id, lease.generation, observed_sequence, now.0],
                )?;
                transaction.commit()?;

                Ok(Some(AttachedRun {
                    lease,
                    work_item_id: WorkItemId::from_string(work_item_id),
                    run_id: RunId::from_string(run_id),
                    observed_sequence,
                    source_event_ids: event_rows
                        .iter()
                        .map(|event| EventId::from_string(event.id.clone()))
                        .collect(),
                    audience: decode_audience(&audience_kind, audience_address)?,
                    messages: event_rows
                        .into_iter()
                        .filter(|event| !event.incorporated)
                        .map(|event| event_message(&event.payload_json))
                        .collect::<Result<Vec<_>>>()?,
                }))
            })
            .await
            .map_err(map_call_error)
    }

    async fn release_lease(&self, lease: &ActorLease) -> Result<()> {
        let lease = lease.clone();
        self.connection
            .call(move |connection| -> tokio_rusqlite::rusqlite::Result<()> {
                connection.execute(
                    "DELETE FROM actor_leases WHERE actor_id = ?1 AND owner_id = ?2 AND generation = ?3",
                    params![lease.actor_id.as_str(), lease.owner_id, lease.generation],
                )?;
                Ok(())
            })
            .await
            .map_err(|error| anyhow!("failed to release actor lease: {error}"))
    }
}

#[async_trait]
impl ControlStore for SqliteRuntimeStore {
    async fn newer_control_event(
        &self,
        lease: &ActorLease,
        observed_sequence: i64,
        now: Timestamp,
    ) -> Result<Option<ControlEvent>> {
        let lease = lease.clone();
        self.connection
            .call(move |connection| -> Result<Option<ControlEvent>> {
                let transaction = connection.transaction()?;
                ensure_current_lease(&transaction, &lease, now)?;
                let event = transaction
                    .query_row(
                        "SELECT events.id, events.mailbox_sequence, events.kind
                         FROM events
                         WHERE events.actor_id = ?1
                           AND events.state = 'pending'
                           AND events.mailbox_sequence > ?2
                           AND events.kind IN ('cancel_requested', 'user_message')
                           AND EXISTS (
                              SELECT 1 FROM runs
                              JOIN work_items ON work_items.id = runs.work_item_id
                              WHERE runs.actor_id = events.actor_id
                                AND runs.state = 'active'
                                AND work_items.audience_kind = events.audience_kind
                                AND work_items.audience_address IS events.audience_address
                           )
                         ORDER BY CASE events.kind
                            WHEN 'cancel_requested' THEN 0 ELSE 1 END,
                            events.mailbox_sequence
                         LIMIT 1",
                        params![lease.actor_id.as_str(), observed_sequence],
                        |row| {
                            Ok((
                                row.get::<_, String>(0)?,
                                row.get::<_, i64>(1)?,
                                row.get::<_, String>(2)?,
                            ))
                        },
                    )
                    .optional()?;
                event
                    .map(|(id, sequence, kind)| {
                        Ok(ControlEvent {
                            event_id: EventId::from_string(id),
                            sequence,
                            kind: decode_event_kind(&kind)?,
                        })
                    })
                    .transpose()
            })
            .await
            .map_err(map_call_error)
    }
}

struct StoredEvent {
    id: String,
    sequence: i64,
    payload_json: String,
    incorporated: bool,
}

pub(super) fn ensure_current_lease(
    transaction: &tokio_rusqlite::rusqlite::Transaction<'_>,
    lease: &ActorLease,
    now: Timestamp,
) -> Result<()> {
    let current = transaction
        .query_row(
            "SELECT 1 FROM actor_leases
             WHERE actor_id = ?1 AND owner_id = ?2 AND generation = ?3 AND expires_at > ?4",
            params![
                lease.actor_id.as_str(),
                lease.owner_id,
                lease.generation,
                now.0,
            ],
            |_| Ok(()),
        )
        .optional()?;
    if current.is_none() {
        return Err(StaleLease.into());
    }
    Ok(())
}

fn run_event_count(
    transaction: &tokio_rusqlite::rusqlite::Transaction<'_>,
    run_id: &str,
) -> tokio_rusqlite::rusqlite::Result<usize> {
    transaction.query_row(
        "SELECT COUNT(*) FROM run_events WHERE run_id = ?1",
        [run_id],
        |row| row.get::<_, i64>(0).map(|count| count as usize),
    )
}

fn load_run_events(
    transaction: &tokio_rusqlite::rusqlite::Transaction<'_>,
    run_id: &str,
) -> tokio_rusqlite::rusqlite::Result<Vec<StoredEvent>> {
    let mut statement = transaction.prepare(
        "SELECT events.id, events.mailbox_sequence, events.payload_json, run_events.incorporated
         FROM events
         JOIN run_events ON run_events.event_id = events.id
         WHERE run_events.run_id = ?1
         ORDER BY events.mailbox_sequence",
    )?;
    statement
        .query_map([run_id], |row| {
            Ok(StoredEvent {
                id: row.get(0)?,
                sequence: row.get(1)?,
                payload_json: row.get(2)?,
                incorporated: row.get(3)?,
            })
        })?
        .collect()
}

fn decode_audience(kind: &str, address: Option<String>) -> Result<Audience> {
    match (kind, address) {
        ("actor_private", None) => Ok(Audience::ActorPrivate),
        ("shareable", None) => Ok(Audience::Shareable),
        ("conversation_scoped", Some(address)) => Ok(Audience::ConversationScoped { address }),
        _ => bail!("invalid stored audience"),
    }
}

fn event_message(payload_json: &str) -> Result<Message> {
    let payload: serde_json::Value = serde_json::from_str(payload_json)?;
    let text = payload
        .get("text")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| anyhow!("inbound event payload is missing text"))?;
    Ok(Message::user(text))
}

fn decode_event_kind(kind: &str) -> Result<crate::runtime::model::EventKind> {
    match kind {
        "user_message" => Ok(crate::runtime::model::EventKind::UserMessage),
        "cancel_requested" => Ok(crate::runtime::model::EventKind::CancelRequested),
        "external_completion" => Ok(crate::runtime::model::EventKind::ExternalCompletion),
        _ => bail!("invalid stored event kind: {kind}"),
    }
}

#[cfg(test)]
impl SqliteRuntimeStore {
    async fn pending_group_event_count(&self) -> Result<i64> {
        self.connection
            .call(|connection| {
                connection.query_row(
                    "SELECT COUNT(*) FROM events WHERE state = 'pending' AND audience_kind = 'conversation_scoped'",
                    [],
                    |row| row.get(0),
                )
            })
            .await
            .map_err(|error| anyhow!("failed to count pending group events: {error}"))
    }

    async fn current_lease(&self) -> Result<Option<ActorLease>> {
        self.connection
            .call(|connection| -> tokio_rusqlite::rusqlite::Result<Option<ActorLease>> {
                connection
                    .query_row(
                        "SELECT actor_id, owner_id, generation, expires_at FROM actor_leases LIMIT 1",
                        [],
                        |row| {
                            Ok(ActorLease {
                                actor_id: ActorId::from_string(row.get::<_, String>(0)?),
                                owner_id: row.get(1)?,
                                generation: row.get(2)?,
                                expires_at: Timestamp(row.get(3)?),
                            })
                        },
                    )
                    .optional()
            })
            .await
            .map_err(|error| anyhow!("failed to inspect actor lease: {error}"))
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
                DispatchStore, IngressStore, NewInboundEvent, RuntimeAuthorizationStore, StaleLease,
            },
        },
    };

    async fn store_with_event() -> SqliteRuntimeStore {
        let store = SqliteRuntimeStore::open_in_memory().await.unwrap();
        store
            .import_legacy_authorization(
                LegacyAuthorizationSnapshot {
                    version: 1,
                    actors: vec![LegacyActor {
                        id: "actor:telegram:123".into(),
                        enabled: true,
                        tools: vec!["*".into()],
                        identities: vec![LegacyIdentity {
                            provider: "telegram".into(),
                            subject: "123".into(),
                            username: None,
                        }],
                    }],
                },
                Timestamp(1),
            )
            .await
            .unwrap();
        store
            .ingest(
                NewInboundEvent::text(
                    "local",
                    "event-1",
                    "telegram",
                    "123",
                    Audience::ActorPrivate,
                    "hello",
                )
                .unwrap(),
                Timestamp(2),
            )
            .await
            .unwrap();
        store
    }

    #[tokio::test]
    async fn stale_lease_cannot_attach_after_reacquisition() {
        let store = store_with_event().await;
        let first = store
            .acquire_ready_actor("worker-1", Timestamp(100), Timestamp(110))
            .await
            .unwrap()
            .unwrap();
        let second = store
            .acquire_ready_actor("worker-2", Timestamp(111), Timestamp(121))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(second.generation, first.generation + 1);
        let error = store
            .attach_next_run(&first, 8, Timestamp(112))
            .await
            .unwrap_err();
        assert!(error.downcast_ref::<StaleLease>().is_some());
    }

    #[tokio::test]
    async fn attachment_batches_only_one_audience_and_resumes_after_expiry() {
        let store = store_with_event().await;
        store
            .ingest(
                NewInboundEvent::text(
                    "local",
                    "event-2",
                    "telegram",
                    "123",
                    Audience::ActorPrivate,
                    "follow-up",
                )
                .unwrap(),
                Timestamp(3),
            )
            .await
            .unwrap();
        store
            .ingest(
                NewInboundEvent::text(
                    "local",
                    "event-3",
                    "telegram",
                    "123",
                    Audience::ConversationScoped {
                        address: "telegram-group:7".into(),
                    },
                    "group",
                )
                .unwrap(),
                Timestamp(4),
            )
            .await
            .unwrap();
        let first_lease = store
            .acquire_ready_actor("worker-1", Timestamp(100), Timestamp(110))
            .await
            .unwrap()
            .unwrap();

        let first_run = store
            .attach_next_run(&first_lease, 8, Timestamp(101))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(first_run.audience, Audience::ActorPrivate);
        assert_eq!(first_run.source_event_ids.len(), 2);
        assert_eq!(store.pending_group_event_count().await.unwrap(), 1);

        let second_lease = store
            .acquire_ready_actor("worker-2", Timestamp(111), Timestamp(121))
            .await
            .unwrap()
            .unwrap();
        let resumed = store
            .attach_next_run(&second_lease, 8, Timestamp(112))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(resumed.run_id, first_run.run_id);
        assert_eq!(resumed.source_event_ids, first_run.source_event_ids);
        assert_eq!(store.pending_group_event_count().await.unwrap(), 1);
    }

    #[tokio::test]
    async fn stale_release_does_not_remove_newer_lease() {
        let store = store_with_event().await;
        let first = store
            .acquire_ready_actor("worker-1", Timestamp(100), Timestamp(110))
            .await
            .unwrap()
            .unwrap();
        let second = store
            .acquire_ready_actor("worker-2", Timestamp(111), Timestamp(121))
            .await
            .unwrap()
            .unwrap();

        store.release_lease(&first).await.unwrap();

        assert_eq!(store.current_lease().await.unwrap(), Some(second));
    }

    #[tokio::test]
    async fn durable_cancellation_survives_signal_loss() {
        use crate::runtime::{
            model::EventKind,
            signals::ActorSignals,
            store::{ControlEvent, ControlStore, NewInboundEvent},
        };

        let store = store_with_event().await;
        let lease = store
            .acquire_ready_actor("worker", Timestamp(100), Timestamp(400))
            .await
            .unwrap()
            .unwrap();
        let run = store
            .attach_next_run(&lease, 8, Timestamp(101))
            .await
            .unwrap()
            .unwrap();
        store
            .ingest(
                NewInboundEvent {
                    gateway: "local".into(),
                    external_id: "cancel-1".into(),
                    identity_provider: "telegram".into(),
                    identity_subject: "123".into(),
                    kind: EventKind::CancelRequested,
                    audience: Audience::ActorPrivate,
                    payload_json: r#"{"type":"cancel"}"#.into(),
                },
                Timestamp(200),
            )
            .await
            .unwrap();

        let signals = ActorSignals::default();
        drop(signals);

        let control = store
            .newer_control_event(&lease, run.observed_sequence, Timestamp(300))
            .await
            .unwrap();
        assert!(matches!(
            control,
            Some(ControlEvent {
                sequence: 2,
                kind: EventKind::CancelRequested,
                ..
            })
        ));
    }
}
