pub mod bridge;
pub mod ingress;
pub mod protocol;

use std::{path::PathBuf, sync::Arc, time::Duration};

use anyhow::{Context, Result, bail};

use crate::runtime::{
    gateway::{ClaimedGatewayDelivery, GatewayDeliveryState},
    identity_link::IdentityLinkManager,
    model::{Clock, Timestamp},
    signals::ActorSignals,
    sqlite::SqliteRuntimeStore,
    store::{ActorStore, GatewayDeliveryStore, IngressStore, OutboxPayload},
};

pub struct PreparedReticulumGateway<S, C> {
    destination: String,
    bridge: tokio::sync::Mutex<Option<bridge::BridgeProcess>>,
    store: S,
    ingress: ingress::ReticulumIngressService<S, C>,
    clock: C,
    gateway: String,
    owner: String,
    active_delivery: tokio::sync::Mutex<Option<ClaimedGatewayDelivery>>,
}

impl<S, C> PreparedReticulumGateway<S, C>
where
    S: ActorStore + IngressStore + GatewayDeliveryStore + Clone + Send + Sync + 'static,
    C: Clock,
{
    pub fn destination(&self) -> &str {
        &self.destination
    }

    pub async fn shutdown(&self) -> Result<()> {
        if let Some(delivery) = self.active_delivery.lock().await.take() {
            Self::transition_delivery(
                &self.store,
                &delivery,
                protocol::BridgeDeliveryOutcome::OutcomeUnknown,
                None,
                self.clock.now(),
            )
            .await?;
        }
        if let Some(bridge) = self.bridge.lock().await.take() {
            bridge.shutdown().await?;
        }
        Ok(())
    }

    pub async fn run_delivery_once(&self) -> Result<usize> {
        let now = self.clock.now();
        let mut deliveries = self
            .store
            .claim_gateway_deliveries(&self.gateway, &self.owner, now, now.plus_millis(30_000), 1)
            .await?;
        let Some(mut delivery) = deliveries.pop() else {
            return Ok(0);
        };
        let OutboxPayload::Text { text } = &delivery.payload else {
            Self::transition_delivery(
                &self.store,
                &delivery,
                protocol::BridgeDeliveryOutcome::Terminal,
                None,
                self.clock.now(),
            )
            .await?;
            return Ok(1);
        };
        let command = protocol::BridgeCommand::Send {
            delivery_id: delivery.claim.id.as_str().to_owned(),
            destination: delivery.route.address.clone(),
            text: text.clone(),
        };
        if protocol::encode_command(&command).is_err() {
            Self::transition_delivery(
                &self.store,
                &delivery,
                protocol::BridgeDeliveryOutcome::Terminal,
                None,
                self.clock.now(),
            )
            .await?;
            return Ok(1);
        }
        if !self
            .store
            .set_gateway_delivery_retry_safe(&delivery.claim, false, now)
            .await?
        {
            bail!("Reticulum delivery claim was lost before bridge submission");
        }
        *self.active_delivery.lock().await = Some(delivery.clone());
        let mut guard = self.bridge.lock().await;
        let bridge = guard.as_mut().context("Reticulum bridge is stopped")?;
        if let Err(error) = bridge.send(&command).await {
            Self::transition_delivery(
                &self.store,
                &delivery,
                protocol::BridgeDeliveryOutcome::OutcomeUnknown,
                None,
                self.clock.now(),
            )
            .await?;
            self.active_delivery.lock().await.take();
            return Err(error.context("failed to submit Reticulum delivery"));
        }
        let mut renewal = tokio::time::interval(Duration::from_secs(10));
        renewal.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        renewal.tick().await;
        loop {
            let event = tokio::select! {
                event = bridge.next_event() => event,
                _ = renewal.tick() => {
                    let now = self.clock.now();
                    let Some(claim) = self.store.renew_gateway_delivery(
                        &delivery.claim,
                        now,
                        now.plus_millis(30_000),
                    ).await? else {
                        bail!("Reticulum delivery claim was lost during bridge submission");
                    };
                    delivery.claim = claim;
                    continue;
                }
            };
            match event {
                Ok(protocol::BridgeEvent::Delivery {
                    delivery_id,
                    outcome,
                    retry_after_ms,
                }) if delivery_id == delivery.claim.id.as_str() => {
                    Self::transition_delivery(
                        &self.store,
                        &delivery,
                        outcome,
                        retry_after_ms,
                        self.clock.now(),
                    )
                    .await?;
                    self.active_delivery.lock().await.take();
                    return Ok(1);
                }
                Ok(protocol::BridgeEvent::Inbound {
                    message_hash,
                    source,
                    timestamp,
                    text,
                }) => {
                    self.ingress
                        .handle(ingress::InboundMessage {
                            message_hash,
                            source,
                            timestamp,
                            text,
                        })
                        .await?;
                }
                Ok(protocol::BridgeEvent::Fatal { error }) => {
                    Self::transition_delivery(
                        &self.store,
                        &delivery,
                        protocol::BridgeDeliveryOutcome::OutcomeUnknown,
                        None,
                        self.clock.now(),
                    )
                    .await?;
                    self.active_delivery.lock().await.take();
                    bail!("Reticulum bridge failed: {error}");
                }
                Ok(_) => bail!("Reticulum bridge emitted an unexpected event"),
                Err(error) => {
                    Self::transition_delivery(
                        &self.store,
                        &delivery,
                        protocol::BridgeDeliveryOutcome::OutcomeUnknown,
                        None,
                        self.clock.now(),
                    )
                    .await?;
                    self.active_delivery.lock().await.take();
                    return Err(error);
                }
            }
        }
    }

    pub async fn run(&self, shutdown: tokio::sync::watch::Receiver<bool>) -> Result<()> {
        let result = self.run_inner(shutdown).await;
        let cleanup = self.shutdown().await;
        result.and(cleanup)
    }

    async fn run_inner(&self, mut shutdown: tokio::sync::watch::Receiver<bool>) -> Result<()> {
        loop {
            if *shutdown.borrow() {
                return Ok(());
            }
            let delivery = self.run_delivery_once();
            tokio::pin!(delivery);
            tokio::select! {
                result = &mut delivery => {
                    if result? > 0 {
                        continue;
                    }
                }
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return Ok(());
                    }
                    continue;
                }
            }
            let mut guard = self.bridge.lock().await;
            let bridge = guard.as_mut().context("Reticulum bridge is stopped")?;
            tokio::select! {
                changed = shutdown.changed() => {
                    drop(guard);
                    if changed.is_err() || *shutdown.borrow() {
                        return Ok(());
                    }
                }
                event = tokio::time::timeout(Duration::from_millis(500), bridge.next_event()) => {
                    match event {
                        Err(_) => {}
                        Ok(Ok(protocol::BridgeEvent::Inbound { message_hash, source, timestamp, text })) => {
                            drop(guard);
                            self.ingress.handle(ingress::InboundMessage {
                                message_hash,
                                source,
                                timestamp,
                                text,
                            }).await?;
                        }
                        Ok(Ok(protocol::BridgeEvent::Fatal { error })) => {
                            bail!("Reticulum bridge failed: {error}");
                        }
                        Ok(Ok(_)) => bail!("Reticulum bridge emitted an unsolicited event"),
                        Ok(Err(error)) => return Err(error),
                    }
                }
            }
        }
    }

    async fn transition_delivery(
        store: &S,
        delivery: &ClaimedGatewayDelivery,
        outcome: protocol::BridgeDeliveryOutcome,
        retry_after_ms: Option<u64>,
        now: Timestamp,
    ) -> Result<()> {
        let changed = match outcome {
            protocol::BridgeDeliveryOutcome::Delivered => {
                store
                    .complete_gateway_delivery(&delivery.claim, None, now)
                    .await?
            }
            protocol::BridgeDeliveryOutcome::Retryable => {
                let delay = retry_after_ms.unwrap_or_else(|| {
                    let exponent = delivery.attempt_count.saturating_sub(1).min(5);
                    1000_u64
                        .checked_shl(exponent as u32)
                        .unwrap_or(30_000)
                        .min(30_000)
                });
                store
                    .retry_gateway_delivery(
                        &delivery.claim,
                        now.plus_millis(delay.min(i64::MAX as u64) as i64),
                        "reticulum_retryable",
                        "LXMF delivery failed retryably",
                        now,
                    )
                    .await?
            }
            protocol::BridgeDeliveryOutcome::Terminal => {
                store
                    .fail_gateway_delivery(
                        &delivery.claim,
                        GatewayDeliveryState::FailedTerminal,
                        "reticulum_terminal",
                        "LXMF delivery was rejected",
                        now,
                    )
                    .await?
            }
            protocol::BridgeDeliveryOutcome::OutcomeUnknown => {
                store
                    .fail_gateway_delivery(
                        &delivery.claim,
                        GatewayDeliveryState::OutcomeUnknown,
                        "reticulum_outcome_unknown",
                        "LXMF delivery outcome is unknown",
                        now,
                    )
                    .await?
            }
        };
        if !changed {
            bail!("Reticulum delivery claim was lost before transition");
        }
        Ok(())
    }
}

pub async fn prepare<C>(
    config: crate::config::ValidatedReticulumConfig,
    store: SqliteRuntimeStore,
    linking: Arc<dyn IdentityLinkManager>,
    signals: ActorSignals,
    clock: C,
    state_dir: PathBuf,
) -> Result<PreparedReticulumGateway<SqliteRuntimeStore, C>>
where
    C: Clock,
{
    crate::runtime::ipc::security::validate_secure_directory(&state_dir)?;
    let mut bridge = bridge::BridgeProcess::spawn(&config, &state_dir).await?;
    let destination = match bridge.start().await {
        Ok(destination) => destination,
        Err(error) => {
            let _ = bridge.shutdown().await;
            return Err(error);
        }
    };
    let ingress = ingress::ReticulumIngressService::new(
        store.clone(),
        linking,
        signals,
        destination.clone(),
        clock.clone(),
    )?;
    Ok(PreparedReticulumGateway {
        gateway: format!("reticulum:{destination}"),
        destination,
        bridge: tokio::sync::Mutex::new(Some(bridge)),
        store,
        ingress,
        clock,
        owner: format!("reticulum-delivery-{}", std::process::id()),
        active_delivery: tokio::sync::Mutex::new(None),
    })
}

#[cfg(test)]
mod tests {
    use std::{fs, os::unix::fs::PermissionsExt, sync::Arc};

    use anyhow::Result;

    use super::PreparedReticulumGateway;
    use crate::{
        interfaces::reticulum::protocol::BridgeDeliveryOutcome,
        runtime::{
            gateway::{DeliveryRoute, GatewayDeliveryState, NewGatewayDelivery},
            model::{ManualClock, Timestamp},
            sqlite::SqliteRuntimeStore,
            store::{GatewayDeliveryStore, OutboxPayload},
        },
    };

    const DESTINATION: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    #[tokio::test]
    async fn prepare_waits_for_bridge_destination() -> Result<()> {
        let root = std::env::temp_dir()
            .canonicalize()?
            .join(format!("codrik-reticulum-prepare-{}", uuid::Uuid::new_v4()));
        crate::runtime::ipc::security::create_secure_directory(&root)?;
        let python = root.join("fake-python");
        fs::write(
            &python,
            format!(
                "#!/bin/sh\nread start\nprintf '%s\\n' '{{\"type\":\"ready\",\"destination\":\"{DESTINATION}\"}}'\nread shutdown\n"
            ),
        )?;
        fs::set_permissions(&python, fs::Permissions::from_mode(0o700))?;
        let store = SqliteRuntimeStore::open_in_memory().await?;
        let linking: Arc<dyn crate::runtime::identity_link::IdentityLinkManager> =
            Arc::new(crate::runtime::identity_link::IdentityLinkService::new(
                store.clone(),
                ManualClock::new(1),
                crate::runtime::identity_link::SystemLinkCodeGenerator,
            ));
        let gateway = super::prepare(
            crate::config::ValidatedReticulumConfig {
                host: "mesh.example".into(),
                port: 4242,
                python,
            },
            store,
            linking,
            crate::runtime::signals::ActorSignals::default(),
            ManualClock::new(1),
            root.clone(),
        )
        .await?;
        assert_eq!(gateway.destination(), DESTINATION);
        gateway.shutdown().await?;
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[tokio::test]
    async fn bridge_delivery_event_completes_durable_claim() -> Result<()> {
        let root = std::env::temp_dir().canonicalize()?.join(format!(
            "codrik-reticulum-delivery-{}",
            uuid::Uuid::new_v4()
        ));
        crate::runtime::ipc::security::create_secure_directory(&root)?;
        let python = root.join("fake-python");
        fs::write(
            &python,
            format!(
                "#!/bin/sh\nread start\nprintf '%s\\n' '{{\"type\":\"ready\",\"destination\":\"{DESTINATION}\"}}'\nread send\nid=$(printf '%s' \"$send\" | sed -n 's/.*\"delivery_id\":\"\\([^\"]*\\)\".*/\\1/p')\nprintf '{{\"type\":\"delivery\",\"delivery_id\":\"%s\",\"outcome\":\"delivered\",\"retry_after_ms\":null}}\\n' \"$id\"\nread shutdown\n"
            ),
        )?;
        fs::set_permissions(&python, fs::Permissions::from_mode(0o700))?;
        let store = SqliteRuntimeStore::open_in_memory().await?;
        let linking: Arc<dyn crate::runtime::identity_link::IdentityLinkManager> =
            Arc::new(crate::runtime::identity_link::IdentityLinkService::new(
                store.clone(),
                ManualClock::new(1),
                crate::runtime::identity_link::SystemLinkCodeGenerator,
            ));
        let gateway = super::prepare(
            crate::config::ValidatedReticulumConfig {
                host: "mesh.example".into(),
                port: 4242,
                python,
            },
            store.clone(),
            linking,
            crate::runtime::signals::ActorSignals::default(),
            ManualClock::new(2),
            root.clone(),
        )
        .await?;
        let gateway_name = format!("reticulum:{DESTINATION}");
        store
            .enqueue_gateway_delivery(
                NewGatewayDelivery::new(
                    "bridge-delivery",
                    None,
                    0,
                    DeliveryRoute::new(
                        &gateway_name,
                        "cccccccccccccccccccccccccccccccc",
                        None,
                        4096,
                        1,
                    )?,
                    OutboxPayload::Text {
                        text: "hello".into(),
                    },
                )?,
                Timestamp(1),
            )
            .await?;
        assert_eq!(gateway.run_delivery_once().await?, 1);
        assert!(
            store
                .claim_gateway_deliveries(&gateway_name, "verify", Timestamp(40), Timestamp(70), 1,)
                .await?
                .is_empty()
        );
        gateway.shutdown().await?;
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[tokio::test]
    async fn inbound_event_during_delivery_is_dispatched_before_completion() -> Result<()> {
        let root = std::env::temp_dir().canonicalize()?.join(format!(
            "codrik-reticulum-concurrent-{}",
            uuid::Uuid::new_v4()
        ));
        crate::runtime::ipc::security::create_secure_directory(&root)?;
        let python = root.join("fake-python");
        fs::write(
            &python,
            format!(
                "#!/bin/sh\nread start\nprintf '%s\\n' '{{\"type\":\"ready\",\"destination\":\"{DESTINATION}\"}}'\nread send\nid=$(printf '%s' \"$send\" | sed -n 's/.*\"delivery_id\":\"\\([^\"]*\\)\".*/\\1/p')\nprintf '%s\\n' '{{\"type\":\"inbound\",\"message_hash\":\"dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd\",\"source\":\"cccccccccccccccccccccccccccccccc\",\"timestamp\":2.0,\"text\":\"hello\"}}'\nprintf '{{\"type\":\"delivery\",\"delivery_id\":\"%s\",\"outcome\":\"delivered\",\"retry_after_ms\":null}}\\n' \"$id\"\nread shutdown\n"
            ),
        )?;
        fs::set_permissions(&python, fs::Permissions::from_mode(0o700))?;
        let store = SqliteRuntimeStore::open_in_memory().await?;
        let actor = crate::runtime::model::ActorId::from_string("owner");
        crate::runtime::store::ActorStore::ensure_initial_actor(&store, &actor, &[], Timestamp(1))
            .await?;
        let linking: Arc<dyn crate::runtime::identity_link::IdentityLinkManager> =
            Arc::new(crate::runtime::identity_link::IdentityLinkService::new(
                store.clone(),
                ManualClock::new(1),
                crate::runtime::identity_link::SystemLinkCodeGenerator,
            ));
        let code = linking.issue_code(&actor).await?.code;
        let gateway = super::prepare(
            crate::config::ValidatedReticulumConfig {
                host: "mesh.example".into(),
                port: 4242,
                python,
            },
            store.clone(),
            linking,
            crate::runtime::signals::ActorSignals::default(),
            ManualClock::new(2),
            root.clone(),
        )
        .await?;
        gateway
            .ingress
            .handle(crate::interfaces::reticulum::ingress::InboundMessage {
                message_hash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    .into(),
                source: "cccccccccccccccccccccccccccccccc".into(),
                timestamp: 1.0,
                text: format!("/link {code}"),
            })
            .await?;
        let gateway_name = format!("reticulum:{DESTINATION}");
        store
            .enqueue_gateway_delivery(
                NewGatewayDelivery::new(
                    "concurrent-delivery",
                    None,
                    0,
                    DeliveryRoute::new(
                        &gateway_name,
                        "cccccccccccccccccccccccccccccccc",
                        None,
                        4096,
                        1,
                    )?,
                    OutboxPayload::Text {
                        text: "reply".into(),
                    },
                )?,
                Timestamp(1),
            )
            .await?;
        assert_eq!(gateway.run_delivery_once().await?, 1);
        gateway.shutdown().await?;
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[tokio::test]
    async fn delivery_outcomes_map_to_existing_durable_states() -> Result<()> {
        for (outcome, expected) in [
            (BridgeDeliveryOutcome::Delivered, None),
            (
                BridgeDeliveryOutcome::Terminal,
                Some(GatewayDeliveryState::FailedTerminal),
            ),
            (
                BridgeDeliveryOutcome::OutcomeUnknown,
                Some(GatewayDeliveryState::OutcomeUnknown),
            ),
        ] {
            let store = SqliteRuntimeStore::open_in_memory().await?;
            let gateway = format!("reticulum:{DESTINATION}");
            store
                .enqueue_gateway_delivery(
                    NewGatewayDelivery::new(
                        format!("intent:{outcome:?}"),
                        None,
                        0,
                        DeliveryRoute::new(
                            &gateway,
                            "cccccccccccccccccccccccccccccccc",
                            None,
                            4096,
                            1,
                        )?,
                        OutboxPayload::Text {
                            text: "hello".into(),
                        },
                    )?,
                    Timestamp(1),
                )
                .await?;
            let delivery = store
                .claim_gateway_deliveries(&gateway, "test", Timestamp(2), Timestamp(32), 1)
                .await?
                .remove(0);
            PreparedReticulumGateway::<SqliteRuntimeStore, ManualClock>::transition_delivery(
                &store,
                &delivery,
                outcome,
                None,
                Timestamp(3),
            )
            .await?;
            let reclaimed = store
                .claim_gateway_deliveries(&gateway, "test", Timestamp(4), Timestamp(34), 1)
                .await?;
            match expected {
                None => assert!(reclaimed.is_empty()),
                Some(
                    GatewayDeliveryState::FailedTerminal | GatewayDeliveryState::OutcomeUnknown,
                ) => {
                    assert!(reclaimed.is_empty())
                }
                _ => unreachable!(),
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn retryable_delivery_uses_supplied_delay() -> Result<()> {
        let store = SqliteRuntimeStore::open_in_memory().await?;
        let gateway = format!("reticulum:{DESTINATION}");
        store
            .enqueue_gateway_delivery(
                NewGatewayDelivery::new(
                    "retry",
                    None,
                    0,
                    DeliveryRoute::new(
                        &gateway,
                        "cccccccccccccccccccccccccccccccc",
                        None,
                        4096,
                        1,
                    )?,
                    OutboxPayload::Text {
                        text: "hello".into(),
                    },
                )?,
                Timestamp(1),
            )
            .await?;
        let delivery = store
            .claim_gateway_deliveries(&gateway, "test", Timestamp(2), Timestamp(32), 1)
            .await?
            .remove(0);
        PreparedReticulumGateway::<SqliteRuntimeStore, ManualClock>::transition_delivery(
            &store,
            &delivery,
            BridgeDeliveryOutcome::Retryable,
            Some(5000),
            Timestamp(3),
        )
        .await?;
        assert!(
            store
                .claim_gateway_deliveries(&gateway, "test", Timestamp(5002), Timestamp(5032), 1)
                .await?
                .is_empty()
        );
        assert_eq!(
            store
                .claim_gateway_deliveries(&gateway, "test", Timestamp(5003), Timestamp(5033), 1)
                .await?
                .len(),
            1
        );
        Ok(())
    }

    #[tokio::test]
    async fn oversized_utf8_delivery_fails_terminal_before_bridge_submission() -> Result<()> {
        let store = SqliteRuntimeStore::open_in_memory().await?;
        let gateway = format!("reticulum:{DESTINATION}");
        store
            .enqueue_gateway_delivery(
                NewGatewayDelivery::new(
                    "oversized-unicode",
                    None,
                    0,
                    DeliveryRoute::new(
                        &gateway,
                        "cccccccccccccccccccccccccccccccc",
                        None,
                        262_144,
                        1,
                    )?,
                    OutboxPayload::Text {
                        text: "🦀".repeat(70_000),
                    },
                )?,
                Timestamp(1),
            )
            .await?;
        let delivery = store
            .claim_gateway_deliveries(&gateway, "test", Timestamp(2), Timestamp(32), 1)
            .await?
            .remove(0);
        let command = crate::interfaces::reticulum::protocol::BridgeCommand::Send {
            delivery_id: delivery.claim.id.as_str().into(),
            destination: delivery.route.address.clone(),
            text: match &delivery.payload {
                OutboxPayload::Text { text } => text.clone(),
                _ => unreachable!(),
            },
        };
        assert!(crate::interfaces::reticulum::protocol::encode_command(&command).is_err());
        assert!(
            store
                .set_gateway_delivery_retry_safe(&delivery.claim, true, Timestamp(3))
                .await?
        );
        Ok(())
    }
}
