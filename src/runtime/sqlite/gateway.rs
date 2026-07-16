use anyhow::{Result, bail};
use async_trait::async_trait;
use tokio_rusqlite::params;

use crate::runtime::{
    gateway::{
        ClaimedGatewayDelivery, DeliveryRoute, GatewayDeliveryClaim, GatewayDeliveryState,
        NewGatewayDelivery,
    },
    model::{GatewayDeliveryId, OutboxId, Timestamp, WorkItemId},
    sqlite::{SqliteRuntimeStore, map_call_error},
    store::{GatewayDeliveryStore, OutboxPayload},
};

#[async_trait]
impl GatewayDeliveryStore for SqliteRuntimeStore {
    async fn enqueue_gateway_delivery(
        &self,
        delivery: NewGatewayDelivery,
        now: Timestamp,
    ) -> Result<GatewayDeliveryId> {
        let payload_json = serde_json::to_string(&delivery.payload)?;
        self.connection
            .call(move |connection| -> Result<GatewayDeliveryId> {
                let transaction = connection.transaction_with_behavior(
                    tokio_rusqlite::rusqlite::TransactionBehavior::Immediate,
                )?;
                let id = GatewayDeliveryId::new();
                transaction.execute(
                    "INSERT INTO gateway_deliveries(
                        id, intent_key, source_outbox_id, gateway, address,
                        reply_to_external_id, max_text_chars, max_caption_chars,
                        ordinal, payload_json, state, attempt_count, created_at, updated_at
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, 'pending', 0, ?11, ?11)
                     ON CONFLICT(intent_key) DO NOTHING",
                    params![
                        id.as_str(),
                        delivery.intent_key,
                        delivery.source_outbox_id.as_ref().map(OutboxId::as_str),
                        delivery.route.gateway,
                        delivery.route.address,
                        delivery.route.reply_to_external_id,
                        delivery.route.max_text_chars as i64,
                        delivery.route.max_caption_chars as i64,
                        delivery.ordinal as i64,
                        payload_json,
                        now.0,
                    ],
                )?;
                let stored = transaction.query_row(
                    "SELECT id, source_outbox_id, gateway, address, reply_to_external_id,
                            max_text_chars, max_caption_chars, ordinal, payload_json
                     FROM gateway_deliveries WHERE intent_key = ?1",
                    [delivery.intent_key.as_str()],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, Option<String>>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, String>(3)?,
                            row.get::<_, Option<String>>(4)?,
                            row.get::<_, i64>(5)?,
                            row.get::<_, i64>(6)?,
                            row.get::<_, i64>(7)?,
                            row.get::<_, String>(8)?,
                        ))
                    },
                )?;
                if stored.1.as_deref() != delivery.source_outbox_id.as_ref().map(OutboxId::as_str)
                    || stored.2 != delivery.route.gateway
                    || stored.3 != delivery.route.address
                    || stored.4 != delivery.route.reply_to_external_id
                    || stored.5 != delivery.route.max_text_chars as i64
                    || stored.6 != delivery.route.max_caption_chars as i64
                    || stored.7 != delivery.ordinal as i64
                    || stored.8 != payload_json
                {
                    bail!("gateway delivery intent key was reused with different immutable data");
                }
                transaction.commit()?;
                Ok(GatewayDeliveryId::from_string(stored.0))
            })
            .await
            .map_err(map_call_error)
    }

    async fn claim_gateway_deliveries(
        &self,
        gateway: &str,
        owner: &str,
        now: Timestamp,
        claim_until: Timestamp,
        limit: usize,
    ) -> Result<Vec<ClaimedGatewayDelivery>> {
        if gateway.trim().is_empty() || owner.trim().is_empty() || claim_until <= now || limit == 0
        {
            bail!("invalid gateway delivery claim");
        }
        let gateway = gateway.to_owned();
        let owner = owner.to_owned();
        self.connection
            .call(move |connection| -> Result<Vec<ClaimedGatewayDelivery>> {
                let transaction = connection.transaction_with_behavior(
                    tokio_rusqlite::rusqlite::TransactionBehavior::Immediate,
                )?;
                transaction.execute(
                    "UPDATE gateway_deliveries
                     SET state = 'outcome_unknown', claim_owner = NULL,
                         claim_expires_at = NULL, error_class = 'expired_claim',
                         last_error = 'delivery claim expired with unknown transport outcome',
                         updated_at = ?1
                     WHERE gateway = ?2 AND state = 'delivering'
                       AND claim_expires_at <= ?1",
                    params![now.0, gateway],
                )?;
                let ids = {
                    let mut statement = transaction.prepare(
                        "SELECT candidate.id
                         FROM gateway_deliveries candidate
                         WHERE candidate.gateway = ?1
                           AND candidate.state IN ('pending','failed_retryable')
                           AND (candidate.next_attempt_at IS NULL OR candidate.next_attempt_at <= ?2)
                           AND candidate.id = (
                               SELECT first.id FROM gateway_deliveries first
                               WHERE first.gateway = candidate.gateway
                                 AND first.address = candidate.address
                                 AND first.state IN ('pending','failed_retryable')
                                 AND (first.next_attempt_at IS NULL OR first.next_attempt_at <= ?2)
                               ORDER BY first.created_at, first.id LIMIT 1
                           )
                           AND NOT EXISTS (
                               SELECT 1 FROM gateway_deliveries active
                               WHERE active.gateway = candidate.gateway
                                 AND active.address = candidate.address
                                 AND active.state = 'delivering'
                           )
                         ORDER BY candidate.created_at, candidate.id LIMIT ?3",
                    )?;
                    statement
                        .query_map(params![gateway, now.0, limit as i64], |row| {
                            row.get::<_, String>(0)
                        })?
                        .collect::<std::result::Result<Vec<_>, _>>()?
                };
                let mut claimed = Vec::with_capacity(ids.len());
                for id in ids {
                    transaction.execute(
                        "UPDATE gateway_deliveries SET state = 'delivering',
                            attempt_count = attempt_count + 1, claim_owner = ?2,
                            claim_expires_at = ?3, updated_at = ?4 WHERE id = ?1",
                        params![id, owner, claim_until.0, now.0],
                    )?;
                    claimed.push(load_claimed(&transaction, &id, &owner, claim_until)?);
                }
                transaction.commit()?;
                Ok(claimed)
            })
            .await
            .map_err(map_call_error)
    }

    async fn renew_gateway_delivery(
        &self,
        claim: &GatewayDeliveryClaim,
        now: Timestamp,
        claim_until: Timestamp,
    ) -> Result<Option<GatewayDeliveryClaim>> {
        let claim = claim.clone();
        self.connection
            .call(move |connection| {
                let changed = connection.execute(
                    "UPDATE gateway_deliveries SET claim_expires_at = ?4, updated_at = ?3
                 WHERE id = ?1 AND state = 'delivering' AND claim_owner = ?2
                   AND claim_expires_at = ?5",
                    params![
                        claim.id.as_str(),
                        claim.owner,
                        now.0,
                        claim_until.0,
                        claim.expires_at.0
                    ],
                )?;
                Ok((changed == 1).then(|| GatewayDeliveryClaim {
                    expires_at: claim_until,
                    ..claim
                }))
            })
            .await
            .map_err(map_call_error)
    }

    async fn complete_gateway_delivery(
        &self,
        claim: &GatewayDeliveryClaim,
        remote_message_id: Option<String>,
        now: Timestamp,
    ) -> Result<bool> {
        transition_claim(self, claim, "delivered", None, None, remote_message_id, now).await
    }

    async fn retry_gateway_delivery(
        &self,
        claim: &GatewayDeliveryClaim,
        next_attempt_at: Timestamp,
        error_class: &str,
        error: &str,
        now: Timestamp,
    ) -> Result<bool> {
        transition_claim(
            self,
            claim,
            "failed_retryable",
            Some(next_attempt_at),
            Some((error_class, error)),
            None,
            now,
        )
        .await
    }

    async fn fail_gateway_delivery(
        &self,
        claim: &GatewayDeliveryClaim,
        state: GatewayDeliveryState,
        error_class: &str,
        error: &str,
        now: Timestamp,
    ) -> Result<bool> {
        if !state.is_failure_target() {
            bail!("invalid terminal gateway delivery state");
        }
        let state = match state {
            GatewayDeliveryState::FailedTerminal => "failed_terminal",
            GatewayDeliveryState::OutcomeUnknown => "outcome_unknown",
            _ => unreachable!(),
        };
        transition_claim(
            self,
            claim,
            state,
            None,
            Some((error_class, error)),
            None,
            now,
        )
        .await
    }
}

fn load_claimed(
    transaction: &tokio_rusqlite::rusqlite::Transaction<'_>,
    id: &str,
    owner: &str,
    expires_at: Timestamp,
) -> Result<ClaimedGatewayDelivery> {
    let row = transaction.query_row(
        "SELECT delivery.intent_key, delivery.source_outbox_id, outbox.work_item_id,
                delivery.ordinal, delivery.gateway, delivery.address,
                delivery.reply_to_external_id, delivery.max_text_chars,
                delivery.max_caption_chars, delivery.payload_json,
                delivery.attempt_count, delivery.remote_message_id
         FROM gateway_deliveries delivery
         LEFT JOIN outbox ON outbox.id = delivery.source_outbox_id
         WHERE delivery.id = ?1",
        [id],
        |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, Option<String>>(6)?,
                row.get::<_, i64>(7)?,
                row.get::<_, i64>(8)?,
                row.get::<_, String>(9)?,
                row.get::<_, i64>(10)?,
                row.get::<_, Option<String>>(11)?,
            ))
        },
    )?;
    Ok(ClaimedGatewayDelivery {
        claim: GatewayDeliveryClaim {
            id: GatewayDeliveryId::from_string(id),
            owner: owner.to_owned(),
            expires_at,
        },
        intent_key: row.0,
        source_outbox_id: row.1.map(OutboxId::from_string),
        work_item_id: row.2.map(WorkItemId::from_string),
        ordinal: row.3 as usize,
        route: DeliveryRoute::new(row.4, row.5, row.6, row.7 as usize, row.8 as usize)?,
        payload: serde_json::from_str::<OutboxPayload>(&row.9)?,
        attempt_count: row.10 as usize,
        remote_message_id: row.11,
    })
}

async fn transition_claim(
    store: &SqliteRuntimeStore,
    claim: &GatewayDeliveryClaim,
    state: &'static str,
    next_attempt_at: Option<Timestamp>,
    error: Option<(&str, &str)>,
    remote_message_id: Option<String>,
    now: Timestamp,
) -> Result<bool> {
    let claim = claim.clone();
    let error = error.map(|(class, message)| (class.to_owned(), message.to_owned()));
    store.connection.call(move |connection| {
        let changed = connection.execute(
            "UPDATE gateway_deliveries SET state = ?4, next_attempt_at = ?5,
                claim_owner = NULL, claim_expires_at = NULL, remote_message_id = COALESCE(?6, remote_message_id),
                error_class = ?7, last_error = ?8, updated_at = ?9
             WHERE id = ?1 AND state = 'delivering' AND claim_owner = ?2 AND claim_expires_at = ?3",
            params![claim.id.as_str(), claim.owner, claim.expires_at.0, state,
                next_attempt_at.map(|value| value.0), remote_message_id,
                error.as_ref().map(|value| value.0.as_str()),
                error.as_ref().map(|value| value.1.as_str()), now.0],
        )?;
        Ok(changed == 1)
    }).await.map_err(map_call_error)
}

#[cfg(test)]
mod tests {
    use anyhow::Result;

    use crate::runtime::{
        gateway::{DeliveryRoute, NewGatewayDelivery},
        model::Timestamp,
        sqlite::SqliteRuntimeStore,
        store::{GatewayDeliveryStore, OutboxPayload},
    };

    fn delivery(key: &str, address: &str, text: &str) -> Result<NewGatewayDelivery> {
        NewGatewayDelivery::new(
            key,
            None,
            0,
            DeliveryRoute::new("telegram:900", address, None, 4096, 1024)?,
            OutboxPayload::Text { text: text.into() },
        )
    }

    #[tokio::test]
    async fn identical_enqueue_is_idempotent_but_immutable_mismatch_fails() -> Result<()> {
        let store = SqliteRuntimeStore::open_in_memory().await?;
        let first = store
            .enqueue_gateway_delivery(delivery("reply:1", "100", "hello")?, Timestamp(1))
            .await?;
        let repeated = store
            .enqueue_gateway_delivery(delivery("reply:1", "100", "hello")?, Timestamp(2))
            .await?;
        assert_eq!(repeated, first);
        assert!(
            store
                .enqueue_gateway_delivery(delivery("reply:1", "200", "hello")?, Timestamp(3))
                .await
                .is_err()
        );
        Ok(())
    }

    #[tokio::test]
    async fn claims_serialize_each_address_and_fence_completion() -> Result<()> {
        let store = SqliteRuntimeStore::open_in_memory().await?;
        for (key, address) in [("a1", "100"), ("a2", "100"), ("b1", "200")] {
            store
                .enqueue_gateway_delivery(delivery(key, address, key)?, Timestamp(1))
                .await?;
        }
        let claims = store
            .claim_gateway_deliveries("telegram:900", "worker", Timestamp(2), Timestamp(32), 10)
            .await?;
        assert_eq!(claims.len(), 2);
        assert_ne!(claims[0].route.address, claims[1].route.address);
        let address_100 = claims
            .iter()
            .find(|claim| claim.route.address == "100")
            .expect("address 100 was claimed");
        assert!(
            store
                .complete_gateway_delivery(&address_100.claim, Some("77".into()), Timestamp(3),)
                .await?
        );
        let next = store
            .claim_gateway_deliveries("telegram:900", "worker", Timestamp(4), Timestamp(34), 10)
            .await?;
        assert_eq!(next.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn expired_delivery_claim_becomes_outcome_unknown_instead_of_retrying() -> Result<()> {
        let store = SqliteRuntimeStore::open_in_memory().await?;
        store
            .enqueue_gateway_delivery(delivery("expired", "100", "hello")?, Timestamp(1))
            .await?;
        let claimed = store
            .claim_gateway_deliveries("telegram:900", "worker", Timestamp(2), Timestamp(32), 10)
            .await?;
        assert_eq!(claimed.len(), 1);

        let reclaimed = store
            .claim_gateway_deliveries("telegram:900", "worker-2", Timestamp(33), Timestamp(63), 10)
            .await?;
        assert!(reclaimed.is_empty());
        assert!(
            !store
                .complete_gateway_delivery(&claimed[0].claim, Some("77".into()), Timestamp(34))
                .await?
        );
        Ok(())
    }
}
