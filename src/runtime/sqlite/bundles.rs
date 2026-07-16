use std::collections::HashSet;

use anyhow::{Result, anyhow, bail};
use async_trait::async_trait;
use sha2::{Digest, Sha256};
use tokio_rusqlite::{params, rusqlite::OptionalExtension};

use crate::runtime::{
    model::{
        ActorId, BundleId, BundleState, DeliveryId, MAX_BUNDLE_BYTES, MAX_BUNDLE_DELIVERIES,
        MAX_FINAL_CHUNK_BYTES, MAX_MANIFEST_BYTES, RequestId, Timestamp,
    },
    sqlite::{SqliteRuntimeStore, map_call_error},
    store::{
        AckOutcome, AckRejected, BundleAck, BundleClaim, BundleManifest, BundleManifestEntry,
        BundleStore, ClaimRenewal, ClaimTransition, ClaimedBundle, ClaimedBundleLoad,
        ClaimedBundleRef, FinalPayload, ManagedArtifact, OutboxPayload, ResultBundle,
    },
};

#[async_trait]
impl BundleStore for SqliteRuntimeStore {
    async fn claim_ready_bundle_refs(
        &self,
        owner: &str,
        request_ids: &[RequestId],
        now: Timestamp,
        until: Timestamp,
        limit: usize,
    ) -> Result<Vec<ClaimedBundleRef>> {
        if owner.trim().is_empty() || until <= now {
            bail!("bundle claim requires an owner and a future expiry");
        }
        let owner = owner.to_owned();
        let requested = request_ids
            .iter()
            .map(ToString::to_string)
            .collect::<HashSet<_>>();
        let limit = limit.min(32);
        self.connection
            .call(move |connection| -> Result<Vec<ClaimedBundleRef>> {
                let transaction = connection.transaction_with_behavior(
                    tokio_rusqlite::rusqlite::TransactionBehavior::Immediate,
                )?;
                transaction.execute(
                    "UPDATE result_bundles
                     SET state = 'failed_retryable', claim_owner = NULL,
                         claim_expires_at = NULL, next_attempt_at = ?1,
                         last_error = 'bundle claim expired before acknowledgement',
                         updated_at = ?1
                     WHERE state = 'delivering' AND claim_expires_at IS NOT NULL
                       AND claim_expires_at <= ?1",
                    [now.0],
                )?;
                let candidates = {
                    let mut statement = transaction.prepare(
                        "SELECT id, request_id FROM result_bundles
                         WHERE state = 'pending'
                            OR (state = 'failed_retryable' AND
                                (next_attempt_at IS NULL OR next_attempt_at <= ?1))
                         ORDER BY created_at, id",
                    )?;
                    statement
                        .query_map([now.0], |row| {
                            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                        })?
                        .collect::<std::result::Result<Vec<_>, _>>()?
                };
                let mut claimed = Vec::new();
                for (bundle_id, request_id) in candidates {
                    if claimed.len() >= limit || !requested.contains(&request_id) {
                        continue;
                    }
                    let changed = transaction.execute(
                        "UPDATE result_bundles
                         SET state = 'delivering', attempt_count = attempt_count + 1,
                             claim_owner = ?2, claim_expires_at = ?3,
                             next_attempt_at = NULL, last_error = NULL, updated_at = ?4
                         WHERE id = ?1 AND (state = 'pending' OR
                            (state = 'failed_retryable' AND
                             (next_attempt_at IS NULL OR next_attempt_at <= ?4)))",
                        params![bundle_id, owner, until.0, now.0],
                    )?;
                    if changed != 1 {
                        continue;
                    }
                    let attempt_count = transaction.query_row(
                        "SELECT attempt_count FROM result_bundles WHERE id = ?1",
                        [&bundle_id],
                        |row| row.get(0),
                    )?;
                    claimed.push(ClaimedBundleRef {
                        claim: BundleClaim {
                            bundle_id: BundleId::parse(&bundle_id)?,
                            owner: owner.clone(),
                            expires_at: until,
                        },
                        request_id: RequestId::parse(&request_id)?,
                        attempt_count,
                    });
                }
                transaction.commit()?;
                Ok(claimed)
            })
            .await
            .map_err(map_call_error)
    }

    async fn claim_ready_bundles(
        &self,
        owner: &str,
        request_ids: &[RequestId],
        now: Timestamp,
        until: Timestamp,
        limit: usize,
    ) -> Result<Vec<ClaimedBundle>> {
        let claims = self
            .claim_ready_bundle_refs(owner, request_ids, now, until, limit)
            .await?;
        let mut bundles = Vec::with_capacity(claims.len());
        for claimed in claims {
            match self.load_claimed_bundle(&claimed.claim, now).await? {
                ClaimedBundleLoad::Loaded(bundle) => bundles.push(ClaimedBundle {
                    claim: claimed.claim,
                    bundle,
                    attempt_count: claimed.attempt_count,
                }),
                ClaimedBundleLoad::FailedTerminal
                | ClaimedBundleLoad::Delivered
                | ClaimedBundleLoad::Fenced => {}
            }
        }
        Ok(bundles)
    }

    async fn renew_bundle(
        &self,
        claim: &BundleClaim,
        now: Timestamp,
        until: Timestamp,
    ) -> Result<ClaimRenewal> {
        if until <= now {
            bail!("renewed bundle expiry must be in the future");
        }
        let claim = claim.clone();
        self.connection
            .call(move |connection| -> Result<ClaimRenewal> {
                let changed = connection.execute(
                    "UPDATE result_bundles SET claim_expires_at = ?4, updated_at = ?3
                     WHERE id = ?1 AND state = 'delivering' AND claim_owner = ?2
                       AND claim_expires_at = ?5 AND claim_expires_at > ?3",
                    params![
                        claim.bundle_id.as_str(),
                        claim.owner,
                        now.0,
                        until.0,
                        claim.expires_at.0
                    ],
                )?;
                if changed != 1 {
                    return Ok(ClaimRenewal::Fenced);
                }
                Ok(ClaimRenewal::Renewed(BundleClaim {
                    expires_at: until,
                    ..claim
                }))
            })
            .await
            .map_err(map_call_error)
    }

    async fn load_claimed_bundle(
        &self,
        claim: &BundleClaim,
        now: Timestamp,
    ) -> Result<ClaimedBundleLoad> {
        let claim = claim.clone();
        self.connection
            .call(move |connection| -> Result<ClaimedBundleLoad> {
                let transaction = connection.transaction_with_behavior(
                    tokio_rusqlite::rusqlite::TransactionBehavior::Immediate,
                )?;
                let ownership = transaction
                    .query_row(
                        "SELECT result_bundles.state, result_bundles.claim_owner,
                                result_bundles.claim_expires_at, result_bundles.request_id,
                                local_requests.result_bundle_id
                         FROM result_bundles
                         LEFT JOIN local_requests
                           ON local_requests.request_id = result_bundles.request_id
                         WHERE result_bundles.id = ?1",
                        [claim.bundle_id.as_str()],
                        |row| {
                            Ok((
                                row.get::<_, String>(0)?,
                                row.get::<_, Option<String>>(1)?,
                                row.get::<_, Option<i64>>(2)?,
                                row.get::<_, String>(3)?,
                                row.get::<_, Option<String>>(4)?,
                            ))
                        },
                    )
                    .optional()?;
                let Some((state, owner, expires_at, _request_id, reciprocal_bundle_id)) = ownership
                else {
                    return Ok(ClaimedBundleLoad::Fenced);
                };
                if state == "delivered" {
                    return Ok(ClaimedBundleLoad::Delivered);
                }
                if state != "delivering"
                    || owner.as_deref() != Some(claim.owner.as_str())
                    || expires_at != Some(claim.expires_at.0)
                    || expires_at.is_some_and(|expires_at| expires_at <= now.0)
                {
                    return Ok(ClaimedBundleLoad::Fenced);
                }
                if reciprocal_bundle_id.as_deref() != Some(claim.bundle_id.as_str()) {
                    transaction.execute(
                        "UPDATE result_bundles
                         SET state = 'failed_terminal', claim_owner = NULL,
                             claim_expires_at = NULL, next_attempt_at = NULL,
                             last_error = 'request does not reciprocally own claimed bundle',
                             updated_at = ?2
                         WHERE id = ?1",
                        params![claim.bundle_id.as_str(), now.0],
                    )?;
                    transaction.commit()?;
                    return Ok(ClaimedBundleLoad::FailedTerminal);
                }
                match load_bundle_row(&transaction, claim.bundle_id.as_str()) {
                    Ok(bundle) => Ok(ClaimedBundleLoad::Loaded(bundle)),
                    Err(error)
                        if error
                            .chain()
                            .any(|cause| cause.is::<tokio_rusqlite::rusqlite::Error>()) =>
                    {
                        Err(error)
                    }
                    Err(error) => {
                        let changed = transaction.execute(
                            "UPDATE result_bundles
                             SET state = 'failed_terminal', claim_owner = NULL,
                                 claim_expires_at = NULL, next_attempt_at = NULL,
                                 last_error = ?4, updated_at = ?3
                             WHERE id = ?1 AND state = 'delivering' AND claim_owner = ?2
                               AND claim_expires_at = ?5 AND claim_expires_at > ?3",
                            params![
                                claim.bundle_id.as_str(),
                                claim.owner,
                                now.0,
                                error.to_string(),
                                claim.expires_at.0
                            ],
                        )?;
                        if changed != 1 {
                            bail!("bundle claim is stale");
                        }
                        transaction.commit()?;
                        Ok(ClaimedBundleLoad::FailedTerminal)
                    }
                }
            })
            .await
            .map_err(map_call_error)
    }

    async fn load_bundle(&self, id: &BundleId) -> Result<ResultBundle> {
        let id = id.to_string();
        self.connection
            .call(move |connection| load_bundle_row(connection, &id))
            .await
            .map_err(map_call_error)
    }

    async fn acknowledge_bundle(&self, ack: BundleAck, now: Timestamp) -> Result<AckOutcome> {
        self.connection
            .call(move |connection| -> Result<AckOutcome> {
                let transaction = connection.transaction_with_behavior(
                    tokio_rusqlite::rusqlite::TransactionBehavior::Immediate,
                )?;
                let (request_id, state, reciprocal_bundle_id, actor_id) = transaction
                    .query_row(
                        "SELECT result_bundles.request_id, result_bundles.state,
                                local_requests.result_bundle_id, local_requests.actor_id
                         FROM result_bundles
                         JOIN local_requests
                           ON local_requests.request_id = result_bundles.request_id
                         WHERE result_bundles.id = ?1",
                        [ack.bundle_id.as_str()],
                        |row| {
                            Ok((
                                row.get::<_, String>(0)?,
                                row.get::<_, String>(1)?,
                                row.get::<_, Option<String>>(2)?,
                                row.get::<_, String>(3)?,
                            ))
                        },
                    )
                    .optional()?
                    .ok_or_else(|| anyhow!(AckRejected("bundle was not found".into())))?;
                if request_id != ack.request_id.as_str() {
                    return Err(anyhow!(AckRejected(
                        "bundle does not belong to request".into()
                    )));
                }
                if actor_id != ack.actor_id.as_str() {
                    return Err(anyhow!(AckRejected(
                        "bundle does not belong to actor".into()
                    )));
                }
                if reciprocal_bundle_id.as_deref() != Some(ack.bundle_id.as_str()) {
                    return Err(anyhow!(AckRejected(
                        "request does not reciprocally own the acknowledged bundle".into()
                    )));
                }
                let canonical_bundle = load_bundle_row(&transaction, ack.bundle_id.as_str())?;
                let durable = {
                    let mut statement = transaction.prepare(
                        "SELECT outbox_deliveries.id, outbox_deliveries.transport,
                                outbox_deliveries.address, outbox.actor_id
                         FROM outbox_deliveries
                         JOIN outbox ON outbox.id = outbox_deliveries.outbox_id
                         WHERE outbox_deliveries.bundle_id = ?1 ORDER BY ordinal",
                    )?;
                    statement
                        .query_map([ack.bundle_id.as_str()], |row| {
                            Ok((
                                row.get::<_, String>(0)?,
                                row.get::<_, String>(1)?,
                                row.get::<_, String>(2)?,
                                row.get::<_, String>(3)?,
                            ))
                        })?
                        .collect::<std::result::Result<Vec<_>, _>>()?
                };
                if durable.iter().any(|(_, transport, address, actor_id)| {
                    transport != "local_ipc"
                        || address != ack.request_id.as_str()
                        || actor_id != ack.actor_id.as_str()
                }) {
                    return Err(anyhow!(AckRejected(
                        "bundle route does not belong to request".into()
                    )));
                }
                let expected = canonical_bundle
                    .manifest
                    .entries
                    .iter()
                    .map(|entry| entry.delivery_id.as_str())
                    .collect::<HashSet<_>>();
                let supplied = ack
                    .delivery_ids
                    .iter()
                    .map(DeliveryId::as_str)
                    .collect::<HashSet<_>>();
                if expected.len() != durable.len()
                    || supplied.len() != ack.delivery_ids.len()
                    || expected != supplied
                {
                    return Err(anyhow!(AckRejected(
                        "ACK delivery IDs do not exactly match the bundle manifest".into()
                    )));
                }
                let outcome = match state.as_str() {
                    "delivered" => AckOutcome::AlreadyDelivered,
                    "delivering" | "failed_retryable" => {
                        let changed = transaction.execute(
                            "UPDATE result_bundles
                             SET state = 'delivered', claim_owner = NULL,
                                 claim_expires_at = NULL, next_attempt_at = NULL,
                                 last_error = NULL, updated_at = ?2
                             WHERE id = ?1 AND state IN ('delivering','failed_retryable')",
                            params![ack.bundle_id.as_str(), now.0],
                        )?;
                        if changed != 1 {
                            return Err(anyhow!(AckRejected("bundle changed during ACK".into())));
                        }
                        AckOutcome::Delivered
                    }
                    "pending" => {
                        return Err(anyhow!(AckRejected(
                            "pending bundle cannot be acknowledged".into()
                        )));
                    }
                    "failed_terminal" => {
                        return Err(anyhow!(AckRejected(
                            "terminally failed bundle cannot be acknowledged".into()
                        )));
                    }
                    other => {
                        return Err(anyhow!(AckRejected(format!(
                            "invalid bundle state: {other}"
                        ))));
                    }
                };
                transaction.commit()?;
                Ok(outcome)
            })
            .await
            .map_err(map_call_error)
    }

    async fn fail_bundle_retryable(
        &self,
        claim: &BundleClaim,
        error: &str,
        next_attempt: Timestamp,
        now: Timestamp,
    ) -> Result<ClaimTransition> {
        let claim = claim.clone();
        let error = error.to_owned();
        self.connection
            .call(move |connection| -> Result<ClaimTransition> {
                let changed = connection.execute(
                    "UPDATE result_bundles
                     SET state = 'failed_retryable', claim_owner = NULL,
                         claim_expires_at = NULL, next_attempt_at = ?4,
                         last_error = ?5, updated_at = ?3
                     WHERE id = ?1 AND state = 'delivering' AND claim_owner = ?2
                       AND claim_expires_at = ?6 AND claim_expires_at > ?3",
                    params![
                        claim.bundle_id.as_str(),
                        claim.owner,
                        now.0,
                        next_attempt.0,
                        error,
                        claim.expires_at.0
                    ],
                )?;
                if changed != 1 {
                    return Ok(ClaimTransition::Fenced);
                }
                Ok(ClaimTransition::Applied)
            })
            .await
            .map_err(map_call_error)
    }

    async fn replay_bundle(
        &self,
        actor: &ActorId,
        request: &RequestId,
    ) -> Result<Option<ResultBundle>> {
        let actor = actor.to_string();
        let request = request.to_string();
        self.connection
            .call(move |connection| -> Result<Option<ResultBundle>> {
                let bundle_id = connection
                    .query_row(
                        "SELECT result_bundles.id FROM result_bundles
                         JOIN local_requests
                           ON local_requests.request_id = result_bundles.request_id
                         WHERE result_bundles.request_id = ?1
                           AND local_requests.actor_id = ?2
                           AND result_bundles.state = 'delivered'",
                        params![request, actor],
                        |row| row.get::<_, String>(0),
                    )
                    .optional()?;
                bundle_id
                    .map(|id| load_bundle_row(connection, &id))
                    .transpose()
            })
            .await
            .map_err(map_call_error)
    }

    async fn fail_bundle_terminal(
        &self,
        claim: &BundleClaim,
        error: &str,
        now: Timestamp,
    ) -> Result<()> {
        let claim = claim.clone();
        let error = error.to_owned();
        self.connection
            .call(move |connection| -> Result<()> {
                let changed = connection.execute(
                    "UPDATE result_bundles
                     SET state = 'failed_terminal', claim_owner = NULL,
                         claim_expires_at = NULL, next_attempt_at = NULL,
                         last_error = ?4, updated_at = ?3
                     WHERE id = ?1 AND state = 'delivering' AND claim_owner = ?2
                       AND claim_expires_at = ?5 AND claim_expires_at > ?3",
                    params![
                        claim.bundle_id.as_str(),
                        claim.owner,
                        now.0,
                        error,
                        claim.expires_at.0
                    ],
                )?;
                if changed != 1 {
                    bail!("bundle claim is stale");
                }
                Ok(())
            })
            .await
            .map_err(map_call_error)
    }
}

pub(super) fn payload_from_outbox(payload: OutboxPayload) -> FinalPayload {
    match payload {
        OutboxPayload::Text { text } => FinalPayload::Text { text },
        OutboxPayload::File {
            artifact_id,
            managed_path,
            display_name,
            media_type,
            size,
            sha256,
            caption,
        } => FinalPayload::File {
            artifact: ManagedArtifact {
                id: artifact_id,
                managed_path,
                display_name,
                media_type,
                size,
                sha256,
                caption,
            },
        },
        OutboxPayload::TerminalError { code, message } => {
            FinalPayload::TerminalError { code, message }
        }
    }
}

pub(super) fn manifest_for(deliveries: &[(DeliveryId, FinalPayload)]) -> Result<BundleManifest> {
    if deliveries.is_empty() || deliveries.len() > MAX_BUNDLE_DELIVERIES {
        bail!("bundle delivery count is outside 1..={MAX_BUNDLE_DELIVERIES}");
    }
    let mut total = 0usize;
    let mut entries = Vec::with_capacity(deliveries.len());
    for (delivery_id, payload) in deliveries {
        let encoded = serde_json::to_vec(payload)?;
        total = total
            .checked_add(encoded.len())
            .ok_or_else(|| anyhow!("bundle decoded byte count overflow"))?;
        let payload_kind = match payload {
            FinalPayload::Text { .. } => "text",
            FinalPayload::File { .. } => "file",
            FinalPayload::TerminalError { .. } => "terminal_error",
        };
        entries.push(BundleManifestEntry {
            delivery_id: delivery_id.clone(),
            payload_kind: payload_kind.into(),
            decoded_bytes: encoded.len(),
            sha256: hex_sha256(&encoded),
            chunk_count: encoded.len().div_ceil(MAX_FINAL_CHUNK_BYTES),
        });
    }
    validate_bundle_bytes(total)?;
    let canonical = serde_json::to_vec(&entries)?;
    validate_manifest_bytes(canonical.len())?;
    Ok(BundleManifest {
        sha256: hex_sha256(&canonical),
        entries,
    })
}

fn load_bundle_row(
    connection: &tokio_rusqlite::rusqlite::Connection,
    id: &str,
) -> Result<ResultBundle> {
    let (request_id, state, delivery_count, manifest_sha256) = connection
        .query_row(
            "SELECT request_id, state, delivery_count, manifest_sha256
             FROM result_bundles WHERE id = ?1",
            [id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, usize>(2)?,
                    row.get::<_, String>(3)?,
                ))
            },
        )
        .optional()?
        .ok_or_else(|| anyhow!("bundle was not found"))?;
    let rows = {
        let mut statement = connection.prepare(
            "SELECT deliveries.id, deliveries.ordinal, deliveries.transport,
                    deliveries.address, outbox.payload_json
             FROM outbox_deliveries deliveries
             JOIN outbox ON outbox.id = deliveries.outbox_id
             WHERE deliveries.bundle_id = ?1 ORDER BY deliveries.ordinal",
        )?;
        statement
            .query_map([id], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, usize>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                ))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?
    };
    if rows.len() != delivery_count || rows.len() > MAX_BUNDLE_DELIVERIES {
        bail!("bundle delivery count does not match its immutable memberships");
    }
    let mut deliveries = Vec::with_capacity(rows.len());
    for (expected, (delivery_id, ordinal, transport, address, payload_json)) in
        rows.into_iter().enumerate()
    {
        if ordinal != expected {
            bail!("bundle membership ordinals are not contiguous");
        }
        if transport != "local_ipc" || address != request_id {
            bail!("bundle membership route does not belong to its request");
        }
        let payload: OutboxPayload = serde_json::from_str(&payload_json)?;
        deliveries.push((
            DeliveryId::parse(&delivery_id)?,
            payload_from_outbox(payload),
        ));
    }
    let manifest = manifest_for(&deliveries)?;
    if manifest.sha256 != manifest_sha256 {
        bail!("bundle manifest hash does not match immutable memberships");
    }
    Ok(ResultBundle {
        id: BundleId::parse(id)?,
        request_id: RequestId::parse(&request_id)?,
        state: decode_state(&state)?,
        manifest,
        deliveries,
    })
}

fn decode_state(state: &str) -> Result<BundleState> {
    match state {
        "pending" => Ok(BundleState::Pending),
        "delivering" => Ok(BundleState::Delivering),
        "delivered" => Ok(BundleState::Delivered),
        "failed_retryable" => Ok(BundleState::FailedRetryable),
        "failed_terminal" => Ok(BundleState::FailedTerminal),
        other => bail!("invalid bundle state: {other}"),
    }
}

fn hex_sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn validate_bundle_bytes(bytes: usize) -> Result<()> {
    if bytes > MAX_BUNDLE_BYTES {
        bail!("bundle exceeds {MAX_BUNDLE_BYTES} decoded bytes");
    }
    Ok(())
}

fn validate_manifest_bytes(bytes: usize) -> Result<()> {
    if bytes > MAX_MANIFEST_BYTES {
        bail!("bundle manifest exceeds {MAX_MANIFEST_BYTES} bytes");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use anyhow::Result;
    use async_trait::async_trait;

    use crate::runtime::{
        ipc::protocol::ServerEvent,
        model::{MAX_BUNDLE_DELIVERIES, ManualClock},
        outbox_worker::{BundleDeliverySink, OutboxWorker},
        store::{
            AckOutcome, BundleAck, BundleStore, ClaimRenewal, ClaimTransition, ClaimedBundleLoad,
            FinalPayload,
        },
        stream_hub::StreamHub,
    };

    use super::*;

    #[test]
    fn manifest_rejects_more_than_one_thousand_twenty_four_memberships() {
        let deliveries = (0..=MAX_BUNDLE_DELIVERIES)
            .map(|_| (DeliveryId::new(), FinalPayload::Text { text: "x".into() }))
            .collect::<Vec<_>>();
        assert!(manifest_for(&deliveries).is_err());
    }

    #[test]
    fn manifest_records_chunking_and_canonical_hash() {
        let deliveries = vec![(
            DeliveryId::new(),
            FinalPayload::Text {
                text: "x".repeat(MAX_FINAL_CHUNK_BYTES),
            },
        )];
        let manifest = manifest_for(&deliveries).unwrap();
        assert_eq!(manifest.entries.len(), 1);
        assert_eq!(manifest.entries[0].chunk_count, 2);
        assert_eq!(manifest.entries[0].sha256.len(), 64);
        assert_eq!(manifest.sha256.len(), 64);
    }

    #[test]
    fn manifest_rejects_more_than_sixteen_mibibytes() {
        let deliveries = vec![(
            DeliveryId::new(),
            FinalPayload::Text {
                text: "x".repeat(MAX_BUNDLE_BYTES + 1),
            },
        )];
        assert!(manifest_for(&deliveries).is_err());
    }

    #[test]
    fn decoded_bundle_and_manifest_limits_are_inclusive() {
        assert!(validate_bundle_bytes(MAX_BUNDLE_BYTES).is_ok());
        assert!(validate_bundle_bytes(MAX_BUNDLE_BYTES + 1).is_err());
        assert!(validate_manifest_bytes(MAX_MANIFEST_BYTES).is_ok());
        assert!(validate_manifest_bytes(MAX_MANIFEST_BYTES + 1).is_err());
    }

    #[tokio::test]
    async fn ack_requires_exact_manifest_and_accepts_retryable_stale_ack() {
        let store = SqliteRuntimeStore::open_in_memory().await.unwrap();
        let seeded = seed_bundle(&store, "pending", 2, false).await.unwrap();
        assert!(
            store
                .acknowledge_bundle(seeded.ack(), Timestamp(2))
                .await
                .is_err()
        );
        let claimed = store
            .claim_ready_bundles(
                "worker",
                std::slice::from_ref(&seeded.request_id),
                Timestamp(2),
                Timestamp(30),
                1,
            )
            .await
            .unwrap()
            .pop()
            .unwrap();
        let mut partial = seeded.ack();
        partial.delivery_ids.pop();
        assert!(
            store
                .acknowledge_bundle(partial, Timestamp(3))
                .await
                .is_err()
        );
        store
            .fail_bundle_retryable(
                &claimed.claim,
                "connection closed",
                Timestamp(20),
                Timestamp(4),
            )
            .await
            .unwrap();
        assert_eq!(
            store
                .acknowledge_bundle(seeded.ack(), Timestamp(5))
                .await
                .unwrap(),
            AckOutcome::Delivered
        );
        assert_eq!(
            store
                .acknowledge_bundle(seeded.ack(), Timestamp(6))
                .await
                .unwrap(),
            AckOutcome::AlreadyDelivered
        );
    }

    #[tokio::test]
    async fn replay_and_ack_reject_another_actor_in_the_same_transaction() {
        let store = SqliteRuntimeStore::open_in_memory().await.unwrap();
        let seeded = seed_bundle(&store, "delivering", 1, false).await.unwrap();
        let other = ActorId::from_string("actor:other");
        assert!(
            store
                .replay_bundle(&other, &seeded.request_id)
                .await
                .unwrap()
                .is_none()
        );
        let mut ack = seeded.ack();
        ack.actor_id = other;
        assert!(store.acknowledge_bundle(ack, Timestamp(2)).await.is_err());
        assert_eq!(bundle_state(&store, &seeded.bundle_id).await, "delivering");
    }

    #[tokio::test]
    async fn renew_and_retry_failure_reject_stale_bundle_claims() {
        let store = SqliteRuntimeStore::open_in_memory().await.unwrap();
        let seeded = seed_bundle(&store, "pending", 1, false).await.unwrap();
        let claim = store
            .claim_ready_bundles(
                "worker",
                std::slice::from_ref(&seeded.request_id),
                Timestamp(2),
                Timestamp(30),
                1,
            )
            .await
            .unwrap()
            .pop()
            .unwrap()
            .claim;
        let mut wrong_owner = claim.clone();
        wrong_owner.owner = "other-worker".into();
        assert_eq!(
            store
                .renew_bundle(&wrong_owner, Timestamp(3), Timestamp(40))
                .await
                .unwrap(),
            ClaimRenewal::Fenced
        );
        let mut wrong_expiry = claim.clone();
        wrong_expiry.expires_at = Timestamp(29);
        assert_eq!(
            store
                .fail_bundle_retryable(&wrong_expiry, "stale", Timestamp(10), Timestamp(3))
                .await
                .unwrap(),
            ClaimTransition::Fenced
        );
        let ClaimRenewal::Renewed(renewed) = store
            .renew_bundle(&claim, Timestamp(3), Timestamp(40))
            .await
            .unwrap()
        else {
            panic!("live claim was fenced");
        };
        assert_eq!(
            store
                .fail_bundle_retryable(&claim, "old fence", Timestamp(10), Timestamp(4))
                .await
                .unwrap(),
            ClaimTransition::Fenced
        );
        assert_eq!(
            store
                .fail_bundle_retryable(&renewed, "disconnect", Timestamp(10), Timestamp(4))
                .await
                .unwrap(),
            ClaimTransition::Applied
        );
    }

    #[tokio::test]
    async fn renewal_returns_typed_fence_for_stale_owner() {
        let store = SqliteRuntimeStore::open_in_memory().await.unwrap();
        let seeded = seed_bundle(&store, "pending", 1, false).await.unwrap();
        let claim = store
            .claim_ready_bundle_refs(
                "worker",
                std::slice::from_ref(&seeded.request_id),
                Timestamp(2),
                Timestamp(30),
                1,
            )
            .await
            .unwrap()
            .pop()
            .unwrap()
            .claim;
        let mut stale = claim;
        stale.owner = "other".into();
        assert_eq!(
            store
                .renew_bundle(&stale, Timestamp(3), Timestamp(40))
                .await
                .unwrap(),
            ClaimRenewal::Fenced
        );
    }

    #[tokio::test]
    async fn renewal_schema_failure_propagates_instead_of_returning_fenced() {
        let store = SqliteRuntimeStore::open_in_memory().await.unwrap();
        let seeded = seed_bundle(&store, "pending", 1, false).await.unwrap();
        let claim = store
            .claim_ready_bundle_refs(
                "worker",
                std::slice::from_ref(&seeded.request_id),
                Timestamp(2),
                Timestamp(30),
                1,
            )
            .await
            .unwrap()
            .pop()
            .unwrap()
            .claim;
        store
            .connection
            .call(|connection| {
                connection.execute_batch(
                    "ALTER TABLE result_bundles RENAME TO unavailable_result_bundles",
                )
            })
            .await
            .unwrap();

        assert!(
            store
                .renew_bundle(&claim, Timestamp(3), Timestamp(40))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn retry_schema_failure_propagates_instead_of_returning_fenced() {
        let store = SqliteRuntimeStore::open_in_memory().await.unwrap();
        let seeded = seed_bundle(&store, "pending", 1, false).await.unwrap();
        let claim = store
            .claim_ready_bundle_refs(
                "worker",
                std::slice::from_ref(&seeded.request_id),
                Timestamp(2),
                Timestamp(30),
                1,
            )
            .await
            .unwrap()
            .pop()
            .unwrap()
            .claim;
        store
            .connection
            .call(|connection| {
                connection.execute_batch(
                    "ALTER TABLE result_bundles RENAME TO unavailable_result_bundles",
                )
            })
            .await
            .unwrap();

        assert!(
            store
                .fail_bundle_retryable(&claim, "disconnect", Timestamp(10), Timestamp(3),)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn claimed_load_requires_exact_live_claim_and_observes_ack_race() {
        let store = SqliteRuntimeStore::open_in_memory().await.unwrap();
        let seeded = seed_bundle(&store, "pending", 1, false).await.unwrap();
        let claim = store
            .claim_ready_bundle_refs(
                "worker",
                std::slice::from_ref(&seeded.request_id),
                Timestamp(2),
                Timestamp(30),
                1,
            )
            .await
            .unwrap()
            .pop()
            .unwrap()
            .claim;
        let mut wrong_owner = claim.clone();
        wrong_owner.owner = "other".into();
        assert_eq!(
            store
                .load_claimed_bundle(&wrong_owner, Timestamp(3))
                .await
                .unwrap(),
            ClaimedBundleLoad::Fenced
        );
        let mut wrong_expiry = claim.clone();
        wrong_expiry.expires_at = Timestamp(29);
        assert_eq!(
            store
                .load_claimed_bundle(&wrong_expiry, Timestamp(3))
                .await
                .unwrap(),
            ClaimedBundleLoad::Fenced
        );
        assert_eq!(
            store
                .load_claimed_bundle(&claim, Timestamp(30))
                .await
                .unwrap(),
            ClaimedBundleLoad::Fenced
        );
        store
            .acknowledge_bundle(seeded.ack(), Timestamp(3))
            .await
            .unwrap();
        assert_eq!(
            store
                .load_claimed_bundle(&claim, Timestamp(4))
                .await
                .unwrap(),
            ClaimedBundleLoad::Delivered
        );
    }

    #[derive(Default)]
    struct NoopDeliverySink;

    #[async_trait]
    impl BundleDeliverySink for NoopDeliverySink {
        async fn send(&self, _event: ServerEvent) -> Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn worker_terminalizes_malformed_claimed_bundle_in_sqlite() {
        let store = Arc::new(SqliteRuntimeStore::open_in_memory().await.unwrap());
        let seeded = seed_bundle(&store, "pending", 2, false).await.unwrap();
        let bundle_id = seeded.bundle_id.to_string();
        store
            .connection
            .call(move |connection| {
                connection
                    .execute_batch("DROP TRIGGER outbox_deliveries_are_immutable_on_update;")?;
                connection.execute(
                    "UPDATE outbox_deliveries SET ordinal = 3
                     WHERE bundle_id = ?1 AND ordinal = 1",
                    [bundle_id],
                )
            })
            .await
            .unwrap();
        let hub = Arc::new(StreamHub::default());
        let _subscription = hub
            .subscribe_with_delivery_sink(seeded.request_id.clone(), Arc::new(NoopDeliverySink))
            .unwrap();

        OutboxWorker::new(store.clone(), hub, ManualClock::new(10), "worker")
            .run_once()
            .await
            .unwrap();
        assert_eq!(
            bundle_state(&store, &seeded.bundle_id).await,
            "failed_terminal"
        );
    }

    #[tokio::test]
    async fn expired_delivering_bundle_is_recovered_and_reclaimed_without_stealing_live_claim() {
        let store = SqliteRuntimeStore::open_in_memory().await.unwrap();
        let expired = seed_bundle(&store, "delivering", 1, false).await.unwrap();
        let live = seed_bundle(&store, "delivering", 1, false).await.unwrap();
        let live_bundle = live.bundle_id.to_string();
        store
            .connection
            .call(move |connection| {
                connection.execute(
                    "UPDATE result_bundles SET claim_expires_at = 50 WHERE id = ?1",
                    [live_bundle],
                )
            })
            .await
            .unwrap();
        let old_claim = BundleClaim {
            bundle_id: expired.bundle_id.clone(),
            owner: "worker".into(),
            expires_at: Timestamp(30),
        };

        let claimed = store
            .claim_ready_bundles(
                "replacement",
                &[expired.request_id.clone(), live.request_id.clone()],
                Timestamp(31),
                Timestamp(60),
                32,
            )
            .await
            .unwrap();

        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].bundle.id, expired.bundle_id);
        assert_eq!(claimed[0].claim.owner, "replacement");
        assert_eq!(
            store
                .renew_bundle(&old_claim, Timestamp(32), Timestamp(70))
                .await
                .unwrap(),
            ClaimRenewal::Fenced
        );
        assert_eq!(
            store
                .fail_bundle_retryable(&old_claim, "stale worker", Timestamp(40), Timestamp(32))
                .await
                .unwrap(),
            ClaimTransition::Fenced
        );
        assert_eq!(
            store.load_bundle(&live.bundle_id).await.unwrap().state,
            BundleState::Delivering
        );
    }

    #[tokio::test]
    async fn ack_rejects_null_reciprocal_request_bundle_link_without_state_change() {
        let store = SqliteRuntimeStore::open_in_memory().await.unwrap();
        let seeded = seed_bundle(&store, "delivering", 1, false).await.unwrap();
        let request_id = seeded.request_id.to_string();
        store
            .connection
            .call(move |connection| {
                connection.execute_batch("PRAGMA ignore_check_constraints = ON;")?;
                connection.execute(
                    "UPDATE local_requests SET result_bundle_id = NULL WHERE request_id = ?1",
                    [request_id],
                )
            })
            .await
            .unwrap();

        assert!(
            store
                .acknowledge_bundle(seeded.ack(), Timestamp(2))
                .await
                .is_err()
        );
        assert_eq!(bundle_state(&store, &seeded.bundle_id).await, "delivering");
    }

    #[tokio::test]
    async fn ack_rejects_crossed_reciprocal_request_bundle_links_without_state_change() {
        let store = SqliteRuntimeStore::open_in_memory().await.unwrap();
        let first = seed_bundle(&store, "delivering", 1, false).await.unwrap();
        let second = seed_bundle(&store, "delivering", 1, false).await.unwrap();
        let first_request = first.request_id.to_string();
        let second_request = second.request_id.to_string();
        let first_bundle = first.bundle_id.to_string();
        let second_bundle = second.bundle_id.to_string();
        store
            .connection
            .call(move |connection| {
                connection.execute_batch("PRAGMA ignore_check_constraints = ON;")?;
                connection.execute(
                    "UPDATE local_requests SET result_bundle_id = NULL
                     WHERE request_id IN (?1, ?2)",
                    params![first_request, second_request],
                )?;
                connection.execute(
                    "UPDATE local_requests SET result_bundle_id = ?2 WHERE request_id = ?1",
                    params![first_request, second_bundle],
                )?;
                connection.execute(
                    "UPDATE local_requests SET result_bundle_id = ?2 WHERE request_id = ?1",
                    params![second_request, first_bundle],
                )
            })
            .await
            .unwrap();

        assert!(
            store
                .acknowledge_bundle(first.ack(), Timestamp(2))
                .await
                .is_err()
        );
        assert_eq!(bundle_state(&store, &first.bundle_id).await, "delivering");
    }

    #[tokio::test]
    async fn ack_validates_request_route_without_partial_state_change() {
        let store = SqliteRuntimeStore::open_in_memory().await.unwrap();
        let seeded = seed_bundle(&store, "delivering", 1, true).await.unwrap();
        assert!(
            store
                .acknowledge_bundle(seeded.ack(), Timestamp(2))
                .await
                .is_err()
        );
        let bundle_id = seeded.bundle_id.to_string();
        assert_eq!(
            store
                .connection
                .call(move |connection| {
                    connection.query_row(
                        "SELECT state FROM result_bundles WHERE id = ?1",
                        [bundle_id],
                        |row| row.get::<_, String>(0),
                    )
                })
                .await
                .unwrap(),
            "delivering"
        );
    }

    #[tokio::test]
    async fn bundle_load_supports_more_than_worker_claim_batch_and_checks_ordinals() {
        let store = SqliteRuntimeStore::open_in_memory().await.unwrap();
        let seeded = seed_bundle(&store, "pending", 33, false).await.unwrap();
        assert_eq!(
            store
                .load_bundle(&seeded.bundle_id)
                .await
                .unwrap()
                .deliveries
                .len(),
            33
        );

        let malformed = seed_bundle(&store, "pending", 2, false).await.unwrap();
        let bundle_id = malformed.bundle_id.to_string();
        store
            .connection
            .call(move |connection| {
                connection
                    .execute_batch("DROP TRIGGER outbox_deliveries_are_immutable_on_update;")?;
                connection.execute(
                    "UPDATE outbox_deliveries SET ordinal = 3
                     WHERE bundle_id = ?1 AND ordinal = 1",
                    [bundle_id],
                )
            })
            .await
            .unwrap();
        assert!(store.load_bundle(&malformed.bundle_id).await.is_err());
    }

    struct SeededBundle {
        request_id: RequestId,
        bundle_id: BundleId,
        delivery_ids: Vec<DeliveryId>,
    }

    impl SeededBundle {
        fn ack(&self) -> BundleAck {
            BundleAck {
                actor_id: ActorId::from_string("bundle-actor"),
                request_id: self.request_id.clone(),
                bundle_id: self.bundle_id.clone(),
                delivery_ids: self.delivery_ids.clone(),
            }
        }
    }

    async fn bundle_state(store: &SqliteRuntimeStore, id: &BundleId) -> String {
        let id = id.to_string();
        store
            .connection
            .call(move |connection| {
                connection.query_row(
                    "SELECT state FROM result_bundles WHERE id = ?1",
                    [id],
                    |row| row.get(0),
                )
            })
            .await
            .unwrap()
    }

    async fn seed_bundle(
        store: &SqliteRuntimeStore,
        state: &str,
        count: usize,
        wrong_route: bool,
    ) -> Result<SeededBundle> {
        let request_id = RequestId::new();
        let bundle_id = BundleId::new();
        let delivery_ids = (0..count).map(|_| DeliveryId::new()).collect::<Vec<_>>();
        let payloads = delivery_ids
            .iter()
            .map(|id| (id.clone(), FinalPayload::Text { text: "ok".into() }))
            .collect::<Vec<_>>();
        let manifest = manifest_for(&payloads)?;
        let request = request_id.to_string();
        let bundle = bundle_id.to_string();
        let deliveries = delivery_ids.clone();
        let state = state.to_owned();
        store
            .connection
            .call(move |connection| -> Result<()> {
                let transaction = connection.transaction_with_behavior(
                    tokio_rusqlite::rusqlite::TransactionBehavior::Immediate,
                )?;
                transaction.execute_batch(
                    "INSERT OR IGNORE INTO actors(id, enabled, tools_json, created_at)
                     VALUES ('bundle-actor', 1, '[]', 1);",
                )?;
                let work = format!("work-{request}");
                let run = format!("run-{request}");
                let event = format!("event-{request}");
                let sequence = transaction.query_row(
                    "SELECT COALESCE(MAX(mailbox_sequence), 0) + 1 FROM events
                     WHERE actor_id = 'bundle-actor'",
                    [],
                    |row| row.get::<_, i64>(0),
                )?;
                transaction.execute(
                    "INSERT INTO work_items(id, actor_id, kind, audience_kind, state, created_at, updated_at)
                     VALUES (?1, 'bundle-actor', 'interactive', 'actor_private', 'completed', 1, 1)",
                    [work.as_str()],
                )?;
                transaction.execute(
                    "INSERT INTO runs(id, actor_id, work_item_id, state, lease_generation,
                        observed_sequence, created_at, updated_at)
                     VALUES (?1, 'bundle-actor', ?2, 'completed', 1, 1, 1, 1)",
                    params![run, work],
                )?;
                transaction.execute(
                    "INSERT INTO events(id, actor_id, work_item_id, mailbox_sequence, gateway,
                        external_id, kind, audience_kind, payload_json, state, created_at, updated_at)
                     VALUES (?1, 'bundle-actor', ?2, ?4, 'local:submit', ?3,
                        'user_message', 'actor_private', '{}', 'completed', 1, 1)",
                    params![event, work, request, sequence],
                )?;
                transaction.execute(
                    "INSERT INTO local_requests(request_id, actor_id, event_id, work_item_id,
                        prompt_sha256, state, result_bundle_id, created_at, updated_at)
                     VALUES (?1, 'bundle-actor', ?2, ?3, ?4, 'active', NULL, 1, 1)",
                    params![request, event, work, "a".repeat(64)],
                )?;
                transaction.execute(
                    "INSERT INTO result_bundles(id, request_id, delivery_count,
                        manifest_sha256, state, attempt_count, claim_owner, claim_expires_at,
                        created_at, updated_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, 0,
                        CASE WHEN ?5 = 'delivering' THEN 'worker' END,
                        CASE WHEN ?5 = 'delivering' THEN 30 END, 1, 1)",
                    params![bundle, request, count, manifest.sha256, state],
                )?;
                for (ordinal, delivery_id) in deliveries.iter().enumerate() {
                    let outbox_id = format!("outbox-{delivery_id}");
                    transaction.execute(
                        "INSERT INTO outbox(id, intent_key, actor_id, work_item_id, run_id,
                            intent_class, audience_kind, payload_json, created_at)
                         VALUES (?1, ?2, 'bundle-actor', ?3, ?4, 'reply', 'actor_private',
                            '{\"type\":\"text\",\"text\":\"ok\"}', 1)",
                        params![outbox_id, format!("intent-{delivery_id}"), work, run],
                    )?;
                    transaction.execute(
                        "INSERT INTO outbox_deliveries(id, outbox_id, bundle_id, ordinal,
                            transport, address, created_at)
                         VALUES (?1, ?2, ?3, ?4, 'local_ipc', ?5, 1)",
                        params![
                            delivery_id.as_str(),
                            outbox_id,
                            bundle,
                            ordinal,
                            if wrong_route { RequestId::new().to_string() } else { request.clone() },
                        ],
                    )?;
                }
                transaction.execute(
                    "UPDATE local_requests SET state = 'completed', result_bundle_id = ?2
                     WHERE request_id = ?1",
                    params![request, bundle],
                )?;
                transaction.commit()?;
                Ok(())
            })
            .await
            .map_err(map_call_error)?;
        Ok(SeededBundle {
            request_id,
            bundle_id,
            delivery_ids,
        })
    }
}
