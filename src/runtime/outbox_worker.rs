use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use futures_util::future::join_all;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};

use crate::runtime::{
    ipc::protocol::{ServerEvent, prepare_bundle},
    model::{BundleId, BundleState, Clock, MAX_BUNDLE_BYTES, RequestId},
    store::{
        AckOutcome, BundleAck, BundleStore, ClaimRenewal, ClaimTransition, ClaimedBundle,
        ClaimedBundleLoad, ClaimedBundleRef,
    },
};

const CLAIM_MILLIS: i64 = 30_000;
const ACK_DEADLINE: Duration = Duration::from_secs(30);
const RENEW_INTERVAL: Duration = Duration::from_secs(10);
const POLL_INTERVAL: Duration = Duration::from_millis(500);
const CLAIM_BATCH: usize = 32;
const TRANSMISSION_CONCURRENCY: usize = 4;
const GLOBAL_CANONICAL_MEMORY_BYTES: usize = 64 * 1024 * 1024;
const MAX_TRANSMISSION_RESERVATION_BYTES: usize = 2 * MAX_BUNDLE_BYTES;

struct TransmissionMemoryBudget {
    permits: Arc<Semaphore>,
    reserved: AtomicUsize,
    peak: AtomicUsize,
}

impl TransmissionMemoryBudget {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            permits: Arc::new(Semaphore::new(GLOBAL_CANONICAL_MEMORY_BYTES)),
            reserved: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
        })
    }

    async fn reserve_max_bundle(self: &Arc<Self>) -> Result<MemoryReservation> {
        let permit = self
            .permits
            .clone()
            .acquire_many_owned(MAX_TRANSMISSION_RESERVATION_BYTES as u32)
            .await
            .map_err(|_| anyhow!("final transmission memory budget closed"))?;
        let current = self
            .reserved
            .fetch_add(MAX_TRANSMISSION_RESERVATION_BYTES, Ordering::SeqCst)
            + MAX_TRANSMISSION_RESERVATION_BYTES;
        self.peak.fetch_max(current, Ordering::SeqCst);
        Ok(MemoryReservation {
            budget: self.clone(),
            _permit: permit,
        })
    }
}

struct MemoryReservation {
    budget: Arc<TransmissionMemoryBudget>,
    _permit: OwnedSemaphorePermit,
}

impl Drop for MemoryReservation {
    fn drop(&mut self) {
        self.budget
            .reserved
            .fetch_sub(MAX_TRANSMISSION_RESERVATION_BYTES, Ordering::SeqCst);
    }
}

enum AckWait {
    Delivered,
    Retry(crate::runtime::store::BundleClaim),
    Fenced,
}

#[async_trait]
pub trait BundleDeliverySink: Send + Sync {
    fn reserve_transmission(&self, _bundle: &BundleId) -> bool {
        true
    }

    async fn send(&self, event: ServerEvent) -> Result<()>;

    async fn send_shared(&self, event: &ServerEvent) -> Result<()> {
        self.send(event.clone()).await
    }

    async fn abort(&self, _error: &str) {}
}

pub trait DeliveryRegistry: Send + Sync {
    fn subscribed_request_ids(&self) -> Vec<RequestId>;
    fn snapshot(&self, request: &RequestId) -> Vec<Arc<dyn BundleDeliverySink>>;
    fn reserve_snapshot(
        &self,
        request: &RequestId,
        bundle: &BundleId,
    ) -> Vec<Arc<dyn BundleDeliverySink>> {
        self.snapshot(request)
            .into_iter()
            .filter(|sink| sink.reserve_transmission(bundle))
            .collect()
    }
    fn subscribe_changes(&self) -> watch::Receiver<u64>;
}

pub struct OutboxWorker<S, R, C> {
    store: Arc<S>,
    registry: Arc<R>,
    clock: C,
    owner: String,
    transmissions: Arc<Semaphore>,
    memory: Arc<TransmissionMemoryBudget>,
}

impl<S, R, C> OutboxWorker<S, R, C>
where
    S: BundleStore + 'static,
    R: DeliveryRegistry + 'static,
    C: Clock,
{
    pub fn new(store: Arc<S>, registry: Arc<R>, clock: C, owner: impl Into<String>) -> Self {
        let owner = owner.into();
        assert!(
            !owner.trim().is_empty(),
            "outbox worker owner must not be empty"
        );
        Self {
            store,
            registry,
            clock,
            owner,
            transmissions: Arc::new(Semaphore::new(TRANSMISSION_CONCURRENCY)),
            memory: TransmissionMemoryBudget::new(),
        }
    }

    #[cfg(test)]
    fn peak_reserved_bytes(&self) -> usize {
        self.memory.peak.load(Ordering::SeqCst)
    }

    pub async fn run(&self, mut shutdown: watch::Receiver<bool>) -> Result<()> {
        let mut changes = self.registry.subscribe_changes();
        loop {
            if *shutdown.borrow() {
                return Ok(());
            }
            self.run_once().await?;
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return Ok(());
                    }
                }
                _ = changes.changed() => {}
                _ = tokio::time::sleep(POLL_INTERVAL) => {}
            }
        }
    }

    pub async fn run_once(&self) -> Result<usize> {
        let requests = self.registry.subscribed_request_ids();
        if requests.is_empty() {
            return Ok(0);
        }
        let now = self.clock.now();
        let claimed = self
            .store
            .claim_ready_bundle_refs(
                &self.owner,
                &requests,
                now,
                now.plus_millis(CLAIM_MILLIS),
                CLAIM_BATCH,
            )
            .await?;
        let count = claimed.len();
        let results = join_all(
            claimed
                .into_iter()
                .map(|claimed| self.await_transmission_slot(claimed, self.transmissions.clone())),
        )
        .await;
        for result in results {
            result?;
        }
        Ok(count)
    }

    pub async fn acknowledge(&self, ack: BundleAck) -> Result<AckOutcome> {
        self.store.acknowledge_bundle(ack, self.clock.now()).await
    }

    pub async fn replay(
        &self,
        actor: &crate::runtime::model::ActorId,
        request: &RequestId,
        sink: Arc<dyn BundleDeliverySink>,
    ) -> Result<bool> {
        let Some(replay) = self.store.resolve_replay_bundle(actor, request).await? else {
            return Ok(false);
        };
        let _permit = self
            .transmissions
            .acquire()
            .await
            .map_err(|_| anyhow!("final transmission limiter closed"))?;
        let _memory = self.memory.reserve_max_bundle().await?;
        let bundle = self.store.load_replay_bundle(&replay).await?;
        let prepared = prepare_bundle(&bundle, true).map_err(|error| anyhow!(error))?;
        drop(bundle);
        for event in prepared.events() {
            sink.send_shared(&event).await?;
        }
        Ok(true)
    }

    async fn await_transmission_slot(
        &self,
        mut claimed: ClaimedBundleRef,
        transmissions: Arc<Semaphore>,
    ) -> Result<()> {
        let mut renewal = tokio::time::interval(RENEW_INTERVAL);
        renewal.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        renewal.tick().await;
        let permit = loop {
            tokio::select! {
                permit = transmissions.clone().acquire_owned() => {
                    match permit {
                        Ok(permit) => break permit,
                        Err(_) => return Ok(()),
                    }
                }
                _ = renewal.tick() => {
                    let now = self.clock.now();
                    match self.store.renew_bundle(
                        &claimed.claim,
                        now,
                        now.plus_millis(CLAIM_MILLIS),
                    ).await {
                        Ok(ClaimRenewal::Renewed(renewed)) => claimed.claim = renewed,
                        Ok(ClaimRenewal::Fenced) => return Ok(()),
                        Err(error) => return Err(error),
                    }
                }
            }
        };
        let memory = loop {
            tokio::select! {
                reservation = self.memory.reserve_max_bundle() => break reservation?,
                _ = renewal.tick() => {
                    let now = self.clock.now();
                    match self.store.renew_bundle(
                        &claimed.claim,
                        now,
                        now.plus_millis(CLAIM_MILLIS),
                    ).await {
                        Ok(ClaimRenewal::Renewed(renewed)) => claimed.claim = renewed,
                        Ok(ClaimRenewal::Fenced) => return Ok(()),
                        Err(error) => return Err(error),
                    }
                }
            }
        };
        let now = self.clock.now();
        claimed.claim = match self
            .store
            .renew_bundle(&claimed.claim, now, now.plus_millis(CLAIM_MILLIS))
            .await
        {
            Ok(ClaimRenewal::Renewed(claim)) => claim,
            Ok(ClaimRenewal::Fenced) => return Ok(()),
            Err(error) => return Err(error),
        };
        let bundle = match self
            .store
            .load_claimed_bundle(&claimed.claim, self.clock.now())
            .await?
        {
            ClaimedBundleLoad::Loaded(bundle) => bundle,
            ClaimedBundleLoad::FailedTerminal
            | ClaimedBundleLoad::Delivered
            | ClaimedBundleLoad::Fenced => return Ok(()),
        };
        let result = self
            .prepare_and_transmit(ClaimedBundle {
                claim: claimed.claim,
                bundle,
                attempt_count: claimed.attempt_count,
            })
            .await;
        drop(memory);
        drop(permit);
        result
    }

    async fn prepare_and_transmit(&self, claimed: ClaimedBundle) -> Result<()> {
        let ClaimedBundle {
            claim,
            bundle,
            attempt_count,
        } = claimed;
        let request_id = bundle.request_id.clone();
        let bundle_id = bundle.id.clone();
        let prepared = match prepare_bundle(&bundle, false) {
            Ok(prepared) => Arc::new(prepared),
            Err(error) => {
                self.store
                    .fail_bundle_terminal(&claim, &error.to_string(), self.clock.now())
                    .await?;
                return Ok(());
            }
        };
        drop(bundle);
        self.transmit(claim, request_id, bundle_id, attempt_count, prepared)
            .await
    }

    async fn transmit(
        &self,
        claim: crate::runtime::store::BundleClaim,
        request_id: RequestId,
        bundle_id: BundleId,
        attempt_count: usize,
        prepared: Arc<crate::runtime::ipc::protocol::PreparedBundle>,
    ) -> Result<()> {
        let recipients = self.registry.reserve_snapshot(&request_id, &bundle_id);
        if recipients.is_empty() {
            self.retry(claim, attempt_count, "all bundle subscribers disconnected")
                .await?;
            return Ok(());
        }

        let sends = join_all(recipients.iter().cloned().map(|sink| {
            let prepared = prepared.clone();
            async move {
                for frame in prepared.events() {
                    sink.send_shared(&frame).await?;
                }
                Result::<Arc<dyn BundleDeliverySink>>::Ok(sink)
            }
        }));
        tokio::pin!(sends);
        let mut claim = claim;
        let mut renewal = tokio::time::interval(RENEW_INTERVAL);
        renewal.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        renewal.tick().await;

        loop {
            let results = tokio::select! {
                results = &mut sends => results,
                _ = renewal.tick() => {
                    let now = self.clock.now();
                    match self.store.renew_bundle(&claim, now, now.plus_millis(CLAIM_MILLIS)).await {
                        Ok(ClaimRenewal::Renewed(renewed)) => claim = renewed,
                        Ok(ClaimRenewal::Fenced) => {
                            match self.store.load_bundle_state(&claim.bundle_id).await {
                                Ok(BundleState::Delivered) => {}
                                Ok(_) => {
                                    abort_recipients(&recipients, "bundle delivery claim was fenced").await;
                                    return Ok(());
                                }
                                Err(error) => return Err(error),
                            }
                        }
                        Err(error) => return Err(error),
                    }
                    continue;
                }
            };
            if results.iter().all(Result::is_err) {
                self.retry(claim, attempt_count, "every bundle subscriber failed")
                    .await?;
            } else {
                match self.wait_for_ack(claim, &request_id, &recipients).await? {
                    AckWait::Delivered => {}
                    AckWait::Retry(active_claim) => {
                        abort_recipients(&recipients, "bundle acknowledgement deadline elapsed")
                            .await;
                        self.retry(
                            active_claim,
                            attempt_count,
                            "bundle acknowledgement deadline elapsed",
                        )
                        .await?;
                    }
                    AckWait::Fenced => {
                        abort_recipients(&recipients, "bundle delivery claim was fenced").await;
                    }
                }
            }
            break;
        }
        Ok(())
    }

    async fn wait_for_ack(
        &self,
        mut claim: crate::runtime::store::BundleClaim,
        request: &RequestId,
        recipients: &[Arc<dyn BundleDeliverySink>],
    ) -> Result<AckWait> {
        let deadline = tokio::time::sleep(ACK_DEADLINE);
        tokio::pin!(deadline);
        let mut renewal = tokio::time::interval(RENEW_INTERVAL);
        renewal.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        renewal.tick().await;
        let mut state_poll = tokio::time::interval(POLL_INTERVAL);
        loop {
            tokio::select! {
                _ = &mut deadline => return Ok(AckWait::Retry(claim)),
                _ = state_poll.tick() => {
                    match self.store.load_bundle_state(&claim.bundle_id).await {
                        Ok(BundleState::Delivered) => return Ok(AckWait::Delivered),
                        Ok(_) => {}
                        Err(error) => return Err(error),
                    }
                    let current = self.registry.snapshot(request);
                    if !recipients.iter().any(|recipient| {
                        current.iter().any(|sink| Arc::ptr_eq(recipient, sink))
                    }) {
                        return Ok(AckWait::Retry(claim));
                    }
                }
                _ = renewal.tick() => {
                    let now = self.clock.now();
                    match self.store.renew_bundle(&claim, now, now.plus_millis(CLAIM_MILLIS)).await {
                        Ok(ClaimRenewal::Renewed(renewed)) => claim = renewed,
                        Ok(ClaimRenewal::Fenced) => {
                            return match self.store.load_bundle_state(&claim.bundle_id).await {
                                Ok(BundleState::Delivered) => Ok(AckWait::Delivered),
                                Ok(_) => Ok(AckWait::Fenced),
                                Err(error) => Err(error),
                            };
                        }
                        Err(error) => return Err(error),
                    }
                }
            }
        }
    }

    async fn retry(
        &self,
        claim: crate::runtime::store::BundleClaim,
        attempt_count: usize,
        error: &str,
    ) -> Result<()> {
        let now = self.clock.now();
        let delay = retry_delay_seconds(attempt_count);
        match self
            .store
            .fail_bundle_retryable(&claim, error, now.plus_millis(delay * 1_000), now)
            .await?
        {
            ClaimTransition::Applied => Ok(()),
            ClaimTransition::Fenced => match self.store.load_bundle_state(&claim.bundle_id).await {
                Ok(BundleState::Delivered) => Ok(()),
                Ok(_) => Ok(()),
                Err(error) => Err(error),
            },
        }
    }
}

async fn abort_recipients(recipients: &[Arc<dyn BundleDeliverySink>], error: &str) {
    join_all(recipients.iter().map(|recipient| recipient.abort(error))).await;
}

fn retry_delay_seconds(attempt_count: usize) -> i64 {
    match attempt_count {
        0 | 1 => 1,
        2 => 2,
        3 => 4,
        4 => 8,
        _ => 30,
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use anyhow::{Result, bail};
    use async_trait::async_trait;
    use tokio::sync::{Notify, watch};

    use crate::runtime::{
        ipc::protocol::{ServerEvent, ServerEventBody},
        model::{
            BundleId, BundleState, DeliveryId, MAX_BUNDLE_BYTES, ManualClock, RequestId, Timestamp,
        },
        store::{
            AckOutcome, BundleAck, BundleClaim, BundleManifest, BundleStore, ClaimRenewal,
            ClaimTransition, ClaimedBundle, ClaimedBundleLoad, ClaimedBundleRef, FinalPayload,
            ReplayBundleRef, ResultBundle,
        },
    };

    use super::{BundleDeliverySink, DeliveryRegistry, OutboxWorker, retry_delay_seconds};

    #[derive(Default)]
    struct FakeStore {
        bundles: Mutex<Vec<ResultBundle>>,
        claim_calls: AtomicUsize,
        claimed_loads: AtomicUsize,
        renewals: AtomicUsize,
        renewals_by_bundle: Mutex<HashMap<BundleId, usize>>,
        retry_delays: Mutex<Vec<i64>>,
        terminal_failures: AtomicUsize,
        replay_calls: AtomicUsize,
        acks: Mutex<Vec<BundleAck>>,
        auto_ack_on_load: AtomicBool,
        active_claims: Mutex<HashMap<BundleId, BundleClaim>>,
        renew_authority_error: AtomicBool,
        retry_authority_error: AtomicBool,
        ack_before_retry: AtomicBool,
        ack_before_claimed_load: AtomicBool,
    }

    impl FakeStore {
        fn with_bundles(count: usize) -> Arc<Self> {
            Arc::new(Self {
                bundles: Mutex::new((0..count).map(|_| bundle()).collect()),
                auto_ack_on_load: AtomicBool::new(true),
                ..Self::default()
            })
        }

        fn set_delivered(&self, id: &BundleId) {
            if let Some(bundle) = self
                .bundles
                .lock()
                .unwrap()
                .iter_mut()
                .find(|bundle| &bundle.id == id)
            {
                bundle.state = BundleState::Delivered;
            }
            self.active_claims.lock().unwrap().remove(id);
        }
    }

    #[async_trait]
    impl BundleStore for FakeStore {
        async fn claim_ready_bundle_refs(
            &self,
            owner: &str,
            request_ids: &[RequestId],
            now: Timestamp,
            until: Timestamp,
            limit: usize,
        ) -> Result<Vec<ClaimedBundleRef>> {
            Ok(self
                .claim_ready_bundles(owner, request_ids, now, until, limit)
                .await?
                .into_iter()
                .map(|claimed| ClaimedBundleRef {
                    claim: claimed.claim,
                    request_id: claimed.bundle.request_id,
                    attempt_count: claimed.attempt_count,
                })
                .collect())
        }

        async fn claim_ready_bundles(
            &self,
            owner: &str,
            request_ids: &[RequestId],
            _now: Timestamp,
            until: Timestamp,
            limit: usize,
        ) -> Result<Vec<ClaimedBundle>> {
            self.claim_calls.fetch_add(1, Ordering::SeqCst);
            let mut bundles = self.bundles.lock().unwrap();
            let mut claims = Vec::new();
            for bundle in bundles.iter_mut() {
                if claims.len() == limit {
                    break;
                }
                if bundle.state == BundleState::Pending
                    && request_ids.iter().any(|id| id == &bundle.request_id)
                {
                    bundle.state = BundleState::Delivering;
                    claims.push(ClaimedBundle {
                        claim: BundleClaim {
                            bundle_id: bundle.id.clone(),
                            owner: owner.into(),
                            expires_at: until,
                        },
                        bundle: bundle.clone(),
                        attempt_count: 1,
                    });
                    let claim = claims.last().unwrap().claim.clone();
                    self.active_claims
                        .lock()
                        .unwrap()
                        .insert(claim.bundle_id.clone(), claim);
                }
            }
            Ok(claims)
        }

        async fn renew_bundle(
            &self,
            claim: &BundleClaim,
            now: Timestamp,
            until: Timestamp,
        ) -> Result<ClaimRenewal> {
            if self.renew_authority_error.load(Ordering::SeqCst) {
                bail!("simulated renewal authority failure");
            }
            self.renewals.fetch_add(1, Ordering::SeqCst);
            *self
                .renewals_by_bundle
                .lock()
                .unwrap()
                .entry(claim.bundle_id.clone())
                .or_default() += 1;
            let mut claims = self.active_claims.lock().unwrap();
            if claims.get(&claim.bundle_id) != Some(claim) || claim.expires_at <= now {
                return Ok(ClaimRenewal::Fenced);
            }
            let renewed = BundleClaim {
                expires_at: until,
                ..claim.clone()
            };
            claims.insert(claim.bundle_id.clone(), renewed.clone());
            Ok(ClaimRenewal::Renewed(renewed))
        }

        async fn load_claimed_bundle(
            &self,
            claim: &BundleClaim,
            now: Timestamp,
        ) -> Result<ClaimedBundleLoad> {
            self.claimed_loads.fetch_add(1, Ordering::SeqCst);
            if self.ack_before_claimed_load.swap(false, Ordering::SeqCst) {
                self.set_delivered(&claim.bundle_id);
            }
            if self.active_claims.lock().unwrap().get(&claim.bundle_id) != Some(claim)
                || claim.expires_at <= now
            {
                return Ok(
                    if self.bundles.lock().unwrap().iter().any(|bundle| {
                        bundle.id == claim.bundle_id && bundle.state == BundleState::Delivered
                    }) {
                        ClaimedBundleLoad::Delivered
                    } else {
                        ClaimedBundleLoad::Fenced
                    },
                );
            }
            Ok(ClaimedBundleLoad::Loaded(
                self.bundles
                    .lock()
                    .unwrap()
                    .iter()
                    .find(|bundle| bundle.id == claim.bundle_id)
                    .cloned()
                    .ok_or_else(|| anyhow::anyhow!("missing bundle"))?,
            ))
        }

        async fn load_bundle(&self, id: &BundleId) -> Result<ResultBundle> {
            let mut bundles = self.bundles.lock().unwrap();
            let bundle = bundles
                .iter_mut()
                .find(|bundle| &bundle.id == id)
                .ok_or_else(|| anyhow::anyhow!("missing bundle"))?;
            let auto_ack = self.auto_ack_on_load.load(Ordering::SeqCst)
                && bundle.state == BundleState::Delivering;
            if auto_ack {
                bundle.state = BundleState::Delivered;
            }
            let bundle = bundle.clone();
            drop(bundles);
            if auto_ack {
                self.active_claims.lock().unwrap().remove(id);
            }
            Ok(bundle)
        }

        async fn load_bundle_state(&self, id: &BundleId) -> Result<BundleState> {
            let mut bundles = self.bundles.lock().unwrap();
            let bundle = bundles
                .iter_mut()
                .find(|bundle| &bundle.id == id)
                .ok_or_else(|| anyhow::anyhow!("missing bundle"))?;
            let auto_ack = self.auto_ack_on_load.load(Ordering::SeqCst)
                && bundle.state == BundleState::Delivering;
            if auto_ack {
                bundle.state = BundleState::Delivered;
            }
            let state = bundle.state;
            if auto_ack {
                self.active_claims.lock().unwrap().remove(id);
            }
            Ok(state)
        }

        async fn acknowledge_bundle(&self, ack: BundleAck, _now: Timestamp) -> Result<AckOutcome> {
            self.acks.lock().unwrap().push(ack.clone());
            self.set_delivered(&ack.bundle_id);
            Ok(AckOutcome::Delivered)
        }

        async fn fail_bundle_retryable(
            &self,
            claim: &BundleClaim,
            _error: &str,
            next_attempt: Timestamp,
            now: Timestamp,
        ) -> Result<ClaimTransition> {
            if self.retry_authority_error.load(Ordering::SeqCst) {
                bail!("simulated retry authority failure");
            }
            if self.ack_before_retry.swap(false, Ordering::SeqCst) {
                self.set_delivered(&claim.bundle_id);
            }
            let mut active = self.active_claims.lock().unwrap();
            if active.get(&claim.bundle_id) != Some(claim) || claim.expires_at <= now {
                return Ok(ClaimTransition::Fenced);
            }
            active.remove(&claim.bundle_id);
            self.retry_delays
                .lock()
                .unwrap()
                .push(next_attempt.0 - now.0);
            if let Some(bundle) = self
                .bundles
                .lock()
                .unwrap()
                .iter_mut()
                .find(|bundle| bundle.id == claim.bundle_id)
            {
                bundle.state = BundleState::FailedRetryable;
            }
            Ok(ClaimTransition::Applied)
        }

        async fn fail_bundle_terminal(
            &self,
            claim: &BundleClaim,
            _error: &str,
            _now: Timestamp,
        ) -> Result<()> {
            self.terminal_failures.fetch_add(1, Ordering::SeqCst);
            if let Some(bundle) = self
                .bundles
                .lock()
                .unwrap()
                .iter_mut()
                .find(|bundle| bundle.id == claim.bundle_id)
            {
                bundle.state = BundleState::FailedTerminal;
            }
            Ok(())
        }

        async fn resolve_replay_bundle(
            &self,
            actor: &crate::runtime::model::ActorId,
            request: &RequestId,
        ) -> Result<Option<ReplayBundleRef>> {
            Ok(self
                .bundles
                .lock()
                .unwrap()
                .iter()
                .find(|bundle| {
                    &bundle.request_id == request && bundle.state == BundleState::Delivered
                })
                .map(|bundle| ReplayBundleRef {
                    actor_id: actor.clone(),
                    request_id: request.clone(),
                    bundle_id: bundle.id.clone(),
                }))
        }

        async fn load_replay_bundle(&self, replay: &ReplayBundleRef) -> Result<ResultBundle> {
            self.replay_calls.fetch_add(1, Ordering::SeqCst);
            self.bundles
                .lock()
                .unwrap()
                .iter()
                .find(|bundle| bundle.id == replay.bundle_id)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("missing replay bundle"))
        }
    }

    #[derive(Default)]
    struct FakeRegistry {
        sinks: Mutex<HashMap<RequestId, Vec<Arc<dyn BundleDeliverySink>>>>,
        changes: watch::Sender<u64>,
    }

    impl FakeRegistry {
        fn add(&self, request: RequestId, sink: Arc<dyn BundleDeliverySink>) {
            self.sinks
                .lock()
                .unwrap()
                .entry(request)
                .or_default()
                .push(sink);
            self.changes.send_modify(|value| *value += 1);
        }
    }

    impl DeliveryRegistry for FakeRegistry {
        fn subscribed_request_ids(&self) -> Vec<RequestId> {
            self.sinks.lock().unwrap().keys().cloned().collect()
        }

        fn snapshot(&self, request: &RequestId) -> Vec<Arc<dyn BundleDeliverySink>> {
            self.sinks
                .lock()
                .unwrap()
                .get(request)
                .cloned()
                .unwrap_or_default()
        }

        fn subscribe_changes(&self) -> watch::Receiver<u64> {
            self.changes.subscribe()
        }
    }

    #[derive(Default)]
    struct RecordingSink {
        events: Mutex<Vec<ServerEvent>>,
        fail: AtomicBool,
        slow_first: AtomicBool,
    }

    #[async_trait]
    impl BundleDeliverySink for RecordingSink {
        async fn send(&self, event: ServerEvent) -> Result<()> {
            if self.fail.load(Ordering::SeqCst) {
                bail!("disconnected");
            }
            if self.slow_first.swap(false, Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_secs(45)).await;
            }
            self.events.lock().unwrap().push(event);
            Ok(())
        }
    }

    struct SubscribeOnFirstSend {
        registry: Arc<FakeRegistry>,
        request: RequestId,
        late: Arc<RecordingSink>,
        sent: AtomicBool,
        events: Mutex<Vec<ServerEvent>>,
    }

    struct ReservationBarrierSink {
        reserved: Arc<AtomicBool>,
        all_reservations: Vec<Arc<AtomicBool>>,
        checked: AtomicBool,
    }

    #[async_trait]
    impl BundleDeliverySink for ReservationBarrierSink {
        fn reserve_transmission(&self, _bundle: &BundleId) -> bool {
            self.reserved.store(true, Ordering::SeqCst);
            true
        }

        async fn send(&self, _event: ServerEvent) -> Result<()> {
            if !self.checked.swap(true, Ordering::SeqCst) {
                assert!(
                    self.all_reservations
                        .iter()
                        .all(|reserved| reserved.load(Ordering::SeqCst)),
                    "every fixed-snapshot recipient must be reserved before the first send"
                );
            }
            Ok(())
        }
    }

    #[async_trait]
    impl BundleDeliverySink for SubscribeOnFirstSend {
        async fn send(&self, event: ServerEvent) -> Result<()> {
            self.events.lock().unwrap().push(event);
            if !self.sent.swap(true, Ordering::SeqCst) {
                self.registry.add(self.request.clone(), self.late.clone());
            }
            Ok(())
        }
    }

    struct AcknowledgeOnFirstSend {
        store: Arc<FakeStore>,
        bundle_id: BundleId,
        sent: AtomicBool,
    }

    struct BlockingReplaySink {
        started: Arc<AtomicUsize>,
        release: Arc<Notify>,
        blocked: AtomicBool,
    }

    struct FinalEndSink {
        slow_first: AtomicBool,
        completed: Arc<AtomicBool>,
    }

    #[async_trait]
    impl BundleDeliverySink for FinalEndSink {
        async fn send(&self, event: ServerEvent) -> Result<()> {
            if self.slow_first.swap(false, Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_secs(45)).await;
            }
            if matches!(event.body, ServerEventBody::FinalEnd { .. }) {
                self.completed.store(true, Ordering::SeqCst);
            }
            Ok(())
        }
    }

    #[async_trait]
    impl BundleDeliverySink for BlockingReplaySink {
        async fn send(&self, _event: ServerEvent) -> Result<()> {
            if !self.blocked.swap(true, Ordering::SeqCst) {
                self.started.fetch_add(1, Ordering::SeqCst);
                self.release.notified().await;
            }
            Ok(())
        }
    }

    #[async_trait]
    impl BundleDeliverySink for AcknowledgeOnFirstSend {
        async fn send(&self, _event: ServerEvent) -> Result<()> {
            if !self.sent.swap(true, Ordering::SeqCst) {
                self.store.set_delivered(&self.bundle_id);
            }
            Ok(())
        }
    }

    fn bundle() -> ResultBundle {
        ResultBundle {
            id: BundleId::new(),
            request_id: RequestId::new(),
            state: BundleState::Pending,
            manifest: BundleManifest {
                entries: Vec::new(),
                sha256: String::new(),
            },
            deliveries: vec![(
                DeliveryId::new(),
                FinalPayload::Text {
                    text: "done".into(),
                },
            )],
        }
    }

    fn worker(
        store: Arc<FakeStore>,
        registry: Arc<FakeRegistry>,
        clock: ManualClock,
    ) -> OutboxWorker<FakeStore, FakeRegistry, ManualClock> {
        OutboxWorker::new(store, registry, clock, "worker-1")
    }

    #[test]
    fn delivery_retry_schedule_is_one_two_four_eight_then_thirty_seconds() {
        assert_eq!(
            (1..=7).map(retry_delay_seconds).collect::<Vec<_>>(),
            vec![1, 2, 4, 8, 30, 30, 30]
        );
    }

    #[tokio::test]
    async fn no_subscriber_causes_no_claim_or_attempt() -> Result<()> {
        let store = FakeStore::with_bundles(1);
        let registry = Arc::new(FakeRegistry::default());
        assert_eq!(
            worker(store.clone(), registry, ManualClock::new(0))
                .run_once()
                .await?,
            0
        );
        assert_eq!(store.claim_calls.load(Ordering::SeqCst), 0);
        assert_eq!(store.bundles.lock().unwrap()[0].state, BundleState::Pending);
        Ok(())
    }

    #[tokio::test]
    async fn worker_claims_at_most_thirty_two_whole_bundles_and_sends_terminal_frames() -> Result<()>
    {
        let store = FakeStore::with_bundles(33);
        let registry = Arc::new(FakeRegistry::default());
        let sink = Arc::new(RecordingSink::default());
        for bundle in store.bundles.lock().unwrap().iter() {
            registry.add(bundle.request_id.clone(), sink.clone());
        }
        assert_eq!(
            worker(store, registry, ManualClock::new(0))
                .run_once()
                .await?,
            32
        );
        let events = sink.events.lock().unwrap();
        assert_eq!(events.len(), 32 * 3);
        assert!(matches!(
            events.first().unwrap().body,
            ServerEventBody::FinalBegin { .. }
        ));
        assert!(matches!(
            events.last().unwrap().body,
            ServerEventBody::FinalEnd { .. }
        ));
        Ok(())
    }

    #[tokio::test]
    async fn worker_reserves_every_snapshot_recipient_before_any_frame_send() -> Result<()> {
        let store = FakeStore::with_bundles(1);
        let registry = Arc::new(FakeRegistry::default());
        let request = store.bundles.lock().unwrap()[0].request_id.clone();
        let reservations = vec![
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicBool::new(false)),
        ];
        for reserved in &reservations {
            registry.add(
                request.clone(),
                Arc::new(ReservationBarrierSink {
                    reserved: reserved.clone(),
                    all_reservations: reservations.clone(),
                    checked: AtomicBool::new(false),
                }),
            );
        }

        assert_eq!(
            worker(store, registry, ManualClock::new(0))
                .run_once()
                .await?,
            1
        );
        assert!(
            reservations
                .iter()
                .all(|reserved| reserved.load(Ordering::SeqCst))
        );
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn worker_renews_claim_during_slow_transmission() -> Result<()> {
        let store = FakeStore::with_bundles(1);
        let registry = Arc::new(FakeRegistry::default());
        let sink = Arc::new(RecordingSink {
            slow_first: AtomicBool::new(true),
            ..Default::default()
        });
        registry.add(store.bundles.lock().unwrap()[0].request_id.clone(), sink);
        let worker = worker(store.clone(), registry, ManualClock::new(0));
        let (result, ()) = tokio::join!(worker.run_once(), async {
            for _ in 0..3 {
                tokio::time::advance(Duration::from_secs(10)).await;
                tokio::task::yield_now().await;
            }
            assert!(store.renewals.load(Ordering::SeqCst) >= 3);
            tokio::time::advance(Duration::from_secs(20)).await;
        });
        result?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn claimed_bundle_renews_while_waiting_for_one_of_four_transmission_slots() -> Result<()>
    {
        let store = FakeStore::with_bundles(5);
        let waiting_bundle = store.bundles.lock().unwrap()[4].clone();
        let registry = Arc::new(FakeRegistry::default());
        for bundle in store.bundles.lock().unwrap().iter() {
            registry.add(
                bundle.request_id.clone(),
                Arc::new(RecordingSink {
                    slow_first: AtomicBool::new(true),
                    ..Default::default()
                }),
            );
        }
        let worker = worker(store.clone(), registry, ManualClock::new(0));
        let (result, ()) = tokio::join!(worker.run_once(), async {
            tokio::time::advance(Duration::from_secs(11)).await;
            tokio::task::yield_now().await;
            assert!(
                store
                    .renewals_by_bundle
                    .lock()
                    .unwrap()
                    .get(&waiting_bundle.id)
                    .copied()
                    .unwrap_or_default()
                    >= 1
            );
            tokio::time::advance(Duration::from_secs(100)).await;
        });
        result?;
        Ok(())
    }

    #[tokio::test]
    async fn every_failed_sink_records_retryable_disconnect_delay() -> Result<()> {
        let store = FakeStore::with_bundles(1);
        let registry = Arc::new(FakeRegistry::default());
        let sink = Arc::new(RecordingSink {
            fail: AtomicBool::new(true),
            ..Default::default()
        });
        registry.add(store.bundles.lock().unwrap()[0].request_id.clone(), sink);
        worker(store.clone(), registry, ManualClock::new(10))
            .run_once()
            .await?;
        assert_eq!(*store.retry_delays.lock().unwrap(), vec![1_000]);
        assert_eq!(
            store.bundles.lock().unwrap()[0].state,
            BundleState::FailedRetryable
        );
        Ok(())
    }

    #[tokio::test]
    async fn malformed_terminal_bundle_is_failed_terminal_instead_of_retried() -> Result<()> {
        let store = FakeStore::with_bundles(1);
        store.bundles.lock().unwrap()[0].deliveries.clear();
        let registry = Arc::new(FakeRegistry::default());
        let sink = Arc::new(RecordingSink::default());
        registry.add(store.bundles.lock().unwrap()[0].request_id.clone(), sink);
        worker(store.clone(), registry, ManualClock::new(0))
            .run_once()
            .await?;
        assert_eq!(store.terminal_failures.load(Ordering::SeqCst), 1);
        assert!(store.retry_delays.lock().unwrap().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn delivered_replay_is_read_only_and_marks_every_frame_as_replay() -> Result<()> {
        let store = FakeStore::with_bundles(1);
        let request = store.bundles.lock().unwrap()[0].request_id.clone();
        store.bundles.lock().unwrap()[0].state = BundleState::Delivered;
        let registry = Arc::new(FakeRegistry::default());
        let sink = Arc::new(RecordingSink::default());
        assert!(
            worker(store.clone(), registry, ManualClock::new(0))
                .replay(
                    &crate::runtime::model::ActorId::from_string("actor"),
                    &request,
                    sink.clone(),
                )
                .await?
        );
        assert_eq!(store.claim_calls.load(Ordering::SeqCst), 0);
        assert_eq!(store.replay_calls.load(Ordering::SeqCst), 1);
        assert!(matches!(
            sink.events.lock().unwrap()[0].body,
            ServerEventBody::FinalBegin { replay: true, .. }
        ));
        Ok(())
    }

    #[tokio::test]
    async fn sixty_four_delivered_replays_do_not_preload_before_transmission_permits() -> Result<()>
    {
        let store = FakeStore::with_bundles(64);
        for bundle in store.bundles.lock().unwrap().iter_mut() {
            bundle.state = BundleState::Delivered;
        }
        let worker = Arc::new(worker(
            store.clone(),
            Arc::new(FakeRegistry::default()),
            ManualClock::new(0),
        ));
        let started = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(Notify::new());
        let requests = store
            .bundles
            .lock()
            .unwrap()
            .iter()
            .map(|bundle| bundle.request_id.clone())
            .collect::<Vec<_>>();
        let tasks = requests
            .into_iter()
            .map(|request| {
                let worker = worker.clone();
                let sink = Arc::new(BlockingReplaySink {
                    started: started.clone(),
                    release: release.clone(),
                    blocked: AtomicBool::new(false),
                });
                tokio::spawn(async move {
                    worker
                        .replay(
                            &crate::runtime::model::ActorId::from_string("actor"),
                            &request,
                            sink,
                        )
                        .await
                })
            })
            .collect::<Vec<_>>();
        tokio::time::timeout(Duration::from_secs(1), async {
            while started.load(Ordering::SeqCst) < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        tokio::task::yield_now().await;
        assert!(started.load(Ordering::SeqCst) <= 4);
        assert!(
            store.replay_calls.load(Ordering::SeqCst) <= 4,
            "full replay bundle loads must happen only after a transmission permit"
        );
        release.notify_waiters();
        while started.load(Ordering::SeqCst) < 64 {
            tokio::task::yield_now().await;
            release.notify_waiters();
        }
        release.notify_waiters();
        for task in tasks {
            task.await??;
        }
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn fast_recipient_reaches_final_end_without_waiting_for_slow_recipient() -> Result<()> {
        let store = FakeStore::with_bundles(1);
        let registry = Arc::new(FakeRegistry::default());
        let request = store.bundles.lock().unwrap()[0].request_id.clone();
        let slow_completed = Arc::new(AtomicBool::new(false));
        let fast_completed = Arc::new(AtomicBool::new(false));
        registry.add(
            request.clone(),
            Arc::new(FinalEndSink {
                slow_first: AtomicBool::new(true),
                completed: slow_completed.clone(),
            }),
        );
        registry.add(
            request,
            Arc::new(FinalEndSink {
                slow_first: AtomicBool::new(false),
                completed: fast_completed.clone(),
            }),
        );
        let task = tokio::spawn(async move {
            worker(store, registry, ManualClock::new(0))
                .run_once()
                .await
        });
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        assert!(
            fast_completed.load(Ordering::SeqCst),
            "fast recipient must finish independently"
        );
        assert!(!slow_completed.load(Ordering::SeqCst));
        tokio::time::advance(Duration::from_secs(50)).await;
        task.await??;
        assert!(slow_completed.load(Ordering::SeqCst));
        Ok(())
    }

    #[tokio::test]
    async fn max_bundles_never_reserve_more_than_sixty_four_mibibytes() -> Result<()> {
        let store = FakeStore::with_bundles(3);
        for bundle in store.bundles.lock().unwrap().iter_mut() {
            bundle.deliveries[0].1 = FinalPayload::Text {
                text: "x".repeat(MAX_BUNDLE_BYTES - 64),
            };
        }
        store.bundles.lock().unwrap()[2].state = BundleState::Delivered;
        let registry = Arc::new(FakeRegistry::default());
        let worker = Arc::new(worker(store.clone(), registry.clone(), ManualClock::new(0)));
        let started = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(Notify::new());
        let bundles = store.bundles.lock().unwrap().clone();
        for bundle in &bundles[..2] {
            registry.add(
                bundle.request_id.clone(),
                Arc::new(BlockingReplaySink {
                    started: started.clone(),
                    release: release.clone(),
                    blocked: AtomicBool::new(false),
                }),
            );
        }
        let claimed_worker = worker.clone();
        let claimed = tokio::spawn(async move { claimed_worker.run_once().await });
        let replay_worker = worker.clone();
        let replay_request = bundles[2].request_id.clone();
        let replay_sink = Arc::new(BlockingReplaySink {
            started: started.clone(),
            release: release.clone(),
            blocked: AtomicBool::new(false),
        });
        let replay = tokio::spawn(async move {
            replay_worker
                .replay(
                    &crate::runtime::model::ActorId::from_string("actor"),
                    &replay_request,
                    replay_sink,
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(15), async {
            while started.load(Ordering::SeqCst) < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        assert_eq!(worker.peak_reserved_bytes(), 64 * 1024 * 1024);
        assert!(
            store.claimed_loads.load(Ordering::SeqCst) + store.replay_calls.load(Ordering::SeqCst)
                <= 2
        );
        release.notify_waiters();
        while started.load(Ordering::SeqCst) < 3 {
            tokio::task::yield_now().await;
            release.notify_waiters();
        }
        release.notify_waiters();
        claimed.await??;
        replay.await??;
        assert!(worker.peak_reserved_bytes() <= 64 * 1024 * 1024);
        Ok(())
    }

    #[tokio::test]
    async fn acknowledge_routes_the_exact_bundle_ack_to_persistence() -> Result<()> {
        let store = FakeStore::with_bundles(1);
        let bundle = store.bundles.lock().unwrap()[0].clone();
        let ack = BundleAck {
            actor_id: crate::runtime::model::ActorId::from_string("actor"),
            request_id: bundle.request_id,
            bundle_id: bundle.id,
            delivery_ids: bundle.deliveries.into_iter().map(|(id, _)| id).collect(),
        };
        worker(
            store.clone(),
            Arc::new(FakeRegistry::default()),
            ManualClock::new(7),
        )
        .acknowledge(ack.clone())
        .await?;
        assert_eq!(*store.acks.lock().unwrap(), vec![ack]);
        Ok(())
    }

    #[tokio::test]
    async fn recipient_snapshot_excludes_late_subscriber_until_full_replay() -> Result<()> {
        let store = FakeStore::with_bundles(1);
        let bundle = store.bundles.lock().unwrap()[0].clone();
        let registry = Arc::new(FakeRegistry::default());
        let late = Arc::new(RecordingSink::default());
        let first = Arc::new(SubscribeOnFirstSend {
            registry: registry.clone(),
            request: bundle.request_id.clone(),
            late: late.clone(),
            sent: AtomicBool::new(false),
            events: Mutex::new(Vec::new()),
        });
        registry.add(bundle.request_id.clone(), first.clone());
        let worker = worker(store.clone(), registry, ManualClock::new(0));
        worker.run_once().await?;
        assert_eq!(first.events.lock().unwrap().len(), 3);
        assert!(late.events.lock().unwrap().is_empty());

        store.set_delivered(&bundle.id);
        assert!(
            worker
                .replay(
                    &crate::runtime::model::ActorId::from_string("actor"),
                    &bundle.request_id,
                    late.clone(),
                )
                .await?
        );
        let events = late.events.lock().unwrap();
        assert_eq!(events.len(), 3);
        assert!(matches!(
            events[0].body,
            ServerEventBody::FinalBegin { replay: true, .. }
        ));
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn first_ack_does_not_truncate_another_snapshotted_send() -> Result<()> {
        let store = FakeStore::with_bundles(1);
        let bundle = store.bundles.lock().unwrap()[0].clone();
        let registry = Arc::new(FakeRegistry::default());
        registry.add(
            bundle.request_id.clone(),
            Arc::new(AcknowledgeOnFirstSend {
                store: store.clone(),
                bundle_id: bundle.id,
                sent: AtomicBool::new(false),
            }),
        );
        let slow = Arc::new(RecordingSink {
            slow_first: AtomicBool::new(true),
            ..Default::default()
        });
        registry.add(bundle.request_id, slow.clone());
        let worker = worker(store.clone(), registry, ManualClock::new(0));
        let (result, ()) = tokio::join!(worker.run_once(), async {
            tokio::time::advance(Duration::from_secs(50)).await;
        });
        result?;
        assert_eq!(slow.events.lock().unwrap().len(), 3);
        assert!(store.retry_delays.lock().unwrap().is_empty());
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn missing_ack_is_renewed_then_retried_after_thirty_second_deadline() -> Result<()> {
        let store = FakeStore::with_bundles(1);
        store.auto_ack_on_load.store(false, Ordering::SeqCst);
        let registry = Arc::new(FakeRegistry::default());
        registry.add(
            store.bundles.lock().unwrap()[0].request_id.clone(),
            Arc::new(RecordingSink::default()),
        );
        let worker = worker(store.clone(), registry, ManualClock::new(0));
        let (result, ()) = tokio::join!(worker.run_once(), async {
            for _ in 0..3 {
                tokio::time::advance(Duration::from_secs(10)).await;
                tokio::task::yield_now().await;
            }
        });
        result?;
        assert!(store.renewals.load(Ordering::SeqCst) >= 2);
        assert_eq!(*store.retry_delays.lock().unwrap(), vec![1_000]);
        Ok(())
    }

    #[tokio::test]
    async fn renewal_authority_failure_propagates_instead_of_becoming_a_fence() {
        let store = FakeStore::with_bundles(1);
        store.renew_authority_error.store(true, Ordering::SeqCst);
        let registry = Arc::new(FakeRegistry::default());
        registry.add(
            store.bundles.lock().unwrap()[0].request_id.clone(),
            Arc::new(RecordingSink::default()),
        );

        let error = worker(store, registry, ManualClock::new(0))
            .run_once()
            .await
            .unwrap_err();
        assert!(error.to_string().contains("renewal authority failure"));
    }

    #[tokio::test]
    async fn retry_authority_failure_propagates_instead_of_becoming_a_fence() {
        let store = FakeStore::with_bundles(1);
        store.retry_authority_error.store(true, Ordering::SeqCst);
        let registry = Arc::new(FakeRegistry::default());
        registry.add(
            store.bundles.lock().unwrap()[0].request_id.clone(),
            Arc::new(RecordingSink {
                fail: AtomicBool::new(true),
                ..Default::default()
            }),
        );

        let error = worker(store, registry, ManualClock::new(0))
            .run_once()
            .await
            .unwrap_err();
        assert!(error.to_string().contains("retry authority failure"));
    }

    #[tokio::test]
    async fn ack_winning_retry_transition_keeps_worker_alive() -> Result<()> {
        let store = FakeStore::with_bundles(1);
        store.ack_before_retry.store(true, Ordering::SeqCst);
        let registry = Arc::new(FakeRegistry::default());
        registry.add(
            store.bundles.lock().unwrap()[0].request_id.clone(),
            Arc::new(RecordingSink {
                fail: AtomicBool::new(true),
                ..Default::default()
            }),
        );

        worker(store.clone(), registry, ManualClock::new(0))
            .run_once()
            .await?;
        assert_eq!(
            store.bundles.lock().unwrap()[0].state,
            BundleState::Delivered
        );
        assert!(store.retry_delays.lock().unwrap().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn ack_between_prepermit_renew_and_claimed_load_starts_no_send() -> Result<()> {
        let store = FakeStore::with_bundles(1);
        store.ack_before_claimed_load.store(true, Ordering::SeqCst);
        let registry = Arc::new(FakeRegistry::default());
        let sink = Arc::new(RecordingSink::default());
        registry.add(
            store.bundles.lock().unwrap()[0].request_id.clone(),
            sink.clone(),
        );

        worker(store.clone(), registry, ManualClock::new(0))
            .run_once()
            .await?;
        assert_eq!(
            store.bundles.lock().unwrap()[0].state,
            BundleState::Delivered
        );
        assert!(sink.events.lock().unwrap().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn fake_store_fences_wrong_owner_expiry_and_expired_time() -> Result<()> {
        let store = FakeStore::with_bundles(1);
        let request = store.bundles.lock().unwrap()[0].request_id.clone();
        let claim = store
            .claim_ready_bundle_refs("worker", &[request], Timestamp(1), Timestamp(30), 1)
            .await?
            .pop()
            .unwrap()
            .claim;
        let mut wrong_owner = claim.clone();
        wrong_owner.owner = "other".into();
        assert_eq!(
            store
                .renew_bundle(&wrong_owner, Timestamp(2), Timestamp(40))
                .await?,
            ClaimRenewal::Fenced
        );
        let mut wrong_expiry = claim.clone();
        wrong_expiry.expires_at = Timestamp(29);
        assert_eq!(
            store
                .renew_bundle(&wrong_expiry, Timestamp(2), Timestamp(40))
                .await?,
            ClaimRenewal::Fenced
        );
        assert_eq!(
            store
                .renew_bundle(&claim, Timestamp(30), Timestamp(60))
                .await?,
            ClaimRenewal::Fenced
        );
        Ok(())
    }
}
