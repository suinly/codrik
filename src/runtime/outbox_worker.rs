use std::{sync::Arc, time::Duration};

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use futures_util::future::join_all;
use tokio::sync::{Semaphore, watch};

use crate::runtime::{
    ipc::protocol::{ServerEvent, encode_bundle},
    model::{BundleState, Clock, RequestId},
    store::{
        AckOutcome, BundleAck, BundleStore, ClaimedBundle, ClaimedBundleLoad, ClaimedBundleRef,
    },
};

const CLAIM_MILLIS: i64 = 30_000;
const ACK_DEADLINE: Duration = Duration::from_secs(30);
const RENEW_INTERVAL: Duration = Duration::from_secs(10);
const POLL_INTERVAL: Duration = Duration::from_millis(500);
const CLAIM_BATCH: usize = 32;
const TRANSMISSION_CONCURRENCY: usize = 4;

enum AckWait {
    Delivered,
    Retry(crate::runtime::store::BundleClaim),
    Fenced,
}

#[async_trait]
pub trait BundleDeliverySink: Send + Sync {
    async fn send(&self, event: ServerEvent) -> Result<()>;

    async fn abort(&self, _error: &str) {}
}

pub trait DeliveryRegistry: Send + Sync {
    fn subscribed_request_ids(&self) -> Vec<RequestId>;
    fn snapshot(&self, request: &RequestId) -> Vec<Arc<dyn BundleDeliverySink>>;
    fn subscribe_changes(&self) -> watch::Receiver<u64>;
}

pub struct OutboxWorker<S, R, C> {
    store: Arc<S>,
    registry: Arc<R>,
    clock: C,
    owner: String,
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
        }
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
        let transmissions = Arc::new(Semaphore::new(TRANSMISSION_CONCURRENCY));
        let results = join_all(
            claimed
                .into_iter()
                .map(|claimed| self.await_transmission_slot(claimed, transmissions.clone())),
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
        request: &RequestId,
        sink: Arc<dyn BundleDeliverySink>,
    ) -> Result<bool> {
        let Some(bundle) = self.store.replay_bundle(request).await? else {
            return Ok(false);
        };
        for event in encode_bundle(&bundle, true).map_err(|error| anyhow!(error))? {
            sink.send(event).await?;
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
                        Ok(renewed) => claimed.claim = renewed,
                        Err(_) => {
                            match self.store.load_bundle(&claimed.claim.bundle_id).await {
                                Ok(_) => return Ok(()),
                                Err(error) => return Err(error),
                            }
                        }
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
            Ok(claim) => claim,
            Err(_) => {
                return match self.store.load_bundle(&claimed.claim.bundle_id).await {
                    Ok(bundle) if bundle.state == BundleState::Delivered => Ok(()),
                    Ok(_) => Ok(()),
                    Err(error) => Err(error),
                };
            }
        };
        let bundle = match self
            .store
            .load_claimed_bundle(&claimed.claim, self.clock.now())
            .await?
        {
            ClaimedBundleLoad::Loaded(bundle) => bundle,
            ClaimedBundleLoad::FailedTerminal => return Ok(()),
        };
        let result = self
            .transmit(ClaimedBundle {
                claim: claimed.claim,
                bundle,
                attempt_count: claimed.attempt_count,
            })
            .await;
        drop(permit);
        result
    }

    async fn transmit(&self, claimed: ClaimedBundle) -> Result<()> {
        let frames = match encode_bundle(&claimed.bundle, false) {
            Ok(frames) => frames,
            Err(error) => {
                self.store
                    .fail_bundle_terminal(&claimed.claim, &error.to_string(), self.clock.now())
                    .await?;
                return Ok(());
            }
        };
        let recipients = self.registry.snapshot(&claimed.bundle.request_id);
        if recipients.is_empty() {
            self.retry(claimed, "all bundle subscribers disconnected")
                .await?;
            return Ok(());
        }

        let sends = recipients.iter().cloned().map(|sink| {
            let frames = frames.clone();
            async move {
                for frame in frames {
                    sink.send(frame).await?;
                }
                Result::<()>::Ok(())
            }
        });
        let mut sends = Box::pin(join_all(sends));
        let mut claim = claimed.claim.clone();
        let mut renewal = tokio::time::interval(RENEW_INTERVAL);
        renewal.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        renewal.tick().await;

        loop {
            let results = tokio::select! {
                results = &mut sends => results,
                _ = renewal.tick() => {
                    let now = self.clock.now();
                    match self.store.renew_bundle(&claim, now, now.plus_millis(CLAIM_MILLIS)).await {
                        Ok(renewed) => claim = renewed,
                        Err(_) => {
                            match self.store.load_bundle(&claim.bundle_id).await {
                                Ok(bundle) if bundle.state == BundleState::Delivered => {}
                                Ok(_) => {
                                    abort_recipients(&recipients, "bundle delivery claim was fenced").await;
                                    return Ok(());
                                }
                                Err(error) => return Err(error),
                            }
                        }
                    }
                    continue;
                }
            };
            if results.iter().all(Result::is_err) {
                self.retry(
                    ClaimedBundle { claim, ..claimed },
                    "every bundle subscriber failed",
                )
                .await?;
            } else {
                match self
                    .wait_for_ack(claim, &claimed.bundle.request_id, &recipients)
                    .await?
                {
                    AckWait::Delivered => {}
                    AckWait::Retry(active_claim) => {
                        abort_recipients(&recipients, "bundle acknowledgement deadline elapsed")
                            .await;
                        self.retry(
                            ClaimedBundle {
                                claim: active_claim,
                                ..claimed
                            },
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
                    match self.store.load_bundle(&claim.bundle_id).await {
                        Ok(bundle) if bundle.state == BundleState::Delivered => return Ok(AckWait::Delivered),
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
                        Ok(renewed) => claim = renewed,
                        Err(_) => {
                            return match self.store.load_bundle(&claim.bundle_id).await {
                                Ok(bundle) if bundle.state == BundleState::Delivered => Ok(AckWait::Delivered),
                                Ok(_) => Ok(AckWait::Fenced),
                                Err(error) => Err(error),
                            };
                        }
                    }
                }
            }
        }
    }

    async fn retry(&self, claimed: ClaimedBundle, error: &str) -> Result<()> {
        let now = self.clock.now();
        let delay = retry_delay_seconds(claimed.attempt_count);
        self.store
            .fail_bundle_retryable(&claimed.claim, error, now.plus_millis(delay * 1_000), now)
            .await
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
    use tokio::sync::watch;

    use crate::runtime::{
        ipc::protocol::{ServerEvent, ServerEventBody},
        model::{BundleId, BundleState, DeliveryId, ManualClock, RequestId, Timestamp},
        store::{
            AckOutcome, BundleAck, BundleClaim, BundleManifest, BundleStore, ClaimedBundle,
            ClaimedBundleLoad, ClaimedBundleRef, FinalPayload, ResultBundle,
        },
    };

    use super::{BundleDeliverySink, DeliveryRegistry, OutboxWorker, retry_delay_seconds};

    #[derive(Default)]
    struct FakeStore {
        bundles: Mutex<Vec<ResultBundle>>,
        claim_calls: AtomicUsize,
        renewals: AtomicUsize,
        renewals_by_bundle: Mutex<HashMap<BundleId, usize>>,
        retry_delays: Mutex<Vec<i64>>,
        terminal_failures: AtomicUsize,
        replay_calls: AtomicUsize,
        acks: Mutex<Vec<BundleAck>>,
        auto_ack_on_load: AtomicBool,
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
                }
            }
            Ok(claims)
        }

        async fn renew_bundle(
            &self,
            claim: &BundleClaim,
            _now: Timestamp,
            until: Timestamp,
        ) -> Result<BundleClaim> {
            self.renewals.fetch_add(1, Ordering::SeqCst);
            *self
                .renewals_by_bundle
                .lock()
                .unwrap()
                .entry(claim.bundle_id.clone())
                .or_default() += 1;
            if self.bundles.lock().unwrap().iter().any(|bundle| {
                bundle.id == claim.bundle_id && bundle.state == BundleState::Delivered
            }) {
                bail!("bundle already delivered");
            }
            Ok(BundleClaim {
                expires_at: until,
                ..claim.clone()
            })
        }

        async fn load_claimed_bundle(
            &self,
            claim: &BundleClaim,
            _now: Timestamp,
        ) -> Result<ClaimedBundleLoad> {
            Ok(ClaimedBundleLoad::Loaded(
                self.load_bundle(&claim.bundle_id).await?,
            ))
        }

        async fn load_bundle(&self, id: &BundleId) -> Result<ResultBundle> {
            let mut bundles = self.bundles.lock().unwrap();
            let bundle = bundles
                .iter_mut()
                .find(|bundle| &bundle.id == id)
                .ok_or_else(|| anyhow::anyhow!("missing bundle"))?;
            if self.auto_ack_on_load.load(Ordering::SeqCst)
                && bundle.state == BundleState::Delivering
            {
                bundle.state = BundleState::Delivered;
            }
            Ok(bundle.clone())
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
        ) -> Result<()> {
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
            Ok(())
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

        async fn replay_bundle(&self, request: &RequestId) -> Result<Option<ResultBundle>> {
            self.replay_calls.fetch_add(1, Ordering::SeqCst);
            Ok(self
                .bundles
                .lock()
                .unwrap()
                .iter()
                .find(|bundle| {
                    &bundle.request_id == request && bundle.state == BundleState::Delivered
                })
                .cloned())
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
                .replay(&request, sink.clone())
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
    async fn acknowledge_routes_the_exact_bundle_ack_to_persistence() -> Result<()> {
        let store = FakeStore::with_bundles(1);
        let bundle = store.bundles.lock().unwrap()[0].clone();
        let ack = BundleAck {
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
        assert!(worker.replay(&bundle.request_id, late.clone()).await?);
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
}
