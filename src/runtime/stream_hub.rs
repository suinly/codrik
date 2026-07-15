use std::{
    collections::{HashMap, VecDeque},
    convert::Infallible,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
};

use crate::{
    llm::client::AgentActivityEvent,
    runtime::{
        ipc::protocol::{ActivityEvent, ServerEvent, ServerEventBody},
        model::RequestId,
    },
};

const DEFAULT_EVENTS_PER_SUBSCRIPTION: usize = 256;
const DEFAULT_BYTES_PER_SUBSCRIPTION: usize = 512 * 1024;
const DEFAULT_GLOBAL_BYTES: usize = 32 * 1024 * 1024;

pub trait RuntimeEventPublisher: Send + Sync {
    fn publish_text(&self, requests: &[RequestId], delta: &str);
    fn publish_activity(&self, requests: &[RequestId], event: AgentActivityEvent);
}

pub struct NoopRuntimeEventPublisher;

impl RuntimeEventPublisher for NoopRuntimeEventPublisher {
    fn publish_text(&self, _requests: &[RequestId], _delta: &str) {}

    fn publish_activity(&self, _requests: &[RequestId], _event: AgentActivityEvent) {}
}

#[derive(Clone)]
pub struct StreamHub {
    inner: Arc<HubInner>,
}

struct HubInner {
    subscriptions: Mutex<HashMap<RequestId, Vec<(u64, Weak<SubscriptionState>)>>>,
    next_subscription_id: AtomicU64,
    queued_bytes: AtomicUsize,
    event_limit: usize,
    byte_limit: usize,
    global_byte_limit: usize,
}

struct SubscriptionState {
    queue: Mutex<SubscriptionQueue>,
    notify: tokio::sync::Notify,
    hub: Weak<HubInner>,
}

struct SubscriptionQueue {
    events: VecDeque<QueuedEvent>,
    queued_bytes: usize,
    gap_emitted: bool,
    suppress_text: bool,
}

struct QueuedEvent {
    event: ServerEvent,
    bytes: usize,
}

pub struct StreamSubscription {
    request_id: RequestId,
    id: u64,
    state: Arc<SubscriptionState>,
}

impl Default for StreamHub {
    fn default() -> Self {
        Self::with_limits(
            DEFAULT_EVENTS_PER_SUBSCRIPTION,
            DEFAULT_BYTES_PER_SUBSCRIPTION,
            DEFAULT_GLOBAL_BYTES,
        )
    }
}

impl StreamHub {
    pub fn with_limits(event_limit: usize, byte_limit: usize, global_byte_limit: usize) -> Self {
        assert!(
            event_limit >= 2,
            "event limit must reserve a stream-gap slot"
        );
        assert!(byte_limit > 0, "byte limit must be greater than zero");
        assert!(
            global_byte_limit > 0,
            "global byte limit must be greater than zero"
        );
        Self {
            inner: Arc::new(HubInner {
                subscriptions: Mutex::new(HashMap::new()),
                next_subscription_id: AtomicU64::new(1),
                queued_bytes: AtomicUsize::new(0),
                event_limit,
                byte_limit,
                global_byte_limit,
            }),
        }
    }

    pub fn subscribe(&self, request_id: RequestId) -> Result<StreamSubscription, Infallible> {
        let id = self
            .inner
            .next_subscription_id
            .fetch_add(1, Ordering::Relaxed);
        let state = Arc::new(SubscriptionState {
            queue: Mutex::new(SubscriptionQueue {
                events: VecDeque::new(),
                queued_bytes: 0,
                gap_emitted: false,
                suppress_text: false,
            }),
            notify: tokio::sync::Notify::new(),
            hub: Arc::downgrade(&self.inner),
        });
        self.inner
            .subscriptions
            .lock()
            .expect("stream hub subscriptions poisoned")
            .entry(request_id.clone())
            .or_default()
            .push((id, Arc::downgrade(&state)));
        Ok(StreamSubscription {
            request_id,
            id,
            state,
        })
    }

    fn subscribers(&self, request: &RequestId) -> Vec<Arc<SubscriptionState>> {
        let mut subscriptions = self
            .inner
            .subscriptions
            .lock()
            .expect("stream hub subscriptions poisoned");
        let Some(entries) = subscriptions.get_mut(request) else {
            return Vec::new();
        };
        let mut live = Vec::with_capacity(entries.len());
        entries.retain(|(_, state)| {
            if let Some(state) = state.upgrade() {
                live.push(state);
                true
            } else {
                false
            }
        });
        if entries.is_empty() {
            subscriptions.remove(request);
        }
        live
    }

    fn publish(&self, requests: &[RequestId], body: PublishedBody<'_>) {
        for request in requests {
            for subscriber in self.subscribers(request) {
                subscriber.enqueue(&self.inner, request, &body);
            }
        }
    }
}

impl RuntimeEventPublisher for StreamHub {
    fn publish_text(&self, requests: &[RequestId], delta: &str) {
        if !delta.is_empty() {
            self.publish(requests, PublishedBody::Text(delta));
        }
    }

    fn publish_activity(&self, requests: &[RequestId], event: AgentActivityEvent) {
        self.publish(requests, PublishedBody::Activity(&event));
    }
}

enum PublishedBody<'a> {
    Text(&'a str),
    Activity(&'a AgentActivityEvent),
}

impl PublishedBody<'_> {
    fn is_text(&self) -> bool {
        matches!(self, Self::Text(_))
    }

    fn bytes(&self) -> usize {
        match self {
            Self::Text(delta) => delta.len(),
            Self::Activity(AgentActivityEvent::Description(description)) => description.len(),
            Self::Activity(AgentActivityEvent::ToolStarted { name })
            | Self::Activity(AgentActivityEvent::ToolFinished { name, .. }) => name.len(),
            Self::Activity(_) => 0,
        }
    }

    fn event(&self, request_id: RequestId) -> ServerEvent {
        let body = match self {
            Self::Text(delta) => ServerEventBody::TextDelta {
                request_id,
                delta: (*delta).to_owned(),
            },
            Self::Activity(event) => ServerEventBody::Activity {
                request_id,
                event: activity_event((*event).clone()),
            },
        };
        ServerEvent::new(body)
    }
}

impl SubscriptionState {
    fn enqueue(&self, hub: &HubInner, request: &RequestId, body: &PublishedBody<'_>) {
        let mut queue = self.queue.lock().expect("stream subscription poisoned");
        if body.is_text() && queue.suppress_text {
            return;
        }
        let bytes = body.bytes();
        let normal_capacity = hub.event_limit - 1;
        let has_capacity = queue.events.len() < normal_capacity
            && queue.queued_bytes.saturating_add(bytes) <= hub.byte_limit
            && reserve_global_bytes(hub, bytes);
        if has_capacity {
            queue.queued_bytes += bytes;
            queue.events.push_back(QueuedEvent {
                event: body.event(request.clone()),
                bytes,
            });
            drop(queue);
            self.notify.notify_one();
            return;
        }
        if !queue.gap_emitted {
            queue.gap_emitted = true;
            queue.suppress_text = true;
            queue.events.push_back(QueuedEvent {
                event: ServerEvent::new(ServerEventBody::StreamGap {
                    request_id: request.clone(),
                }),
                bytes: 0,
            });
            drop(queue);
            self.notify.notify_one();
        }
    }
}

fn reserve_global_bytes(hub: &HubInner, bytes: usize) -> bool {
    hub.queued_bytes
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |queued| {
            queued
                .checked_add(bytes)
                .filter(|next| *next <= hub.global_byte_limit)
        })
        .is_ok()
}

impl StreamSubscription {
    pub async fn recv(&mut self) -> Option<ServerEvent> {
        loop {
            let state = Arc::clone(&self.state);
            let notified = state.notify.notified();
            if let Some(event) = self.try_recv() {
                return Some(event);
            }
            notified.await;
        }
    }

    pub fn try_recv(&mut self) -> Option<ServerEvent> {
        let mut queue = self
            .state
            .queue
            .lock()
            .expect("stream subscription poisoned");
        let queued = queue.events.pop_front()?;
        queue.queued_bytes -= queued.bytes;
        if let Some(hub) = self.state.hub.upgrade() {
            hub.queued_bytes.fetch_sub(queued.bytes, Ordering::AcqRel);
        }
        Some(queued.event)
    }
}

impl Drop for StreamSubscription {
    fn drop(&mut self) {
        if let Some(hub) = self.state.hub.upgrade()
            && let Ok(mut subscriptions) = hub.subscriptions.lock()
            && let Some(entries) = subscriptions.get_mut(&self.request_id)
        {
            entries.retain(|(id, _)| *id != self.id);
            if entries.is_empty() {
                subscriptions.remove(&self.request_id);
            }
        }
    }
}

impl Drop for SubscriptionState {
    fn drop(&mut self) {
        let queue = self.queue.lock().expect("stream subscription poisoned");
        if let Some(hub) = self.hub.upgrade() {
            hub.queued_bytes
                .fetch_sub(queue.queued_bytes, Ordering::AcqRel);
        }
    }
}

fn activity_event(event: AgentActivityEvent) -> ActivityEvent {
    match event {
        AgentActivityEvent::ModelStepStarted => ActivityEvent::ModelStepStarted,
        AgentActivityEvent::Description(description) => ActivityEvent::Description { description },
        AgentActivityEvent::ToolStarted { name } => ActivityEvent::ToolStarted { name },
        AgentActivityEvent::ToolFinished { name, succeeded } => {
            ActivityEvent::ToolFinished { name, succeeded }
        }
        AgentActivityEvent::Completed => ActivityEvent::Completed,
        AgentActivityEvent::Cancelled => ActivityEvent::Cancelled,
        AgentActivityEvent::Failed => ActivityEvent::Failed,
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        llm::client::AgentActivityEvent,
        runtime::{
            ipc::protocol::ServerEventBody,
            model::RequestId,
            stream_hub::{RuntimeEventPublisher, StreamHub},
        },
    };

    fn request() -> RequestId {
        RequestId::new()
    }

    #[tokio::test]
    async fn publishes_to_every_subscription_without_replacing_an_existing_one() {
        let hub = StreamHub::with_limits(4, 64, 128);
        let request = request();
        let mut first = hub.subscribe(request.clone()).unwrap();
        let mut second = hub.subscribe(request.clone()).unwrap();

        hub.publish_text(std::slice::from_ref(&request), "hello");

        assert!(matches!(
            first.recv().await.unwrap().body,
            ServerEventBody::TextDelta { delta, .. } if delta == "hello"
        ));
        assert!(matches!(
            second.recv().await.unwrap().body,
            ServerEventBody::TextDelta { delta, .. } if delta == "hello"
        ));
    }

    #[tokio::test]
    async fn overflow_emits_one_reserved_gap_suppresses_text_and_keeps_activity() {
        let hub = StreamHub::with_limits(4, 16, 64);
        let request = request();
        let mut subscription = hub.subscribe(request.clone()).unwrap();

        hub.publish_text(std::slice::from_ref(&request), "0123456789");
        hub.publish_text(std::slice::from_ref(&request), "overflow");
        hub.publish_text(std::slice::from_ref(&request), "ignored");

        assert!(matches!(
            subscription.recv().await.unwrap().body,
            ServerEventBody::TextDelta { .. }
        ));
        assert!(matches!(
            subscription.recv().await.unwrap().body,
            ServerEventBody::StreamGap { .. }
        ));

        hub.publish_activity(
            std::slice::from_ref(&request),
            AgentActivityEvent::Completed,
        );
        assert!(matches!(
            subscription.recv().await.unwrap().body,
            ServerEventBody::Activity { .. }
        ));
        assert!(subscription.try_recv().is_none());
    }

    #[tokio::test]
    async fn byte_and_global_limits_isolate_slow_or_disconnected_subscriptions() {
        let hub = StreamHub::with_limits(8, 5, 5);
        let slow_request = request();
        let fast_request = request();
        let slow = hub.subscribe(slow_request.clone()).unwrap();
        let mut fast = hub.subscribe(fast_request.clone()).unwrap();

        hub.publish_text(std::slice::from_ref(&slow_request), "12345");
        hub.publish_text(std::slice::from_ref(&fast_request), "x");
        assert!(matches!(
            fast.recv().await.unwrap().body,
            ServerEventBody::StreamGap { .. }
        ));

        drop(slow);
        let later_request = request();
        let mut later = hub.subscribe(later_request.clone()).unwrap();
        hub.publish_text(std::slice::from_ref(&later_request), "12345");
        assert!(matches!(
            later.recv().await.unwrap().body,
            ServerEventBody::TextDelta { .. }
        ));
    }

    #[tokio::test]
    async fn event_limit_keeps_one_slot_reserved_for_the_gap() {
        let hub = StreamHub::with_limits(2, 64, 64);
        let request = request();
        let mut subscription = hub.subscribe(request.clone()).unwrap();

        hub.publish_text(std::slice::from_ref(&request), "first");
        hub.publish_activity(
            std::slice::from_ref(&request),
            AgentActivityEvent::Completed,
        );

        assert!(matches!(
            subscription.recv().await.unwrap().body,
            ServerEventBody::TextDelta { .. }
        ));
        assert!(matches!(
            subscription.recv().await.unwrap().body,
            ServerEventBody::StreamGap { .. }
        ));
    }
}
