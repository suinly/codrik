use std::{
    collections::{BTreeSet, HashMap},
    io,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use anyhow::{Result, anyhow, bail};
use async_trait::async_trait;
use sha2::{Digest, Sha256};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf},
    net::{UnixListener, UnixStream},
    sync::{Semaphore, watch},
};

use crate::runtime::{
    hooks::{NoopRuntimeBoundaryHooks, RuntimeBoundaryHooks},
    identity_link::IdentityLinkManager,
    ipc::{
        protocol::{
            ClientRequestBody, FrameReader, FrameWriter, ProtocolFailure, ServerEvent,
            ServerEventBody,
        },
        security::{AuthorizedUnixStream, OsPeerCredentials, PeerCredentials},
    },
    model::{ActorId, BundleId, BundleState, Clock, LocalRequestState, RequestId, SystemClock},
    outbox_worker::{BundleDeliverySink, OutboxWorker},
    signals::ActorSignals,
    store::{
        AckOutcome, AckRejected, BundleAck, BundleStore, CancelOutcome, LocalCancel,
        LocalIngressStore, LocalSubmission, LocalSubmitOutcome,
    },
    stream_hub::StreamHub,
};

pub const MAX_CONNECTIONS: usize = 64;

#[derive(Clone, Default)]
struct DecodeRegistry {
    inner: Arc<DecodeRegistryInner>,
}

#[derive(Default)]
struct DecodeRegistryInner {
    next_ticket: AtomicU64,
    pending: Mutex<BTreeSet<u64>>,
    changed: tokio::sync::Notify,
}

struct DecodeGuard {
    ticket: u64,
    registry: DecodeRegistry,
}

impl DecodeRegistry {
    fn register(&self) -> DecodeGuard {
        let ticket = self.inner.next_ticket.fetch_add(1, Ordering::Relaxed);
        self.inner
            .pending
            .lock()
            .expect("decode registry poisoned")
            .insert(ticket);
        DecodeGuard {
            ticket,
            registry: self.clone(),
        }
    }

    async fn wait_for_prior(&self, ticket: u64) {
        loop {
            let changed = self.inner.changed.notified();
            let prior_pending = self
                .inner
                .pending
                .lock()
                .expect("decode registry poisoned")
                .range(..ticket)
                .next()
                .is_some();
            if !prior_pending {
                return;
            }
            changed.await;
        }
    }
}

impl Drop for DecodeGuard {
    fn drop(&mut self) {
        self.registry
            .inner
            .pending
            .lock()
            .expect("decode registry poisoned")
            .remove(&self.ticket);
        self.registry.inner.changed.notify_waiters();
    }
}

#[derive(Clone, Default)]
pub struct SubmissionRegistry {
    inner: Arc<Mutex<HashMap<RequestId, Arc<SubmissionEntry>>>>,
}

struct SubmissionEntry {
    complete: watch::Sender<bool>,
}

pub struct SubmissionGuard {
    request: RequestId,
    entry: Arc<SubmissionEntry>,
    registry: SubmissionRegistry,
}

impl SubmissionRegistry {
    pub fn register(&self, request: RequestId) -> Result<SubmissionGuard> {
        let entry = Arc::new(SubmissionEntry {
            complete: watch::channel(false).0,
        });
        let mut submissions = self.inner.lock().expect("submission registry poisoned");
        if submissions.contains_key(&request) {
            bail!("submission is already in flight for request {request:?}");
        }
        submissions.insert(request.clone(), entry.clone());
        Ok(SubmissionGuard {
            request,
            entry,
            registry: self.clone(),
        })
    }

    pub async fn wait_for(&self, request: &RequestId) -> Result<()> {
        let mut complete = self
            .inner
            .lock()
            .expect("submission registry poisoned")
            .get(request)
            .map(|entry| entry.complete.subscribe());
        if let Some(receiver) = complete.as_mut() {
            while !*receiver.borrow() {
                if receiver.changed().await.is_err() {
                    break;
                }
            }
        }
        Ok(())
    }

    pub fn is_inflight(&self, request: &RequestId) -> bool {
        self.inner
            .lock()
            .expect("submission registry poisoned")
            .contains_key(request)
    }
}

impl SubmissionGuard {
    pub fn complete(self) {}
}

impl Drop for SubmissionGuard {
    fn drop(&mut self) {
        self.entry.complete.send_replace(true);
        let mut submissions = self
            .registry
            .inner
            .lock()
            .expect("submission registry poisoned");
        if submissions
            .get(&self.request)
            .is_some_and(|entry| Arc::ptr_eq(entry, &self.entry))
        {
            submissions.remove(&self.request);
        }
    }
}

#[async_trait]
pub trait IpcOutbox: Send + Sync {
    async fn acknowledge(&self, ack: BundleAck) -> Result<AckOutcome>;
    async fn replay(
        &self,
        actor: &ActorId,
        request: &RequestId,
        sink: Arc<dyn BundleDeliverySink>,
    ) -> Result<bool>;
}

#[async_trait]
impl<S, R, C> IpcOutbox for OutboxWorker<S, R, C>
where
    S: BundleStore + 'static,
    R: crate::runtime::outbox_worker::DeliveryRegistry + 'static,
    C: Clock,
{
    async fn acknowledge(&self, ack: BundleAck) -> Result<AckOutcome> {
        self.acknowledge(ack).await
    }

    async fn replay(
        &self,
        actor: &ActorId,
        request: &RequestId,
        sink: Arc<dyn BundleDeliverySink>,
    ) -> Result<bool> {
        self.replay(actor, request, sink).await
    }
}

pub struct LocalIpcServer {
    listener: UnixListener,
    actor: ActorId,
    ingress: Arc<dyn LocalIngressStore>,
    outbox: Arc<dyn IpcOutbox>,
    hub: Arc<StreamHub>,
    credentials: Arc<dyn PeerCredentials>,
    submissions: SubmissionRegistry,
    connections: Arc<Semaphore>,
    active: ActiveConnections,
    decodes: DecodeRegistry,
    hooks: Arc<dyn RuntimeBoundaryHooks>,
    signals: ActorSignals,
    linking: Option<Arc<dyn IdentityLinkManager>>,
}

impl LocalIpcServer {
    pub fn bind(
        socket_path: &std::path::Path,
        actor: ActorId,
        ingress: Arc<dyn LocalIngressStore>,
        outbox: Arc<dyn IpcOutbox>,
        hub: Arc<StreamHub>,
    ) -> Result<Self> {
        Self::bind_with_hooks(
            socket_path,
            actor,
            ingress,
            outbox,
            hub,
            Arc::new(NoopRuntimeBoundaryHooks),
        )
    }

    pub fn bind_with_hooks(
        socket_path: &std::path::Path,
        actor: ActorId,
        ingress: Arc<dyn LocalIngressStore>,
        outbox: Arc<dyn IpcOutbox>,
        hub: Arc<StreamHub>,
        hooks: Arc<dyn RuntimeBoundaryHooks>,
    ) -> Result<Self> {
        let listener = crate::runtime::ipc::security::bind_secure_listener(socket_path)?;
        Ok(Self::with_credentials_and_hooks(
            listener,
            actor,
            ingress,
            outbox,
            hub,
            Arc::new(OsPeerCredentials),
            hooks,
        ))
    }

    pub fn new(
        listener: UnixListener,
        actor: ActorId,
        ingress: Arc<dyn LocalIngressStore>,
        outbox: Arc<dyn IpcOutbox>,
        hub: Arc<StreamHub>,
    ) -> Self {
        Self::with_credentials(
            listener,
            actor,
            ingress,
            outbox,
            hub,
            Arc::new(OsPeerCredentials),
        )
    }

    pub fn with_credentials(
        listener: UnixListener,
        actor: ActorId,
        ingress: Arc<dyn LocalIngressStore>,
        outbox: Arc<dyn IpcOutbox>,
        hub: Arc<StreamHub>,
        credentials: Arc<dyn PeerCredentials>,
    ) -> Self {
        Self::with_credentials_and_hooks(
            listener,
            actor,
            ingress,
            outbox,
            hub,
            credentials,
            Arc::new(NoopRuntimeBoundaryHooks),
        )
    }

    pub fn with_credentials_and_hooks(
        listener: UnixListener,
        actor: ActorId,
        ingress: Arc<dyn LocalIngressStore>,
        outbox: Arc<dyn IpcOutbox>,
        hub: Arc<StreamHub>,
        credentials: Arc<dyn PeerCredentials>,
        hooks: Arc<dyn RuntimeBoundaryHooks>,
    ) -> Self {
        Self {
            listener,
            actor,
            ingress,
            outbox,
            hub,
            credentials,
            submissions: SubmissionRegistry::default(),
            connections: Arc::new(Semaphore::new(MAX_CONNECTIONS)),
            active: ActiveConnections::default(),
            decodes: DecodeRegistry::default(),
            hooks,
            signals: ActorSignals::default(),
            linking: None,
        }
    }

    pub fn with_actor_signals(mut self, signals: ActorSignals) -> Self {
        self.signals = signals;
        self
    }

    pub fn with_identity_linking(mut self, linking: Arc<dyn IdentityLinkManager>) -> Self {
        self.linking = Some(linking);
        self
    }

    pub fn submissions(&self) -> SubmissionRegistry {
        self.submissions.clone()
    }

    pub async fn run(self, mut shutdown: watch::Receiver<bool>) -> Result<()> {
        let mut handlers = tokio::task::JoinSet::new();
        'accept: loop {
            while let Some(completed) = handlers.try_join_next() {
                inspect_handler_result(completed)?;
            }
            if *shutdown.borrow() {
                shutdown_handlers(&self.active, &mut handlers).await?;
                return Ok(());
            }
            let permit = loop {
                tokio::select! {
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() {
                            shutdown_handlers(&self.active, &mut handlers).await?;
                            return Ok(());
                        }
                    }
                    completed = handlers.join_next(), if !handlers.is_empty() => {
                        inspect_handler_result(completed.expect("guarded nonempty JoinSet"))?;
                    }
                    permit = self.connections.clone().acquire_owned() => {
                        break permit.map_err(|_| anyhow!("IPC connection limiter closed"))?;
                    }
                }
            };
            let (stream, _) = tokio::select! {
                changed = shutdown.changed() => {
                    drop(permit);
                    if changed.is_err() || *shutdown.borrow() {
                        shutdown_handlers(&self.active, &mut handlers).await?;
                        return Ok(());
                    }
                    continue 'accept;
                }
                accepted = self.listener.accept() => accepted?,
                completed = handlers.join_next(), if !handlers.is_empty() => {
                    drop(permit);
                    inspect_handler_result(completed.expect("guarded nonempty JoinSet"))?;
                    continue 'accept;
                }
            };
            let handler = ConnectionHandler {
                actor: self.actor.clone(),
                ingress: self.ingress.clone(),
                outbox: self.outbox.clone(),
                hub: self.hub.clone(),
                credentials: self.credentials.clone(),
                submissions: self.submissions.clone(),
                active: self.active.clone(),
                decodes: self.decodes.clone(),
                hooks: self.hooks.clone(),
                signals: self.signals.clone(),
                linking: self.linking.clone(),
            };
            let decode = self.decodes.register();
            handlers.spawn(async move {
                let _permit = permit;
                handler.handle_with_decode(stream, Some(decode)).await
            });
        }
    }
}

fn inspect_handler_result(
    completed: std::result::Result<Result<()>, tokio::task::JoinError>,
) -> Result<()> {
    match completed {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) if connection_scoped(&error) => Ok(()),
        Ok(Err(error)) => Err(error),
        Err(error) => Err(anyhow!("IPC connection handler task failed: {error}")),
    }
}

fn connection_scoped(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause.is::<io::Error>() || cause.is::<ProtocolFailure>() || cause.is::<AckRejected>()
    })
}

async fn shutdown_handlers(
    active: &ActiveConnections,
    handlers: &mut tokio::task::JoinSet<Result<()>>,
) -> Result<()> {
    active.begin_shutdown().await;
    while let Some(completed) = handlers.join_next().await {
        inspect_handler_result(completed)?;
    }
    Ok(())
}

struct ConnectionHandler {
    actor: ActorId,
    ingress: Arc<dyn LocalIngressStore>,
    outbox: Arc<dyn IpcOutbox>,
    hub: Arc<StreamHub>,
    credentials: Arc<dyn PeerCredentials>,
    submissions: SubmissionRegistry,
    active: ActiveConnections,
    decodes: DecodeRegistry,
    hooks: Arc<dyn RuntimeBoundaryHooks>,
    signals: ActorSignals,
    linking: Option<Arc<dyn IdentityLinkManager>>,
}

impl ConnectionHandler {
    #[cfg(test)]
    async fn handle(&self, stream: UnixStream) -> Result<()> {
        self.handle_with_decode(stream, None).await
    }

    async fn handle_with_decode(
        &self,
        stream: UnixStream,
        decode: Option<DecodeGuard>,
    ) -> Result<()> {
        // Authentication deliberately precedes even the first frame read.
        let stream =
            AuthorizedUnixStream::authorize(stream, self.credentials.as_ref())?.into_inner();
        let (mut read, write) = tokio::io::split(stream);
        let sink = Arc::new(SocketDeliverySink::new(write));
        let _active = self.active.register(&sink);
        if self.active.is_shutting_down() {
            self.active.notify_sink(&sink).await;
            sink.abort("server shutting down").await;
            return Ok(());
        }
        let request = {
            let mut reader = FrameReader::new(&mut read);
            match reader.read_client_request().await {
                Ok(request) => request,
                Err(error) => {
                    let _ = sink.send_control(protocol_error(&error)).await;
                    sink.abort("invalid request frame").await;
                    return Ok(());
                }
            }
        };
        sink.set_request(request_id(&request.body));
        if self.active.is_shutting_down() {
            self.active.notify_sink(&sink).await;
            sink.abort("server shutting down").await;
            return Ok(());
        }
        let decode_ticket = decode.as_ref().map(|guard| guard.ticket);
        match request.body {
            ClientRequestBody::Submit { request_id, text } => {
                let guard = match self.submissions.register(request_id.clone()) {
                    Ok(guard) => Some(guard),
                    Err(_) => {
                        self.submissions.wait_for(&request_id).await?;
                        None
                    }
                };
                drop(decode);
                self.submit(request_id, text, sink, read, guard).await
            }
            ClientRequestBody::Resume { request_id } => {
                drop(decode);
                self.resume(request_id, sink, read, decode_ticket).await
            }
            ClientRequestBody::Cancel {
                request_id,
                cancel_id,
            } => {
                drop(decode);
                let outcome = self
                    .ingress
                    .cancel_for_actor(
                        &self.actor,
                        LocalCancel {
                            cancel_id,
                            request_id: request_id.clone(),
                        },
                        SystemClock.now(),
                    )
                    .await?;
                self.signals.notify(&self.actor, 0).await;
                sink.send_control(cancel_accepted(request_id, outcome))
                    .await?;
                sink.close().await
            }
            ClientRequestBody::AckFinal {
                request_id,
                bundle_id,
                delivery_ids,
            } => {
                drop(decode);
                let result = self
                    .outbox
                    .acknowledge(BundleAck {
                        actor_id: self.actor.clone(),
                        request_id: request_id.clone(),
                        bundle_id: bundle_id.clone(),
                        delivery_ids,
                    })
                    .await;
                match result {
                    Ok(_) => {
                        sink.send_control(ServerEvent::new(ServerEventBody::AckAccepted {
                            request_id,
                            bundle_id,
                        }))
                        .await?;
                    }
                    Err(error) if error.downcast_ref::<AckRejected>().is_some() => {
                        sink.send_control(request_error(
                            request_id,
                            "ack_failed",
                            &format!("durable final acknowledgement failed: {error}"),
                        ))
                        .await?;
                    }
                    Err(error) => return Err(error),
                }
                sink.close().await
            }
            ClientRequestBody::IssueLinkCode { request_id } => {
                drop(decode);
                let Some(linking) = self.linking.as_ref() else {
                    sink.send_control(request_error(
                        request_id,
                        "linking_unavailable",
                        "identity linking is not configured",
                    ))
                    .await?;
                    return sink.close().await;
                };
                match linking.issue_code(&self.actor).await {
                    Ok(issued) => {
                        sink.send_control(ServerEvent::new(ServerEventBody::LinkCodeIssued {
                            request_id,
                            code: issued.code,
                            expires_at: issued.expires_at.0,
                        }))
                        .await?;
                    }
                    Err(error) => {
                        sink.send_control(request_error(
                            request_id,
                            "link_code_failed",
                            &format!("failed to issue identity link code: {error}"),
                        ))
                        .await?;
                    }
                }
                sink.close().await
            }
        }
    }

    async fn submit(
        &self,
        request: RequestId,
        text: String,
        sink: Arc<SocketDeliverySink>,
        mut read: ReadHalf<UnixStream>,
        guard: Option<SubmissionGuard>,
    ) -> Result<()> {
        let mut subscription = self
            .hub
            .subscribe_with_delivery_sink(request.clone(), sink.clone())?;
        let prompt_sha256 = format!("{:x}", Sha256::digest(text.as_bytes()));
        let outcome = self
            .ingress
            .submit_for_actor(
                &self.actor,
                LocalSubmission {
                    request_id: request.clone(),
                    text,
                    prompt_sha256,
                },
                SystemClock.now(),
            )
            .await;
        if outcome.is_ok() {
            self.hooks.ingress_committed(&request).await;
        }
        drop(guard); // publish completion after commit or rollback, before any durable lookup can proceed.
        match outcome? {
            LocalSubmitOutcome::Accepted {
                work_item_id,
                sequence,
                ..
            } => {
                self.signals.notify(&self.actor, sequence).await;
                sink.send_control(ServerEvent::new(ServerEventBody::Accepted {
                    request_id: request.clone(),
                    work_item_id,
                    sequence,
                }))
                .await?;
                sink.open_delivery();
            }
            LocalSubmitOutcome::Duplicate {
                work_item_id: Some(work_item_id),
                sequence,
                ..
            } => {
                sink.send_control(ServerEvent::new(ServerEventBody::Accepted {
                    request_id: request.clone(),
                    work_item_id,
                    sequence,
                }))
                .await?;
                sink.open_delivery();
            }
            LocalSubmitOutcome::Duplicate {
                work_item_id: None, ..
            } => {
                let record = self
                    .ingress
                    .resolve_local_request(&self.actor, &request)
                    .await?;
                return self
                    .serve_durable_request(request, record, sink, &mut read, Some(subscription))
                    .await;
            }
            LocalSubmitOutcome::Conflict => {
                sink.send_control(request_error(
                    request,
                    "request_conflict",
                    "request ID was already used with different text",
                ))
                .await?;
                return sink.close().await;
            }
            LocalSubmitOutcome::ActorUnavailable => {
                sink.send_control(request_error(
                    request,
                    "actor_unavailable",
                    "configured runtime actor is unavailable",
                ))
                .await?;
                return sink.close().await;
            }
        }
        loop {
            tokio::select! {
                event = subscription.recv() => match event {
                    Some(event) => sink.send(event).await?,
                    None => return Ok(()),
                },
                _ = wait_for_disconnect(&mut read) => return sink.close().await,
            }
        }
    }

    async fn resume(
        &self,
        request: RequestId,
        sink: Arc<SocketDeliverySink>,
        mut read: ReadHalf<UnixStream>,
        decode_ticket: Option<u64>,
    ) -> Result<()> {
        self.submissions.wait_for(&request).await?;
        let mut record = self
            .ingress
            .resolve_local_request(&self.actor, &request)
            .await?;
        if record.is_none()
            && let Some(ticket) = decode_ticket
        {
            self.decodes.wait_for_prior(ticket).await;
            self.submissions.wait_for(&request).await?;
            record = self
                .ingress
                .resolve_local_request(&self.actor, &request)
                .await?;
        }
        let delivery = record.as_ref().map(|_| {
            self.hub
                .subscribe_delivery(request.clone(), sink.clone())
                .expect("delivery subscription is infallible")
        });
        let result = self
            .serve_durable_request(
                request.clone(),
                record.clone(),
                sink,
                &mut read,
                record
                    .as_ref()
                    .filter(|record| record.state == LocalRequestState::Active)
                    .map(|_| {
                        self.hub
                            .subscribe(request)
                            .expect("subscription is infallible")
                    }),
            )
            .await;
        drop(delivery);
        result
    }

    async fn serve_durable_request(
        &self,
        request: RequestId,
        mut record: Option<crate::runtime::store::LocalRequestRecord>,
        sink: Arc<SocketDeliverySink>,
        read: &mut ReadHalf<UnixStream>,
        mut transient: Option<crate::runtime::stream_hub::StreamSubscription>,
    ) -> Result<()> {
        let Some(mut current) = record.take() else {
            sink.send_control(request_error(
                request,
                "missing_request",
                "request does not exist",
            ))
            .await?;
            return sink.close().await;
        };

        loop {
            match (
                current.state,
                current.work_item_id.clone(),
                current.result_bundle_state,
            ) {
                (LocalRequestState::Active, Some(work_item_id), _) => {
                    sink.send_control(ServerEvent::new(ServerEventBody::Accepted {
                        request_id: request,
                        work_item_id,
                        sequence: current.sequence,
                    }))
                    .await?;
                    sink.open_delivery();
                    loop {
                        tokio::select! {
                            event = receive_transient(&mut transient) => match event {
                                Some(event) => sink.send(event).await?,
                                None => return sink.close().await,
                            },
                            _ = wait_for_disconnect(read) => return sink.close().await,
                        }
                    }
                }
                (LocalRequestState::Active, None, _) => {
                    tokio::select! {
                        event = receive_transient(&mut transient) => match event {
                            Some(event) => sink.send(event).await?,
                            None => return sink.close().await,
                        },
                        _ = wait_for_disconnect(read) => return sink.close().await,
                        _ = tokio::time::sleep(std::time::Duration::from_millis(50)) => {
                            let Some(updated) = self.ingress.resolve_local_request(&self.actor, &request).await? else {
                                sink.send_control(request_error(request, "missing_request", "request disappeared during resume")).await?;
                                return sink.close().await;
                            };
                            current = updated;
                        }
                    }
                }
                (_, _, Some(BundleState::Delivered)) => {
                    sink.open_delivery();
                    let Some(bundle_id) = current.result_bundle_id.as_ref() else {
                        sink.send_control(request_error(
                            request,
                            "invalid_request_state",
                            "delivered request has no durable result bundle ID",
                        ))
                        .await?;
                        return sink.close().await;
                    };
                    if !sink.reserve_replay(bundle_id) {
                        wait_for_disconnect(read).await;
                        return sink.close().await;
                    }
                    if !self
                        .outbox
                        .replay(&self.actor, &request, sink.clone())
                        .await?
                    {
                        sink.send_control(request_error(
                            request,
                            "missing_result",
                            "delivered request result is unavailable",
                        ))
                        .await?;
                    }
                    return sink.close().await;
                }
                (
                    _,
                    _,
                    Some(
                        BundleState::Pending
                        | BundleState::Delivering
                        | BundleState::FailedRetryable,
                    ),
                ) => {
                    sink.open_delivery();
                    tokio::select! {
                        _ = wait_for_disconnect(read) => return sink.close().await,
                        _ = tokio::time::sleep(std::time::Duration::from_millis(50)) => {
                            let Some(updated) = self.ingress.resolve_local_request(&self.actor, &request).await? else {
                                sink.send_control(request_error(request, "missing_request", "request disappeared during resume")).await?;
                                return sink.close().await;
                            };
                            current = updated;
                        }
                    }
                }
                (_, _, Some(BundleState::FailedTerminal)) => {
                    sink.send_control(request_error(
                        request,
                        "result_failed_terminal",
                        "durable result cannot be delivered",
                    ))
                    .await?;
                    return sink.close().await;
                }
                (_, _, None) => {
                    sink.send_control(request_error(
                        request,
                        "invalid_request_state",
                        "terminal request has no durable result bundle",
                    ))
                    .await?;
                    return sink.close().await;
                }
            }
        }
    }
}

async fn receive_transient(
    subscription: &mut Option<crate::runtime::stream_hub::StreamSubscription>,
) -> Option<ServerEvent> {
    match subscription {
        Some(subscription) => subscription.recv().await,
        None => std::future::pending().await,
    }
}

fn request_id(body: &ClientRequestBody) -> RequestId {
    match body {
        ClientRequestBody::Submit { request_id, .. }
        | ClientRequestBody::Resume { request_id }
        | ClientRequestBody::AckFinal { request_id, .. }
        | ClientRequestBody::Cancel { request_id, .. }
        | ClientRequestBody::IssueLinkCode { request_id } => request_id.clone(),
    }
}

fn cancel_accepted(request_id: RequestId, outcome: CancelOutcome) -> ServerEvent {
    ServerEvent::new(ServerEventBody::CancelAccepted {
        request_id,
        cancel_id: outcome.cancel_id,
        affected_request_ids: outcome.affected_request_ids,
    })
}

fn request_error(request_id: RequestId, code: &str, message: &str) -> ServerEvent {
    ServerEvent::new(ServerEventBody::RequestError {
        request_id,
        code: code.to_owned(),
        message: message.to_owned(),
    })
}

fn protocol_error(error: &ProtocolFailure) -> ServerEvent {
    ServerEvent::new(ServerEventBody::ProtocolError {
        code: error.code(),
        message: error.to_string(),
    })
}

struct SocketDeliverySink {
    writer: tokio::sync::Mutex<WriteHalf<UnixStream>>,
    delivery_open: AtomicBool,
    delivery_ready: tokio::sync::Notify,
    request: Mutex<Option<RequestId>>,
    bundle_participation: Mutex<HashMap<BundleId, BundleParticipation>>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum BundleParticipation {
    Transmission,
    Replay,
}

impl SocketDeliverySink {
    fn new(write: WriteHalf<UnixStream>) -> Self {
        Self {
            writer: tokio::sync::Mutex::new(write),
            delivery_open: AtomicBool::new(false),
            delivery_ready: tokio::sync::Notify::new(),
            request: Mutex::new(None),
            bundle_participation: Mutex::new(HashMap::new()),
        }
    }

    fn open_delivery(&self) {
        self.delivery_open.store(true, Ordering::Release);
        self.delivery_ready.notify_waiters();
    }

    fn set_request(&self, request: RequestId) {
        *self.request.lock().expect("socket request poisoned") = Some(request);
    }

    fn request(&self) -> Option<RequestId> {
        self.request
            .lock()
            .expect("socket request poisoned")
            .clone()
    }

    fn reserve_replay(&self, bundle: &BundleId) -> bool {
        let mut participation = self
            .bundle_participation
            .lock()
            .expect("bundle participation poisoned");
        match participation.get(bundle) {
            Some(BundleParticipation::Transmission) => false,
            Some(BundleParticipation::Replay) => true,
            None => {
                participation.insert(bundle.clone(), BundleParticipation::Replay);
                true
            }
        }
    }

    async fn send_control(&self, event: ServerEvent) -> Result<()> {
        self.write(event).await
    }

    async fn write(&self, event: ServerEvent) -> Result<()> {
        let mut writer = self.writer.lock().await;
        if let Err(error) = FrameWriter::new(&mut *writer)
            .write_server_event(&event)
            .await
        {
            let _ = writer.shutdown().await;
            return Err(error.into());
        }
        Ok(())
    }

    async fn wait_until_open(&self) {
        while !self.delivery_open.load(Ordering::Acquire) {
            let notified = self.delivery_ready.notified();
            if self.delivery_open.load(Ordering::Acquire) {
                break;
            }
            notified.await;
        }
    }

    async fn close(&self) -> Result<()> {
        self.open_delivery();
        match self.writer.lock().await.shutdown().await {
            Ok(()) => Ok(()),
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::NotConnected | io::ErrorKind::BrokenPipe
                ) =>
            {
                Ok(())
            }
            Err(error) => Err(error.into()),
        }
    }
}

#[async_trait]
impl BundleDeliverySink for SocketDeliverySink {
    fn reserve_transmission(&self, bundle: &BundleId) -> bool {
        let mut participation = self
            .bundle_participation
            .lock()
            .expect("bundle participation poisoned");
        match participation.get(bundle) {
            Some(BundleParticipation::Replay) => false,
            Some(BundleParticipation::Transmission) => true,
            None => {
                participation.insert(bundle.clone(), BundleParticipation::Transmission);
                true
            }
        }
    }

    async fn send(&self, event: ServerEvent) -> Result<()> {
        self.wait_until_open().await;
        self.write(event).await
    }

    async fn send_shared(&self, event: &ServerEvent) -> Result<()> {
        self.wait_until_open().await;
        let mut writer = self.writer.lock().await;
        if let Err(error) = FrameWriter::new(&mut *writer)
            .write_server_event(event)
            .await
        {
            let _ = writer.shutdown().await;
            return Err(error.into());
        }
        Ok(())
    }

    async fn abort(&self, _error: &str) {
        let _ = self.close().await;
    }
}

async fn wait_for_disconnect(read: &mut ReadHalf<UnixStream>) {
    let mut byte = [0_u8; 1];
    let _ = read.read(&mut byte).await;
}

#[derive(Clone, Default)]
struct ActiveConnections {
    inner: Arc<ActiveConnectionsInner>,
}

#[derive(Default)]
struct ActiveConnectionsInner {
    next_id: AtomicU64,
    sinks: Mutex<HashMap<u64, Weak<SocketDeliverySink>>>,
    shutting_down: AtomicBool,
}

struct ActiveConnectionGuard {
    id: u64,
    connections: ActiveConnections,
}

impl ActiveConnections {
    fn register(&self, sink: &Arc<SocketDeliverySink>) -> ActiveConnectionGuard {
        let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        self.inner
            .sinks
            .lock()
            .expect("active connections poisoned")
            .insert(id, Arc::downgrade(sink));
        ActiveConnectionGuard {
            id,
            connections: self.clone(),
        }
    }

    fn is_shutting_down(&self) -> bool {
        self.inner.shutting_down.load(Ordering::SeqCst)
    }

    async fn notify_sink(&self, sink: &SocketDeliverySink) {
        let request_id = sink.request();
        let resume_command = request_id
            .as_ref()
            .map(|request| format!("codrik resume {}", request.as_str()));
        let _ = sink
            .send_control(ServerEvent::new(ServerEventBody::ServerShuttingDown {
                request_id,
                resume_command,
            }))
            .await;
    }

    async fn begin_shutdown(&self) {
        self.inner.shutting_down.store(true, Ordering::SeqCst);
        let sinks = self
            .inner
            .sinks
            .lock()
            .expect("active connections poisoned")
            .values()
            .filter_map(Weak::upgrade)
            .collect::<Vec<_>>();
        let connections = self.clone();
        futures_util::future::join_all(sinks.into_iter().map(move |sink| {
            let connections = connections.clone();
            async move { connections.notify_sink(&sink).await }
        }))
        .await;
    }
}

impl Drop for ActiveConnectionGuard {
    fn drop(&mut self) {
        self.connections
            .inner
            .sinks
            .lock()
            .expect("active connections poisoned")
            .remove(&self.id);
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc, Barrier, Mutex,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use anyhow::{Result, bail};
    use async_trait::async_trait;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt, split},
        net::UnixStream,
        sync::watch,
        time::timeout,
    };

    use crate::{
        runtime::{
            gateway::GatewayCommandKey,
            identity_link::{IdentityLinkManager, IssuedLinkCode, LinkRedemption},
            ipc::{
                protocol::{
                    ClientRequest, ClientRequestBody, FrameReader, FrameWriter, ProtocolErrorCode,
                    ServerEvent, ServerEventBody, encode_bundle,
                },
                security::{PeerCredentials, bind_secure_listener, create_secure_directory},
            },
            model::{
                ActorId, BundleId, BundleState, CancelId, DeliveryId, LocalRequestState, RequestId,
                Timestamp, WorkItemId,
            },
            outbox_worker::{BundleDeliverySink, DeliveryRegistry},
            signals::ActorSignals,
            store::{
                AckOutcome, AckRejected, BundleAck, BundleManifest, CancelOutcome, FinalPayload,
                LinkIdentity, LocalCancel, LocalIngressStore, LocalRequestRecord, LocalSubmission,
                LocalSubmitOutcome, ResultBundle,
            },
            stream_hub::StreamHub,
        },
        test_fixtures::{ActorSeed, ActorSeedSet},
    };

    use super::{
        ActiveConnections, ConnectionHandler, DecodeRegistry, IpcOutbox, LocalIpcServer,
        MAX_CONNECTIONS, SocketDeliverySink, SubmissionRegistry,
    };

    struct SameUid;

    struct TestLinking;

    #[async_trait]
    impl IdentityLinkManager for TestLinking {
        async fn issue_code(&self, _actor: &ActorId) -> Result<IssuedLinkCode> {
            Ok(IssuedLinkCode {
                code: "ABCD-EFGH".into(),
                expires_at: Timestamp(1_721_234_567_890),
            })
        }

        async fn redeem_code(
            &self,
            _identity: LinkIdentity,
            _code: &str,
        ) -> Result<LinkRedemption> {
            unreachable!("IPC issuance never redeems a link code")
        }

        async fn redeem_code_once(
            &self,
            _key: GatewayCommandKey,
            _identity: LinkIdentity,
            _code: &str,
        ) -> Result<LinkRedemption> {
            unreachable!("IPC issuance never redeems a link code")
        }

        async fn collect_expired(&self, _limit: usize) -> Result<usize> {
            Ok(0)
        }
    }

    impl PeerCredentials for SameUid {
        fn peer_uid(&self, _stream: &UnixStream) -> std::io::Result<u32> {
            Ok(unsafe { libc::geteuid() })
        }
    }

    fn short_temp() -> &'static std::path::Path {
        #[cfg(target_os = "macos")]
        return std::path::Path::new("/private/tmp");
        #[cfg(target_os = "linux")]
        return std::path::Path::new("/tmp");
    }

    struct CountingCredentials(Arc<AtomicUsize>);

    impl PeerCredentials for CountingCredentials {
        fn peer_uid(&self, _stream: &UnixStream) -> std::io::Result<u32> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(unsafe { libc::geteuid() })
        }
    }

    struct BlockingCredentials {
        entered: Arc<AtomicBool>,
        release: Arc<Barrier>,
    }

    struct PanicCredentials;

    impl PeerCredentials for PanicCredentials {
        fn peer_uid(&self, _stream: &UnixStream) -> std::io::Result<u32> {
            panic!("simulated credential panic")
        }
    }

    impl PeerCredentials for BlockingCredentials {
        fn peer_uid(&self, _stream: &UnixStream) -> std::io::Result<u32> {
            self.entered.store(true, Ordering::SeqCst);
            self.release.wait();
            Ok(unsafe { libc::geteuid() })
        }
    }

    struct TestIngress {
        hub: Arc<StreamHub>,
        submissions: SubmissionRegistry,
        record: Mutex<Option<LocalRequestRecord>>,
    }

    struct RaceIngress {
        hub: Arc<StreamHub>,
        submissions: SubmissionRegistry,
        commit: bool,
        started: tokio::sync::Notify,
        release: tokio::sync::Notify,
        record: Mutex<Option<LocalRequestRecord>>,
        resolve_calls: AtomicUsize,
    }

    struct DuplicateIngress {
        hub: Arc<StreamHub>,
        submissions: SubmissionRegistry,
        record: Mutex<Option<LocalRequestRecord>>,
    }

    struct AuthorityIngress;

    #[async_trait]
    impl LocalIngressStore for AuthorityIngress {
        async fn submit_for_actor(
            &self,
            _actor: &ActorId,
            _command: LocalSubmission,
            _now: Timestamp,
        ) -> Result<LocalSubmitOutcome> {
            bail!("simulated SQLite I/O authority failure")
        }

        async fn cancel_for_actor(
            &self,
            _actor: &ActorId,
            _command: LocalCancel,
            _now: Timestamp,
        ) -> Result<CancelOutcome> {
            bail!("simulated SQLite I/O authority failure")
        }

        async fn resolve_local_request(
            &self,
            _actor: &ActorId,
            _id: &RequestId,
        ) -> Result<Option<LocalRequestRecord>> {
            bail!("simulated SQLite I/O authority failure")
        }
    }

    #[async_trait]
    impl LocalIngressStore for DuplicateIngress {
        async fn submit_for_actor(
            &self,
            _actor: &ActorId,
            command: LocalSubmission,
            _now: Timestamp,
        ) -> Result<LocalSubmitOutcome> {
            use crate::runtime::outbox_worker::DeliveryRegistry;
            assert!(self.submissions.is_inflight(&command.request_id));
            assert_eq!(self.hub.snapshot(&command.request_id).len(), 1);
            let record = self.record.lock().unwrap().clone().unwrap();
            Ok(LocalSubmitOutcome::Duplicate {
                event_id: record.event_id,
                work_item_id: None,
                sequence: record.sequence,
            })
        }

        async fn cancel_for_actor(
            &self,
            _actor: &ActorId,
            command: LocalCancel,
            _now: Timestamp,
        ) -> Result<CancelOutcome> {
            Ok(CancelOutcome {
                cancel_id: command.cancel_id,
                affected_request_ids: vec![command.request_id],
                already_terminal: false,
            })
        }

        async fn resolve_local_request(
            &self,
            _actor: &ActorId,
            _id: &RequestId,
        ) -> Result<Option<LocalRequestRecord>> {
            Ok(self.record.lock().unwrap().clone())
        }
    }

    #[async_trait]
    impl LocalIngressStore for RaceIngress {
        async fn submit_for_actor(
            &self,
            actor: &ActorId,
            command: LocalSubmission,
            _now: Timestamp,
        ) -> Result<LocalSubmitOutcome> {
            use crate::runtime::outbox_worker::DeliveryRegistry;
            assert!(self.submissions.is_inflight(&command.request_id));
            assert!(!self.hub.snapshot(&command.request_id).is_empty());
            self.started.notify_waiters();
            self.release.notified().await;
            if !self.commit {
                anyhow::bail!("injected submit rollback");
            }
            let event_id = crate::runtime::model::EventId::new();
            let work_item_id = WorkItemId::new();
            let record = LocalRequestRecord {
                request_id: command.request_id,
                actor_id: actor.clone(),
                event_id: event_id.clone(),
                sequence: 7,
                work_item_id: Some(work_item_id.clone()),
                state: LocalRequestState::Active,
                result_bundle_id: None,
                result_bundle_state: None,
            };
            *self.record.lock().unwrap() = Some(record);
            Ok(LocalSubmitOutcome::Accepted {
                event_id,
                work_item_id,
                sequence: 7,
            })
        }

        async fn cancel_for_actor(
            &self,
            _actor: &ActorId,
            command: LocalCancel,
            _now: Timestamp,
        ) -> Result<CancelOutcome> {
            Ok(CancelOutcome {
                cancel_id: command.cancel_id,
                affected_request_ids: vec![command.request_id],
                already_terminal: false,
            })
        }

        async fn resolve_local_request(
            &self,
            _actor: &ActorId,
            _id: &RequestId,
        ) -> Result<Option<LocalRequestRecord>> {
            self.resolve_calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.record.lock().unwrap().clone())
        }
    }

    #[async_trait]
    impl LocalIngressStore for TestIngress {
        async fn submit_for_actor(
            &self,
            actor: &ActorId,
            command: LocalSubmission,
            _now: Timestamp,
        ) -> Result<LocalSubmitOutcome> {
            use crate::runtime::outbox_worker::DeliveryRegistry;
            assert!(self.submissions.is_inflight(&command.request_id));
            assert_eq!(self.hub.snapshot(&command.request_id).len(), 1);
            let work_item_id = WorkItemId::new();
            let event_id = crate::runtime::model::EventId::new();
            *self.record.lock().unwrap() = Some(LocalRequestRecord {
                request_id: command.request_id,
                actor_id: actor.clone(),
                event_id: event_id.clone(),
                sequence: 1,
                work_item_id: Some(work_item_id.clone()),
                state: LocalRequestState::Active,
                result_bundle_id: None,
                result_bundle_state: None,
            });
            Ok(LocalSubmitOutcome::Accepted {
                event_id,
                work_item_id,
                sequence: 1,
            })
        }

        async fn cancel_for_actor(
            &self,
            _actor: &ActorId,
            command: LocalCancel,
            _now: Timestamp,
        ) -> Result<CancelOutcome> {
            Ok(CancelOutcome {
                cancel_id: command.cancel_id,
                affected_request_ids: vec![command.request_id],
                already_terminal: false,
            })
        }

        async fn resolve_local_request(
            &self,
            _actor: &ActorId,
            _id: &RequestId,
        ) -> Result<Option<LocalRequestRecord>> {
            Ok(self.record.lock().unwrap().clone())
        }
    }

    #[derive(Default)]
    struct TestOutbox {
        acks: Mutex<Vec<BundleAck>>,
        ack_error: Mutex<Option<String>>,
        ack_rejected: AtomicBool,
        replay: AtomicBool,
        replay_calls: AtomicUsize,
        replay_events: Mutex<Vec<ServerEvent>>,
    }

    #[derive(Default)]
    struct NoopDeliverySink;

    #[async_trait]
    impl BundleDeliverySink for NoopDeliverySink {
        async fn send(&self, _event: ServerEvent) -> Result<()> {
            Ok(())
        }
    }

    #[async_trait]
    impl IpcOutbox for TestOutbox {
        async fn acknowledge(&self, ack: BundleAck) -> Result<AckOutcome> {
            if self.ack_rejected.load(Ordering::SeqCst) {
                return Err(anyhow::Error::new(AckRejected(
                    "invalid acknowledgement".into(),
                )));
            }
            if let Some(message) = self.ack_error.lock().unwrap().clone() {
                bail!(message);
            }
            self.acks.lock().unwrap().push(ack);
            Ok(AckOutcome::Delivered)
        }

        async fn replay(
            &self,
            _actor: &ActorId,
            _request: &RequestId,
            sink: Arc<dyn BundleDeliverySink>,
        ) -> Result<bool> {
            self.replay_calls.fetch_add(1, Ordering::SeqCst);
            if !self.replay.load(Ordering::SeqCst) {
                return Ok(false);
            }
            let events = self.replay_events.lock().unwrap().clone();
            for event in events {
                sink.send(event).await?;
            }
            Ok(true)
        }
    }

    fn result_bundle(request_id: RequestId, bundle_id: BundleId) -> ResultBundle {
        ResultBundle {
            id: bundle_id,
            request_id,
            state: BundleState::Delivered,
            manifest: BundleManifest {
                entries: Vec::new(),
                sha256: String::new(),
            },
            deliveries: vec![(
                DeliveryId::new(),
                FinalPayload::Text {
                    text: "done".to_owned(),
                },
            )],
        }
    }

    fn test_handler() -> ConnectionHandler {
        let submissions = SubmissionRegistry::default();
        let hub = Arc::new(StreamHub::default());
        let ingress = Arc::new(TestIngress {
            hub: hub.clone(),
            submissions: submissions.clone(),
            record: Mutex::new(None),
        });
        ConnectionHandler {
            actor: ActorId::from_string("actor:local:test"),
            ingress,
            outbox: Arc::new(TestOutbox::default()),
            hub,
            credentials: Arc::new(SameUid),
            submissions,
            active: Default::default(),
            decodes: Default::default(),
            hooks: Arc::new(crate::runtime::hooks::NoopRuntimeBoundaryHooks),
            signals: ActorSignals::default(),
            linking: None,
        }
    }

    fn test_handler_with_record(
        record: Option<LocalRequestRecord>,
    ) -> (
        ConnectionHandler,
        Arc<TestIngress>,
        Arc<TestOutbox>,
        Arc<StreamHub>,
    ) {
        let submissions = SubmissionRegistry::default();
        let hub = Arc::new(StreamHub::default());
        let ingress = Arc::new(TestIngress {
            hub: hub.clone(),
            submissions: submissions.clone(),
            record: Mutex::new(record),
        });
        let outbox = Arc::new(TestOutbox::default());
        (
            ConnectionHandler {
                actor: ActorId::from_string("actor:local:test"),
                ingress: ingress.clone(),
                outbox: outbox.clone(),
                hub: hub.clone(),
                credentials: Arc::new(SameUid),
                submissions,
                active: Default::default(),
                decodes: Default::default(),
                hooks: Arc::new(crate::runtime::hooks::NoopRuntimeBoundaryHooks),
                signals: ActorSignals::default(),
                linking: None,
            },
            ingress,
            outbox,
            hub,
        )
    }

    fn request_record(
        request_id: RequestId,
        work_item_id: Option<WorkItemId>,
        state: LocalRequestState,
        result_bundle_id: Option<crate::runtime::model::BundleId>,
    ) -> LocalRequestRecord {
        LocalRequestRecord {
            request_id,
            actor_id: ActorId::from_string("actor:local:test"),
            event_id: crate::runtime::model::EventId::new(),
            sequence: 1,
            work_item_id,
            state,
            result_bundle_id: result_bundle_id.clone(),
            result_bundle_state: result_bundle_id
                .map(|_| crate::runtime::model::BundleState::Pending),
        }
    }

    fn race_handler(
        ingress: Arc<RaceIngress>,
        hub: Arc<StreamHub>,
        submissions: SubmissionRegistry,
    ) -> ConnectionHandler {
        ConnectionHandler {
            actor: ActorId::from_string("actor:local:test"),
            ingress,
            outbox: Arc::new(TestOutbox::default()),
            hub,
            credentials: Arc::new(SameUid),
            submissions,
            active: Default::default(),
            decodes: Default::default(),
            hooks: Arc::new(crate::runtime::hooks::NoopRuntimeBoundaryHooks),
            signals: ActorSignals::default(),
            linking: None,
        }
    }

    fn duplicate_handler(
        record: LocalRequestRecord,
    ) -> (
        ConnectionHandler,
        Arc<DuplicateIngress>,
        Arc<TestOutbox>,
        Arc<StreamHub>,
    ) {
        let submissions = SubmissionRegistry::default();
        let hub = Arc::new(StreamHub::default());
        let ingress = Arc::new(DuplicateIngress {
            hub: hub.clone(),
            submissions: submissions.clone(),
            record: Mutex::new(Some(record)),
        });
        let outbox = Arc::new(TestOutbox::default());
        (
            ConnectionHandler {
                actor: ActorId::from_string("actor:local:test"),
                ingress: ingress.clone(),
                outbox: outbox.clone(),
                hub: hub.clone(),
                credentials: Arc::new(SameUid),
                submissions,
                active: Default::default(),
                decodes: Default::default(),
                hooks: Arc::new(crate::runtime::hooks::NoopRuntimeBoundaryHooks),
                signals: ActorSignals::default(),
                linking: None,
            },
            ingress,
            outbox,
            hub,
        )
    }

    #[tokio::test]
    async fn resume_waits_for_inflight_submit_completion() -> Result<()> {
        let registry = SubmissionRegistry::default();
        let request = RequestId::new();
        let submission = registry.register(request.clone())?;
        let waiting = {
            let registry = registry.clone();
            let request = request.clone();
            tokio::spawn(async move { registry.wait_for(&request).await })
        };
        assert!(
            timeout(Duration::from_millis(20), async {
                while !waiting.is_finished() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .is_err()
        );
        submission.complete();
        waiting.await??;
        Ok(())
    }

    #[tokio::test]
    async fn registry_rejects_two_simultaneous_submits_for_one_request() -> Result<()> {
        let registry = SubmissionRegistry::default();
        let request = RequestId::new();
        let _first = registry.register(request.clone())?;
        assert!(registry.register(request).is_err());
        Ok(())
    }

    #[tokio::test]
    async fn earlier_decode_error_releases_later_request_barrier() {
        let decodes = DecodeRegistry::default();
        let earlier = decodes.register();
        let later = decodes.register();
        let later_ticket = later.ticket;
        drop(later);
        let waiting = {
            let decodes = decodes.clone();
            tokio::spawn(async move { decodes.wait_for_prior(later_ticket).await })
        };
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());
        drop(earlier);
        waiting.await.unwrap();
    }

    #[tokio::test]
    async fn resume_cannot_report_missing_before_earlier_connection_registers_submit() -> Result<()>
    {
        let request = RequestId::new();
        let submissions = SubmissionRegistry::default();
        let decodes = DecodeRegistry::default();
        let submit_decode = decodes.register();
        let resume_decode = decodes.register();
        let hub = Arc::new(StreamHub::default());
        let ingress = Arc::new(RaceIngress {
            hub: hub.clone(),
            submissions: submissions.clone(),
            commit: true,
            started: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
            record: Mutex::new(None),
            resolve_calls: AtomicUsize::new(0),
        });
        let (submit_server, mut submit_client) = UnixStream::pair()?;
        let (resume_server, mut resume_client) = UnixStream::pair()?;
        let mut submit_handler = race_handler(ingress.clone(), hub.clone(), submissions.clone());
        submit_handler.decodes = decodes.clone();
        let mut resume_handler = race_handler(ingress.clone(), hub, submissions);
        resume_handler.decodes = decodes;
        let submit_task = tokio::spawn(async move {
            submit_handler
                .handle_with_decode(submit_server, Some(submit_decode))
                .await
        });
        let resume_task = tokio::spawn(async move {
            resume_handler
                .handle_with_decode(resume_server, Some(resume_decode))
                .await
        });

        FrameWriter::new(&mut resume_client)
            .write_client_request(&ClientRequest::new(ClientRequestBody::Resume {
                request_id: request.clone(),
            }))
            .await?;
        assert!(
            timeout(
                Duration::from_millis(20),
                FrameReader::new(&mut resume_client).read_server_event(),
            )
            .await
            .is_err(),
            "resume reported missing before the earlier connection decoded"
        );

        let started = ingress.started.notified();
        FrameWriter::new(&mut submit_client)
            .write_client_request(&ClientRequest::new(ClientRequestBody::Submit {
                request_id: request.clone(),
                text: "hello".to_owned(),
            }))
            .await?;
        started.await;
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(ingress.resolve_calls.load(Ordering::SeqCst), 1);
        ingress.release.notify_one();

        let submit_event = FrameReader::new(&mut submit_client)
            .read_server_event()
            .await?;
        let resume_event = FrameReader::new(&mut resume_client)
            .read_server_event()
            .await?;
        assert!(matches!(
            submit_event.body,
            ServerEventBody::Accepted { .. }
        ));
        assert!(matches!(
            resume_event.body,
            ServerEventBody::Accepted { .. }
        ));
        drop(submit_client);
        drop(resume_client);
        submit_task.await??;
        resume_task.await??;
        Ok(())
    }

    #[tokio::test]
    async fn resume_waits_for_submit_commit_then_emits_exact_accepted() -> Result<()> {
        let request = RequestId::new();
        let submissions = SubmissionRegistry::default();
        let hub = Arc::new(StreamHub::default());
        let ingress = Arc::new(RaceIngress {
            hub: hub.clone(),
            submissions: submissions.clone(),
            commit: true,
            started: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
            record: Mutex::new(None),
            resolve_calls: AtomicUsize::new(0),
        });
        let (submit_server, mut submit_client) = UnixStream::pair()?;
        let (resume_server, mut resume_client) = UnixStream::pair()?;
        let submit_handler = race_handler(ingress.clone(), hub.clone(), submissions.clone());
        let resume_handler = race_handler(ingress.clone(), hub, submissions);
        let submit_task = tokio::spawn(async move { submit_handler.handle(submit_server).await });
        let resume_task = tokio::spawn(async move { resume_handler.handle(resume_server).await });
        let started = ingress.started.notified();
        FrameWriter::new(&mut submit_client)
            .write_client_request(&ClientRequest::new(ClientRequestBody::Submit {
                request_id: request.clone(),
                text: "hello".to_owned(),
            }))
            .await?;
        started.await;
        FrameWriter::new(&mut resume_client)
            .write_client_request(&ClientRequest::new(ClientRequestBody::Resume {
                request_id: request.clone(),
            }))
            .await?;
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(ingress.resolve_calls.load(Ordering::SeqCst), 0);
        ingress.release.notify_one();
        let submit_event = FrameReader::new(&mut submit_client)
            .read_server_event()
            .await?;
        let resume_event = FrameReader::new(&mut resume_client)
            .read_server_event()
            .await?;
        let ServerEventBody::Accepted {
            work_item_id,
            sequence,
            ..
        } = submit_event.body
        else {
            panic!("submit did not receive Accepted");
        };
        assert_eq!(
            resume_event.body,
            ServerEventBody::Accepted {
                request_id: request,
                work_item_id,
                sequence,
            }
        );
        drop(submit_client);
        drop(resume_client);
        submit_task.await??;
        resume_task.await??;
        Ok(())
    }

    #[tokio::test]
    async fn resume_waits_for_submit_rollback_then_reports_missing() -> Result<()> {
        let request = RequestId::new();
        let submissions = SubmissionRegistry::default();
        let hub = Arc::new(StreamHub::default());
        let ingress = Arc::new(RaceIngress {
            hub: hub.clone(),
            submissions: submissions.clone(),
            commit: false,
            started: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
            record: Mutex::new(None),
            resolve_calls: AtomicUsize::new(0),
        });
        let (submit_server, mut submit_client) = UnixStream::pair()?;
        let (resume_server, mut resume_client) = UnixStream::pair()?;
        let submit_handler = race_handler(ingress.clone(), hub.clone(), submissions.clone());
        let resume_handler = race_handler(ingress.clone(), hub, submissions);
        let submit_task = tokio::spawn(async move { submit_handler.handle(submit_server).await });
        let resume_task = tokio::spawn(async move { resume_handler.handle(resume_server).await });
        let started = ingress.started.notified();
        FrameWriter::new(&mut submit_client)
            .write_client_request(&ClientRequest::new(ClientRequestBody::Submit {
                request_id: request.clone(),
                text: "hello".to_owned(),
            }))
            .await?;
        started.await;
        FrameWriter::new(&mut resume_client)
            .write_client_request(&ClientRequest::new(ClientRequestBody::Resume {
                request_id: request.clone(),
            }))
            .await?;
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(ingress.resolve_calls.load(Ordering::SeqCst), 0);
        ingress.release.notify_one();
        let event = FrameReader::new(&mut resume_client)
            .read_server_event()
            .await?;
        assert!(matches!(
            event.body,
            ServerEventBody::RequestError { request_id, ref code, .. }
                if request_id == request && code == "missing_request"
        ));
        assert!(submit_task.await?.is_err());
        resume_task.await??;
        drop(submit_client);
        Ok(())
    }

    #[tokio::test]
    async fn delivery_waits_until_accepted_is_written() -> Result<()> {
        let request = RequestId::new();
        let (server, client) = UnixStream::pair()?;
        let (_, write) = split(server);
        let sink = std::sync::Arc::new(SocketDeliverySink::new(write));
        let delivery = {
            let sink = sink.clone();
            let request = request.clone();
            tokio::spawn(async move {
                sink.send(ServerEvent::new(ServerEventBody::StreamGap {
                    request_id: request,
                }))
                .await
            })
        };
        tokio::task::yield_now().await;
        assert!(!delivery.is_finished());
        sink.send_control(ServerEvent::new(ServerEventBody::Accepted {
            request_id: request.clone(),
            work_item_id: WorkItemId::new(),
            sequence: 1,
        }))
        .await?;
        sink.open_delivery();
        delivery.await??;
        let mut client = client;
        let mut reader = FrameReader::new(&mut client);
        assert!(matches!(
            reader.read_server_event().await?.body,
            ServerEventBody::Accepted { .. }
        ));
        assert!(matches!(
            reader.read_server_event().await?.body,
            ServerEventBody::StreamGap { .. }
        ));
        Ok(())
    }

    #[test]
    fn server_connection_limit_is_exactly_64() {
        assert_eq!(MAX_CONNECTIONS, 64);
    }

    #[tokio::test]
    async fn sixty_fifth_connection_is_not_spawned_until_a_permit_is_free() -> Result<()> {
        let root = short_temp().join(format!("cc-{}", uuid::Uuid::new_v4()));
        create_secure_directory(&root)?;
        let socket = root.join("c.sock");
        let listener = bind_secure_listener(&socket)?;
        let handler = test_handler();
        let authenticated = Arc::new(AtomicUsize::new(0));
        let server = LocalIpcServer::with_credentials(
            listener,
            handler.actor,
            handler.ingress,
            handler.outbox,
            handler.hub,
            Arc::new(CountingCredentials(authenticated.clone())),
        );
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let task = tokio::spawn(server.run(shutdown_rx));
        let mut clients = Vec::new();
        for _ in 0..=MAX_CONNECTIONS {
            clients.push(UnixStream::connect(&socket).await?);
        }
        timeout(Duration::from_secs(1), async {
            while authenticated.load(Ordering::SeqCst) < MAX_CONNECTIONS {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        tokio::task::yield_now().await;
        assert_eq!(authenticated.load(Ordering::SeqCst), MAX_CONNECTIONS);
        drop(clients.remove(0));
        timeout(Duration::from_secs(1), async {
            while authenticated.load(Ordering::SeqCst) < MAX_CONNECTIONS + 1 {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        assert_eq!(authenticated.load(Ordering::SeqCst), MAX_CONNECTIONS + 1);
        shutdown_tx.send(true)?;
        drop(clients);
        task.await??;
        tokio::task::yield_now().await;
        std::fs::remove_file(socket)?;
        std::fs::remove_dir(root)?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shutdown_waits_for_handler_caught_between_accept_and_registration() -> Result<()> {
        let root = short_temp().join(format!("cr-{}", uuid::Uuid::new_v4()));
        create_secure_directory(&root)?;
        let socket = root.join("c.sock");
        let listener = bind_secure_listener(&socket)?;
        let handler = test_handler();
        let entered = Arc::new(AtomicBool::new(false));
        let release = Arc::new(Barrier::new(2));
        let server = LocalIpcServer::with_credentials(
            listener,
            handler.actor,
            handler.ingress,
            handler.outbox,
            handler.hub,
            Arc::new(BlockingCredentials {
                entered: entered.clone(),
                release: release.clone(),
            }),
        );
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let task = tokio::spawn(server.run(shutdown_rx));
        let client = UnixStream::connect(&socket).await?;
        timeout(Duration::from_secs(1), async {
            while !entered.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        shutdown_tx.send(true)?;
        tokio::time::sleep(Duration::from_millis(20)).await;
        let returned_early = task.is_finished();
        tokio::task::spawn_blocking(move || release.wait()).await?;
        task.await??;
        assert!(
            !returned_early,
            "server returned with an accepted handler alive"
        );
        drop(client);
        std::fs::remove_file(socket)?;
        std::fs::remove_dir(root)?;
        Ok(())
    }

    #[tokio::test]
    async fn incomplete_frame_returns_protocol_error_and_closes() -> Result<()> {
        let (server, mut client) = UnixStream::pair()?;
        let task = tokio::spawn(async move { test_handler().handle(server).await });
        client.write_all(&[0, 0]).await?;
        client.shutdown().await?;
        let event = FrameReader::new(client).read_server_event().await?;
        assert!(matches!(
            event.body,
            ServerEventBody::ProtocolError {
                code: ProtocolErrorCode::IncompleteFrame,
                ..
            }
        ));
        task.await??;
        Ok(())
    }

    #[tokio::test]
    async fn issue_link_code_returns_terminal_response_without_submitting_work() -> Result<()> {
        let mut handler = test_handler();
        handler.linking = Some(Arc::new(TestLinking));
        let ingress = handler.ingress.clone();
        let request_id = RequestId::new();
        let expected = request_id.clone();
        let (server, mut client) = UnixStream::pair()?;
        let task = tokio::spawn(async move { handler.handle(server).await });
        FrameWriter::new(&mut client)
            .write_client_request(&ClientRequest::new(ClientRequestBody::IssueLinkCode {
                request_id,
            }))
            .await?;
        client.shutdown().await?;

        let event = FrameReader::new(client).read_server_event().await?;
        assert_eq!(
            event.body,
            ServerEventBody::LinkCodeIssued {
                request_id: expected,
                code: "ABCD-EFGH".into(),
                expires_at: 1_721_234_567_890,
            }
        );
        assert!(
            ingress
                .resolve_local_request(&ActorId::from_string("actor:local:test"), &RequestId::new())
                .await?
                .is_none()
        );
        task.await??;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn slow_frame_header_returns_timeout_and_closes() -> Result<()> {
        let (server, client) = UnixStream::pair()?;
        let task = tokio::spawn(async move { test_handler().handle(server).await });
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(6)).await;
        let event = FrameReader::new(client).read_server_event().await?;
        assert!(matches!(
            event.body,
            ServerEventBody::ProtocolError {
                code: ProtocolErrorCode::HeaderTimeout,
                ..
            }
        ));
        task.await??;
        Ok(())
    }

    #[tokio::test]
    async fn shutdown_notice_includes_resume_command() -> Result<()> {
        let request = RequestId::new();
        let active = ActiveConnections::default();
        let (server, client) = UnixStream::pair()?;
        let (_, write) = split(server);
        let sink = Arc::new(SocketDeliverySink::new(write));
        sink.set_request(request.clone());
        let _guard = active.register(&sink);
        active.begin_shutdown().await;
        let event = FrameReader::new(client).read_server_event().await?;
        assert_eq!(
            event.body,
            ServerEventBody::ServerShuttingDown {
                request_id: Some(request.clone()),
                resume_command: Some(format!("codrik resume {}", request.as_str())),
            }
        );
        Ok(())
    }

    struct PausedAckOutbox {
        entered: tokio::sync::Notify,
        release: tokio::sync::Notify,
        acked: AtomicBool,
    }

    #[async_trait]
    impl IpcOutbox for PausedAckOutbox {
        async fn acknowledge(&self, _ack: BundleAck) -> Result<AckOutcome> {
            self.entered.notify_one();
            self.release.notified().await;
            self.acked.store(true, Ordering::SeqCst);
            Ok(AckOutcome::Delivered)
        }

        async fn replay(
            &self,
            _actor: &ActorId,
            _request: &RequestId,
            _sink: Arc<dyn BundleDeliverySink>,
        ) -> Result<bool> {
            Ok(false)
        }
    }

    #[tokio::test]
    async fn acknowledgement_already_started_commits_during_shutdown_drain() -> Result<()> {
        let outbox = Arc::new(PausedAckOutbox {
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
            acked: AtomicBool::new(false),
        });
        let mut handler = test_handler();
        handler.outbox = outbox.clone();
        let active = handler.active.clone();
        let request = RequestId::new();
        let bundle = BundleId::new();
        let delivery = DeliveryId::new();
        let (server, mut client) = UnixStream::pair()?;
        let task = tokio::spawn(async move { handler.handle(server).await });
        FrameWriter::new(&mut client)
            .write_client_request(&ClientRequest::new(ClientRequestBody::AckFinal {
                request_id: request.clone(),
                bundle_id: bundle.clone(),
                delivery_ids: vec![delivery],
            }))
            .await?;
        outbox.entered.notified().await;
        active.begin_shutdown().await;
        outbox.release.notify_one();

        let mut reader = FrameReader::new(&mut client);
        assert!(matches!(
            reader.read_server_event().await?.body,
            ServerEventBody::ServerShuttingDown { .. }
        ));
        assert!(matches!(
            reader.read_server_event().await?.body,
            ServerEventBody::AckAccepted { .. }
        ));
        task.await??;
        assert!(outbox.acked.load(Ordering::SeqCst));
        Ok(())
    }

    #[tokio::test]
    async fn submit_is_registered_and_subscribed_before_ingress_and_second_operation_closes()
    -> Result<()> {
        let request = RequestId::new();
        let handler = test_handler();
        let (server, mut client) = UnixStream::pair()?;
        let task = tokio::spawn(async move { handler.handle(server).await });
        FrameWriter::new(&mut client)
            .write_client_request(&ClientRequest::new(ClientRequestBody::Submit {
                request_id: request.clone(),
                text: "hello".to_owned(),
            }))
            .await?;
        {
            let mut reader = FrameReader::new(&mut client);
            assert!(matches!(
                reader.read_server_event().await?.body,
                ServerEventBody::Accepted { .. }
            ));
        }
        FrameWriter::new(&mut client)
            .write_client_request(&ClientRequest::new(ClientRequestBody::Resume {
                request_id: request.clone(),
            }))
            .await?;
        timeout(Duration::from_secs(1), task).await???;
        Ok(())
    }

    #[tokio::test]
    async fn active_resume_emits_accepted_with_the_durable_work_item() -> Result<()> {
        let request = RequestId::new();
        let work = WorkItemId::new();
        let (handler, _, _, hub) = test_handler_with_record(Some(request_record(
            request.clone(),
            Some(work.clone()),
            LocalRequestState::Active,
            None,
        )));
        let (server, mut client) = UnixStream::pair()?;
        let task = tokio::spawn(async move { handler.handle(server).await });
        FrameWriter::new(&mut client)
            .write_client_request(&ClientRequest::new(ClientRequestBody::Resume {
                request_id: request.clone(),
            }))
            .await?;
        let event = timeout(
            Duration::from_millis(100),
            FrameReader::new(&mut client).read_server_event(),
        )
        .await??;
        assert!(
            matches!(event.body, ServerEventBody::Accepted { request_id, work_item_id, .. }
            if request_id == request && work_item_id == work)
        );
        hub.publish_text(std::slice::from_ref(&request), "live");
        let delta = timeout(
            Duration::from_millis(100),
            FrameReader::new(&mut client).read_server_event(),
        )
        .await??;
        assert!(matches!(
            delta.body,
            ServerEventBody::TextDelta { request_id, delta }
                if request_id == request && delta == "live"
        ));
        drop(client);
        task.await??;
        Ok(())
    }

    #[tokio::test]
    async fn pending_terminal_resume_stays_registered_without_missing_result() -> Result<()> {
        let request = RequestId::new();
        let (handler, _, outbox, hub) = test_handler_with_record(Some(request_record(
            request.clone(),
            None,
            LocalRequestState::Completed,
            Some(crate::runtime::model::BundleId::new()),
        )));
        let (server, mut client) = UnixStream::pair()?;
        let task = tokio::spawn(async move { handler.handle(server).await });
        FrameWriter::new(&mut client)
            .write_client_request(&ClientRequest::new(ClientRequestBody::Resume {
                request_id: request.clone(),
            }))
            .await?;
        assert!(
            timeout(
                Duration::from_millis(50),
                FrameReader::new(&mut client).read_server_event()
            )
            .await
            .is_err()
        );
        assert!(!task.is_finished());
        use crate::runtime::outbox_worker::DeliveryRegistry;
        assert_eq!(hub.snapshot(&request).len(), 1);
        assert_eq!(outbox.replay_calls.load(Ordering::SeqCst), 0);
        drop(client);
        task.await??;
        assert!(hub.snapshot(&request).is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn late_resume_excluded_from_delivering_snapshot_replays_after_delivered() -> Result<()> {
        let request = RequestId::new();
        let bundle_id = BundleId::new();
        let mut record = request_record(
            request.clone(),
            None,
            LocalRequestState::Completed,
            Some(bundle_id.clone()),
        );
        record.result_bundle_state = Some(BundleState::Delivering);
        let (handler, ingress, outbox, hub) = test_handler_with_record(Some(record.clone()));
        outbox.replay.store(true, Ordering::SeqCst);
        *outbox.replay_events.lock().unwrap() =
            encode_bundle(&result_bundle(request.clone(), bundle_id), true)?;
        let _existing = hub.subscribe_delivery(request.clone(), Arc::new(NoopDeliverySink))?;
        let original_snapshot = hub.reserve_snapshot(
            &request,
            record.result_bundle_id.as_ref().expect("bundle ID"),
        );
        assert_eq!(original_snapshot.len(), 1);

        let (server, mut client) = UnixStream::pair()?;
        let task = tokio::spawn(async move { handler.handle(server).await });
        FrameWriter::new(&mut client)
            .write_client_request(&ClientRequest::new(ClientRequestBody::Resume {
                request_id: request.clone(),
            }))
            .await?;
        timeout(Duration::from_secs(1), async {
            while hub.snapshot(&request).len() != 2 {
                tokio::task::yield_now().await;
            }
        })
        .await?;

        record.result_bundle_state = Some(BundleState::Delivered);
        *ingress.record.lock().unwrap() = Some(record);
        let mut reader = FrameReader::new(&mut client);
        let begin = timeout(Duration::from_millis(300), reader.read_server_event()).await??;
        assert!(matches!(
            begin.body,
            ServerEventBody::FinalBegin { replay: true, .. }
        ));
        while !matches!(
            reader.read_server_event().await?.body,
            ServerEventBody::FinalEnd { .. }
        ) {}
        task.await??;
        assert_eq!(outbox.replay_calls.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn failed_retryable_resume_replays_after_stale_ack_delivers() -> Result<()> {
        let request = RequestId::new();
        let bundle_id = BundleId::new();
        let mut record = request_record(
            request.clone(),
            None,
            LocalRequestState::Completed,
            Some(bundle_id.clone()),
        );
        record.result_bundle_state = Some(BundleState::FailedRetryable);
        let (handler, ingress, outbox, hub) = test_handler_with_record(Some(record.clone()));
        outbox.replay.store(true, Ordering::SeqCst);
        *outbox.replay_events.lock().unwrap() =
            encode_bundle(&result_bundle(request.clone(), bundle_id), true)?;

        let (server, mut client) = UnixStream::pair()?;
        let task = tokio::spawn(async move { handler.handle(server).await });
        FrameWriter::new(&mut client)
            .write_client_request(&ClientRequest::new(ClientRequestBody::Resume {
                request_id: request.clone(),
            }))
            .await?;
        timeout(Duration::from_secs(1), async {
            while hub.snapshot(&request).is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await?;

        record.result_bundle_state = Some(BundleState::Delivered);
        *ingress.record.lock().unwrap() = Some(record);
        let mut reader = FrameReader::new(&mut client);
        let begin = timeout(Duration::from_millis(300), reader.read_server_event()).await??;
        assert!(matches!(
            begin.body,
            ServerEventBody::FinalBegin { replay: true, .. }
        ));
        while !matches!(
            reader.read_server_event().await?.body,
            ServerEventBody::FinalEnd { .. }
        ) {}
        task.await??;
        assert_eq!(outbox.replay_calls.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn reserved_resume_never_replays_when_another_ack_wins_before_first_frame() -> Result<()>
    {
        let request = RequestId::new();
        let bundle_id = BundleId::new();
        let mut record = request_record(
            request.clone(),
            None,
            LocalRequestState::Completed,
            Some(bundle_id.clone()),
        );
        record.result_bundle_state = Some(BundleState::Delivering);
        let (handler, ingress, outbox, hub) = test_handler_with_record(Some(record.clone()));
        outbox.replay.store(true, Ordering::SeqCst);
        *outbox.replay_events.lock().unwrap() =
            encode_bundle(&result_bundle(request.clone(), bundle_id.clone()), true)?;

        let (server, mut client) = UnixStream::pair()?;
        let task = tokio::spawn(async move { handler.handle(server).await });
        FrameWriter::new(&mut client)
            .write_client_request(&ClientRequest::new(ClientRequestBody::Resume {
                request_id: request.clone(),
            }))
            .await?;
        let recipients = timeout(Duration::from_secs(1), async {
            loop {
                let recipients = hub.reserve_snapshot(&request, &bundle_id);
                if !recipients.is_empty() {
                    break recipients;
                }
                tokio::task::yield_now().await;
            }
        })
        .await?;

        record.result_bundle_state = Some(BundleState::Delivered);
        *ingress.record.lock().unwrap() = Some(record);
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(outbox.replay_calls.load(Ordering::SeqCst), 0);

        let frames = encode_bundle(&result_bundle(request.clone(), bundle_id), false)?;
        for frame in frames {
            recipients[0].send(frame).await?;
        }
        let mut reader = FrameReader::new(&mut client);
        let begin = reader.read_server_event().await?;
        assert!(matches!(
            begin.body,
            ServerEventBody::FinalBegin { replay: false, .. }
        ));
        while !matches!(
            reader.read_server_event().await?.body,
            ServerEventBody::FinalEnd { .. }
        ) {}
        assert!(
            timeout(Duration::from_millis(100), reader.read_server_event())
                .await
                .is_err()
        );
        drop(client);
        task.await??;
        assert_eq!(outbox.replay_calls.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[tokio::test]
    async fn detached_active_resume_emits_accepted_after_durable_rebind() -> Result<()> {
        let request = RequestId::new();
        let work = WorkItemId::new();
        let (handler, ingress, _, _) = test_handler_with_record(Some(request_record(
            request.clone(),
            None,
            LocalRequestState::Active,
            None,
        )));
        let (server, mut client) = UnixStream::pair()?;
        let task = tokio::spawn(async move { handler.handle(server).await });
        FrameWriter::new(&mut client)
            .write_client_request(&ClientRequest::new(ClientRequestBody::Resume {
                request_id: request.clone(),
            }))
            .await?;
        tokio::time::sleep(Duration::from_millis(20)).await;
        *ingress.record.lock().unwrap() = Some(request_record(
            request.clone(),
            Some(work.clone()),
            LocalRequestState::Active,
            None,
        ));
        let event = timeout(
            Duration::from_millis(300),
            FrameReader::new(&mut client).read_server_event(),
        )
        .await??;
        assert!(
            matches!(event.body, ServerEventBody::Accepted { work_item_id, .. } if work_item_id == work)
        );
        drop(client);
        task.await??;
        Ok(())
    }

    #[tokio::test]
    async fn detached_duplicate_submit_waits_for_rebind_then_emits_accepted() -> Result<()> {
        let request = RequestId::new();
        let work = WorkItemId::new();
        let record = request_record(request.clone(), None, LocalRequestState::Active, None);
        let (handler, ingress, _, _) = duplicate_handler(record);
        let (server, mut client) = UnixStream::pair()?;
        let task = tokio::spawn(async move { handler.handle(server).await });
        FrameWriter::new(&mut client)
            .write_client_request(&ClientRequest::new(ClientRequestBody::Submit {
                request_id: request.clone(),
                text: "same prompt".to_owned(),
            }))
            .await?;
        tokio::time::sleep(Duration::from_millis(20)).await;
        *ingress.record.lock().unwrap() = Some(request_record(
            request,
            Some(work.clone()),
            LocalRequestState::Active,
            None,
        ));
        let event = timeout(
            Duration::from_millis(300),
            FrameReader::new(&mut client).read_server_event(),
        )
        .await??;
        assert!(
            matches!(event.body, ServerEventBody::Accepted { work_item_id, .. } if work_item_id == work)
        );
        drop(client);
        task.await??;
        Ok(())
    }

    #[tokio::test]
    async fn detached_terminal_duplicate_stays_connected_for_worker_delivery() -> Result<()> {
        let request = RequestId::new();
        let record = request_record(
            request.clone(),
            None,
            LocalRequestState::Completed,
            Some(BundleId::new()),
        );
        let (handler, _, outbox, hub) = duplicate_handler(record);
        let (server, mut client) = UnixStream::pair()?;
        let task = tokio::spawn(async move { handler.handle(server).await });
        FrameWriter::new(&mut client)
            .write_client_request(&ClientRequest::new(ClientRequestBody::Submit {
                request_id: request.clone(),
                text: "same prompt".to_owned(),
            }))
            .await?;
        assert!(
            timeout(
                Duration::from_millis(50),
                FrameReader::new(&mut client).read_server_event()
            )
            .await
            .is_err()
        );
        use crate::runtime::outbox_worker::DeliveryRegistry;
        assert_eq!(hub.snapshot(&request).len(), 1);
        assert_eq!(outbox.replay_calls.load(Ordering::SeqCst), 0);
        assert!(!task.is_finished());
        drop(client);
        task.await??;
        Ok(())
    }

    #[tokio::test]
    async fn delivered_resume_uses_read_only_replay_and_closes() -> Result<()> {
        let request = RequestId::new();
        let mut record = request_record(
            request.clone(),
            None,
            LocalRequestState::Completed,
            Some(BundleId::new()),
        );
        record.result_bundle_state = Some(BundleState::Delivered);
        let (handler, _, outbox, _) = test_handler_with_record(Some(record));
        outbox.replay.store(true, Ordering::SeqCst);
        let (server, mut client) = UnixStream::pair()?;
        let task = tokio::spawn(async move { handler.handle(server).await });
        FrameWriter::new(&mut client)
            .write_client_request(&ClientRequest::new(ClientRequestBody::Resume {
                request_id: request,
            }))
            .await?;
        task.await??;
        assert_eq!(outbox.replay_calls.load(Ordering::SeqCst), 1);
        let mut byte = [0_u8; 1];
        assert_eq!(client.read(&mut byte).await?, 0);
        Ok(())
    }

    #[tokio::test]
    async fn production_handler_cannot_resume_another_actors_request() -> Result<()> {
        let store = crate::runtime::sqlite::SqliteRuntimeStore::open_in_memory().await?;
        let owner = ActorId::from_string("actor:owner");
        let other = ActorId::from_string("actor:other");
        store
            .seed_actors_for_test(
                ActorSeedSet {
                    actors: vec![
                        ActorSeed {
                            id: owner.to_string(),
                            enabled: true,
                            tools: vec![],
                            identities: vec![],
                        },
                        ActorSeed {
                            id: other.to_string(),
                            enabled: true,
                            tools: vec![],
                            identities: vec![],
                        },
                    ],
                },
                Timestamp(0),
            )
            .await?;
        let request = RequestId::new();
        store
            .submit_for_actor(
                &owner,
                LocalSubmission {
                    request_id: request.clone(),
                    text: "private".into(),
                    prompt_sha256: "a".repeat(64),
                },
                Timestamp(1),
            )
            .await?;
        let hub = Arc::new(StreamHub::default());
        let handler = ConnectionHandler {
            actor: other,
            ingress: Arc::new(store),
            outbox: Arc::new(TestOutbox::default()),
            hub: hub.clone(),
            credentials: Arc::new(SameUid),
            submissions: SubmissionRegistry::default(),
            active: ActiveConnections::default(),
            decodes: DecodeRegistry::default(),
            hooks: Arc::new(crate::runtime::hooks::NoopRuntimeBoundaryHooks),
            signals: ActorSignals::default(),
            linking: None,
        };
        let (server, mut client) = UnixStream::pair()?;
        let task = tokio::spawn(async move { handler.handle(server).await });
        FrameWriter::new(&mut client)
            .write_client_request(&ClientRequest::new(ClientRequestBody::Resume {
                request_id: request.clone(),
            }))
            .await?;
        let event = FrameReader::new(&mut client).read_server_event().await?;
        assert!(matches!(
            event.body,
            ServerEventBody::RequestError { request_id, code, .. }
                if request_id == request && code == "missing_request"
        ));
        assert!(hub.snapshot(&request).is_empty());
        task.await??;
        Ok(())
    }

    #[tokio::test]
    async fn cancel_emits_exact_cancel_accepted_then_closes() -> Result<()> {
        let request = RequestId::new();
        let cancel = CancelId::new();
        let (server, mut client) = UnixStream::pair()?;
        let task = tokio::spawn(async move { test_handler().handle(server).await });
        FrameWriter::new(&mut client)
            .write_client_request(&ClientRequest::new(ClientRequestBody::Cancel {
                request_id: request.clone(),
                cancel_id: cancel.clone(),
            }))
            .await?;
        let event = FrameReader::new(&mut client).read_server_event().await?;
        assert_eq!(
            event.body,
            ServerEventBody::CancelAccepted {
                request_id: request.clone(),
                cancel_id: cancel,
                affected_request_ids: vec![request],
            }
        );
        task.await??;
        let mut byte = [0_u8; 1];
        assert_eq!(client.read(&mut byte).await?, 0);
        Ok(())
    }

    #[tokio::test]
    async fn ack_delegates_exact_bundle_ack_then_emits_positive_response() -> Result<()> {
        let request = RequestId::new();
        let bundle = BundleId::new();
        let deliveries = vec![DeliveryId::new(), DeliveryId::new()];
        let mut handler = test_handler();
        let outbox = Arc::new(TestOutbox::default());
        handler.outbox = outbox.clone();
        let (server, mut client) = UnixStream::pair()?;
        let task = tokio::spawn(async move { handler.handle(server).await });
        FrameWriter::new(&mut client)
            .write_client_request(&ClientRequest::new(ClientRequestBody::AckFinal {
                request_id: request.clone(),
                bundle_id: bundle.clone(),
                delivery_ids: deliveries.clone(),
            }))
            .await?;
        let event = FrameReader::new(&mut client).read_server_event().await?;
        assert_eq!(
            event.body,
            ServerEventBody::AckAccepted {
                request_id: request.clone(),
                bundle_id: bundle.clone(),
            }
        );
        task.await??;
        assert_eq!(
            outbox.acks.lock().unwrap().as_slice(),
            &[BundleAck {
                actor_id: ActorId::from_string("actor:local:test"),
                request_id: request,
                bundle_id: bundle,
                delivery_ids: deliveries,
            }]
        );
        let mut byte = [0_u8; 1];
        assert_eq!(client.read(&mut byte).await?, 0);
        Ok(())
    }

    #[tokio::test]
    async fn ack_authority_failure_propagates_without_request_error() -> Result<()> {
        let request = RequestId::new();
        let bundle = BundleId::new();
        let mut handler = test_handler();
        let outbox = Arc::new(TestOutbox::default());
        *outbox.ack_error.lock().unwrap() = Some("commit failed".into());
        handler.outbox = outbox;
        let (server, mut client) = UnixStream::pair()?;
        let task = tokio::spawn(async move { handler.handle(server).await });
        FrameWriter::new(&mut client)
            .write_client_request(&ClientRequest::new(ClientRequestBody::AckFinal {
                request_id: request.clone(),
                bundle_id: bundle,
                delivery_ids: vec![DeliveryId::new()],
            }))
            .await?;
        assert!(task.await?.is_err());
        let mut byte = [0_u8; 1];
        assert_eq!(client.read(&mut byte).await?, 0);
        Ok(())
    }

    #[tokio::test]
    async fn ack_validation_rejection_stays_request_scoped() -> Result<()> {
        let request = RequestId::new();
        let mut handler = test_handler();
        let outbox = Arc::new(TestOutbox::default());
        outbox.ack_rejected.store(true, Ordering::SeqCst);
        handler.outbox = outbox;
        let (server, mut client) = UnixStream::pair()?;
        let task = tokio::spawn(async move { handler.handle(server).await });
        FrameWriter::new(&mut client)
            .write_client_request(&ClientRequest::new(ClientRequestBody::AckFinal {
                request_id: request.clone(),
                bundle_id: BundleId::new(),
                delivery_ids: vec![DeliveryId::new()],
            }))
            .await?;
        let event = FrameReader::new(&mut client).read_server_event().await?;
        assert!(matches!(
            event.body,
            ServerEventBody::RequestError { request_id, code, .. }
                if request_id == request && code == "ack_failed"
        ));
        task.await??;
        Ok(())
    }

    #[tokio::test]
    async fn server_run_propagates_submit_authority_failure() -> Result<()> {
        let root = short_temp().join(format!("authority-{}", uuid::Uuid::new_v4()));
        create_secure_directory(&root)?;
        let socket = root.join("c.sock");
        let listener = bind_secure_listener(&socket)?;
        let server = LocalIpcServer::with_credentials(
            listener,
            ActorId::from_string("actor:local:test"),
            Arc::new(AuthorityIngress),
            Arc::new(TestOutbox::default()),
            Arc::new(StreamHub::default()),
            Arc::new(SameUid),
        );
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let mut task = tokio::spawn(server.run(shutdown_rx));
        let mut client = UnixStream::connect(&socket).await?;
        FrameWriter::new(&mut client)
            .write_client_request(&ClientRequest::new(ClientRequestBody::Submit {
                request_id: RequestId::new(),
                text: "hello".into(),
            }))
            .await?;
        let result = timeout(Duration::from_millis(200), &mut task).await;
        if result.is_err() {
            shutdown_tx.send_replace(true);
            task.await??;
        }
        let error = result
            .expect("server swallowed handler authority failure")?
            .expect_err("server unexpectedly succeeded");
        assert!(error.to_string().contains("authority failure"));
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[tokio::test]
    async fn server_run_propagates_connection_handler_panic() -> Result<()> {
        let root = short_temp().join(format!("panic-{}", uuid::Uuid::new_v4()));
        create_secure_directory(&root)?;
        let socket = root.join("c.sock");
        let listener = bind_secure_listener(&socket)?;
        let handler = test_handler();
        let server = LocalIpcServer::with_credentials(
            listener,
            handler.actor,
            handler.ingress,
            handler.outbox,
            handler.hub,
            Arc::new(PanicCredentials),
        );
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let task = tokio::spawn(server.run(shutdown_rx));
        let _client = UnixStream::connect(&socket).await?;
        let error = timeout(Duration::from_millis(200), task)
            .await
            .expect("server swallowed handler panic")?
            .expect_err("server unexpectedly succeeded");
        assert!(error.to_string().contains("handler task failed"));
        std::fs::remove_dir_all(root)?;
        Ok(())
    }
}
