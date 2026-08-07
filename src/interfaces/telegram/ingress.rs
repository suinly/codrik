use std::sync::Arc;

use anyhow::{Result, bail};
use async_trait::async_trait;

use crate::{
    interfaces::telegram::{
        api::{GetFile, TelegramIngressApi},
        types::{TelegramInbound, TelegramUpdate},
    },
    runtime::{
        attachments::{RuntimeAttachmentStore, TELEGRAM_MAX_DOWNLOAD_BYTES},
        gateway::{GatewayCommandKey, NewGatewayDelivery},
        identity_link::{IdentityLinkManager, LinkRedemption},
        model::{ActorId, Clock},
        signals::ActorSignals,
        store::{
            ActorStore, GatewayDeliveryStore, IngressOutcome, IngressStore, NewInboundEvent,
            OutboxPayload,
        },
    },
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TelegramIngressOutcome {
    Accepted { actor_id: ActorId, sequence: i64 },
    Duplicate,
    CommandHandled,
    Unsupported,
}

#[async_trait]
pub trait TelegramIngress: Send + Sync + 'static {
    async fn handle(&self, update: TelegramUpdate) -> Result<TelegramIngressOutcome>;
}

pub struct TelegramIngressService<S, C> {
    store: S,
    linking: Arc<dyn IdentityLinkManager>,
    signals: ActorSignals,
    bot_id: String,
    bot_username: String,
    clock: C,
    attachment_api: Option<Arc<dyn TelegramIngressApi>>,
    attachments: Option<RuntimeAttachmentStore>,
}

impl<S, C> TelegramIngressService<S, C>
where
    S: ActorStore + IngressStore + GatewayDeliveryStore + Clone,
    C: Clock,
{
    pub fn new(
        store: S,
        linking: Arc<dyn IdentityLinkManager>,
        signals: ActorSignals,
        bot_id: impl Into<String>,
        bot_username: impl Into<String>,
        clock: C,
    ) -> Result<Self> {
        let bot_id = bot_id.into();
        let bot_username = bot_username.into();
        if bot_id.trim().is_empty() || bot_username.trim().is_empty() {
            bail!("Telegram bot identity must not be blank");
        }
        Ok(Self {
            store,
            linking,
            signals,
            bot_id,
            bot_username,
            clock,
            attachment_api: None,
            attachments: None,
        })
    }

    pub fn with_attachment_ingress(
        mut self,
        api: Arc<dyn TelegramIngressApi>,
        attachments: RuntimeAttachmentStore,
    ) -> Self {
        self.attachment_api = Some(api);
        self.attachments = Some(attachments);
        self
    }

    async fn enqueue_response(
        &self,
        update_id: i64,
        route: crate::runtime::gateway::DeliveryRoute,
        text: &str,
    ) -> Result<()> {
        self.store
            .enqueue_gateway_delivery(
                NewGatewayDelivery::new(
                    format!("gateway-response:telegram:{}:{update_id}", self.bot_id),
                    None,
                    0,
                    route,
                    OutboxPayload::Text { text: text.into() },
                )?,
                self.clock.now(),
            )
            .await?;
        Ok(())
    }
}

#[async_trait]
impl<S, C> TelegramIngress for TelegramIngressService<S, C>
where
    S: ActorStore + IngressStore + GatewayDeliveryStore + Clone + Send + Sync + 'static,
    C: Clock,
{
    async fn handle(&self, update: TelegramUpdate) -> Result<TelegramIngressOutcome> {
        let update_id = update.update_id;
        match update.classify(&self.bot_id, &self.bot_username)? {
            TelegramInbound::Unsupported => Ok(TelegramIngressOutcome::Unsupported),
            TelegramInbound::Attachment {
                caption,
                attachment,
                identity,
                route,
            } => {
                let Some(actor) = self
                    .store
                    .resolve_identity(&identity.provider, &identity.subject)
                    .await?
                else {
                    self.enqueue_response(
                        update_id,
                        route,
                        "This channel is not linked. Run `codrik link`, then send `/link CODE` here.",
                    )
                    .await?;
                    return Ok(TelegramIngressOutcome::CommandHandled);
                };
                if !actor.enabled {
                    self.enqueue_response(update_id, route, "This actor is disabled.")
                        .await?;
                    return Ok(TelegramIngressOutcome::CommandHandled);
                }
                let Some(api) = &self.attachment_api else {
                    return Ok(TelegramIngressOutcome::Unsupported);
                };
                let Some(attachments) = &self.attachments else {
                    return Ok(TelegramIngressOutcome::Unsupported);
                };
                let stored = async {
                    if attachment
                        .file_size
                        .is_some_and(|size| size > TELEGRAM_MAX_DOWNLOAD_BYTES)
                    {
                        bail!("Telegram file exceeds hosted download limit")
                    }
                    let remote = api
                        .get_file(GetFile {
                            file_id: attachment.file_id,
                        })
                        .await?;
                    if remote
                        .file_size
                        .is_some_and(|size| size > TELEGRAM_MAX_DOWNLOAD_BYTES)
                    {
                        bail!("Telegram file exceeds hosted download limit")
                    }
                    let file_path = remote
                        .file_path
                        .filter(|path| !path.trim().is_empty())
                        .ok_or_else(|| anyhow::anyhow!("Telegram getFile omitted file_path"))?;
                    let stream = api.download_file(&file_path).await?;
                    attachments
                        .store_stream(&actor.id, &attachment.display_name, stream)
                        .await
                }
                .await;
                let stored = match stored {
                    Ok(stored) => stored,
                    Err(_) => {
                        self.enqueue_response(
                            update_id,
                            route,
                            "Could not receive this file. Telegram files must be 20 MB or smaller.",
                        )
                        .await?;
                        return Ok(TelegramIngressOutcome::CommandHandled);
                    }
                };
                match self
                    .store
                    .ingest(
                        NewInboundEvent::attachment_with_route(
                            format!("telegram:{}", self.bot_id),
                            update_id.to_string(),
                            identity.provider,
                            identity.subject,
                            crate::runtime::model::Audience::ActorPrivate,
                            route,
                            caption,
                            stored,
                        )?
                        .with_latest_telegram_route_tracking(),
                        self.clock.now(),
                    )
                    .await?
                {
                    IngressOutcome::Accepted { sequence, .. } => {
                        self.signals.notify(&actor.id, sequence).await;
                        Ok(TelegramIngressOutcome::Accepted {
                            actor_id: actor.id,
                            sequence,
                        })
                    }
                    IngressOutcome::Duplicate { .. } => Ok(TelegramIngressOutcome::Duplicate),
                    IngressOutcome::Unauthorized => {
                        bail!("Telegram identity became unauthorized during ingress")
                    }
                }
            }
            TelegramInbound::Link {
                code,
                identity,
                route,
            } => {
                let text = if let Some(code) = code {
                    match self
                        .linking
                        .redeem_code_once(
                            GatewayCommandKey {
                                gateway: format!("telegram:{}", self.bot_id),
                                external_id: update_id.to_string(),
                            },
                            identity,
                            &code,
                        )
                        .await?
                    {
                        LinkRedemption::Linked { .. } => "This channel is now linked.",
                        LinkRedemption::AlreadyLinked { .. } => "This channel was already linked.",
                        LinkRedemption::InvalidOrExpired => "Invalid or expired link code.",
                        LinkRedemption::RateLimited { .. } => {
                            "Too many failed attempts. Try again later."
                        }
                        LinkRedemption::IdentityConflict => {
                            "This channel is already linked to another actor."
                        }
                    }
                } else {
                    "This channel is not linked. Run `codrik link`, then send `/link CODE` here."
                };
                self.enqueue_response(update_id, route, text).await?;
                Ok(TelegramIngressOutcome::CommandHandled)
            }
            TelegramInbound::Text {
                text,
                identity,
                route,
            } => {
                let Some(actor) = self
                    .store
                    .resolve_identity(&identity.provider, &identity.subject)
                    .await?
                else {
                    self.enqueue_response(
                        update_id,
                        route,
                        "This channel is not linked. Run `codrik link`, then send `/link CODE` here.",
                    )
                    .await?;
                    return Ok(TelegramIngressOutcome::CommandHandled);
                };
                if !actor.enabled {
                    self.enqueue_response(update_id, route, "This actor is disabled.")
                        .await?;
                    return Ok(TelegramIngressOutcome::CommandHandled);
                }
                match self
                    .store
                    .ingest(
                        NewInboundEvent::text_with_route(
                            format!("telegram:{}", self.bot_id),
                            update_id.to_string(),
                            identity.provider,
                            identity.subject,
                            crate::runtime::model::Audience::ActorPrivate,
                            route,
                            text,
                        )?
                        .with_latest_telegram_route_tracking(),
                        self.clock.now(),
                    )
                    .await?
                {
                    IngressOutcome::Accepted { sequence, .. } => {
                        self.signals.notify(&actor.id, sequence).await;
                        Ok(TelegramIngressOutcome::Accepted {
                            actor_id: actor.id,
                            sequence,
                        })
                    }
                    IngressOutcome::Duplicate { .. } => Ok(TelegramIngressOutcome::Duplicate),
                    IngressOutcome::Unauthorized => {
                        bail!("Telegram identity became unauthorized during ingress")
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        convert::Infallible,
        path::PathBuf,
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use anyhow::Result;
    use async_trait::async_trait;
    use bytes::Bytes;
    use futures_util::stream;

    use super::{TelegramIngress, TelegramIngressOutcome, TelegramIngressService};
    use crate::{
        interfaces::telegram::{
            api::{
                GetFile, TelegramApiError, TelegramDownloadStream, TelegramFile, TelegramIngressApi,
            },
            types::{TelegramBot, TelegramUpdate},
        },
        runtime::{
            attachments::{RuntimeAttachmentStore, TELEGRAM_MAX_DOWNLOAD_BYTES},
            identity_link::{IdentityLinkManager, IdentityLinkService, SystemLinkCodeGenerator},
            model::{ActorId, ManualClock, Timestamp},
            signals::ActorSignals,
            sqlite::SqliteRuntimeStore,
            store::{
                ActorAdminStore, ActorStore, DispatchStore, FailureDisposition, FailureFence,
                FailureStore, GatewayDeliveryStore, NewWebhookEvent, OutboxPayload,
                QuantumProgress, WebhookIdempotency, WebhookIngressOutcome, WebhookIngressStore,
            },
        },
    };

    #[derive(Clone, Default)]
    struct AttachmentApi {
        get_file_calls: Arc<AtomicUsize>,
        download_calls: Arc<AtomicUsize>,
        returned_size: Arc<Mutex<Option<u64>>>,
        bytes: Arc<Mutex<Bytes>>,
    }

    #[async_trait]
    impl TelegramIngressApi for AttachmentApi {
        async fn get_me(&self) -> std::result::Result<TelegramBot, TelegramApiError> {
            unreachable!()
        }

        async fn set_webhook(
            &self,
            _command: crate::interfaces::telegram::api::SetWebhook,
        ) -> std::result::Result<(), TelegramApiError> {
            unreachable!()
        }

        async fn delete_webhook(
            &self,
            _command: crate::interfaces::telegram::api::DeleteWebhook,
        ) -> std::result::Result<(), TelegramApiError> {
            unreachable!()
        }

        async fn get_webhook_info(
            &self,
        ) -> std::result::Result<crate::interfaces::telegram::api::WebhookInfo, TelegramApiError>
        {
            unreachable!()
        }

        async fn get_updates(
            &self,
            _command: crate::interfaces::telegram::api::GetUpdates,
        ) -> std::result::Result<Vec<TelegramUpdate>, TelegramApiError> {
            unreachable!()
        }

        async fn get_file(
            &self,
            command: GetFile,
        ) -> std::result::Result<TelegramFile, TelegramApiError> {
            self.get_file_calls.fetch_add(1, Ordering::SeqCst);
            Ok(TelegramFile {
                file_id: command.file_id,
                file_unique_id: "unique".into(),
                file_size: *self.returned_size.lock().unwrap(),
                file_path: Some("documents/sample.png".into()),
            })
        }

        async fn download_file(
            &self,
            _file_path: &str,
        ) -> std::result::Result<TelegramDownloadStream, TelegramApiError> {
            self.download_calls.fetch_add(1, Ordering::SeqCst);
            Ok(Box::pin(stream::iter([Ok::<_, TelegramApiError>(
                self.bytes.lock().unwrap().clone(),
            )])))
        }
    }

    fn temp_attachment_root(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "codrik-telegram-ingress-{}-{name}",
            std::process::id()
        ))
    }

    #[tokio::test]
    async fn linked_attachment_is_stored_and_accepted() -> Result<()> {
        let root = temp_attachment_root("accepted");
        tokio::fs::remove_dir_all(&root).await.ok();
        let store = SqliteRuntimeStore::open_in_memory().await?;
        let actor = ActorId::parse_workspace_safe("alice")?;
        store
            .ensure_initial_actor(&actor, &[], Timestamp(1))
            .await?;
        let manager: Arc<dyn IdentityLinkManager> = Arc::new(IdentityLinkService::new(
            store.clone(),
            ManualClock::new(10),
            SystemLinkCodeGenerator,
        ));
        let code = manager.issue_code(&actor).await?.code;
        let api = AttachmentApi {
            returned_size: Arc::new(Mutex::new(Some(16))),
            bytes: Arc::new(Mutex::new(Bytes::from_static(
                b"\x89PNG\r\n\x1a\n\0\0\0\rIHDR",
            ))),
            ..AttachmentApi::default()
        };
        let ingress = TelegramIngressService::new(
            store.clone(),
            manager,
            ActorSignals::default(),
            "900",
            "codrik_bot",
            ManualClock::new(20),
        )?
        .with_attachment_ingress(Arc::new(api), RuntimeAttachmentStore::new(&root));
        ingress.handle(serde_json::from_value(serde_json::json!({
            "update_id": 1,
            "message": {"message_id":1,"from":{"id":100,"is_bot":false},"chat":{"id":100,"type":"private"},"text":format!("/link {code}")}
        }))?).await?;

        let outcome = ingress.handle(serde_json::from_value(serde_json::json!({
            "update_id": 2,
            "message": {"message_id":2,"from":{"id":100,"is_bot":false},"chat":{"id":100,"type":"private"},"caption":"inspect","document":{"file_id":"file","file_unique_id":"unique","file_name":"sample.png","file_size":16}}
        }))?).await?;

        assert!(matches!(
            outcome,
            TelegramIngressOutcome::Accepted { sequence: 1, .. }
        ));
        assert!(tokio::fs::try_exists(root.join("alice")).await?);
        tokio::fs::remove_dir_all(root).await.ok();
        Ok(())
    }

    #[tokio::test]
    async fn unlinked_attachment_does_not_call_telegram_file_api() -> Result<()> {
        let root = temp_attachment_root("unlinked");
        tokio::fs::remove_dir_all(&root).await.ok();
        let store = SqliteRuntimeStore::open_in_memory().await?;
        let manager: Arc<dyn IdentityLinkManager> = Arc::new(IdentityLinkService::new(
            store.clone(),
            ManualClock::new(10),
            SystemLinkCodeGenerator,
        ));
        let api = AttachmentApi::default();
        let ingress = TelegramIngressService::new(
            store,
            manager,
            ActorSignals::default(),
            "900",
            "codrik_bot",
            ManualClock::new(20),
        )?
        .with_attachment_ingress(Arc::new(api.clone()), RuntimeAttachmentStore::new(&root));

        let outcome = ingress.handle(serde_json::from_value(serde_json::json!({
            "update_id": 2,
            "message": {"message_id":2,"from":{"id":100,"is_bot":false},"chat":{"id":100,"type":"private"},"document":{"file_id":"file","file_unique_id":"unique","file_name":"sample.png","file_size":16}}
        }))?).await?;

        assert_eq!(outcome, TelegramIngressOutcome::CommandHandled);
        assert_eq!(api.get_file_calls.load(Ordering::SeqCst), 0);
        assert_eq!(api.download_calls.load(Ordering::SeqCst), 0);
        assert!(!tokio::fs::try_exists(&root).await?);
        Ok(())
    }

    #[tokio::test]
    async fn declared_oversize_attachment_skips_telegram_file_api() -> Result<()> {
        let root = temp_attachment_root("declared-oversize");
        tokio::fs::remove_dir_all(&root).await.ok();
        let store = SqliteRuntimeStore::open_in_memory().await?;
        let actor = ActorId::parse_workspace_safe("alice")?;
        store
            .ensure_initial_actor(&actor, &[], Timestamp(1))
            .await?;
        let manager: Arc<dyn IdentityLinkManager> = Arc::new(IdentityLinkService::new(
            store.clone(),
            ManualClock::new(10),
            SystemLinkCodeGenerator,
        ));
        let code = manager.issue_code(&actor).await?.code;
        let api = AttachmentApi::default();
        let ingress = TelegramIngressService::new(
            store,
            manager,
            ActorSignals::default(),
            "900",
            "codrik_bot",
            ManualClock::new(20),
        )?
        .with_attachment_ingress(Arc::new(api.clone()), RuntimeAttachmentStore::new(&root));
        ingress.handle(serde_json::from_value(serde_json::json!({
            "update_id": 1,
            "message": {"message_id":1,"from":{"id":100,"is_bot":false},"chat":{"id":100,"type":"private"},"text":format!("/link {code}")}
        }))?).await?;

        let outcome = ingress.handle(serde_json::from_value(serde_json::json!({
            "update_id": 2,
            "message": {"message_id":2,"from":{"id":100,"is_bot":false},"chat":{"id":100,"type":"private"},"document":{"file_id":"file","file_unique_id":"unique","file_name":"large.bin","file_size":20000001}}
        }))?).await?;

        assert_eq!(outcome, TelegramIngressOutcome::CommandHandled);
        assert_eq!(api.get_file_calls.load(Ordering::SeqCst), 0);
        assert_eq!(api.download_calls.load(Ordering::SeqCst), 0);
        assert!(!tokio::fs::try_exists(&root).await?);
        Ok(())
    }

    #[tokio::test]
    async fn disabled_attachment_does_not_call_telegram_file_api() -> Result<()> {
        let root = temp_attachment_root("disabled");
        tokio::fs::remove_dir_all(&root).await.ok();
        let store = SqliteRuntimeStore::open_in_memory().await?;
        let actor = ActorId::parse_workspace_safe("alice")?;
        store
            .ensure_initial_actor(&actor, &[], Timestamp(1))
            .await?;
        let manager: Arc<dyn IdentityLinkManager> = Arc::new(IdentityLinkService::new(
            store.clone(),
            ManualClock::new(10),
            SystemLinkCodeGenerator,
        ));
        let code = manager.issue_code(&actor).await?.code;
        let api = AttachmentApi::default();
        let ingress = TelegramIngressService::new(
            store.clone(),
            manager,
            ActorSignals::default(),
            "900",
            "codrik_bot",
            ManualClock::new(20),
        )?
        .with_attachment_ingress(Arc::new(api.clone()), RuntimeAttachmentStore::new(&root));
        ingress.handle(serde_json::from_value(serde_json::json!({
            "update_id": 1,
            "message": {"message_id":1,"from":{"id":100,"is_bot":false},"chat":{"id":100,"type":"private"},"text":format!("/link {code}")}
        }))?).await?;
        store.set_actor_enabled(&actor, false).await?;

        let outcome = ingress.handle(serde_json::from_value(serde_json::json!({
            "update_id": 2,
            "message": {"message_id":2,"from":{"id":100,"is_bot":false},"chat":{"id":100,"type":"private"},"document":{"file_id":"file","file_unique_id":"unique","file_name":"sample.bin","file_size":16}}
        }))?).await?;

        assert_eq!(outcome, TelegramIngressOutcome::CommandHandled);
        assert_eq!(api.get_file_calls.load(Ordering::SeqCst), 0);
        assert_eq!(api.download_calls.load(Ordering::SeqCst), 0);
        assert!(!tokio::fs::try_exists(&root).await?);
        Ok(())
    }

    #[tokio::test]
    async fn enforces_returned_and_streamed_file_size_limits() -> Result<()> {
        let root = temp_attachment_root("size-limits");
        tokio::fs::remove_dir_all(&root).await.ok();
        let store = SqliteRuntimeStore::open_in_memory().await?;
        let actor = ActorId::parse_workspace_safe("alice")?;
        store
            .ensure_initial_actor(&actor, &[], Timestamp(1))
            .await?;
        let manager: Arc<dyn IdentityLinkManager> = Arc::new(IdentityLinkService::new(
            store.clone(),
            ManualClock::new(10),
            SystemLinkCodeGenerator,
        ));
        let code = manager.issue_code(&actor).await?.code;
        let api = AttachmentApi {
            returned_size: Arc::new(Mutex::new(Some(TELEGRAM_MAX_DOWNLOAD_BYTES + 1))),
            ..AttachmentApi::default()
        };
        let ingress = TelegramIngressService::new(
            store,
            manager,
            ActorSignals::default(),
            "900",
            "codrik_bot",
            ManualClock::new(20),
        )?
        .with_attachment_ingress(Arc::new(api.clone()), RuntimeAttachmentStore::new(&root));
        ingress.handle(serde_json::from_value(serde_json::json!({
            "update_id": 1,
            "message": {"message_id":1,"from":{"id":100,"is_bot":false},"chat":{"id":100,"type":"private"},"text":format!("/link {code}")}
        }))?).await?;

        let attachment = |update_id, file_size| {
            serde_json::json!({
                "update_id": update_id,
                "message": {"message_id":update_id,"from":{"id":100,"is_bot":false},"chat":{"id":100,"type":"private"},"document":{"file_id":"file","file_unique_id":format!("unique-{update_id}"),"file_name":"sample.bin","file_size":file_size}}
            })
        };
        assert_eq!(
            ingress
                .handle(serde_json::from_value(attachment(2, 16))?)
                .await?,
            TelegramIngressOutcome::CommandHandled
        );
        assert_eq!(api.download_calls.load(Ordering::SeqCst), 0);

        *api.returned_size.lock().unwrap() = Some(TELEGRAM_MAX_DOWNLOAD_BYTES);
        *api.bytes.lock().unwrap() = Bytes::from(vec![b'x'; TELEGRAM_MAX_DOWNLOAD_BYTES as usize]);
        assert!(matches!(
            ingress
                .handle(serde_json::from_value(attachment(
                    3,
                    TELEGRAM_MAX_DOWNLOAD_BYTES
                ))?)
                .await?,
            TelegramIngressOutcome::Accepted { sequence: 1, .. }
        ));

        *api.returned_size.lock().unwrap() = None;
        *api.bytes.lock().unwrap() =
            Bytes::from(vec![b'x'; TELEGRAM_MAX_DOWNLOAD_BYTES as usize + 1]);
        assert_eq!(
            ingress
                .handle(serde_json::from_value(attachment(4, 16))?)
                .await?,
            TelegramIngressOutcome::CommandHandled
        );
        assert_eq!(api.get_file_calls.load(Ordering::SeqCst), 3);
        assert_eq!(api.download_calls.load(Ordering::SeqCst), 2);
        let mut files = tokio::fs::read_dir(root.join("alice")).await?;
        assert!(files.next_entry().await?.is_some());
        assert!(files.next_entry().await?.is_none());
        tokio::fs::remove_dir_all(root).await.ok();
        Ok(())
    }

    #[tokio::test]
    async fn link_command_uses_idempotent_core_and_enqueues_response_without_agent_work()
    -> Result<()> {
        let store = SqliteRuntimeStore::open_in_memory().await?;
        let actor = ActorId::from_string("owner");
        store
            .ensure_initial_actor(&actor, &[], Timestamp(1))
            .await?;
        let manager: Arc<dyn IdentityLinkManager> = Arc::new(IdentityLinkService::new(
            store.clone(),
            ManualClock::new(10),
            SystemLinkCodeGenerator,
        ));
        let issued = manager.issue_code(&actor).await?;
        let ingress = TelegramIngressService::new(
            store.clone(),
            manager,
            ActorSignals::default(),
            "900",
            "codrik_bot",
            ManualClock::new(20),
        )?;
        let update: TelegramUpdate = serde_json::from_value(serde_json::json!({
            "update_id": 42,
            "message": {
                "message_id": 7,
                "from": {"id": 100, "is_bot": false, "username": "owner"},
                "chat": {"id": 100, "type": "private"},
                "text": format!("/link {}", issued.code)
            }
        }))?;
        assert_eq!(
            ingress.handle(update).await?,
            TelegramIngressOutcome::CommandHandled
        );
        assert_eq!(
            store
                .resolve_identity("telegram:900", "100")
                .await?
                .unwrap()
                .id,
            actor
        );
        let deliveries = store
            .claim_gateway_deliveries("telegram:900", "test", Timestamp(21), Timestamp(51), 10)
            .await?;
        assert_eq!(deliveries.len(), 1);
        assert_eq!(
            deliveries[0].payload,
            OutboxPayload::Text {
                text: "This channel is now linked.".into()
            }
        );
        Ok(())
    }

    #[tokio::test]
    async fn disabled_actor_text_is_rejected_without_ingress_or_signal() -> Result<()> {
        let store = SqliteRuntimeStore::open_in_memory().await?;
        let actor = ActorId::parse_workspace_safe("alice")?;
        store
            .ensure_initial_actor(&actor, &[], Timestamp(1))
            .await?;
        let manager: Arc<dyn IdentityLinkManager> = Arc::new(IdentityLinkService::new(
            store.clone(),
            ManualClock::new(10),
            SystemLinkCodeGenerator,
        ));
        let issued = manager.issue_code(&actor).await?;
        let signals = ActorSignals::default();
        let signal = signals.subscribe(&actor).await;
        let ingress = TelegramIngressService::new(
            store.clone(),
            manager,
            signals,
            "900",
            "codrik_bot",
            ManualClock::new(20),
        )?;
        ingress
            .handle(serde_json::from_value(serde_json::json!({
                "update_id": 1,
                "message": {
                    "message_id": 1,
                    "from": {"id": 100, "is_bot": false},
                    "chat": {"id": 100, "type": "private"},
                    "text": format!("/link {}", issued.code)
                }
            }))?)
            .await?;
        store.set_actor_enabled(&actor, false).await?;

        assert_eq!(
            ingress
                .handle(serde_json::from_value(serde_json::json!({
                    "update_id": 2,
                    "message": {
                        "message_id": 2,
                        "from": {"id": 100, "is_bot": false},
                        "chat": {"id": 100, "type": "private"},
                        "text": "hello"
                    }
                }))?)
                .await?,
            TelegramIngressOutcome::CommandHandled
        );
        assert_eq!(*signal.borrow(), 0);
        assert!(!store.actor_details(&actor).await?.unwrap().has_active_work);
        let mut deliveries = store
            .claim_gateway_deliveries("telegram:900", "test", Timestamp(21), Timestamp(51), 10)
            .await?;
        for delivery in &deliveries {
            store
                .complete_gateway_delivery(&delivery.claim, Some("sent".into()), Timestamp(22))
                .await?;
        }
        deliveries.extend(
            store
                .claim_gateway_deliveries("telegram:900", "test", Timestamp(23), Timestamp(53), 10)
                .await?,
        );
        assert!(deliveries.iter().any(|delivery| {
            delivery.payload
                == OutboxPayload::Text {
                    text: "This actor is disabled.".into(),
                }
        }));
        Ok(())
    }

    #[tokio::test]
    async fn accepted_private_text_records_route_for_later_webhooks() -> Result<()> {
        let store = SqliteRuntimeStore::open_in_memory().await?;
        let actor = ActorId::from_string("owner");
        store
            .ensure_initial_actor(&actor, &[], Timestamp(1))
            .await?;
        let manager: Arc<dyn IdentityLinkManager> = Arc::new(IdentityLinkService::new(
            store.clone(),
            ManualClock::new(10),
            SystemLinkCodeGenerator,
        ));
        let code = manager.issue_code(&actor).await?.code;
        let ingress = TelegramIngressService::new(
            store.clone(),
            manager,
            ActorSignals::default(),
            "900",
            "codrik_bot",
            ManualClock::new(20),
        )?;
        for (update_id, text) in [(1, format!("/link {code}")), (2, "hello".into())] {
            ingress
                .handle(serde_json::from_value(serde_json::json!({
                    "update_id": update_id,
                    "message": {
                        "message_id": update_id,
                        "from": {"id": 100, "is_bot": false},
                        "chat": {"id": 100, "type": "private"},
                        "text": text
                    }
                }))?)
                .await?;
        }

        assert!(matches!(
            store
                .ingest_webhook(
                    NewWebhookEvent {
                        endpoint: "grafana".into(),
                        actor_id: actor,
                        idempotency: WebhookIdempotency::Explicit([7; 32]),
                        payload_json: "{}".into(),
                    },
                    Timestamp(21),
                )
                .await?,
            WebhookIngressOutcome::Accepted {
                route_snapshotted: true,
                ..
            }
        ));
        Ok(())
    }

    #[tokio::test]
    async fn nonaccepted_inputs_do_not_release_deferred_webhook_result() -> Result<()> {
        let store = SqliteRuntimeStore::open_in_memory().await?;
        let actor = ActorId::from_string("owner");
        store
            .ensure_initial_actor(&actor, &[], Timestamp(1))
            .await?;
        store
            .ingest_webhook(
                NewWebhookEvent {
                    endpoint: "grafana".into(),
                    actor_id: actor.clone(),
                    idempotency: WebhookIdempotency::Explicit([8; 32]),
                    payload_json: r#"{"type":"webhook","source":"grafana","received_at":"1970-01-01T00:00:00.001Z","data":{}}"#.into(),
                },
                Timestamp(2),
            )
            .await?;
        let lease = store
            .acquire_ready_actor("worker", Timestamp(3), Timestamp(100))
            .await?
            .unwrap();
        let run = store
            .attach_next_run(&lease, 8, Timestamp(4))
            .await?
            .unwrap();
        let fence = FailureFence::from(&run);
        for attempt in 0..5 {
            let disposition = store
                .record_failure(
                    &fence,
                    "terminal webhook",
                    QuantumProgress::None,
                    &ManualClock::new(5 + attempt),
                )
                .await?;
            if attempt == 4 {
                assert_eq!(disposition, FailureDisposition::Terminalized);
            }
        }
        store.release_lease(&lease).await?;

        let manager: Arc<dyn IdentityLinkManager> = Arc::new(IdentityLinkService::new(
            store.clone(),
            ManualClock::new(20),
            SystemLinkCodeGenerator,
        ));
        let code = manager.issue_code(&actor).await?.code;
        let ingress = TelegramIngressService::new(
            store.clone(),
            manager,
            ActorSignals::default(),
            "900",
            "codrik_bot",
            ManualClock::new(30),
        )?;
        for update in [
            serde_json::json!({"update_id": 10, "inline_query": {"id": "x", "from": {"id": 100, "is_bot": false}, "query": "ignored", "offset": ""}}),
            serde_json::json!({"update_id": 11, "message": {"message_id": 11, "from": {"id": 100, "is_bot": false}, "chat": {"id": 100, "type": "private"}, "text": "unlinked"}}),
            serde_json::json!({"update_id": 12, "message": {"message_id": 12, "from": {"id": 100, "is_bot": false}, "chat": {"id": 100, "type": "private"}, "text": format!("/link {code}")}}),
        ] {
            ingress.handle(serde_json::from_value(update)?).await?;
        }
        store.set_actor_enabled(&actor, false).await?;
        ingress
            .handle(serde_json::from_value(serde_json::json!({
                "update_id": 13,
                "message": {
                    "message_id": 13,
                    "from": {"id": 100, "is_bot": false},
                    "chat": {"id": 100, "type": "private"},
                    "text": "disabled"
                }
            }))?)
            .await?;
        store.set_actor_enabled(&actor, true).await?;

        assert!(matches!(
            store
                .ingest_webhook(
                    NewWebhookEvent {
                        endpoint: "grafana".into(),
                        actor_id: actor,
                        idempotency: WebhookIdempotency::Explicit([9; 32]),
                        payload_json: "{}".into(),
                    },
                    Timestamp(40),
                )
                .await?,
            WebhookIngressOutcome::Accepted {
                route_snapshotted: false,
                ..
            }
        ));
        let deliveries = store
            .claim_gateway_deliveries("telegram:900", "test", Timestamp(41), Timestamp(71), 20)
            .await?;
        assert!(deliveries.iter().all(|delivery| {
            delivery.payload
                != (OutboxPayload::Text {
                    text: "terminal webhook".into(),
                })
        }));
        Ok(())
    }
}
