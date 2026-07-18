pub mod activity;
pub mod api;
pub mod delivery;
pub mod ingress;
pub mod polling;
pub mod types;
pub mod webhook;

use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};

use anyhow::{Context, Result, bail};
use tokio::{net::TcpListener, sync::watch};

use crate::{
    config::{ValidatedTelegramConfig, ValidatedTelegramIngressConfig},
    interfaces::telegram::{
        activity::TelegramActivityWorker,
        api::{DeleteWebhook, ReqwestTelegramApi, SetWebhook, TelegramApi, TelegramIngressApi},
        delivery::TelegramDeliveryWorker,
        ingress::TelegramIngressService,
        polling::TelegramPollingWorker,
        webhook::{SecretToken, TelegramWebhookServer},
    },
    runtime::{
        gateway_activity::GatewayActivityHub,
        identity_link::IdentityLinkManager,
        model::Clock,
        signals::ActorSignals,
        store::{ActorStore, GatewayDeliveryStore, IngressStore},
    },
};

enum PreparedTelegramIngress {
    Webhook {
        listener: Mutex<Option<TcpListener>>,
        public_url: url::Url,
        webhook_secret: String,
    },
    Polling,
}

pub struct PreparedTelegramGateway<S, A, C> {
    transport: PreparedTelegramIngress,
    ingress: Arc<TelegramIngressService<S, C>>,
    store: S,
    api: A,
    clock: C,
    activity: GatewayActivityHub,
    artifact_root: PathBuf,
    bot_id: String,
    gateway: String,
    delivery_owner: String,
}

impl<S, A, C> PreparedTelegramGateway<S, A, C>
where
    S: ActorStore + IngressStore + GatewayDeliveryStore + Clone + Send + Sync + 'static,
    A: TelegramApi + TelegramIngressApi + Clone + Send + Sync + 'static,
    C: Clock,
{
    pub fn bot_id(&self) -> &str {
        &self.bot_id
    }

    pub fn gateway_name(&self) -> &str {
        &self.gateway
    }

    pub async fn ingress(self: Arc<Self>, shutdown: watch::Receiver<bool>) -> Result<()> {
        match &self.transport {
            PreparedTelegramIngress::Webhook {
                listener,
                public_url,
                webhook_secret,
            } => {
                let listener = listener
                    .lock()
                    .expect("Telegram listener poisoned")
                    .take()
                    .context("Telegram webhook listener was already started")?;
                TelegramWebhookServer::new(
                    listener,
                    public_url.path(),
                    SecretToken::new(webhook_secret),
                    self.ingress.clone(),
                )?
                .run(shutdown)
                .await
            }
            PreparedTelegramIngress::Polling => {
                TelegramPollingWorker::new(self.api.clone(), self.ingress.clone())
                    .run(shutdown)
                    .await
            }
        }
    }

    pub async fn delivery(self: Arc<Self>, shutdown: watch::Receiver<bool>) -> Result<()> {
        TelegramDeliveryWorker::new(
            self.store.clone(),
            self.api.clone(),
            self.clock.clone(),
            self.gateway.clone(),
            self.delivery_owner.clone(),
            self.artifact_root.clone(),
        )
        .run(shutdown)
        .await
    }

    pub async fn streaming(self: Arc<Self>, shutdown: watch::Receiver<bool>) -> Result<()> {
        TelegramActivityWorker::new(self.api.clone(), self.gateway.clone())
            .run(self.activity.subscribe(), shutdown)
            .await
    }
}

pub async fn prepare<C>(
    config: ValidatedTelegramConfig,
    store: crate::runtime::sqlite::SqliteRuntimeStore,
    linking: Arc<dyn IdentityLinkManager>,
    signals: ActorSignals,
    activity: GatewayActivityHub,
    clock: C,
    artifact_root: PathBuf,
) -> Result<
    PreparedTelegramGateway<crate::runtime::sqlite::SqliteRuntimeStore, ReqwestTelegramApi, C>,
>
where
    C: Clock,
{
    let api = ReqwestTelegramApi::new(config.token.clone())?;
    prepare_with_api(
        config,
        store,
        linking,
        signals,
        activity,
        clock,
        artifact_root,
        api,
    )
    .await
}

#[doc(hidden)]
pub async fn prepare_with_api<S, A, C>(
    config: ValidatedTelegramConfig,
    store: S,
    linking: Arc<dyn IdentityLinkManager>,
    signals: ActorSignals,
    activity: GatewayActivityHub,
    clock: C,
    artifact_root: PathBuf,
    api: A,
) -> Result<PreparedTelegramGateway<S, A, C>>
where
    S: ActorStore + IngressStore + GatewayDeliveryStore + Clone + Send + Sync + 'static,
    A: TelegramApi + TelegramIngressApi + Clone + Send + Sync + 'static,
    C: Clock,
{
    let transport = match config.ingress {
        ValidatedTelegramIngressConfig::Webhook {
            public_url,
            listen,
            webhook_secret,
        } => PreparedTelegramIngress::Webhook {
            listener: Mutex::new(Some(
                TcpListener::bind(listen)
                    .await
                    .context("failed to bind Telegram webhook listener")?,
            )),
            public_url,
            webhook_secret,
        },
        ValidatedTelegramIngressConfig::Polling => PreparedTelegramIngress::Polling,
    };
    let bot = api.get_me().await?;
    if !bot.is_bot {
        bail!("Telegram getMe returned a non-bot identity");
    }
    let bot_id = bot.id.to_string();
    let bot_username = bot
        .username
        .filter(|value| !value.trim().is_empty())
        .context("Telegram bot username is missing")?;
    match &transport {
        PreparedTelegramIngress::Webhook {
            public_url,
            webhook_secret,
            ..
        } => {
            let public_url = public_url.as_str().to_owned();
            api.set_webhook(SetWebhook {
                url: public_url.clone(),
                secret_token: webhook_secret.clone(),
                allowed_updates: vec!["message".into()],
                drop_pending_updates: false,
            })
            .await?;
            let info = api.get_webhook_info().await?;
            if info.url != public_url || info.allowed_updates != ["message"] {
                bail!("Telegram webhook reconciliation mismatch");
            }
        }
        PreparedTelegramIngress::Polling => {
            api.delete_webhook(DeleteWebhook {
                drop_pending_updates: false,
            })
            .await?;
            if !api.get_webhook_info().await?.url.is_empty() {
                bail!("Telegram polling reconciliation mismatch");
            }
        }
    }
    let gateway = format!("telegram:{bot_id}");
    let ingress = Arc::new(TelegramIngressService::new(
        store.clone(),
        linking,
        signals,
        bot_id.clone(),
        bot_username,
        clock.clone(),
    )?);
    Ok(PreparedTelegramGateway {
        transport,
        ingress,
        store,
        api,
        clock,
        activity,
        artifact_root,
        bot_id,
        gateway,
        delivery_owner: format!("telegram-delivery-{}", std::process::id()),
    })
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use anyhow::Result;
    use async_trait::async_trait;

    use super::{
        api::{
            DeleteWebhook, EditMessageText, GetUpdates, SendFile, SendMessage, SendRichMessage,
            SetWebhook, TelegramApi, TelegramApiError, TelegramIngressApi, TelegramMessageRef,
            WebhookInfo,
        },
        prepare_with_api,
        types::TelegramBot,
    };
    use crate::{
        config::{ValidatedTelegramConfig, ValidatedTelegramIngressConfig},
        runtime::{
            gateway_activity::GatewayActivityHub,
            identity_link::{IdentityLinkManager, IdentityLinkService, SystemLinkCodeGenerator},
            model::ManualClock,
            signals::ActorSignals,
            sqlite::SqliteRuntimeStore,
        },
    };

    #[derive(Clone)]
    struct StartupApi {
        calls: Arc<Mutex<Vec<String>>>,
        webhook: Arc<Mutex<Option<(String, String, Vec<String>, bool)>>>,
        deleted: Arc<Mutex<Vec<bool>>>,
        info: WebhookInfo,
        expected_bound: Option<std::net::SocketAddr>,
    }

    #[async_trait]
    impl TelegramIngressApi for StartupApi {
        async fn get_me(&self) -> std::result::Result<TelegramBot, TelegramApiError> {
            self.calls.lock().unwrap().push("getMe".into());
            if let Some(address) = self.expected_bound {
                assert!(
                    std::net::TcpListener::bind(address).is_err(),
                    "Telegram listener was not bound before getMe"
                );
            }
            Ok(TelegramBot {
                id: 900,
                is_bot: true,
                username: Some("codrik_bot".into()),
            })
        }

        async fn set_webhook(
            &self,
            command: SetWebhook,
        ) -> std::result::Result<(), TelegramApiError> {
            self.calls.lock().unwrap().push("setWebhook".into());
            *self.webhook.lock().unwrap() = Some((
                command.url,
                command.secret_token,
                command.allowed_updates,
                command.drop_pending_updates,
            ));
            Ok(())
        }

        async fn delete_webhook(
            &self,
            command: DeleteWebhook,
        ) -> std::result::Result<(), TelegramApiError> {
            self.calls.lock().unwrap().push("deleteWebhook".into());
            self.deleted
                .lock()
                .unwrap()
                .push(command.drop_pending_updates);
            Ok(())
        }

        async fn get_webhook_info(&self) -> std::result::Result<WebhookInfo, TelegramApiError> {
            self.calls.lock().unwrap().push("getWebhookInfo".into());
            Ok(self.info.clone())
        }

        async fn get_updates(
            &self,
            _command: GetUpdates,
        ) -> std::result::Result<Vec<super::types::TelegramUpdate>, TelegramApiError> {
            unreachable!("webhook preparation must not poll updates")
        }
    }

    #[async_trait]
    impl TelegramApi for StartupApi {
        async fn send_message(
            &self,
            _command: SendMessage,
        ) -> std::result::Result<TelegramMessageRef, TelegramApiError> {
            unreachable!()
        }

        async fn send_rich_message(
            &self,
            _command: SendRichMessage,
        ) -> std::result::Result<TelegramMessageRef, TelegramApiError> {
            unreachable!()
        }

        async fn send_chat_action(
            &self,
            _command: crate::interfaces::telegram::api::SendChatAction,
        ) -> std::result::Result<(), TelegramApiError> {
            unreachable!()
        }

        async fn edit_message_text(
            &self,
            _command: EditMessageText,
        ) -> std::result::Result<(), TelegramApiError> {
            unreachable!()
        }

        async fn send_photo(
            &self,
            _command: SendFile,
        ) -> std::result::Result<TelegramMessageRef, TelegramApiError> {
            unreachable!()
        }

        async fn send_document(
            &self,
            _command: SendFile,
        ) -> std::result::Result<TelegramMessageRef, TelegramApiError> {
            unreachable!()
        }
    }

    #[tokio::test]
    async fn prepare_binds_and_reconciles_webhook_in_order() -> Result<()> {
        let store = SqliteRuntimeStore::open_in_memory().await?;
        let clock = ManualClock::new(1);
        let linking: Arc<dyn IdentityLinkManager> = Arc::new(IdentityLinkService::new(
            store.clone(),
            clock.clone(),
            SystemLinkCodeGenerator,
        ));
        let probe = std::net::TcpListener::bind("127.0.0.1:0")?;
        let listen = probe.local_addr()?;
        drop(probe);
        let api = StartupApi {
            calls: Arc::new(Mutex::new(Vec::new())),
            webhook: Arc::new(Mutex::new(None)),
            deleted: Arc::new(Mutex::new(Vec::new())),
            info: WebhookInfo {
                url: "https://agent.example/webhooks/telegram".into(),
                allowed_updates: vec!["message".into()],
                pending_update_count: 0,
            },
            expected_bound: Some(listen),
        };
        let config = ValidatedTelegramConfig {
            token: "secret-token".into(),
            ingress: ValidatedTelegramIngressConfig::Webhook {
                public_url: url::Url::parse("https://agent.example/webhooks/telegram")?,
                listen,
                webhook_secret: "secret_value".into(),
            },
        };

        let prepared = prepare_with_api(
            config,
            store,
            linking,
            ActorSignals::default(),
            GatewayActivityHub::default(),
            clock,
            std::env::temp_dir(),
            api.clone(),
        )
        .await?;

        assert_eq!(prepared.bot_id(), "900");
        assert_eq!(prepared.gateway_name(), "telegram:900");
        assert_eq!(
            *api.calls.lock().unwrap(),
            vec!["getMe", "setWebhook", "getWebhookInfo"]
        );
        assert_eq!(
            *api.webhook.lock().unwrap(),
            Some((
                "https://agent.example/webhooks/telegram".into(),
                "secret_value".into(),
                vec!["message".into()],
                false,
            ))
        );
        Ok(())
    }

    #[tokio::test]
    async fn prepare_rejects_mismatched_webhook_info() -> Result<()> {
        let store = SqliteRuntimeStore::open_in_memory().await?;
        let clock = ManualClock::new(1);
        let linking: Arc<dyn IdentityLinkManager> = Arc::new(IdentityLinkService::new(
            store.clone(),
            clock.clone(),
            SystemLinkCodeGenerator,
        ));
        let api = StartupApi {
            calls: Arc::new(Mutex::new(Vec::new())),
            webhook: Arc::new(Mutex::new(None)),
            deleted: Arc::new(Mutex::new(Vec::new())),
            info: WebhookInfo {
                url: "https://wrong.example/hook".into(),
                allowed_updates: vec!["message".into()],
                pending_update_count: 0,
            },
            expected_bound: None,
        };
        let config = ValidatedTelegramConfig {
            token: "secret-token".into(),
            ingress: ValidatedTelegramIngressConfig::Webhook {
                public_url: url::Url::parse("https://agent.example/webhooks/telegram")?,
                listen: "127.0.0.1:0".parse()?,
                webhook_secret: "secret_value".into(),
            },
        };

        let error = prepare_with_api(
            config,
            store,
            linking,
            ActorSignals::default(),
            GatewayActivityHub::default(),
            clock,
            std::env::temp_dir(),
            api,
        )
        .await
        .err()
        .expect("mismatched webhook info must fail");
        assert!(error.to_string().contains("reconciliation mismatch"));
        Ok(())
    }

    #[tokio::test]
    async fn prepare_polling_removes_webhook_without_dropping_updates() -> Result<()> {
        let store = SqliteRuntimeStore::open_in_memory().await?;
        let clock = ManualClock::new(1);
        let linking: Arc<dyn IdentityLinkManager> = Arc::new(IdentityLinkService::new(
            store.clone(),
            clock.clone(),
            SystemLinkCodeGenerator,
        ));
        let api = StartupApi {
            calls: Arc::new(Mutex::new(Vec::new())),
            webhook: Arc::new(Mutex::new(None)),
            deleted: Arc::new(Mutex::new(Vec::new())),
            info: WebhookInfo {
                url: String::new(),
                allowed_updates: vec!["message".into()],
                pending_update_count: 3,
            },
            expected_bound: None,
        };

        let prepared = prepare_with_api(
            ValidatedTelegramConfig {
                token: "secret-token".into(),
                ingress: ValidatedTelegramIngressConfig::Polling,
            },
            store,
            linking,
            ActorSignals::default(),
            GatewayActivityHub::default(),
            clock,
            std::env::temp_dir(),
            api.clone(),
        )
        .await?;

        assert_eq!(prepared.bot_id(), "900");
        assert_eq!(
            *api.calls.lock().unwrap(),
            vec!["getMe", "deleteWebhook", "getWebhookInfo"]
        );
        assert_eq!(*api.deleted.lock().unwrap(), vec![false]);
        assert_eq!(*api.webhook.lock().unwrap(), None);
        Ok(())
    }
}
