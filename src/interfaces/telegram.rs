pub mod api;
pub mod delivery;
pub mod streaming;
pub mod types;
pub mod webhook;

use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};

use anyhow::{Context, Result, bail};
use tokio::{net::TcpListener, sync::watch};

use crate::{
    config::ValidatedTelegramConfig,
    interfaces::telegram::{
        api::{ReqwestTelegramApi, SetWebhook, TelegramApi},
        delivery::TelegramDeliveryWorker,
        streaming::TelegramStreamingWorker,
        webhook::{SecretToken, TelegramIngressService, TelegramWebhookServer},
    },
    runtime::{
        gateway_activity::GatewayActivityHub,
        identity_link::IdentityLinkManager,
        model::Clock,
        signals::ActorSignals,
        store::{ActorStore, GatewayDeliveryStore, GatewayStreamStore, IngressStore},
    },
};

pub struct PreparedTelegramGateway<S, A, C> {
    listener: Mutex<Option<TcpListener>>,
    path: String,
    webhook_secret: String,
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
    S: ActorStore
        + IngressStore
        + GatewayDeliveryStore
        + GatewayStreamStore
        + Clone
        + Send
        + Sync
        + 'static,
    A: TelegramApi + Clone + Send + Sync + 'static,
    C: Clock,
{
    pub fn bot_id(&self) -> &str {
        &self.bot_id
    }

    pub fn gateway_name(&self) -> &str {
        &self.gateway
    }

    pub async fn webhook(self: Arc<Self>, shutdown: watch::Receiver<bool>) -> Result<()> {
        let listener = self
            .listener
            .lock()
            .expect("Telegram listener poisoned")
            .take()
            .context("Telegram webhook listener was already started")?;
        TelegramWebhookServer::new(
            listener,
            self.path.clone(),
            SecretToken::new(&self.webhook_secret),
            self.ingress.clone(),
        )?
        .run(shutdown)
        .await
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
        TelegramStreamingWorker::new(
            self.store.clone(),
            self.api.clone(),
            self.clock.clone(),
            self.gateway.clone(),
        )
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
    S: ActorStore
        + IngressStore
        + GatewayDeliveryStore
        + GatewayStreamStore
        + Clone
        + Send
        + Sync
        + 'static,
    A: TelegramApi + Clone + Send + Sync + 'static,
    C: Clock,
{
    let listener = TcpListener::bind(config.listen)
        .await
        .context("failed to bind Telegram webhook listener")?;
    let bot = api.get_me().await?;
    if !bot.is_bot {
        bail!("Telegram getMe returned a non-bot identity");
    }
    let bot_id = bot.id.to_string();
    let bot_username = bot
        .username
        .filter(|value| !value.trim().is_empty())
        .context("Telegram bot username is missing")?;
    let public_url = config.public_url.as_str().to_owned();
    api.set_webhook(SetWebhook {
        url: public_url.clone(),
        secret_token: config.webhook_secret.clone(),
        allowed_updates: vec!["message".into()],
        drop_pending_updates: false,
    })
    .await?;
    let info = api.get_webhook_info().await?;
    if info.url != public_url || info.allowed_updates != ["message"] {
        bail!("Telegram webhook reconciliation mismatch");
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
        listener: Mutex::new(Some(listener)),
        path: config.public_url.path().to_owned(),
        webhook_secret: config.webhook_secret,
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
            EditMessageText, SendFile, SendMessage, SetWebhook, TelegramApi, TelegramApiError,
            TelegramMessageRef, WebhookInfo,
        },
        prepare_with_api,
        types::TelegramBot,
    };
    use crate::{
        config::ValidatedTelegramConfig,
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
        info: WebhookInfo,
        expected_bound: Option<std::net::SocketAddr>,
    }

    #[async_trait]
    impl TelegramApi for StartupApi {
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

        async fn get_webhook_info(&self) -> std::result::Result<WebhookInfo, TelegramApiError> {
            self.calls.lock().unwrap().push("getWebhookInfo".into());
            Ok(self.info.clone())
        }

        async fn send_message(
            &self,
            _command: SendMessage,
        ) -> std::result::Result<TelegramMessageRef, TelegramApiError> {
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
            info: WebhookInfo {
                url: "https://agent.example/webhooks/telegram".into(),
                allowed_updates: vec!["message".into()],
                pending_update_count: 0,
            },
            expected_bound: Some(listen),
        };
        let config = ValidatedTelegramConfig {
            token: "secret-token".into(),
            public_url: url::Url::parse("https://agent.example/webhooks/telegram")?,
            listen,
            webhook_secret: "secret_value".into(),
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
            info: WebhookInfo {
                url: "https://wrong.example/hook".into(),
                allowed_updates: vec!["message".into()],
                pending_update_count: 0,
            },
            expected_bound: None,
        };
        let config = ValidatedTelegramConfig {
            token: "secret-token".into(),
            public_url: url::Url::parse("https://agent.example/webhooks/telegram")?,
            listen: "127.0.0.1:0".parse()?,
            webhook_secret: "secret_value".into(),
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
}
