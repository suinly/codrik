use std::{path::PathBuf, time::Duration};

use anyhow::{Result, bail};
use futures_util::{StreamExt, stream};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::sync::watch;

use crate::{
    interfaces::telegram::{
        api::{
            EditMessageText, ReplyParameters, SendFile, SendMessage, TelegramApi, TelegramApiError,
            TelegramApiErrorClass, TelegramMessageRef,
        },
        streaming::lock_gateway_stream,
    },
    runtime::{
        gateway::{ClaimedGatewayDelivery, GatewayDeliveryState},
        model::Clock,
        store::{GatewayDeliveryStore, GatewayStreamStore, OutboxPayload},
    },
};

const CLAIM_DURATION: Duration = Duration::from_secs(30);
const RENEW_INTERVAL: Duration = Duration::from_secs(10);
const POLL_INTERVAL: Duration = Duration::from_millis(500);
const CLAIM_BATCH: usize = 32;
const MAX_CONCURRENCY: usize = 4;
const MAX_PHOTO_BYTES: u64 = 10 * 1024 * 1024;
const MAX_DOCUMENT_BYTES: u64 = 50 * 1024 * 1024;

#[derive(Debug)]
enum DeliveryError {
    Api(TelegramApiError),
    Terminal(&'static str),
    StreamEditTerminal,
}

struct DeliverySuccess {
    message: TelegramMessageRef,
    edited_stream: bool,
}

pub struct TelegramDeliveryWorker<S, A, C> {
    store: S,
    api: A,
    clock: C,
    gateway: String,
    owner: String,
    artifact_root: PathBuf,
}

impl<S, A, C> TelegramDeliveryWorker<S, A, C>
where
    S: GatewayDeliveryStore + GatewayStreamStore,
    A: TelegramApi,
    C: Clock,
{
    pub fn new(
        store: S,
        api: A,
        clock: C,
        gateway: impl Into<String>,
        owner: impl Into<String>,
        artifact_root: PathBuf,
    ) -> Self {
        Self {
            store,
            api,
            clock,
            gateway: gateway.into(),
            owner: owner.into(),
            artifact_root,
        }
    }

    pub async fn run(&self, mut shutdown: watch::Receiver<bool>) -> Result<()> {
        loop {
            if *shutdown.borrow() {
                return Ok(());
            }
            let delivered = self.run_once().await?;
            if delivered > 0 {
                continue;
            }
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return Ok(());
                    }
                }
                () = tokio::time::sleep(POLL_INTERVAL) => {}
            }
        }
    }

    pub async fn run_once(&self) -> Result<usize> {
        let now = self.clock.now();
        let claims = self
            .store
            .claim_gateway_deliveries(
                &self.gateway,
                &self.owner,
                now,
                now.plus_millis(CLAIM_DURATION.as_millis() as i64),
                CLAIM_BATCH,
            )
            .await?;
        let count = claims.len();
        let results = stream::iter(claims)
            .map(|delivery| self.deliver(delivery))
            .buffer_unordered(MAX_CONCURRENCY)
            .collect::<Vec<_>>()
            .await;
        for result in results {
            result?;
        }
        Ok(count)
    }

    async fn deliver(&self, delivery: ClaimedGatewayDelivery) -> Result<()> {
        let now = self.clock.now();
        let Some(claim) = self
            .store
            .renew_gateway_delivery(
                &delivery.claim,
                now,
                now.plus_millis(CLAIM_DURATION.as_millis() as i64),
            )
            .await?
        else {
            return Ok(());
        };

        let _stream_guard = if delivery.ordinal == 0
            && let Some(work_item) = &delivery.work_item_id
        {
            Some(lock_gateway_stream(work_item, &delivery.route).await)
        } else {
            None
        };
        let stream_message_id = if delivery.ordinal == 0 {
            match &delivery.work_item_id {
                Some(work_item) => self
                    .store
                    .claim_gateway_stream_for_final(work_item, &delivery.route, self.clock.now())
                    .await?
                    .and_then(|value| value.parse::<i64>().ok()),
                None => None,
            }
        } else {
            None
        };
        if stream_message_id.is_some()
            && !self
                .store
                .set_gateway_delivery_retry_safe(&claim, true, self.clock.now())
                .await?
        {
            bail!("Telegram delivery claim was lost before retry-safe edit");
        }
        let (mut claim, mut result) = self
            .send_with_renewals(claim, &delivery, stream_message_id)
            .await?;
        if matches!(result, Err(DeliveryError::StreamEditTerminal)) {
            if !self
                .store
                .set_gateway_delivery_retry_safe(&claim, false, self.clock.now())
                .await?
            {
                bail!("Telegram delivery claim was lost before send fallback");
            }
            (claim, result) = self.send_with_renewals(claim, &delivery, None).await?;
        }
        let transition_time = self.clock.now();
        match result {
            Ok(success) => {
                if !self
                    .store
                    .complete_gateway_delivery(
                        &claim,
                        Some(success.message.message_id.to_string()),
                        transition_time,
                    )
                    .await?
                {
                    bail!("Telegram delivery claim was lost after transport completed");
                }
                if success.edited_stream
                    && let Some(work_item) = &delivery.work_item_id
                {
                    self.store
                        .close_gateway_stream(work_item, &delivery.route, transition_time)
                        .await?;
                }
            }
            Err(DeliveryError::Terminal(summary)) => {
                if !self
                    .store
                    .fail_gateway_delivery(
                        &claim,
                        GatewayDeliveryState::FailedTerminal,
                        "terminal",
                        summary,
                        transition_time,
                    )
                    .await?
                {
                    bail!("Telegram delivery claim was lost before terminal transition");
                }
            }
            Err(DeliveryError::StreamEditTerminal) => unreachable!("fallback handled above"),
            Err(DeliveryError::Api(error)) => match error.class() {
                TelegramApiErrorClass::Retryable { retry_after } => {
                    if !self
                        .store
                        .retry_gateway_delivery(
                            &claim,
                            retry_at(
                                transition_time,
                                delivery.attempt_count,
                                retry_after,
                                delivery.claim.id.as_str(),
                            ),
                            "telegram_retryable",
                            "Telegram API retryable failure",
                            transition_time,
                        )
                        .await?
                    {
                        bail!("Telegram delivery claim was lost before retry transition");
                    }
                }
                TelegramApiErrorClass::Terminal => {
                    if !self
                        .store
                        .fail_gateway_delivery(
                            &claim,
                            GatewayDeliveryState::FailedTerminal,
                            "telegram_terminal",
                            "Telegram API rejected the delivery",
                            transition_time,
                        )
                        .await?
                    {
                        bail!("Telegram delivery claim was lost before terminal transition");
                    }
                }
                TelegramApiErrorClass::OutcomeUnknown => {
                    if !self
                        .store
                        .fail_gateway_delivery(
                            &claim,
                            GatewayDeliveryState::OutcomeUnknown,
                            "telegram_outcome_unknown",
                            "Telegram delivery outcome is unknown",
                            transition_time,
                        )
                        .await?
                    {
                        bail!("Telegram delivery claim was lost before unknown transition");
                    }
                }
            },
        }
        Ok(())
    }

    async fn send_with_renewals(
        &self,
        mut claim: crate::runtime::gateway::GatewayDeliveryClaim,
        delivery: &ClaimedGatewayDelivery,
        stream_message_id: Option<i64>,
    ) -> Result<(
        crate::runtime::gateway::GatewayDeliveryClaim,
        std::result::Result<DeliverySuccess, DeliveryError>,
    )> {
        let transport = self.send_payload(delivery, stream_message_id);
        tokio::pin!(transport);
        let mut renewal =
            tokio::time::interval_at(tokio::time::Instant::now() + RENEW_INTERVAL, RENEW_INTERVAL);
        renewal.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                result = &mut transport => return Ok((claim, result)),
                _ = renewal.tick() => {
                    let now = self.clock.now();
                    let Some(renewed) = self.store.renew_gateway_delivery(
                        &claim,
                        now,
                        now.plus_millis(CLAIM_DURATION.as_millis() as i64),
                    ).await? else {
                        bail!("Telegram delivery claim was lost during transport");
                    };
                    claim = renewed;
                }
            }
        }
    }

    async fn send_payload(
        &self,
        delivery: &ClaimedGatewayDelivery,
        stream_message_id: Option<i64>,
    ) -> std::result::Result<DeliverySuccess, DeliveryError> {
        match &delivery.payload {
            OutboxPayload::Text { text } => {
                if let Some(message_id) = stream_message_id {
                    match self
                        .api
                        .edit_message_text(EditMessageText {
                            chat_id: delivery.route.address.clone(),
                            message_id,
                            text: text.clone(),
                        })
                        .await
                        .map(|()| DeliverySuccess {
                            message: TelegramMessageRef { message_id },
                            edited_stream: true,
                        }) {
                        Ok(success) => return Ok(success),
                        Err(error) if !matches!(error.class(), TelegramApiErrorClass::Terminal) => {
                            return Err(DeliveryError::Api(error));
                        }
                        Err(_) => return Err(DeliveryError::StreamEditTerminal),
                    }
                }
                let reply_parameters = delivery
                    .route
                    .reply_to_external_id
                    .as_deref()
                    .and_then(|value| value.parse::<i64>().ok())
                    .map(|message_id| ReplyParameters { message_id });
                self.api
                    .send_message(SendMessage {
                        chat_id: delivery.route.address.clone(),
                        text: text.clone(),
                        reply_parameters,
                    })
                    .await
                    .map(|message| DeliverySuccess {
                        message,
                        edited_stream: false,
                    })
                    .map_err(DeliveryError::Api)
            }
            OutboxPayload::File {
                managed_path,
                display_name,
                media_type,
                size,
                sha256,
                caption,
                ..
            } => {
                let is_photo = matches!(
                    media_type.to_ascii_lowercase().as_str(),
                    "image/jpeg" | "image/png" | "image/webp"
                ) && *size <= MAX_PHOTO_BYTES;
                if *size > MAX_DOCUMENT_BYTES {
                    return Err(DeliveryError::Terminal(
                        "managed artifact exceeds Telegram size limit",
                    ));
                }
                let file = validate_managed_file(&self.artifact_root, managed_path, *size, sha256)
                    .await
                    .map_err(|_| {
                        DeliveryError::Terminal("managed artifact integrity check failed")
                    })?;
                let command = SendFile {
                    chat_id: delivery.route.address.clone(),
                    path: managed_path.clone(),
                    file,
                    length: *size,
                    display_name: display_name.clone(),
                    caption: caption.clone(),
                };
                if is_photo {
                    self.api.send_photo(command).await
                } else {
                    self.api.send_document(command).await
                }
                .map(|message| DeliverySuccess {
                    message,
                    edited_stream: false,
                })
                .map_err(DeliveryError::Api)
            }
            OutboxPayload::TerminalError { .. } => Err(DeliveryError::Terminal(
                "terminal errors must be projected to text before Telegram delivery",
            )),
        }
    }
}

fn retry_at(
    now: crate::runtime::model::Timestamp,
    attempt: usize,
    telegram_delay: Option<Duration>,
    delivery_id: &str,
) -> crate::runtime::model::Timestamp {
    if let Some(delay) = telegram_delay {
        return now.plus_millis(delay.as_millis().min(i64::MAX as u128) as i64);
    }
    let exponent = attempt.saturating_sub(1).min(18);
    let seconds = 1_u64
        .checked_shl(exponent as u32)
        .unwrap_or(u64::MAX)
        .min(300);
    let base_millis = seconds.saturating_mul(1_000);
    let digest = Sha256::digest(delivery_id.as_bytes());
    let jitter_ceiling = (base_millis / 4).min(1_000);
    let jitter = if jitter_ceiling == 0 {
        0
    } else {
        u64::from_be_bytes(digest[..8].try_into().expect("SHA-256 prefix")) % jitter_ceiling
    };
    now.plus_millis(base_millis.saturating_add(jitter).min(i64::MAX as u64) as i64)
}

async fn validate_managed_file(
    root: &std::path::Path,
    path: &std::path::Path,
    expected_size: u64,
    expected_sha256: &str,
) -> Result<tokio::fs::File> {
    let metadata = tokio::fs::symlink_metadata(path).await?;
    if !metadata.file_type().is_file() || metadata.len() != expected_size {
        bail!("managed artifact metadata mismatch");
    }
    let canonical_root = tokio::fs::canonicalize(root).await?;
    let canonical_path = tokio::fs::canonicalize(path).await?;
    if !canonical_path.starts_with(&canonical_root) {
        bail!("managed artifact is outside the artifact root");
    }
    let mut file = tokio::fs::File::open(&canonical_path).await?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    if format!("{:x}", hasher.finalize()) != expected_sha256.to_ascii_lowercase() {
        bail!("managed artifact SHA-256 mismatch");
    }
    file.rewind().await?;
    Ok(file)
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        path::{Path, PathBuf},
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use anyhow::Result;
    use async_trait::async_trait;
    use sha2::{Digest, Sha256};
    use tokio::io::AsyncReadExt;

    use super::{TelegramDeliveryWorker, retry_at, validate_managed_file};
    use crate::{
        interfaces::telegram::{
            api::{
                EditMessageText, SendFile, SendMessage, SetWebhook, TelegramApi, TelegramApiError,
                TelegramApiErrorClass, TelegramMessageRef, WebhookInfo,
            },
            types::TelegramBot,
        },
        runtime::{
            gateway::{DeliveryRoute, NewGatewayDelivery},
            model::{ArtifactId, ManualClock, Timestamp},
            sqlite::SqliteRuntimeStore,
            store::{GatewayDeliveryStore, OutboxPayload},
        },
    };

    #[derive(Clone, Default)]
    struct RecordingApi {
        messages: Arc<Mutex<Vec<(String, String)>>>,
        edits: Arc<Mutex<Vec<(i64, String)>>>,
        files: Arc<Mutex<Vec<(&'static str, PathBuf)>>>,
        responses: Arc<Mutex<VecDeque<TelegramApiErrorClass>>>,
        delay: Duration,
        active: Arc<AtomicUsize>,
        max_active: Arc<AtomicUsize>,
        terminal_edit: Arc<AtomicBool>,
    }

    impl RecordingApi {
        fn with_error(class: TelegramApiErrorClass) -> Self {
            let api = Self::default();
            api.responses.lock().unwrap().push_back(class);
            api
        }

        fn with_delay(delay: Duration) -> Self {
            Self {
                delay,
                ..Self::default()
            }
        }

        async fn response(&self) -> Result<TelegramMessageRef, TelegramApiError> {
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_active.fetch_max(active, Ordering::SeqCst);
            if !self.delay.is_zero() {
                tokio::time::sleep(self.delay).await;
            }
            self.active.fetch_sub(1, Ordering::SeqCst);
            if let Some(class) = self.responses.lock().unwrap().pop_front() {
                Err(TelegramApiError::classified(
                    class,
                    "sendMessage",
                    "injected failure",
                ))
            } else {
                Ok(TelegramMessageRef { message_id: 77 })
            }
        }
    }

    #[async_trait]
    impl TelegramApi for RecordingApi {
        async fn get_me(&self) -> Result<TelegramBot, TelegramApiError> {
            unreachable!()
        }

        async fn set_webhook(&self, _command: SetWebhook) -> Result<(), TelegramApiError> {
            unreachable!()
        }

        async fn get_webhook_info(&self) -> Result<WebhookInfo, TelegramApiError> {
            unreachable!()
        }

        async fn send_message(
            &self,
            command: SendMessage,
        ) -> Result<TelegramMessageRef, TelegramApiError> {
            self.messages
                .lock()
                .unwrap()
                .push((command.chat_id, command.text));
            self.response().await
        }

        async fn send_chat_action(
            &self,
            _command: crate::interfaces::telegram::api::SendChatAction,
        ) -> Result<(), TelegramApiError> {
            unreachable!()
        }

        async fn edit_message_text(
            &self,
            command: EditMessageText,
        ) -> Result<(), TelegramApiError> {
            if self.terminal_edit.load(Ordering::SeqCst) {
                return Err(TelegramApiError::classified(
                    TelegramApiErrorClass::Terminal,
                    "editMessageText",
                    "message not found",
                ));
            }
            self.edits
                .lock()
                .unwrap()
                .push((command.message_id, command.text));
            Ok(())
        }

        async fn send_photo(
            &self,
            command: SendFile,
        ) -> Result<TelegramMessageRef, TelegramApiError> {
            self.files.lock().unwrap().push(("photo", command.path));
            self.response().await
        }

        async fn send_document(
            &self,
            command: SendFile,
        ) -> Result<TelegramMessageRef, TelegramApiError> {
            self.files.lock().unwrap().push(("document", command.path));
            self.response().await
        }
    }

    fn route(address: &str) -> Result<DeliveryRoute> {
        DeliveryRoute::new("telegram:900", address, Some("42".into()), 4096, 1024)
    }

    async fn enqueue_text(store: &SqliteRuntimeStore, key: &str) -> Result<()> {
        store
            .enqueue_gateway_delivery(
                NewGatewayDelivery::new(
                    key,
                    None,
                    0,
                    route("100")?,
                    OutboxPayload::Text {
                        text: "hello".into(),
                    },
                )?,
                Timestamp(1),
            )
            .await?;
        Ok(())
    }

    async fn enqueue_file(
        store: &SqliteRuntimeStore,
        key: &str,
        path: &Path,
        media_type: &str,
        size: u64,
        sha256: String,
    ) -> Result<()> {
        store
            .enqueue_gateway_delivery(
                NewGatewayDelivery::new(
                    key,
                    None,
                    0,
                    route(key)?,
                    OutboxPayload::File {
                        artifact_id: ArtifactId::new(),
                        managed_path: path.to_owned(),
                        display_name: "result.bin".into(),
                        media_type: media_type.into(),
                        size,
                        sha256,
                        caption: Some("result".into()),
                    },
                )?,
                Timestamp(1),
            )
            .await?;
        Ok(())
    }

    fn temp_artifact_root() -> PathBuf {
        std::env::temp_dir().join(format!("codrik-telegram-{}", uuid::Uuid::new_v4()))
    }

    #[tokio::test]
    async fn text_delivery_completes_and_is_not_sent_again() -> Result<()> {
        let store = SqliteRuntimeStore::open_in_memory().await?;
        store
            .enqueue_gateway_delivery(
                NewGatewayDelivery::new(
                    "reply:1",
                    None,
                    0,
                    DeliveryRoute::new("telegram:900", "100", Some("42".into()), 4096, 1024)?,
                    OutboxPayload::Text {
                        text: "hello".into(),
                    },
                )?,
                Timestamp(1),
            )
            .await?;
        let api = RecordingApi::default();
        let worker = TelegramDeliveryWorker::new(
            store.clone(),
            api.clone(),
            ManualClock::new(2),
            "telegram:900",
            "worker-1",
            std::env::temp_dir(),
        );

        assert_eq!(worker.run_once().await?, 1);
        assert_eq!(worker.run_once().await?, 0);
        assert_eq!(
            *api.messages.lock().unwrap(),
            vec![("100".into(), "hello".into())]
        );
        Ok(())
    }

    #[tokio::test]
    async fn telegram_retry_after_is_scheduled_exactly() -> Result<()> {
        let store = SqliteRuntimeStore::open_in_memory().await?;
        enqueue_text(&store, "retry:1").await?;
        let api = RecordingApi::with_error(TelegramApiErrorClass::Retryable {
            retry_after: Some(Duration::from_secs(5)),
        });
        let clock = ManualClock::new(10);
        let worker = TelegramDeliveryWorker::new(
            store,
            api.clone(),
            clock.clone(),
            "telegram:900",
            "worker-1",
            std::env::temp_dir(),
        );

        assert_eq!(worker.run_once().await?, 1);
        clock.advance(4_999);
        assert_eq!(worker.run_once().await?, 0);
        clock.advance(1);
        assert_eq!(worker.run_once().await?, 1);
        assert_eq!(api.messages.lock().unwrap().len(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn terminal_and_unknown_errors_are_not_retried() -> Result<()> {
        for (key, class) in [
            ("terminal", TelegramApiErrorClass::Terminal),
            ("unknown", TelegramApiErrorClass::OutcomeUnknown),
        ] {
            let store = SqliteRuntimeStore::open_in_memory().await?;
            enqueue_text(&store, key).await?;
            let api = RecordingApi::with_error(class);
            let clock = ManualClock::new(10);
            let worker = TelegramDeliveryWorker::new(
                store,
                api.clone(),
                clock.clone(),
                "telegram:900",
                "worker-1",
                std::env::temp_dir(),
            );

            assert_eq!(worker.run_once().await?, 1);
            clock.advance(60_000);
            assert_eq!(worker.run_once().await?, 0);
            assert_eq!(api.messages.lock().unwrap().len(), 1);
        }
        Ok(())
    }

    #[tokio::test]
    async fn valid_image_uses_photo_and_other_file_uses_document() -> Result<()> {
        let root = temp_artifact_root();
        tokio::fs::create_dir_all(&root).await?;
        let image = root.join("image.png");
        let document = root.join("result.txt");
        tokio::fs::write(&image, b"png").await?;
        tokio::fs::write(&document, b"text").await?;
        let store = SqliteRuntimeStore::open_in_memory().await?;
        enqueue_file(
            &store,
            "photo",
            &image,
            "image/png",
            3,
            format!("{:x}", Sha256::digest(b"png")),
        )
        .await?;
        enqueue_file(
            &store,
            "document",
            &document,
            "text/plain",
            4,
            format!("{:x}", Sha256::digest(b"text")),
        )
        .await?;
        let api = RecordingApi::default();
        let worker = TelegramDeliveryWorker::new(
            store,
            api.clone(),
            ManualClock::new(10),
            "telegram:900",
            "worker-1",
            root.clone(),
        );

        assert_eq!(worker.run_once().await?, 2);
        let mut kinds = api
            .files
            .lock()
            .unwrap()
            .iter()
            .map(|(kind, _)| *kind)
            .collect::<Vec<_>>();
        kinds.sort_unstable();
        assert_eq!(kinds, vec!["document", "photo"]);
        tokio::fs::remove_dir_all(root).await?;
        Ok(())
    }

    #[tokio::test]
    async fn invalid_managed_file_fails_terminally_without_transport() -> Result<()> {
        let root = temp_artifact_root();
        tokio::fs::create_dir_all(&root).await?;
        let file = root.join("result.txt");
        tokio::fs::write(&file, b"actual").await?;
        let store = SqliteRuntimeStore::open_in_memory().await?;
        enqueue_file(&store, "bad-file", &file, "text/plain", 6, "0".repeat(64)).await?;
        let api = RecordingApi::default();
        let clock = ManualClock::new(10);
        let worker = TelegramDeliveryWorker::new(
            store,
            api.clone(),
            clock.clone(),
            "telegram:900",
            "worker-1",
            root.clone(),
        );

        assert_eq!(worker.run_once().await?, 1);
        clock.advance(60_000);
        assert_eq!(worker.run_once().await?, 0);
        assert!(api.files.lock().unwrap().is_empty());
        tokio::fs::remove_dir_all(root).await?;
        Ok(())
    }

    #[tokio::test]
    async fn validated_file_handle_is_pinned_across_path_replacement() -> Result<()> {
        let root = temp_artifact_root();
        tokio::fs::create_dir_all(&root).await?;
        let path = root.join("result.txt");
        tokio::fs::write(&path, b"validated").await?;
        let mut file = validate_managed_file(
            &root,
            &path,
            9,
            &format!("{:x}", Sha256::digest(b"validated")),
        )
        .await?;

        let replacement = root.join("replacement.txt");
        tokio::fs::write(&replacement, b"replaced!").await?;
        tokio::fs::rename(&replacement, &path).await?;
        let mut uploaded = Vec::new();
        file.read_to_end(&mut uploaded).await?;

        assert_eq!(uploaded, b"validated");
        tokio::fs::remove_dir_all(root).await?;
        Ok(())
    }

    #[test]
    fn exponential_retry_is_deterministic_and_capped() {
        let first = retry_at(Timestamp(100), 1, None, "delivery-1");
        assert!((1_100..1_350).contains(&first.0));
        assert_eq!(first, retry_at(Timestamp(100), 1, None, "delivery-1"));

        let capped = retry_at(Timestamp(100), 99, None, "delivery-1");
        assert!((300_100..301_100).contains(&capped.0));
        assert_eq!(
            retry_at(
                Timestamp(100),
                99,
                Some(Duration::from_secs(5)),
                "delivery-1"
            ),
            Timestamp(5_100)
        );
    }

    #[tokio::test]
    async fn worker_limits_global_transport_concurrency_to_four() -> Result<()> {
        let store = SqliteRuntimeStore::open_in_memory().await?;
        for index in 0..8 {
            store
                .enqueue_gateway_delivery(
                    NewGatewayDelivery::new(
                        format!("concurrent:{index}"),
                        None,
                        0,
                        route(&format!("address:{index}"))?,
                        OutboxPayload::Text {
                            text: format!("message {index}"),
                        },
                    )?,
                    Timestamp(1),
                )
                .await?;
        }
        let api = RecordingApi::with_delay(Duration::from_millis(20));
        let worker = TelegramDeliveryWorker::new(
            store,
            api.clone(),
            ManualClock::new(10),
            "telegram:900",
            "worker-1",
            std::env::temp_dir(),
        );

        assert_eq!(worker.run_once().await?, 8);
        assert_eq!(api.max_active.load(Ordering::SeqCst), 4);
        Ok(())
    }

    #[tokio::test]
    async fn worker_exits_immediately_when_shutdown_is_already_requested() -> Result<()> {
        let store = SqliteRuntimeStore::open_in_memory().await?;
        enqueue_text(&store, "shutdown").await?;
        let api = RecordingApi::default();
        let worker = TelegramDeliveryWorker::new(
            store,
            api.clone(),
            ManualClock::new(10),
            "telegram:900",
            "worker-1",
            std::env::temp_dir(),
        );
        let (_sender, receiver) = tokio::sync::watch::channel(true);

        worker.run(receiver).await?;
        assert!(api.messages.lock().unwrap().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn first_final_text_edits_known_stream_and_later_text_sends() -> Result<()> {
        let store = SqliteRuntimeStore::open_in_memory().await?;
        let api = RecordingApi::default();
        let worker = TelegramDeliveryWorker::new(
            store,
            api.clone(),
            ManualClock::new(10),
            "telegram:900",
            "worker-1",
            std::env::temp_dir(),
        );
        let mut delivery = crate::runtime::gateway::ClaimedGatewayDelivery {
            claim: crate::runtime::gateway::GatewayDeliveryClaim {
                id: crate::runtime::model::GatewayDeliveryId::new(),
                owner: "worker-1".into(),
                expires_at: Timestamp(1_000),
            },
            intent_key: "final:0".into(),
            source_outbox_id: None,
            work_item_id: Some(crate::runtime::model::WorkItemId::new()),
            ordinal: 0,
            route: route("100")?,
            payload: OutboxPayload::Text {
                text: "final".into(),
            },
            attempt_count: 1,
            remote_message_id: None,
        };

        let edited = worker
            .send_payload(&delivery, Some(77))
            .await
            .expect("stream edit succeeds");
        assert!(edited.edited_stream);
        assert_eq!(*api.edits.lock().unwrap(), vec![(77, "final".into())]);
        assert!(api.messages.lock().unwrap().is_empty());

        delivery.ordinal = 1;
        delivery.payload = OutboxPayload::Text {
            text: "continued".into(),
        };
        let sent = worker
            .send_payload(&delivery, None)
            .await
            .expect("later chunk send succeeds");
        assert!(!sent.edited_stream);
        assert_eq!(
            *api.messages.lock().unwrap(),
            vec![("100".into(), "continued".into())]
        );
        Ok(())
    }

    #[tokio::test]
    async fn terminal_stream_edit_failure_falls_back_to_durable_send() -> Result<()> {
        let store = SqliteRuntimeStore::open_in_memory().await?;
        let api = RecordingApi::default();
        api.terminal_edit.store(true, Ordering::SeqCst);
        let worker = TelegramDeliveryWorker::new(
            store,
            api.clone(),
            ManualClock::new(10),
            "telegram:900",
            "worker-1",
            std::env::temp_dir(),
        );
        let delivery = crate::runtime::gateway::ClaimedGatewayDelivery {
            claim: crate::runtime::gateway::GatewayDeliveryClaim {
                id: crate::runtime::model::GatewayDeliveryId::new(),
                owner: "worker-1".into(),
                expires_at: Timestamp(1_000),
            },
            intent_key: "final:fallback".into(),
            source_outbox_id: None,
            work_item_id: Some(crate::runtime::model::WorkItemId::new()),
            ordinal: 0,
            route: route("100")?,
            payload: OutboxPayload::Text {
                text: "durable fallback".into(),
            },
            attempt_count: 1,
            remote_message_id: None,
        };

        assert!(matches!(
            worker.send_payload(&delivery, Some(77)).await,
            Err(super::DeliveryError::StreamEditTerminal)
        ));
        let sent = worker
            .send_payload(&delivery, None)
            .await
            .expect("terminal edit failure falls back to send");
        assert!(!sent.edited_stream);
        assert_eq!(
            *api.messages.lock().unwrap(),
            vec![("100".into(), "durable fallback".into())]
        );
        Ok(())
    }
}
