use std::{sync::Arc, time::Duration};

use anyhow::{Result, anyhow};
use tokio::sync::watch;

use crate::interfaces::telegram::{
    api::{GetUpdates, TelegramApiErrorClass, TelegramIngressApi},
    ingress::TelegramIngress,
};

const POLL_TIMEOUT_SECONDS: u64 = 25;
const POLL_LIMIT: u8 = 100;

pub struct TelegramPollingWorker<A, I> {
    api: A,
    ingress: Arc<I>,
}

impl<A, I> TelegramPollingWorker<A, I>
where
    A: TelegramIngressApi,
    I: TelegramIngress,
{
    pub fn new(api: A, ingress: Arc<I>) -> Self {
        Self { api, ingress }
    }

    pub async fn run(self, mut shutdown: watch::Receiver<bool>) -> Result<()> {
        let mut offset = None;
        let mut failures = 0;

        'poll: loop {
            if *shutdown.borrow() {
                return Ok(());
            }
            let command = GetUpdates {
                offset,
                timeout: POLL_TIMEOUT_SECONDS,
                limit: POLL_LIMIT,
                allowed_updates: vec!["message".into()],
            };
            let response = tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return Ok(());
                    }
                    continue;
                }
                response = self.api.get_updates(command) => response,
            };
            let mut updates = match response {
                Ok(updates) => {
                    failures = 0;
                    updates
                }
                Err(error) => {
                    let retry_after = match error.class() {
                        TelegramApiErrorClass::Retryable { retry_after } => retry_after,
                        TelegramApiErrorClass::Terminal | TelegramApiErrorClass::OutcomeUnknown => {
                            return Err(anyhow!(error));
                        }
                    };
                    if wait_before_retry(&mut shutdown, retry_delay(failures, retry_after)).await {
                        return Ok(());
                    }
                    failures = failures.saturating_add(1);
                    continue;
                }
            };
            updates.sort_by_key(|update| update.update_id);
            for update in updates {
                let update_id = update.update_id;
                if self.ingress.handle(update).await.is_err() {
                    offset = Some(offset.map_or(update_id, |current: i64| current.max(update_id)));
                    if wait_before_retry(&mut shutdown, retry_delay(failures, None)).await {
                        return Ok(());
                    }
                    failures = failures.saturating_add(1);
                    continue 'poll;
                }
                let next = update_id
                    .checked_add(1)
                    .ok_or_else(|| anyhow!("Telegram update ID overflow"))?;
                offset = Some(offset.map_or(next, |current: i64| current.max(next)));
            }
        }
    }
}

async fn wait_before_retry(shutdown: &mut watch::Receiver<bool>, delay: Duration) -> bool {
    tokio::select! {
        _ = tokio::time::sleep(delay) => false,
        changed = shutdown.changed() => changed.is_err() || *shutdown.borrow(),
    }
}

fn retry_delay(failures: u32, retry_after: Option<Duration>) -> Duration {
    retry_after.unwrap_or_else(|| {
        Duration::from_secs(1_u64.checked_shl(failures).unwrap_or(u64::MAX).min(30))
    })
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        sync::{Arc, Mutex},
        time::Duration,
    };

    use anyhow::{Result, bail};
    use async_trait::async_trait;
    use tokio::sync::watch;

    use super::{TelegramPollingWorker, retry_delay};
    use crate::interfaces::telegram::{
        api::{
            DeleteWebhook, GetUpdates, SetWebhook, TelegramApiError, TelegramApiErrorClass,
            TelegramIngressApi, WebhookInfo,
        },
        ingress::{TelegramIngress, TelegramIngressOutcome},
        types::{TelegramBot, TelegramUpdate},
    };

    enum ScriptedResponse {
        Updates(Vec<TelegramUpdate>),
        Error(TelegramApiErrorClass),
        Pending,
    }

    #[derive(Clone)]
    struct ScriptedApi {
        responses: Arc<Mutex<VecDeque<ScriptedResponse>>>,
        commands: Arc<Mutex<Vec<GetUpdates>>>,
        shutdown: watch::Sender<bool>,
    }

    #[async_trait]
    impl TelegramIngressApi for ScriptedApi {
        async fn get_me(&self) -> std::result::Result<TelegramBot, TelegramApiError> {
            unreachable!()
        }

        async fn set_webhook(
            &self,
            _command: SetWebhook,
        ) -> std::result::Result<(), TelegramApiError> {
            unreachable!()
        }

        async fn delete_webhook(
            &self,
            _command: DeleteWebhook,
        ) -> std::result::Result<(), TelegramApiError> {
            unreachable!()
        }

        async fn get_webhook_info(&self) -> std::result::Result<WebhookInfo, TelegramApiError> {
            unreachable!()
        }

        async fn get_updates(
            &self,
            command: GetUpdates,
        ) -> std::result::Result<Vec<TelegramUpdate>, TelegramApiError> {
            self.commands.lock().unwrap().push(command);
            let response = self.responses.lock().unwrap().pop_front();
            match response {
                Some(ScriptedResponse::Updates(batch)) => Ok(batch),
                Some(ScriptedResponse::Error(class)) => {
                    Err(TelegramApiError::classified(class, "getUpdates", "failed"))
                }
                Some(ScriptedResponse::Pending) => std::future::pending().await,
                None => {
                    self.shutdown.send_replace(true);
                    Ok(Vec::new())
                }
            }
        }
    }

    struct RecordingIngress {
        handled: Arc<Mutex<Vec<i64>>>,
        fail_once: Mutex<Option<i64>>,
    }

    #[async_trait]
    impl TelegramIngress for RecordingIngress {
        async fn handle(&self, update: TelegramUpdate) -> Result<TelegramIngressOutcome> {
            self.handled.lock().unwrap().push(update.update_id);
            let mut fail_once = self.fail_once.lock().unwrap();
            if *fail_once == Some(update.update_id) {
                *fail_once = None;
                bail!("durable ingress failed")
            }
            Ok(TelegramIngressOutcome::Unsupported)
        }
    }

    fn update(update_id: i64) -> TelegramUpdate {
        TelegramUpdate {
            update_id,
            message: None,
        }
    }

    #[tokio::test]
    async fn sorts_updates_and_advances_offset_after_each_success() -> Result<()> {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let commands = Arc::new(Mutex::new(Vec::new()));
        let api = ScriptedApi {
            responses: Arc::new(Mutex::new(VecDeque::from([ScriptedResponse::Updates(
                vec![update(3), update(1), update(2)],
            )]))),
            commands: commands.clone(),
            shutdown: shutdown_tx,
        };
        let handled = Arc::new(Mutex::new(Vec::new()));
        let ingress = Arc::new(RecordingIngress {
            handled: handled.clone(),
            fail_once: Mutex::new(None),
        });

        TelegramPollingWorker::new(api, ingress)
            .run(shutdown_rx)
            .await?;

        assert_eq!(*handled.lock().unwrap(), vec![1, 2, 3]);
        let offsets = commands
            .lock()
            .unwrap()
            .iter()
            .map(|command| command.offset)
            .collect::<Vec<_>>();
        assert_eq!(offsets, vec![None, Some(4)]);
        Ok(())
    }

    #[tokio::test]
    async fn retries_failed_ingress_from_its_update_id() -> Result<()> {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let commands = Arc::new(Mutex::new(Vec::new()));
        let api = ScriptedApi {
            responses: Arc::new(Mutex::new(VecDeque::from([
                ScriptedResponse::Updates(vec![update(2), update(1)]),
                ScriptedResponse::Updates(vec![update(2)]),
            ]))),
            commands: commands.clone(),
            shutdown: shutdown_tx,
        };
        let handled = Arc::new(Mutex::new(Vec::new()));
        let ingress = Arc::new(RecordingIngress {
            handled: handled.clone(),
            fail_once: Mutex::new(Some(2)),
        });

        TelegramPollingWorker::new(api, ingress)
            .run(shutdown_rx)
            .await?;

        assert_eq!(*handled.lock().unwrap(), vec![1, 2, 2]);
        let offsets = commands
            .lock()
            .unwrap()
            .iter()
            .map(|command| command.offset)
            .collect::<Vec<_>>();
        assert_eq!(offsets, vec![None, Some(2), Some(3)]);
        Ok(())
    }

    #[tokio::test]
    async fn shutdown_interrupts_retry_after_backoff() -> Result<()> {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let commands = Arc::new(Mutex::new(Vec::new()));
        let api = ScriptedApi {
            responses: Arc::new(Mutex::new(VecDeque::from([
                ScriptedResponse::Error(TelegramApiErrorClass::Retryable {
                    retry_after: Some(Duration::from_secs(60)),
                }),
                ScriptedResponse::Pending,
            ]))),
            commands: commands.clone(),
            shutdown: shutdown_tx.clone(),
        };
        let ingress = Arc::new(RecordingIngress {
            handled: Arc::new(Mutex::new(Vec::new())),
            fail_once: Mutex::new(None),
        });
        let task = tokio::spawn(TelegramPollingWorker::new(api, ingress).run(shutdown_rx));
        while commands.lock().unwrap().is_empty() {
            tokio::task::yield_now().await;
        }

        shutdown_tx.send_replace(true);

        tokio::time::timeout(Duration::from_secs(1), task).await???;
        Ok(())
    }

    #[tokio::test]
    async fn shutdown_interrupts_long_poll() -> Result<()> {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let commands = Arc::new(Mutex::new(Vec::new()));
        let api = ScriptedApi {
            responses: Arc::new(Mutex::new(VecDeque::from([ScriptedResponse::Pending]))),
            commands: commands.clone(),
            shutdown: shutdown_tx.clone(),
        };
        let ingress = Arc::new(RecordingIngress {
            handled: Arc::new(Mutex::new(Vec::new())),
            fail_once: Mutex::new(None),
        });
        let task = tokio::spawn(TelegramPollingWorker::new(api, ingress).run(shutdown_rx));
        while commands.lock().unwrap().is_empty() {
            tokio::task::yield_now().await;
        }

        shutdown_tx.send_replace(true);

        tokio::time::timeout(Duration::from_secs(1), task).await???;
        Ok(())
    }

    #[test]
    fn retry_delay_is_capped_and_retry_after_wins() {
        assert_eq!(retry_delay(0, None), Duration::from_secs(1));
        assert_eq!(retry_delay(1, None), Duration::from_secs(2));
        assert_eq!(retry_delay(4, None), Duration::from_secs(16));
        assert_eq!(retry_delay(5, None), Duration::from_secs(30));
        assert_eq!(retry_delay(20, None), Duration::from_secs(30));
        assert_eq!(
            retry_delay(1, Some(Duration::from_secs(17))),
            Duration::from_secs(17)
        );
    }
}
