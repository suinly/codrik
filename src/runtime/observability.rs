use std::{io::Write, path::PathBuf, sync::Mutex};

use anyhow::Result;
use serde::Serialize;

use crate::runtime::model::{
    ActorId, AttemptId, DeliveryId, GatewayDeliveryId, OutboxId, RequestId, RunId, WorkItemId,
};

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeComponent {
    Startup,
    Ipc,
    Dispatcher,
    Outbox,
    TelegramWebhook,
    TelegramDelivery,
    TelegramStreaming,
    Recovery,
    Supervisor,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeTransition {
    Starting,
    Recovered,
    Ready,
    Accepted,
    ShuttingDown,
    FailedTerminal,
    OutcomeUnknown,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeErrorClass {
    Configuration,
    AuthorityUnavailable,
    ComponentExit,
    Protocol,
    MalformedDurableState,
    UnknownExternalOutcome,
    WorkFailure,
}

#[derive(Debug, Default, Serialize)]
pub struct RuntimeRecoveryCounts {
    pub expired_actor_leases: u64,
    pub expired_bundle_claims: u64,
    pub orphaned_running_attempts: u64,
}

#[derive(Debug, Serialize)]
pub struct RuntimeLogEvent {
    pub component: RuntimeComponent,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub actor_id: Option<ActorId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub work_item_id: Option<WorkItemId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_id: Option<RunId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<RequestId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attempt_id: Option<AttemptId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub outbox_id: Option<OutboxId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delivery_id: Option<DeliveryId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gateway_delivery_id: Option<GatewayDeliveryId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gateway_update_id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lease_generation: Option<i64>,
    pub transition: RuntimeTransition,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latency_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_class: Option<RuntimeErrorClass>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub database_path: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub socket_path: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_version: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub telegram_bot_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recovery: Option<RuntimeRecoveryCounts>,
}

impl RuntimeLogEvent {
    pub fn transition(component: RuntimeComponent, transition: RuntimeTransition) -> Self {
        Self {
            component,
            actor_id: None,
            work_item_id: None,
            run_id: None,
            request_id: None,
            attempt_id: None,
            outbox_id: None,
            delivery_id: None,
            gateway_delivery_id: None,
            gateway_update_id: None,
            lease_generation: None,
            transition,
            latency_ms: None,
            error_class: None,
            database_path: None,
            socket_path: None,
            schema_version: None,
            telegram_bot_id: None,
            recovery: None,
        }
    }

    pub fn request_transition(
        component: RuntimeComponent,
        request: &RequestId,
        transition: RuntimeTransition,
    ) -> Self {
        let mut event = Self::transition(component, transition);
        event.request_id = Some(request.clone());
        event
    }
}

pub trait RuntimeLogger: Send + Sync {
    fn log(&self, event: &RuntimeLogEvent) -> Result<()>;
}

#[derive(Default)]
pub struct NoopRuntimeLogger;

impl RuntimeLogger for NoopRuntimeLogger {
    fn log(&self, _event: &RuntimeLogEvent) -> Result<()> {
        Ok(())
    }
}

pub struct StderrRuntimeLogger {
    writer: Mutex<Box<dyn Write + Send>>,
}

impl Default for StderrRuntimeLogger {
    fn default() -> Self {
        Self {
            writer: Mutex::new(Box::new(std::io::stderr())),
        }
    }
}

impl StderrRuntimeLogger {
    #[cfg(test)]
    fn with_writer(writer: Box<dyn Write + Send>) -> Self {
        Self {
            writer: Mutex::new(writer),
        }
    }
}

impl RuntimeLogger for StderrRuntimeLogger {
    fn log(&self, event: &RuntimeLogEvent) -> Result<()> {
        let mut writer = self.writer.lock().expect("runtime logger poisoned");
        serde_json::to_writer(&mut *writer, event)?;
        writer.write_all(b"\n")?;
        writer.flush()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io::Write,
        sync::{Arc, Mutex},
    };

    use crate::runtime::{
        RequestId,
        model::{ActorId, AttemptId, DeliveryId, GatewayDeliveryId, OutboxId, RunId, WorkItemId},
        observability::{
            RuntimeComponent, RuntimeErrorClass, RuntimeLogEvent, RuntimeLogger,
            RuntimeRecoveryCounts, RuntimeTransition, StderrRuntimeLogger,
        },
    };

    struct SharedWriter(Arc<Mutex<Vec<u8>>>);

    impl Write for SharedWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn structured_event_contains_ids_without_payload_fields() {
        let request = RequestId::new();
        let json = serde_json::to_string(&RuntimeLogEvent::request_transition(
            RuntimeComponent::Ipc,
            &request,
            RuntimeTransition::Accepted,
        ))
        .unwrap();
        assert!(json.contains(request.as_str()));
        for forbidden in [
            "prompt",
            "model_text",
            "tool_payload",
            "outbox_payload",
            "link_code",
            "code_hash",
            "identity_subject",
        ] {
            assert!(!json.contains(forbidden));
        }
    }

    #[test]
    fn stderr_logger_writes_one_redacted_json_line_with_all_typed_coordinates() {
        let bytes = Arc::new(Mutex::new(Vec::new()));
        let logger = StderrRuntimeLogger::with_writer(Box::new(SharedWriter(bytes.clone())));
        let mut event = RuntimeLogEvent::transition(
            RuntimeComponent::Recovery,
            RuntimeTransition::OutcomeUnknown,
        );
        event.actor_id = Some(ActorId::from_string("actor-id"));
        event.work_item_id = Some(WorkItemId::from_string("work-id"));
        event.run_id = Some(RunId::from_string("run-id"));
        event.request_id = Some(RequestId::new());
        event.attempt_id = Some(AttemptId::from_string("attempt-id"));
        event.outbox_id = Some(OutboxId::from_string("outbox-id"));
        event.delivery_id = Some(DeliveryId::new());
        event.gateway_delivery_id = Some(GatewayDeliveryId::from_string("gateway-delivery-id"));
        event.gateway_update_id = Some(4242);
        event.telegram_bot_id = Some("900".into());
        event.lease_generation = Some(7);
        event.error_class = Some(RuntimeErrorClass::UnknownExternalOutcome);
        event.recovery = Some(RuntimeRecoveryCounts {
            expired_actor_leases: 1,
            expired_bundle_claims: 2,
            orphaned_running_attempts: 3,
        });
        logger.log(&event).unwrap();

        let line = String::from_utf8(bytes.lock().unwrap().clone()).unwrap();
        assert_eq!(line.lines().count(), 1);
        let json: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
        for value in [
            "actor-id",
            "work-id",
            "run-id",
            "attempt-id",
            "outbox-id",
            "gateway-delivery-id",
        ] {
            assert!(line.contains(value));
        }
        assert_eq!(json["gateway_update_id"], 4242);
        assert_eq!(json["telegram_bot_id"], "900");
        assert_eq!(json["error_class"], "unknown_external_outcome");
        assert_eq!(json["recovery"]["orphaned_running_attempts"], 3);
        for forbidden in [
            "secret prompt",
            "secret model",
            "secret tool",
            "secret outbox",
            "prompt",
            "model_text",
            "tool_payload",
            "outbox_payload",
            "link_code",
            "code_hash",
            "identity_subject",
            "/link ABCD-EFGH",
            "123456789:AARealLookingBotToken",
            "real_webhook_secret",
            "private Telegram message",
            "telegram-user-subject-4242",
            "chat-address-4242",
        ] {
            assert!(!line.contains(forbidden));
        }
    }
}
