use std::{io::Write, path::PathBuf, sync::Mutex};

use anyhow::Result;
use serde::Serialize;

use crate::runtime::model::{
    ActorId, AttemptId, DeliveryId, OutboxId, RequestId, RunId, WorkItemId,
};

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeComponent {
    Startup,
    Ipc,
    Dispatcher,
    Outbox,
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
            lease_generation: None,
            transition,
            latency_ms: None,
            error_class: None,
            database_path: None,
            socket_path: None,
            schema_version: None,
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
    use crate::runtime::{
        RequestId,
        observability::{RuntimeComponent, RuntimeLogEvent, RuntimeTransition},
    };

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
        for forbidden in ["prompt", "model_text", "tool_payload", "outbox_payload"] {
            assert!(!json.contains(forbidden));
        }
    }
}
