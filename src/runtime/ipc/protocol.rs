use std::{fmt, io, sync::Arc, time::Duration};

use base64::{Engine, engine::general_purpose::STANDARD};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    time::timeout,
};

use crate::runtime::{
    model::{
        BundleId, CancelId, DeliveryId, MAX_BUNDLE_BYTES, MAX_BUNDLE_DELIVERIES,
        MAX_FINAL_CHUNK_BYTES, MAX_FRAME_BYTES, MAX_MANIFEST_BYTES, MAX_SUBMIT_BYTES, RequestId,
        WorkItemId,
    },
    store::{FinalPayload, ResultBundle},
};

pub const PROTOCOL_VERSION: u8 = 1;
pub const FRAME_HEADER_TIMEOUT: Duration = Duration::from_secs(5);
pub const FRAME_BODY_TIMEOUT: Duration = Duration::from_secs(30);
pub const FRAME_WRITE_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientRequest {
    pub version: u8,
    pub body: ClientRequestBody,
}

impl Serialize for ClientRequest {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        require_v1(self.version).map_err(serde::ser::Error::custom)?;
        validate_client_body(&self.body).map_err(serde::ser::Error::custom)?;
        StrictEnvelopeRef {
            version: self.version,
            body: &self.body,
        }
        .serialize(serializer)
    }
}

impl ClientRequest {
    pub fn new(body: ClientRequestBody) -> Self {
        Self {
            version: PROTOCOL_VERSION,
            body,
        }
    }
}

impl<'de> Deserialize<'de> for ClientRequest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let envelope = StrictEnvelope::<ClientRequestBody>::deserialize(deserializer)?;
        require_v1(envelope.version).map_err(serde::de::Error::custom)?;
        validate_client_body(&envelope.body).map_err(serde::de::Error::custom)?;
        Ok(Self {
            version: envelope.version,
            body: envelope.body,
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ClientRequestBody {
    Submit {
        request_id: RequestId,
        text: String,
    },
    Resume {
        request_id: RequestId,
    },
    AckFinal {
        request_id: RequestId,
        bundle_id: BundleId,
        delivery_ids: Vec<DeliveryId>,
    },
    Cancel {
        request_id: RequestId,
        cancel_id: CancelId,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServerEvent {
    pub version: u8,
    pub body: ServerEventBody,
}

impl Serialize for ServerEvent {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        require_v1(self.version).map_err(serde::ser::Error::custom)?;
        validate_server_body(&self.body).map_err(serde::ser::Error::custom)?;
        StrictEnvelopeRef {
            version: self.version,
            body: &self.body,
        }
        .serialize(serializer)
    }
}

impl ServerEvent {
    pub fn new(body: ServerEventBody) -> Self {
        Self {
            version: PROTOCOL_VERSION,
            body,
        }
    }
}

impl<'de> Deserialize<'de> for ServerEvent {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let envelope = StrictEnvelope::<ServerEventBody>::deserialize(deserializer)?;
        require_v1(envelope.version).map_err(serde::de::Error::custom)?;
        validate_server_body(&envelope.body).map_err(serde::de::Error::custom)?;
        Ok(Self {
            version: envelope.version,
            body: envelope.body,
        })
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StrictEnvelope<T> {
    version: u8,
    body: T,
}

#[derive(Serialize)]
struct StrictEnvelopeRef<'a, T> {
    version: u8,
    body: &'a T,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ServerEventBody {
    Accepted {
        request_id: RequestId,
        work_item_id: WorkItemId,
        sequence: i64,
    },
    CancelAccepted {
        request_id: RequestId,
        cancel_id: CancelId,
        affected_request_ids: Vec<RequestId>,
    },
    AckAccepted {
        request_id: RequestId,
        bundle_id: BundleId,
    },
    Activity {
        request_id: RequestId,
        event: ActivityEvent,
    },
    TextDelta {
        request_id: RequestId,
        delta: String,
    },
    StreamGap {
        request_id: RequestId,
    },
    FinalBegin {
        request_id: RequestId,
        bundle_id: BundleId,
        replay: bool,
        manifest: Vec<FinalManifestEntry>,
    },
    FinalChunk {
        request_id: RequestId,
        bundle_id: BundleId,
        delivery_id: DeliveryId,
        chunk_index: usize,
        bytes_base64: String,
    },
    FinalEnd {
        request_id: RequestId,
        bundle_id: BundleId,
        manifest_sha256: String,
    },
    RequestError {
        request_id: RequestId,
        code: String,
        message: String,
    },
    ProtocolError {
        code: ProtocolErrorCode,
        message: String,
    },
    ServerShuttingDown {
        request_id: Option<RequestId>,
        resume_command: Option<String>,
    },
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ActivityEvent {
    ModelStepStarted,
    Description { description: String },
    ToolStarted { name: String },
    ToolFinished { name: String, succeeded: bool },
    Completed,
    Cancelled,
    Failed,
}

impl<'de> Deserialize<'de> for ActivityEvent {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        let object = value
            .as_object()
            .ok_or_else(|| serde::de::Error::custom("activity event must be an object"))?;
        let kind = object
            .get("type")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| serde::de::Error::custom("activity event type is required"))?;
        let allowed: &[&str] = match kind {
            "model_step_started" | "completed" | "cancelled" | "failed" => &["type"],
            "description" => &["type", "description"],
            "tool_started" => &["type", "name"],
            "tool_finished" => &["type", "name", "succeeded"],
            _ => return Err(serde::de::Error::custom("unknown activity event type")),
        };
        if object.keys().any(|key| !allowed.contains(&key.as_str())) {
            return Err(serde::de::Error::custom(
                "activity event contains an unknown field",
            ));
        }
        let wire: ActivityEventWire =
            serde_json::from_value(value).map_err(serde::de::Error::custom)?;
        Ok(wire.into())
    }
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ActivityEventWire {
    ModelStepStarted,
    Description { description: String },
    ToolStarted { name: String },
    ToolFinished { name: String, succeeded: bool },
    Completed,
    Cancelled,
    Failed,
}

impl From<ActivityEventWire> for ActivityEvent {
    fn from(value: ActivityEventWire) -> Self {
        match value {
            ActivityEventWire::ModelStepStarted => Self::ModelStepStarted,
            ActivityEventWire::Description { description } => Self::Description { description },
            ActivityEventWire::ToolStarted { name } => Self::ToolStarted { name },
            ActivityEventWire::ToolFinished { name, succeeded } => {
                Self::ToolFinished { name, succeeded }
            }
            ActivityEventWire::Completed => Self::Completed,
            ActivityEventWire::Cancelled => Self::Cancelled,
            ActivityEventWire::Failed => Self::Failed,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FinalManifestEntry {
    pub delivery_id: DeliveryId,
    pub payload_kind: String,
    pub decoded_bytes: usize,
    pub sha256: String,
    pub chunk_count: usize,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProtocolErrorCode {
    InvalidUtf8,
    InvalidJson,
    UnsupportedVersion,
    InvalidUuid,
    InvalidRequest,
    ZeroLengthFrame,
    FrameTooLarge,
    IncompleteFrame,
    HeaderTimeout,
    BodyTimeout,
    WriteTimeout,
}

#[derive(Debug)]
pub struct ProtocolFailure {
    code: ProtocolErrorCode,
    message: String,
}

impl ProtocolFailure {
    fn new(code: ProtocolErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    pub fn code(&self) -> ProtocolErrorCode {
        self.code
    }
}

impl fmt::Display for ProtocolFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for ProtocolFailure {}

pub struct FrameReader<R> {
    inner: R,
}

impl<R> FrameReader<R>
where
    R: AsyncRead + Unpin,
{
    pub fn new(inner: R) -> Self {
        Self { inner }
    }

    pub async fn read_client_request(&mut self) -> Result<ClientRequest, ProtocolFailure> {
        let payload = self.read_payload().await?;
        decode_envelope(&payload, ProtocolErrorCode::InvalidRequest)
    }

    pub async fn read_server_event(&mut self) -> Result<ServerEvent, ProtocolFailure> {
        let payload = self.read_payload().await?;
        decode_envelope(&payload, ProtocolErrorCode::InvalidJson)
    }

    pub async fn read_server_event_without_deadline(
        &mut self,
    ) -> Result<ServerEvent, ProtocolFailure> {
        let payload = self.read_payload_without_deadline().await?;
        decode_envelope(&payload, ProtocolErrorCode::InvalidJson)
    }

    async fn read_payload_without_deadline(&mut self) -> Result<Vec<u8>, ProtocolFailure> {
        let mut header = [0_u8; 4];
        self.inner
            .read_exact(&mut header)
            .await
            .map_err(|error| incomplete(error, "frame header is incomplete"))?;
        let length = validate_frame_length(header)?;
        let mut payload = vec![0_u8; length];
        self.inner
            .read_exact(&mut payload)
            .await
            .map_err(|error| incomplete(error, "frame body is incomplete"))?;
        Ok(payload)
    }

    async fn read_payload(&mut self) -> Result<Vec<u8>, ProtocolFailure> {
        let mut header = [0_u8; 4];
        match timeout(FRAME_HEADER_TIMEOUT, self.inner.read_exact(&mut header)).await {
            Err(_) => {
                return Err(ProtocolFailure::new(
                    ProtocolErrorCode::HeaderTimeout,
                    "frame header deadline exceeded",
                ));
            }
            Ok(Err(error)) => return Err(incomplete(error, "frame header is incomplete")),
            Ok(Ok(_)) => {}
        }
        let length = validate_frame_length(header)?;

        // The announced length is checked before this allocation.
        let mut payload = vec![0_u8; length];
        match timeout(FRAME_BODY_TIMEOUT, self.inner.read_exact(&mut payload)).await {
            Err(_) => Err(ProtocolFailure::new(
                ProtocolErrorCode::BodyTimeout,
                "frame body deadline exceeded",
            )),
            Ok(Err(error)) => Err(incomplete(error, "frame body is incomplete")),
            Ok(Ok(_)) => Ok(payload),
        }
    }
}

fn validate_frame_length(header: [u8; 4]) -> Result<usize, ProtocolFailure> {
    let length = u32::from_be_bytes(header) as usize;
    if length == 0 {
        return Err(ProtocolFailure::new(
            ProtocolErrorCode::ZeroLengthFrame,
            "zero-length frames are forbidden",
        ));
    }
    if length > MAX_FRAME_BYTES {
        return Err(ProtocolFailure::new(
            ProtocolErrorCode::FrameTooLarge,
            format!("frame length {length} exceeds {MAX_FRAME_BYTES}"),
        ));
    }
    Ok(length)
}

fn incomplete(error: io::Error, context: &str) -> ProtocolFailure {
    ProtocolFailure::new(
        ProtocolErrorCode::IncompleteFrame,
        format!("{context}: {error}"),
    )
}

pub struct FrameWriter<W> {
    inner: W,
}

impl<W> FrameWriter<W>
where
    W: AsyncWrite + Unpin,
{
    pub fn new(inner: W) -> Self {
        Self { inner }
    }

    pub async fn write_client_request(
        &mut self,
        request: &ClientRequest,
    ) -> Result<(), ProtocolFailure> {
        self.write_json(request).await
    }

    pub async fn write_server_event(&mut self, event: &ServerEvent) -> Result<(), ProtocolFailure> {
        self.write_json(event).await
    }

    async fn write_json<T: Serialize>(&mut self, value: &T) -> Result<(), ProtocolFailure> {
        let payload = serde_json::to_vec(value).map_err(|error| {
            ProtocolFailure::new(
                ProtocolErrorCode::InvalidJson,
                format!("failed to encode JSON frame: {error}"),
            )
        })?;
        if payload.is_empty() {
            return Err(ProtocolFailure::new(
                ProtocolErrorCode::ZeroLengthFrame,
                "zero-length frames are forbidden",
            ));
        }
        if payload.len() > MAX_FRAME_BYTES {
            return Err(ProtocolFailure::new(
                ProtocolErrorCode::FrameTooLarge,
                format!("frame length {} exceeds {MAX_FRAME_BYTES}", payload.len()),
            ));
        }
        let header = (payload.len() as u32).to_be_bytes();
        timeout(FRAME_WRITE_TIMEOUT, async {
            self.inner.write_all(&header).await?;
            self.inner.write_all(&payload).await?;
            self.inner.flush().await
        })
        .await
        .map_err(|_| {
            ProtocolFailure::new(
                ProtocolErrorCode::WriteTimeout,
                "frame write deadline exceeded",
            )
        })?
        .map_err(|error| {
            ProtocolFailure::new(
                ProtocolErrorCode::IncompleteFrame,
                format!("failed to write frame: {error}"),
            )
        })
    }
}

fn decode_envelope<T>(payload: &[u8], schema_error: ProtocolErrorCode) -> Result<T, ProtocolFailure>
where
    T: DeserializeOwned,
{
    let text = std::str::from_utf8(payload).map_err(|error| {
        ProtocolFailure::new(
            ProtocolErrorCode::InvalidUtf8,
            format!("frame is not UTF-8: {error}"),
        )
    })?;
    let value: serde_json::Value = serde_json::from_str(text).map_err(|error| {
        ProtocolFailure::new(
            ProtocolErrorCode::InvalidJson,
            format!("frame is not valid JSON: {error}"),
        )
    })?;
    let version = value.get("version").and_then(serde_json::Value::as_u64);
    if version != Some(u64::from(PROTOCOL_VERSION)) {
        return Err(ProtocolFailure::new(
            ProtocolErrorCode::UnsupportedVersion,
            format!("unsupported protocol version {version:?}"),
        ));
    }
    validate_wire_uuids(&value)?;
    serde_json::from_value(value).map_err(|error| {
        ProtocolFailure::new(
            schema_error,
            format!("frame does not match the protocol schema: {error}"),
        )
    })
}

fn validate_wire_uuids(value: &serde_json::Value) -> Result<(), ProtocolFailure> {
    const UUID_FIELDS: &[&str] = &[
        "request_id",
        "work_item_id",
        "bundle_id",
        "delivery_id",
        "cancel_id",
    ];
    match value {
        serde_json::Value::Object(fields) => {
            for (name, value) in fields {
                if UUID_FIELDS.contains(&name.as_str())
                    && let Some(raw) = value.as_str()
                {
                    parse_wire_uuid(name, raw)?;
                } else if name == "delivery_ids" || name == "affected_request_ids" {
                    if let Some(values) = value.as_array() {
                        for raw in values.iter().filter_map(serde_json::Value::as_str) {
                            parse_wire_uuid(name, raw)?;
                        }
                    }
                }
                validate_wire_uuids(value)?;
            }
        }
        serde_json::Value::Array(values) => {
            for value in values {
                validate_wire_uuids(value)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn parse_wire_uuid(field: &str, raw: &str) -> Result<(), ProtocolFailure> {
    uuid::Uuid::parse_str(raw).map(|_| ()).map_err(|error| {
        ProtocolFailure::new(
            ProtocolErrorCode::InvalidUuid,
            format!("{field} is not a UUID: {error}"),
        )
    })
}

fn require_v1(version: u8) -> Result<(), &'static str> {
    if version == PROTOCOL_VERSION {
        Ok(())
    } else {
        Err("unsupported protocol version")
    }
}

fn validate_client_body(body: &ClientRequestBody) -> Result<(), &'static str> {
    match body {
        ClientRequestBody::Submit { text, .. } if text.trim().is_empty() => {
            Err("submit text must contain non-whitespace content")
        }
        ClientRequestBody::Submit { text, .. } if text.len() > MAX_SUBMIT_BYTES => {
            Err("submit text exceeds the UTF-8 byte limit")
        }
        ClientRequestBody::AckFinal { delivery_ids, .. }
            if delivery_ids.is_empty() || delivery_ids.len() > MAX_BUNDLE_DELIVERIES =>
        {
            Err("ack delivery count is outside the bundle limits")
        }
        _ => Ok(()),
    }
}

fn validate_server_body(body: &ServerEventBody) -> Result<(), &'static str> {
    if let ServerEventBody::Accepted { work_item_id, .. } = body {
        uuid::Uuid::parse_str(work_item_id.as_str())
            .map(|_| ())
            .map_err(|_| "work_item_id is not a UUID")?;
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BundleLimitError {
    DeliveryCount { actual: usize, maximum: usize },
    TextBytes { actual: usize, maximum: usize },
    DecodedBundleBytes { actual: usize, maximum: usize },
    ManifestBytes { actual: usize, maximum: usize },
    EncodedFrameBytes { actual: usize, maximum: usize },
    PayloadEncoding(String),
}

impl fmt::Display for BundleLimitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DeliveryCount { actual, maximum } => {
                write!(
                    formatter,
                    "bundle has {actual} deliveries; maximum is {maximum}"
                )
            }
            Self::TextBytes { actual, maximum } => {
                write!(
                    formatter,
                    "text has {actual} UTF-8 bytes; maximum is {maximum}"
                )
            }
            Self::DecodedBundleBytes { actual, maximum } => write!(
                formatter,
                "bundle has {actual} decoded canonical bytes; maximum is {maximum}"
            ),
            Self::ManifestBytes { actual, maximum } => {
                write!(
                    formatter,
                    "manifest has {actual} bytes; maximum is {maximum}"
                )
            }
            Self::EncodedFrameBytes { actual, maximum } => {
                write!(
                    formatter,
                    "encoded frame has {actual} bytes; maximum is {maximum}"
                )
            }
            Self::PayloadEncoding(message) => formatter.write_str(message),
        }
    }
}

impl std::error::Error for BundleLimitError {}

#[derive(Clone)]
struct EncodedDelivery {
    id: DeliveryId,
    bytes: Vec<u8>,
    entry: FinalManifestEntry,
}

#[derive(Clone)]
pub(crate) struct PreparedBundle {
    request_id: RequestId,
    bundle_id: BundleId,
    replay: bool,
    manifest: Vec<FinalManifestEntry>,
    manifest_sha256: String,
    deliveries: Arc<Vec<EncodedDelivery>>,
}

impl PreparedBundle {
    pub(crate) fn events(&self) -> impl Iterator<Item = ServerEvent> + '_ {
        let begin = ServerEvent::new(ServerEventBody::FinalBegin {
            request_id: self.request_id.clone(),
            bundle_id: self.bundle_id.clone(),
            replay: self.replay,
            manifest: self.manifest.clone(),
        });
        let chunks = self.deliveries.iter().flat_map(|delivery| {
            delivery
                .bytes
                .chunks(MAX_FINAL_CHUNK_BYTES)
                .enumerate()
                .map(|(chunk_index, chunk)| {
                    ServerEvent::new(ServerEventBody::FinalChunk {
                        request_id: self.request_id.clone(),
                        bundle_id: self.bundle_id.clone(),
                        delivery_id: delivery.id.clone(),
                        chunk_index,
                        bytes_base64: STANDARD.encode(chunk),
                    })
                })
        });
        let end = ServerEvent::new(ServerEventBody::FinalEnd {
            request_id: self.request_id.clone(),
            bundle_id: self.bundle_id.clone(),
            manifest_sha256: self.manifest_sha256.clone(),
        });
        std::iter::once(begin)
            .chain(chunks)
            .chain(std::iter::once(end))
    }
}

pub(crate) fn prepare_bundle(
    bundle: &ResultBundle,
    replay: bool,
) -> Result<PreparedBundle, BundleLimitError> {
    let count = bundle.deliveries.len();
    if count == 0 || count > MAX_BUNDLE_DELIVERIES {
        return Err(BundleLimitError::DeliveryCount {
            actual: count,
            maximum: MAX_BUNDLE_DELIVERIES,
        });
    }

    let mut decoded_total = 0usize;
    let mut encoded = Vec::with_capacity(count);
    for (delivery_id, payload) in &bundle.deliveries {
        if let FinalPayload::Text { text } = payload
            && text.len() > MAX_BUNDLE_BYTES
        {
            return Err(BundleLimitError::TextBytes {
                actual: text.len(),
                maximum: MAX_BUNDLE_BYTES,
            });
        }
        let bytes = serde_json::to_vec(payload)
            .map_err(|error| BundleLimitError::PayloadEncoding(error.to_string()))?;
        decoded_total =
            decoded_total
                .checked_add(bytes.len())
                .ok_or(BundleLimitError::DecodedBundleBytes {
                    actual: usize::MAX,
                    maximum: MAX_BUNDLE_BYTES,
                })?;
        if decoded_total > MAX_BUNDLE_BYTES {
            return Err(BundleLimitError::DecodedBundleBytes {
                actual: decoded_total,
                maximum: MAX_BUNDLE_BYTES,
            });
        }
        let entry = FinalManifestEntry {
            delivery_id: delivery_id.clone(),
            payload_kind: payload_kind(payload).into(),
            decoded_bytes: bytes.len(),
            sha256: incremental_sha256(&bytes),
            chunk_count: bytes.len().div_ceil(MAX_FINAL_CHUNK_BYTES),
        };
        encoded.push(EncodedDelivery {
            id: delivery_id.clone(),
            bytes,
            entry,
        });
    }

    let manifest: Vec<_> = encoded.iter().map(|item| item.entry.clone()).collect();
    let canonical_manifest = serde_json::to_vec(&manifest)
        .map_err(|error| BundleLimitError::PayloadEncoding(error.to_string()))?;
    if canonical_manifest.len() > MAX_MANIFEST_BYTES {
        return Err(BundleLimitError::ManifestBytes {
            actual: canonical_manifest.len(),
            maximum: MAX_MANIFEST_BYTES,
        });
    }
    let manifest_sha256 = incremental_sha256(&canonical_manifest);

    let prepared = PreparedBundle {
        request_id: bundle.request_id.clone(),
        bundle_id: bundle.id.clone(),
        replay,
        manifest,
        manifest_sha256,
        deliveries: Arc::new(encoded),
    };
    for frame in prepared.events() {
        let actual = serde_json::to_vec(&frame)
            .map_err(|error| BundleLimitError::PayloadEncoding(error.to_string()))?
            .len();
        if actual >= MAX_FRAME_BYTES {
            return Err(BundleLimitError::EncodedFrameBytes {
                actual,
                maximum: MAX_FRAME_BYTES - 1,
            });
        }
    }
    Ok(prepared)
}

pub fn encode_bundle(
    bundle: &ResultBundle,
    replay: bool,
) -> Result<Vec<ServerEvent>, BundleLimitError> {
    Ok(prepare_bundle(bundle, replay)?.events().collect())
}

fn payload_kind(payload: &FinalPayload) -> &'static str {
    match payload {
        FinalPayload::Text { .. } => "text",
        FinalPayload::File { .. } => "file",
        FinalPayload::TerminalError { .. } => "terminal_error",
    }
}

fn incremental_sha256(bytes: &[u8]) -> String {
    let mut digest = Sha256::new();
    for chunk in bytes.chunks(MAX_FINAL_CHUNK_BYTES) {
        digest.update(chunk);
    }
    format!("{:x}", digest.finalize())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use std::time::Duration;

    use anyhow::Result;
    use base64::{Engine, engine::general_purpose::STANDARD};
    use sha2::{Digest, Sha256};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::{
        ActivityEvent, BundleLimitError, ClientRequest, ClientRequestBody, FinalManifestEntry,
        FrameReader, FrameWriter, ProtocolErrorCode, ServerEvent, ServerEventBody, encode_bundle,
        prepare_bundle,
    };
    use crate::runtime::{
        ArtifactId, BundleId, BundleState, CancelId, DeliveryId, RequestId,
        model::{MAX_BUNDLE_BYTES, MAX_FINAL_CHUNK_BYTES, MAX_FRAME_BYTES, WorkItemId},
        store::{BundleManifest, FinalPayload, ManagedArtifact, ResultBundle},
    };

    fn request_id() -> RequestId {
        RequestId::parse("0190f2ef-0000-7000-8000-000000000001").unwrap()
    }

    fn bundle_id() -> BundleId {
        BundleId::parse("0190f2ef-0000-7000-8000-000000000002").unwrap()
    }

    fn delivery_id() -> DeliveryId {
        DeliveryId::parse("0190f2ef-0000-7000-8000-000000000003").unwrap()
    }

    fn cancel_id() -> CancelId {
        CancelId::parse("0190f2ef-0000-7000-8000-000000000004").unwrap()
    }

    fn entry() -> FinalManifestEntry {
        FinalManifestEntry {
            delivery_id: delivery_id(),
            payload_kind: "text".into(),
            decoded_bytes: 16,
            sha256: "a".repeat(64),
            chunk_count: 1,
        }
    }

    #[test]
    fn every_client_request_round_trips_with_a_v1_envelope() -> Result<()> {
        let values = [
            ClientRequest::new(ClientRequestBody::Submit {
                request_id: request_id(),
                text: "hello".into(),
            }),
            ClientRequest::new(ClientRequestBody::Resume {
                request_id: request_id(),
            }),
            ClientRequest::new(ClientRequestBody::AckFinal {
                request_id: request_id(),
                bundle_id: bundle_id(),
                delivery_ids: vec![delivery_id()],
            }),
            ClientRequest::new(ClientRequestBody::Cancel {
                request_id: request_id(),
                cancel_id: cancel_id(),
            }),
        ];
        let expected = [
            r#"{"version":1,"body":{"type":"submit","request_id":"0190f2ef-0000-7000-8000-000000000001","text":"hello"}}"#,
            r#"{"version":1,"body":{"type":"resume","request_id":"0190f2ef-0000-7000-8000-000000000001"}}"#,
            r#"{"version":1,"body":{"type":"ack_final","request_id":"0190f2ef-0000-7000-8000-000000000001","bundle_id":"0190f2ef-0000-7000-8000-000000000002","delivery_ids":["0190f2ef-0000-7000-8000-000000000003"]}}"#,
            r#"{"version":1,"body":{"type":"cancel","request_id":"0190f2ef-0000-7000-8000-000000000001","cancel_id":"0190f2ef-0000-7000-8000-000000000004"}}"#,
        ];
        for (value, expected) in values.into_iter().zip(expected) {
            let json = serde_json::to_vec(&value)?;
            assert_eq!(std::str::from_utf8(&json)?, expected);
            assert_eq!(serde_json::from_slice::<ClientRequest>(&json)?, value);
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&json)?["version"],
                1
            );
            assert!(
                serde_json::from_slice::<serde_json::Value>(&json)?["body"]["type"].is_string()
            );
        }
        Ok(())
    }

    #[test]
    fn every_server_event_round_trips_with_a_v1_envelope() -> Result<()> {
        let values = [
            ServerEvent::new(ServerEventBody::Accepted {
                request_id: request_id(),
                work_item_id: WorkItemId::from_string("0190f2ef-0000-7000-8000-000000000005"),
                sequence: 7,
            }),
            ServerEvent::new(ServerEventBody::CancelAccepted {
                request_id: request_id(),
                cancel_id: cancel_id(),
                affected_request_ids: vec![request_id()],
            }),
            ServerEvent::new(ServerEventBody::AckAccepted {
                request_id: request_id(),
                bundle_id: bundle_id(),
            }),
            ServerEvent::new(ServerEventBody::Activity {
                request_id: request_id(),
                event: ActivityEvent::ToolFinished {
                    name: "bash".into(),
                    succeeded: true,
                },
            }),
            ServerEvent::new(ServerEventBody::TextDelta {
                request_id: request_id(),
                delta: "part".into(),
            }),
            ServerEvent::new(ServerEventBody::StreamGap {
                request_id: request_id(),
            }),
            ServerEvent::new(ServerEventBody::FinalBegin {
                request_id: request_id(),
                bundle_id: bundle_id(),
                replay: false,
                manifest: vec![entry()],
            }),
            ServerEvent::new(ServerEventBody::FinalChunk {
                request_id: request_id(),
                bundle_id: bundle_id(),
                delivery_id: delivery_id(),
                chunk_index: 0,
                bytes_base64: "eA==".into(),
            }),
            ServerEvent::new(ServerEventBody::FinalEnd {
                request_id: request_id(),
                bundle_id: bundle_id(),
                manifest_sha256: "b".repeat(64),
            }),
            ServerEvent::new(ServerEventBody::RequestError {
                request_id: request_id(),
                code: "missing_request".into(),
                message: "not found".into(),
            }),
            ServerEvent::new(ServerEventBody::ProtocolError {
                code: ProtocolErrorCode::InvalidJson,
                message: "bad json".into(),
            }),
            ServerEvent::new(ServerEventBody::ServerShuttingDown {
                request_id: Some(request_id()),
                resume_command: Some("codrik resume id".into()),
            }),
        ];
        let hash_a = "a".repeat(64);
        let hash_b = "b".repeat(64);
        let expected = [
            r#"{"version":1,"body":{"type":"accepted","request_id":"0190f2ef-0000-7000-8000-000000000001","work_item_id":"0190f2ef-0000-7000-8000-000000000005","sequence":7}}"#.to_string(),
            r#"{"version":1,"body":{"type":"cancel_accepted","request_id":"0190f2ef-0000-7000-8000-000000000001","cancel_id":"0190f2ef-0000-7000-8000-000000000004","affected_request_ids":["0190f2ef-0000-7000-8000-000000000001"]}}"#.to_string(),
            r#"{"version":1,"body":{"type":"ack_accepted","request_id":"0190f2ef-0000-7000-8000-000000000001","bundle_id":"0190f2ef-0000-7000-8000-000000000002"}}"#.to_string(),
            r#"{"version":1,"body":{"type":"activity","request_id":"0190f2ef-0000-7000-8000-000000000001","event":{"type":"tool_finished","name":"bash","succeeded":true}}}"#.to_string(),
            r#"{"version":1,"body":{"type":"text_delta","request_id":"0190f2ef-0000-7000-8000-000000000001","delta":"part"}}"#.to_string(),
            r#"{"version":1,"body":{"type":"stream_gap","request_id":"0190f2ef-0000-7000-8000-000000000001"}}"#.to_string(),
            format!(r#"{{"version":1,"body":{{"type":"final_begin","request_id":"0190f2ef-0000-7000-8000-000000000001","bundle_id":"0190f2ef-0000-7000-8000-000000000002","replay":false,"manifest":[{{"delivery_id":"0190f2ef-0000-7000-8000-000000000003","payload_kind":"text","decoded_bytes":16,"sha256":"{hash_a}","chunk_count":1}}]}}}}"#),
            r#"{"version":1,"body":{"type":"final_chunk","request_id":"0190f2ef-0000-7000-8000-000000000001","bundle_id":"0190f2ef-0000-7000-8000-000000000002","delivery_id":"0190f2ef-0000-7000-8000-000000000003","chunk_index":0,"bytes_base64":"eA=="}}"#.to_string(),
            format!(r#"{{"version":1,"body":{{"type":"final_end","request_id":"0190f2ef-0000-7000-8000-000000000001","bundle_id":"0190f2ef-0000-7000-8000-000000000002","manifest_sha256":"{hash_b}"}}}}"#),
            r#"{"version":1,"body":{"type":"request_error","request_id":"0190f2ef-0000-7000-8000-000000000001","code":"missing_request","message":"not found"}}"#.to_string(),
            r#"{"version":1,"body":{"type":"protocol_error","code":"invalid_json","message":"bad json"}}"#.to_string(),
            r#"{"version":1,"body":{"type":"server_shutting_down","request_id":"0190f2ef-0000-7000-8000-000000000001","resume_command":"codrik resume id"}}"#.to_string(),
        ];
        for (value, expected) in values.into_iter().zip(expected) {
            let json = serde_json::to_vec(&value)?;
            assert_eq!(std::str::from_utf8(&json)?, expected);
            assert_eq!(serde_json::from_slice::<ServerEvent>(&json)?, value);
        }
        Ok(())
    }

    #[test]
    fn ack_accepted_has_exact_frozen_v1_json() -> Result<()> {
        let event = ServerEvent::new(ServerEventBody::AckAccepted {
            request_id: request_id(),
            bundle_id: bundle_id(),
        });
        assert_eq!(
            serde_json::to_string(&event)?,
            r#"{"version":1,"body":{"type":"ack_accepted","request_id":"0190f2ef-0000-7000-8000-000000000001","bundle_id":"0190f2ef-0000-7000-8000-000000000002"}}"#
        );
        Ok(())
    }

    #[test]
    fn every_activity_variant_round_trips_strictly() -> Result<()> {
        let values = [
            ActivityEvent::ModelStepStarted,
            ActivityEvent::Description {
                description: "working".into(),
            },
            ActivityEvent::ToolStarted {
                name: "bash".into(),
            },
            ActivityEvent::ToolFinished {
                name: "bash".into(),
                succeeded: true,
            },
            ActivityEvent::Completed,
            ActivityEvent::Cancelled,
            ActivityEvent::Failed,
        ];
        for value in values {
            let json = serde_json::to_vec(&value)?;
            assert_eq!(serde_json::from_slice::<ActivityEvent>(&json)?, value);
        }
        assert!(
            serde_json::from_str::<ActivityEvent>(r#"{"type":"completed","unexpected":true}"#)
                .is_err()
        );
        Ok(())
    }

    #[test]
    fn envelopes_and_tagged_bodies_reject_unknown_fields() {
        let id = request_id();
        let envelope = format!(
            r#"{{"version":1,"body":{{"type":"resume","request_id":"{id}"}},"extra":true}}"#
        );
        let body = format!(
            r#"{{"version":1,"body":{{"type":"resume","request_id":"{id}","extra":true}}}}"#
        );
        assert!(serde_json::from_str::<ClientRequest>(&envelope).is_err());
        assert!(serde_json::from_str::<ClientRequest>(&body).is_err());
    }

    #[test]
    fn accepted_rejects_a_non_uuid_work_item_id() {
        let id = request_id();
        let json = format!(
            r#"{{"version":1,"body":{{"type":"accepted","request_id":"{id}","work_item_id":"not-a-uuid","sequence":1}}}}"#
        );
        assert!(serde_json::from_str::<ServerEvent>(&json).is_err());
    }

    #[test]
    fn serialization_rejects_invalid_envelopes_and_wire_ids() {
        let invalid_version = ClientRequest {
            version: 2,
            body: ClientRequestBody::Resume {
                request_id: request_id(),
            },
        };
        let invalid_work = ServerEvent::new(ServerEventBody::Accepted {
            request_id: request_id(),
            work_item_id: WorkItemId::from_string("not-a-uuid"),
            sequence: 1,
        });
        assert!(serde_json::to_vec(&invalid_version).is_err());
        assert!(serde_json::to_vec(&invalid_work).is_err());
    }

    #[tokio::test]
    async fn frame_writer_uses_a_big_endian_u32_prefix() -> Result<()> {
        let (writer, mut peer) = tokio::io::duplex(256);
        let mut writer = FrameWriter::new(writer);
        let event = ServerEvent::new(ServerEventBody::StreamGap {
            request_id: request_id(),
        });
        writer.write_server_event(&event).await?;
        let expected = serde_json::to_vec(&event)?;
        let mut prefix = [0_u8; 4];
        peer.read_exact(&mut prefix).await?;
        assert_eq!(prefix, (expected.len() as u32).to_be_bytes());
        let mut body = vec![0; expected.len()];
        peer.read_exact(&mut body).await?;
        assert_eq!(body, expected);
        Ok(())
    }

    async fn read_error(bytes: &[u8]) -> ProtocolErrorCode {
        let (mut peer, reader) = tokio::io::duplex(bytes.len().max(8));
        peer.write_all(bytes).await.unwrap();
        peer.shutdown().await.unwrap();
        FrameReader::new(reader)
            .read_client_request()
            .await
            .unwrap_err()
            .code()
    }

    async fn read_server_error(bytes: &[u8]) -> ProtocolErrorCode {
        let (mut peer, reader) = tokio::io::duplex(bytes.len().max(8));
        peer.write_all(bytes).await.unwrap();
        peer.shutdown().await.unwrap();
        FrameReader::new(reader)
            .read_server_event()
            .await
            .unwrap_err()
            .code()
    }

    fn framed(body: &[u8]) -> Vec<u8> {
        let mut framed = (body.len() as u32).to_be_bytes().to_vec();
        framed.extend_from_slice(body);
        framed
    }

    #[tokio::test]
    async fn reader_classifies_invalid_utf8_json_version_and_uuid() {
        assert_eq!(
            read_error(&framed(&[0xff])).await,
            ProtocolErrorCode::InvalidUtf8
        );
        assert_eq!(
            read_error(&framed(b"{")).await,
            ProtocolErrorCode::InvalidJson
        );
        assert_eq!(
            read_error(&framed(br#"{"version":2,"body":{"type":"resume","request_id":"0190f2ef-0000-7000-8000-000000000001"}}"#)).await,
            ProtocolErrorCode::UnsupportedVersion
        );
        assert_eq!(
            read_error(&framed(
                br#"{"version":1,"body":{"type":"resume","request_id":"not-a-uuid"}}"#
            ))
            .await,
            ProtocolErrorCode::InvalidUuid
        );
    }

    #[tokio::test]
    async fn reader_classifies_valid_json_schema_and_semantic_failures_as_invalid_requests() {
        let id = request_id();
        let blank = format!(
            r#"{{"version":1,"body":{{"type":"submit","request_id":"{id}","text":"  "}}}}"#
        );
        let unknown = format!(
            r#"{{"version":1,"body":{{"type":"resume","request_id":"{id}","extra":true}}}}"#
        );
        for json in [blank, unknown] {
            assert_eq!(
                read_error(&framed(json.as_bytes())).await,
                ProtocolErrorCode::InvalidRequest
            );
        }
    }

    #[tokio::test]
    async fn every_wire_id_field_rejects_non_uuid_text() {
        let request = request_id();
        let bundle = bundle_id();
        let invalid_requests = [
            format!(
                r#"{{"version":1,"body":{{"type":"cancel","request_id":"{request}","cancel_id":"bad"}}}}"#
            ),
            format!(
                r#"{{"version":1,"body":{{"type":"ack_final","request_id":"{request}","bundle_id":"bad","delivery_ids":["{delivery}"]}}}}"#,
                delivery = delivery_id()
            ),
            format!(
                r#"{{"version":1,"body":{{"type":"ack_final","request_id":"{request}","bundle_id":"{bundle}","delivery_ids":["bad"]}}}}"#
            ),
        ];
        for json in invalid_requests {
            assert_eq!(
                read_error(&framed(json.as_bytes())).await,
                ProtocolErrorCode::InvalidUuid
            );
        }

        let invalid_events = [
            format!(
                r#"{{"version":1,"body":{{"type":"accepted","request_id":"{request}","work_item_id":"bad","sequence":1}}}}"#
            ),
            format!(
                r#"{{"version":1,"body":{{"type":"cancel_accepted","request_id":"{request}","cancel_id":"{cancel}","affected_request_ids":["bad"]}}}}"#,
                cancel = cancel_id()
            ),
            format!(
                r#"{{"version":1,"body":{{"type":"final_chunk","request_id":"{request}","bundle_id":"{bundle}","delivery_id":"bad","chunk_index":0,"bytes_base64":""}}}}"#
            ),
        ];
        for json in invalid_events {
            assert_eq!(
                read_server_error(&framed(json.as_bytes())).await,
                ProtocolErrorCode::InvalidUuid
            );
        }
    }

    #[tokio::test]
    async fn reader_rejects_zero_oversized_and_incomplete_frames() {
        assert_eq!(
            read_error(&0_u32.to_be_bytes()).await,
            ProtocolErrorCode::ZeroLengthFrame
        );
        assert_eq!(
            read_error(&((MAX_FRAME_BYTES + 1) as u32).to_be_bytes()).await,
            ProtocolErrorCode::FrameTooLarge
        );
        assert_eq!(
            read_error(&[0, 0]).await,
            ProtocolErrorCode::IncompleteFrame
        );
        assert_eq!(
            read_error(&[0, 0, 0, 4, b'{']).await,
            ProtocolErrorCode::IncompleteFrame
        );
    }

    #[tokio::test(start_paused = true)]
    async fn reader_applies_a_five_second_header_deadline() {
        let (_peer, reader) = tokio::io::duplex(8);
        let task =
            tokio::spawn(async move { FrameReader::new(reader).read_client_request().await });
        tokio::time::advance(Duration::from_secs(5)).await;
        tokio::task::yield_now().await;
        assert_eq!(
            task.await.unwrap().unwrap_err().code(),
            ProtocolErrorCode::HeaderTimeout
        );
    }

    #[tokio::test(start_paused = true)]
    async fn reader_applies_a_separate_thirty_second_body_deadline() -> Result<()> {
        let (mut peer, reader) = tokio::io::duplex(8);
        peer.write_all(&8_u32.to_be_bytes()).await?;
        let task =
            tokio::spawn(async move { FrameReader::new(reader).read_client_request().await });
        tokio::time::advance(Duration::from_secs(29)).await;
        tokio::task::yield_now().await;
        assert!(!task.is_finished());
        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        assert_eq!(
            task.await.unwrap().unwrap_err().code(),
            ProtocolErrorCode::BodyTimeout
        );
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn writer_applies_a_thirty_second_deadline() {
        let (writer, _peer) = tokio::io::duplex(1);
        let mut writer = FrameWriter::new(writer);
        let event = ServerEvent::new(ServerEventBody::TextDelta {
            request_id: request_id(),
            delta: "blocked".repeat(32),
        });
        let task = tokio::spawn(async move { writer.write_server_event(&event).await });
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(30)).await;
        tokio::task::yield_now().await;
        assert_eq!(
            task.await.unwrap().unwrap_err().code(),
            ProtocolErrorCode::WriteTimeout
        );
    }

    fn bundle(payload: FinalPayload) -> ResultBundle {
        ResultBundle {
            id: bundle_id(),
            request_id: request_id(),
            state: BundleState::Delivering,
            manifest: BundleManifest {
                entries: vec![],
                sha256: String::new(),
            },
            deliveries: vec![(delivery_id(), payload)],
        }
    }

    #[test]
    fn canonical_payload_is_split_before_base64_at_192_kib() -> Result<()> {
        let text = "x".repeat(MAX_FINAL_CHUNK_BYTES * 2);
        let expected = serde_json::to_vec(&FinalPayload::Text { text: text.clone() })?;
        let frames = encode_bundle(&bundle(FinalPayload::Text { text }), false)?;
        let chunks: Vec<_> = frames
            .iter()
            .filter_map(|frame| match &frame.body {
                ServerEventBody::FinalChunk { bytes_base64, .. } => {
                    Some(STANDARD.decode(bytes_base64).unwrap())
                }
                _ => None,
            })
            .collect();
        assert!(
            chunks
                .iter()
                .all(|chunk| chunk.len() <= MAX_FINAL_CHUNK_BYTES)
        );
        assert_eq!(chunks.concat(), expected);
        assert_eq!(chunks.len(), expected.len().div_ceil(MAX_FINAL_CHUNK_BYTES));
        Ok(())
    }

    #[test]
    fn every_typed_final_payload_keeps_task_five_canonical_bytes() -> Result<()> {
        let payloads = [
            FinalPayload::Text {
                text: "answer".into(),
            },
            FinalPayload::File {
                artifact: ManagedArtifact {
                    id: ArtifactId::parse("0190f2ef-0000-7000-8000-000000000006")?,
                    managed_path: "/tmp/artifacts/result.txt".into(),
                    display_name: "result.txt".into(),
                    media_type: "text/plain".into(),
                    size: 6,
                    sha256: "c".repeat(64),
                    caption: Some("result".into()),
                },
            },
            FinalPayload::TerminalError {
                code: "failed".into(),
                message: "bounded".into(),
            },
        ];
        for payload in payloads {
            let expected = serde_json::to_vec(&payload)?;
            let frames = encode_bundle(&bundle(payload), false)?;
            let decoded: Vec<u8> = frames
                .iter()
                .filter_map(|frame| match &frame.body {
                    ServerEventBody::FinalChunk { bytes_base64, .. } => {
                        Some(STANDARD.decode(bytes_base64).unwrap())
                    }
                    _ => None,
                })
                .flatten()
                .collect();
            assert_eq!(decoded, expected);
        }
        Ok(())
    }

    #[test]
    fn encoded_bundle_uses_canonical_payload_and_manifest_hashes() -> Result<()> {
        let payload = FinalPayload::TerminalError {
            code: "failed".into(),
            message: "bounded".into(),
        };
        let payload_bytes = serde_json::to_vec(&payload)?;
        let frames = encode_bundle(&bundle(payload), true)?;
        let ServerEventBody::FinalBegin {
            replay, manifest, ..
        } = &frames[0].body
        else {
            panic!("missing final_begin")
        };
        assert!(*replay);
        assert_eq!(manifest[0].decoded_bytes, payload_bytes.len());
        assert_eq!(
            manifest[0].sha256,
            format!("{:x}", Sha256::digest(&payload_bytes))
        );
        let manifest_bytes = serde_json::to_vec(manifest)?;
        let ServerEventBody::FinalEnd {
            manifest_sha256, ..
        } = &frames.last().unwrap().body
        else {
            panic!("missing final_end")
        };
        assert_eq!(
            *manifest_sha256,
            format!("{:x}", Sha256::digest(manifest_bytes))
        );
        Ok(())
    }

    #[test]
    fn encoded_final_frames_are_strictly_below_one_mib() -> Result<()> {
        let text = "x".repeat(MAX_BUNDLE_BYTES - 32);
        let frames = encode_bundle(&bundle(FinalPayload::Text { text }), false)?;
        assert!(
            frames
                .iter()
                .all(|frame| serde_json::to_vec(frame).unwrap().len() < MAX_FRAME_BYTES)
        );
        Ok(())
    }

    #[test]
    fn prepared_bundle_clones_share_canonical_payload_storage() -> Result<()> {
        let prepared = prepare_bundle(
            &bundle(FinalPayload::Text {
                text: "x".repeat(MAX_BUNDLE_BYTES - 32),
            }),
            false,
        )?;
        let clone = prepared.clone();
        assert!(Arc::ptr_eq(&prepared.deliveries, &clone.deliveries));
        assert_eq!(prepared.events().count(), prepared.clone().events().count());
        Ok(())
    }

    #[test]
    fn encoder_returns_typed_limit_errors() {
        let oversized = bundle(FinalPayload::Text {
            text: "x".repeat(MAX_BUNDLE_BYTES + 1),
        });
        assert!(matches!(
            encode_bundle(&oversized, false),
            Err(BundleLimitError::TextBytes { .. })
        ));
    }
}
