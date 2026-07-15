#[cfg(test)]
mod tests {
    use anyhow::Result;
    use base64::{Engine, engine::general_purpose::STANDARD};
    use sha2::{Digest, Sha256};

    use super::{LocalRenderer, RenderAction};
    use crate::runtime::{
        BundleId, BundleState, DeliveryId, RequestId,
        ipc::protocol::{FinalManifestEntry, ServerEvent, ServerEventBody, encode_bundle},
        store::{BundleManifest, FinalPayload, ResultBundle},
    };

    #[test]
    fn non_tty_suppresses_deltas_and_prints_only_verified_final() -> Result<()> {
        let request = RequestId::new();
        let events = final_events(&request, "authoritative")?;
        let mut renderer = LocalRenderer::with_terminal(Vec::new(), false);
        renderer.handle(ServerEvent::new(ServerEventBody::TextDelta {
            request_id: request.clone(),
            delta: "transient".into(),
        }))?;
        let mut final_action = RenderAction::Continue;
        for event in events {
            final_action = renderer.handle(event)?;
        }
        assert!(matches!(final_action, RenderAction::FinalVerified { .. }));
        assert_eq!(String::from_utf8(renderer.into_inner())?, "authoritative\n");
        Ok(())
    }

    #[test]
    fn tty_stops_deltas_after_gap_and_reprints_verified_final_from_start() -> Result<()> {
        let request = RequestId::new();
        let mut renderer = LocalRenderer::with_terminal(Vec::new(), true);
        for body in [
            ServerEventBody::TextDelta {
                request_id: request.clone(),
                delta: "early".into(),
            },
            ServerEventBody::StreamGap {
                request_id: request.clone(),
            },
            ServerEventBody::TextDelta {
                request_id: request.clone(),
                delta: "hidden".into(),
            },
        ] {
            renderer.handle(ServerEvent::new(body))?;
        }
        for event in final_events(&request, "early and complete")? {
            renderer.handle(event)?;
        }
        let output = String::from_utf8(renderer.into_inner())?;
        assert!(output.contains("early"));
        assert!(!output.contains("hidden"));
        assert!(output.ends_with("early and complete\n"));
        Ok(())
    }

    #[test]
    fn corrupt_bundle_produces_no_authoritative_output_or_ack() -> Result<()> {
        let request = RequestId::new();
        let bundle = BundleId::new();
        let delivery = DeliveryId::new();
        let bytes = serde_json::to_vec(&FinalPayload::Text {
            text: "secret".into(),
        })?;
        let entry = FinalManifestEntry {
            delivery_id: delivery.clone(),
            payload_kind: "text".into(),
            decoded_bytes: bytes.len(),
            sha256: format!("{:x}", Sha256::digest(&bytes)),
            chunk_count: 1,
        };
        let manifest_hash = format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(&vec![entry.clone()])?)
        );
        let mut renderer = LocalRenderer::with_terminal(Vec::new(), false);
        renderer.handle(ServerEvent::new(ServerEventBody::FinalBegin {
            request_id: request.clone(),
            bundle_id: bundle.clone(),
            replay: false,
            manifest: vec![entry],
        }))?;
        renderer.handle(ServerEvent::new(ServerEventBody::FinalChunk {
            request_id: request.clone(),
            bundle_id: bundle.clone(),
            delivery_id: delivery,
            chunk_index: 0,
            bytes_base64: STANDARD.encode(b"tampered"),
        }))?;
        assert!(
            renderer
                .handle(ServerEvent::new(ServerEventBody::FinalEnd {
                    request_id: request,
                    bundle_id: bundle,
                    manifest_sha256: manifest_hash,
                }))
                .is_err()
        );
        assert!(renderer.output().is_empty());
        Ok(())
    }

    fn final_events(request: &RequestId, text: &str) -> Result<Vec<ServerEvent>> {
        let delivery = DeliveryId::new();
        Ok(encode_bundle(
            &ResultBundle {
                id: BundleId::new(),
                request_id: request.clone(),
                state: BundleState::Delivered,
                manifest: BundleManifest {
                    entries: vec![],
                    sha256: String::new(),
                },
                deliveries: vec![(delivery, FinalPayload::Text { text: text.into() })],
            },
            false,
        )?)
    }
}
use std::{
    collections::{HashMap, HashSet},
    io::{self, IsTerminal, Write},
};

use anyhow::{Context, Result, bail};
use base64::{Engine, engine::general_purpose::STANDARD};
use sha2::{Digest, Sha256};

use crate::runtime::{
    BundleId, DeliveryId, MAX_BUNDLE_BYTES, MAX_BUNDLE_DELIVERIES, MAX_FINAL_CHUNK_BYTES,
    MAX_MANIFEST_BYTES, RequestId,
    ipc::protocol::{FinalManifestEntry, ServerEvent, ServerEventBody},
    store::FinalPayload,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RenderAction {
    Continue,
    Accepted,
    CancelAccepted,
    FinalVerified {
        request_id: RequestId,
        bundle_id: BundleId,
        delivery_ids: Vec<DeliveryId>,
    },
    Recover,
    DaemonError(String),
}

pub struct LocalRenderer<W> {
    output: W,
    terminal: bool,
    expected_request: Option<RequestId>,
    gap: bool,
    transient_written: bool,
    spinner_frame: usize,
    bundle: Option<PendingBundle>,
}

struct PendingBundle {
    request_id: RequestId,
    bundle_id: BundleId,
    manifest: Vec<FinalManifestEntry>,
    chunks: HashMap<DeliveryId, Vec<Option<Vec<u8>>>>,
    buffered_bytes: usize,
}

impl LocalRenderer<io::Stdout> {
    pub fn stdout(request_id: RequestId) -> Self {
        let output = io::stdout();
        let terminal = output.is_terminal();
        Self::for_request(output, terminal, request_id)
    }
}

impl<W: Write> LocalRenderer<W> {
    pub fn with_terminal(output: W, terminal: bool) -> Self {
        Self {
            output,
            terminal,
            expected_request: None,
            gap: false,
            transient_written: false,
            spinner_frame: 0,
            bundle: None,
        }
    }

    pub fn for_request(output: W, terminal: bool, request_id: RequestId) -> Self {
        let mut renderer = Self::with_terminal(output, terminal);
        renderer.expected_request = Some(request_id);
        renderer
    }

    pub fn into_inner(self) -> W {
        self.output
    }

    pub fn handle(&mut self, event: ServerEvent) -> Result<RenderAction> {
        match event.body {
            ServerEventBody::Accepted { request_id, .. } => {
                self.require_request(&request_id)?;
                self.activity()?;
                Ok(RenderAction::Accepted)
            }
            ServerEventBody::CancelAccepted { request_id, .. } => {
                self.require_request(&request_id)?;
                Ok(RenderAction::CancelAccepted)
            }
            ServerEventBody::Activity { request_id, .. } => {
                self.require_request(&request_id)?;
                self.activity()?;
                Ok(RenderAction::Continue)
            }
            ServerEventBody::TextDelta { request_id, delta } => {
                self.require_request(&request_id)?;
                if self.terminal && !self.gap {
                    self.output.write_all(delta.as_bytes())?;
                    self.output.flush()?;
                    self.transient_written = true;
                }
                Ok(RenderAction::Continue)
            }
            ServerEventBody::StreamGap { request_id } => {
                self.require_request(&request_id)?;
                self.gap = true;
                Ok(RenderAction::Continue)
            }
            ServerEventBody::FinalBegin {
                request_id,
                bundle_id,
                manifest,
                ..
            } => {
                self.begin(request_id, bundle_id, manifest)?;
                Ok(RenderAction::Continue)
            }
            ServerEventBody::FinalChunk {
                request_id,
                bundle_id,
                delivery_id,
                chunk_index,
                bytes_base64,
            } => {
                self.chunk(
                    request_id,
                    bundle_id,
                    delivery_id,
                    chunk_index,
                    bytes_base64,
                )?;
                Ok(RenderAction::Continue)
            }
            ServerEventBody::FinalEnd {
                request_id,
                bundle_id,
                manifest_sha256,
            } => self.end(request_id, bundle_id, &manifest_sha256),
            ServerEventBody::RequestError {
                request_id,
                code,
                message,
            } => {
                self.require_request(&request_id)?;
                Ok(RenderAction::DaemonError(format!(
                    "daemon rejected request {request_id} ({code}): {message}"
                )))
            }
            ServerEventBody::ProtocolError { code, message } => Ok(RenderAction::DaemonError(
                format!("daemon protocol error ({code:?}): {message}"),
            )),
            ServerEventBody::ServerShuttingDown { request_id, .. } => {
                if let Some(request_id) = request_id {
                    self.require_request(&request_id)?;
                }
                Ok(RenderAction::Recover)
            }
        }
    }

    fn require_request(&self, request: &RequestId) -> Result<()> {
        if let Some(expected) = &self.expected_request
            && expected != request
        {
            bail!("server event request ID {request} does not match {expected}")
        }
        Ok(())
    }

    fn activity(&mut self) -> Result<()> {
        if self.terminal && !self.transient_written {
            const FRAMES: [&str; 4] = ["|", "/", "-", "\\"];
            write!(self.output, "\r{} ", FRAMES[self.spinner_frame])?;
            self.output.flush()?;
            self.spinner_frame = (self.spinner_frame + 1) % FRAMES.len();
        }
        Ok(())
    }

    fn begin(
        &mut self,
        request_id: RequestId,
        bundle_id: BundleId,
        manifest: Vec<FinalManifestEntry>,
    ) -> Result<()> {
        self.require_request(&request_id)?;
        if self.bundle.is_some() {
            bail!("received a second final bundle before the first completed")
        }
        if manifest.is_empty() || manifest.len() > MAX_BUNDLE_DELIVERIES {
            bail!("final manifest delivery count is outside protocol limits")
        }
        let canonical = serde_json::to_vec(&manifest)?;
        if canonical.len() > MAX_MANIFEST_BYTES {
            bail!("final manifest exceeds protocol limit")
        }
        let mut seen = HashSet::new();
        let mut decoded_total = 0usize;
        let mut chunks = HashMap::new();
        for entry in &manifest {
            if !seen.insert(entry.delivery_id.clone()) {
                bail!("final manifest contains a duplicate delivery ID")
            }
            if !matches!(
                entry.payload_kind.as_str(),
                "text" | "file" | "terminal_error"
            ) {
                bail!("final manifest contains an unknown payload kind")
            }
            let expected_chunks = entry.decoded_bytes.div_ceil(MAX_FINAL_CHUNK_BYTES);
            if entry.decoded_bytes == 0 || entry.chunk_count != expected_chunks {
                bail!("final manifest chunk count does not match decoded size")
            }
            decoded_total = decoded_total
                .checked_add(entry.decoded_bytes)
                .context("final bundle size overflow")?;
            if decoded_total > MAX_BUNDLE_BYTES {
                bail!("final bundle exceeds decoded byte limit")
            }
            chunks.insert(entry.delivery_id.clone(), vec![None; entry.chunk_count]);
        }
        self.bundle = Some(PendingBundle {
            request_id,
            bundle_id,
            manifest,
            chunks,
            buffered_bytes: 0,
        });
        Ok(())
    }

    fn chunk(
        &mut self,
        request_id: RequestId,
        bundle_id: BundleId,
        delivery_id: DeliveryId,
        chunk_index: usize,
        bytes_base64: String,
    ) -> Result<()> {
        self.require_request(&request_id)?;
        let bundle = self
            .bundle
            .as_mut()
            .context("final chunk arrived before begin")?;
        if bundle.request_id != request_id || bundle.bundle_id != bundle_id {
            bail!("final chunk IDs do not match final begin")
        }
        let decoded = STANDARD
            .decode(bytes_base64)
            .context("final chunk is not valid base64")?;
        if decoded.len() > MAX_FINAL_CHUNK_BYTES {
            bail!("final chunk exceeds decoded byte limit")
        }
        bundle.buffered_bytes = bundle
            .buffered_bytes
            .checked_add(decoded.len())
            .context("final bundle size overflow")?;
        if bundle.buffered_bytes > MAX_BUNDLE_BYTES {
            bail!("final bundle exceeds decoded byte limit")
        }
        let slots = bundle
            .chunks
            .get_mut(&delivery_id)
            .context("final chunk has an unknown delivery ID")?;
        let slot = slots
            .get_mut(chunk_index)
            .context("final chunk index is outside the manifest")?;
        if slot.is_some() {
            bail!("final chunk index was delivered more than once")
        }
        *slot = Some(decoded);
        Ok(())
    }

    fn end(
        &mut self,
        request_id: RequestId,
        bundle_id: BundleId,
        manifest_sha256: &str,
    ) -> Result<RenderAction> {
        self.require_request(&request_id)?;
        let bundle = self
            .bundle
            .take()
            .context("final end arrived before begin")?;
        if bundle.request_id != request_id || bundle.bundle_id != bundle_id {
            bail!("final end IDs do not match final begin")
        }
        let canonical_manifest = serde_json::to_vec(&bundle.manifest)?;
        let actual_manifest_hash = format!("{:x}", Sha256::digest(&canonical_manifest));
        if actual_manifest_hash != manifest_sha256 {
            bail!("final manifest hash does not match")
        }

        let mut verified = Vec::with_capacity(bundle.manifest.len());
        for entry in &bundle.manifest {
            let slots = bundle
                .chunks
                .get(&entry.delivery_id)
                .expect("manifest initialized chunks");
            let mut bytes = Vec::with_capacity(entry.decoded_bytes);
            for slot in slots {
                bytes
                    .extend_from_slice(slot.as_deref().context("final bundle is missing a chunk")?);
            }
            if bytes.len() != entry.decoded_bytes {
                bail!("final delivery decoded size does not match manifest")
            }
            if format!("{:x}", Sha256::digest(&bytes)) != entry.sha256 {
                bail!("final delivery hash does not match manifest")
            }
            let payload: FinalPayload =
                serde_json::from_slice(&bytes).context("final delivery payload is invalid")?;
            if payload_kind(&payload) != entry.payload_kind {
                bail!("final delivery kind does not match manifest")
            }
            verified.push((entry.delivery_id.clone(), payload));
        }

        if self.transient_written || self.terminal {
            self.output.write_all(b"\n")?;
        }
        for (_, payload) in &verified {
            match payload {
                FinalPayload::Text { text } => writeln!(self.output, "{text}")?,
                FinalPayload::File { artifact } => {
                    writeln!(self.output, "{}", artifact.managed_path.display())?
                }
                FinalPayload::TerminalError { code, message } => {
                    writeln!(self.output, "Error [{code}]: {message}")?
                }
            }
        }
        self.output.flush()?;
        Ok(RenderAction::FinalVerified {
            request_id,
            bundle_id,
            delivery_ids: verified.into_iter().map(|(id, _)| id).collect(),
        })
    }
}

fn payload_kind(payload: &FinalPayload) -> &'static str {
    match payload {
        FinalPayload::Text { .. } => "text",
        FinalPayload::File { .. } => "file",
        FinalPayload::TerminalError { .. } => "terminal_error",
    }
}

impl LocalRenderer<Vec<u8>> {
    pub fn output(&self) -> &[u8] {
        &self.output
    }
}
