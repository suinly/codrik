#[cfg(test)]
mod tests {
    use anyhow::Result;
    use base64::{Engine, engine::general_purpose::STANDARD};
    use sha2::{Digest, Sha256};

    use super::{FinalBundleVerifier, LocalRenderer, RenderAction};
    use crate::runtime::{
        ArtifactId, BundleId, BundleState, DeliveryId, RequestId,
        ipc::protocol::{FinalManifestEntry, ServerEvent, ServerEventBody, encode_bundle},
        store::{BundleManifest, FinalPayload, ManagedArtifact, ResultBundle},
    };

    #[test]
    fn shared_final_verifier_returns_typed_payloads_and_rejects_malformed_files() -> Result<()> {
        let request = RequestId::new();
        let mut verifier = FinalBundleVerifier::for_request(request.clone());
        let mut verified = None;
        for event in final_events(&request, "hello 🙂")? {
            verified = verifier.handle(event)?.or(verified);
        }
        let verified = verified.expect("complete verified bundle");
        assert_eq!(verified.request_id, request);
        assert_eq!(verified.delivery_ids().len(), 1);
        assert!(matches!(
            &verified.deliveries[0].payload,
            FinalPayload::Text { text } if text == "hello 🙂"
        ));

        let malformed = raw_final_events(
            &request,
            vec![("file", br#"{"type":"file","artifact":{}}"#.to_vec())],
        )?;
        let mut verifier = FinalBundleVerifier::for_request(request);
        assert!(
            malformed
                .into_iter()
                .any(|event| verifier.handle(event).is_err())
        );
        Ok(())
    }

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
            bytes_base64: STANDARD.encode(vec![b'x'; bytes.len()]),
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

    #[test]
    fn chunks_must_be_contiguous_in_manifest_delivery_order() -> Result<()> {
        let request = RequestId::new();
        let text = "x".repeat(crate::runtime::MAX_FINAL_CHUNK_BYTES + 1);
        let mut events = final_events(&request, &text)?;
        let first_chunk = events.remove(1);
        let second_chunk = events.remove(1);
        let mut renderer = LocalRenderer::for_request(Vec::new(), false, request);
        renderer.handle(events.remove(0))?;
        assert!(renderer.handle(second_chunk).is_err());
        assert!(renderer.output().is_empty());
        drop(first_chunk);
        Ok(())
    }

    #[test]
    fn one_bundle_buffer_capacity_never_exceeds_decoded_limit() -> Result<()> {
        let request = RequestId::new();
        let delivery = DeliveryId::new();
        let mut renderer = LocalRenderer::for_request(Vec::new(), false, request.clone());
        renderer.handle(ServerEvent::new(ServerEventBody::FinalBegin {
            request_id: request,
            bundle_id: BundleId::new(),
            replay: false,
            manifest: vec![FinalManifestEntry {
                delivery_id: delivery,
                payload_kind: "text".into(),
                decoded_bytes: crate::runtime::MAX_BUNDLE_BYTES,
                sha256: "0".repeat(64),
                chunk_count: crate::runtime::MAX_BUNDLE_BYTES
                    .div_ceil(crate::runtime::MAX_FINAL_CHUNK_BYTES),
            }],
        }))?;
        assert!(renderer.buffer_capacity_bytes() <= crate::runtime::MAX_BUNDLE_BYTES);
        Ok(())
    }

    #[test]
    fn rejects_wrong_id_duplicate_missing_and_interleaved_chunks_before_output() -> Result<()> {
        let request = RequestId::new();
        let bundle = ResultBundle {
            id: BundleId::new(),
            request_id: request.clone(),
            state: BundleState::Delivered,
            manifest: BundleManifest {
                entries: vec![],
                sha256: String::new(),
            },
            deliveries: vec![
                (DeliveryId::new(), FinalPayload::Text { text: "one".into() }),
                (DeliveryId::new(), FinalPayload::Text { text: "two".into() }),
            ],
        };
        let events = encode_bundle(&bundle, false)?;

        let mut wrong_id = events.clone();
        if let ServerEventBody::FinalChunk { request_id, .. } = &mut wrong_id[1].body {
            *request_id = RequestId::new();
        }
        assert_rejected(&request, wrong_id)?;

        let mut wrong_bundle = events.clone();
        if let ServerEventBody::FinalChunk { bundle_id, .. } = &mut wrong_bundle[1].body {
            *bundle_id = BundleId::new();
        }
        assert_rejected(&request, wrong_bundle)?;

        let mut wrong_delivery = events.clone();
        if let ServerEventBody::FinalChunk { delivery_id, .. } = &mut wrong_delivery[1].body {
            *delivery_id = DeliveryId::new();
        }
        assert_rejected(&request, wrong_delivery)?;

        let mut interleaved = events.clone();
        interleaved.swap(1, 2);
        assert_rejected(&request, interleaved)?;

        let mut duplicate = events.clone();
        duplicate.insert(2, duplicate[1].clone());
        assert_rejected(&request, duplicate)?;

        let mut missing = events;
        missing.remove(1);
        assert_rejected(&request, missing)?;
        Ok(())
    }

    #[test]
    fn rejects_bad_base64_size_manifest_hash_and_payload_kind_before_output() -> Result<()> {
        let request = RequestId::new();
        let events = final_events(&request, "payload")?;

        let mut bad_base64 = events.clone();
        if let ServerEventBody::FinalChunk { bytes_base64, .. } = &mut bad_base64[1].body {
            *bytes_base64 = "!".repeat(bytes_base64.len());
        }
        assert_rejected(&request, bad_base64)?;

        let mut bad_manifest_hash = events.clone();
        if let ServerEventBody::FinalEnd {
            manifest_sha256, ..
        } = &mut bad_manifest_hash[2].body
        {
            *manifest_sha256 = "0".repeat(64);
        }
        assert_rejected(&request, bad_manifest_hash)?;

        let mut bad_kind = events;
        let manifest = match &mut bad_kind[0].body {
            ServerEventBody::FinalBegin { manifest, .. } => {
                manifest[0].payload_kind = "file".into();
                manifest.clone()
            }
            _ => unreachable!(),
        };
        if let ServerEventBody::FinalEnd {
            manifest_sha256, ..
        } = &mut bad_kind[2].body
        {
            *manifest_sha256 = format!("{:x}", Sha256::digest(serde_json::to_vec(&manifest)?));
        }
        assert_rejected(&request, bad_kind)?;

        let oversized = ServerEvent::new(ServerEventBody::FinalBegin {
            request_id: request.clone(),
            bundle_id: BundleId::new(),
            replay: false,
            manifest: vec![FinalManifestEntry {
                delivery_id: DeliveryId::new(),
                payload_kind: "text".into(),
                decoded_bytes: crate::runtime::MAX_BUNDLE_BYTES + 1,
                sha256: "0".repeat(64),
                chunk_count: (crate::runtime::MAX_BUNDLE_BYTES + 1)
                    .div_ceil(crate::runtime::MAX_FINAL_CHUNK_BYTES),
            }],
        });
        let mut verifier = FinalBundleVerifier::for_request(request.clone());
        assert!(verifier.handle(oversized.clone()).is_err());
        let mut renderer = LocalRenderer::for_request(Vec::new(), false, request);
        assert!(renderer.handle(oversized).is_err());
        assert!(renderer.output().is_empty());
        Ok(())
    }

    #[test]
    fn shared_verifier_rejects_duplicate_manifest_ids_and_aggregate_overflow() -> Result<()> {
        let request = RequestId::new();
        let duplicate = DeliveryId::new();
        let half = crate::runtime::MAX_BUNDLE_BYTES / 2 + 1;
        let manifest = vec![
            FinalManifestEntry {
                delivery_id: duplicate.clone(),
                payload_kind: "text".into(),
                decoded_bytes: half,
                sha256: "0".repeat(64),
                chunk_count: half.div_ceil(crate::runtime::MAX_FINAL_CHUNK_BYTES),
            },
            FinalManifestEntry {
                delivery_id: duplicate,
                payload_kind: "text".into(),
                decoded_bytes: half,
                sha256: "0".repeat(64),
                chunk_count: half.div_ceil(crate::runtime::MAX_FINAL_CHUNK_BYTES),
            },
        ];
        let mut verifier = FinalBundleVerifier::for_request(request.clone());
        assert!(
            verifier
                .handle(ServerEvent::new(ServerEventBody::FinalBegin {
                    request_id: request.clone(),
                    bundle_id: BundleId::new(),
                    replay: false,
                    manifest,
                }))
                .is_err()
        );

        let distinct = [DeliveryId::new(), DeliveryId::new()];
        let manifest = distinct
            .into_iter()
            .map(|delivery_id| FinalManifestEntry {
                delivery_id,
                payload_kind: "text".into(),
                decoded_bytes: half,
                sha256: "0".repeat(64),
                chunk_count: half.div_ceil(crate::runtime::MAX_FINAL_CHUNK_BYTES),
            })
            .collect();
        let mut verifier = FinalBundleVerifier::for_request(request.clone());
        assert!(
            verifier
                .handle(ServerEvent::new(ServerEventBody::FinalBegin {
                    request_id: request,
                    bundle_id: BundleId::new(),
                    replay: false,
                    manifest,
                }))
                .is_err()
        );
        Ok(())
    }

    #[test]
    fn renders_verified_file_and_terminal_error_payloads() -> Result<()> {
        let request = RequestId::new();
        let artifact = ManagedArtifact {
            id: ArtifactId::new(),
            managed_path: "/tmp/result.txt".into(),
            display_name: "result.txt".into(),
            media_type: "text/plain".into(),
            size: 3,
            sha256: "a".repeat(64),
            caption: Some("caption".into()),
        };
        let bundle = ResultBundle {
            id: BundleId::new(),
            request_id: request.clone(),
            state: BundleState::Delivered,
            manifest: BundleManifest {
                entries: vec![],
                sha256: String::new(),
            },
            deliveries: vec![
                (DeliveryId::new(), FinalPayload::File { artifact }),
                (
                    DeliveryId::new(),
                    FinalPayload::TerminalError {
                        code: "failed".into(),
                        message: "try again".into(),
                    },
                ),
            ],
        };
        let mut renderer = LocalRenderer::for_request(Vec::new(), false, request);
        for event in encode_bundle(&bundle, false)? {
            renderer.handle(event)?;
        }
        assert_eq!(
            String::from_utf8(renderer.into_inner())?,
            "/tmp/result.txt\nError [failed]: try again\n"
        );
        Ok(())
    }

    #[test]
    fn final_begin_is_accepted_only_once_per_renderer() -> Result<()> {
        let request = RequestId::new();
        let events = final_events(&request, "done")?;
        let mut renderer = LocalRenderer::for_request(Vec::new(), false, request);
        for event in events.clone() {
            renderer.handle(event)?;
        }
        assert!(renderer.handle(events[0].clone()).is_err());
        Ok(())
    }

    #[test]
    fn renders_escaped_json_strings_from_borrowed_bundle_storage() -> Result<()> {
        let request = RequestId::new();
        let mut renderer = LocalRenderer::for_request(Vec::new(), false, request.clone());
        for event in final_events(&request, "line one\n\"quoted\" 🙂")? {
            renderer.handle(event)?;
        }
        assert_eq!(
            String::from_utf8(renderer.into_inner())?,
            "line one\n\"quoted\" 🙂\n"
        );
        Ok(())
    }

    #[test]
    fn rejects_duplicate_payload_fields_and_invalid_artifact_id_before_output() -> Result<()> {
        let request = RequestId::new();
        for (kind, payload) in [
            ("text", br#"{"type":"text","text":"first","text":"second"}"#.to_vec()),
            ("text", br#"{"type":"text","type":"text","text":"value"}"#.to_vec()),
            ("text", br#"{"type":"text","text":"value","unknown":true}"#.to_vec()),
            ("text", br#"{"type":"text"}"#.to_vec()),
            ("file", br#"{"type":"file","artifact":{"id":"not-a-uuid","managed_path":"/tmp/result","display_name":"result","media_type":"text/plain","size":1,"sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","caption":null}}"#.to_vec()),
            ("file", br#"{"type":"file","artifact":{"id":"0190f2ef-0000-7000-8000-000000000001","id":"0190f2ef-0000-7000-8000-000000000002","managed_path":"/tmp/result","display_name":"result","media_type":"text/plain","size":1,"sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","caption":null}}"#.to_vec()),
            ("file", br#"{"type":"file","artifact":{"id":"0190f2ef-0000-7000-8000-000000000001","managed_path":"/tmp/result","display_name":"result","media_type":"text/plain","size":1,"sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","caption":null,"unknown":true}}"#.to_vec()),
            ("file", br#"{"type":"file","artifact":{"id":"0190f2ef-0000-7000-8000-000000000001","managed_path":"/tmp/result","display_name":"result","media_type":"text/plain","size":1,"sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}"#.to_vec()),
        ] {
            assert_rejected(&request, raw_final_events(&request, vec![(kind, payload)])?)?;
        }
        Ok(())
    }

    #[test]
    fn validates_all_json_string_escapes_controls_and_surrogates_before_output() -> Result<()> {
        let request = RequestId::new();
        let mut renderer = LocalRenderer::for_request(Vec::new(), false, request.clone());
        for event in raw_final_events(
            &request,
            vec![(
                "text",
                br#"{"type":"text","text":"\uD83D\uDE42\n\t\\\""}"#.to_vec(),
            )],
        )? {
            renderer.handle(event)?;
        }
        assert_eq!(String::from_utf8(renderer.into_inner())?, "🙂\n\t\\\"\n");

        let mut renderer = LocalRenderer::for_request(Vec::new(), false, request.clone());
        for event in raw_final_events(
            &request,
            vec![(
                "text",
                br#"{"type":"text","text":"\u263A\b\f\r\/"}"#.to_vec(),
            )],
        )? {
            renderer.handle(event)?;
        }
        assert_eq!(
            renderer.into_inner(),
            ["☺".as_bytes(), b"\x08\x0c\r/\n"].concat()
        );

        for invalid in [
            br#"{"type":"text","text":"\uD800"}"#.as_slice(),
            br#"{"type":"text","text":"\uDC00"}"#.as_slice(),
            br#"{"type":"text","text":"\uD800\u0041"}"#.as_slice(),
            br#"{"type":"text","text":"\u12"}"#.as_slice(),
            br#"{"type":"text","text":"\uZZZZ"}"#.as_slice(),
            br#"{"type":"text","text":"\x"}"#.as_slice(),
            b"{\"type\":\"text\",\"text\":\"bad\x01control\"}".as_slice(),
        ] {
            assert_rejected(
                &request,
                raw_final_events(&request, vec![("text", invalid.to_vec())])?,
            )?;
        }
        for invalid in [
            br#"{"type":"terminal_error","code":"\uD800","message":"message"}"#.to_vec(),
            br#"{"type":"terminal_error","code":"code","message":"\uD800"}"#.to_vec(),
        ] {
            assert_rejected(
                &request,
                raw_final_events(&request, vec![("terminal_error", invalid)])?,
            )?;
        }
        Ok(())
    }

    #[test]
    fn malformed_later_delivery_produces_zero_authoritative_output_or_ack() -> Result<()> {
        let request = RequestId::new();
        assert_rejected(
            &request,
            raw_final_events(
                &request,
                vec![
                    ("text", br#"{"type":"text","text":"first"}"#.to_vec()),
                    (
                        "terminal_error",
                        br#"{"type":"terminal_error","code":"bad","message":"\uD800"}"#.to_vec(),
                    ),
                ],
            )?,
        )?;
        Ok(())
    }

    #[test]
    fn validates_every_artifact_field_and_semantics_before_output() -> Result<()> {
        let request = RequestId::new();
        for artifact in [
            r#"{"id":"0190f2ef-0000-7000-8000-000000000001","managed_path":"relative","display_name":"result","media_type":"text/plain","size":1,"sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","caption":null}"#,
            r#"{"id":"0190f2ef-0000-7000-8000-000000000001","managed_path":"/tmp/result","display_name":"result","media_type":"text/plain","size":1,"sha256":"short","caption":null}"#,
            r#"{"id":"0190f2ef-0000-7000-8000-000000000001","managed_path":"/tmp/result","display_name":"result","media_type":"text/plain","size":-1,"sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","caption":null}"#,
            r#"{"id":"\uD800","managed_path":"/tmp/result","display_name":"result","media_type":"text/plain","size":1,"sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","caption":null}"#,
            r#"{"id":"0190f2ef-0000-7000-8000-000000000001","managed_path":"/tmp/\uD800","display_name":"result","media_type":"text/plain","size":1,"sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","caption":null}"#,
            r#"{"id":"0190f2ef-0000-7000-8000-000000000001","managed_path":"/tmp/result","display_name":"\uD800","media_type":"text/plain","size":1,"sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","caption":null}"#,
            r#"{"id":"0190f2ef-0000-7000-8000-000000000001","managed_path":"/tmp/result","display_name":"result","media_type":"\uD800","size":1,"sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","caption":null}"#,
            r#"{"id":"0190f2ef-0000-7000-8000-000000000001","managed_path":"/tmp/result","display_name":"result","media_type":"text/plain","size":1,"sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\uD800","caption":null}"#,
            r#"{"id":"0190f2ef-0000-7000-8000-000000000001","managed_path":"/tmp/result","display_name":"result","media_type":"text/plain","size":1,"sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","caption":"\uD800"}"#,
        ] {
            let payload = format!(r#"{{"type":"file","artifact":{artifact}}}"#).into_bytes();
            assert_rejected(
                &request,
                raw_final_events(&request, vec![("file", payload)])?,
            )?;
        }
        Ok(())
    }

    #[test]
    fn shutdown_during_bundle_returns_recovery_without_output() -> Result<()> {
        let request = RequestId::new();
        let events = final_events(&request, "done")?;
        let mut renderer = LocalRenderer::for_request(Vec::new(), false, request.clone());
        renderer.handle(events[0].clone())?;
        assert_eq!(
            renderer.handle(ServerEvent::new(ServerEventBody::ServerShuttingDown {
                request_id: Some(request),
                resume_command: None,
            }))?,
            RenderAction::Recover
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

    fn raw_final_events(
        request: &RequestId,
        payloads: Vec<(&str, Vec<u8>)>,
    ) -> Result<Vec<ServerEvent>> {
        let bundle = BundleId::new();
        let mut manifest = Vec::new();
        let mut chunks = Vec::new();
        for (kind, bytes) in payloads {
            let delivery = DeliveryId::new();
            manifest.push(FinalManifestEntry {
                delivery_id: delivery.clone(),
                payload_kind: kind.into(),
                decoded_bytes: bytes.len(),
                sha256: format!("{:x}", Sha256::digest(&bytes)),
                chunk_count: bytes.len().div_ceil(crate::runtime::MAX_FINAL_CHUNK_BYTES),
            });
            for (chunk_index, chunk) in bytes
                .chunks(crate::runtime::MAX_FINAL_CHUNK_BYTES)
                .enumerate()
            {
                chunks.push(ServerEvent::new(ServerEventBody::FinalChunk {
                    request_id: request.clone(),
                    bundle_id: bundle.clone(),
                    delivery_id: delivery.clone(),
                    chunk_index,
                    bytes_base64: STANDARD.encode(chunk),
                }));
            }
        }
        let manifest_sha256 = format!("{:x}", Sha256::digest(serde_json::to_vec(&manifest)?));
        let mut events = vec![ServerEvent::new(ServerEventBody::FinalBegin {
            request_id: request.clone(),
            bundle_id: bundle.clone(),
            replay: false,
            manifest,
        })];
        events.extend(chunks);
        events.push(ServerEvent::new(ServerEventBody::FinalEnd {
            request_id: request.clone(),
            bundle_id: bundle,
            manifest_sha256,
        }));
        Ok(events)
    }

    fn assert_rejected(request: &RequestId, events: Vec<ServerEvent>) -> Result<()> {
        let mut verifier = FinalBundleVerifier::for_request(request.clone());
        assert!(
            events
                .iter()
                .cloned()
                .any(|event| verifier.handle(event).is_err())
        );
        let mut renderer = LocalRenderer::for_request(Vec::new(), false, request.clone());
        let mut rejected = false;
        for event in events {
            if renderer.handle(event).is_err() {
                rejected = true;
                break;
            }
        }
        assert!(rejected);
        assert!(renderer.output().is_empty());
        Ok(())
    }
}
use std::io::{self, IsTerminal, Write};

use anyhow::{Context, Result, bail};
use base64::{Engine, engine::general_purpose::STANDARD};
use serde_json::value::RawValue;
use sha2::{Digest, Sha256};

use crate::runtime::{
    ArtifactId, BundleId, DeliveryId, MAX_BUNDLE_BYTES, MAX_BUNDLE_DELIVERIES,
    MAX_FINAL_CHUNK_BYTES, MAX_MANIFEST_BYTES, RequestId,
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
    gap: bool,
    transient_written: bool,
    spinner_frame: usize,
    verifier: FinalBundleVerifier,
}

pub struct FinalBundleVerifier {
    expected_request: Option<RequestId>,
    bundle: Option<PendingBundle>,
    final_seen: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VerifiedFinalBundle {
    pub request_id: RequestId,
    pub bundle_id: BundleId,
    pub replay: bool,
    pub manifest_sha256: String,
    pub deliveries: Vec<VerifiedDelivery>,
}

impl VerifiedFinalBundle {
    pub fn delivery_ids(&self) -> Vec<DeliveryId> {
        self.deliveries
            .iter()
            .map(|delivery| delivery.delivery_id.clone())
            .collect()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VerifiedDelivery {
    pub delivery_id: DeliveryId,
    pub payload: FinalPayload,
}

struct PendingBundle {
    request_id: RequestId,
    bundle_id: BundleId,
    replay: bool,
    manifest: Vec<FinalManifestEntry>,
    deliveries: Vec<BufferedDelivery>,
    current_delivery: usize,
    next_chunk: usize,
    buffered_bytes: usize,
}

struct BufferedDelivery {
    bytes: Vec<u8>,
    digest: Sha256,
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
            gap: false,
            transient_written: false,
            spinner_frame: 0,
            verifier: FinalBundleVerifier::new(),
        }
    }

    pub fn for_request(output: W, terminal: bool, request_id: RequestId) -> Self {
        let mut renderer = Self::with_terminal(output, terminal);
        renderer.verifier = FinalBundleVerifier::for_request(request_id);
        renderer
    }

    pub fn into_inner(self) -> W {
        self.output
    }

    pub fn handle(&mut self, event: ServerEvent) -> Result<RenderAction> {
        if self.verifier.is_complete() {
            bail!("received an event after the final bundle completed")
        }
        if self.verifier.is_in_progress()
            && !matches!(
                event.body,
                ServerEventBody::FinalChunk { .. }
                    | ServerEventBody::FinalEnd { .. }
                    | ServerEventBody::RequestError { .. }
                    | ServerEventBody::ProtocolError { .. }
                    | ServerEventBody::ServerShuttingDown { .. }
            )
        {
            bail!("received a non-final event while a final bundle was in progress")
        }
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
            ServerEventBody::AckAccepted { request_id, .. } => {
                self.require_request(&request_id)?;
                Ok(RenderAction::DaemonError(
                    "unexpected final acknowledgement on operation stream".into(),
                ))
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
            body @ (ServerEventBody::FinalBegin { .. }
            | ServerEventBody::FinalChunk { .. }
            | ServerEventBody::FinalEnd { .. }) => {
                let Some(bundle) = self.verifier.handle(ServerEvent::new(body))? else {
                    return Ok(RenderAction::Continue);
                };
                self.render_verified(&bundle)?;
                let delivery_ids = bundle.delivery_ids();
                Ok(RenderAction::FinalVerified {
                    request_id: bundle.request_id,
                    bundle_id: bundle.bundle_id,
                    delivery_ids,
                })
            }
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
        self.verifier.require_request(request)
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

    fn render_verified(&mut self, bundle: &VerifiedFinalBundle) -> Result<()> {
        if self.transient_written || self.terminal {
            self.output.write_all(b"\n")?;
        }
        for delivery in &bundle.deliveries {
            match &delivery.payload {
                FinalPayload::Text { text } => {
                    self.output.write_all(text.as_bytes())?;
                    self.output.write_all(b"\n")?;
                }
                FinalPayload::File { artifact } => {
                    self.output
                        .write_all(artifact.managed_path.to_string_lossy().as_bytes())?;
                    self.output.write_all(b"\n")?;
                }
                FinalPayload::TerminalError { code, message } => {
                    self.output.write_all(b"Error [")?;
                    self.output.write_all(code.as_bytes())?;
                    self.output.write_all(b"]: ")?;
                    self.output.write_all(message.as_bytes())?;
                    self.output.write_all(b"\n")?;
                }
            }
        }
        self.output.flush()?;
        Ok(())
    }

    #[cfg(test)]
    fn buffer_capacity_bytes(&self) -> usize {
        self.verifier.buffer_capacity_bytes()
    }
}

impl FinalBundleVerifier {
    pub fn new() -> Self {
        Self {
            expected_request: None,
            bundle: None,
            final_seen: false,
        }
    }

    pub fn for_request(request_id: RequestId) -> Self {
        Self {
            expected_request: Some(request_id),
            bundle: None,
            final_seen: false,
        }
    }

    pub fn handle(&mut self, event: ServerEvent) -> Result<Option<VerifiedFinalBundle>> {
        if self.final_seen {
            bail!("received an event after the final bundle completed")
        }
        match event.body {
            ServerEventBody::FinalBegin {
                request_id,
                bundle_id,
                replay,
                manifest,
            } => {
                self.begin(request_id, bundle_id, replay, manifest)?;
                Ok(None)
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
                Ok(None)
            }
            ServerEventBody::FinalEnd {
                request_id,
                bundle_id,
                manifest_sha256,
            } => self.end(request_id, bundle_id, &manifest_sha256).map(Some),
            _ => bail!("shared final verifier received a non-final event"),
        }
    }

    pub fn is_in_progress(&self) -> bool {
        self.bundle.is_some()
    }

    pub fn is_complete(&self) -> bool {
        self.final_seen
    }

    fn require_request(&self, request: &RequestId) -> Result<()> {
        if let Some(expected) = &self.expected_request
            && expected != request
        {
            bail!("server event request ID {request} does not match {expected}")
        }
        Ok(())
    }

    fn begin(
        &mut self,
        request_id: RequestId,
        bundle_id: BundleId,
        replay: bool,
        manifest: Vec<FinalManifestEntry>,
    ) -> Result<()> {
        self.require_request(&request_id)?;
        if self.bundle.is_some() || self.final_seen {
            bail!("received a second final bundle before the first completed")
        }
        if manifest.is_empty() || manifest.len() > MAX_BUNDLE_DELIVERIES {
            bail!("final manifest delivery count is outside protocol limits")
        }
        let canonical = serde_json::to_vec(&manifest)?;
        if canonical.len() > MAX_MANIFEST_BYTES {
            bail!("final manifest exceeds protocol limit")
        }
        let mut seen = std::collections::HashSet::new();
        let mut decoded_total = 0usize;
        let mut deliveries = Vec::with_capacity(manifest.len());
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
            deliveries.push(BufferedDelivery {
                bytes: Vec::with_capacity(entry.decoded_bytes),
                digest: Sha256::new(),
            });
        }
        self.bundle = Some(PendingBundle {
            request_id,
            bundle_id,
            replay,
            manifest,
            deliveries,
            current_delivery: 0,
            next_chunk: 0,
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
        let entry = bundle
            .manifest
            .get(bundle.current_delivery)
            .context("final bundle received an extra chunk")?;
        if entry.delivery_id != delivery_id {
            bail!("final chunks are not in manifest delivery order")
        }
        if chunk_index != bundle.next_chunk {
            bail!("final chunk indices are not contiguous")
        }
        let offset = chunk_index
            .checked_mul(MAX_FINAL_CHUNK_BYTES)
            .context("final chunk offset overflow")?;
        let expected_bytes = (entry.decoded_bytes - offset).min(MAX_FINAL_CHUNK_BYTES);
        let expected_base64 = expected_bytes.div_ceil(3) * 4;
        if bytes_base64.len() != expected_base64 {
            bail!("final chunk encoded size is not canonical")
        }
        let delivery = &mut bundle.deliveries[bundle.current_delivery];
        let before = delivery.bytes.len();
        delivery.bytes.reserve(expected_bytes);
        if let Err(error) = STANDARD.decode_vec(bytes_base64.as_bytes(), &mut delivery.bytes) {
            delivery.bytes.truncate(before);
            return Err(error).context("final chunk is not valid base64");
        }
        let decoded = &delivery.bytes[before..];
        if decoded.len() != expected_bytes {
            delivery.bytes.truncate(before);
            bail!("final chunk decoded size is not the canonical partition")
        }
        bundle.buffered_bytes = bundle
            .buffered_bytes
            .checked_add(decoded.len())
            .context("final bundle size overflow")?;
        if bundle.buffered_bytes > MAX_BUNDLE_BYTES {
            delivery.bytes.truncate(before);
            bail!("final bundle exceeds decoded byte limit")
        }
        delivery.digest.update(decoded);
        bundle.next_chunk += 1;
        if bundle.next_chunk == entry.chunk_count {
            bundle.current_delivery += 1;
            bundle.next_chunk = 0;
        }
        Ok(())
    }

    fn end(
        &mut self,
        request_id: RequestId,
        bundle_id: BundleId,
        manifest_sha256: &str,
    ) -> Result<VerifiedFinalBundle> {
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
        if bundle.current_delivery != bundle.manifest.len() || bundle.next_chunk != 0 {
            bail!("final bundle ended before every canonical chunk arrived")
        }
        if bundle.buffered_bytes
            != bundle
                .manifest
                .iter()
                .map(|entry| entry.decoded_bytes)
                .sum::<usize>()
        {
            bail!("final bundle decoded total does not match manifest")
        }

        let mut verified_deliveries = Vec::with_capacity(bundle.deliveries.len());
        for (entry, delivery) in bundle.manifest.iter().zip(&bundle.deliveries) {
            if delivery.bytes.len() != entry.decoded_bytes {
                bail!("final delivery decoded size does not match manifest")
            }
            if format!("{:x}", delivery.digest.clone().finalize()) != entry.sha256 {
                bail!("final delivery hash does not match manifest")
            }
            let payload =
                parse_payload(&delivery.bytes).context("final delivery payload is invalid")?;
            payload.validate()?;
            if payload.kind() != entry.payload_kind {
                bail!("final delivery kind does not match manifest")
            }
            verified_deliveries.push(VerifiedDelivery {
                delivery_id: entry.delivery_id.clone(),
                payload: serde_json::from_slice(&delivery.bytes)
                    .context("validated final payload could not be decoded")?,
            });
        }
        self.final_seen = true;
        Ok(VerifiedFinalBundle {
            request_id,
            bundle_id,
            replay: bundle.replay,
            manifest_sha256: manifest_sha256.to_owned(),
            deliveries: verified_deliveries,
        })
    }

    #[cfg(test)]
    fn buffer_capacity_bytes(&self) -> usize {
        self.bundle
            .as_ref()
            .map(|bundle| {
                bundle
                    .deliveries
                    .iter()
                    .map(|delivery| delivery.bytes.capacity())
                    .sum()
            })
            .unwrap_or(0)
    }
}

impl Default for FinalBundleVerifier {
    fn default() -> Self {
        Self::new()
    }
}

enum FinalPayloadView<'a> {
    Text {
        text: &'a RawValue,
    },
    File {
        artifact: ManagedArtifactView<'a>,
    },
    TerminalError {
        code: &'a RawValue,
        message: &'a RawValue,
    },
}

impl FinalPayloadView<'_> {
    fn kind(&self) -> &'static str {
        match self {
            Self::Text { .. } => "text",
            Self::File { .. } => "file",
            Self::TerminalError { .. } => "terminal_error",
        }
    }

    fn validate(&self) -> Result<()> {
        match self {
            Self::Text { text } => validate_json_string(text),
            Self::File { artifact } => artifact.validate(),
            Self::TerminalError { code, message } => {
                validate_json_string(code)?;
                validate_json_string(message)
            }
        }
    }
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ManagedArtifactView<'a> {
    #[serde(borrow)]
    id: &'a RawValue,
    #[serde(borrow)]
    managed_path: &'a RawValue,
    #[serde(borrow)]
    display_name: &'a RawValue,
    #[serde(borrow)]
    media_type: &'a RawValue,
    size: u64,
    #[serde(borrow)]
    sha256: &'a RawValue,
    #[serde(borrow)]
    caption: &'a RawValue,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct FinalPayloadObject<'a> {
    #[serde(rename = "type", borrow)]
    kind: &'a RawValue,
    #[serde(default, borrow)]
    text: Option<&'a RawValue>,
    #[serde(default, borrow)]
    artifact: Option<ManagedArtifactView<'a>>,
    #[serde(default, borrow)]
    code: Option<&'a RawValue>,
    #[serde(default, borrow)]
    message: Option<&'a RawValue>,
}

fn parse_payload(bytes: &[u8]) -> Result<FinalPayloadView<'_>> {
    let payload: FinalPayloadObject<'_> =
        serde_json::from_slice(bytes).context("final payload shape is not canonical")?;
    match payload.kind.get() {
        "\"text\""
            if payload.artifact.is_none()
                && payload.code.is_none()
                && payload.message.is_none() =>
        {
            Ok(FinalPayloadView::Text {
                text: payload.text.context("final text payload is missing text")?,
            })
        }
        "\"file\""
            if payload.text.is_none() && payload.code.is_none() && payload.message.is_none() =>
        {
            Ok(FinalPayloadView::File {
                artifact: payload
                    .artifact
                    .context("final file payload is missing artifact")?,
            })
        }
        "\"terminal_error\"" if payload.text.is_none() && payload.artifact.is_none() => {
            Ok(FinalPayloadView::TerminalError {
                code: payload
                    .code
                    .context("final terminal error payload is missing code")?,
                message: payload
                    .message
                    .context("final terminal error payload is missing message")?,
            })
        }
        _ => bail!("final payload fields or type are not canonical"),
    }
}

impl ManagedArtifactView<'_> {
    fn validate(&self) -> Result<()> {
        let id = collect_small_json_string(self.id, 64)?;
        ArtifactId::parse(std::str::from_utf8(&id)?).context("final artifact ID is not a UUID")?;

        let mut path_first = None;
        visit_json_string(self.managed_path, |decoded| {
            if path_first.is_none() {
                path_first = decoded.first().copied();
            }
            if decoded.contains(&0) {
                bail!("final artifact path contains NUL")
            }
            Ok(())
        })?;
        if path_first != Some(b'/') {
            bail!("final artifact managed path must be absolute")
        }

        require_nonempty_json_string(self.display_name, "display name")?;
        require_nonempty_json_string(self.media_type, "media type")?;
        let sha256 = collect_small_json_string(self.sha256, 64)?;
        if sha256.len() != 64 || !sha256.iter().all(u8::is_ascii_hexdigit) {
            bail!("final artifact SHA-256 must be 64 hexadecimal characters")
        }
        if self.caption.get() != "null" {
            validate_json_string(self.caption)?;
        }
        let _ = self.size;
        Ok(())
    }
}

fn validate_json_string(value: &RawValue) -> Result<()> {
    visit_json_string(value, |_| Ok(()))
}

fn require_nonempty_json_string(value: &RawValue, field: &str) -> Result<()> {
    let mut decoded_bytes = 0usize;
    visit_json_string(value, |decoded| {
        decoded_bytes += decoded.len();
        Ok(())
    })?;
    if decoded_bytes == 0 {
        bail!("final artifact {field} must not be empty")
    }
    Ok(())
}

fn collect_small_json_string(value: &RawValue, limit: usize) -> Result<Vec<u8>> {
    let mut decoded = Vec::new();
    visit_json_string(value, |bytes| {
        if decoded.len().saturating_add(bytes.len()) > limit {
            bail!("final payload identifier exceeds its size limit")
        }
        decoded.extend_from_slice(bytes);
        Ok(())
    })?;
    Ok(decoded)
}

fn visit_json_string(value: &RawValue, mut decoded: impl FnMut(&[u8]) -> Result<()>) -> Result<()> {
    let bytes = value.get().as_bytes();
    if bytes.len() < 2 || bytes.first() != Some(&b'"') || bytes.last() != Some(&b'"') {
        bail!("final payload field must be a JSON string")
    }
    let bytes = &value.get().as_bytes()[1..value.get().len() - 1];
    let mut index = 0;
    let mut plain_start = 0;
    while index < bytes.len() {
        if bytes[index] < 0x20 || bytes[index] == b'"' {
            bail!("final payload string contains an unescaped control or quote")
        }
        if bytes[index] != b'\\' {
            index += 1;
            continue;
        }
        decoded(&bytes[plain_start..index])?;
        index += 1;
        let escape = *bytes.get(index).context("incomplete JSON string escape")?;
        index += 1;
        match escape {
            b'"' | b'\\' | b'/' => decoded(&[escape])?,
            b'b' => decoded(&[0x08])?,
            b'f' => decoded(&[0x0c])?,
            b'n' => decoded(b"\n")?,
            b'r' => decoded(b"\r")?,
            b't' => decoded(b"\t")?,
            b'u' => {
                let (mut scalar, next) = parse_hex_escape(bytes, index)?;
                index = next;
                if (0xd800..=0xdbff).contains(&scalar) {
                    if bytes.get(index..index + 2) != Some(b"\\u") {
                        bail!("high surrogate is missing its low surrogate")
                    }
                    let (low, next) = parse_hex_escape(bytes, index + 2)?;
                    if !(0xdc00..=0xdfff).contains(&low) {
                        bail!("invalid low surrogate")
                    }
                    scalar = 0x1_0000 + ((scalar - 0xd800) << 10) + (low - 0xdc00);
                    index = next;
                }
                let character = char::from_u32(scalar).context("invalid Unicode scalar")?;
                let mut encoded = [0_u8; 4];
                decoded(character.encode_utf8(&mut encoded).as_bytes())?;
            }
            _ => bail!("invalid JSON string escape"),
        }
        plain_start = index;
    }
    decoded(&bytes[plain_start..])?;
    Ok(())
}

fn parse_hex_escape(bytes: &[u8], start: usize) -> Result<(u32, usize)> {
    let digits = bytes
        .get(start..start + 4)
        .context("incomplete Unicode escape")?;
    let mut value = 0_u32;
    for digit in digits {
        value = value * 16
            + match digit {
                b'0'..=b'9' => u32::from(digit - b'0'),
                b'a'..=b'f' => u32::from(digit - b'a' + 10),
                b'A'..=b'F' => u32::from(digit - b'A' + 10),
                _ => bail!("invalid Unicode escape"),
            };
    }
    Ok((value, start + 4))
}

impl LocalRenderer<Vec<u8>> {
    pub fn output(&self) -> &[u8] {
        &self.output
    }
}
