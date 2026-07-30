use std::path::PathBuf;

use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};

pub const MAX_PROTOCOL_LINE_BYTES: usize = 1024 * 1024;
pub const MAX_TEXT_BYTES: usize = 256 * 1024;
const LXMF_HASH_HEX_CHARS: usize = 64;
const DESTINATION_HASH_HEX_CHARS: usize = 32;
const MAX_DELIVERY_ID_BYTES: usize = 128;
const MAX_ERROR_BYTES: usize = 4096;

#[derive(Clone, Debug, Serialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum BridgeCommand {
    Start {
        state_dir: PathBuf,
        rns_host: String,
        rns_port: u16,
    },
    Send {
        delivery_id: String,
        destination: String,
        text: String,
    },
    Shutdown,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum BridgeEvent {
    Ready {
        destination: String,
    },
    Inbound {
        message_hash: String,
        source: String,
        timestamp: f64,
        text: String,
    },
    Delivery {
        delivery_id: String,
        outcome: BridgeDeliveryOutcome,
        retry_after_ms: Option<u64>,
    },
    Fatal {
        error: String,
    },
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BridgeDeliveryOutcome {
    Delivered,
    Retryable,
    Terminal,
    OutcomeUnknown,
}

pub fn decode_event(line: &[u8]) -> Result<BridgeEvent> {
    if line.len() > MAX_PROTOCOL_LINE_BYTES {
        bail!("Reticulum bridge protocol line is too large");
    }
    let event: BridgeEvent = serde_json::from_slice(line)?;
    match &event {
        BridgeEvent::Ready { destination } => {
            validate_hash(destination, DESTINATION_HASH_HEX_CHARS)?
        }
        BridgeEvent::Inbound {
            message_hash,
            source,
            timestamp,
            text,
        } => {
            validate_hash(message_hash, LXMF_HASH_HEX_CHARS)?;
            validate_hash(source, DESTINATION_HASH_HEX_CHARS)?;
            if !timestamp.is_finite() || *timestamp < 0.0 {
                bail!("Reticulum bridge timestamp is invalid");
            }
            validate_text(text)?;
        }
        BridgeEvent::Delivery {
            delivery_id,
            outcome,
            retry_after_ms,
        } => {
            validate_delivery_id(delivery_id)?;
            if !matches!(outcome, BridgeDeliveryOutcome::Retryable) && retry_after_ms.is_some() {
                bail!("Reticulum bridge retry delay requires a retryable outcome");
            }
        }
        BridgeEvent::Fatal { error } => {
            if error.trim().is_empty() || error.len() > MAX_ERROR_BYTES {
                bail!("Reticulum bridge fatal error is invalid");
            }
        }
    }
    Ok(event)
}

pub fn encode_command(command: &BridgeCommand) -> Result<Vec<u8>> {
    match command {
        BridgeCommand::Start {
            state_dir,
            rns_host,
            rns_port,
        } => {
            if !state_dir.is_absolute() || rns_host.trim().is_empty() || *rns_port == 0 {
                bail!("Reticulum bridge start command is invalid");
            }
        }
        BridgeCommand::Send {
            delivery_id,
            destination,
            text,
        } => {
            validate_delivery_id(delivery_id)?;
            validate_hash(destination, DESTINATION_HASH_HEX_CHARS)?;
            validate_text(text)?;
        }
        BridgeCommand::Shutdown => {}
    }
    let mut encoded = serde_json::to_vec(command)?;
    if encoded.len() + 1 > MAX_PROTOCOL_LINE_BYTES {
        bail!("Reticulum bridge protocol line is too large");
    }
    encoded.push(b'\n');
    Ok(encoded)
}

fn validate_hash(value: &str, expected_len: usize) -> Result<()> {
    if value.len() != expected_len
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        bail!("Reticulum bridge hash is invalid");
    }
    Ok(())
}

fn validate_text(text: &str) -> Result<()> {
    if text.trim().is_empty() || text.len() > MAX_TEXT_BYTES {
        bail!("Reticulum bridge text is invalid");
    }
    Ok(())
}

fn validate_delivery_id(delivery_id: &str) -> Result<()> {
    if delivery_id.trim().is_empty() || delivery_id.len() > MAX_DELIVERY_ID_BYTES {
        bail!("Reticulum bridge delivery ID is invalid");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use anyhow::Result;

    use super::{
        BridgeCommand, BridgeEvent, MAX_PROTOCOL_LINE_BYTES, decode_event, encode_command,
    };

    #[test]
    fn valid_events_round_trip_and_validate_hashes() -> Result<()> {
        let inbound = br#"{"type":"inbound","message_hash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","source":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","timestamp":42.5,"text":"hello"}"#;
        assert!(matches!(
            decode_event(inbound)?,
            BridgeEvent::Inbound { text, .. } if text == "hello"
        ));
        let command = BridgeCommand::Send {
            delivery_id: "delivery-1".into(),
            destination: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".into(),
            text: "reply".into(),
        };
        let encoded = encode_command(&command)?;
        assert!(encoded.ends_with(b"\n"));
        assert_eq!(encoded.iter().filter(|byte| **byte == b'\n').count(), 1);
        Ok(())
    }

    #[test]
    fn invalid_or_oversized_protocol_objects_fail_closed() {
        for line in [
            br#"{"type":"unknown"}"#.as_slice(),
            br#"{"type":"ready","destination":"xyz"}"#.as_slice(),
            br#"{"type":"inbound","message_hash":"aa","source":"bb","timestamp":1,"text":"x"}"#.as_slice(),
            br#"{"type":"delivery","delivery_id":"","outcome":"delivered"}"#.as_slice(),
            br#"{"type":"inbound","message_hash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","source":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","timestamp":-1,"text":"x"}"#.as_slice(),
            br#"{"type":"fatal","error":""}"#.as_slice(),
        ] {
            assert!(decode_event(line).is_err());
        }
        assert!(decode_event(&vec![b'x'; MAX_PROTOCOL_LINE_BYTES + 1]).is_err());
        assert!(
            encode_command(&BridgeCommand::Send {
                delivery_id: "delivery-1".into(),
                destination: "ABCDEFABCDEFABCDEFABCDEFABCDEFAB".into(),
                text: "reply".into(),
            })
            .is_err()
        );
    }
}
