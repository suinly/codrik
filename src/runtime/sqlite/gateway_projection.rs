use anyhow::Result;
use tokio_rusqlite::{params, rusqlite::Transaction};

use crate::runtime::{
    gateway::{DeliveryRoute, split_unicode},
    model::{GatewayDeliveryId, OutboxId, Timestamp},
    store::OutboxPayload,
};

pub(super) fn project_outbox_to_gateway(
    transaction: &Transaction<'_>,
    intent_key: &str,
    outbox_id: &OutboxId,
    payload: &OutboxPayload,
    route: &DeliveryRoute,
    now: Timestamp,
) -> Result<()> {
    let payloads = match payload {
        OutboxPayload::Text { text } => split_unicode(text, route.max_text_chars)
            .into_iter()
            .map(|text| OutboxPayload::Text { text })
            .collect(),
        OutboxPayload::TerminalError { message, .. } => {
            split_unicode(message, route.max_text_chars)
                .into_iter()
                .map(|text| OutboxPayload::Text { text })
                .collect()
        }
        OutboxPayload::File { caption, .. }
            if caption
                .as_ref()
                .is_some_and(|caption| caption.chars().count() > route.max_caption_chars) =>
        {
            let mut payloads =
                split_unicode(caption.as_deref().unwrap_or_default(), route.max_text_chars)
                    .into_iter()
                    .map(|text| OutboxPayload::Text { text })
                    .collect::<Vec<_>>();
            let mut file = payload.clone();
            if let OutboxPayload::File { caption, .. } = &mut file {
                *caption = None;
            }
            payloads.push(file);
            payloads
        }
        OutboxPayload::File { .. } => vec![payload.clone()],
    };
    for (ordinal, payload) in payloads.into_iter().enumerate() {
        transaction.execute(
            "INSERT INTO gateway_deliveries(
                id, intent_key, source_outbox_id, gateway, address,
                reply_to_external_id, max_text_chars, max_caption_chars,
                ordinal, payload_json, state, attempt_count, created_at, updated_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, 'pending', 0, ?11, ?11)
             ON CONFLICT(intent_key) DO NOTHING",
            params![
                GatewayDeliveryId::new().as_str(),
                format!("gateway:{intent_key}:{ordinal}"),
                outbox_id.as_str(),
                route.gateway,
                route.address,
                route.reply_to_external_id,
                route.max_text_chars as i64,
                route.max_caption_chars as i64,
                ordinal as i64,
                serde_json::to_string(&payload)?,
                now.0,
            ],
        )?;
    }
    Ok(())
}
