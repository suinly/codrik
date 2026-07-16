# Telegram Rich Markdown Design

## Goal

Render final assistant Markdown correctly in Telegram while preserving Codrik's
durable, retry-safe delivery model.

The implementation targets Telegram Bot API 10.2 and uses the Rich Messages API
introduced in Bot API 10.1. Telegram accepts rich Markdown directly through
`sendRichMessage`, including headings, lists, tables, formulas, details,
footnotes, quotations, code, links, spoilers, and other rich message constructs.

Official references:

- <https://core.telegram.org/bots/api#rich-message-formatting-options>
- <https://core.telegram.org/bots/api#inputrichmessage>
- <https://core.telegram.org/bots/api#sendrichmessage>

## Scope

This change applies rich Markdown rendering to durable final text deliveries
sent through the Telegram gateway.

It does not change:

- model output or prompting;
- tool activity and elapsed-time status messages;
- typing indicators;
- file upload transport or caption formatting;
- private-chat reply-to behavior;
- the durable outbox and delivery claim model.

## Selected Approach

Codrik will send final text through `sendRichMessage` with an
`InputRichMessage` whose `markdown` field contains the original assistant text.
Codrik will not translate Markdown to MarkdownV2, HTML, message entities, or
explicit rich blocks.

Telegram remains the source of truth for parsing its current rich Markdown
dialect. This avoids duplicating Telegram's parser and automatically covers the
formatting constructs supported by the configured Bot API version.

## API Model

Add typed Telegram API commands:

```text
SendRichMessage
  chat_id
  rich_message

InputRichMessage
  markdown
```

Extend `TelegramApi` with `send_rich_message`. The reqwest adapter calls
`sendRichMessage` and decodes the returned message identifier through the same
Bot API envelope handling used by `sendMessage`.

Rich-message sends are not retry-safe at the transport layer. A connection
failure after request transmission therefore remains `outcome_unknown`.

## Delivery Flow

For an `OutboxPayload::Text` Telegram delivery:

1. Call `sendRichMessage` with the chunk's original Markdown.
2. If it succeeds, complete the durable delivery using the returned Telegram
   message identifier.
3. If Telegram definitively rejects it with a terminal API response, call
   `sendMessage` once with the same text and no parse mode.
4. If the rich send fails as retryable or outcome-unknown, preserve the existing
   delivery transition and do not attempt a second transport.

Fallback on terminal rejection is safe because Telegram has definitively
rejected the rich message. It covers malformed Markdown, unsupported rich
message methods, and other definitive rejections without risking duplicate
delivery. If the plain send also fails, its error classification controls the
durable delivery transition.

Tool statuses remain plain `sendMessage` calls because they are transient UI,
not assistant-authored Markdown.

## Chunking and Limits

Keep the current Telegram route text limit at 4096 characters and the existing
durable Unicode chunk projection. Each persisted gateway delivery continues to
map to exactly one successful Telegram message.

Telegram Rich Messages allow more text, but raising the limit would make the
plain fallback exceed `sendMessage` limits and would require a multi-message
transport inside one durable delivery claim. That would weaken retry and
duplicate guarantees.

If a 4096-character boundary splits Markdown syntax, Telegram may reject that
chunk. The terminal plain-text fallback ensures the content remains readable,
although formatting may be lost for that chunk. Markdown-aware durable
chunking is explicitly deferred because it would require adding a
gateway-specific formatting policy to the gateway projection model.

## Error Handling

- Rich send succeeds: mark delivered.
- Rich send returns a terminal Bot API error: attempt one plain send.
- Rich send is retryable: retain existing bounded retry behavior; no fallback.
- Rich send outcome is unknown: mark `outcome_unknown`; no fallback.
- Plain fallback succeeds: mark delivered using its message identifier.
- Plain fallback fails: classify and transition exactly like an ordinary
  `sendMessage` failure.

The implementation must not inspect error strings to decide whether a fallback
is safe. It relies only on the existing typed terminal classification.

## Testing

### Telegram API tests

- `send_rich_message` posts to `sendRichMessage`;
- the JSON body contains `chat_id` and `rich_message.markdown`;
- no reply parameters or parse mode are added.

### Delivery tests

- valid Markdown uses only `sendRichMessage`;
- terminal rich rejection falls back once to plain `sendMessage`;
- retryable rich failure does not fall back;
- outcome-unknown rich failure does not fall back;
- plain fallback errors retain their own classification;
- private final text remains free of reply-to parameters.

Representative Markdown fixtures include headings, nested lists, tables,
fenced code, links, quotations, spoilers, formulas, and details blocks.

### Acceptance coverage

The Telegram webhook-to-delivery acceptance test records the rich message
payload and verifies that assistant Markdown reaches `sendRichMessage`
unchanged. The existing duplicate webhook and restarted delivery assertions
remain intact.

## Documentation

Update the Telegram section in `README.md` to state that final assistant text
uses Telegram Rich Messages and that terminal formatting rejection falls back
to readable plain text. Mention that the durable 4096-character chunk limit
remains in place.

## Non-Goals

- streaming partial rich-message drafts;
- converting tool statuses to rich messages;
- embedding generated media inside rich Markdown;
- formatting file captions;
- implementing a local Telegram Markdown parser;
- increasing the durable Telegram text chunk size;
- changing group or channel support.
