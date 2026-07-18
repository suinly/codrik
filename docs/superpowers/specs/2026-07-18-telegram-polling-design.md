# Telegram Polling Design

## Goal

Add an explicit Telegram long-polling ingress mode for installations that
cannot accept public webhook connections. Polling and webhook delivery remain
mutually exclusive transports feeding the same durable Telegram ingress.

## Scope

This change adds:

- `telegram.mode: polling` alongside the existing webhook mode;
- Bot API support for `deleteWebhook` and `getUpdates`;
- a supervised long-poll worker;
- conditional Telegram configuration validation;
- documentation and regression coverage for both modes.

It does not add automatic webhook fallback, a second agent loop, a sidecar,
another Telegram dependency, or persistent polling cursors.

## Configuration

`telegram.mode` is an enum with `webhook` and `polling` values. Omitting it
selects `webhook`, preserving existing configuration behavior.

Webhook mode continues to require and validate:

```yaml
telegram:
  token: "..."
  mode: webhook
  public_url: "https://agent.example.com/webhooks/telegram"
  listen: "127.0.0.1:8080"
  webhook_secret: "..."
```

Polling mode requires only the token:

```yaml
telegram:
  token: "..."
  mode: polling
```

For an easy mode switch, polling accepts but does not use `public_url`,
`listen`, or `webhook_secret` when they remain in the file. Unknown Telegram
fields remain rejected. A blank token is rejected in both modes.

Validated configuration exposes a typed transport enum rather than optional
webhook fields. The webhook variant owns its parsed HTTPS URL, socket address,
and validated secret; the polling variant has no webhook state.

## Architecture

`TelegramIngressService`, `TelegramIngress`, and the ingress outcome move from
the webhook module to a transport-neutral ingress module. Both transports pass
the same `TelegramUpdate` into that service, preserving identity linking,
authorization, durable deduplication, actor notification, and reply routing.

The prepared gateway owns one typed ingress transport:

- webhook: the bound listener, route path, and secret;
- polling: no listener or public endpoint.

Its single supervised `ingress` method dispatches to `TelegramWebhookServer`
or the polling worker. The composition root registers this as
`telegram-ingress`. Existing delivery and activity components remain separate
and unchanged.

The current broad Bot API trait is split at the touched boundary:

- `TelegramApi` retains outbound delivery and activity methods;
- `TelegramIngressApi` owns `getMe`, webhook configuration, and polling calls.

`ReqwestTelegramApi` implements both. Delivery and activity test doubles no
longer need unrelated webhook or polling methods.

## Polling Startup

Polling startup performs these operations in order:

1. call `getMe` and validate the bot identity exactly as webhook mode does;
2. call `deleteWebhook` with `drop_pending_updates: false`;
3. call `getWebhookInfo` and require an empty webhook URL;
4. report readiness only after this reconciliation succeeds;
5. start the supervised polling loop.

Existing pending Telegram updates are therefore retained when switching from
webhook mode.

## Polling Loop

One worker issues `getUpdates` with:

- the current optional offset;
- a 25-second Telegram long-poll timeout, fitting inside the existing
  30-second HTTP request timeout;
- limit `100`;
- `allowed_updates: ["message"]`.

Returned updates are sorted by update ID and handled sequentially in ascending
order, so malformed response ordering cannot acknowledge an unprocessed lower
ID. After each successful ingress result, including duplicate, command, or
unsupported, the worker sets the next offset to the checked value
`update_id + 1`. It never advances past an update whose ingress handling failed.

The next `getUpdates` request acknowledges all lower update IDs at Telegram.
If Codrik crashes after durable handling but before that request, Telegram may
redeliver the update. Existing durable idempotency by Telegram update ID makes
the replay safe, so no local cursor storage is added.

Only one polling worker exists for a configured bot. Polling never runs while
the webhook transport is active.

## Errors and Shutdown

Read-only polling requests are retry-safe. Bot API rate limits use the supplied
`retry_after`; other retryable transport failures use bounded exponential
delays of 1, 2, 4, 8, 16, then 30 seconds. A successful poll resets the delay.

An ingress handling failure retains the failing offset and uses the same
bounded retry delay before requesting it again. Terminal Bot API or
configuration errors exit the ingress component and flow through the existing
runtime supervisor failure path.

Shutdown interrupts an active long poll or retry delay through the existing
watch channel. It does not wait for the full timeout before allowing graceful
service shutdown.

## Testing

Focused tests cover:

- omitted mode retaining webhook behavior;
- polling accepting a token alone and ignoring retained webhook fields;
- webhook-only fields remaining required and validated in webhook mode;
- exact `deleteWebhook(false)` and `getUpdates` Bot API payloads;
- polling startup ordering and empty-webhook reconciliation;
- no TCP listener bind in polling mode;
- ordered handling and offset advancement after success;
- no offset skip after ingress failure;
- retry-after, bounded backoff reset, and prompt shutdown;
- existing webhook startup and acceptance behavior;
- unchanged delivery and activity behavior.

README documentation shows both minimal configurations and states that modes
are explicit and mutually exclusive. Installer and updater behavior does not
change because both modes run inside `codrik serve`.
