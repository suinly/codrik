# Silent Webhook Telegram Status Design

## Goal

Webhook-triggered runs may show Telegram typing activity, but must not create or
edit Telegram status messages. The actor receives only the existing final
webhook notification. Ordinary Telegram and other routed runs retain their
current activity behavior.

## Scope

Apply this behavior to every generic webhook endpoint, identified by a non-null
durable `AttachedRun.ingress_source`. Do not special-case Grafana or endpoint
names. Do not change webhook execution, tool access, finalization, route
snapshotting, deferred delivery, or observability.

## Design

Extend ephemeral `GatewayActivity` with the run's optional `ingress_source`.
`CompositeRuntimeEventPublisher` copies this trusted value from `AttachedRun`
whenever it publishes routed activity or text deltas.

`TelegramActivityWorker` uses the metadata only as a presentation policy:

- For ordinary activity (`ingress_source == None`), preserve all existing
  typing, status creation, status editing, and terminal status behavior.
- For webhook activity (`ingress_source != None`), process
  `ModelStepStarted` normally so Telegram receives typing actions.
- Ignore webhook `Description`, `ToolStarted`, `ToolFinished`, and terminal
  activity events. They must not create or edit a status message.
- Continue ignoring text deltas as today. Final user-visible text remains the
  durable outbox delivery, not ephemeral activity.

The filter belongs in the Telegram adapter because it is Telegram presentation
policy. The shared runtime continues publishing complete local activity for CLI
and IPC subscribers.

## State And Failure Handling

Webhook activity must not leave a Telegram `ActivityState` entry after a
terminal event. A typing-only state may exist while a run is active; terminal
webhook activity removes it without sending or editing a message. API failures
while sending typing remain best-effort, matching current behavior.

The final webhook notification and deferred latest-only delivery remain
authoritative. Suppressing status activity cannot suppress, duplicate, reroute,
or acknowledge the durable final outbox intent.

## Tests

Add focused tests proving:

- gateway activity copies webhook `ingress_source` from `AttachedRun`;
- webhook model start sends typing;
- webhook description and tool events send no Telegram message;
- webhook completion sends or edits no status and clears ephemeral state;
- a complete webhook tool run produces no service message before final outbox
  delivery;
- ordinary activity tests remain unchanged and continue creating/updating
  statuses.

Run Telegram activity, gateway activity, stream publisher, webhook end-to-end,
and full regression suites.
