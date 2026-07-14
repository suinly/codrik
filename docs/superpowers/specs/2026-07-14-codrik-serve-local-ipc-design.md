# Codrik Serve, Local IPC, and Durable Delivery Design

## Goal

Turn the internal durable kernel into the only interactive execution path for
the local CLI. A foreground `codrik serve` process owns the runtime database,
continuously dispatches actor work, streams best-effort model progress over a
Unix socket, and delivers every authoritative final result through durable
outbox delivery records.

After this change, `codrik "question"` never runs an agent in the client
process. It requires a running daemon and fails clearly when the daemon is
unavailable.

## Scope

This slice includes:

- the `codrik serve` foreground supervisor;
- an exclusive single-instance lock;
- production SQLite, socket, and actor configuration;
- a versioned framed-JSON Unix-socket protocol;
- `codrik "question"` submission and live rendering;
- `codrik resume <request-id>` after disconnect;
- best-effort text and activity streaming;
- durable final text, file, and terminal-error delivery;
- outbox delivery claims, acknowledgements, retry, and recovery;
- a continuous actor dispatcher with persisted failure backoff;
- graceful shutdown and crash recovery;
- installer service definitions that launch `codrik serve`.

This slice deliberately removes:

- legacy one-shot execution in the CLI process;
- `--session`, `--stream`, and other session-oriented CLI variants;
- the legacy `codrik gateway telegram` polling command.

This slice does not include Telegram webhooks, a public HTTP listener, identity
linking codes, actor merging, recurring tasks, long-term memory extraction,
or migration and deletion of legacy session files. Telegram is temporarily
unavailable until its webhook slice lands.

## User-visible commands

The supported commands are:

```text
codrik serve
codrik "question"
codrik resume <request-id>
codrik update
```

`codrik serve` remains in the foreground. systemd or launchd owns background
execution and restart policy; Codrik does not daemonize itself.

`codrik "question"` waits without an artificial response timeout. Ctrl-C
disconnects the client but does not cancel durable actor work. The client
persists request metadata before sending and prints a recovery command:

```text
codrik resume 0190f2ef-...
```

Request identifiers are operational references to individual submissions,
not sessions or memory boundaries.

## Configuration

`config.yml` gains a required runtime section:

```yaml
runtime:
  actor_id: actor:telegram:12312931
  database_path: ~/.codrik/runtime.sqlite
  socket_path: ~/.codrik/codrik.sock
  lock_path: ~/.codrik/runtime.lock
```

Only `actor_id` is logically required. The paths default to the values shown
and honor `CODRIK_HOME`. Tilde expansion is performed only for a leading
`~/`; arbitrary shell expansion is never performed.

At startup the configured actor must exist in the runtime authorization store
and be enabled. Authorization is imported once from the existing `users.json`
without modifying that file. The explicit actor selection avoids silently
choosing the wrong identity and lets the local CLI use the same actor memory as
an existing Telegram identity.

For a clean interactive installation only, the installer creates
`actor:local:owner` with standard tool access and writes the matching
`runtime.actor_id` when `users.json` is absent or contains no actors. It never
adds, selects, enables, or rewrites an actor when authorization already exists.
A manual installation with no actor fails readiness with an actionable example
instead of silently granting access from the daemon.

The Codrik directory is created with mode `0700`; the Unix socket is mode
`0600`. Linux and macOS are the supported local IPC platforms.

## Process architecture

`codrik serve` is the production composition root. It builds and supervises
the following independent components:

- `InstanceLock` owns the exclusive OS file lock.
- `SqliteRuntimeStore` owns migrations and durable state.
- `LocalIpcServer` accepts and authenticates local Unix connections.
- `StreamHub` owns bounded, in-process transient subscriptions.
- `ActorDispatcher` continuously leases and executes ready actor work.
- `OutboxWorker` claims and delivers durable local delivery records.
- `Supervisor` controls component lifetime and graceful shutdown.

Composition stays in `app.rs`; protocol, dispatch, persistence, and rendering
do not depend on concrete OpenAI, SQLite, or CLI implementations beyond their
declared adapter traits.

An unexpected exit from the IPC listener, dispatcher, or outbox worker is a
process-level failure. The supervisor cancels its siblings and exits nonzero so
the service manager can restart the daemon. An error in one actor quantum is
isolated to its work item and never terminates the supervisor.

## Startup sequence

Startup is ordered and fail-closed:

1. Load and validate configuration.
2. Create the Codrik directory with mode `0700`.
3. Acquire the exclusive runtime lock.
4. Open SQLite and apply versioned migrations.
5. Import the legacy authorization snapshot if it has not been imported.
6. Verify that `runtime.actor_id` exists and is enabled.
7. Remove a stale socket path only while holding the instance lock.
8. Bind the Unix listener and set its mode to `0600`.
9. Recover expired leases, outbox claims, and interrupted attempts.
10. Start the stream hub, dispatcher, outbox worker, and IPC accept loop.
11. Emit a structured ready log entry.

A second daemon fails on the lock before touching the socket. Correctness does
not depend on the lock alone: existing actor and outbox fencing remains
authoritative if a process is misconfigured.

The lock is an OS-managed advisory lock, so process death releases it. The
socket file is removed only by the process that holds the lock.

## IPC protocol

The socket uses length-prefixed JSON frames. Each frame starts with an unsigned
32-bit big-endian byte length followed by exactly one UTF-8 JSON object. The
maximum payload length is 1 MiB. Invalid UTF-8, malformed JSON, oversized
frames, and unsupported protocol versions are rejected before durable ingress.
Submit text must contain at least one non-whitespace character and may contain
at most 256 KiB of UTF-8 bytes; the original accepted text is preserved.

Every request carries `version: 1` and one of these bodies:

```text
Submit {
  request_id: UUID,
  text: String
}

Resume {
  request_id: UUID
}

AckFinal {
  request_id: UUID,
  delivery_ids: [UUID]
}
```

The daemon emits:

```text
Accepted {
  request_id: UUID,
  work_item_id: UUID,
  sequence: Integer
}

Activity {
  request_id: UUID,
  event: ActivityEvent
}

TextDelta {
  request_id: UUID,
  delta: String
}

StreamGap {
  request_id: UUID
}

Final {
  request_id: UUID,
  deliveries: [FinalDelivery]
}

RequestError {
  request_id: UUID,
  code: String,
  message: String
}
```

`RequestError` is reserved for failures that prevent a request from entering
or resolving durable work, such as a protocol violation, conflicting request
ID, missing request, or disabled actor. A failure after acceptance is a typed
durable error delivery inside `Final`, not a transient `RequestError`.

`FinalDelivery` includes its delivery ID and one typed text, file, or terminal
error payload. File payloads contain an authorized local path, display name,
media type, and optional caption. The CLI displays this metadata and never
copies a file implicitly.

The server sends `Accepted` before forwarding transient stream events on that
connection. Protocol errors close only the affected connection.

## Durable request correlation

Submission is represented by a new `local_requests` table:

```text
request_id         primary key
actor_id           foreign key
event_id           unique foreign key
work_item_id       foreign key
prompt_sha256      fixed lowercase hex
state              active | completed | cancelled | failed_terminal
created_at
updated_at
```

The local-request row and inbound event are inserted in the same ingress
transaction. The request ID is also the gateway external ID. Repeating a
request ID with the same prompt is idempotent and returns the original event,
work item, and sequence. Reusing it with different text is a conflict and never
creates another event.

`Resume` resolves the request through `local_requests`; it never creates work.
An active request subscribes to live events and waits for final delivery. A
completed, cancelled, or failed request immediately replays its retained final
deliveries.

Several local submissions may be incorporated into one interactive work item.
The runner publishes transient events to every attached active local request.
At finalization, each incorporated local request receives its own delivery row
for the same immutable outbox intent. This prevents an older waiting client
from hanging when newer compatible input amends the run.

## Streaming semantics

The durable runner uses a streaming-capable model abstraction. Model text
deltas and activity events are published to `StreamHub`; they are never treated
as checkpoints and are not written to SQLite.

Each request subscription has a bounded queue. When the queue fills, the hub
drops only transient text and activity events and schedules one `StreamGap`.
After a gap, newer deltas may continue. The final full response remains
authoritative and lets the renderer replace or complete partial output.

Disconnecting drops the subscription, not the actor `RunContext`. A later
resume does not replay old deltas. It either joins the still-live stream or
returns the retained durable final.

The server registers the request subscription before durable submission. The
runner publishes by attached request ID, so it cannot emit for that request
before the subscription exists. A duplicate submit or resume replaces no
existing subscriber; multiple clients may observe the same request.

## Durable output and delivery rows

The existing outbox intent remains the immutable logical result. Schema
migration v2 rebuilds the internal, not-yet-public v1 outbox table as an
intent-only table and moves all claim, attempt, and delivery state into child
`outbox_deliveries` rows:

```text
id
outbox_id
transport          local_ipc
address            request UUID
state              pending | delivering | delivered | failed_retryable |
                   failed_terminal | outcome_unknown | acknowledged_duplicate
attempt_count
claim_owner
claim_expires_at
last_error
created_at
updated_at
unique(outbox_id, transport, address)
```

Finalization atomically creates the outbox intent, expands its reply routes
into one delivery row per incorporated local request, completes only
incorporated events, and completes the run and work item. Text and tool-produced
file artifacts are both converted into typed intents. No runner calls a CLI or
gateway sink for final output.

The outbox worker considers a local delivery claimable only when at least one
subscriber exists for its request ID. It claims a bounded batch with an
expiring lease, sends `Final`, and waits for `AckFinal`. ACK is idempotent and
transitions only the addressed delivery IDs to `delivered`.

The first slice uses one outbox worker, batches at most 32 delivery rows, and
uses a 30-second delivery claim and ACK deadline. It does not increment an
attempt merely because no subscriber exists.

If the connection closes before ACK, the claim is released to
`failed_retryable`; an expired local claim is safe to retry. A client may have
displayed the result before losing its ACK, so local delivery is explicitly
at-least-once. Delivered payloads are retained and remain replayable through
`Resume` without changing delivery state.

The logical outbox intent has no aggregate delivery state. Each target evolves
independently, so one disconnected client cannot block or overwrite another
client's acknowledgement.

## Actor dispatcher and persisted failures

The dispatcher sleeps on an in-process notification and a 500-millisecond
fallback poll.
SQLite remains authoritative; losing a notification can delay but cannot lose
work. A bounded number of worker tasks call the existing fenced runner. The
initial personal-runtime configuration uses one actor worker, while the
component boundary permits more actors later.

Work items persist:

```text
failure_count
next_attempt_at
last_error
```

The ready query excludes work whose `next_attempt_at` is in the future. A
recoverable model or runner failure increments `failure_count`. Failures one
through four schedule retries after 1, 2, 4, and 8 seconds. The fifth
consecutive failed quantum moves the request and work item to
`failed_terminal` and atomically creates a typed durable error outbox intent.
The client receives that payload in `Final`. A successful checkpoint resets
the counter only when it records new model output, a known tool outcome, or
finalization after the most recent failure. Replaying an already incorporated
source-event checkpoint does not reset failure history; repeated failure of
the same model step therefore reaches the terminal limit.

SQLite busy errors receive a small bounded transaction retry before they reach
the dispatcher. A database I/O failure, corruption signal, or unsupported
schema version is process-critical: the supervisor shuts down for service
manager restart because it cannot truthfully persist a per-work-item failure
while the authority store is unavailable.

Malformed durable payloads are marked `blocked` immediately and reported in
structured diagnostics; they never form a busy loop. Tool execution errors
with known outcomes remain model observations and do not count as dispatcher
failures. An orphaned `Running` tool becomes `outcome_unknown` and moves the
work item to `waiting_for_decision`, as in the kernel protocol.

## Shutdown and recovery

SIGINT and SIGTERM initiate graceful shutdown:

1. stop accepting new IPC connections;
2. stop acquiring new actor and delivery leases;
3. allow active quanta and acknowledged deliveries up to 30 seconds;
4. cancel unfinished model futures without finalizing them;
5. close the listener and remove the socket while still holding the lock;
6. release the instance lock and exit.

If the grace period expires, the process exits without inventing outcomes. A
tool already committed as `Running` is recovered as `outcome_unknown`. Active
runs and incorporated checkpoints remain resumable under a new fence.

Startup recovery:

- reacquires actors only after persisted leases expire;
- resumes existing active runs rather than creating replacements;
- changes orphaned `Running` attempts to `outcome_unknown`;
- releases expired local delivery claims to retryable state;
- preserves completed outbox intents and deliveries without duplication.

## CLI rendering and local request files

Before connecting, the CLI writes request metadata atomically under:

```text
~/.codrik/client/requests/<request-id>.json
```

The file contains the request ID, creation time, prompt hash, and last observed
terminal state, but not the prompt text or response payload. The directory is
mode `0700`; files are mode `0600`.

The CLI renders activity with the existing spinner and writes text deltas as
they arrive. `StreamGap` clears any claim that the visible text is complete.
On `Final`, the CLI reconciles displayed text with the authoritative full text,
prints file metadata, sends `AckFinal`, and marks the local request metadata
terminal. A daemon-unavailable error names the expected socket and instructs
the user to start `codrik serve`.

## Installer behavior

Generated systemd and launchd services execute `codrik serve`. Upgrades replace
the old polling-gateway service definition. They do not start a second daemon
when the lock is held. Installation and rollback never delete the runtime
database, legacy sessions, or `users.json`.

On a clean interactive install, the installer may create the initial local
owner described in the configuration section. On upgrade or whenever
`users.json` already contains an actor, the file is treated as user-owned and
left byte-for-byte unchanged.

## Observability

Structured logs include component, actor ID, work item ID, run ID, request ID,
attempt ID, outbox ID, delivery ID, lease generation, transition, latency, and
redacted error class where applicable. Prompts, model text, tool payloads, and
outbox payloads are excluded from normal logs.

Startup logs identify the database and socket paths, schema version, selected
actor, recovered-item counts, and readiness. Terminal work failures and
unknown outcomes remain queryable in SQLite even before a dedicated operator
command exists.

## Testing strategy

Focused unit tests cover:

- protocol framing, version rejection, malformed frames, and size limits;
- request-ID idempotency and conflicting prompt reuse;
- bounded subscription queues and single-gap behavior;
- instance locking and stale-socket ownership;
- request-to-delivery route expansion;
- delivery claim, ACK, disconnect, retry, and expiration transitions;
- persisted failure backoff and terminal error creation;
- CLI parsing with all legacy variants rejected.

End-to-end tests use a real Unix socket, an on-disk SQLite database, a scripted
streaming model, and deterministic clocks:

1. Submit emits accepted, deltas, final, and ACKs delivery.
2. Duplicate submit creates no second event, run, intent, or delivery.
3. A request ID with different text is rejected.
4. Disconnect during streaming does not cancel durable work.
5. Resume joins a live run or replays its completed result.
6. Multiple incorporated requests all receive final delivery rows.
7. Restart after ingress, attachment, model output, and finalization is safe.
8. Crash after `Final` but before ACK may redeliver without recomputation.
9. A second daemon cannot acquire the lock or remove the live socket.
10. SIGTERM stops ingress and preserves resumable active state.
11. Orphaned running tools are never automatically invoked again.
12. Five consecutive runtime failures produce a terminal request delivery.
13. Missing or disabled configured actors prevent readiness.

Repository verification requires `cargo fmt --check`, `cargo test`,
`cargo check`, `cargo clippy --all-targets --all-features`, and a manual
foreground transcript covering submit, Ctrl-C, and resume.

## Acceptance criteria

The slice is complete when one foreground daemon exclusively owns the runtime,
ordinary CLI prompts execute only through that daemon, live output streams
without becoming durable token history, disconnect and restart never duplicate
agent execution, every authoritative final result comes from the durable
outbox, and legacy session and polling commands no longer exist.
