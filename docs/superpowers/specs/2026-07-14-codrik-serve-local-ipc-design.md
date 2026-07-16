# Codrik Serve, Local IPC, and Durable Delivery Design

> Superseded for actor bootstrap by
> `docs/superpowers/specs/2026-07-16-sqlite-actor-bootstrap-design.md`.
> `users.json` and legacy authorization import are no longer implemented.

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
- `codrik cancel <request-id>` for durable cancellation;
- best-effort text and activity streaming;
- durable final text, file, and terminal-error delivery;
- result-bundle claims, acknowledgements, retry, and recovery;
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
codrik cancel <request-id>
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

`codrik cancel <request-id>` creates a durable cancellation event for the
request's underlying work item. Cancellation applies to every active local
request assigned to that work item, including input not yet incorporated into
the run, and terminalizes their pending events. The command prints the affected
request IDs. Cancelling an already terminal request is an idempotent success
and does not create a control event.

## Configuration

`config.yml` gains a required runtime section:

```yaml
runtime:
  actor_id: actor:telegram:12312931
  database_path: ~/.codrik/runtime.sqlite
  socket_path: ~/.codrik/codrik.sock
  lock_path: ~/.codrik/runtime.lock
  artifact_path: ~/.codrik/artifacts
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
`actor:local:owner` with `tools: ["*"]` and writes the matching
`runtime.actor_id` when `users.json` is absent or contains no actors. It never
adds, selects, enables, or rewrites an actor when authorization already exists.
A manual installation with no actor fails readiness with an actionable example
instead of silently granting access from the daemon.

The wildcard authorizes only tools classified as `Standard`; privileged tools
such as the unrestricted shell remain excluded unless explicitly configured.

The Codrik directory is created with mode `0700`; the Unix socket is mode
`0600`. Linux and macOS are the supported local IPC platforms. All processes
running under the daemon's effective Unix UID are trusted local clients; other
UIDs are rejected. No additional bearer token exists in protocol v1.

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
7. Verify that the runtime directory, database parent, lock parent, artifact
   parent, and socket parent are owned by the effective UID, are not symlinks,
   and are not group- or world-writable.
8. Remove a stale socket path only while holding the instance lock.
9. Set a restrictive umask, bind the Unix listener, and set its mode to `0600`
   before accepting connections.
10. Recover expired leases, outbox claims, and interrupted attempts.
11. Start the stream hub, dispatcher, outbox worker, and IPC accept loop.
12. Emit a structured ready log entry.

A second daemon fails on the lock before touching the socket. Correctness does
not depend on the lock alone: existing actor and outbox fencing remains
authoritative if a process is misconfigured.

The lock is an OS-managed advisory lock, so process death releases it. The
socket file is removed only by the process that holds the lock.

Every accepted connection is checked with `SO_PEERCRED` on Linux or
`getpeereid` on macOS. A peer UID mismatch is rejected before parsing request
frames. Existing lock, database, socket, and artifact paths are opened without
following unexpected symlinks where the platform permits it; startup otherwise
fails closed with the unsafe path named in the error.

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
  bundle_id: UUID,
  delivery_ids: [UUID]
}

Cancel {
  request_id: UUID,
  cancel_id: UUID
}
```

The daemon emits:

```text
Accepted {
  request_id: UUID,
  work_item_id: UUID,
  sequence: Integer
}

CancelAccepted {
  request_id: UUID,
  cancel_id: UUID,
  affected_request_ids: [UUID]
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

FinalBegin {
  request_id: UUID,
  bundle_id: UUID,
  replay: Boolean,
  manifest: [FinalManifestEntry]
}

FinalChunk {
  request_id: UUID,
  bundle_id: UUID,
  delivery_id: UUID,
  chunk_index: Integer,
  bytes_base64: String
}

FinalEnd {
  request_id: UUID,
  bundle_id: UUID,
  manifest_sha256: String
}

RequestError {
  request_id: UUID,
  code: String,
  message: String
}

ProtocolError {
  code: String,
  message: String
}

ServerShuttingDown {
  request_id: UUID | null,
  resume_command: String | null
}
```

`RequestError` is reserved for failures that prevent a request from entering
or resolving durable work, such as a protocol violation, conflicting request
ID, missing request, or disabled actor. A failure after acceptance is a typed
durable error delivery inside the final bundle, not a transient `RequestError`.

`ProtocolError` is connection-scoped and is used only when no trustworthy
request ID can be decoded, such as malformed JSON or an unsupported envelope.
An oversized or incomplete frame may close the connection without a response.

`FinalManifestEntry` includes the delivery ID, payload kind, decoded byte
length, content SHA-256, and chunk count. Each typed text, managed-file
metadata, or terminal-error payload is canonical JSON split into chunks of at
most 192 KiB before base64 encoding. This guarantees every emitted frame fits
the 1 MiB envelope. A result bundle contains at most 1,024 deliveries and each
text payload contains at most 16 MiB of UTF-8. The complete canonical manifest
is at most 256 KiB and the sum of decoded canonical payload bytes in one bundle
is at most 16 MiB. Exceeding any limit produces a single small typed
terminal-error intent instead of committing an undeliverable result; validation
and replacement occur inside finalization.

That replacement transaction commits none of the proposed original intents or
bundle membership. It marks the run, work item, and all affected local requests
`failed_terminal`, then creates exactly one bounded error intent and bundle per
request. Artifacts staged only for the rejected result remain unreferenced for
GC; artifacts already referenced by an earlier tool checkpoint remain valid
history but are not added to the rejected delivery bundle.

`FinalBegin`, every manifest delivery chunk, and `FinalEnd` form one logical
bundle. The client verifies chunk counts, decoded lengths, hashes, and the
manifest hash before rendering the result as terminal or sending `AckFinal`.
EOF before `FinalEnd` leaves the local request nonterminal and safe to resume.
ACK names the bundle and the exact delivery IDs proven to have been received.
File payloads contain a managed artifact ID, immutable authorized local path,
display name, media type, size, content hash, and optional caption. The CLI
displays this metadata and never copies a file implicitly.

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
result_bundle_id   nullable unique foreign key
created_at
updated_at
```

The local-request row and inbound event are inserted in the same ingress
transaction. The request ID is the external ID under gateway namespace
`local:submit`. Repeating a request ID with the same prompt is idempotent and
returns the original event, work item, and sequence. Reusing it with different
text is a conflict and never creates another event.

The database enforces the lifecycle invariants transactionally:

- `active` implies `result_bundle_id IS NULL`;
- every terminal state implies a non-null bundle ID;
- every bundle has exactly one non-null owning request ID, matching the
  request's bundle ID;
- `delivery_count` equals a contiguous set of ordinals from zero;
- terminal request state, bundle, intents, and membership rows commit together.

`Cancel` uses `cancel_id` as the external ID under the separate gateway
namespace `local:cancel` for one durable `CancelRequested` control event.
Repeating the same cancel ID is idempotent and cannot collide with a submit ID.
The cancel ingress transaction sets `cancellation_requested_at` on the work
item and snapshots every affected request ID under the cancel ID. New submits
cannot attach to that work item after this marker and instead form a new work
item. When the runner checkpoints cancellation, exactly the snapshotted active
requests become `cancelled` and receive their own terminal result bundles, so
`CancelAccepted.affected_request_ids` cannot drift from the committed outcome.

Local IPC is trusted actor ingress, not gateway identity resolution. After
peer-UID authentication the adapter calls a dedicated
`ingest_for_actor(runtime.actor_id, event)` store operation. That operation
still verifies that the actor exists and is enabled, but does not fabricate a
Telegram identity or create an implicit identity link. Identity-resolving
`ingest` remains the boundary for future external gateways.

The IPC server registers each fully decoded `Submit` in an in-memory
`SubmissionRegistry` before starting its SQLite transaction and removes it
only after commit or rollback. `Resume` first joins a matching in-flight entry
and waits for its outcome, then reads `local_requests`. Consequently a dropped
connection before `Accepted` cannot race a false `missing_request` response.
After daemon restart SQLite transaction atomicity makes the request either
present or definitively absent. The client therefore stores no prompt: a
definitively absent request is safe to submit again as a new user action.

`Resume` resolves the request through the submission registry and
`local_requests`; it never creates work. It only registers a subscription and
wakes the outbox worker. An active request waits for live events and final
delivery. For a completed, cancelled, or failed request, the same outbox worker
replays its retained bundle through the normal bundle encoder. No IPC handler
directly emits durable final payloads.

Several local submissions may be incorporated into one interactive work item.
The runner publishes transient events to every attached active local request.
At finalization, each incorporated local request receives one delivery row for
every immutable outbox intent in its bundle. This prevents an older waiting
client from hanging when newer compatible input amends the run.

## Streaming semantics

The durable runner uses a streaming-capable model abstraction. Model text
deltas and activity events are published to `StreamHub`; they are never treated
as checkpoints and are not written to SQLite.

Each request subscription has a bounded queue. When the queue fills, the hub
drops transient text and activity events, sets an out-of-band gap flag, and
reserves capacity for one `StreamGap`. No further text deltas are sent to that
subscription after the gap. Activity may resume when capacity is available.
Hub publication is non-blocking, and subscriber backpressure or disconnect is
never returned to the model runner as an execution failure.

On an interactive terminal, the renderer ends the partial stream with a clear
gap marker and prints the authoritative final text from the beginning after
`FinalEnd`; it does not attempt to edit arbitrary prior terminal lines. For
non-TTY stdout, deltas are suppressed entirely and only the verified final is
written, so redirected output is deterministic.

Disconnecting drops the subscription, not the actor `RunContext`. A later
resume does not replay old deltas. It either joins the still-live stream or
returns the retained durable final.

The server registers the request subscription before durable submission. The
runner publishes by attached request ID, so it cannot emit for that request
before the subscription exists. A duplicate submit or resume replaces no
existing subscriber; multiple clients may observe the same request.

Protocol v1 permits one active `Submit`, `Resume`, or `Cancel` operation per
connection. The daemon accepts at most 64 concurrent IPC connections and one
subscription per connection, with a global 32 MiB queued-event budget. Each
subscription queue is limited to 256 events and 512 KiB. Reading the frame
header must complete within 5 seconds and its payload within 30 seconds;
socket writes and the final ACK each have a 30-second deadline. Limit
violations return `server_busy` when a request ID is available, then close the
connection. These limits apply even though same-UID peers are trusted, because
bugs and abandoned local clients must not exhaust daemon resources.

At most four final-bundle transmissions run concurrently, bounding daemon-side
decoded payload memory to 64 MiB plus manifests and frame buffers. Encoding,
hashing, and verification are incremental. A CLI holds at most one 16 MiB
bundle and never persists the response after the command exits.

## Durable output and delivery rows

The existing outbox intent remains the immutable logical result. In v2,
`outbox_deliveries` are immutable bundle membership rows, while the owning
result bundle is the atomic local delivery target:

```text
id
outbox_id
bundle_id           foreign key
ordinal             integer
transport          local_ipc
address            request UUID
created_at
unique(outbox_id, transport, address)
unique(bundle_id, ordinal)
```

Each terminal local request owns one immutable result-bundle row containing
its ID, request ID, delivery count, canonical manifest hash, delivery state,
attempt count, claim owner and expiry, last error, and timestamps. Bundle
delivery state is `pending | delivering | delivered | failed_retryable |
failed_terminal`. The bundle and every membership ordinal are created in the
finalization transaction. The manifest is therefore complete before the bundle
is claimable, regardless of delivery count.

Before a successful tool outcome containing a file can be checkpointed, the
runtime first inserts an `artifacts` row in `staging` with an expiring staging
lease. It then copies the authorized regular-file bytes into actor-owned
managed storage with restrictive permissions, computes SHA-256, fsyncs, and
atomically renames to a content-addressed immutable path before fsyncing the
directory. Symlinks are never retained. The tool checkpoint transaction
verifies the staging lease, records the managed metadata, changes the artifact
to `referenced`, and stores the reference. A committed reference therefore
never precedes durable file contents.

GC claims only expired `staging` rows, rechecks their state and lease in SQLite
before unlinking, and cannot race an active stager. It also removes orphan
files that are older than one hour and still have no database row after a
second check. Stagers renew their lease during long copies. One artifact is
limited to 256 MiB and all retained artifacts for one actor to 2 GiB; exceeding
a quota is a known tool error before checkpoint. Referenced artifacts are
retained with their result bundles.

Finalization atomically creates the outbox intents, expands their reply routes
into one delivery row per incorporated local request, completes only
incorporated events, creates one complete bundle per incorporated request, and
completes the run and work item. Text and managed file artifacts are both
converted into typed intents. No runner calls a CLI or gateway sink for final
output.

The outbox worker considers a bundle claimable only when at least one
subscriber exists for its request ID. It atomically claims the whole bundle,
loads its complete immutable manifest, and sends one
`FinalBegin`/`FinalChunk`/`FinalEnd` sequence. Membership rows are never claimed
separately. The worker snapshots recipients before `FinalBegin`; a subscriber
joining mid-transmission waits for a separate full replay from the beginning.
Multiple snapshotted subscribers may receive the same sequence, and the first
valid ACK proves local receipt under the same-UID trust model.

That first ACK changes only durable bundle state. Every per-connection send
already started from the recipient snapshot continues through `FinalEnd`, or
ends with an explicit connection error that causes that client to resume. ACK
never truncates another subscriber's in-flight byte stream.

The first slice uses one outbox worker, claims at most 32 bundles per
transaction, and uses a 30-second bundle claim and ACK deadline. It does not
increment an attempt merely because no subscriber exists.

From claim through the last `FinalChunk` write and ACK wait, the worker renews
the bundle lease every 10 seconds. If renewal fails or ownership is fenced, it
stops the affected transmissions with an explicit connection error; clients
resume, and any already received complete bundle may still produce a valid
stale ACK under the rules below.

If every subscribed connection closes before ACK, the claim is released to
`failed_retryable`; an expired local claim is safe to retry. A client may have
displayed the result before losing its ACK, so local delivery is explicitly
at-least-once. Delivered payloads are retained and remain replayable through
`Resume` without changing bundle delivery state.

The logical outbox intent has no aggregate delivery state. Each request bundle
is one independent target, so one disconnected request cannot block or
overwrite another request's acknowledgement.

ACK handling is one SQLite transaction. Its delivery-ID set must exactly equal
the bundle manifest; partial or additional IDs reject the entire ACK. Every
membership row must match `bundle_id`, `transport = local_ipc`, and
`address = request_id`. A valid ACK moves a bundle from `delivering` or
`failed_retryable` to `delivered`; an already delivered bundle is an idempotent
no-op. `pending` cannot be ACKed. A stale ACK after claim expiry is accepted
from `failed_retryable` as proof of receipt. Any mismatch fails the whole ACK
without updating state.

The normative delivery transitions are:

| Actor | Precondition | Transition |
| --- | --- | --- |
| Worker claim | bundle is `pending` or due `failed_retryable`; subscriber exists | `delivering`, increment `attempt_count`, set owner and expiry |
| Send/encode failure | worker owns unexpired claim | `failed_retryable`, clear claim, record error |
| All subscribers disconnect | worker owns claim | `failed_retryable`, clear claim |
| Claim recovery | `delivering` and expired | `failed_retryable`, clear claim |
| Valid ACK | exact matching manifest and route; bundle is delivering, retryable, or delivered | `delivered`, clear claim |
| Invalid durable payload | any nonterminal state | `failed_terminal`, record diagnostic |
| Read-only replay | `delivered` | no state change |

`attempt_count` increases only on a successful claim. Transient delivery retry
delays are 1, 2, 4, 8, then at most 30 seconds; attempt count alone never makes
a durable result unavailable. Malformed immutable payloads never retry.

Migration v1 to v2 runs in one SQLite transaction and updates `user_version`
last. Every v1 outbox row, including unmanaged file paths, is copied with its
IDs, payload, state, claim, attempts, and error metadata into the
schema-versioned operator-visible `legacy_outbox_archive`; no v1 row is inserted
into the v2 intent or delivery tables and no route is guessed.

V1 has no `local_requests`, so incomplete v1 work cannot safely produce a v2
bundle. The migration snapshots every nonterminal work item, run, event, and
tool attempt into `legacy_runtime_quarantine`, changes its active runtime rows
to their existing terminal or blocked states, clears leases, and never
dispatches them automatically. Specifically, nonterminal work items and active
runs become `failed_terminal`, pending or processing events become
`failed_terminal`, prepared tool attempts become `cancelled_known`, and running
tool attempts become `outcome_unknown` before their work is terminalized.
Completed v1 history remains readable. This is an internal runtime migration,
not migration of legacy session files.

The migration verifies source/archive counts and foreign keys before commit;
rollback restores the complete v1 schema after interruption. Startup logs the
quarantine counts and path to the database for manual inspection.

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
consecutive failed quantum moves the work item and every incorporated local
request to `failed_terminal` and atomically creates a typed durable error
outbox intent and result bundle for each such request. Attached events not yet
incorporated return to `ready` and may form a new work item; they are not
silently failed with work they never influenced. The client receives the error
through the final bundle protocol. A successful checkpoint resets
the counter only when it records new model output, a known tool outcome, or
finalization after the most recent failure. Replaying an already incorporated
source-event checkpoint does not reset failure history; repeated failure of
the same model step therefore reaches the terminal limit.

SQLite busy errors receive a small bounded transaction retry before they reach
the dispatcher. A database I/O failure, corruption signal, or unsupported
schema version is process-critical: the supervisor shuts down for service
manager restart because it cannot truthfully persist a per-work-item failure
while the authority store is unavailable.

Malformed durable inbound or checkpoint payloads mark their work item
`blocked` immediately; malformed outbox payloads follow the delivery table and
become `failed_terminal`. Both are reported in structured diagnostics and
never form a busy loop. Tool execution errors
with known outcomes remain model observations and do not count as dispatcher
failures. An orphaned `Running` tool becomes `outcome_unknown` and moves the
work item to `waiting_for_decision`, as in the kernel protocol.

## Shutdown and recovery

SIGINT and SIGTERM initiate graceful shutdown:

1. stop accepting new IPC connections;
2. send `ServerShuttingDown` to connected clients, including the request ID and
   resume command when known;
3. stop acquiring new actor and delivery leases;
4. allow active quanta, delivery transmissions, and ACK waits up to 30 seconds;
5. cancel unfinished model futures without finalizing them or incrementing
   their persisted failure counts;
6. release actor leases whose last durable checkpoint is unambiguous and move
   sent-but-unacknowledged result bundles to `failed_retryable`; leave an
   already-running tool attempt for unknown-outcome recovery;
7. close the listener and remove the socket while still holding the lock;
8. release the instance lock and exit.

If the grace period expires, the process exits without inventing outcomes. A
tool already committed as `Running` is recovered as `outcome_unknown`. Active
runs and incorporated checkpoints remain resumable under a new fence.

Startup recovery:

- reacquires actors only after persisted leases expire;
- resumes existing active runs rather than creating replacements;
- changes orphaned `Running` attempts to `outcome_unknown`;
- releases expired local bundle claims to retryable state;
- preserves completed outbox intents and deliveries without duplication.

An unexpected EOF is rendered like shutdown: the CLI keeps local metadata
nonterminal and prints `codrik resume <request-id>`. Cleanly released leases
are immediately claimable after restart; ambiguous external effects retain the
existing conservative recovery rules.

SQLite makes state transitions exactly-once, but an external model call cannot
participate in that transaction. A crash after the provider accepts a request
and before Codrik checkpoints its response may repeat that model call, billing,
and generation after restart. The runtime guarantees that incorporated events,
known tool outcomes, immutable final intents, bundles, and ACK transitions are
not duplicated; it does not claim exactly-once model execution.

## CLI rendering and local request files

Before connecting, the CLI writes request metadata atomically under:

```text
~/.codrik/client/requests/<request-id>.json
```

The file contains the request ID, creation time, prompt hash, and local state
`created | sent_unconfirmed | accepted | terminal`, but not the prompt text or
response payload. State changes use atomic replacement. The directory is mode
`0700`; files are mode `0600`. After an ambiguous disconnect the recovery
command first resumes the same ID; `missing_request` is definitive only after
the daemon's submission registry has resolved or after a daemon restart.

On a TTY the CLI renders activity with the existing spinner and writes text
deltas as they arrive. `StreamGap` ends delta rendering for that subscription.
The CLI buffers and verifies the final bundle, prints authoritative text and
file metadata only after `FinalEnd`, sends the bundle-scoped `AckFinal`, and
then marks the local request metadata terminal. On non-TTY stdout it emits only
the verified authoritative result. A daemon-unavailable error names the
expected socket and instructs the user to start `codrik serve`.

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
- chunked bundle manifests, hashes, incomplete bundles, and oversized results;
- request-ID idempotency and conflicting prompt reuse;
- submit/resume races through the in-flight submission registry;
- bounded subscription queues and single-gap behavior;
- connection, queue-byte, read, write, and ACK resource limits;
- peer-UID checks, unsafe parents, instance locking, and stale-socket ownership;
- managed artifact staging, immutable hashes, crash leftovers, and GC;
- request-to-delivery route expansion;
- bundle claim, ACK, disconnect, retry, and expiration transitions;
- bundle/request/route-scoped ACK rejection and stale ACK acceptance;
- transactional v1 archive migration for every old outbox state;
- quarantine of incomplete v1 work and unmanaged file intents;
- late-subscriber replay and non-truncating multi-subscriber ACK behavior;
- persisted failure backoff and terminal error creation;
- work-item-wide cancellation and multi-request terminalization;
- CLI parsing with all legacy variants rejected.

End-to-end tests use a real Unix socket, an on-disk SQLite database, a scripted
streaming model, and deterministic clocks:

1. Submit emits accepted, deltas, a verified final bundle, and ACKs delivery.
2. Duplicate submit creates no second event, run, intent, or delivery.
3. A request ID with different text is rejected.
4. Disconnect during streaming does not cancel durable work.
5. Resume joins a live run or replays its completed result.
6. Multiple incorporated requests all receive final delivery rows.
7. Restart after ingress, attachment, model output checkpoint, and finalization
   preserves durable state; an uncheckpointed model call may repeat.
8. Crash after `FinalEnd` but before ACK may redeliver without recomputation.
9. A second daemon cannot acquire the lock or remove the live socket.
10. SIGTERM stops ingress and preserves resumable active state.
11. Orphaned running tools are never automatically invoked again.
12. Five consecutive runtime failures produce a terminal request delivery.
13. Missing or disabled configured actors prevent readiness.
14. Ctrl-C before `Accepted` followed by resume never races a duplicate submit.
15. More than 32 deliveries and text larger than one frame replay as one bundle.
16. Cancel terminalizes every active request on the affected work item.
17. A slow or malformed same-UID client cannot exhaust daemon resources.

Repository verification requires `cargo fmt --check`, `cargo test`,
`cargo check`, `cargo clippy --all-targets --all-features`, and a manual
foreground transcript covering submit, Ctrl-C, and resume.

## Acceptance criteria

The slice is complete when one foreground daemon exclusively owns the runtime,
ordinary CLI prompts execute only through that daemon, live output streams
without becoming durable token history, disconnect and restart preserve
exactly-once durable incorporation and known external outcomes while permitting
an ambiguous model call to repeat, every authoritative final result comes from
the durable outbox bundle protocol, and legacy session and polling commands no
longer exist.
