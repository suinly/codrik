# Persistent Actor Runtime Design

## Goal

Turn Codrik from a per-request, session-oriented agent into one continuously available runtime started with `codrik serve`. Gateway events, scheduled work, external completions, and autonomous recurring tasks all enter the same durable actor loop. A person has one shared memory across linked identities and channels; sessions and chats are not memory boundaries exposed to the user.

The runtime is continuously available but does not continuously call the model. It sleeps when no durable event or scheduled task is ready.

## Product model

An actor represents one person. Each actor may have multiple linked gateway identities, such as Telegram and a future local or web client. Conversation addresses remain delivery metadata, not memory ownership boundaries.

The user interacts naturally instead of creating or switching sessions. Replies return to the channel that produced the newest interactive input. Autonomous notifications go to the actor's preferred channel, falling back to the last active deliverable channel. Notifications are not duplicated to every linked channel.

The first release makes a clean break from existing sessions. It does not migrate, import, or automatically delete old session files. The user may remove them manually. Session commands and the session-oriented runtime path are removed when the new runtime becomes the supported path.

## Runtime architecture

`codrik serve` starts a supervisor that composes these independent components:

- `Gateway` accepts external input and converts it to a normalized `InboundEvent`.
- `IdentityResolver` authenticates a gateway identity and resolves it to an actor.
- `EventStore` persists inbound events, run state, tasks, leases, and outcomes in SQLite.
- `Scheduler` turns due one-shot and recurring tasks into ordinary durable events.
- `Dispatcher` finds actors with ready work and grants temporary actor leases.
- `ActorRunner` processes one actor's mailbox sequentially until it yields.
- `ContextBuilder` projects actor memory into bounded model context.
- `Outbox` persists intended replies and notifications before delivery.
- Gateway delivery workers deliver outbox records and retry temporary failures.

Application composition remains in `app.rs`. Gateway adapters do not own agent orchestration, memory selection, session state, or scheduling. Agent and memory modules remain independent of Telegram wire types.

The inbound path is:

```text
gateway webhook
    -> authenticate and resolve identity
    -> persist normalized event in SQLite
    -> return success to the gateway
    -> dispatch actor runner asynchronously
```

External HTTP requests never wait for an LLM or tool call.

## Durable storage

SQLite is the primary runtime store. It provides local deployment without a separate database while supporting transactions, indexes, migrations, leases, and crash recovery. Storage access remains behind focused traits so another implementation, such as PostgreSQL, can be added later without changing the loop.

The durable model contains at least:

- actors and linked identities;
- inbound events and their processing status;
- runs and resumable checkpoints;
- scheduled and recurring tasks;
- tool execution attempts and outcomes;
- actor memory projections and their provenance;
- outbox records and delivery attempts;
- actor leases.

Large attachments remain file-backed, referenced by durable records. SQLite stores ownership, metadata, hashes, and lifecycle state rather than large binary payloads.

## Event and run lifecycle

Events move through:

```text
pending -> processing -> completed | failed | cancelled
```

Runs move through:

```text
active -> waiting | completed | cancelled
```

Tasks move through:

```text
active -> due -> running -> active | paused | completed
```

Events have stable external and internal identifiers, actor ownership, origin metadata, payload, timestamps, and an idempotency key. Ready events are ordered by priority and then receipt time:

1. cancellation requests;
2. user messages;
3. external operation completions;
4. one-shot scheduled wake-ups;
5. recurring wake-ups;
6. maintenance events.

Short bursts of user messages are coalesced with a small debounce window, initially 300-500 milliseconds. Coalescing changes model input batching but never removes individual durable events.

## Dispatcher, leases, and actor runners

There is no permanent in-memory agent object per actor. The dispatcher selects an actor with ready events and grants an expiring lease containing an owner identifier and expiry time. Only the lease holder may execute that actor.

The temporary `ActorRunner`:

1. loads pending events and an unfinished run or creates a new run;
2. builds bounded context;
3. asks the model for the next response or actions;
4. executes actions one at a time;
5. checks for new events between safe steps;
6. commits checkpoints, memory changes, task changes, and outbox records;
7. continues, waits, completes, or cancels;
8. releases the lease.

The runner renews its lease while active. After a process failure, another runner may resume when the lease expires. A lease prevents concurrent decision streams for one actor; different actors may execute concurrently.

## Cooperative preemption

One actor has one sequential decision stream. A second user message does not start a competing agent run and does not blindly terminate an in-progress side effect.

When a new user message arrives during execution, the runtime persists it and sets `attention_requested`. The runner observes this at the nearest safe point, retains completed observations, rebuilds context with all pending user input, and continues the same work with the correction.

Safe points include:

- before and after a model request;
- between tool calls;
- after an external operation completes;
- before committing a final response.

Model requests and explicitly cancellable operations may stop promptly. A non-cancellable side-effecting operation is allowed to reach a known outcome before the runner incorporates the new input.

An explicit stop request creates `CancelRequested`. It prevents new actions, cancels cancellable work, and waits for any already-started non-cancellable operation to reach a recoverable boundary.

## Waiting and resumption

A run enters `waiting` when it needs a future time, an external webhook, a human decision, or another unavailable resource. Waiting consumes no active runner and performs no polling unless the task explicitly schedules a polling wake-up.

The run becomes ready again through a durable event. External operation callbacks must carry a correlation identifier and be deduplicated before they resume work.

## Autonomous tasks

The agent may independently create, update, pause, complete, and delete one-shot or recurring tasks when it judges future work useful. Explicit user approval is not required for each task.

A durable task records:

- actor ownership;
- purpose and execution instructions;
- trigger and next run time;
- completion condition;
- notification policy;
- budget policy;
- consecutive failure and no-change counts;
- status and creation reason.

The scheduler creates an ordinary event with a stable idempotency key when a task becomes due. A scheduled task never bypasses the actor mailbox.

Runtime guardrails apply regardless of the model's decision:

- a minimum recurrence interval;
- per-wake limits for model and tool calls;
- per-task execution and resource budgets;
- exponential backoff after temporary failures;
- automatic pause after repeated failures or repeated no-change results;
- limits on task creation from one wake-up;
- an operator-controlled global background-work switch.

The default notification policy is `meaningful_change`. Routine checks with no new result remain in the activity journal. The runtime notifies the user for a meaningful change, goal completion, required decision, or material failure.

Natural-language task control is primary. System commands such as `/tasks` and `/stop` may remain as reliable fallback controls.

## Actor memory and context construction

Memory belongs to the actor and is shared by every linked identity. It is separated into projections with different responsibilities:

- `EventLog`: immutable raw events, actions, and responses;
- `WorkingState`: checkpoint of unfinished goals, observations, and waits;
- `RecentConversation`: recent natural dialogue;
- `Episodes`: summaries of completed situations and tasks;
- `Knowledge`: stable facts, preferences, relationships, and agreements;
- `Tasks`: current autonomous commitments and waits.

Before a model call, `ContextBuilder` constructs:

```text
agent instructions
actor profile and stable knowledge
current working state
relevant memories
recent conversation
new events
```

Context construction is bounded; the complete history is not sent on every turn. After meaningful work, memory processing may extract knowledge, supersede outdated knowledge, create episode summaries, and compact the recent-context projection. Raw events are retained, so derived projections can be rebuilt.

Every durable knowledge record includes source event identifiers, confidence, creation time, last confirmation time, and an optional superseded-record reference. This provenance prevents summaries from becoming an untraceable source of truth.

## Gateways and identity linking

Telegram uses webhooks. At startup the gateway registers its configured public URL, authenticates incoming requests with Telegram's secret mechanism, and deduplicates update identifiers. A normalized inbound event contains the gateway, external identifier, gateway identity, conversation address, payload, and receipt time.

An authenticated actor can request a link code through an existing channel. The runtime creates a short-lived, single-use code bound to that actor. The user submits it from the new identity. The resolver atomically consumes the code and attaches the identity to the existing actor.

Only a hash of the code is stored. Codes have a short expiry and bounded attempts. Successful linking is confirmed through both channels. Linking cannot grant capabilities beyond those already assigned to the actor.

A future CLI client communicates with the running runtime through a loopback endpoint or Unix socket. It does not silently create a second runtime when `codrik serve` is unavailable.

## Reply routing and outbox

An interactive reply targets the origin conversation of the newest user input incorporated into the run. Input from another linked channel therefore redirects the next interactive reply to that channel.

Autonomous notifications target the actor's preferred channel and fall back to the last active deliverable channel. Delivery intent is committed to the outbox in the same transaction as the agent step that produced it.

Delivery workers retry temporary gateway failures. Provider message identifiers are persisted when available. Permanent delivery failures remain visible to later runs and operator diagnostics.

## Execution guarantees and tool policies

The system provides at-least-once processing. Exactly-once behavior cannot be guaranteed across SQLite, gateway APIs, and external tools, so every boundary is designed for deduplication or reconciliation.

Tools declare one execution policy:

- `read_only`: safe to retry;
- `idempotent`: repeatable with the same key;
- `reversible`: supports a compensating action;
- `side_effecting`: requires a pre-action checkpoint and cautious recovery.

Before a side effect, the runtime records an execution attempt. After completion, it records the outcome before taking another action. Confirmed outcomes are reused on retry. An unknown outcome after a crash is not repeated automatically; the runner reconciles external state or requests a decision.

Errors are classified as temporary, permanent, unknown-outcome, budget exhaustion, or corrupted actor state. Temporary errors use bounded backoff. Permanent failures become observations the agent can handle. Unknown outcomes require reconciliation. Budget exhaustion waits or pauses. Corrupted state isolates the affected actor without stopping other actors.

## Startup recovery

On startup the runtime:

1. makes work with expired leases eligible for dispatch;
2. resumes unfinished events from committed checkpoints;
3. creates events for overdue scheduled tasks according to their catch-up policy;
4. resumes outbox delivery;
5. reuses confirmed action outcomes instead of repeating them.

Recovery logic is safe to run more than once.

## Operations and observability

Events, runs, tasks, tool attempts, and outbox records have correlation identifiers. Structured logs describe lifecycle transitions without logging secrets or full private payloads by default.

The operator interface includes:

```text
codrik status
codrik tasks
codrik logs
codrik doctor
```

There is no legacy-session cleanup command. The sole current operator may remove legacy files manually.

## Testing

Implementation requires focused coverage for:

- event, run, and task state transitions;
- SQLite transactions, schema migrations, leases, and lease expiry;
- deduplication and stable idempotency keys;
- cooperative preemption at every safe boundary;
- cancellation during model calls and each tool policy;
- crash recovery around every persisted checkpoint;
- one-shot, recurring, overdue, paused, and backed-off schedules;
- autonomous-task guardrails and notification filtering;
- gateway authentication and contract normalization;
- Telegram webhook-to-outbox end-to-end behavior;
- outbox retry and permanent delivery failure;
- link-code expiry, reuse prevention, attempt limits, and cross-channel memory;
- context bounding, knowledge provenance, supersession, and projection rebuilding;
- concurrent execution across actors with strict serialization within one actor.

Tests use temporary SQLite databases and deterministic clocks. External gateways, LLM clients, and tools are exercised through traits and fakes. Recovery tests intentionally stop execution after selected commits and start a new runtime against the same database.

## Delivery sequence

Implementation is divided into four sequential projects, each receiving its own detailed plan:

1. SQLite event store, dispatcher, leases, and a minimal actor runner.
2. Scheduler, autonomous tasks, checkpoints, and cooperative preemption.
3. Telegram webhook, outbox delivery, and identity linking.
4. Actor memory, bounded context, episodes, summaries, and knowledge extraction.

The existing Telegram polling path may remain temporarily while the webhook and outbox vertical slice is developed. The release that adopts the new architecture exposes `codrik serve` as the primary runtime command and removes user-facing session behavior.

## Out of scope

- Migrating or automatically deleting existing session data;
- distributed execution in the first implementation;
- a PostgreSQL storage implementation;
- guaranteeing exactly-once external side effects;
- broadcasting one notification to every linked channel;
- continuously calling the model while no event is ready;
- exposing internal sessions or context windows as user-facing concepts.
