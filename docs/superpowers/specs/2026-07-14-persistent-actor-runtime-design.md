# Persistent Actor Runtime Design

## Goal

Turn Codrik from a per-request, session-oriented agent into one continuously available runtime started with `codrik serve`. Gateway events, scheduled work, external completions, and autonomous recurring tasks enter the same durable actor loop. A person has shared memory across linked private identities and channels; sessions and chats are not user-facing memory boundaries.

The runtime is continuously available but does not continuously call the model. It sleeps when no durable event or scheduled task is ready.

## Product model

An actor represents one person. Each actor may have multiple linked gateway identities, such as Telegram and a future local or web client. Conversation addresses remain delivery and disclosure metadata, not separate user-managed sessions.

Interactive replies return to the private channel or conversation that produced the newest incorporated user input. Autonomous notifications go to the actor's preferred verified private channel, falling back to the last active deliverable private channel. Notifications are not broadcast to all linked channels.

The release makes a clean break from existing conversation sessions. It does not migrate, import, or automatically delete old session files. The operator may remove them manually. Session commands and the session-oriented runtime path are removed when the new runtime passes its release gates.

## Runtime architecture

`codrik serve` starts a supervisor that composes these independent components:

- `Gateway` accepts external input and converts it to a normalized `InboundEvent`.
- `IdentityResolver` authenticates a gateway identity and resolves it to an actor.
- focused repositories read and write actors, events, work items, runs, tasks, occurrences, attempts, memory, leases, and outbox records;
- a shared SQLite unit of work owns transactions spanning those repositories;
- `Scheduler` turns due one-shot and recurring task occurrences into ordinary durable events;
- `Dispatcher` finds actors with ready work and grants fenced actor leases;
- `ActorRunner` processes one actor's mailbox sequentially until it yields;
- `ContextBuilder` projects audience-safe actor memory into bounded model context;
- gateway delivery workers deliver durable outbox records and retry known temporary failures.

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

## Deployment model and source of truth

SQLite is the sole authority for actors, identities, capability assignments, link state, events, runs, tasks, attempts, memory projections, leases, and outbox state.

The first release supports one `codrik serve` process per database and enforces this with an instance lock. Correctness does not depend only on that lock: actor writes are fenced so a stale task within a process or a misconfigured second process cannot overwrite a newer owner. Multi-process operation is out of scope, but the storage protocol must fail safely if it occurs.

Existing `users.json` is imported once into the initial SQLite migration because it contains authorization rather than conversation history. Every existing actor remains separate; the current sole enabled Telegram actor becomes the initial owner. After a successful import, SQLite is authoritative and `users.json` remains untouched but ignored. Existing session directories are neither imported nor deleted.

Only the process holding the instance lock may register or reconcile gateway webhooks. A configured webhook URL or secret mismatch is a visible startup error unless explicit reconciliation is enabled.

## Correctness invariants

The following invariants are normative:

1. A gateway external event key is unique within its gateway instance.
2. A `(provider, subject)` identity belongs to at most one actor.
3. Every actor event has a unique, database-assigned, monotonically increasing `mailbox_sequence`.
4. An actor has at most one executing decision step, but may have multiple durable work items that are waiting or ready independently.
5. An actor has at most one current lease row. Each successful acquisition increments a monotonic `lease_generation` fencing token.
6. Every actor-state mutation from a runner is conditional on its `lease_generation`. A conditional write must affect exactly one current lease or the runner is stale and must stop.
7. A scheduled occurrence is unique by `(task_id, task_revision, scheduled_for)`.
8. An outbox intent key is unique, even when its delivery outcome is unknown.
9. Referential integrity and foreign keys are enabled for actors, events, runs, tasks, attempts, memory sources, attachments, and outbox records.
10. Runtime-owned sequence numbers, generations, budgets, counters, and task lineage cannot be modified by model output.

If a runner loses its fence while an external action is in flight, it may not commit ordinary success or retry the action. The attempt becomes an unknown outcome unless the tool can prove its result through an outcome probe.

## Durable transaction protocol

Correctness-critical operations use the following transaction boundaries.

### Ingress transaction

Authenticate and resolve the identity, insert or deduplicate the external event, allocate its actor mailbox sequence, persist its audience and origin, and commit. The gateway receives success only after this commit. If durable persistence fails, the gateway receives a retryable non-success response.

### Actor acquisition transaction

Select an actor with ready work using a deterministic fairness order, acquire or renew its lease, increment `lease_generation` on new ownership, and record the worker owner and expiry. Acquisition never marks events completed.

### Event attachment transaction

Under the lease fence, select one ready work item and a bounded, audience-compatible event batch. A batch may combine verified private-channel events carrying `actor_private` scope, but it never combines a group or other `conversation_scoped` event with a different audience. Events excluded by the selected work item or target audience remain pending and unconsumed.

Attach each selected event to one run for that work item, record the run's observed mailbox high-water mark, and move only those events from `pending` to `processing`. Replaying this transaction returns the same attachment rather than incorporating an event twice. Lease expiry preserves the event-to-run attachment for resumption; detaching and returning an event to `pending` requires an explicit aborted-run transaction that proves no ambiguous side effect depends on it.

### Checkpoint transaction

Under the lease fence, atomically persist the run state, consumed mailbox high-water mark, completed tool outcomes, task mutations, memory projection changes, and any outbox intents produced by the step. An external tool attempt is persisted before the call and its outcome is persisted in a separate fenced checkpoint immediately after the call.

### Finalization transaction

Under the lease fence, verify that no newer compatible cancellation or interactive event exists beyond the run's observed high-water mark. If one exists, finalization aborts and the runner attaches new input. Otherwise, atomically mark only the source events actually incorporated by this decision step terminal, mark or wait the work item, complete the run, persist final memory changes, and create uniquely keyed outbox intents. Pending events for another work item or audience are unaffected.

### Scheduler transaction

Lock the task revision, insert or deduplicate the immutable scheduled occurrence and corresponding actor event, advance `next_run_at`, update persisted budget counters, and commit together. Cancellation or revision changes serialize through the same task row.

### Outbox claim transaction

Claim a bounded delivery batch with an expiring delivery lease and persisted attempt counter. Delivery completion, known failure, or unknown outcome is recorded in a separate transaction. An expired delivery claim may be retried only when the gateway result is known retry-safe.

## State machines

Work items represent independent conversational activities, autonomous tasks, or external-operation continuations. An actor may have several waiting work items, but its fenced runner executes only one at a time. An unassigned private user event joins the current compatible interactive work item or creates one; a task occurrence and external callback carry their work item correlation explicitly. Group and other scoped conversations use distinct work items.

Event states:

```text
pending -> processing -> completed | cancelled | failed_terminal
processing -> pending              # explicit aborted-run transaction only
processing -> blocked              # poison data or unknown dependent outcome
blocked -> pending | cancelled     # explicit operator/agent resolution
```

Work-item states:

```text
ready -> waiting | completed | cancelled | failed_terminal
waiting -> ready                   # durable compatible resume event attached
ready -> blocked_unknown_outcome | waiting_for_decision
blocked_unknown_outcome -> ready | waiting_for_decision | cancelled
waiting_for_decision -> ready | cancelled
```

Runs are bounded executions of one work item. They move through `active -> completed | cancelled | failed_terminal`; a crash leaves an active run resumable under its preserved event attachments and fenced checkpoints.

Task-definition states:

```text
active -> paused | completed | cancelled
paused -> active | cancelled
```

Task-occurrence states:

```text
pending -> claimed -> running -> completed | failed | skipped | cancelled
claimed -> pending                 # expired claim before execution
pending -> cancelled               # task cancelled or superseded before attachment
```

Tool-attempt states:

```text
prepared -> running -> succeeded | failed_known | outcome_unknown | cancelled_known
outcome_unknown -> succeeded | failed_known | waiting_for_decision
```

Outbox states:

```text
pending -> delivering -> delivered | failed_retryable | failed_terminal | outcome_unknown
failed_retryable -> pending
outcome_unknown -> pending | delivered | failed_terminal | acknowledged_duplicate
```

Every transition records its initiator, timestamp, attempt number, and reason. Poison events and terminal delivery failures remain inspectable and do not block unrelated actors.

## Mailbox ordering, fairness, and cooperative preemption

One actor has one sequential executor across potentially many work items. A second user message does not start a competing runner and does not blindly terminate an in-progress side effect. A compatible message may amend the executing interactive work item; an incompatible audience or unrelated task remains a separate ready work item.

Ready events are prioritized as follows:

1. cancellation requests;
2. user messages;
3. external operation completions;
4. one-shot scheduled wake-ups;
5. recurring wake-ups;
6. maintenance events.

Priority is not absolute starvation. Each lease has a bounded quantum: a configurable maximum number of attached events, model steps, tool steps, and wall-clock duration. The runner releases the actor after the quantum unless it must immediately checkpoint an in-flight outcome. Dispatcher ordering includes priority aging so recurring work eventually runs. Cancellation always bypasses batching.

Short bursts of audience-compatible user messages use a fixed 400 millisecond debounce from the first message, not a sliding timer. New compatible messages inside that window join the batch; later messages receive higher mailbox sequences and trigger preemption. Messages from incompatible audiences never coalesce.

Correctness uses mailbox sequences rather than a Boolean flag. At each checkpoint the runner records the highest observed sequence. Before a model call, between tool calls, after an external operation, and before finalization it checks for newer events. It retains completed observations, attaches new input, and rebuilds context.

Model requests and explicitly cancellable operations may stop promptly. A non-cancellable side-effecting operation reaches a known or unknown outcome before the runner incorporates new input. An explicit `CancelRequested` prevents new actions and cancels only work whose contract supports known cancellation.

## Waiting and resumption

A run enters `waiting` when it needs a future time, an authenticated external callback, a human decision, or another unavailable resource. Waiting consumes no runner and performs no polling unless a task explicitly schedules a polling occurrence.

A durable resume event transitions the run back to `active`. External callbacks require provider-specific authentication in addition to a correlation identifier and are transactionally deduplicated.

## Autonomous tasks and scheduling semantics

The agent may independently create, update, pause, complete, and cancel one-shot or recurring tasks when it judges future work useful. Explicit user approval is not required for each task, but task creation never expands the actor's capabilities or bypasses approval policy for privileged actions.

A durable task definition records:

- actor ownership, creator event IDs, and parent task lineage;
- delegated capability scope, purpose, and execution instructions;
- trigger, task revision, IANA timezone, and next run time in UTC;
- completion condition, overlap policy, and catch-up policy;
- notification policy and immutable runtime budget policy;
- consecutive failure and no-change counts;
- status and creation reason.

Each occurrence is a separate durable record and uses `(task_id, task_revision, scheduled_for)` as its idempotency identity. Calendar schedules are interpreted in the stored IANA timezone. On daylight-saving gaps, the occurrence runs at the next valid local instant; on overlaps, it runs once at the earlier instant unless the task explicitly requests both.

Before attaching an occurrence event, the transaction revalidates the task's current status, revision, overlap policy, and budget. Updating or cancelling a task cancels all still-pending occurrences and mailbox events from superseded revisions. An already-running occurrence follows cooperative cancellation and tool outcome rules. Because definition and occurrence states are separate, a recurring definition remains active while one occurrence runs or while bounded catch-up occurrences wait.

The default overlap policy is `skip_while_running`. The default catch-up policy is `latest_once`: after downtime, create one occurrence for the latest missed time. Optional policies are `skip` and `all_bounded`; `all_bounded` is capped by the task's persisted maximum backlog. Updating a schedule increments its revision and prevents old revisions from producing new occurrences.

Runtime guardrails include:

- minimum recurrence interval;
- per-wake model, tool, time, and resource limits;
- persisted rolling per-task, per-lineage, per-actor, and global budgets;
- maximum active tasks, lineage depth, and task creation rate;
- notification rate limits;
- exponential backoff after temporary failures;
- automatic pause after repeated failures or repeated no-change fingerprints;
- actor and global circuit breakers controlled outside the model.

`meaningful_change` is evaluated from typed task outcomes and stable result fingerprints where possible. If only model judgment is available, the decision and evidence are stored, while notification rate limits remain authoritative.

Routine checks with no change remain in the journal. The runtime notifies the user for meaningful change, goal completion, a required decision, or material failure. Natural-language task control is primary; `/tasks` and `/stop` may remain reliable fallback controls.

## Tool execution contract

Tool capabilities are orthogonal rather than represented by one broad category. Each tool declares:

- whether retry is safe;
- whether it accepts a stable idempotency or attempt key;
- whether it supports known cancellation;
- whether it supports an outcome probe;
- whether it supports compensation and the limits of that compensation;
- whether it requires an explicit actor approval policy.

The runtime passes a durable attempt ID and authorized capability scope into every tool call. External content returned by tools is untrusted data; it cannot authorize additional tools, recurring tasks, linking, or capability changes. Policy enforcement occurs outside model reasoning.

Before a side effect, the runtime persists `prepared` with arguments or an integrity hash, capability scope, and attempt ID. Immediately before crossing the external invocation boundary it commits `prepared -> running`. Recovery may start an attempt still proven `prepared`; any `running` attempt without a durable outcome is `outcome_unknown` until an outcome probe resolves it or a decision is obtained. A confirmed outcome is reused. A known retry-safe failure may retry within budget. If neither idempotency nor an outcome probe exists after an ambiguous failure, the work item enters `waiting_for_decision`; automatic retry is prohibited. Compensation is a new fallible attempt and never retroactively makes the original call exactly once.

## Actor memory, audiences, and context construction

Memory belongs to the actor, but disclosure is constrained by audience provenance. Every event, attachment, memory record, tool result, and generated output carries one of:

- `actor_private`: usable across verified one-to-one channels belonging to the actor;
- `conversation_scoped(address)`: usable only in that conversation audience;
- `shareable`: explicitly safe to disclose more broadly.

Private one-to-one channels linked to the actor share `actor_private` memory. Group messages default to `conversation_scoped` and are not promoted into actor-global knowledge unless the actor explicitly asks to remember them. When responding into a group, `ContextBuilder` excludes `actor_private` and other conversations' data rather than relying on the model to keep secrets. Autonomous work may use actor-private memory, but its output defaults to a verified private notification target unless an explicit task scope authorizes a conversation.

Memory projections have distinct responsibilities:

- `EventLog`: raw durable events, actions, and responses;
- `WorkingState`: checkpoint of unfinished goals, observations, and waits;
- `RecentConversation`: recent audience-compatible dialogue;
- `Episodes`: summaries of completed situations and tasks;
- `Knowledge`: stable facts, preferences, relationships, and agreements;
- `Tasks`: current autonomous commitments and waits.

Before a model call, `ContextBuilder` constructs:

```text
agent instructions and trust policy
target audience and authorized capability scope
audience-safe actor profile and knowledge
current working state filtered for the target
relevant audience-safe memories
recent compatible conversation
new events with untrusted-data labels
```

Context is bounded; complete history is not sent on every turn. Memory processing may extract knowledge, supersede outdated knowledge, create episodes, and compact recent context. Derived records inherit the most restrictive audience of their sources unless an explicit actor action marks material shareable.

Every knowledge record includes source event IDs, audience, confidence, creation time, last confirmation time, and an optional superseded-record reference. Raw events are retained by default but remain deletable under the retention policy.

## Retention and deletion

New actor history is retained indefinitely by default for the personal-runtime use case. Retention is configurable by data class for raw events, tool payloads, outbox payloads, logs, and attachments.

Deleting source data invalidates or deletes dependent projections and search indexes before context can use them again. Attachments use actor ownership and reference counting; unreferenced files are garbage-collected after a safety delay. Identity revocation immediately prevents new access while retaining an audit record that contains no reusable credential.

An actor-data deletion operation removes events, projections, task payloads, outbox payloads, and attachments according to the requested scope. Backups, if enabled, must document their separate expiry. Operational logs redact private payloads by default.

## Gateways, callbacks, and identity linking

Telegram uses webhooks. The gateway validates Telegram's secret, enforces request body and attachment limits, rate-limits abusive sources, and transactionally deduplicates update IDs. Secret rotation supports an overlap window with explicit old-secret expiry. Provider callback endpoints require provider-specific signatures or credentials; a correlation ID alone never authenticates a callback.

An authenticated actor requests a link code through an existing verified private channel. The runtime stores an HMAC/keyed digest, intended provider, expiry, and bounded global and per-source attempt counters. Submitting the code from a new identity creates a pending link but grants no memory or tool access. The existing channel must confirm the exact provider and new identity before atomic activation. The actor can list and revoke linked identities through an audited path.

Linking cannot grant capabilities beyond those assigned to the actor. Untrusted content and autonomous tasks cannot approve a pending identity.

A future CLI client communicates with the runtime through an authenticated loopback endpoint or Unix socket. It does not silently create a second runtime when `codrik serve` is unavailable.

## Reply routing and outbox delivery

Every outbox intent has an immutable class and source event set:

- `interactive_reply` targets the audience of the newest incorporated interactive mailbox sequence;
- `background_notification` targets the task's authorized conversation or the actor's preferred verified private channel;
- `required_decision` targets a verified private channel unless the originating conversation is explicitly authorized.

If user input arrives during background work, the existing background result retains its notification class while the user-facing continuation becomes a separate interactive reply. A finalization transaction determines the newest source sequence and prevents stale routing. Preferred and last-active routes update only after authenticated inbound activity or confirmed delivery; revoked or repeatedly failing routes become ineligible.

Delivery intent is committed in the same transaction as the step that produced it. Delivery semantics are gateway-specific. Telegram uses at-least-once retry policy: if `sendMessage` may have succeeded but the process failed before recording its message ID, the attempt becomes `outcome_unknown`, is retried after bounded backoff, and may produce a visible duplicate. The authorized `outcome_unknown -> pending` transition records that duplicate risk. Gateways with a trustworthy lookup or idempotency facility reconcile instead. The runtime never claims exactly-once delivery.

Permanent delivery failures and unknown outcomes remain visible to later runs and operator diagnostics. Retry limits prevent one message from blocking other outbox records.

## Clocks and ordering

SQLite-assigned mailbox sequences provide per-actor total order; timestamps never decide ties. Persisted schedules and deadlines use UTC wall-clock instants, with an injected clock in tests. Calendar interpretation uses the task's IANA timezone.

Lease expiry uses short persisted UTC deadlines plus fencing generations. Clock jumps may delay or accelerate reacquisition but cannot authorize stale commits because every write checks the fence. Runner heartbeats renew well before expiry. Backoff and fairness calculations use deterministic persisted instants and stable sequence tie-breakers.

## Startup recovery and operator repair

On startup the runtime:

1. acquires the single-instance database lock;
2. validates schema and gateway ownership;
3. makes expired leases and known-safe claims eligible for dispatch;
4. leaves ambiguous tool attempts blocked, while gateway-specific policy either reconciles delivery or requeues it with recorded duplicate risk;
5. creates bounded catch-up occurrences according to task policy;
6. resumes known-safe outbox delivery;
7. reuses confirmed outcomes instead of repeating actions.

Recovery is idempotent. Corrupt actor state, poison events, terminal outbox failures, and unknown outcomes isolate only their affected work. Operator repair actions require an explicit actor/event identifier, record an audit reason, and may retry only when the underlying contract permits it.

The operator interface includes:

```text
codrik status
codrik tasks
codrik logs
codrik doctor
```

Commands default to redacted summaries, require actor filters when multiple actors exist, and paginate results. There is no legacy-session cleanup command.

## Testing

Implementation requires focused coverage for:

- schema constraints, migrations, foreign keys, and one-time auth import;
- fenced leases, stale-runner commits, lease expiry, and clock jumps;
- ingress, attachment, checkpoint, finalization, scheduler, and outbox transactions;
- mailbox sequencing, high-water checks, lost-wakeup prevention, and stale-reply prevention;
- bounded debounce, runner quantum, priority aging, and actor fairness;
- complete event, work-item, run, task-definition, occurrence, attempt, lease, and outbox transitions;
- audience-compatible attachment, independent waiting work items, and source-only event completion;
- the committed `prepared -> running` boundary and recovery on both sides of invocation;
- every tool capability combination and unknown-outcome recovery;
- one-shot, recurring, DST, overlap, catch-up, revision, and cancellation scheduling;
- persisted task lineage, aggregate budgets, circuit breakers, and notification filtering;
- audience isolation between linked private channels, groups, and unrelated conversations;
- prompt injection through web pages, files, tool output, and callbacks;
- gateway authentication, body limits, replay defense, and secret rotation;
- link expiry, brute-force limits, pending access denial, confirmation, and revocation;
- Telegram webhook-to-outbox behavior, including ambiguous delivery and duplicates;
- retention, projection invalidation, attachment garbage collection, and identity revocation;
- context bounding, provenance, supersession, and projection rebuilding;
- crash injection before and after every correctness-critical transaction.

Tests use temporary SQLite databases, deterministic clocks, and controllable executors. External gateways, LLM clients, and tools are exercised through traits and protocol fakes. Recovery tests stop execution at selected boundaries and start a new runtime against the same database.

## Delivery sequence and release gates

Implementation is divided into four vertical projects, each receiving its own detailed plan.

### 1. Durable local kernel

Build the SQLite schema and migrations, actor/auth import, mailbox sequences, fenced leases, complete run/checkpoint protocol, durable cancellation, a minimal recent-event memory projection, the new tool-attempt contract, and outbox-only output. A local authenticated ingress and fake delivery adapter provide a runnable end-to-end slice. Direct `LlmStreamSink` delivery is adapted behind durable output events before crash-safety claims are made.

### 2. Telegram webhook vertical slice

Add `codrik serve`, webhook ownership and authentication, Telegram ingress, durable outbox delivery, identity linking and revocation, audience-aware routing, and operational diagnostics. Polling and webhook modes are mutually exclusive for a bot token. The release switch requires the old polling service to be stopped before webhook registration, and rollback unregisters the webhook before polling resumes.

### 3. Scheduler and autonomous work

Add task revisions, scheduling occurrences, catch-up and overlap policies, aggregate budgets, cooperative resumption, meaningful-change filtering, and task controls. Background notifications use the already durable delivery path.

### 4. Long-term actor memory

Add episodes, stable knowledge, audience-safe retrieval, compaction, provenance, retention controls, and rebuildable projections. The recent-event projection from the kernel keeps earlier slices runnable without pretending to provide full long-term memory.

The supported release requires all four slices, migration tests for authorization, crash-recovery tests, Telegram end-to-end tests, and a documented manual removal path for old session files. Session commands and direct polling execution are removed only after those gates pass.

## Out of scope

- Migrating or automatically deleting existing conversation session data;
- distributed multi-process execution in the first implementation;
- a PostgreSQL storage implementation;
- guaranteeing exactly-once tools or gateway delivery;
- broadcasting notifications to every linked channel;
- continuously calling the model while no event is ready;
- exposing internal sessions or context windows as user-facing concepts;
- treating model output or untrusted external content as an authority boundary.
