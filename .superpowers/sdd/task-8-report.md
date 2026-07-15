# Task 8 Report: Persisted Dispatcher Failures and Continuous Loop

## Outcome

Implemented the one-worker configured-actor dispatcher. It drains fenced runner
quanta, sleeps on actor notifications plus a 500 ms fallback poll, honors
persisted `next_attempt_at`, isolates recoverable work failures, and terminates
on authority-store failures.

Failure persistence uses exact 1/2/4/8 second retry scheduling. The fifth
consecutive failure atomically creates a typed terminal-error result bundle for
every incorporated local request through the Task 5 helper, fails the parent
run/work and incorporated events, and returns unincorporated events to ready.
Running tool attempts become `outcome_unknown`; existing unknown/decision states
are not rewritten. Only committed model checkpoints, known tool outcomes, and
finalization report progress and reset failure history; incorporation replay
reports no progress.

Malformed inbound and checkpoint JSON is detected inside the dispatch
transaction, moves the work/event out of the ready loop, and records a
structured diagnostic. Immediate dispatch/failure transactions use three
bounded SQLite busy/locked retries at 10, 25, and 50 ms. Exhausted busy/locked,
I/O, corruption, not-a-database, and related authority codes are classified
from typed SQLite error codes rather than message text.

## TDD Evidence

RED was captured before production APIs existed:

- `rtk cargo test runtime::dispatcher::tests` — failed with unresolved
  `FailureDisposition` and `QuantumProgress`.
- `rtk cargo test runtime::sqlite::failures::tests` — failed with unresolved
  `FailureStore`.
- `rtk cargo test runtime::sqlite::dispatch::tests::malformed_persisted_payload_is_atomically_blocked_without_redispatch`
  — failed because malformed JSON escaped `attach_next_run` instead of being
  durably blocked.

GREEN focused verification:

- `rtk cargo test runtime::dispatcher::tests` — 3 passed.
- `rtk cargo test runtime::sqlite::failures::tests` — 2 passed.
- `rtk cargo test runtime::sqlite::retry::tests` — 3 passed.
- `rtk cargo test runtime::sqlite::dispatch::tests` — 9 passed.
- `rtk cargo test runtime::runner::tests` — 10 passed.

## Final Verification

- `rtk cargo test` — 310 passed, 1 ignored.
- `rtk cargo fmt --check` — passed.
- `rtk cargo check` — passed with the existing crate-wide unused/dead-code
  warnings while later serve composition remains absent.
- `rtk cargo clippy --all-targets --all-features` — 0 errors; existing warnings
  remain.
- `rtk git diff --check` — passed.

The full suite initially exposed two restart tests failing because zero SQLite
busy timeout was applied during WAL/schema startup. The root cause was isolated
to second-connection initialization. Startup retains its existing bounded lock
tolerance, then switches the live connection to zero timeout so explicit
runtime retry timing remains authoritative. Both failing restart tests passed
individually before the full suite was rerun.

## Design Decisions and Scope

- `QuantumRunner` receives the configured actor ID, and SQLite has a scoped
  acquisition operation; the dispatcher cannot lease an unrelated actor.
- `QuantumProgress::None` is used for replayed incorporation and non-progress
  yields. Progress is reported only after a durable model/tool/final commit.
- Terminalization shares one immutable terminal-error intent across the
  per-request bundles constructed by Task 5.
- Malformed work uses the schema's existing `blocked_unknown_outcome` work
  state and `blocked` event state, with a JSON diagnostic in `last_error`.
- No outbox delivery worker, socket listener, supervisor, or production serve
  composition from Task 9 was added.

## Concerns

The original implementation reused `blocked_unknown_outcome` for poison input.
The review correction below supersedes that decision with the distinct
`blocked_malformed` state.

## Review Correction

The follow-up review identified authority and state-machine gaps in the first
implementation. The correction makes failure bookkeeping part of the runner's
lease-fenced quantum: `FailureFence` binds actor owner/generation, run, and work,
and progress/failure updates validate that fence inside the same immediate
transaction before the lease is released. A replaced worker therefore cannot
reset or terminalize work after a newer generation checkpoints or finalizes it.

Fifth-failure terminalization now detaches every unincorporated event and local
request from the terminal work. Dispatch creates a fresh work item before the
event is acquired again, while all ready/acquire paths require `state = 'ready'`
for already-associated work. The terminal work and its incorporated request
bundles remain immutable.

Poison persistence now has the distinct `blocked_malformed` work state in the
Rust model, fresh schema, poison transactions, and diagnostics. It is no longer
conflated with the recoverable tool-safety state `blocked_unknown_outcome`.

All Task 8 immediate authority writes involved in dispatch, checkpointing,
finalization, cancellation, tool preparation/outcomes, artifact outcome commit,
and failure/progress bookkeeping use the bounded busy/locked retry policy. The
retry closure contains only the database transaction, so filesystem/tool/model
effects are not repeated. A real two-connection SQLite write-lock test confirms
four attempts with the exact 10/25/50 ms sleeps; a separate wrapper test confirms
non-busy errors are propagated after one attempt.

The production runner now durably checkpoints assistant tool-call output before
tool execution. Focused tests exercise its four progress mappings directly:
model checkpoint, known tool outcome, finalization, and incorporation replay
with no progress.

Correction verification:

- `rtk cargo test runtime::sqlite::failures::tests` — 4 passed.
- `rtk cargo test runtime::sqlite::dispatch::tests` — 9 passed.
- `rtk cargo test runtime::sqlite::retry::tests` — 5 passed.
- `rtk cargo test runtime::runner::tests` — 14 passed.
- `rtk cargo test` — 318 passed, 1 ignored.
- `rtk cargo check` — passed with the existing crate-wide unused/dead-code warnings.
- `rtk cargo fmt --check` — passed.
- `rtk cargo clippy --all-targets --all-features` — 0 errors; existing warnings remain.
- `rtk git diff --check` — passed.

## Second Review Correction

The second review correction preserves the explicit fresh-database policy for
this unreleased single-user branch. Historical migration files define the final
fresh schema; existing development databases and sessions will be deleted
manually. No schema v3 or forward compatibility migration was added.

Progress is now monotonic within a runner quantum. If a model checkpoint or
known tool outcome commits and a later step fails, the runner passes that
progress into the failure transition. One lease-fenced immediate transaction
then resets the earlier failure history and records the new error as failure
one, avoiding a separately committed reset and its crash gap. Production tests
cover both model-checkpoint followed by tool-start failure and known-tool
outcome followed by a later model failure after four seeded failures; both
persist a one-second retry with no terminal bundle.

Failure fences now compare actor ID, owner, generation, and the exact stored
lease expiry token, plus run/work identity, lease generation, and compatible
current states. The clock is sampled independently inside every busy-retry
attempt. Same-owner renewal/reacquisition tests reject old expiry tokens, and
real SQLite contention tests prove that progress and fifth-failure writes become
stale when retry delay crosses lease expiry. Runner heartbeat renewal refreshes
the in-memory run and failure fence before further bookkeeping.

Detached local requests are a fully supported durable state. Duplicate submit
decodes and returns an optional work ID. Cancellation of an active detached
request atomically creates a fresh ready work item, rebinds the original event
and local request, freezes the cancellation targets, and attaches the cancel
event to that work. Direct duplicate, cancellation, resolution, and subsequent
dispatch-rebind paths are covered.

The tool-step limit now applies to both newly prepared calls and recovered
prepared attempts. Completed outcomes are checkpointed before a budget yield;
unexecuted attempts retain their original durable IDs for later quanta. Tests
cover a zero budget across recovery and three calls resumed one per quantum,
with no over-execution or duplicate attempt IDs.

Second-correction verification:

- `rtk cargo test runtime::sqlite::failures::tests` — 7 passed.
- `rtk cargo test runtime::runner::tests` — 18 passed.
- `rtk cargo test runtime::sqlite::local_ingress::tests` — 9 passed.
- `rtk cargo test runtime::sqlite::dispatch::tests` — 9 passed.
- `rtk cargo test runtime::sqlite::checkpoint::tests` — 13 passed.
- `rtk cargo test runtime::sqlite::retry::tests` — 5 passed.
- `rtk cargo test` — 327 passed, 1 ignored.
- `rtk cargo check` — passed with existing crate-wide warnings.
- `rtk cargo fmt --check` — passed.
- `rtk cargo clippy --all-targets --all-features` — 0 errors; warnings remain.
- `rtk git diff --check` — passed.

## Final Focused Correction

Mixed recovery can commit a known earlier tool outcome and then discover a
later outcome-unknown attempt in the same active run. Blocking that later
attempt truthfully leaves the run `active`, the work `waiting_for_decision`, and
at least one attempt `waiting_for_decision`. Because the quantum already made
known durable progress, failure history must still reset before the lease is
released.

The progress fence predicate now accepts precisely that state combination for
the exact actor owner/generation/expiry and run/work identity. It continues to
reject `blocked_unknown_outcome`, `blocked_malformed`, failed-terminal, expired,
renewed, and otherwise stale combinations. Failure recording remains restricted
to active/ready work, while completed and cancelled finalization pairs retain
their existing progress path.

A direct store predicate test captured the prior stale-lease failure and now
proves the waiting-for-decision pair resets failure count without admitting the
invalid blocked or terminal states. Production runner and dispatcher regressions
build a realistic active run containing an uncheckpointed known outcome followed
by a running attempt. Recovery checkpoints the known result, blocks the unknown
attempt, returns `WaitingForDecision`, resets four prior failures, and releases
the lease without an authority error.

Final-focused verification:

- `rtk cargo test runtime::sqlite::failures::tests` — 8 passed.
- `rtk cargo test runtime::runner::tests` — 20 passed.
- `rtk cargo test runtime::dispatcher::tests` — 3 passed.
- `rtk cargo test runtime::sqlite::checkpoint::tests` — 13 passed.
- `rtk cargo test` — 330 passed, 1 ignored.
- `rtk cargo check` — passed with existing crate-wide warnings.
- `rtk cargo fmt --check` — passed.
- `rtk cargo clippy --all-targets --all-features` — 0 errors; warnings remain.
- `rtk git diff --check` — passed.
