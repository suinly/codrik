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

The schema does not have a generic `blocked` work-item variant, so poison input
uses `blocked_unknown_outcome` as the existing non-dispatchable blocked state.
This is truthful and loop-safe, but a later schema revision may choose a more
specific poison-data state.
