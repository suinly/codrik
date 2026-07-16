# Task 13 Report: Installer, End-to-End Recovery, and Final Verification

## Outcome

Task 13 is complete. The installer now provisions a foreground `codrik serve`
service and a private local owner only on a true clean install, preserves
existing authorization byte-for-byte, and refuses unsafe upgrade or broken
runtime-actor configurations. The acceptance suite exercises both the
production binary and an explicitly composed production supervisor through real
Unix sockets and on-disk SQLite.

The work also resolved two production defects exposed by acceptance testing:

- The local IPC client imposed 5-second header and 30-second body deadlines on
  trusted daemon responses. Server-event reads now have no artificial response
  deadline; request-side protections for untrusted clients remain in place.
- A Resume accepted immediately after Submit could be decoded first and report
  a false missing request. Decode registration now respects accept order until
  an earlier Submit has registered or failed decoding.

## Test-Driven Evidence

### Installer RED and GREEN

The first `rtk cargo test --test install_script` run failed six tests against
the legacy gateway installer: it still generated gateway service names and
arguments, did not bootstrap the runtime owner/config, did not preserve the
required old-config behavior, and retained polling-gateway installation.

After implementation and review correction, all 17 textual, golden, and sourced-shell
behavior tests pass. They verify:

- systemd `codrik.service` runs only `<binary> serve`;
- launchd `com.suinly.codrik` contains only the `serve` program argument;
- legacy gateway units are removed;
- absent or empty authorization creates only enabled `actor:local:owner`, empty
  identities, and `tools: ["*"]` on a true clean interactive install;
- pre-write config, authorization, and service state distinguishes a clean
  install from an upgrade; an upgrade with absent/empty authorization never
  silently grants wildcard tools and never starts the service;
- any existing installed binary, service, config directory, users/runtime
  artifact, or legacy config makes the run an upgrade; a two-run binary-only
  install cannot gain wildcard authorization on its second run;
- the runtime directory is mode 0700 and `users.json` is mode 0600;
- existing authorization remains byte-for-byte unchanged and requires an
  explicit actor-ID prompt;
- hidden installer validation commands delegate to production `AppConfig` and
  `AuthorizationStore` parsers; valid quoted/unquoted actor strings are accepted,
  while YAML bool/number/null/duplicate fields and malformed/actorless JSON
  block service startup;
- a retained old config prints the exact `runtime.actor_id` YAML instruction
  and prevents service startup.

### Runtime acceptance matrix

`rtk cargo test --test serve_runtime -- --nocapture` passes all 17 scenarios:

1. Submit streams deltas, a cryptographically verified immutable final bundle
   whose ID matches SQLite, and supports ACK only after verification.
2. Duplicate Submit creates one durable execution.
3. Reusing a request ID with different content is rejected as a conflict.
4. Disconnect during streaming does not cancel durable work.
5. Resume joins a live run or replays its completion.
6. Two requests share one work item and one active/terminal incorporated run,
   produce request-specific bundles/intents, and invoke the provider once.
7. Four deterministic crash boundaries recover: a live IPC ingress commit
   before dispatch, live attachment/incorporation before the model, committed
   model/tool checkpoint, and terminal finalization before delivery ACK.
8. Lost final ACK redelivers the same bundle without model recomputation.
9. A second daemon fails lock acquisition without removing the live socket.
10. SIGTERM preserves resumable active state.
11. Orphaned running tools recover without reinvocation.
12. Failures persist across a supervisor restart and the fifth invocation
    produces a verified `dispatcher_failure_limit` terminal bundle.
13. A disabled configured actor prevents readiness.
14. Disconnect before Accepted cannot race into a false missing Resume.
15. Exact 300 KB text spans multiple FinalChunk frames alongside 33 files; all
    34 deliveries ACK, then Resume replays the same full bundle read-only.
16. Cancel produces and ACKs verified request-specific cancellation bundles for
    both incorporated requests.
17. Ninety-six slow and 32 malformed clients remain bounded, after which a
    valid client succeeds.

`FinalBundleVerifier` is the production LocalRenderer assembler and verifier,
not an acceptance copy. Both rendering and acceptance require FinalBegin, every
canonical ordered FinalChunk, and FinalEnd through this same component. It
checks request, bundle, and delivery IDs; duplicate IDs; aggregate limits;
contiguous chunk order and exact chunk formula; canonical base64 partition and
sizes; decoded totals; per-payload SHA-256 and kind; canonical manifest hash;
exact JSON object shape; Unicode; artifact UUID/path/hash/u64 semantics; and
missing/unknown/duplicate fields. It returns typed `FinalPayload` deliveries and
ACK coordinates. No output or acceptance ACK occurs before verification.

The harness uses short private temporary roots and non-recursive last-guard
cleanup. Binary scenarios use a loopback-only scripted Responses API. Recovery
scenarios use the production `app::serve` composition seam with an injected
ManualClock and scripted `LlmStreamClient`, while still running the real
supervisor, Unix server/socket, and on-disk SQLite. Production uses the same seam
with SystemClock and OpenAI. Injectable runtime boundary hooks are zero-op in
production; acceptance uses them to pause actual orchestration after ingress
commit and after incorporation commit, proving scenario 7 A/B with running
servers rather than direct database synthesis. It uses no hidden environment
hooks, external network, or credentials. Expiry, backoff, and the fifth failure
are driven by clock advancement and DB/provider rendezvous rather than raw
timestamp rewrites or sleep-only crash boundaries.

Focused RED tests also captured both production bugs before their fixes: the
paused-time Unix-socket client test failed with `frame header deadline
exceeded`, and an intermittent 16/17 acceptance run was reduced to a
deterministic server test for the pre-Accepted ordering race. The corresponding
focused suites now pass (59 IPC tests and 27 server tests at the time of the
focused runs).

The first review correction REDs were also observed: the new installer flows
failed because clean/upgrade state and semantic parsing did not exist; strict
cancellation Resume timed out while bundles were already claimed on Submit
streams; and the initial deterministic in-process crash restart hit a stale
component lifetime until the aborted supervisor task was awaited. Each now has
a green focused regression path.

The third review correction also followed RED/GREEN:

- the shared verifier test first failed because `FinalBundleVerifier` did not
  exist, then passed with LocalRenderer and acceptance on the same code path;
- scenario 7 failed to compile until injectable runtime boundary hooks and the
  hook-aware composition seam existed; the genuine live A/B crash scenario then
  passed with zero model calls before each crash;
- installer validator tests initially failed because the hidden command was
  parsed as an ordinary prompt; production-parser commands and binary-aware
  clean-state capture now pass all 17 installer tests.

## Ledger Minor Resolutions

- Task 6: protocol round-trip tests now assert exact frozen JSON bytes for all
  four client requests and all twelve server events, not only ACK.
- Task 11: the cross-process request-metadata test now waits for an explicit
  child-started rendezvous marker rather than a fixed 100 ms sleep.

The Task 4 synthetic sequence-overflow/non-Unix concern was reviewed and is not
part of Task 13's supported Linux/macOS runtime path, so no unrelated behavior
was changed.

## Manual Foreground Transcript

The original Task 13 transcript used `target/debug/codrik` through `rtk env`, a private temporary
runtime/config, and a local scripted streaming provider.

```text
$ rtk env ... target/debug/codrik serve
{"component":"startup","actor_id":"actor:local:owner","transition":"recovered","database_path":".../runtime.sqlite","socket_path":".../codrik.sock","schema_version":2,"recovery":{...}}
{"component":"startup","transition":"ready"}

$ rtk env ... target/debug/codrik serve
Error: another runtime owns lock .../runtime.lock
Caused by: Resource temporarily unavailable (os error 35)
```

The first daemon's socket remained live and accepted subsequent CLI requests.
A scripted provider response delayed more than five seconds then completed,
confirming that the client no longer times out a healthy foreground runtime.

```text
$ rtk env ... target/debug/codrik "hello"
| / streamed
^Ccodrik resume 52f017be-be20-42da-9026-c75cbd1f5938

$ rtk env ... target/debug/codrik resume 52f017be-be20-42da-9026-c75cbd1f5938
authoritative final text

$ rtk env ... target/debug/codrik cancel 511ab456-2ee0-4c62-9dcf-cf1c36f4b358

$ rtk env ... target/debug/codrik resume 511ab456-2ee0-4c62-9dcf-cf1c36f4b358
Error [cancelled]: request was cancelled
```

The cancel command exited successfully with no stdout, and Resume delivered the
durable terminal cancellation. All temporary manual and acceptance artifacts
were removed afterward. The correction did not change the foreground CLI
contract; process-binary acceptance coverage was rerun after the composition
refactor, while the transcript above remains the recorded manual evidence.

## Final Verification

The required commands were run in order:

```text
rtk cargo fmt --check                              PASS
rtk cargo test                                    PASS: 446 passed, 1 ignored, 0 failed
rtk cargo check                                   PASS: 0 errors
rtk cargo clippy --all-targets --all-features     PASS: 0 errors
rtk git diff --check                              PASS
```

The crate's existing warning set remains visible in `cargo check` and Clippy;
there are no lint errors. Acceptance tests intentionally serialize daemon-level
scenarios because they exercise process signals, sockets, and shared timing.

## Commit

Original commit: `4c8ae2eefa63c451c34e1a1c6696916f2d6bf0a2`
(`feat(runtime): ship the serve workflow`).

Review-correction subject: `fix(runtime): harden serve acceptance and upgrades`.
Commit: `71698d0aa671fcd1cd03531ceed2a97bd5932f3f`.

Third-review correction subject:
`fix(runtime): share final verification and validate upgrades`.
Its resulting SHA is recorded in the handoff because this report is part of the
correction commit.
