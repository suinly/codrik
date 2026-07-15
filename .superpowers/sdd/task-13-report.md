# Task 13 Report: Installer, End-to-End Recovery, and Final Verification

## Outcome

Task 13 is complete. The installer now provisions a foreground `codrik serve`
service and a private local owner on a clean install, preserves existing
authorization byte-for-byte, and refuses to start an old configuration that has
no runtime actor. The acceptance suite exercises the production binary through
a real Unix socket, on-disk SQLite, and a loopback scripted Responses API.

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

After the installer implementation, all 10 textual, golden, and sourced-shell
behavior tests pass. They verify:

- systemd `codrik.service` runs only `<binary> serve`;
- launchd `com.suinly.codrik` contains only the `serve` program argument;
- legacy gateway units are removed;
- absent or empty authorization creates only enabled
  `actor:local:owner`, empty identities, and `tools: ["*"]`;
- the runtime directory is mode 0700 and `users.json` is mode 0600;
- existing authorization remains byte-for-byte unchanged and requires an
  explicit actor-ID prompt;
- a retained old config prints the exact `runtime.actor_id` YAML instruction
  and prevents service startup.

### Runtime acceptance matrix

`rtk cargo test --test serve_runtime -- --nocapture` passes all 17 scenarios:

1. Submit streams deltas, a verified immutable final bundle, and supports ACK.
2. Duplicate Submit creates one durable execution.
3. Reusing a request ID with different content is rejected as a conflict.
4. Disconnect during streaming does not cancel durable work.
5. Resume joins a live run or replays its completion.
6. Multiple incorporated requests receive their final rows.
7. Restart after ingress preserves durable state.
8. Lost final ACK redelivers the same bundle without model recomputation.
9. A second daemon fails lock acquisition without removing the live socket.
10. SIGTERM preserves resumable active state.
11. Orphaned running tools recover without reinvocation.
12. The fifth runtime failure delivers a terminal error.
13. A disabled configured actor prevents readiness.
14. Disconnect before Accepted cannot race into a false missing Resume.
15. A multi-frame large result with 34 deliveries replays as one bundle.
16. Cancel produces a terminal cancellation bundle.
17. Ninety-six slow and 32 malformed clients remain bounded, after which a
    valid client succeeds.

The harness uses short private temporary roots, cleanup guards, a spawned
production supervisor, and a loopback-only scripted provider. It uses no
external network or credentials. The lost-ACK crash boundary expires the
persisted claim deterministically instead of sleeping for a production lease.

Focused RED tests also captured both production bugs before their fixes: the
paused-time Unix-socket client test failed with `frame header deadline
exceeded`, and an intermittent 16/17 acceptance run was reduced to a
deterministic server test for the pre-Accepted ordering race. The corresponding
focused suites now pass (57 IPC tests and 27 server tests at the time of the
focused runs).

## Ledger Minor Resolutions

- Task 6: protocol round-trip tests now assert exact frozen JSON bytes for all
  four client requests and all twelve server events, not only ACK.
- Task 11: the cross-process request-metadata test now waits for an explicit
  child-started rendezvous marker rather than a fixed 100 ms sleep.

The Task 4 synthetic sequence-overflow/non-Unix concern was reviewed and is not
part of Task 13's supported Linux/macOS runtime path, so no unrelated behavior
was changed.

## Manual Foreground Transcript

The transcript used `target/debug/codrik` through `rtk env`, a private temporary
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
were removed afterward.

## Final Verification

The required commands were run in order:

```text
rtk cargo fmt --check                              PASS
rtk cargo test                                    PASS: 436 passed, 1 ignored, 0 failed
rtk cargo check                                   PASS: 0 errors
rtk cargo clippy --all-targets --all-features     PASS: 0 errors
rtk git diff --check                              PASS
```

The crate's existing warning set remains visible in `cargo check` and Clippy;
there are no lint errors. Acceptance tests intentionally serialize daemon-level
scenarios because they exercise process signals, sockets, and shared timing.

## Commit

Conventional commit subject: `feat(runtime): ship the serve workflow`.
The resulting commit SHA is recorded in the task handoff because the report is
part of that commit.
