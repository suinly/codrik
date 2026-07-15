# Task 10 Report: Unix Socket Security, Instance Lock, and IPC Server

Implemented a fail-closed same-UID Unix IPC boundary for Linux and macOS. The
runtime now has an OS-managed exclusive instance lock, secure directory/socket
validation and binding, injectable peer credential checks, a 64-connection
server, per-connection serialized delivery writers, and an in-flight submission
registry that closes the submit/resume durable-lookup race.

## RED evidence

The required suites were run separately before production definitions existed:

- `rtk cargo test runtime::instance_lock::tests` failed with unresolved
  `InstanceLock`.
- `rtk cargo test runtime::ipc::security::tests` failed with unresolved
  `AuthorizedUnixStream`, `PeerCredentials`, and secure-path helpers.
- `rtk cargo test runtime::ipc::server::tests` failed with unresolved
  `SubmissionRegistry`.

A second RED cycle added stale regular-file rejection and the
delivery-before-`Accepted` race. It failed because stale cleanup still accepted
regular files and because the delivery gate/control-write API did not exist.
Slow/incomplete-frame tests subsequently exposed that protocol errors were
incorrectly waiting behind the delivery gate; the fix routes protocol errors
through the control-write boundary.

Post-review RED regressions then reproduced the remaining lifecycle and
security defects:

- active Resume timed out without `Accepted`, pending terminal Resume returned
  `missing_result`, and detached active requests never observed durable rebind;
- Resume allocated a transient queue and consumed the global byte budget;
- the accept loop returned while a handler was blocked between accept and
  connection registration;
- nested writable/symlink ancestors passed path validation;
- stale cleanup followed the configured pathname after its parent was renamed;
- integrated commit/rollback races, detached duplicates, Cancel, and ACK lacked
  server-level wire coverage.

A final RED cycle reproduced the retained-terminal delivery race: Resume
connections that entered while a bundle was `Delivering` or `FailedRetryable`
never re-read durable state, so an ACK transition to `Delivered` left excluded
sinks hanging forever. The outbox snapshot also had no explicit per-bundle
participation marker, so a newly included sink could not safely distinguish an
already-started transmission from replay when another sink ACKed before its
first frame.

## GREEN implementation

- `InstanceLock::acquire(lock, socket)` requires both configured names to be
  direct children of one effective-UID-owned mode-`0700` runtime directory.
  It holds that directory fd, opens the exact mode-`0600` lock through
  `openat(O_NOFOLLOW)`, and uses `fs2::FileExt::try_lock_exclusive`. Cleanup has
  no caller-supplied path: it validates the bound socket name with
  `fstatat(AT_SYMLINK_NOFOLLOW)` and removes it with `unlinkat` on the held fd.
  An unrelated same-UID socket or replacement parent pathname is never removed.
- Security helpers atomically request mode `0700` through `DirBuilderExt` under
  a serialized `0077` umask and require an existing managed runtime directory
  to have exact mode `0700`. Validation walks every normalized absolute path
  component with `symlink_metadata`, rejecting nested symlinks, unsafe owners,
  and writable ancestors; root-owned sticky system temp ancestry is the sole
  explicit writable exception. Socket binding runs under the same restrictive
  umask and verifies owner-mode `0600` before Tokio can accept.
- `PeerCredentials` is injectable. Production reads Linux `SO_PEERCRED` or
  macOS `getpeereid`; `AuthorizedUnixStream` compares the peer UID with
  `geteuid` before any frame bytes are read.
- `LocalIpcServer` acquires one of exactly 64 permits before `accept` and before
  spawning a handler. The 65th connection remains unhandled until a permit is
  released, and proceeds after an earlier connection closes. Each handler
  decodes exactly one operation and monitors the read half for EOF or a second
  operation so abandoned clients release permits.
- The existing protocol reader/writer supplies 5-second header, 30-second body,
  and 30-second write deadlines. Malformed, incomplete, and slow frames receive
  a typed protocol error when possible and then cross the explicit close/abort
  boundary.
- Every connection has one `Mutex`-serialized socket writer implementing
  `BundleDeliverySink`; `send` completes the actual frame write and failures
  close the write half. A delivery gate ensures outbox transmissions cannot
  overtake Submit `Accepted`. Control responses bypass that gate.
- A fully decoded Submit enters `SubmissionRegistry` before subscriptions and
  before the trusted SQLite ingress future. The RAII guard signals waiters on
  both commit and rollback paths and removes only its own generation. A
  concurrent duplicate Submit joins the first registration before performing
  its idempotent durable ingress call. Resume joins the same watch entry before
  durable lookup.
- `LocalRequestRecord` now carries its durable event ID, mailbox sequence, and
  joined bundle state. Active Resume emits `Accepted` with the real work item
  and sequence. Detached active Resume/duplicate Submit poll durable state until
  a real rebind can be announced or terminal delivery becomes available.
  Pending, delivering, and retryable terminal requests retain a delivery-only
  connection for the worker; only delivered bundles use read-only replay.
- `StreamHub::subscribe_delivery` stores only sink membership and notification
  state. It allocates no transient queue and consumes none of Task 7's per-
  subscription or global transient byte budget. Submit retains its combined
  transient/delivery subscription.
- Every outbox fixed snapshot now atomically reserves per-bundle transmission
  participation on each retained connection before any frame send begins.
  Resume polls durable state while bundles remain pending, delivering, or
  retryable; on `Delivered`, it claims mutually exclusive replay participation.
  Excluded late/stale-ACK connections replay read-only from `FinalBegin`, while
  transmission-reserved connections never race a duplicate replay even if a
  different sink ACKs before their first frame. Participation disappears with
  the connection, and the original fixed snapshot and already-started sends
  remain unchanged.
- Cancel emits exact `CancelAccepted`; ACK delegates the exact `BundleAck` and
  closes after success. Integrated frame tests cover both.
- Every accepted handler is owned by the server's `JoinSet`, including the
  accept-to-registration window. Shutdown stops acceptance, broadcasts
  `ServerShuttingDown` to registered sinks, aborts full handler futures (read
  and write halves), drains all tasks, and returns only after permits and
  subscriptions are released.

## Verification

- `rtk cargo test runtime::instance_lock::tests` — 6 passed.
- `rtk cargo test runtime::ipc::security::tests` — 6 passed.
- `rtk cargo test runtime::ipc::server::tests` — 23 passed.
- `rtk cargo test runtime::stream_hub::tests` — 11 passed.
- `rtk cargo test runtime::outbox_worker::tests` — 18 passed.
- `rtk cargo test runtime::ipc::protocol::tests` — 19 passed.
- `rtk cargo test runtime::sqlite::local_ingress::tests` — 9 passed.
- `rtk cargo test` — 393 passed, 1 ignored.
- `rtk cargo check` — passed; the existing crate-wide unused/dead-code warning
  baseline remains until production composition in Task 12.
- `rtk cargo fmt --check` — passed.
- `rtk cargo clippy --all-targets --all-features` — 0 errors; existing warning
  baseline remains.
- `rtk git diff --check` — passed.

## Self-review and concerns

- Authentication occurs before `FrameReader` construction, so rejected peers
  cannot drive parsing or allocation.
- The socket listener is not reachable through `LocalIpcServer::bind` until
  parent validation, restrictive bind, chmod, and post-bind verification all
  succeed.
- Final delivery and transient events share one serialized writer. Task 9
  snapshot/ACK semantics are unchanged: ACK uses a separate one-operation
  connection, and a first ACK does not cancel already-started snapshot sends.
- Resume intentionally does not replay transient events. It owns only a
  delivery subscription; active work receives a real durable `Accepted`, while
  terminal pending work remains registered until worker delivery or EOF.
- No work item ID is fabricated for detached requests. The connection observes
  durable rebind and emits `Accepted` only when a real ID exists, or transitions
  to pending/replay delivery according to joined bundle state.
- Commit and rollback races assert that Resume never queries durable state while
  a matching Submit transaction is in flight. Commit produces matching exact
  `Accepted` frames; rollback produces `missing_request` only after completion.
- Grace-period coordination, component lease shutdown, production composition,
  CLI behavior, and signal ownership remain Task 12/11 scope and were not added.
