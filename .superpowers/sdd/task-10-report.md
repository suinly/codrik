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

## GREEN implementation

- `InstanceLock` opens the lock with `O_NOFOLLOW | O_CLOEXEC`, validates the
  direct parent and opened file using `symlink_metadata`/`fstat` ownership and
  permission data, and uses `fs2::FileExt::try_lock_exclusive`. Its stale-path
  API is available only while holding the lock and removes only an actual Unix
  socket, never a regular file, directory, or symlink.
- Security helpers create mode-`0700` directories, reject symlinks,
  wrong-effective-UID ownership, and group/world-writable directories. Socket
  binding is performed under a serialized `0077` umask window, then explicitly
  set and verified as owner-mode `0600` before conversion to a Tokio listener.
- `PeerCredentials` is injectable. Production reads Linux `SO_PEERCRED` or
  macOS `getpeereid`; `AuthorizedUnixStream` compares the peer UID with
  `geteuid` before any frame bytes are read.
- `LocalIpcServer` acquires one of exactly 64 permits before `accept` and before
  spawning a handler. The 65th connection remains unhandled until a permit is
  released. Each handler decodes exactly one operation and monitors the read
  half for EOF or a second operation so abandoned clients release permits.
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
- Submit installs stream and delivery subscriptions before ingress, writes
  `Accepted` first for new/attached durable work, then forwards transient and
  final events. Resume installs only a delivery sink and wakes the Task 9 worker
  through the existing registry change notification. Terminal Resume uses the
  Task 9 read-only replay path. Cancel emits `CancelAccepted`; ACK delegates the
  exact `BundleAck` and closes after success.
- Active connections are tracked without persistence dependencies. Server
  shutdown stops accepting, sends `ServerShuttingDown` with a typed request ID
  and resume command when known, and explicitly aborts the socket sink.

## Verification

- `rtk cargo test runtime::instance_lock::tests` — 3 passed.
- `rtk cargo test runtime::ipc::security::tests` — 3 passed.
- `rtk cargo test runtime::ipc::server::tests` — 9 passed.
- `rtk cargo test` — 369 passed, 1 ignored.
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
- Resume intentionally does not replay transient events. An active Resume holds
  only a delivery subscription until client EOF; a terminal Resume delegates
  retained replay to `OutboxWorker`.
- Submit duplicate rows whose detached work item is no longer present take the
  durable replay path directly. Protocol v1 has no duplicate event and its
  `Accepted` shape requires a work item ID, so no identifier is fabricated.
- Grace-period coordination, component lease shutdown, production composition,
  CLI behavior, and signal ownership remain Task 12/11 scope and were not added.
