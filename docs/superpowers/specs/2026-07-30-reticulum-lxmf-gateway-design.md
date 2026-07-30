# Reticulum LXMF Gateway Design

## Goal

Add an optional LXMF channel for private text conversations with Codrik agents.
The gateway connects to an existing Reticulum `TCPServerInterface`, supports
identity linking through `/link CODE`, and reuses Codrik's durable ingress,
actor routing, and delivery machinery.

## Scope

The first version supports:

- one stable Codrik LXMF identity;
- one configured Reticulum TCP endpoint;
- direct inbound UTF-8 text messages;
- `/link CODE` identity linking;
- durable text replies and delivery outcomes;
- supervised startup and shutdown with `codrik serve`.

It does not support attachments, group conversations, LXMF propagation-node
configuration, rich-message rendering, multiple local LXMF identities, or a
native Rust implementation of Reticulum and LXMF.

## Approach

Codrik starts a bundled Python bridge and exchanges newline-delimited JSON over
the child's standard input and standard output. The bridge uses the upstream
Python `RNS` and `LXMF` packages. The user installs those packages in an
environment visible to the configured Python executable.

This avoids implementing Reticulum and LXMF in Rust. Standard streams avoid a
second listener, socket permissions, endpoint discovery, and stale socket
cleanup. A separate service and PyO3 embedding are excluded because both add
lifecycle and deployment complexity without improving the initial channel.

## Configuration

Reticulum support is optional. A minimal configuration is:

```yaml
reticulum:
  rns_address: "127.0.0.1:4242"
  python: "/absolute/path/to/venv/bin/python3"
```

`rns_address` is required when the section exists and is parsed as a nonempty
host plus a TCP port. `python` is optional and defaults to `python3`. An empty
value is rejected. Codrik invokes the executable directly without a shell.

The bridge receives runtime paths and the parsed endpoint through its initial
JSON request, not command-line arguments. This keeps mutable values out of
process listings and gives startup one typed protocol boundary.

## Persistent State

Codrik creates `<CODRIK_HOME>/reticulum` under the same private-directory rules
used by other runtime state. The bridge stores its stable identity at
`<CODRIK_HOME>/reticulum/identity` and its generated RNS configuration below
`<CODRIK_HOME>/reticulum/rns`.

The generated RNS configuration contains one enabled `TCPClientInterface`
pointing at `rns_address`. It is owned by Codrik and may be replaced on startup
to reconcile endpoint changes. The identity file is created only when absent
and is never replaced automatically. Its resulting LXMF destination therefore
remains stable across restarts and endpoint changes.

Identity and RNS state must be regular files below the validated private state
directory. Unsafe ownership, permissions, or symlink traversal fails startup.

## Components

`interfaces::reticulum` owns the channel adapter:

- bridge process lifecycle and JSON Lines transport;
- LXMF ingress translation;
- durable delivery worker;
- mapping bridge outcomes into gateway delivery states.

The bundled Python bridge owns only protocol-specific work:

- validating imports and loading or creating the RNS identity;
- creating the RNS stack against the generated TCP client configuration;
- announcing and receiving through one LXMF delivery destination;
- sending outbound LXMF messages and reporting outcomes;
- serializing validated events to Codrik.

The existing runtime remains authoritative for actor identity links,
deduplication, work creation, reply routing, outbox persistence, retries, and
terminal delivery state. Reticulum wiring stays in `app.rs`, beside Telegram
composition.

## Bridge Protocol

Each standard-input and standard-output line is one UTF-8 JSON object. Every
object contains a `type` discriminator. Lines and decoded messages have fixed
size limits; oversized, malformed, or unknown objects are protocol errors.
Diagnostic logs use standard error only. Standard output is reserved for the
protocol.

Codrik sends:

- `start`: state directory and parsed RNS host and port;
- `send`: durable delivery ID, destination hash, and text;
- `shutdown`: graceful termination request.

The bridge sends:

- `ready`: stable local LXMF destination hash;
- `inbound`: LXMF message hash, source destination hash, timestamp, and text;
- `delivery`: delivery ID and `delivered`, `retryable`, `terminal`, or
  `outcome_unknown` outcome, with an optional retry delay;
- `fatal`: startup or unrecoverable protocol error.

Codrik does not report gateway readiness until it receives `ready`. The local
LXMF destination is emitted through the existing runtime logging boundary so
operators can address the agent without exposing private identity material.

Only one request owns a durable delivery ID. Duplicate or late delivery events
are handled idempotently by the existing delivery store. Protocol versioning,
capability negotiation, and bridge hot replacement are omitted until a second
compatible implementation exists.

## Ingress Flow

The bridge accepts only LXMF messages whose content decodes to text. For each
message it emits the immutable LXMF message hash as the external ID, the source
destination hash as both channel identity and reply address, the LXMF
timestamp, and UTF-8 text.

The Reticulum ingress service validates hashes and text bounds, then calls the
existing durable ingress boundary. The message hash supplies deduplication.
Replayed LXMF messages therefore do not create duplicate work or model calls.

`/link CODE` is handled at the gateway boundary exactly like Telegram linking:
it consumes the one-time code, associates the source destination hash with an
actor, and creates no agent memory or model call. Normal messages from linked
identities resolve to that actor. Unlinked identities receive only the minimal
link-required response.

The durable reply route records the Reticulum gateway identity and source
destination hash. Actor execution remains unaware of LXMF.

## Delivery Flow

The delivery worker claims an existing durable gateway delivery and sends its
ID, destination hash, and text to the bridge. LXMF receives plain text;
Markdown remains readable source text. Initial text bounds follow the bridge's
validated LXMF payload ceiling rather than splitting blindly across encoded
LXMF limits.

Bridge results map as follows:

- `delivered` becomes `delivered`;
- `retryable` schedules the existing bounded retry path, respecting a supplied
  retry delay;
- `terminal` becomes `failed_terminal`;
- `outcome_unknown` becomes `outcome_unknown` and is not automatically resent.

Codrik must not infer successful delivery merely from a successful write to
the child process. Bridge termination while a send is unresolved maps that
delivery to `outcome_unknown`, preventing accidental duplicate messages.

## Lifecycle and Failures

Startup validates configuration and state paths, spawns the bridge, sends
`start`, and waits for either `ready`, `fatal`, child exit, or a bounded startup
timeout. Missing Python, missing `RNS` or `LXMF`, an unreachable RNS endpoint,
or identity/configuration failure produces a concise startup error. Reticulum
failure does not silently disable a configured channel.

After readiness, ingress, delivery, child monitoring, and standard-error log
forwarding run as one supervised Reticulum component because they share one
child process and protocol stream. Unexpected bridge exit fails that component;
the existing fail-fast supervisor then stops the runtime and returns an error
to its external service manager. Codrik does not implement a second independent
restart loop.

Shutdown stops new delivery claims, sends `shutdown`, waits for a bounded grace
period, then kills and reaps the child if necessary. Pending durable work stays
in SQLite. Any in-flight send lacking a definitive bridge outcome is marked
`outcome_unknown`.

## Security

- The bridge is launched directly, never through a shell.
- JSON framing is bounded before allocation and deserialization.
- Hashes, ports, message text, event types, and delivery IDs are validated at
  the Rust trust boundary.
- The identity private key is stored only in the private Reticulum state
  directory and never logged or sent over the bridge protocol.
- Child standard error is bounded and sanitized before runtime logging.
- Unknown protocol events and unsolicited delivery IDs fail closed.
- The TCP connection relies on Reticulum's protocol security; operators remain
  responsible for network reachability to the configured `rnsd` endpoint.

## Testing

Focused Rust tests cover:

- optional configuration and strict host/port/Python validation;
- secure Reticulum state path creation;
- child startup, readiness timeout, shutdown, and reap behavior using a fake
  bridge process;
- JSON Lines size limits, malformed input, unknown events, and stderr handling;
- ingress validation and deduplication by LXMF message hash;
- `/link CODE`, identity conflict, and linked actor routing;
- durable reply-route construction;
- delivery outcome mapping, retry delay, late events, bridge exit, and
  `outcome_unknown` handling;
- runtime behavior with Reticulum absent and configured.

The Python bridge provides a runnable self-check for imports, JSON framing, and
protocol serialization without joining a live network. A manual integration
test starts Codrik against the configured `TCPServerInterface`, records the
printed LXMF destination, links a remote identity, exchanges text in both
directions, restarts Codrik, and verifies the destination remains unchanged.

## Documentation

README configuration documents the Python environment requirement, the
`rns_address` endpoint, identity location, printed destination, linking flow,
supported text-only scope, and troubleshooting for imports, TCP reachability,
and bridge termination. It states that Codrik neither installs Python packages
nor manages `rnsd`.
