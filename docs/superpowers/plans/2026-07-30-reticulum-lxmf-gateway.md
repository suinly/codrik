# Reticulum LXMF Gateway Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an optional, durable, text-only LXMF channel that connects Codrik to an existing Reticulum TCP server through a supervised Python bridge.

**Architecture:** `codrik serve` validates Reticulum configuration and private state, materializes a bundled Python bridge, then supervises one Reticulum gateway component. Rust owns durable ingress, identity linking, actor routing, delivery claims, retries, and process fencing; Python owns only RNS/LXMF APIs and exchanges bounded JSON Lines over standard streams.

**Tech Stack:** Rust 2024, Tokio process/I/O/channels, Serde JSON, SQLite gateway store, Python 3, upstream `RNS` and `LXMF` packages.

## Global Constraints

- Run every shell command through `rtk`.
- Add no Rust dependency and no database migration.
- Invoke Python directly; never use a shell.
- Store stable identity at `<CODRIK_HOME>/reticulum/identity`.
- Store generated RNS configuration below `<CODRIK_HOME>/reticulum/rns`.
- Support direct UTF-8 text and `/link CODE` only; no files, groups, propagation-node configuration, or rich rendering.
- Use LXMF message hash as durable external ID and source destination hash as identity subject plus reply address.
- Treat bridge loss after send acceptance as `outcome_unknown`; never auto-resend it.
- Reserve child stdout for bounded JSON Lines; diagnostics use bounded, sanitized stderr.
- Keep all security-sensitive directories mode `0700`; reject symlinks and unsafe ownership.
- The user installs `RNS` and `LXMF`; Codrik never invokes `pip`.

---

## File Map

- Modify `src/config.rs`: deserialize and validate `reticulum.rns_address` and `reticulum.python`.
- Modify `src/interfaces.rs`: export the Reticulum interface module.
- Create `src/interfaces/reticulum.rs`: prepare the gateway, supervise the bridge, coordinate ingress and delivery.
- Create `src/interfaces/reticulum/protocol.rs`: bounded JSON Lines command/event types and hash/text validation.
- Create `src/interfaces/reticulum/bridge.rs`: secure script materialization, child spawn, stdin/stdout/stderr lifecycle.
- Create `src/interfaces/reticulum/ingress.rs`: `/link` handling, linked actor resolution, durable event insertion.
- Create `src/interfaces/reticulum/bridge.py`: RNS/LXMF adapter and standalone protocol self-check.
- Modify `src/app.rs`: private Reticulum state preparation, gateway composition, supervision, readiness logging.
- Modify `src/runtime/observability.rs`: Reticulum component and public destination metadata only.
- Modify `README.md`: setup, configuration, linking, supported scope, manual verification, troubleshooting.
- Modify `tests/serve_runtime.rs`: process-level optional/configured runtime coverage with a fake Python bridge.

---

### Task 1: Reticulum Configuration and Private State

**Files:**
- Modify: `src/config.rs:10-152,158-230,331-527`
- Modify: `src/app.rs:184-214,494-523`

**Interfaces:**
- Produces: `ReticulumConfig { rns_address: String, python: String }`.
- Produces: `ValidatedReticulumConfig { host: String, port: u16, python: PathBuf }`.
- Produces: `RuntimePaths.reticulum: PathBuf`, always `<CODRIK_HOME>/reticulum`.
- Consumes later: `ReticulumConfig::validate() -> Result<ValidatedReticulumConfig>`.

- [ ] **Step 1: Add failing configuration tests**

Add these tests to `config.rs`:

```rust
#[test]
fn reticulum_config_parses_endpoint_and_defaults_python() -> Result<()> {
    let config: AppConfig = yaml_serde::from_str(
        "api_key: k\nbase_url: https://example.test/v1\nmodel: m\nreticulum:\n  rns_address: mesh.example:4242\n",
    )?;
    let reticulum = config.reticulum.unwrap().validate()?;
    assert_eq!(reticulum.host, "mesh.example");
    assert_eq!(reticulum.port, 4242);
    assert_eq!(reticulum.python, PathBuf::from("python3"));
    Ok(())
}

#[test]
fn reticulum_config_rejects_invalid_values_and_unknown_fields() {
    for section in [
        "reticulum:\n  rns_address: ''",
        "reticulum:\n  rns_address: missing-port",
        "reticulum:\n  rns_address: host:0",
        "reticulum:\n  rns_address: host:70000",
        "reticulum:\n  rns_address: host:4242\n  python: ' '",
        "reticulum:\n  rns_address: host:4242\n  extra: true",
    ] {
        let yaml = format!("api_key: k\nbase_url: https://example.test/v1\nmodel: m\n{section}\n");
        let invalid = yaml_serde::from_str::<AppConfig>(&yaml)
            .map(|config| config.reticulum.unwrap().validate().is_err())
            .unwrap_or(true);
        assert!(invalid, "accepted invalid config:\n{yaml}");
    }
}
```

Extend `runtime_config_defaults_under_codrik_home`:

```rust
assert_eq!(paths.reticulum, PathBuf::from("/tmp/codrik-home/reticulum"));
```

- [ ] **Step 2: Verify tests fail**

Run: `rtk cargo test config::tests::reticulum -- --nocapture`

Expected: compile failure because `AppConfig.reticulum`, `ValidatedReticulumConfig`, and `RuntimePaths.reticulum` do not exist.

- [ ] **Step 3: Implement strict configuration parsing**

Add:

```rust
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReticulumConfig {
    #[serde(deserialize_with = "deserialize_strict_string")]
    pub rns_address: String,
    #[serde(default = "default_reticulum_python", deserialize_with = "deserialize_strict_string")]
    pub python: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedReticulumConfig {
    pub host: String,
    pub port: u16,
    pub python: PathBuf,
}

impl ReticulumConfig {
    pub fn validate(&self) -> Result<ValidatedReticulumConfig> {
        let (host, port) = self
            .rns_address
            .rsplit_once(':')
            .context("reticulum.rns_address must be host:port")?;
        if host.trim().is_empty()
            || !host
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'.' || byte == b'-')
        {
            bail!("reticulum.rns_address host is invalid");
        }
        let port = port
            .parse::<u16>()
            .context("reticulum.rns_address port is invalid")?;
        if port == 0 {
            bail!("reticulum.rns_address port must be greater than zero");
        }
        if self.python.trim().is_empty() {
            bail!("reticulum.python must not be blank");
        }
        Ok(ValidatedReticulumConfig {
            host: host.to_owned(),
            port,
            python: PathBuf::from(&self.python),
        })
    }
}

fn default_reticulum_python() -> String {
    "python3".into()
}
```

Add `#[serde(default)] pub reticulum: Option<ReticulumConfig>` to `AppConfig`. Add `reticulum: codrik_home.join("reticulum")` to `RuntimePaths::resolve_paths`.

- [ ] **Step 4: Create and validate the private state directory**

In `prepare_paths`, call:

```rust
create_secure_directory(&paths.reticulum)?;
```

In `validate_runtime_paths`, call:

```rust
validate_secure_directory(&paths.reticulum)?;
```

Include `paths.reticulum` in `required_parents`. Add an app test beside existing path security tests proving a symlinked or mode-`0770` Reticulum directory is rejected.

- [ ] **Step 5: Run focused tests**

Run: `rtk cargo test config::tests::reticulum config::tests::runtime_config_defaults_under_codrik_home app::tests -- --nocapture`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
rtk git add src/config.rs src/app.rs
rtk git commit -m "feat(reticulum): validate gateway configuration"
```

---

### Task 2: Bounded Bridge Protocol

**Files:**
- Modify: `src/interfaces.rs`
- Create: `src/interfaces/reticulum.rs`
- Create: `src/interfaces/reticulum/protocol.rs`

**Interfaces:**
- Produces: `BridgeCommand::{Start, Send, Shutdown}`.
- Produces: `BridgeEvent::{Ready, Inbound, Delivery, Fatal}`.
- Produces: `BridgeDeliveryOutcome::{Delivered, Retryable, Terminal, OutcomeUnknown}`.
- Produces: `decode_event(line: &[u8]) -> Result<BridgeEvent>` and `encode_command(command: &BridgeCommand) -> Result<Vec<u8>>`.
- Constants: `MAX_PROTOCOL_LINE_BYTES = 1_048_576`, `LXMF_HASH_HEX_CHARS = 64`, `DESTINATION_HASH_HEX_CHARS = 32`, `MAX_TEXT_BYTES = 256 * 1024`.

- [ ] **Step 1: Write protocol tests before types**

Create `protocol.rs` with a test module containing:

```rust
#[test]
fn valid_events_round_trip_and_validate_hashes() -> Result<()> {
    let inbound = br#"{"type":"inbound","message_hash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","source":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","timestamp":42.5,"text":"hello"}"#;
    assert!(matches!(
        decode_event(inbound)?,
        BridgeEvent::Inbound { text, .. } if text == "hello"
    ));
    let command = BridgeCommand::Send {
        delivery_id: "delivery-1".into(),
        destination: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".into(),
        text: "reply".into(),
    };
    let encoded = encode_command(&command)?;
    assert!(encoded.ends_with(b"\n"));
    assert_eq!(encoded.iter().filter(|byte| **byte == b'\n').count(), 1);
    Ok(())
}

#[test]
fn invalid_or_oversized_protocol_objects_fail_closed() {
    for line in [
        br#"{"type":"unknown"}"#.as_slice(),
        br#"{"type":"ready","destination":"xyz"}"#.as_slice(),
        br#"{"type":"inbound","message_hash":"aa","source":"bb","timestamp":1,"text":"x"}"#.as_slice(),
        br#"{"type":"delivery","delivery_id":"","outcome":"delivered"}"#.as_slice(),
    ] {
        assert!(decode_event(line).is_err());
    }
    assert!(decode_event(&vec![b'x'; MAX_PROTOCOL_LINE_BYTES + 1]).is_err());
}
```

- [ ] **Step 2: Verify protocol tests fail**

Run: `rtk cargo test interfaces::reticulum::protocol::tests -- --nocapture`

Expected: compile failure for undefined protocol types/functions.

- [ ] **Step 3: Implement tagged protocol types**

Use Serde tagged enums:

```rust
#[derive(Clone, Debug, Serialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum BridgeCommand {
    Start { state_dir: PathBuf, rns_host: String, rns_port: u16 },
    Send { delivery_id: String, destination: String, text: String },
    Shutdown,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum BridgeEvent {
    Ready { destination: String },
    Inbound { message_hash: String, source: String, timestamp: f64, text: String },
    Delivery { delivery_id: String, outcome: BridgeDeliveryOutcome, retry_after_ms: Option<u64> },
    Fatal { error: String },
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BridgeDeliveryOutcome { Delivered, Retryable, Terminal, OutcomeUnknown }
```

`decode_event` must reject a line before deserialization when larger than `MAX_PROTOCOL_LINE_BYTES`, reject non-finite/negative timestamps, enforce lowercase ASCII hex at exact lengths, reject blank/oversized text, delivery IDs, and fatal strings. `encode_command` applies equivalent outbound validation, serializes once, rejects oversized output, appends exactly one newline.

- [ ] **Step 4: Run protocol tests and formatting**

Run: `rtk cargo test interfaces::reticulum::protocol::tests -- --nocapture`

Run: `rtk cargo fmt --check`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
rtk git add src/interfaces.rs src/interfaces/reticulum.rs src/interfaces/reticulum/protocol.rs
rtk git commit -m "feat(reticulum): define bridge protocol"
```

---

### Task 3: Python RNS/LXMF Bridge and Child Lifecycle

**Files:**
- Create: `src/interfaces/reticulum/bridge.py`
- Create: `src/interfaces/reticulum/bridge.rs`
- Modify: `src/interfaces/reticulum.rs`

**Interfaces:**
- Produces: `BridgeProcess::spawn(config: &ValidatedReticulumConfig, state_dir: &Path) -> Result<Self>`.
- Produces: `BridgeProcess::start(&mut self) -> Result<String>` returning destination hash after a 30-second timeout.
- Produces: `BridgeProcess::send(&mut self, command: &BridgeCommand) -> Result<()>`.
- Produces: `BridgeProcess::next_event(&mut self) -> Result<BridgeEvent>`.
- Produces: `BridgeProcess::shutdown(self) -> Result<()>`, with 5-second grace then kill/reap.
- Produces: `BridgeProcess::take_stderr(&mut self) -> mpsc::Receiver<String>`.

- [ ] **Step 1: Add a deterministic Python self-check first**

Create `bridge.py` with `--self-check`. That path must not import `RNS` or `LXMF`; it feeds representative `start`, `send`, and `shutdown` dictionaries through the bridge's JSON writer/parser and exits zero after printing exactly:

```text
reticulum bridge self-check passed
```

The normal path imports `RNS` and `LXMF`; import failure writes one `fatal` event to stdout and exits nonzero.

- [ ] **Step 2: Add Rust lifecycle tests using temporary executable scripts**

In `bridge.rs`, add tests that create mode-`0700` temporary state directories and executable fake Python scripts. Cover:

```rust
#[tokio::test]
async fn bridge_starts_reads_ready_and_shuts_down() -> Result<()> {
    let fake = fake_bridge("ready_then_deliver").await?;
    let mut bridge = spawn_fake(&fake).await?;
    assert_eq!(bridge.start().await?, DESTINATION);
    bridge.send(&send_command("delivery-1")).await?;
    assert!(matches!(
        bridge.next_event().await?,
        BridgeEvent::Delivery { delivery_id, outcome: BridgeDeliveryOutcome::Delivered, .. }
            if delivery_id == "delivery-1"
    ));
    bridge.shutdown().await
}

#[tokio::test(start_paused = true)]
async fn bridge_readiness_timeout_kills_and_reaps_child() -> Result<()> {
    let fake = fake_bridge("never_ready").await?;
    let mut bridge = spawn_fake(&fake).await?;
    let pid = bridge.child_id().unwrap();
    tokio::time::advance(START_TIMEOUT).await;
    assert!(bridge.start().await.is_err());
    assert!(!process_exists(pid));
    Ok(())
}

#[tokio::test]
async fn bridge_rejects_oversized_stdout_and_bounds_stderr() -> Result<()> {
    let fake = fake_bridge("flood").await?;
    let mut bridge = spawn_fake(&fake).await?;
    let mut stderr = bridge.take_stderr();
    assert!(bridge.start().await.is_err());
    assert!(stderr.recv().await.unwrap().len() <= MAX_STDERR_LINE_BYTES);
    bridge.shutdown().await
}
```

Assert the materialized bridge is a regular owner-only file, the child receives `start` through stdin rather than argv, unknown output fails the session, stderr lines are at most 4 KiB, and shutdown does not leave the child alive.

- [ ] **Step 3: Verify tests fail**

Run: `rtk cargo test interfaces::reticulum::bridge::tests -- --nocapture`

Expected: compile failure for undefined `BridgeProcess`.

- [ ] **Step 4: Implement secure materialization and child I/O**

Embed the script:

```rust
const BRIDGE_SOURCE: &str = include_str!("bridge.py");
```

Write it atomically to `<state_dir>/bridge.py` under a restrictive umask, mode `0600`; reject an existing symlink/non-regular file; replace content only when bytes differ. Spawn with:

```rust
Command::new(&config.python)
    .arg(&bridge_path)
    .stdin(Stdio::piped())
    .stdout(Stdio::piped())
    .stderr(Stdio::piped())
    .kill_on_drop(true)
```

Do not place endpoint or state paths in argv. Use
`BufReader::read_until(b'\n', &mut line)`, stopping as soon as the protocol
limit is exceeded. A dedicated bounded channel reads stderr, strips control
characters except tab, truncates each line to 4096 bytes, and never blocks
child progress.

- [ ] **Step 5: Implement the real Python adapter**

Normal startup must:

```python
identity = RNS.Identity.from_file(identity_path) if os.path.isfile(identity_path) else RNS.Identity()
if not os.path.isfile(identity_path):
    identity.to_file(identity_path)
reticulum = RNS.Reticulum(configdir=rns_dir)
router = LXMF.LXMRouter(storagepath=state_dir)
source = router.register_delivery_identity(identity, display_name="Codrik")
router.register_delivery_callback(on_inbound)
router.announce(source.hash)
emit({"type": "ready", "destination": source.hash.hex()})
```

Do not announce or emit `ready` immediately after constructing `RNS.Reticulum`.
Poll `RNS.Transport.interfaces` for the generated `Codrik TCP Client` interface
with `online == True` for at most 15 seconds. Emit `fatal` and exit nonzero if
it never becomes online. This makes an unreachable configured `rnsd` a startup
failure instead of false readiness.

Call `os.umask(0o077)` before any state write. Create state directories mode
`0700`; reject existing identity/config paths that are symlinks or non-regular
files. Create the identity through a mode-`0600` temporary file plus
`os.replace`. Before `RNS.Reticulum`, atomically write `rns/config` containing
only:

```ini
[reticulum]
  share_instance = No

[interfaces]
  [[Codrik TCP Client]]
    type = TCPClientInterface
    enabled = Yes
    target_host = <validated host>
    target_port = <validated port>
    mode = full
```

The Rust validator permits only ASCII letters, digits, `.`, and `-` in the
host, so interpolation cannot inject ConfigObj syntax. `on_inbound` requires
`message.signature_validated`, `message.content_as_string() is not None`, empty
title, empty fields, exact hash lengths, and emits `message.hash.hex()`,
`message.source_hash.hex()`, timestamp, text.

For `send`, decode the 16-byte destination hash. If no path exists, request it
and wait up to 15 seconds. Recall identity, then construct:

```python
destination = RNS.Destination(
    recipient_identity,
    RNS.Destination.OUT,
    RNS.Destination.SINGLE,
    "lxmf",
    "delivery",
)
```

```python
message = LXMF.LXMessage(
    destination,
    source,
    text,
    desired_method=LXMF.LXMessage.DIRECT,
    include_ticket=True,
)
message.register_delivery_callback(lambda _: delivery(delivery_id, "delivered"))
message.register_failed_callback(lambda failed: delivery(
    delivery_id,
    "terminal" if failed.state == LXMF.LXMessage.REJECTED else "retryable",
))
router.handle_outbound(message)
```

Serialize stdout under one lock because LXMF callbacks run on threads. Reject duplicate active delivery IDs. Invalid destination/text is `terminal`; path/recall absence or `handle_outbound` failure is `retryable`. Process termination leaves Rust to mark accepted unresolved sends `outcome_unknown`.

- [ ] **Step 6: Run bridge checks**

Run: `rtk python3 src/interfaces/reticulum/bridge.py --self-check`

Expected: `reticulum bridge self-check passed`.

Run: `rtk cargo test interfaces::reticulum::bridge::tests -- --nocapture`

Expected: PASS without local `RNS`/`LXMF` installation because Rust tests use fake scripts.

- [ ] **Step 7: Commit**

```bash
rtk git add src/interfaces/reticulum.rs src/interfaces/reticulum/bridge.rs src/interfaces/reticulum/bridge.py
rtk git commit -m "feat(reticulum): supervise LXMF bridge"
```

---

### Task 4: Durable Reticulum Ingress

**Files:**
- Create: `src/interfaces/reticulum/ingress.rs`
- Modify: `src/interfaces/reticulum.rs`

**Interfaces:**
- Consumes: `BridgeEvent::Inbound { message_hash, source, timestamp, text }`.
- Produces: `ReticulumIngressService<S, C>::handle(InboundMessage) -> Result<ReticulumIngressOutcome>`.
- Produces: `InboundMessage { message_hash: String, source: String, timestamp: f64, text: String }`.
- Gateway name: `reticulum:<local_destination_hash>`.
- Identity provider: same gateway name; subject and route address: source destination hash.
- Route limits: `max_text_chars = 262_144`, `max_caption_chars = 1`; files remain unsupported by delivery.

- [ ] **Step 1: Add SQLite-backed ingress tests**

Mirror Telegram's focused tests but construct `InboundMessage` directly. Required cases:

```rust
#[tokio::test]
async fn link_command_links_source_without_agent_work_and_enqueues_reply() -> Result<()> {
    let fixture = ingress_fixture().await?;
    let code = fixture.linking.issue_code(&fixture.actor).await?.code;
    let outcome = fixture.ingress.handle(message("message-1", format!("/link {code}"))).await?;
    assert_eq!(outcome, ReticulumIngressOutcome::CommandHandled);
    assert_eq!(fixture.store.resolve_identity(GATEWAY, SOURCE).await?.unwrap().id, fixture.actor);
    assert_eq!(fixture.claimed_texts().await?, vec!["This channel is now linked."]);
    assert!(!fixture.store.actor_details(&fixture.actor).await?.unwrap().has_active_work);
    Ok(())
}

#[tokio::test]
async fn linked_text_uses_message_hash_for_durable_deduplication() -> Result<()> {
    let fixture = linked_ingress_fixture().await?;
    let inbound = message(MESSAGE_HASH, "hello");
    assert!(matches!(fixture.ingress.handle(inbound.clone()).await?, ReticulumIngressOutcome::Accepted { .. }));
    assert_eq!(fixture.ingress.handle(inbound).await?, ReticulumIngressOutcome::Duplicate);
    assert_eq!(fixture.accepted_event_count().await?, 1);
    Ok(())
}

#[tokio::test]
async fn unlinked_and_disabled_identities_receive_gateway_responses() -> Result<()> {
    let unlinked = ingress_fixture().await?;
    assert_eq!(unlinked.ingress.handle(message("message-1", "hello")).await?, ReticulumIngressOutcome::CommandHandled);
    assert!(unlinked.claimed_texts().await?[0].contains("not linked"));
    let disabled = disabled_linked_ingress_fixture().await?;
    assert_eq!(disabled.ingress.handle(message("message-2", "hello")).await?, ReticulumIngressOutcome::CommandHandled);
    assert_eq!(disabled.claimed_texts().await?, vec!["This actor is disabled."]);
    Ok(())
}
```

Assert `/link` with no argument receives link instructions. Assert `/link CODE extra` is passed as one invalid code, not accepted as normal text. Assert route gateway/address and message hash exactly match Reticulum values.

- [ ] **Step 2: Verify ingress tests fail**

Run: `rtk cargo test interfaces::reticulum::ingress::tests -- --nocapture`

Expected: compile failure for undefined ingress service.

- [ ] **Step 3: Implement minimal ingress service**

Follow `TelegramIngressService` without Telegram-specific update classification. Parse only a trimmed first token equal to `/link`; preserve normal message text except rejecting blank text. Build:

```rust
let gateway = format!("reticulum:{}", self.local_destination);
let identity = LinkIdentity {
    provider: gateway.clone(),
    subject: message.source.clone(),
    username: None,
};
let route = DeliveryRoute::new(gateway.clone(), message.source, None, MAX_TEXT_BYTES, 1)?;
```

Use `GatewayCommandKey { gateway, external_id: message.message_hash.clone() }` for link idempotency. Use `NewInboundEvent::text_with_route` with `Audience::ActorPrivate` for normal text. Reuse the exact existing English responses for linked, already linked, invalid/expired, rate-limited, conflict, unlinked, and disabled states.

- [ ] **Step 4: Run focused ingress tests**

Run: `rtk cargo test interfaces::reticulum::ingress::tests -- --nocapture`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
rtk git add src/interfaces/reticulum.rs src/interfaces/reticulum/ingress.rs
rtk git commit -m "feat(reticulum): ingest linked LXMF text"
```

---

### Task 5: Durable Delivery and Unified Gateway Loop

**Files:**
- Modify: `src/interfaces/reticulum.rs`
- Modify: `src/interfaces/reticulum/bridge.rs`

**Interfaces:**
- Produces: `prepare<C>(config: ValidatedReticulumConfig, store: SqliteRuntimeStore, linking: Arc<dyn IdentityLinkManager>, signals: ActorSignals, clock: C, state_dir: PathBuf) -> Result<PreparedReticulumGateway<SqliteRuntimeStore, C>> where C: Clock`.
- Produces: `PreparedReticulumGateway::destination() -> &str`.
- Produces: `PreparedReticulumGateway::run(self: Arc<Self>, shutdown: watch::Receiver<bool>) -> Result<()>`.
- Uses existing `GatewayDeliveryStore` claim/renew/complete/retry/fail methods; no schema changes.

- [ ] **Step 1: Write gateway delivery tests with fake bridge I/O**

Add a test-only `BridgeProcess::from_child` or `spawn_with_source` seam, not a production transport trait. Cover:

```rust
#[tokio::test]
async fn delivered_event_completes_claim() -> Result<()> {
    let fixture = delivery_fixture("delivered").await?;
    fixture.gateway.run_once().await?;
    assert_eq!(fixture.delivery_state().await?, GatewayDeliveryState::Delivered);
    Ok(())
}

#[tokio::test]
async fn retryable_terminal_and_unknown_events_map_to_durable_states() -> Result<()> {
    for (outcome, expected) in [
        ("retryable", GatewayDeliveryState::FailedRetryable),
        ("terminal", GatewayDeliveryState::FailedTerminal),
        ("outcome_unknown", GatewayDeliveryState::OutcomeUnknown),
    ] {
        let fixture = delivery_fixture(outcome).await?;
        fixture.gateway.run_once().await?;
        assert_eq!(fixture.delivery_state().await?, expected);
    }
    Ok(())
}

#[tokio::test]
async fn bridge_exit_marks_only_accepted_unresolved_sends_unknown() -> Result<()> {
    let fixture = two_delivery_fixture("exit_after_first_send").await?;
    assert!(fixture.gateway.run_once().await.is_err());
    assert_eq!(fixture.delivery_states().await?, vec![GatewayDeliveryState::OutcomeUnknown, GatewayDeliveryState::Pending]);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn active_delivery_claim_is_renewed_until_bridge_result() -> Result<()> {
    let fixture = delivery_fixture("delayed_delivery").await?;
    let run = tokio::spawn(fixture.gateway.clone().run_once());
    tokio::time::advance(Duration::from_secs(31)).await;
    fixture.bridge.release_delivery();
    run.await??;
    assert_eq!(fixture.delivery_state().await?, GatewayDeliveryState::Delivered);
    Ok(())
}

#[tokio::test]
async fn file_payload_fails_terminal_without_reaching_bridge() -> Result<()> {
    let fixture = file_delivery_fixture().await?;
    fixture.gateway.run_once().await?;
    assert_eq!(fixture.delivery_state().await?, GatewayDeliveryState::FailedTerminal);
    assert_eq!(fixture.bridge.send_count(), 0);
    Ok(())
}
```

- [ ] **Step 2: Verify gateway tests fail**

Run: `rtk cargo test interfaces::reticulum::tests -- --nocapture`

Expected: compile failure for undefined prepared gateway/loop.

- [ ] **Step 3: Implement one supervised coordinator**

`prepare` validates the private state directory, spawns and starts `BridgeProcess`, builds `ReticulumIngressService`, and returns only after `ready`. `run` owns:

- one 500 ms delivery poll interval;
- bridge events;
- a 10-second claim renewal interval;
- shutdown signal;
- child exit monitoring;
- `HashMap<GatewayDeliveryId, GatewayDeliveryClaim>` for accepted unresolved sends.

Claim up to 32 deliveries for `reticulum:<destination>`. Process sequentially because one bridge stream owns ordering. Before writing `send`, call `set_gateway_delivery_retry_safe(claim, false, now)`; only then insert it into unresolved. If writing fails after that transition, fail the claim as `OutcomeUnknown`. A payload other than `OutboxPayload::Text` becomes `FailedTerminal` before bridge submission.

Map events exactly:

```rust
BridgeDeliveryOutcome::Delivered => complete_gateway_delivery(&claim, None, now)
BridgeDeliveryOutcome::Retryable => retry_gateway_delivery(
    &claim,
    retry_at(now, delivery.attempt_count, retry_after_ms),
    "reticulum_retryable",
    "LXMF delivery failed retryably",
    now,
)
BridgeDeliveryOutcome::Terminal => fail_gateway_delivery(
    &claim,
    GatewayDeliveryState::FailedTerminal,
    "reticulum_terminal",
    "LXMF delivery was rejected",
    now,
)
BridgeDeliveryOutcome::OutcomeUnknown => fail_gateway_delivery(
    &claim,
    GatewayDeliveryState::OutcomeUnknown,
    "reticulum_outcome_unknown",
    "LXMF delivery outcome is unknown",
    now,
)
```

Use supplied `retry_after_ms`; otherwise use bounded delays `1, 2, 4, 8, 16, 30` seconds from attempt count. Reject unsolicited or duplicate delivery IDs as protocol errors. On child EOF/exit, transition every unresolved claim to `OutcomeUnknown`, then return the bridge error so the outer fail-fast supervisor stops all runtime components.

- [ ] **Step 4: Implement shutdown fencing**

On shutdown, stop claiming first. Mark unresolved claims `OutcomeUnknown`, send `shutdown`, wait 5 seconds, kill/reap if needed. Claims never submitted remain pending/retryable in SQLite. Ensure every return path reaps the child.

- [ ] **Step 5: Run delivery tests**

Run: `rtk cargo test interfaces::reticulum -- --nocapture`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
rtk git add src/interfaces/reticulum.rs src/interfaces/reticulum/bridge.rs
rtk git commit -m "feat(reticulum): deliver durable LXMF replies"
```

---

### Task 6: Runtime Composition and Observability

**Files:**
- Modify: `src/app.rs:1-31,184-297,313-430`
- Modify: `src/runtime/observability.rs:10-129,175-293`
- Modify: `tests/serve_runtime.rs`

**Interfaces:**
- Consumes: `config.reticulum`, `paths.reticulum`, `reticulum::prepare`, `PreparedReticulumGateway::run`.
- Produces: supervised component name `reticulum`.
- Produces: startup JSON field `reticulum_destination` containing only the public destination hash.

- [ ] **Step 1: Add observability and process-level failing tests**

Extend observability tests to require:

```rust
event.reticulum_destination = Some("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".into());
assert_eq!(json["reticulum_destination"], "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
```

Add `serve_runtime.rs` cases:

- configuration omitted: runtime starts without spawning Python;
- configured fake Python emits `ready`: runtime reaches ready and logs destination;
- fake Python emits `fatal`: startup exits nonzero with Reticulum context;
- fake Python exits after readiness: fail-fast supervisor shuts runtime down;
- endpoint/state values appear in fake child's stdin capture and not process arguments;
- SIGTERM causes fake child shutdown/reap.

- [ ] **Step 2: Verify composition tests fail**

Run: `rtk cargo test runtime::observability::tests -- --nocapture`

Run: `rtk cargo test --test serve_runtime reticulum -- --nocapture`

Expected: failures because Reticulum is not composed or logged.

- [ ] **Step 3: Compose preparation before runtime readiness**

At startup:

```rust
let reticulum_config = config
    .reticulum
    .as_ref()
    .map(crate::config::ReticulumConfig::validate)
    .transpose()?;
```

After durable recovery/path validation, prepare the optional gateway with the shared SQLite store, identity linker, actor signals, clock, and `paths.reticulum`. Add its public destination to the startup event. Never log identity bytes, source hashes, message text, endpoint credentials, protocol lines, or `/link` content.

- [ ] **Step 4: Register one fail-fast component**

Add only:

```rust
if let Some(reticulum) = reticulum {
    service.component("reticulum", {
        let shutdown = shutdown_rx.clone();
        async move { reticulum.run(shutdown).await }
    });
}
```

Do not add independent ingress/delivery components or a restart loop. Add `RuntimeComponent::Reticulum` and `RuntimeLogEvent.reticulum_destination`; initialize it to `None` in the constructor.

- [ ] **Step 5: Run runtime tests**

Run: `rtk cargo test runtime::observability::tests -- --nocapture`

Run: `rtk cargo test --test serve_runtime reticulum -- --nocapture`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
rtk git add src/app.rs src/runtime/observability.rs tests/serve_runtime.rs
rtk git commit -m "feat(runtime): compose Reticulum gateway"
```

---

### Task 7: Operator Documentation and End-to-End Verification

**Files:**
- Modify: `README.md:41-83,125-148,179-320`
- Modify: `tests/install_script.rs` only if packaging assertions require the embedded bridge source to be documented; do not install a separate script.

**Interfaces:**
- Documents: Python environment, TCP endpoint, stable destination, linking, text-only scope, failures, manual test.
- Verifies: full repository quality gates.

- [ ] **Step 1: Add README assertions before documentation**

In the existing README test area of `tests/install_script.rs`, assert the documentation contains:

```rust
for required in [
    "reticulum:",
    "rns_address: \"127.0.0.1:4242\"",
    "python: \"/absolute/path/to/venv/bin/python3\"",
    "python3 -m venv",
    "python -m pip install lxmf",
    "/link CODE",
    "<CODRIK_HOME>/reticulum/identity",
] {
    assert!(readme.contains(required), "README missing {required}");
}
```

- [ ] **Step 2: Verify documentation test fails**

Run: `rtk cargo test --test install_script readme -- --nocapture`

Expected: FAIL on missing Reticulum documentation.

- [ ] **Step 3: Document exact setup and behavior**

Add this minimal environment setup:

```bash
python3 -m venv ~/.codrik/reticulum-venv
~/.codrik/reticulum-venv/bin/python -m pip install lxmf
```

Add configuration:

```yaml
reticulum:
  rns_address: "127.0.0.1:4242"
  python: "/absolute/path/to/venv/bin/python3"
```

State that `rns_address` targets the existing `TCPServerInterface`, not the Reticulum shared-instance port. Explain startup prints `reticulum_destination`, the stable identity location, `codrik link` then `/link CODE`, direct text-only scope, no propagation-node fetching, no attachments, no automatic package installation, and fail-fast service-manager restart behavior.

Document troubleshooting for `ModuleNotFoundError: RNS/LXMF`, refused TCP connection, no path to peer, invalid state permissions, rejected/unknown delivery, and bridge exit.

- [ ] **Step 4: Document the manual integration check**

Add the sequence:

1. Start `rnsd` with `TCPServerInterface` on port `4242`.
2. Start `codrik serve`; record `reticulum_destination`.
3. Send `/link CODE` from Sideband, MeshChat, or another LXMF client.
4. Send text; verify one agent reply.
5. Restart Codrik; verify the destination is unchanged.
6. Replay the same LXMF message; verify no duplicate agent work.

- [ ] **Step 5: Run all verification gates**

Run: `rtk python3 src/interfaces/reticulum/bridge.py --self-check`

Run: `rtk cargo fmt --check`

Run: `rtk cargo check`

Run: `rtk cargo test`

Run: `rtk cargo clippy --all-targets --all-features`

Expected: self-check prints success; every Rust command exits zero without warnings.

- [ ] **Step 6: Commit documentation**

```bash
rtk git add README.md tests/install_script.rs
rtk git commit -m "docs(reticulum): document LXMF gateway"
```

---

## Manual Acceptance Transcript

Record these values during the live-network check:

```text
rnsd endpoint: <host>:4242
first reticulum_destination: <32 lowercase hex characters>
link response: This channel is now linked.
inbound text: hello over LXMF
agent reply received: yes
post-restart reticulum_destination matches: yes
duplicate work after replay: no
```

Do not record identity private bytes, link codes, message contents beyond the synthetic test text, or real user destination hashes in committed files.
