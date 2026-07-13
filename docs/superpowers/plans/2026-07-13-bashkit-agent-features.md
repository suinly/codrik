# Bashkit Agent Features Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Enable Bashkit's embedded HTTP, jq, and local Git capabilities for Codrik while preserving its actor workspace and process limits.

**Architecture:** Activate `http_client`, `jq`, and `git` alongside the existing `realfs` dependency feature. Configure each Bashkit runtime with an outbound-public network allowlist that retains private-IP blocking and with a fixed local Git identity; keep deterministic capability tests in the Bashkit adapter and document the security boundary.

**Tech Stack:** Rust 2024, Bashkit 0.12, Tokio TCP test server, Cargo feature resolution.

## Global Constraints

- Enabled features are exactly `realfs`, `http_client`, `jq`, and `git`, plus Bashkit's implicit default `bash_tool`.
- Do not enable `bot-auth`, `failpoints`, `interop`, `logging`, `scripted_tool`, `sqlite`, `ssh`, or `typescript`.
- Outbound HTTP(S) is allowed, but Bashkit's private/reserved-IP blocking remains enabled in production.
- Existing actor workspace mounts, allowed host paths, timeouts, and output caps remain unchanged.
- Git operations are local to Bashkit's virtual or mounted filesystems; remote Git operations are not promised.
- HTTP tests use a local in-process server and explicitly relax private-IP blocking only inside the test.

---

### Task 1: Activate and Configure Agent Capabilities

**Files:**
- Modify: `Cargo.toml`
- Modify: `Cargo.lock`
- Modify: `src/tools/bashkit.rs`
- Modify: `README.md`

**Interfaces:**
- Consumes: `bashkit::NetworkAllowlist` and `bashkit::GitConfig` enabled by dependency features.
- Produces: `configure_agent_capabilities(builder: bashkit::BashBuilder) -> bashkit::BashBuilder`.
- Preserves: `bashkit_tool(cwd, max_output_bytes, workspace) -> Result<BashkitRuntimeTool>` and the public Codrik tool schema.

- [ ] **Step 1: Add failing adapter tests for jq and local Git**

Add these tests to `src/tools/bashkit.rs`:

```rust
#[tokio::test]
async fn processes_json_with_embedded_jq() {
    let result = BashkitTool::new(BashkitToolConfig::default())
        .execute(r#"{"command":"printf '{\"name\":\"codrik\"}' | jq -r '.name'"}"#)
        .await
        .expect("jq command should execute");
    let result: Value = serde_json::from_str(&result).expect("result should be valid json");

    assert_eq!(result["exit_code"], 0);
    assert_eq!(result["stdout"], "codrik\n");
}

#[tokio::test]
async fn initializes_repository_with_embedded_git() {
    let result = BashkitTool::new(BashkitToolConfig::default())
        .execute(r#"{"command":"git init /repo"}"#)
        .await
        .expect("git command should execute");
    let result: Value = serde_json::from_str(&result).expect("result should be valid json");

    assert_eq!(result["exit_code"], 0);
    assert!(
        result["stdout"]
            .as_str()
            .expect("stdout should be a string")
            .contains("Initialized empty Git repository")
    );
}
```

- [ ] **Step 2: Run capability tests and verify RED**

Run: `rtk cargo test embedded_`

Expected: tests fail because `jq` and configured local Git are unavailable with the current dependency features/runtime builder.

- [ ] **Step 3: Enable the selected dependency features**

Change `Cargo.toml` to:

```toml
bashkit = { version = "0.12.0", features = ["realfs", "http_client", "jq", "git"] }
```

Run: `rtk cargo check`

Expected: Cargo resolves the new optional dependencies and updates `Cargo.lock`; the crate compiles after runtime configuration is added in the next step if `GitConfig` is otherwise unused.

- [ ] **Step 4: Configure public HTTP and local Git in every Bashkit runtime**

Extend the imports and add a focused builder function:

```rust
use bashkit::{
    BashBuilder, BashTool as BashkitRuntimeTool, ExecutionLimits, GitConfig, NetworkAllowlist,
    Tool as BashkitToolContract,
};

fn configure_agent_capabilities(builder: BashBuilder) -> BashBuilder {
    builder
        .network(NetworkAllowlist::allow_all())
        .git(GitConfig::new().author("Codrik Agent", "agent@codrik.local"))
}
```

Apply it before the existing optional workspace configuration:

```rust
let mut builder = BashkitRuntimeTool::builder()
    .username("agent")
    .hostname("sandbox")
    .limits(limits)
    .configure(configure_agent_capabilities);
```

`NetworkAllowlist::allow_all()` retains Bashkit's default `block_private_ips = true`; do not call `block_private_ips(false)` in production.

- [ ] **Step 5: Run jq and Git tests and verify GREEN**

Run: `rtk cargo test embedded_`

Expected: both tests pass.

- [ ] **Step 6: Add a deterministic local HTTP test**

Import Tokio I/O and networking in the test module and add:

```rust
#[tokio::test]
async fn http_client_fetches_from_local_test_server() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("test server should bind");
    let address = listener.local_addr().expect("server address");
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("request should connect");
        let mut request = [0_u8; 1024];
        let _ = socket.read(&mut request).await.expect("request should read");
        socket
            .write_all(
                b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\nConnection: close\r\n\r\ncodrik",
            )
            .await
            .expect("response should write");
    });
    let url = format!("http://{address}/value");
    let mut bash = bashkit::Bash::builder()
        .network(
            NetworkAllowlist::new()
                .allow(format!("http://{address}"))
                .block_private_ips(false),
        )
        .build();

    let result = bash
        .exec(&format!("curl -s {url}"))
        .await
        .expect("curl should execute");
    server.await.expect("test server should finish");

    assert_eq!(result.exit_code, 0);
    assert_eq!(result.stdout, "codrik");
}
```

- [ ] **Step 7: Run the HTTP test and verify GREEN**

Run: `rtk cargo test http_client_fetches_from_local_test_server`

Expected: one test passes without public internet access.

- [ ] **Step 8: Document built-ins and network security**

After the Bashkit/real-bash explanation in `README.md`, add:

```markdown
Codrik builds Bashkit with its embedded HTTP client, `jq`, and local Git
support. Bashkit `curl` and `wget` may access public HTTP(S) destinations;
private and reserved IP ranges remain blocked to reduce SSRF risk. Git remote
operations are not supported by Bashkit's virtual Git implementation.
```

- [ ] **Step 9: Verify resolved features**

Run: `rtk cargo tree -e features -i bashkit`

Expected output includes `bash_tool`, `default`, `git`, `http_client`, `jq`, and `realfs`, and does not include the excluded optional features.

- [ ] **Step 10: Run focused adapter tests**

Run: `rtk cargo test tools::bashkit::tests`

Expected: all Bashkit adapter tests pass, including workspace, timeout, output cap, jq, Git, and local HTTP tests.

- [ ] **Step 11: Commit the feature implementation**

```bash
rtk git add Cargo.toml Cargo.lock src/tools/bashkit.rs README.md
rtk git commit -m "feat(tools): enable Bashkit agent capabilities"
```

---

### Task 2: Full Verification

**Files:**
- Verify only; no expected code changes.

**Interfaces:**
- Consumes: completed Task 1 feature and runtime configuration.
- Produces: fresh verification evidence for handoff.

- [ ] **Step 1: Run the complete test suite**

Run: `rtk cargo test`

Expected: all non-ignored tests pass.

- [ ] **Step 2: Run build and static checks**

Run: `rtk cargo check`

Expected: exit code 0 with no new warnings.

Run: `rtk cargo fmt --check`

Expected: exit code 0.

Run: `rtk cargo clippy --all-targets --all-features`

Expected: exit code 0 with no new warnings.

Run: `rtk git diff --check`

Expected: exit code 0.

- [ ] **Step 3: Inspect final repository state**

Run: `rtk git status --short --branch`

Expected: clean working tree on `main`, ahead of `origin/main` by the new commits.
