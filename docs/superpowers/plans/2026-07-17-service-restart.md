# Current Service Restart Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make self-update restart only the current Codrik runtime service and remove automatic legacy service cleanup from the installer.

**Architecture:** Reuse the existing platform-specific updater and installer paths, changing only their service names and legacy cleanup statements. No new abstraction or dependency is needed.

**Tech Stack:** Rust 2024, POSIX shell, systemd user services, macOS launchd, built-in Rust tests.

## Global Constraints

- Linux current service: `codrik.service`.
- macOS current LaunchAgent: `com.suinly.codrik`.
- Codrik must not detect, restart, stop, or delete legacy Telegram-specific services.
- Existing no-op behavior remains when the current service is not running or the platform is unsupported.

---

### Task 1: Use Only Current Runtime Services

**Files:**
- Modify: `src/updater.rs`
- Modify: `scripts/install.sh`
- Test: `src/updater.rs`
- Test: `tests/install_script.rs`

**Interfaces:**
- Consumes: existing `restart_gateway_service_if_running()`, platform-specific restart functions, and installer service setup functions.
- Produces: unchanged CLI and installer interfaces with corrected service targets.

- [ ] **Step 1: Write failing service-name tests**

Add this updater unit test and import both constants into the test module:

```rust
#[test]
fn uses_current_foreground_service_names() {
    assert_eq!(SERVICE, "codrik.service");
    assert_eq!(LAUNCHD_LABEL, "com.suinly.codrik");
}
```

Extend `polling_gateway_installation_is_removed` in `tests/install_script.rs`:

```rust
for removed in [
    "Configure a gateway?",
    "Gateway service to run",
    "Telegram bot token",
    "install_gateway_service",
    "gateway telegram",
    "codrik-telegram.service",
    "com.suinly.codrik.telegram",
] {
```

- [ ] **Step 2: Run focused tests and verify RED**

Run:

```sh
rtk cargo test uses_current_foreground_service_names -- --nocapture
rtk cargo test --test install_script polling_gateway_installation_is_removed -- --nocapture
```

Expected: the updater test fails because the constants still name the legacy services; the installer test fails because the installer still contains both legacy names.

- [ ] **Step 3: Update the updater service targets**

Replace the legacy constants:

```rust
const SERVICE: &str = "codrik.service";
const LAUNCHD_LABEL: &str = "com.suinly.codrik";
```

Use `SERVICE` and `LAUNCHD_LABEL` in the existing `systemctl` and `launchctl` commands. Rename `restart_gateway_service_if_running` to `restart_service_if_running`, and change Telegram-specific error messages to `service` or `LaunchAgent` without changing control flow.

- [ ] **Step 4: Remove installer legacy cleanup**

Delete only these behaviors from `scripts/install.sh`:

- the Linux `disable --now codrik-telegram.service` and legacy unit deletion;
- the macOS legacy plist variable, `bootout`, and deletion;
- legacy service checks from `capture_install_state`.

Keep current service creation, reload/bootstrap, enable, and restart behavior unchanged.

- [ ] **Step 5: Run focused tests and verify GREEN**

Run:

```sh
rtk cargo test uses_current_foreground_service_names -- --nocapture
rtk cargo test --test install_script -- --nocapture
```

Expected: both commands pass.

- [ ] **Step 6: Run full verification**

Run:

```sh
rtk cargo fmt --check
rtk cargo check
rtk proxy cargo test -- --test-threads=1
rtk cargo clippy --all-targets --all-features
rtk git diff --check
```

Expected: formatting, check, sequential tests, and diff check pass; clippy has no errors.

- [ ] **Step 7: Commit**

```sh
rtk git add src/updater.rs scripts/install.sh tests/install_script.rs
rtk git commit -m "fix(service): restart current runtime service"
```
