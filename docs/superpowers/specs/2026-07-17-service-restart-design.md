# Current Service Restart Design

## Goal

Make installation and self-update operate only on the current foreground
runtime service. Legacy Telegram-specific services are removed manually by the
operator rather than detected, restarted, or deleted by Codrik.

## Behavior

- On Linux, `codrik update` checks and restarts `codrik.service` only when that
  user service is already active.
- On macOS, `codrik update` checks and restarts `com.suinly.codrik` only when
  that LaunchAgent is already loaded.
- The installer writes and starts only those current service definitions.
- The installer does not inspect, stop, or delete `codrik-telegram.service` or
  `com.suinly.codrik.telegram`.
- Unsupported platforms and installations without a running current service
  retain the existing no-op behavior.

## Verification

Focused tests assert that updater and installer sources use the current names
and contain no legacy Telegram service names. Existing updater checksum,
installer, and full crate tests remain green.
