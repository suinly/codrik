# Reticulum Native Delivery Status Design

## Goal

Remove Codrik's separate `Думаю...` LXMF message. Reticulum clients already
show native LXMF delivery state, so the extra message consumes airtime without
adding necessary information.

## Changes

- Delete the Reticulum activity worker and its tests.
- Remove the `reticulum-activity` supervised component.
- Stop subscribing Reticulum to `GatewayActivityHub`.
- Remove the special one-attempt retry policy for thinking-message deliveries.
- Remove operator documentation for `Думаю...`.

## Preserved Behavior

- Final Reticulum assistant replies remain limited to 500 Unicode characters.
- The Reticulum-only transient brevity instruction remains active.
- Final replies continue through the existing durable gateway delivery path.
- Native LXMF delivery status remains unchanged.
- Telegram activity behavior and CLI output remain unchanged.

## Testing

- Reticulum response-budget tests continue to pass.
- Reticulum gateway tests pass without activity-specific cases.
- Runtime composition tests confirm Telegram still enables gateway activity and
  Reticulum alone no longer requires it.
- Full formatting, compilation, test, clippy, and bridge self-check gates pass.
