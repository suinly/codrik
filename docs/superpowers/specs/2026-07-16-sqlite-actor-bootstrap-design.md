# SQLite Actor Bootstrap Design

## Goal

Make SQLite the only source of actor authorization and allow a clean
`codrik serve` installation to start without `users.json`. The first actor is
created automatically from `runtime.actor_id`; later configuration mistakes
must not silently create additional privileged actors.

## Scope

This change:

- automatically creates the initial runtime actor in an empty database;
- removes `users.json` and its one-time import path;
- removes installer behavior and hidden CLI commands that inspect legacy
  authorization;
- keeps identities in SQLite for future gateway linking;
- updates tests and README documentation for the new bootstrap behavior.

There are no deployed installations that require `users.json` or an existing
runtime database. Compatibility migration for an old SQLite database is
intentionally out of scope. The local development database must be deleted once
after this change.

## Startup Flow

`codrik serve` performs startup in this order:

1. Load and validate `config.yml`.
2. Resolve and validate runtime paths.
3. Acquire the exclusive runtime instance lock.
4. Open SQLite and apply the current schema.
5. Atomically ensure the initial actor.
6. Load and validate the configured actor.
7. Start IPC, dispatch, outbox, artifact, and garbage-collection services.

The bootstrap operation receives the configured `runtime.actor_id`, the
initial tool authorization `["*"]`, and the current timestamp.

Within one SQLite transaction it counts the actors:

- if the table is empty, it inserts an enabled actor using the configured ID
  and returns `Created`;
- if any actor already exists, it performs no write and returns
  `AlreadyInitialized`.

After bootstrap, startup uses the existing actor lookup:

- the configured actor exists and is enabled: continue;
- actors exist but the configured ID is absent: fail without creating it;
- the configured actor is disabled: fail without changing it.

The SQLite transaction makes bootstrap deterministic even if it is reused
outside the current single-instance startup path. The runtime instance lock
still prevents concurrent `serve` processes.

## Actor Defaults and Validation

The initial actor uses:

- ID: the trimmed `runtime.actor_id`;
- enabled: `true`;
- tools: `["*"]`;
- no identities;
- creation time: the injected runtime clock.

The actor ID must pass the same safety constraints used for actor workspace
paths. Blank IDs, `.`, `..`, forward slashes, and backslashes are rejected
before the database write.

The wildcard retains its existing meaning: it authorizes standard tools and
does not implicitly grant separately classified privileged capabilities.

There is no prompt or confirmation. The same behavior works in an interactive
terminal, under systemd, and under launchd.

## Runtime Store Boundaries

The runtime store owns actor persistence through a focused actor API:

- atomically bootstrap an actor only when the actors table is empty;
- load an actor by ID;
- resolve an identity to its actor.

The legacy authorization API is removed:

- `AuthorizationStore`;
- `LegacyAuthorizationSnapshot`;
- `LegacyActor`;
- `LegacyIdentity`;
- legacy import marker inspection;
- one-time legacy snapshot import.

Actor and identity domain types required by gateways belong to the runtime
domain rather than a JSON-backed authorization module.

## Schema

SQLite remains the single source of truth for:

- actors;
- actor tool authorization;
- enabled state;
- gateway identities.

The `runtime_metadata` table and `legacy_auth_imported` key are removed from
the base schema because they have no remaining purpose.

No schema migration for an existing database is added. The supported upgrade
procedure for the current development environment is to stop Codrik and remove
`runtime.sqlite` together with its `-wal` and `-shm` files before the first
run of this version.

## Installer and CLI

The installer no longer:

- creates or reads `users.json`;
- asks the user to select an existing authorization actor;
- validates an actor against `users.json`;
- treats authorization files as user-owned upgrade state.

For a newly generated configuration, the installer writes:

```yaml
runtime:
  actor_id: actor:local:owner
```

The actor itself is created by the first `codrik serve` startup.

For an existing configuration that already contains a nonblank
`runtime.actor_id`, the installer preserves that value. Existing installer
logic unrelated to actor authorization remains unchanged.

The hidden CLI commands used only by the installer to inspect `users.json` are
removed.

## Error Handling

Startup errors remain explicit:

- a missing runtime section asks the user to add `runtime.actor_id`;
- a blank actor ID is rejected during configuration validation;
- an unsafe actor ID is rejected before database mutation;
- a configured actor absent from a nonempty database reports that the selected
  actor does not exist;
- a configured disabled actor reports that it is disabled;
- SQLite failures include operation context and leave bootstrap atomic.

The missing-actor error must no longer instruct the user to authorize an actor
through `users.json`.

## Testing

Focused store tests prove:

- an empty actors table creates the configured initial actor;
- the created actor is enabled and has exactly `["*"]`;
- the injected timestamp is persisted;
- repeated bootstrap is idempotent;
- a nonempty database never receives another actor through bootstrap;
- an invalid actor ID causes no database mutation.

Startup tests prove:

- clean `serve` startup creates and selects the configured actor;
- startup succeeds on subsequent runs with that actor;
- startup fails when a different actor already exists;
- startup fails for a disabled configured actor.

Installer and CLI tests prove:

- a clean installation does not create or reference `users.json`;
- generated configuration uses `actor:local:owner`;
- hidden legacy authorization commands are unsupported.

Existing runtime tests replace `LegacyAuthorizationSnapshot` fixtures with the
new actor store bootstrap or focused test helpers built on the same runtime
domain types.

README checks ensure that configuration and installation documentation describe
automatic SQLite bootstrap and contain no `users.json` or legacy-import
instructions.

## Acceptance Criteria

- `codrik serve` starts against an empty runtime directory using only
  `config.yml`.
- The configured actor is created once with standard-tool wildcard access.
- A typo in `runtime.actor_id` cannot create a second privileged actor.
- Runtime authorization has no JSON-backed source of truth.
- The installer and README do not mention `users.json`.
- All Rust tests, formatting checks, Clippy checks, and installer tests pass.
