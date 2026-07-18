# Actor Administration and Multi-Actor Runtime Design

## Goal

Add a supported local CLI for managing actors and their tool permissions, and
make `codrik serve` process work for every enabled actor. Administration must
not require direct SQLite edits or a daemon restart.

## Scope

The feature adds:

- actor create, list, show, enable, disable, and delete commands;
- actor tool list, grant, and revoke commands;
- actor-specific link-code issuance;
- one dispatcher per enabled actor;
- hot actor and permission changes through the existing local IPC server.

It does not add roles, permission groups, remote administration, declarative
actor synchronization from YAML, or administrative agent tools.

## Authority and Defaults

SQLite remains the single source of truth for actors and tool grants. All
administrative mutations go through the running daemon and its `ActorStore`;
the CLI never opens the runtime database directly.

The existing private Unix socket and peer-owner validation are the
administrative trust boundary. Actor administration is not exposed through
Telegram, gateways, or the agent's tool registry.

`runtime.actor_id` remains the default actor for local prompts and for
`codrik link` without an actor argument. The configured actor cannot be
disabled or deleted while it is the configured default. The user must first
change `runtime.actor_id` and restart Codrik.

A newly created actor is enabled and has `tools: []`. The initial empty-store
bootstrap remains backward compatible and creates `runtime.actor_id` with
`tools: ["*"]`.

## CLI

```text
codrik actors list
codrik actors show <actor-id>
codrik actors create <actor-id>
codrik actors enable <actor-id>
codrik actors disable <actor-id>
codrik actors delete <actor-id>
codrik actors delete <actor-id> --force

codrik actors tools list <actor-id>
codrik actors tools grant <actor-id> <tool>
codrik actors tools revoke <actor-id> <tool>

codrik link [actor-id]
```

Actor and tool lists use stable lexical ordering. `show` returns the actor's
enabled state, tool grants, linked identities, and whether active work exists.
The first version uses concise human-readable output and does not add a JSON
output mode.

Create, enable, disable, grant, and revoke are idempotent. Creating an actor
that already exists returns its current state without resetting permissions.
Grant and revoke accept `"*"` or a registered tool name and reject unknown
names. `"*"` continues to grant standard tools only; privileged `bash`
requires an explicit `bash` grant.

`codrik link <actor-id>` issues a one-time code for that actor. Omitting the
argument preserves the current `runtime.actor_id` behavior. Missing or
disabled target actors are rejected.

## IPC and Store Boundaries

The strict IPC protocol gains typed request and response variants for actor
queries and mutations. Each mutation returns the resulting actor state or a
typed rejection; the CLI does not infer whether a mutation succeeded.

Actor persistence methods live on the actor store boundary. SQLite performs
each logical mutation in one transaction. Actor IDs use the existing
`ActorId::parse_workspace_safe` validation. Tool names are validated against
the application's registered tool names before persistence.

Administrative mutations notify the runtime after commit. The notification
is an optimization for prompt application; the runtime also reconciles actor
state periodically so a lost notification cannot leave it stale.

## Multi-Actor Runtime

`codrik serve` maintains one dispatcher task per enabled actor. A manager
reconciles the task set with the actor table:

- an enabled new actor gets a dispatcher;
- enabling a disabled actor starts its dispatcher;
- disabling an actor prevents another quantum from starting and removes its
  dispatcher after the current quantum finishes;
- deleting an actor removes its dispatcher;
- an unexpected dispatcher failure remains a supervised runtime failure.

Separate dispatcher tasks prevent a long model or tool call for one actor
from blocking another actor.

The LLM client and immutable runtime services are shared. Actor-specific
workspace paths and tool registries are built from the actor state at the
start of each quantum. Therefore a permission change applies to the next
quantum, while an already running quantum retains the tool set with which it
started. System instructions continue to describe the effective workspace
and tools for that actor.

Telegram ingress already resolves a linked identity to an actor ID. With all
enabled actors dispatched, no Telegram transport change is required beyond
rejecting disabled actors consistently at ingress.

## Disable Semantics

Disabling an actor is non-destructive. It prevents new local or gateway
ingress and prevents the dispatcher from starting another quantum. An active
quantum is allowed to finish; disable does not cancel tools or model calls.
Existing history, identities, deliveries, and files remain intact. Enabling
the actor resumes durable pending work.

The configured `runtime.actor_id` is protected from disable to preserve local
administrative access.

## Delete Semantics

`codrik actors delete <actor-id>` succeeds only when the actor has no linked
identities, history, work, memory, deliveries, or artifacts. Otherwise it
returns a concise error directing the user to disable the actor and use
`--force` if permanent deletion is intended.

`--force` is irreversible and requires all of the following:

- the actor is disabled;
- the actor is not `runtime.actor_id`;
- no actor lease or active run exists;
- no delivery for the actor is pending, claimed, retryable, or of unknown
  outcome.

Force deletion purges actor-owned database state in one transaction. Existing
append-only guards receive a narrowly scoped exception only while the target
actor has an actor-deletion marker in that same transaction. Normal deletes
remain forbidden. The marker disappears with the actor at commit.

Managed artifact paths are collected and validated before the transaction.
After commit, Codrik removes the files. A failed unlink does not recreate the
actor; the unreferenced file is left for artifact garbage collection. Garbage
collection is extended to remove validated managed files that have no
database record. It must never follow symlinks or remove a path outside the
configured artifact root.

## Errors and Concurrency

Actor mutations use SQLite transactions and return not-found, protected
default, unknown-tool, actor-busy, nonempty, or delivery-unresolved errors as
appropriate. Concurrent identical mutations converge on the same result.
Conflicting mutations serialize through SQLite; their response reflects the
committed state.

Force deletion rechecks every precondition inside its transaction. A stale
CLI observation cannot authorize deletion. Ingress and dispatcher paths also
check enabled state at their durable boundary, so disable cannot race into a
new accepted event or quantum after it commits.

## Testing

Focused store tests cover create/list/show, enabled state, tool grants,
idempotency, unknown tools, default-actor protection, safe deletion, force
deletion, and transactional race checks.

IPC and CLI tests freeze the strict wire variants, parse every command, and
verify that CLI administration uses the daemon rather than SQLite.

Runtime tests cover independent dispatch of two actors, starting and stopping
dispatchers after enable/disable, preserving an active quantum during disable,
and applying tool changes only to the next quantum.

Linking tests cover explicit actor selection and the backward-compatible
default. Acceptance coverage creates a second actor, grants tools, links an
identity, executes work, disables the actor, and verifies that new work is not
accepted. Deletion tests cover active-work and unresolved-delivery rejection,
database purge, and orphan artifact cleanup.
