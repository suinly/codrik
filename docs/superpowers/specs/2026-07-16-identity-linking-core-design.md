# Identity Linking Core Design

## Goal

Allow a person to link a new communication-channel identity to an existing
Codrik actor using a short-lived one-time code. Linked private channels share
the same actor and therefore the same durable memory and execution context.

This iteration implements the linking core, SQLite persistence, local IPC, and
the `codrik link` CLI command. Telegram webhooks and other gateways will use the
same internal service in later iterations.

## User Flow

The initial supported flow uses the local CLI:

1. The configured actor runs `codrik link`.
2. The running daemon issues a one-time code and the CLI prints:

   ```text
   Link code: ABCD-EFGH
   Expires in 10 minutes.
   In the new channel, send: /link ABCD-EFGH
   ```

3. A future gateway receives `/link ABCD-EFGH` from an unlinked identity.
4. The gateway handles the command before ordinary ingress and calls the
   internal identity-linking service.
5. After successful redemption, subsequent ordinary messages from that
   identity resolve to the same actor.

Once gateway commands exist, a linked private identity may send `/link`
without a code to issue a new code for its actor. An unlinked identity must use
the explicit `/link CODE` form. Linking commands never become conversation
events, work items, model input, or actor memory.

## Code Format and Lifetime

Link codes have eight symbols chosen uniformly from:

```text
23456789ABCDEFGHJKMNPQRSTUVWXYZ
```

The alphabet excludes `0`, `O`, `1`, `I`, and `L`. Codes are displayed as two
groups of four separated by a hyphen. Redemption accepts grouped or ungrouped
input in any letter case. Normalization removes ASCII hyphens and whitespace,
uppercases letters, and then requires exactly eight symbols from the allowed
alphabet.

A code is valid for 10 minutes. It is valid only while
`now < expires_at`; it is expired at the exact expiry timestamp.

Only one active code may exist for an actor. Issuing a new code atomically
revokes and replaces every previous code for that actor. A successful
redemption deletes the code in the same transaction that creates or confirms
the identity link.

## SQLite Schema

SQLite remains the only source of truth. Add:

```sql
CREATE TABLE identity_link_codes (
    actor_id TEXT PRIMARY KEY REFERENCES actors(id) ON DELETE CASCADE,
    code_hash BLOB NOT NULL UNIQUE CHECK(length(code_hash) = 32),
    created_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL CHECK(expires_at > created_at)
) STRICT;

CREATE TABLE identity_link_attempts (
    provider TEXT NOT NULL,
    subject TEXT NOT NULL,
    window_started_at INTEGER NOT NULL,
    failure_count INTEGER NOT NULL CHECK(failure_count BETWEEN 1 AND 5),
    blocked_until INTEGER,
    PRIMARY KEY(provider, subject)
) STRICT;
```

The code hash is SHA-256 over a fixed domain separator and the normalized code:

```text
SHA256("codrik-identity-link-v1\0" || normalized_code)
```

Plaintext codes are never persisted. The schema migration advances the runtime
schema version and upgrades existing version-2 databases without changing
actor, identity, event, or memory rows.

## Domain Types and Store Boundary

Add an `IdentityLinkStore` abstraction with coarse-grained transactional
operations. The store receives hashes, identities, actor IDs, and timestamps;
it does not generate random codes or format user-facing messages.

```rust
pub struct LinkIdentity {
    pub provider: String,
    pub subject: String,
    pub username: Option<String>,
}

pub enum StoreLinkCodeReplacement {
    Stored,
    HashCollision,
}

pub enum StoreLinkRedemption {
    Linked { actor_id: ActorId },
    AlreadyLinked { actor_id: ActorId },
    InvalidOrExpired,
    RateLimited { retry_at: Timestamp },
    IdentityConflict { actor_id: ActorId },
}

#[async_trait]
pub trait IdentityLinkStore: Send + Sync {
    async fn replace_link_code(
        &self,
        actor: &ActorId,
        code_hash: [u8; 32],
        created_at: Timestamp,
        expires_at: Timestamp,
    ) -> anyhow::Result<StoreLinkCodeReplacement>;

    async fn redeem_link_code(
        &self,
        identity: LinkIdentity,
        code_hash: Option<[u8; 32]>,
        now: Timestamp,
    ) -> anyhow::Result<StoreLinkRedemption>;

    async fn collect_expired_link_state(
        &self,
        now: Timestamp,
        limit: usize,
    ) -> anyhow::Result<usize>;
}
```

`replace_link_code` verifies that the actor exists and is enabled. It upserts
the actor's single code in an immediate transaction. A `code_hash` unique
constraint conflict with another actor returns `HashCollision`; other SQLite
failures remain errors.

`redeem_link_code` executes all checks and writes in one immediate transaction:

1. Reject an actively blocked identity.
2. If a syntactically valid hash was supplied, find a nonexpired code whose
   actor is still enabled.
3. For a missing hash, unknown hash, or expired code, record a failed attempt.
4. Inspect an existing `(provider, subject)` identity.
5. If it belongs to a different actor, return `IdentityConflict` without
   consuming the code.
6. If it belongs to the code's actor, update the username when the request
   supplies `Some`, preserve the existing username for `None`, consume the
   code, clear failed-attempt state, and return `AlreadyLinked`.
7. Otherwise insert the identity, consume the code, clear failed-attempt state,
   and return `Linked`.

An identity is never transferred between actors by code redemption. A future
explicitly authorized management operation is required for transfers.
Blank provider or subject values are rejected as invalid service input before
any link-code state is read or changed.

## Failed-Attempt Limiting

Invalid or expired redemption attempts are limited per verified gateway
identity `(provider, subject)`:

- the first failure opens a 10-minute window;
- failures within the window increment `failure_count`;
- the fifth failure sets `blocked_until` to 10 minutes after that attempt;
- attempts before `blocked_until` return `RateLimited` without code lookup;
- after the window or block expires, the next failure starts a fresh window;
- successful `Linked` or `AlreadyLinked` redemption deletes attempt state;
- `IdentityConflict` does not consume the code and does not reveal whether the
  submitted code belongs to the conflicting actor.

Gateways expose `InvalidOrExpired` as:

```text
Invalid or expired link code.
```

They expose conflicts as:

```text
This channel is already linked to another actor.
```

Rate-limited responses may include the retry time but never reveal information
about code validity.

## Identity Linking Service

`IdentityLinkService` owns code generation, normalization, hashing, and
user-facing outcomes. Its dependencies are:

- `IdentityLinkStore`;
- `Clock`;
- an injectable `LinkCodeGenerator`.

```rust
pub trait LinkCodeGenerator: Send + Sync {
    fn generate(&self) -> anyhow::Result<String>;
}
```

The production generator uses operating-system randomness. Tests inject
deterministic sequences.

Issuing a code:

1. Load or receive the authorized actor ID.
2. Generate and validate a normalized code.
3. Hash the normalized code.
4. Call `replace_link_code` with a 10-minute expiry.
5. Return the grouped plaintext code exactly once.

If `replace_link_code` returns `HashCollision`, the service retries with a fresh
code up to five times. Other storage errors fail immediately. Five collisions
return an explicit internal error.

Redeeming a code normalizes and hashes the supplied value before calling the
store. Syntactically invalid values call the store with `code_hash: None`, use
the same failed-attempt path as unknown hashes, and return
`InvalidOrExpired`.

## Local IPC and CLI

Extend local protocol version 1 without changing existing request or event
variants.

Add a request:

```rust
ClientRequestBody::IssueLinkCode {
    request_id: RequestId,
}
```

Add a terminal response:

```rust
ServerEventBody::LinkCodeIssued {
    request_id: RequestId,
    code: String,
    expires_at: i64,
}
```

The server always issues the code for its configured actor. Same-UID Unix
socket authorization remains the authority for the local management
operation.

`LocalIpcClient::issue_link_code` sends the request and waits for exactly one
terminal response. `codrik link` renders the grouped code and the fixed
10-minute instruction. The command:

- creates no client request recovery metadata;
- creates no local request, event, work item, run, outbox row, or result bundle;
- does not call the language model;
- reports daemon protocol and storage failures without printing a partial code.

The new enum variants retain protocol version 1 because existing wire variants
and their semantics are unchanged. An older daemon rejects the new request;
existing operations remain compatible.

## Future Gateway Integration

Gateway adapters will depend on `IdentityLinkService`, not SQLite:

- linked private identity + `/link` → issue a code for the resolved actor;
- unlinked identity + `/link CODE` → redeem against its verified provider and
  subject;
- unlinked identity + any other input → reject without ordinary ingress;
- successful redemption → acknowledge linking, but do not forward the command
  into memory;
- next ordinary message → existing identity resolution routes it to the shared
  actor.

Gateway adapters must verify provider identity from the transport. User-supplied
provider or subject fields are never trusted.

Telegram webhook implementation, HTTP serving, webhook registration, and
gateway-specific reply delivery are outside this iteration.

## Cleanup and Observability

Expired codes and expired attempt rows are removed:

- lazily before issue and redemption operations;
- periodically through the existing runtime garbage-collection lifecycle.

Cleanup is bounded by a caller-provided limit and is safe to repeat.
An attempt row is eligible for cleanup when its block has expired, or when it
has no block and its 10-minute failure window has expired.

Observability may record:

- actor ID for issuance;
- provider;
- outcome class;
- expiry or retry timestamps;
- storage error class.

Observability must not record:

- plaintext or grouped codes;
- code hashes;
- full identity subjects;
- `/link CODE` command text.

## Testing

Store tests cover:

- replacement and revocation of an actor's previous code;
- exact expiry-boundary behavior;
- one-time redemption;
- concurrent redemption of one code;
- new identity insertion;
- idempotent same-actor redemption and username refresh;
- different-actor identity conflict without code consumption;
- failed-attempt windows, fifth-attempt blocking, expiry, and clearing;
- rollback without partial identity creation;
- bounded expired-state cleanup;
- preservation of existing actor and memory rows through migration.

Service tests cover:

- allowed alphabet and eight-symbol length;
- grouped display;
- grouped, ungrouped, lowercase, and whitespace normalization;
- rejection of invalid symbols and lengths;
- deterministic 10-minute expiry;
- hash domain separation;
- collision retry and five-collision failure;
- plaintext code absence from SQLite and logged events.

IPC and CLI tests cover:

- strict serialization and deserialization of the new request and response;
- unknown-field rejection;
- same-UID issuance for the configured actor;
- output formatting;
- daemon and storage errors;
- absence of recovery metadata and actor work rows.

Ingress tests prove that an identity is unauthorized before redemption and
routes events to the same actor after successful redemption.

## Acceptance Criteria

- `codrik link` prints an eight-symbol one-time code grouped as `ABCD-EFGH`.
- The code expires after 10 minutes and issuing another code revokes it.
- SQLite never stores plaintext codes.
- Successful redemption atomically links the verified identity to the actor
  and consumes the code.
- Identity transfer between actors is impossible through redemption.
- Five failed attempts temporarily block one verified gateway identity.
- Linking commands never reach the agent loop or actor memory.
- Existing local submit, resume, cancel, streaming, and durable delivery
  behavior remains unchanged.
- Formatting, build, test, Clippy, installer, and integration checks pass.
