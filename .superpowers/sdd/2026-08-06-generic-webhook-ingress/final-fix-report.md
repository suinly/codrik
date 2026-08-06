# Generic Webhook Ingress Final Fix Report

Date: 2026-08-06

## Outcome

Fixed the three reported production defects and closed every requested test gap.
No dependency or Cargo feature was added. `.t/` was not touched.

## Production Fixes

### Authentication and body handling

- Root cause: Axum's `Bytes` extractor and `DefaultBodyLimit` rejected oversized
  bodies before the handler authenticated the request. The framework-generated
  `413` response also contained a body.
- RED:
  `rtk cargo test interfaces::webhook::server::tests::unauthenticated_oversized_body_is_bodyless_unauthorized -- --nocapture`
  failed with `413` instead of `401`.
- RED:
  `rtk cargo test interfaces::webhook::server::tests::body_limit_accepts_exact_mib_and_rejects_overflow_bodyless_after_auth -- --nocapture`
  failed because the overflow `413` body was not empty.
- Fix: the handler authenticates and validates headers from `Request<Body>`
  before calling `to_bytes` with the exact 1 MiB limit. Limit errors map to an
  explicit bodyless `413`.
- GREEN: all 9 webhook server tests passed.

### Fixed-work bearer comparison

- Root cause: `candidate.len() == configured.len() && ct_eq(...)` returned
  before byte comparison when lengths differed, exposing configured token
  length through work performed.
- Fix: configured and candidate tokens are independently padded to fixed
  256-byte arrays with 256 iterations. Array equality and length equality use
  `subtle::ConstantTimeEq`; both results are combined without branching.
- Candidate lengths over 256 cannot match because constant-time length equality
  still participates in the result.

### Exact valid JSON preservation

- Root cause: `serde_json::Value` parses numbers through its numeric model and
  rejects valid unbounded JSON numbers such as `1e400`.
- RED:
  `rtk cargo test interfaces::webhook::ingress::tests::trusted_envelope_preserves_unbounded_json_number -- --nocapture`
  failed while parsing `1e400`.
- Fix: the already-enabled `serde_json` `raw_value` capability validates the
  complete body as one JSON value, then serializes borrowed `RawValue` into a
  typed trusted envelope. Original JSON number spelling and value semantics are
  retained. Malformed JSON remains rejected.
- GREEN: all scalar and container shapes, including top-level and nested
  `1e400`, passed through HTTP and durable envelope construction.

## Requested Coverage

- Exact 1 MiB accepted: covered.
- 1 MiB plus one byte bodyless `413`: covered.
- Unauthenticated oversized bodyless `401`: covered.
- Null, booleans, strings, ordinary/huge numbers, arrays, objects: covered.
- Durable ingress error bodyless `503`: covered.
- Requests 1-64 occupy permits; request 65 receives bodyless `503`; released
  permit is reused: covered.
- Graceful shutdown waits for an in-flight request: covered.
- Duplicate YAML endpoint key rejection: RED exposed nested `BTreeMap` overwrite;
  field-level unique-map deserialization now rejects duplicates.
- Direct v6 to v7 migration preservation: authentic v6 actor, work item, event,
  run, payload, policies, schema version, and foreign keys verified.

## Malformed Persisted Policy Evaluation

No spin defect exists in the current supervisor chain:

1. `attach_next_run` rejects an unknown persisted event/run execution policy.
2. `RuntimeRunner` classifies attach errors as `AuthorityUnavailable`.
3. `ActorDispatcher` returns authority errors immediately.
4. `ActorDispatcherManager` propagates failed child tasks.
5. `RuntimeSupervisor` treats dispatcher component exit as fatal.

Added direct durable-state characterization
`malformed_persisted_policy_returns_authority_error_without_spinning`. Existing
dispatcher and supervisor suites prove the remaining propagation chain. No
production change was needed.

## Verification

Focused:

- `rtk cargo test interfaces::webhook::server::tests -- --nocapture`: 9 passed.
- `rtk cargo test interfaces::webhook::ingress::tests -- --nocapture`: 2 passed.
- `rtk cargo test config::tests -- --nocapture`: 20 passed.
- `rtk cargo test runtime::sqlite::tests::v6_to_v7_migration_preserves_existing_runtime_rows -- --nocapture`: 1 passed.
- `rtk cargo test runtime::sqlite::dispatch::tests::malformed_persisted_policy_returns_authority_error_without_spinning -- --nocapture`: 1 passed.
- `rtk cargo test runtime::dispatcher::tests -- --nocapture`: 6 passed.
- `rtk cargo test runtime::supervisor::tests -- --nocapture`: 4 passed.

Fresh ordered final gate:

1. `rtk cargo fmt --check`: passed.
2. `rtk cargo check`: passed.
3. `rtk cargo test`: 655 passed, 1 ignored.
4. `rtk cargo clippy --all-targets --all-features`: zero errors; 23 existing
   non-denied warnings remained.
5. `rtk git diff --check`: passed.

Post-commit branch-finishing verification initially exposed a test-only server
startup race under full-suite parallel load: a request could run before the
spawned Axum serve future received its first poll. Both HTTP test spawn helpers
now yield once after spawning. Three consecutive focused server suites passed,
then a fresh full `rtk cargo test` passed with 655 passed and 1 ignored.

## Remaining Concerns

- Constant-time guarantees depend on `subtle` and optimized generated code;
  runtime timing tests would be noisy and were not added.
- The fixed token ceiling remains the validated configuration maximum of 256
  bytes.

## Authorization Field Multiplicity Fix

- Root cause: `HeaderMap::get("authorization")` selected one field value and
  allowed duplicate Authorization field-lines to reach token validation.
- RED: `rtk cargo test interfaces::webhook::server::tests::rejects_duplicate_authorization_headers -- --nocapture`
  failed with `202` instead of bodyless `401`.
- Fix: authentication now requires exactly one Authorization field value before
  parsing the bearer token or reading the body.
- Coverage: conflicting duplicate values and duplicate equal valid values both
  return bodyless `401`.
- GREEN: focused regression passed: 1 passed, 0 failed.

### Second Fix Wave Verification

- `rtk cargo test interfaces::webhook::server::tests -- --nocapture`: 10 passed.
- `rtk cargo fmt --check`: passed.
- `rtk cargo check`: passed.
- `rtk cargo test`: 656 passed, 1 ignored.
- `rtk cargo clippy --all-targets --all-features`: zero errors; 23 existing
  non-denied warnings remained.
- `.t/` remained untouched.
