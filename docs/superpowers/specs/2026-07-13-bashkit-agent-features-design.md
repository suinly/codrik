# Bashkit Agent Features Design

## Goal

Enable the Bashkit capabilities that are broadly useful to Codrik agents
without enabling every optional runtime, remote-access mechanism, or testing
facility shipped by Bashkit.

## Feature Set

Codrik will enable these Bashkit 0.12 features:

- `realfs` for the existing actor workspace mount;
- `http_client` for sandbox-native `curl` and `wget`;
- `jq` for JSON processing;
- `git` for local Git repository operations inside the sandbox.

The default `bash_tool` feature remains enabled implicitly. Codrik will not
enable `bot-auth`, `failpoints`, `interop`, `logging`, `scripted_tool`,
`sqlite`, `ssh`, or `typescript`.

## Architecture and Security Boundary

This change only expands the built-ins compiled into Bashkit. Existing
`ExecutionLimits`, actor workspace mounts, allowed host paths, timeouts, and
output caps remain unchanged. No system executable is spawned for the new
capabilities: HTTP and JSON processing use Bashkit's embedded Rust
implementations, and Git support is limited to the local operations provided
by Bashkit 0.12.

Enabling `http_client` intentionally gives Bashkit commands outbound HTTP(S)
access. It does not grant access to additional host filesystem paths. The
privileged real-server `bash` tool remains separately authorized.

## User-Facing Behavior

An authorized actor can use `bashkit` to:

- fetch HTTP(S) resources with `curl` or `wget`;
- transform JSON with `jq`;
- initialize and inspect local Git repositories in the sandbox or mounted
  actor workspace.

Failures remain normal Bashkit command results containing exit status, stdout,
and stderr. Network availability and remote-server behavior are not guaranteed
by the feature itself.

## Testing and Documentation

- Verify feature activation from Cargo metadata/tree output.
- Add deterministic Bashkit tests for `jq` and local Git behavior.
- Test HTTP support against a local in-process HTTP server rather than the
  public internet.
- Keep existing mount, timeout, and output-limit tests green.
- Document the enabled built-ins and the outbound-network boundary in README.
- Run the full test, check, formatting, and Clippy suite before completion.
