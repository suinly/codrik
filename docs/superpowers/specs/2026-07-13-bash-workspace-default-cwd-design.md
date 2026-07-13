# Bash Workspace Default CWD Design

## Problem

The privileged `bash` tool runs in the Codrik process directory by default,
while Bashkit exposes the actor workspace at `/workspace` and `send_file`
exposes the same host directory through the virtual `workspace/` prefix. This
lets the model select the real `bash` tool but mistakenly write to the
Bashkit-only `/workspace` path. Files written relative to the current process
directory are also unavailable to `send_file`.

## Design

Add an optional `default_cwd` to the `bash` tool configuration. Actor-scoped
composition passes the actor's host workspace as this default. If a tool call
provides an explicit `cwd`, that value takes precedence so the privileged tool
retains its existing server-wide capability. Non-actor composition leaves the
default unset and preserves current behavior.

The tool description will explain the path contract:

- real `bash` starts in the actor workspace and should use relative output
  paths such as `nature_photo.jpg`;
- `/workspace` exists only inside Bashkit;
- a relative file created by `bash` is sent as `workspace/nature_photo.jpg`.

No command rewriting, server-level `/workspace` symlink, or restriction of an
explicit `cwd` will be introduced.

## Components and Data Flow

1. `actor_tool_config` calculates and creates the actor workspace path before
   constructing either shell tool.
2. `ToolRegistryConfig` carries that path to both Bashkit and the real bash
   tool.
3. `BashTool` applies `arguments.cwd.or(config.default_cwd)` to
   `tokio::process::Command`.
4. Files created with relative paths land in the same host directory exposed
   by `send_file` under the `workspace/` virtual prefix.

## Error Handling

Failure to create the actor workspace is returned from application composition
with path context. A caller-provided invalid `cwd` remains a normal process
spawn error with context from the bash tool.

## Testing

- A bash unit test executes `pwd` with a configured default and verifies that
  the process runs there.
- A second test verifies that explicit `cwd` overrides the default.
- Registry/application tests verify that actor configuration supplies the
  workspace to the real bash tool without granting it through wildcard access.
- Definition tests verify that `/workspace` is identified as Bashkit-only and
  that relative paths map to `send_file`'s `workspace/` prefix.
- Run the full test, check, formatting, and Clippy suite before completion.
