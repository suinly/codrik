use std::{fs, path::PathBuf, time::Duration};

use anyhow::{Context, Result};
use async_trait::async_trait;
use bashkit::{BashTool as BashkitRuntimeTool, ExecutionLimits, Tool as BashkitToolContract};
use serde::{Deserialize, Serialize};

use crate::agent::tool::{Tool, ToolHandler, ToolParameter, ToolParameters};

const DEFAULT_TIMEOUT_SECONDS: u64 = 30;
const MAX_TIMEOUT_SECONDS: u64 = 120;
const DEFAULT_MAX_OUTPUT_CHARS: usize = 20_000;
const MAX_OUTPUT_CHARS: usize = 100_000;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BashkitToolConfig {
    pub workspace: Option<PathBuf>,
}

pub struct BashkitTool {
    config: BashkitToolConfig,
}

impl BashkitTool {
    pub fn new(config: BashkitToolConfig) -> Self {
        Self { config }
    }
}

#[derive(Debug, Deserialize)]
struct BashkitArguments {
    command: Option<String>,
    commands: Option<String>,
    cwd: Option<PathBuf>,
    timeout_seconds: Option<u64>,
    timeout_ms: Option<u64>,
    max_output_chars: Option<usize>,
}

#[derive(Debug, Serialize)]
struct BashkitResult {
    exit_code: i32,
    stdout: String,
    stderr: String,
    timed_out: bool,
    stdout_truncated: bool,
    stderr_truncated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[async_trait]
impl ToolHandler for BashkitTool {
    fn name(&self) -> &'static str {
        "bashkit"
    }

    fn definition(&self) -> Tool {
        Tool::new(
            self.name(),
            "Execute bash-compatible commands in a Bashkit in-process sandbox. For authorized actors, the host workspace is mounted read-write at /workspace and is limited to that actor's configured host workspace directory. This is not the real server shell. Other host filesystem and network access are not available unless Bashkit is configured with explicit opt-in capabilities.",
            ToolParameters::new()
                .required(
                    "command",
                    ToolParameter::string("Bash-compatible commands to execute in the virtual Bashkit sandbox."),
                )
                .optional(
                    "commands",
                    ToolParameter::string("Native Bashkit alias for command. If both command and commands are provided, command wins."),
                )
                .optional(
                    "cwd",
                    ToolParameter::string("Initial virtual working directory for the command. Defaults to Bashkit's virtual process directory; this does not mount the host filesystem."),
                )
                .optional(
                    "timeout_seconds",
                    ToolParameter::number("Timeout in seconds. Defaults to 30 and is capped at 120."),
                )
                .optional(
                    "timeout_ms",
                    ToolParameter::number("Native Bashkit timeout in milliseconds. Capped at 120000."),
                )
                .optional(
                    "max_output_chars",
                    ToolParameter::number("Maximum bytes kept from stdout and stderr separately. Defaults to 20000 and is capped at 100000."),
                ),
        )
    }

    async fn execute(&self, arguments: &str) -> Result<String> {
        let arguments: BashkitArguments =
            serde_json::from_str(arguments).context("failed to parse bashkit tool arguments")?;
        let command = arguments
            .command
            .as_deref()
            .or(arguments.commands.as_deref())
            .context("bashkit tool requires `command`")?;
        let timeout_ms = timeout_ms(&arguments);
        let max_output_chars = arguments
            .max_output_chars
            .unwrap_or(DEFAULT_MAX_OUTPUT_CHARS)
            .min(MAX_OUTPUT_CHARS);

        let tool = bashkit_tool(
            arguments.cwd,
            max_output_chars,
            self.config.workspace.clone(),
        )?;
        let execution = tool
            .execution(serde_json::json!({
                "commands": command,
                "timeout_ms": timeout_ms,
            }))
            .context("failed to create bashkit execution")?;
        let output = execution
            .execute()
            .await
            .context("bashkit execution failed")?;
        let result = to_bash_result(output.result);

        serde_json::to_string(&result).context("failed to serialize bash command result")
    }
}

fn bashkit_tool(
    cwd: Option<PathBuf>,
    max_output_bytes: usize,
    workspace: Option<PathBuf>,
) -> Result<BashkitRuntimeTool> {
    let limits = ExecutionLimits::new()
        .timeout(Duration::from_secs(MAX_TIMEOUT_SECONDS))
        .max_stdout_bytes(max_output_bytes)
        .max_stderr_bytes(max_output_bytes);
    let mut builder = BashkitRuntimeTool::builder()
        .username("agent")
        .hostname("sandbox")
        .limits(limits);

    if let Some(cwd) = cwd {
        builder = builder.cwd(cwd);
    }

    if let Some(workspace) = workspace {
        fs::create_dir_all(&workspace).with_context(|| {
            format!(
                "failed to create bash workspace directory: {}",
                workspace.display()
            )
        })?;
        builder = builder.configure(move |builder| {
            builder
                .allowed_mount_paths([workspace.clone()])
                .mount_real_readwrite_at(workspace.clone(), "/workspace")
        });
    }

    Ok(builder.build())
}

fn timeout_ms(arguments: &BashkitArguments) -> u64 {
    arguments
        .timeout_ms
        .or_else(|| arguments.timeout_seconds.map(|seconds| seconds * 1000))
        .unwrap_or(DEFAULT_TIMEOUT_SECONDS * 1000)
        .min(MAX_TIMEOUT_SECONDS * 1000)
}

fn to_bash_result(result: serde_json::Value) -> BashkitResult {
    let stderr = result["stderr"].as_str().unwrap_or_default().to_string();
    let error = result["error"].as_str().map(ToString::to_string);
    let timed_out = error.as_deref() == Some("timeout")
        || stderr.contains("timed out")
        || stderr.contains("execution timeout");

    BashkitResult {
        exit_code: result["exit_code"].as_i64().unwrap_or(1) as i32,
        stdout: result["stdout"].as_str().unwrap_or_default().to_string(),
        stderr,
        timed_out,
        stdout_truncated: result["stdout_truncated"].as_bool().unwrap_or(false),
        stderr_truncated: result["stderr_truncated"].as_bool().unwrap_or(false),
        error,
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::PathBuf,
        sync::atomic::{AtomicU64, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };

    use serde_json::Value;

    use super::*;

    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn definition_requires_command() {
        let definition = BashkitTool::new(BashkitToolConfig::default()).definition();

        assert_eq!(definition.name, "bashkit");
        assert!(
            definition
                .parameters
                .required
                .contains(&"command".to_string())
        );
    }

    #[tokio::test]
    async fn returns_successful_command_output() {
        let result = BashkitTool::new(BashkitToolConfig::default())
            .execute(r#"{"command":"printf hello"}"#)
            .await
            .expect("shell command should execute");
        let result: Value = serde_json::from_str(&result).expect("result should be valid json");

        assert_eq!(result["exit_code"], 0);
        assert_eq!(result["stdout"], "hello");
        assert_eq!(result["stderr"], "");
        assert_eq!(result["timed_out"], false);
    }

    #[tokio::test]
    async fn returns_nonzero_exit_as_tool_output() {
        let result = BashkitTool::new(BashkitToolConfig::default())
            .execute(r#"{"command":"printf nope >&2; exit 7"}"#)
            .await
            .expect("nonzero exit should still be tool output");
        let result: Value = serde_json::from_str(&result).expect("result should be valid json");

        assert_eq!(result["exit_code"], 7);
        assert_eq!(result["stderr"], "nope");
        assert_eq!(result["timed_out"], false);
    }

    #[tokio::test]
    async fn reports_timeout() {
        let result = BashkitTool::new(BashkitToolConfig::default())
            .execute(r#"{"command":"sleep 2","timeout_seconds":1}"#)
            .await
            .expect("timeout should still be tool output");
        let result: Value = serde_json::from_str(&result).expect("result should be valid json");

        assert_eq!(result["exit_code"], 124);
        assert_eq!(result["timed_out"], true);
        assert_eq!(result["error"], "timeout");
    }

    #[tokio::test]
    async fn truncates_large_output() {
        let result = BashkitTool::new(BashkitToolConfig::default())
            .execute(r#"{"command":"printf abcdef","max_output_chars":3}"#)
            .await
            .expect("shell command should execute");
        let result: Value = serde_json::from_str(&result).expect("result should be valid json");

        assert_eq!(result["stdout"], "abc");
        assert_eq!(result["stdout_truncated"], true);
    }

    #[tokio::test]
    async fn caps_output_while_command_is_running() {
        let result = BashkitTool::new(BashkitToolConfig::default())
            .execute(r#"{"command":"printf abcdefghij","max_output_chars":8}"#)
            .await
            .expect("shell command should execute with capped output");
        let result: Value = serde_json::from_str(&result).expect("result should be valid json");

        assert_eq!(
            result["stdout"]
                .as_str()
                .expect("stdout should be a string")
                .len(),
            8
        );
        assert_eq!(result["stdout_truncated"], true);
        assert_eq!(result["timed_out"], false);
    }

    #[tokio::test]
    async fn accepts_native_commands_alias() {
        let result = BashkitTool::new(BashkitToolConfig::default())
            .execute(r#"{"commands":"printf alias"}"#)
            .await
            .expect("shell command should execute");
        let result: Value = serde_json::from_str(&result).expect("result should be valid json");

        assert_eq!(result["exit_code"], 0);
        assert_eq!(result["stdout"], "alias");
    }

    #[tokio::test]
    async fn runs_inside_virtual_filesystem() {
        let result = BashkitTool::new(BashkitToolConfig::default())
            .execute(r#"{"command":"mkdir -p /tmp/data; printf hello > /tmp/data/out.txt; cat /tmp/data/out.txt"}"#)
            .await
            .expect("shell command should execute");
        let result: Value = serde_json::from_str(&result).expect("result should be valid json");

        assert_eq!(result["exit_code"], 0);
        assert_eq!(result["stdout"], "hello");
    }

    #[tokio::test]
    async fn mounts_configured_workspace_at_workspace() {
        let workspace = temp_workspace_path();
        let _ = fs::remove_dir_all(&workspace);
        let tool = BashkitTool::new(BashkitToolConfig {
            workspace: Some(workspace.clone()),
        });

        let result = tool
            .execute(
                r#"{"command":"printf host-file > /workspace/out.txt; cat /workspace/out.txt"}"#,
            )
            .await
            .expect("shell command should execute");
        let result: Value = serde_json::from_str(&result).expect("result should be valid json");

        assert_eq!(result["exit_code"], 0);
        assert_eq!(result["stdout"], "host-file");
        assert_eq!(
            fs::read_to_string(workspace.join("out.txt")).expect("host file should be written"),
            "host-file"
        );

        fs::remove_dir_all(&workspace).expect("temporary workspace should be removable");
    }

    fn temp_workspace_path() -> PathBuf {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after unix epoch")
            .as_nanos();
        let counter = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);

        std::env::temp_dir().join(format!(
            "codrik-bash-workspace-test-{}-{suffix}-{counter}",
            std::process::id()
        ))
    }
}
