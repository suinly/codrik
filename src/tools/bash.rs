use std::{path::PathBuf, process::Stdio, time::Duration};

use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    process::Command,
    time,
};

use crate::agent::tool::{Tool, ToolExposure, ToolHandler, ToolParameter, ToolParameters};

const DEFAULT_TIMEOUT_SECONDS: u64 = 30;
const MAX_TIMEOUT_SECONDS: u64 = 120;
const DEFAULT_MAX_OUTPUT_CHARS: usize = 20_000;
const MAX_OUTPUT_CHARS: usize = 100_000;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BashToolConfig {
    pub default_cwd: Option<PathBuf>,
}

#[derive(Default)]
pub struct BashTool {
    config: BashToolConfig,
}

impl BashTool {
    pub fn new(config: BashToolConfig) -> Self {
        Self { config }
    }
}

#[derive(Debug, Deserialize)]
struct BashArguments {
    command: String,
    cwd: Option<PathBuf>,
    timeout_seconds: Option<u64>,
    max_output_chars: Option<usize>,
}

#[derive(Debug, Serialize)]
struct BashResult {
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
impl ToolHandler for BashTool {
    fn name(&self) -> &'static str {
        "bash"
    }

    fn exposure(&self) -> ToolExposure {
        ToolExposure::Privileged
    }

    fn definition(&self) -> Tool {
        Tool::new(
            self.name(),
            "Execute real bash commands on the server using the codrik process permissions. This is not sandboxed and can read or change server files, spawn processes, and use the server network according to host permissions. Actor-scoped calls start in the actor workspace, so use relative paths for output files. /workspace exists only inside Bashkit. A relative file can be delivered with send_file as workspace/<path>. Use bashkit for normal sandboxed commands.",
            ToolParameters::new()
                .required(
                    "command",
                    ToolParameter::string("Command to execute with `/bin/bash -lc` on the server."),
                )
                .optional(
                    "cwd",
                    ToolParameter::string("Server working directory for the command. Overrides the configured default. Without cwd, actor-scoped calls use the actor workspace and unscoped calls use the codrik process working directory."),
                )
                .optional(
                    "timeout_seconds",
                    ToolParameter::number("Timeout in seconds. Defaults to 30 and is capped at 120."),
                )
                .optional(
                    "max_output_chars",
                    ToolParameter::number("Maximum bytes kept from stdout and stderr separately. Defaults to 20000 and is capped at 100000."),
                ),
        )
    }

    async fn execute(&self, arguments: &str) -> Result<String> {
        let arguments: BashArguments =
            serde_json::from_str(arguments).context("failed to parse bash tool arguments")?;
        let result = run_bash(arguments, self.config.default_cwd.clone()).await?;

        serde_json::to_string(&result).context("failed to serialize bash command result")
    }
}

async fn run_bash(arguments: BashArguments, default_cwd: Option<PathBuf>) -> Result<BashResult> {
    let timeout = Duration::from_secs(
        arguments
            .timeout_seconds
            .unwrap_or(DEFAULT_TIMEOUT_SECONDS)
            .min(MAX_TIMEOUT_SECONDS),
    );
    let max_output_chars = arguments
        .max_output_chars
        .unwrap_or(DEFAULT_MAX_OUTPUT_CHARS)
        .min(MAX_OUTPUT_CHARS);

    let mut command = Command::new("/bin/bash");
    command
        .arg("-lc")
        .arg(arguments.command)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);

    if let Some(cwd) = arguments.cwd.or(default_cwd) {
        command.current_dir(cwd);
    }

    configure_process_group(&mut command);

    let mut child = command.spawn().context("failed to spawn server bash")?;
    let pid = child.id();
    let stdout = child
        .stdout
        .take()
        .context("failed to capture bash stdout")?;
    let stderr = child
        .stderr
        .take()
        .context("failed to capture bash stderr")?;
    let stdout_task = tokio::spawn(read_capped(stdout, max_output_chars));
    let stderr_task = tokio::spawn(read_capped(stderr, max_output_chars));

    let wait_result = time::timeout(timeout, child.wait()).await;
    let (exit_code, timed_out, error) = match wait_result {
        Ok(status) => {
            let status = status.context("failed to wait for server bash")?;
            (status.code().unwrap_or(1), false, None)
        }
        Err(_) => {
            terminate_process_group(pid);
            let _ = child.kill().await;
            let _ = child.wait().await;
            (124, true, Some("timeout".to_string()))
        }
    };

    let stdout = stdout_task
        .await
        .context("stdout reader task failed")?
        .context("failed to read bash stdout")?;
    let stderr = stderr_task
        .await
        .context("stderr reader task failed")?
        .context("failed to read bash stderr")?;

    Ok(BashResult {
        exit_code,
        stdout: stdout.output,
        stderr: stderr.output,
        timed_out,
        stdout_truncated: stdout.truncated,
        stderr_truncated: stderr.truncated,
        error,
    })
}

struct CappedOutput {
    output: String,
    truncated: bool,
}

async fn read_capped<R>(mut reader: R, max_chars: usize) -> Result<CappedOutput>
where
    R: AsyncRead + Unpin,
{
    let mut output = Vec::new();
    let mut buffer = [0_u8; 8192];
    let mut truncated = false;

    loop {
        let read = reader.read(&mut buffer).await?;
        if read == 0 {
            break;
        }

        let remaining = max_chars.saturating_sub(output.len());
        if remaining > 0 {
            output.extend_from_slice(&buffer[..read.min(remaining)]);
        }
        if read > remaining {
            truncated = true;
        }
    }

    Ok(CappedOutput {
        output: String::from_utf8_lossy(&output).to_string(),
        truncated,
    })
}

#[cfg(unix)]
fn configure_process_group(command: &mut Command) {
    unsafe {
        command.pre_exec(|| {
            if libc::setpgid(0, 0) == 0 {
                Ok(())
            } else {
                Err(std::io::Error::last_os_error())
            }
        });
    }
}

#[cfg(not(unix))]
fn configure_process_group(_command: &mut Command) {}

#[cfg(unix)]
fn terminate_process_group(pid: Option<u32>) {
    let Some(pid) = pid else {
        return;
    };
    let pgid = -(pid as libc::pid_t);

    unsafe {
        libc::kill(pgid, libc::SIGTERM);
    }
}

#[cfg(not(unix))]
fn terminate_process_group(_pid: Option<u32>) {}

#[cfg(test)]
mod tests {
    use serde_json::Value;

    use super::*;

    #[test]
    fn definition_requires_command() {
        let definition = BashTool::default().definition();

        assert_eq!(definition.name, "bash");
        assert!(
            definition
                .parameters
                .required
                .contains(&"command".to_string())
        );
    }

    #[test]
    fn definition_explains_workspace_path_contract() {
        let definition = BashTool::default().definition();

        assert!(definition.description.contains("relative paths"));
        assert!(
            definition
                .description
                .contains("/workspace exists only inside Bashkit")
        );
        assert!(definition.description.contains("workspace/<path>"));
    }

    #[test]
    fn is_privileged() {
        assert_eq!(BashTool::default().exposure(), ToolExposure::Privileged);
    }

    #[tokio::test]
    async fn returns_successful_command_output() {
        let result = BashTool::default()
            .execute(r#"{"command":"printf hello"}"#)
            .await
            .expect("server bash command should execute");
        let result: Value = serde_json::from_str(&result).expect("result should be valid json");

        assert_eq!(result["exit_code"], 0);
        assert_eq!(result["stdout"], "hello");
        assert_eq!(result["stderr"], "");
        assert_eq!(result["timed_out"], false);
    }

    #[tokio::test]
    async fn returns_nonzero_exit_as_tool_output() {
        let result = BashTool::default()
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
        let result = BashTool::default()
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
        let result = BashTool::default()
            .execute(r#"{"command":"printf abcdef","max_output_chars":3}"#)
            .await
            .expect("server bash command should execute");
        let result: Value = serde_json::from_str(&result).expect("result should be valid json");

        assert_eq!(result["stdout"], "abc");
        assert_eq!(result["stdout_truncated"], true);
    }

    #[tokio::test]
    async fn runs_in_configured_cwd() {
        let cwd = std::env::current_dir()
            .expect("current dir should exist")
            .join("src");
        let result = BashTool::default()
            .execute(&format!(
                r#"{{"command":"pwd","cwd":{}}}"#,
                serde_json::to_string(&cwd).expect("cwd should serialize")
            ))
            .await
            .expect("server bash command should execute");
        let result: Value = serde_json::from_str(&result).expect("result should be valid json");

        assert_eq!(result["exit_code"], 0);
        assert_eq!(
            result["stdout"]
                .as_str()
                .expect("stdout should be a string")
                .trim(),
            cwd.to_string_lossy()
        );
    }

    #[tokio::test]
    async fn runs_in_default_cwd() {
        let default_cwd = std::env::current_dir()
            .expect("current dir should exist")
            .join("src");
        let result = BashTool::new(BashToolConfig {
            default_cwd: Some(default_cwd.clone()),
        })
        .execute(r#"{"command":"pwd"}"#)
        .await
        .expect("server bash command should execute");
        let result: Value = serde_json::from_str(&result).expect("result should be valid json");

        assert_eq!(
            result["stdout"]
                .as_str()
                .expect("stdout should be a string")
                .trim(),
            default_cwd.to_string_lossy()
        );
    }

    #[tokio::test]
    async fn explicit_cwd_overrides_default_cwd() {
        let root = std::env::current_dir().expect("current dir should exist");
        let explicit_cwd = root.join("src");
        let result = BashTool::new(BashToolConfig {
            default_cwd: Some(root),
        })
        .execute(&format!(
            r#"{{"command":"pwd","cwd":{}}}"#,
            serde_json::to_string(&explicit_cwd).expect("cwd should serialize")
        ))
        .await
        .expect("server bash command should execute");
        let result: Value = serde_json::from_str(&result).expect("result should be valid json");

        assert_eq!(
            result["stdout"]
                .as_str()
                .expect("stdout should be a string")
                .trim(),
            explicit_cwd.to_string_lossy()
        );
    }
}
