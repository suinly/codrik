use std::{env, io, os::unix::process::CommandExt, path::PathBuf, process::Stdio, time::Duration};

use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    process::Command,
    time,
};

use crate::agent::tool::{Tool, ToolHandler, ToolParameter, ToolParameters};

const DEFAULT_TIMEOUT_SECONDS: u64 = 30;
const MAX_TIMEOUT_SECONDS: u64 = 120;
const DEFAULT_MAX_OUTPUT_CHARS: usize = 20_000;
const MAX_OUTPUT_CHARS: usize = 100_000;
const TIMEOUT_TERMINATION_GRACE: Duration = Duration::from_secs(1);

pub struct BashTool;

#[derive(Debug, Deserialize)]
struct BashArguments {
    command: String,
    cwd: Option<PathBuf>,
    timeout_seconds: Option<u64>,
    max_output_chars: Option<usize>,
}

#[derive(Debug, Serialize)]
struct BashResult {
    exit_code: Option<i32>,
    stdout: String,
    stderr: String,
    timed_out: bool,
    stdout_truncated: bool,
    stderr_truncated: bool,
}

struct CappedOutput {
    content: String,
    truncated: bool,
}

#[async_trait]
impl ToolHandler for BashTool {
    fn name(&self) -> &'static str {
        "bash"
    }

    fn definition(&self) -> Tool {
        Tool::new(
            self.name(),
            "Execute a bash-compatible shell command on the host system and return stdout, stderr, exit code, and timeout status.",
            ToolParameters::new()
                .required(
                    "command",
                    ToolParameter::string("Bash-compatible command to execute."),
                )
                .optional(
                    "cwd",
                    ToolParameter::string("Working directory for the command. Defaults to the current process directory."),
                )
                .optional(
                    "timeout_seconds",
                    ToolParameter::number("Timeout in seconds. Defaults to 30 and is capped at 120."),
                )
                .optional(
                    "max_output_chars",
                    ToolParameter::number("Maximum characters kept from stdout and stderr separately. Defaults to 20000 and is capped at 100000."),
                ),
        )
    }

    async fn execute(&self, arguments: &str) -> Result<String> {
        let arguments: BashArguments =
            serde_json::from_str(arguments).context("failed to parse bash tool arguments")?;
        let timeout_seconds = arguments
            .timeout_seconds
            .unwrap_or(DEFAULT_TIMEOUT_SECONDS)
            .min(MAX_TIMEOUT_SECONDS);
        let max_output_chars = arguments
            .max_output_chars
            .unwrap_or(DEFAULT_MAX_OUTPUT_CHARS)
            .min(MAX_OUTPUT_CHARS);

        let mut command = shell_command(&arguments.command);
        command.kill_on_drop(true);
        command.stdout(Stdio::piped()).stderr(Stdio::piped());
        if let Some(cwd) = arguments.cwd {
            command.current_dir(cwd);
        }

        start_new_session(&mut command);

        let mut child = command.spawn().context("failed to spawn shell command")?;
        let process_group_id = child
            .id()
            .context("spawned shell command has no process id")?
            as i32;
        let stdout = child
            .stdout
            .take()
            .context("failed to capture shell command stdout")?;
        let stderr = child
            .stderr
            .take()
            .context("failed to capture shell command stderr")?;
        let stdout_reader = tokio::spawn(read_capped_output(stdout, max_output_chars));
        let stderr_reader = tokio::spawn(read_capped_output(stderr, max_output_chars));

        let (exit_code, timed_out) =
            match time::timeout(Duration::from_secs(timeout_seconds), child.wait()).await {
                Ok(status) => (
                    status.context("failed to wait for shell command")?.code(),
                    false,
                ),
                Err(_) => {
                    terminate_process_group(process_group_id, &mut child).await?;
                    (None, true)
                }
            };

        let stdout = stdout_reader
            .await
            .context("shell stdout reader task failed")??;
        let mut stderr = stderr_reader
            .await
            .context("shell stderr reader task failed")??;

        if timed_out {
            append_capped(
                &mut stderr,
                &format!("command timed out after {timeout_seconds} seconds"),
                max_output_chars,
            );
        }

        let result = BashResult {
            exit_code,
            stdout: stdout.content,
            stderr: stderr.content,
            timed_out,
            stdout_truncated: stdout.truncated,
            stderr_truncated: stderr.truncated,
        };

        serde_json::to_string(&result).context("failed to serialize bash command result")
    }
}

fn shell_command(command: &str) -> Command {
    let shell = env::var("SHELL").unwrap_or_else(|_| "/bin/sh".to_string());
    let mut process = Command::new(shell);
    process.arg("-lc").arg(command);
    process
}

fn start_new_session(command: &mut Command) {
    unsafe {
        command.as_std_mut().pre_exec(|| {
            if libc::setsid() == -1 {
                return Err(io::Error::last_os_error());
            }

            Ok(())
        });
    }
}

async fn terminate_process_group(
    process_group_id: i32,
    child: &mut tokio::process::Child,
) -> Result<()> {
    send_signal_to_process_group(process_group_id, libc::SIGTERM)
        .context("failed to terminate shell command process group")?;

    if time::timeout(TIMEOUT_TERMINATION_GRACE, child.wait())
        .await
        .is_ok()
    {
        return Ok(());
    }

    send_signal_to_process_group(process_group_id, libc::SIGKILL)
        .context("failed to kill shell command process group")?;
    let _ = child.wait().await;

    Ok(())
}

fn send_signal_to_process_group(process_group_id: i32, signal: i32) -> io::Result<()> {
    let result = unsafe { libc::kill(-process_group_id, signal) };
    if result == 0 {
        return Ok(());
    }

    let error = io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::ESRCH) {
        return Ok(());
    }

    Err(error)
}

async fn read_capped_output(
    mut reader: impl AsyncRead + Unpin,
    max_chars: usize,
) -> Result<CappedOutput> {
    let mut output = CappedOutput {
        content: String::new(),
        truncated: false,
    };
    let mut buffer = [0; 8192];

    loop {
        let bytes_read = reader
            .read(&mut buffer)
            .await
            .context("failed to read shell command output")?;
        if bytes_read == 0 {
            return Ok(output);
        }

        let chunk = String::from_utf8_lossy(&buffer[..bytes_read]);
        append_capped(&mut output, &chunk, max_chars);
    }
}

fn append_capped(output: &mut CappedOutput, chunk: &str, max_chars: usize) {
    let kept_chars = output.content.chars().count();
    if kept_chars >= max_chars {
        output.truncated = true;
        return;
    }

    let remaining = max_chars - kept_chars;
    let mut chunk_chars = chunk.chars();
    output.content.extend(chunk_chars.by_ref().take(remaining));
    if chunk_chars.next().is_some() {
        output.truncated = true;
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, path::PathBuf};

    use serde_json::Value;

    use super::*;

    #[test]
    fn definition_requires_command() {
        let definition = BashTool.definition();

        assert_eq!(definition.name, "bash");
        assert!(
            definition
                .parameters
                .required
                .contains(&"command".to_string())
        );
    }

    #[tokio::test]
    async fn returns_successful_command_output() {
        let result = BashTool
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
        let result = BashTool
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
        let result = BashTool
            .execute(r#"{"command":"sleep 2","timeout_seconds":1}"#)
            .await
            .expect("timeout should still be tool output");
        let result: Value = serde_json::from_str(&result).expect("result should be valid json");

        assert_eq!(result["exit_code"], Value::Null);
        assert_eq!(result["timed_out"], true);
    }

    #[tokio::test]
    async fn truncates_large_output() {
        let result = BashTool
            .execute(r#"{"command":"printf abcdef","max_output_chars":3}"#)
            .await
            .expect("shell command should execute");
        let result: Value = serde_json::from_str(&result).expect("result should be valid json");

        assert_eq!(result["stdout"], "abc");
        assert_eq!(result["stdout_truncated"], true);
    }

    #[tokio::test]
    async fn caps_output_while_command_is_running() {
        let result = BashTool
            .execute(r#"{"command":"yes x","timeout_seconds":1,"max_output_chars":8}"#)
            .await
            .expect("shell command should time out with capped output");
        let result: Value = serde_json::from_str(&result).expect("result should be valid json");

        assert_eq!(
            result["stdout"]
                .as_str()
                .expect("stdout should be a string")
                .len(),
            8
        );
        assert_eq!(result["stdout_truncated"], true);
        assert_eq!(result["timed_out"], true);
    }

    #[tokio::test]
    async fn kills_background_children_on_timeout() {
        let marker = timeout_marker_path();
        let _ = fs::remove_file(&marker);
        let command = format!("sleep 3; printf survived > {}", marker.display());
        let arguments = serde_json::json!({
            "command": format!("({command}) & wait"),
            "timeout_seconds": 1
        });
        let result = BashTool
            .execute(&arguments.to_string())
            .await
            .expect("timeout should kill the shell process group");
        let result: Value = serde_json::from_str(&result).expect("result should be valid json");

        assert_eq!(result["timed_out"], true);
        assert_eq!(result["exit_code"], Value::Null);

        time::sleep(Duration::from_secs(3)).await;
        assert!(!marker.exists());
    }

    fn timeout_marker_path() -> PathBuf {
        env::temp_dir().join(format!(
            "codrik-bash-timeout-{}-{}",
            std::process::id(),
            unix_timestamp_millis()
        ))
    }

    fn unix_timestamp_millis() -> u128 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time should be after unix epoch")
            .as_millis()
    }
}
