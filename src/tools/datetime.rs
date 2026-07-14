use std::process::Command;

use anyhow::{Context, Result, bail};
use async_trait::async_trait;

use crate::agent::tool::{Tool, ToolCapabilities, ToolHandler, ToolParameters};

pub struct DatetimeTool;

#[async_trait]
impl ToolHandler for DatetimeTool {
    fn name(&self) -> &'static str {
        "datetime"
    }

    fn definition(&self) -> Tool {
        Tool::new(
            self.name(),
            "Return the current date and time in the system timezone.",
            ToolParameters::new(),
        )
    }

    fn capabilities(&self) -> ToolCapabilities {
        ToolCapabilities::read_only()
    }

    async fn execute(&self, _arguments: &str) -> Result<String> {
        let output = Command::new("date")
            .arg("+%Y-%m-%dT%H:%M:%S%z %Z")
            .output()
            .context("failed to execute date command")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
            bail!("date command failed: {stderr}");
        }

        Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn returns_system_time() {
        let result = DatetimeTool
            .execute("{}")
            .await
            .expect("datetime tool should execute");

        assert!(!result.is_empty());
        assert!(result.contains('T'));
        assert!(result.contains('+') || result.contains('-'));
    }
}
