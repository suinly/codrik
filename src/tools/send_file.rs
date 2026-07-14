use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use serde::Deserialize;
use tokio::fs;

use crate::agent::tool::{
    FileArtifact, Tool, ToolArtifact, ToolCallContext, ToolCapabilities, ToolExecution,
    ToolHandler, ToolParameter, ToolParameters,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileRoot {
    pub prefix: String,
    pub path: PathBuf,
}

impl FileRoot {
    pub fn new(prefix: impl Into<String>, path: impl AsRef<Path>) -> Self {
        Self {
            prefix: prefix.into(),
            path: path.as_ref().to_path_buf(),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SendFileToolConfig {
    pub roots: Vec<FileRoot>,
}

pub struct SendFileTool {
    config: SendFileToolConfig,
}

impl SendFileTool {
    pub fn new(config: SendFileToolConfig) -> Self {
        Self { config }
    }
}

#[derive(Deserialize)]
struct SendFileArguments {
    path: String,
    caption: Option<String>,
}

#[async_trait]
impl ToolHandler for SendFileTool {
    fn name(&self) -> &'static str {
        "send_file"
    }

    fn definition(&self) -> Tool {
        Tool::new(
            self.name(),
            "Send an existing file from an allowed session or workspace path to the user.",
            ToolParameters::new()
                .required(
                    "path",
                    ToolParameter::string(
                        "Virtual path such as session/report.pdf or workspace/output.csv.",
                    ),
                )
                .optional("caption", ToolParameter::string("Optional file caption.")),
        )
    }

    fn capabilities(&self) -> ToolCapabilities {
        ToolCapabilities::read_only()
    }

    async fn execute(&self, _arguments: &str) -> Result<String> {
        bail!("send_file requires typed tool execution")
    }

    async fn execute_typed(
        &self,
        arguments: &str,
        _context: &ToolCallContext,
    ) -> Result<ToolExecution> {
        let arguments: SendFileArguments =
            serde_json::from_str(arguments).context("failed to parse send_file arguments")?;
        let (prefix, relative) = arguments
            .path
            .split_once('/')
            .context("file path must start with a configured virtual root")?;
        let root = self
            .config
            .roots
            .iter()
            .find(|root| root.prefix == prefix)
            .context("file path uses an unknown virtual root")?;
        let canonical_root = fs::canonicalize(&root.path)
            .await
            .with_context(|| format!("failed to resolve file root: {}", root.path.display()))?;
        let candidate = fs::canonicalize(root.path.join(relative))
            .await
            .with_context(|| format!("failed to resolve file: {}", arguments.path))?;
        if !candidate.starts_with(&canonical_root) {
            bail!("file is outside allowed file roots");
        }
        let metadata = fs::metadata(&candidate).await?;
        if !metadata.is_file() {
            bail!("send_file path is not a regular file");
        }
        let bytes = fs::read(&candidate).await?;
        let media_type = infer::get(&bytes)
            .map(|kind| kind.mime_type())
            .unwrap_or("application/octet-stream")
            .to_string();
        let display_name = candidate
            .file_name()
            .and_then(|name| name.to_str())
            .context("file name is not valid UTF-8")?
            .to_string();

        Ok(ToolExecution {
            observation: format!("file ready: {display_name}"),
            artifacts: vec![ToolArtifact::File(FileArtifact {
                path: candidate,
                display_name,
                media_type,
                caption: arguments.caption,
            })],
        })
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use anyhow::Result;
    use tokio::fs;

    use crate::{
        agent::tool::{ToolArtifact, ToolCallContext, ToolHandler},
        llm::client::RunContext,
    };

    use super::{FileRoot, SendFileTool, SendFileToolConfig};

    fn temp_root(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("codrik-send-file-{}-{name}", std::process::id()))
    }

    #[tokio::test]
    async fn accepts_session_file_and_returns_artifact() -> Result<()> {
        let root = temp_root("ok");
        fs::remove_dir_all(&root).await.ok();
        fs::create_dir_all(&root).await?;
        fs::write(root.join("report.pdf"), b"pdf").await?;
        let tool = SendFileTool::new(SendFileToolConfig {
            roots: vec![FileRoot::new("session", &root)],
        });

        let result = tool
            .execute_typed(
                r#"{"path":"session/report.pdf","caption":"Report"}"#,
                &ToolCallContext::legacy(RunContext::new()),
            )
            .await?;

        assert!(matches!(
            result.artifacts.as_slice(),
            [ToolArtifact::File(_)]
        ));
        fs::remove_dir_all(root).await.ok();
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn rejects_symlink_escaping_roots() -> Result<()> {
        let root = temp_root("root");
        let outside = temp_root("outside");
        fs::remove_dir_all(&root).await.ok();
        fs::remove_file(&outside).await.ok();
        fs::create_dir_all(&root).await?;
        fs::write(&outside, b"secret").await?;
        std::os::unix::fs::symlink(&outside, root.join("escape"))?;

        let error = SendFileTool::new(SendFileToolConfig {
            roots: vec![FileRoot::new("session", &root)],
        })
        .execute_typed(
            r#"{"path":"session/escape"}"#,
            &ToolCallContext::legacy(RunContext::new()),
        )
        .await
        .expect_err("symlink escape must fail");

        assert!(error.to_string().contains("outside allowed file roots"));
        fs::remove_dir_all(root).await.ok();
        fs::remove_file(outside).await.ok();
        Ok(())
    }
}
