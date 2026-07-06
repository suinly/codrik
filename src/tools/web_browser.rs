mod actions;
mod types;
mod worker;

#[cfg(test)]
mod tests;

use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use tokio::sync::{mpsc, oneshot};

use crate::agent::tool::{Tool, ToolHandler, ToolParameter, ToolParameters};

use self::{
    types::parse_arguments,
    worker::{BrowserWorkerCommand, start_browser_worker},
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WebBrowserToolConfig {
    pub stealth: bool,
}

impl Default for WebBrowserToolConfig {
    fn default() -> Self {
        Self { stealth: true }
    }
}

pub struct WebBrowserTool {
    commands: mpsc::Sender<BrowserWorkerCommand>,
}

impl WebBrowserTool {
    pub fn new(config: WebBrowserToolConfig) -> Self {
        let (commands, receiver) = mpsc::channel(16);
        start_browser_worker(config, receiver);

        Self { commands }
    }

    #[cfg(test)]
    async fn insert_test_session(&self, session_id: impl Into<String>) {
        let (response, receiver) = oneshot::channel();
        self.commands
            .send(BrowserWorkerCommand::InsertTestSession {
                session_id: session_id.into(),
                response,
            })
            .await
            .expect("worker should accept test session command");
        receiver
            .await
            .expect("worker should respond")
            .expect("test session should be inserted");
    }
}

#[async_trait]
impl ToolHandler for WebBrowserTool {
    fn name(&self) -> &'static str {
        "web_browser"
    }

    fn definition(&self) -> Tool {
        Tool::new(
            self.name(),
            "Use an embedded Obscura browser on the home server. Supports stateful sessions and actions: goto, content, evaluate, wait_for_selector, text, attribute, click, close_session.",
            ToolParameters::new()
                .required(
                    "action",
                    ToolParameter::string_enum(
                        "Browser action to perform.",
                        [
                            "goto",
                            "content",
                            "evaluate",
                            "wait_for_selector",
                            "text",
                            "attribute",
                            "click",
                            "close_session",
                        ],
                    ),
                )
                .optional(
                    "url",
                    ToolParameter::string("URL to open. Required for goto and stateless page reads."),
                )
                .optional(
                    "session_id",
                    ToolParameter::string("Optional browser session id. Use the same id across calls to keep page state, then call close_session."),
                )
                .optional(
                    "selector",
                    ToolParameter::string("CSS selector for wait_for_selector, text, attribute, and click."),
                )
                .optional(
                    "script",
                    ToolParameter::string("JavaScript expression for evaluate."),
                )
                .optional(
                    "attribute",
                    ToolParameter::string("Attribute name for the attribute action."),
                )
                .optional(
                    "output_format",
                    ToolParameter::string_enum("Content output format.", ["html", "text"]),
                )
                .optional(
                    "timeout_seconds",
                    ToolParameter::number("Timeout in seconds. Defaults to 20 and is capped at 30."),
                )
                .optional(
                    "max_output_chars",
                    ToolParameter::number("Maximum content characters returned. Defaults to 20000 and is capped at 100000."),
                ),
        )
    }

    async fn execute(&self, arguments: &str) -> Result<String> {
        let arguments = parse_arguments(arguments)?;
        let (response, receiver) = oneshot::channel();
        self.commands
            .send(BrowserWorkerCommand::Execute {
                arguments,
                response,
            })
            .await
            .context("web_browser worker is not running")?;
        let result = receiver
            .await
            .context("web_browser worker stopped before returning a result")?
            .map_err(|error| anyhow!(error))?;

        serde_json::to_string(&result).context("failed to serialize web_browser result")
    }
}
