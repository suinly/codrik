use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;

const DEFAULT_TIMEOUT_SECONDS: u64 = 20;
const MAX_TIMEOUT_SECONDS: u64 = 30;
const DEFAULT_MAX_OUTPUT_CHARS: usize = 20_000;
const MAX_OUTPUT_CHARS: usize = 100_000;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(super) enum BrowserAction {
    Goto,
    Content,
    Evaluate,
    WaitForSelector,
    Text,
    Attribute,
    Click,
    CloseSession,
}

#[derive(Debug, Deserialize)]
pub(super) struct WebBrowserArguments {
    pub(super) action: BrowserAction,
    pub(super) url: Option<String>,
    pub(super) session_id: Option<String>,
    pub(super) selector: Option<String>,
    pub(super) script: Option<String>,
    pub(super) attribute: Option<String>,
    timeout_seconds: Option<u64>,
    max_output_chars: Option<usize>,
    output_format: Option<OutputFormat>,
}

impl WebBrowserArguments {
    pub(super) fn timeout_seconds(&self) -> u64 {
        self.timeout_seconds
            .unwrap_or(DEFAULT_TIMEOUT_SECONDS)
            .min(MAX_TIMEOUT_SECONDS)
    }

    pub(super) fn max_output_chars(&self) -> usize {
        self.max_output_chars
            .unwrap_or(DEFAULT_MAX_OUTPUT_CHARS)
            .min(MAX_OUTPUT_CHARS)
    }

    pub(super) fn output_format(&self) -> OutputFormat {
        self.output_format.unwrap_or(OutputFormat::Html)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(super) enum OutputFormat {
    Html,
    Text,
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub(super) struct WebBrowserResult {
    pub(super) action: BrowserAction,
    pub(super) status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) content: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) value: Option<Value>,
    pub(super) truncated: bool,
}

pub(super) fn parse_arguments(arguments: &str) -> Result<WebBrowserArguments> {
    serde_json::from_str(arguments).context("failed to parse web_browser tool arguments")
}

pub(super) fn truncate_output(value: &str, max_chars: usize) -> (String, bool) {
    let mut end = 0;

    for (count, (idx, ch)) in value.char_indices().enumerate() {
        if count == max_chars {
            return (value[..end].to_string(), true);
        }
        end = idx + ch.len_utf8();
    }

    (value.to_string(), false)
}
