use crate::agent::tool::ToolHandler;

use super::{
    WebBrowserTool, WebBrowserToolConfig,
    types::{BrowserAction, WebBrowserResult, parse_arguments, truncate_output},
};

#[test]
fn definition_describes_browser_actions() {
    let definition = WebBrowserTool::new(WebBrowserToolConfig::default()).definition();

    assert_eq!(definition.name, "web_browser");
    assert!(
        definition
            .parameters
            .required
            .contains(&"action".to_string())
    );
    assert!(definition.parameters.properties.contains_key("url"));
    assert!(definition.parameters.properties.contains_key("session_id"));
    assert!(definition.parameters.properties.contains_key("selector"));
    assert!(definition.parameters.properties.contains_key("script"));
}

#[test]
fn parse_arguments_clamps_limits() {
    let arguments = parse_arguments(
        r#"{
            "action": "goto",
            "url": "https://example.com",
            "timeout_seconds": 999,
            "max_output_chars": 999999
        }"#,
    )
    .expect("arguments should parse");

    assert_eq!(arguments.action, BrowserAction::Goto);
    assert_eq!(arguments.timeout_seconds(), 30);
    assert_eq!(arguments.max_output_chars(), 100_000);
}

#[test]
fn truncate_output_reports_truncation() {
    let (output, truncated) = truncate_output("abcdef", 3);

    assert_eq!(output, "abc");
    assert!(truncated);
}

#[tokio::test]
async fn close_session_removes_existing_session() {
    let tool = WebBrowserTool::new(WebBrowserToolConfig::default());
    tool.insert_test_session("demo").await;

    let output = tool
        .execute(r#"{"action":"close_session","session_id":"demo"}"#)
        .await
        .expect("close_session should execute");

    let result: WebBrowserResult =
        serde_json::from_str(&output).expect("result should be valid json");
    assert_eq!(result.action, BrowserAction::CloseSession);
    assert_eq!(result.session_id.as_deref(), Some("demo"));
    assert_eq!(result.status, "closed");
}

#[tokio::test]
async fn content_without_url_or_session_is_rejected() {
    let tool = WebBrowserTool::new(WebBrowserToolConfig::default());

    let error = tool
        .execute(r#"{"action":"content"}"#)
        .await
        .expect_err("stateless content should require a url");

    assert_eq!(
        error.to_string(),
        "web_browser content requires `url` when `session_id` is not provided"
    );
}

#[tokio::test]
async fn new_sessions_are_limited() {
    let tool = WebBrowserTool::new(WebBrowserToolConfig::default());

    for index in 0..4 {
        tool.insert_test_session(format!("session-{index}")).await;
    }

    let error = tool
        .execute(
            r#"{
                "action": "goto",
                "session_id": "session-4",
                "url": "data:text/html,<html></html>"
            }"#,
        )
        .await
        .expect_err("new session should be rejected when limit is reached");

    assert_eq!(
        error.to_string(),
        "web_browser session limit reached; close an existing session first"
    );
}

#[tokio::test]
async fn goto_reads_page_content() {
    let tool = WebBrowserTool::new(WebBrowserToolConfig::default());
    let output = tool
        .execute(
            r#"{
                "action": "goto",
                "url": "data:text/html,<html><body><h1>Codrik%20Browser</h1></body></html>",
                "max_output_chars": 1000
            }"#,
        )
        .await
        .expect("goto should execute");

    let result: WebBrowserResult =
        serde_json::from_str(&output).expect("result should be valid json");
    assert_eq!(result.action, BrowserAction::Goto);
    assert_eq!(result.status, "ok");
    assert!(
        result
            .content
            .as_deref()
            .expect("content should be present")
            .contains("Codrik Browser")
    );
}
