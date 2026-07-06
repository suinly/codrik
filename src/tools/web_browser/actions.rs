use std::time::Duration;

use anyhow::{Context, Result, bail};
use obscura::Page;
use serde_json::Value;

use super::types::{
    BrowserAction, OutputFormat, WebBrowserArguments, WebBrowserResult, truncate_output,
};

pub(super) async fn run_page_action(
    page: &mut Page,
    arguments: &WebBrowserArguments,
) -> Result<WebBrowserResult> {
    if let Some(url) = arguments.url.as_deref() {
        goto_with_timeout(page, url, arguments.timeout_seconds()).await?;
    } else if arguments.action == BrowserAction::Goto {
        bail!("web_browser goto requires `url`");
    }

    match arguments.action {
        BrowserAction::Goto | BrowserAction::Content => page_content_result(page, arguments),
        BrowserAction::Evaluate => evaluate_result(page, arguments),
        BrowserAction::WaitForSelector => wait_for_selector_result(page, arguments).await,
        BrowserAction::Text => text_result(page, arguments),
        BrowserAction::Attribute => attribute_result(page, arguments),
        BrowserAction::Click => click_result(page, arguments),
        BrowserAction::CloseSession => {
            unreachable!("close_session is handled before page dispatch")
        }
    }
}

async fn goto_with_timeout(page: &mut Page, url: &str, timeout_seconds: u64) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(timeout_seconds), page.goto(url))
        .await
        .with_context(|| format!("web_browser navigation timed out after {timeout_seconds}s"))??;
    Ok(())
}

fn evaluate_result(page: &mut Page, arguments: &WebBrowserArguments) -> Result<WebBrowserResult> {
    let script = arguments
        .script
        .as_deref()
        .context("web_browser evaluate requires `script`")?;

    Ok(WebBrowserResult {
        action: BrowserAction::Evaluate,
        status: "ok".to_string(),
        session_id: None,
        url: Some(page.url()),
        content: None,
        value: Some(page.evaluate(script)),
        truncated: false,
    })
}

async fn wait_for_selector_result(
    page: &mut Page,
    arguments: &WebBrowserArguments,
) -> Result<WebBrowserResult> {
    let selector = required_selector(arguments)?;
    page.wait_for_selector(selector, Duration::from_secs(arguments.timeout_seconds()))
        .await?;

    Ok(WebBrowserResult {
        action: BrowserAction::WaitForSelector,
        status: "ok".to_string(),
        session_id: None,
        url: Some(page.url()),
        content: None,
        value: Some(Value::String(selector.to_string())),
        truncated: false,
    })
}

fn text_result(page: &mut Page, arguments: &WebBrowserArguments) -> Result<WebBrowserResult> {
    let selector = required_selector(arguments)?;
    let text = page
        .query_selector(selector)
        .with_context(|| format!("selector not found: {selector}"))?
        .text();

    content_value_result(
        page,
        BrowserAction::Text,
        text,
        arguments.max_output_chars(),
    )
}

fn attribute_result(page: &mut Page, arguments: &WebBrowserArguments) -> Result<WebBrowserResult> {
    let selector = required_selector(arguments)?;
    let attribute = arguments
        .attribute
        .as_deref()
        .context("web_browser attribute requires `attribute`")?;
    let value = page
        .query_selector(selector)
        .with_context(|| format!("selector not found: {selector}"))?
        .attribute(attribute);

    Ok(WebBrowserResult {
        action: BrowserAction::Attribute,
        status: "ok".to_string(),
        session_id: None,
        url: Some(page.url()),
        content: None,
        value: Some(value.map_or(Value::Null, Value::String)),
        truncated: false,
    })
}

fn click_result(page: &mut Page, arguments: &WebBrowserArguments) -> Result<WebBrowserResult> {
    let selector = required_selector(arguments)?;
    page.query_selector(selector)
        .with_context(|| format!("selector not found: {selector}"))?
        .click()?;

    Ok(WebBrowserResult {
        action: BrowserAction::Click,
        status: "ok".to_string(),
        session_id: None,
        url: Some(page.url()),
        content: None,
        value: Some(Value::String(selector.to_string())),
        truncated: false,
    })
}

fn page_content_result(
    page: &mut Page,
    arguments: &WebBrowserArguments,
) -> Result<WebBrowserResult> {
    let content = match arguments.output_format() {
        OutputFormat::Html => page.content(),
        OutputFormat::Text => page
            .evaluate("document.body ? document.body.innerText : ''")
            .as_str()
            .unwrap_or_default()
            .to_string(),
    };

    content_value_result(
        page,
        arguments.action.clone(),
        content,
        arguments.max_output_chars(),
    )
}

fn content_value_result(
    page: &mut Page,
    action: BrowserAction,
    content: String,
    max_output_chars: usize,
) -> Result<WebBrowserResult> {
    let (content, truncated) = truncate_output(&content, max_output_chars);
    Ok(WebBrowserResult {
        action,
        status: "ok".to_string(),
        session_id: None,
        url: Some(page.url()),
        content: Some(content),
        value: None,
        truncated,
    })
}

fn required_selector(arguments: &WebBrowserArguments) -> Result<&str> {
    arguments
        .selector
        .as_deref()
        .context("web_browser action requires `selector`")
}
