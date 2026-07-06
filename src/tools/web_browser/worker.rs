use std::{collections::HashMap, thread};

use anyhow::{Context, Result, bail};
use obscura::{Browser, Page};
use tokio::sync::{mpsc, oneshot};

use super::{
    WebBrowserToolConfig,
    actions::run_page_action,
    types::{BrowserAction, WebBrowserArguments, WebBrowserResult},
};

const MAX_SESSIONS: usize = 4;

struct BrowserSession {
    _browser: Browser,
    page: Page,
}

pub(super) enum BrowserWorkerCommand {
    Execute {
        arguments: WebBrowserArguments,
        response: oneshot::Sender<Result<WebBrowserResult, String>>,
    },
    #[cfg(test)]
    InsertTestSession {
        session_id: String,
        response: oneshot::Sender<Result<(), String>>,
    },
}

pub(super) fn start_browser_worker(
    config: WebBrowserToolConfig,
    mut receiver: mpsc::Receiver<BrowserWorkerCommand>,
) {
    thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("web_browser worker runtime should build");
        let mut sessions = HashMap::new();

        runtime.block_on(async move {
            while let Some(command) = receiver.recv().await {
                match command {
                    BrowserWorkerCommand::Execute {
                        arguments,
                        response,
                    } => {
                        let result = execute_worker_action(&config, &mut sessions, arguments).await;
                        let _ = response.send(result.map_err(|error| error.to_string()));
                    }
                    #[cfg(test)]
                    BrowserWorkerCommand::InsertTestSession {
                        session_id,
                        response,
                    } => {
                        let result = insert_test_session(&config, &mut sessions, session_id).await;
                        let _ = response.send(result.map_err(|error| error.to_string()));
                    }
                }
            }
        });
    });
}

async fn execute_worker_action(
    config: &WebBrowserToolConfig,
    sessions: &mut HashMap<String, BrowserSession>,
    arguments: WebBrowserArguments,
) -> Result<WebBrowserResult> {
    if arguments.action == BrowserAction::CloseSession {
        return close_session(sessions, arguments);
    }

    if let Some(session_id) = arguments.session_id.clone() {
        return execute_session_action(config, sessions, session_id, arguments).await;
    }

    if arguments.action == BrowserAction::Content && arguments.url.is_none() {
        bail!("web_browser content requires `url` when `session_id` is not provided");
    }

    execute_stateless_action(config, arguments).await
}

fn close_session(
    sessions: &mut HashMap<String, BrowserSession>,
    arguments: WebBrowserArguments,
) -> Result<WebBrowserResult> {
    let session_id = arguments
        .session_id
        .clone()
        .context("web_browser close_session requires `session_id`")?;
    sessions.remove(&session_id);

    Ok(WebBrowserResult {
        action: BrowserAction::CloseSession,
        status: "closed".to_string(),
        session_id: Some(session_id),
        url: None,
        content: None,
        value: None,
        truncated: false,
    })
}

async fn execute_session_action(
    config: &WebBrowserToolConfig,
    sessions: &mut HashMap<String, BrowserSession>,
    session_id: String,
    arguments: WebBrowserArguments,
) -> Result<WebBrowserResult> {
    if arguments.action == BrowserAction::Goto && !sessions.contains_key(&session_id) {
        insert_session(config, sessions, session_id.clone()).await?;
    }

    let session = sessions
        .get_mut(&session_id)
        .with_context(|| format!("unknown browser session: {session_id}"))?;
    let mut result = run_page_action(&mut session.page, &arguments).await?;
    result.session_id = Some(session_id);
    Ok(result)
}

async fn execute_stateless_action(
    config: &WebBrowserToolConfig,
    arguments: WebBrowserArguments,
) -> Result<WebBrowserResult> {
    let browser = Browser::builder().stealth(config.stealth).build()?;
    let mut page = browser.new_page().await?;

    run_page_action(&mut page, &arguments).await
}

async fn insert_session(
    config: &WebBrowserToolConfig,
    sessions: &mut HashMap<String, BrowserSession>,
    session_id: String,
) -> Result<()> {
    if sessions.len() >= MAX_SESSIONS {
        bail!("web_browser session limit reached; close an existing session first");
    }

    let browser = Browser::builder().stealth(config.stealth).build()?;
    let page = browser.new_page().await?;
    sessions.insert(
        session_id,
        BrowserSession {
            _browser: browser,
            page,
        },
    );
    Ok(())
}

#[cfg(test)]
async fn insert_test_session(
    config: &WebBrowserToolConfig,
    sessions: &mut HashMap<String, BrowserSession>,
    session_id: String,
) -> Result<()> {
    insert_session(config, sessions, session_id).await
}
