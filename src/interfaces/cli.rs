use anyhow::{Context, Result, bail};
use std::{
    env,
    io::{self, Write},
    sync::{Arc, Mutex},
};
use tokio::{task::JoinHandle, time::Duration};

use crate::{
    app,
    config::AppConfig,
    interfaces::telegram,
    llm::client::{LlmStreamEvent, LlmStreamSink},
    updater,
};

pub async fn run() -> Result<()> {
    match CliCommand::parse(env::args().skip(1))? {
        CliCommand::Update => updater::update().await,
        CliCommand::Gateway { name } => match name.as_str() {
            "telegram" => {
                let config = AppConfig::load_default()?;
                telegram::run(config).await
            }
            _ => bail!("unknown gateway: {name}"),
        },
        CliCommand::Session { session_id, query } => {
            let config = AppConfig::load_default()?;
            let result = app::run_once_with_session(query, config, session_id).await?;

            println!("Agent: {}", result);

            Ok(())
        }
        CliCommand::StreamingSession { session_id, query } => {
            let config = AppConfig::load_default()?;
            let mut renderer = StdoutStreamRenderer::start()?;

            let result =
                app::run_once_with_session_streaming(query, config, session_id, &mut renderer)
                    .await;
            renderer.finish()?;
            println!();

            result.map(|_| ())
        }
        CliCommand::OneShot { query } => {
            let result = app::run_once(query).await?;

            println!("Agent: {}", result);

            Ok(())
        }
        CliCommand::StreamingOneShot { query } => {
            let config = AppConfig::load_default()?;
            let mut renderer = StdoutStreamRenderer::start()?;

            let result = app::run_once_streaming(query, config, &mut renderer).await;
            renderer.finish()?;
            println!();

            result.map(|_| ())
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
enum CliCommand {
    Update,
    Gateway { name: String },
    Session { session_id: String, query: String },
    StreamingSession { session_id: String, query: String },
    OneShot { query: String },
    StreamingOneShot { query: String },
}

impl CliCommand {
    fn parse(args: impl IntoIterator<Item = String>) -> Result<Self> {
        let mut args = args.into_iter();
        let command = args.next().context("missing query or command")?;

        if command == "update" {
            return Ok(Self::Update);
        }

        if command == "gateway" {
            return Ok(Self::Gateway {
                name: args.next().context("missing gateway name")?,
            });
        }

        if command == "--session" {
            return Ok(Self::Session {
                session_id: args.next().context("missing session id")?,
                query: args.next().context("missing query")?,
            });
        }

        if command == "--stream" {
            let next = args.next().context("missing query")?;

            if next == "--session" {
                return Ok(Self::StreamingSession {
                    session_id: args.next().context("missing session id")?,
                    query: args.next().context("missing query")?,
                });
            }

            return Ok(Self::StreamingOneShot { query: next });
        }

        Ok(Self::OneShot { query: command })
    }
}

struct StdoutStreamRenderer {
    state: Arc<Mutex<RenderState>>,
    animation: JoinHandle<()>,
}

struct RenderState {
    frame: usize,
    spinner_visible: bool,
    has_text: bool,
}

impl StdoutStreamRenderer {
    fn start() -> Result<Self> {
        write_stdout("Agent: ")?;

        let state = Arc::new(Mutex::new(RenderState {
            frame: 0,
            spinner_visible: false,
            has_text: false,
        }));

        {
            let mut state = state.lock().expect("stream renderer state lock poisoned");
            draw_spinner(&mut state)?;
            flush_stdout()?;
        }

        let animation_state = Arc::clone(&state);
        let animation = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(120));

            loop {
                interval.tick().await;

                let Ok(mut state) = animation_state.lock() else {
                    return;
                };

                if erase_spinner(&mut state).is_err() {
                    return;
                }
                state.frame = (state.frame + 1) % SPINNER_FRAMES.len();
                if draw_spinner(&mut state).is_err() {
                    return;
                }
                if flush_stdout().is_err() {
                    return;
                }
            }
        });

        Ok(Self { state, animation })
    }

    fn finish(self) -> Result<()> {
        self.animation.abort();

        {
            let mut state = self
                .state
                .lock()
                .expect("stream renderer state lock poisoned");
            erase_spinner(&mut state)?;
            flush_stdout()?;
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl LlmStreamSink for StdoutStreamRenderer {
    async fn on_event(&mut self, event: LlmStreamEvent) -> Result<()> {
        if let LlmStreamEvent::TextDelta(delta) = event {
            let mut state = self
                .state
                .lock()
                .expect("stream renderer state lock poisoned");
            let delta = if state.has_text {
                delta.as_str()
            } else {
                delta.trim_start_matches(['\r', '\n'])
            };

            if delta.is_empty() {
                return Ok(());
            }

            erase_spinner(&mut state)?;
            write_stdout(delta)?;
            state.has_text = true;
            draw_spinner(&mut state)?;
            flush_stdout()?;
        }

        Ok(())
    }
}

const SPINNER_FRAMES: [&str; 4] = ["|", "/", "-", "\\"];

fn draw_spinner(state: &mut RenderState) -> io::Result<()> {
    write_stdout(SPINNER_FRAMES[state.frame])?;
    state.spinner_visible = true;

    Ok(())
}

fn erase_spinner(state: &mut RenderState) -> io::Result<()> {
    if state.spinner_visible {
        write_stdout("\u{8} \u{8}")?;
        state.spinner_visible = false;
    }

    Ok(())
}

fn write_stdout(text: &str) -> io::Result<()> {
    io::stdout().write_all(text.as_bytes())
}

fn flush_stdout() -> io::Result<()> {
    io::stdout().flush()
}

#[cfg(test)]
mod tests {
    use anyhow::Result;

    use super::CliCommand;

    #[test]
    fn parses_gateway_command() -> Result<()> {
        let command = CliCommand::parse(["gateway", "telegram"].map(String::from))?;

        assert_eq!(
            command,
            CliCommand::Gateway {
                name: "telegram".to_string()
            }
        );

        Ok(())
    }

    #[test]
    fn parses_update_command() -> Result<()> {
        let command = CliCommand::parse(["update"].map(String::from))?;

        assert_eq!(command, CliCommand::Update);

        Ok(())
    }

    #[test]
    fn parses_session_command() -> Result<()> {
        let command = CliCommand::parse(["--session", "work", "hello"].map(String::from))?;

        assert_eq!(
            command,
            CliCommand::Session {
                session_id: "work".to_string(),
                query: "hello".to_string(),
            }
        );

        Ok(())
    }

    #[test]
    fn parses_streaming_session_command() -> Result<()> {
        let command =
            CliCommand::parse(["--stream", "--session", "work", "hello"].map(String::from))?;

        assert_eq!(
            command,
            CliCommand::StreamingSession {
                session_id: "work".to_string(),
                query: "hello".to_string(),
            }
        );

        Ok(())
    }

    #[test]
    fn parses_one_shot_query() -> Result<()> {
        let command = CliCommand::parse(["hello"].map(String::from))?;

        assert_eq!(
            command,
            CliCommand::OneShot {
                query: "hello".to_string()
            }
        );

        Ok(())
    }

    #[test]
    fn parses_streaming_one_shot_query() -> Result<()> {
        let command = CliCommand::parse(["--stream", "hello"].map(String::from))?;

        assert_eq!(
            command,
            CliCommand::StreamingOneShot {
                query: "hello".to_string()
            }
        );

        Ok(())
    }
}
