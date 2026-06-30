use anyhow::{Context, Result, bail};
use std::env;

use crate::{app, config::AppConfig, interfaces::telegram, updater};

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
        CliCommand::OneShot { query } => {
            let result = app::run_once(query).await?;

            println!("Agent: {}", result);

            Ok(())
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
enum CliCommand {
    Update,
    Gateway { name: String },
    Session { session_id: String, query: String },
    OneShot { query: String },
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

        Ok(Self::OneShot { query: command })
    }
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
}
