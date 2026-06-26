use anyhow::{Context, Result, bail};
use std::env;

use crate::{app, config::AppConfig, interfaces::telegram};

pub async fn run() -> Result<()> {
    let mut args = env::args().skip(1);
    let command = args.next().context("missing query or command")?;

    if command == "gateway" {
        let gateway = args.next().context("missing gateway name")?;

        return match gateway.as_str() {
            "telegram" => {
                let config = AppConfig::load("codrik.config.yml")?;
                telegram::run(config).await
            }
            _ => bail!("unknown gateway: {gateway}"),
        };
    }

    if command == "--session" {
        let session_id = args.next().context("missing session id")?;
        let query = args.next().context("missing query")?;
        let config = AppConfig::load("codrik.config.yml")?;
        let result = app::run_once_with_session(query, config, session_id).await?;

        println!("Agent: {}", result);

        return Ok(());
    }

    let result = app::run_once(command).await?;

    println!("Agent: {}", result);

    Ok(())
}
