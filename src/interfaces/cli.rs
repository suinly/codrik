use anyhow::{Context, Result};
use std::env;

use crate::app;

pub async fn run() -> Result<()> {
    let query = env::args().nth(1).context("missing query")?;

    let result = app::run_once(query).await?;

    println!("Agent: {}", result);

    Ok(())
}
