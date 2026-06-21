use anyhow::{Context, Result};
use std::env;

use crate::{
    agent::Agent, config::AppConfig, llm::openai::OpenAiClient, memory::in_memory::InMemoryStore,
};

pub async fn run() -> Result<()> {
    let config = AppConfig::load("codrik.config.yml")?;

    let llm = OpenAiClient::new()
        .set_api_key(config.api_key)
        .set_base_url(config.base_url)
        .set_model(config.model);

    let memory = InMemoryStore::new();

    let agent = Agent::new(llm, memory)
        .set_instructions("Ты Кодрик -- компьютeр и помощник. Отвечай коротко и с иронией, не используй markdown форматирование.");

    let args: Vec<String> = env::args().collect();
    let query = args.get(1).context("missing query")?;

    let result = agent.execute(query.clone()).await?;

    println!("Agent: {}", result);

    Ok(())
}
