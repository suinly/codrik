use std::path::Path;

use crate::{
    agent::Agent,
    config::AppConfig,
    llm::openai::OpenAiClient,
    memory::{file::FileMemoryStore, in_memory::InMemoryStore, store::MemoryStore},
    tools::ToolRegistry,
};

use anyhow::Result;

pub type AppAgent = Agent<OpenAiClient, InMemoryStore, ToolRegistry>;

pub fn build_agent(config: AppConfig) -> AppAgent {
    build_agent_with_memory(config, InMemoryStore::new())
}

fn build_agent_with_memory<M>(config: AppConfig, memory: M) -> Agent<OpenAiClient, M, ToolRegistry>
where
    M: MemoryStore,
{
    let llm = OpenAiClient::new(config.model, config.api_key, config.base_url);
    let tools = ToolRegistry::new();

    Agent::new(llm, memory, tools).set_instructions("Ты Кодрик -- компьютeр и помощник. Отвечай коротко и с иронией, не используй markdown форматирование.")
}

pub async fn run_once(query: String) -> Result<String> {
    let config = AppConfig::load("codrik.config.yml");

    run_once_with_config(query, config.unwrap()).await
}

pub async fn run_once_with_config(query: String, config: AppConfig) -> Result<String> {
    let agent = build_agent(config);

    agent.execute(query).await
}

pub async fn run_once_with_session(
    query: String,
    config: AppConfig,
    session_id: impl AsRef<str>,
) -> Result<String> {
    let memory = FileMemoryStore::new(Path::new(".codrik").join("sessions"), session_id)?;
    let agent = build_agent_with_memory(config, memory);

    agent.execute(query).await
}
