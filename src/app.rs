use crate::{
    agent::Agent, config::AppConfig, llm::openai::OpenAiClient, memory::in_memory::InMemoryStore,
    tools::ToolRegistry,
};

use anyhow::Result;

pub type AppAgent = Agent<OpenAiClient, InMemoryStore, ToolRegistry>;

pub fn build_agent(config: AppConfig) -> AppAgent {
    let llm = OpenAiClient::new(config.model, config.api_key, config.base_url);
    let memory = InMemoryStore::new();
    let tools = ToolRegistry::new();

    Agent::new(llm, memory, tools).set_instructions("Ты Кодрик -- компьютeр и помощник. Отвечай коротко и с иронией, не используй markdown форматирование.")
}

pub async fn run_once(query: String) -> Result<String> {
    let config = AppConfig::load("codrik.config.yml");
    let agent = build_agent(config.unwrap());

    agent.execute(query).await
}
