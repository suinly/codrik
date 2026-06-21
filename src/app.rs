use crate::{
    agent::{
        Agent,
        tool::{Tool, ToolParameters},
    },
    config::AppConfig,
    llm::openai::OpenAiClient,
    memory::in_memory::InMemoryStore,
};

use anyhow::Result;

pub type AppAgent = Agent<OpenAiClient, InMemoryStore>;

pub fn build_agent(config: AppConfig) -> AppAgent {
    let llm = OpenAiClient::new()
        .set_api_key(config.api_key)
        .set_base_url(config.base_url)
        .set_model(config.model);

    let memory = InMemoryStore::new();

    Agent::new(llm, memory)
        .set_instructions("Ты Кодрик -- компьютeр и помощник. Отвечай коротко и с иронией, не используй markdown форматирование.")
        .add_tool(Tool::new(
            "hello_world",
            "Just print Hello World",
            ToolParameters::new(),
        ))
}

pub async fn run_once(query: String) -> Result<String> {
    let config = AppConfig::load("codrik.config.yml");
    let agent = build_agent(config.unwrap());

    agent.execute(query).await
}
