use std::env;

use crate::{
    agent::Agent,
    auth::AuthorizedActor,
    config::{AppConfig, codrik_dir},
    llm::{client::LlmStreamSink, openai::OpenAiClient},
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

    Agent::new(llm, memory, tools).set_instructions(default_agent_instructions())
}

pub async fn run_once(query: String) -> Result<String> {
    let config = AppConfig::load_default()?;

    run_once_with_config(query, config).await
}

pub async fn run_once_with_config(query: String, config: AppConfig) -> Result<String> {
    let agent = build_agent(config);

    agent.execute(query).await
}

pub async fn run_once_streaming(
    query: String,
    config: AppConfig,
    sink: &mut dyn LlmStreamSink,
) -> Result<String> {
    let agent = build_agent(config);

    agent.execute_streaming(query, sink).await
}

pub async fn run_once_with_session(
    query: String,
    config: AppConfig,
    session_id: impl AsRef<str>,
) -> Result<String> {
    let memory = FileMemoryStore::new(codrik_dir()?.join("sessions"), session_id)?;
    let agent = build_agent_with_memory(config, memory);

    agent.execute(query).await
}

pub async fn run_once_with_session_streaming(
    query: String,
    config: AppConfig,
    session_id: impl AsRef<str>,
    sink: &mut dyn LlmStreamSink,
) -> Result<String> {
    let memory = FileMemoryStore::new(codrik_dir()?.join("sessions"), session_id)?;
    let agent = build_agent_with_memory(config, memory);

    agent.execute_streaming(query, sink).await
}

pub async fn run_once_with_actor_session(
    query: String,
    config: AppConfig,
    actor: AuthorizedActor,
    session_id: impl AsRef<str>,
) -> Result<String> {
    let memory = FileMemoryStore::new(codrik_dir()?.join("sessions"), session_id)?;
    let agent = build_agent_for_actor(config, memory, actor);

    agent.execute(query).await
}

pub async fn run_once_with_actor_session_streaming(
    query: String,
    config: AppConfig,
    actor: AuthorizedActor,
    session_id: impl AsRef<str>,
    sink: &mut dyn LlmStreamSink,
) -> Result<String> {
    let memory = FileMemoryStore::new(codrik_dir()?.join("sessions"), session_id)?;
    let agent = build_agent_for_actor(config, memory, actor);

    agent.execute_streaming(query, sink).await
}

fn build_agent_for_actor<M>(
    config: AppConfig,
    memory: M,
    actor: AuthorizedActor,
) -> Agent<OpenAiClient, M, ToolRegistry>
where
    M: MemoryStore,
{
    let llm = OpenAiClient::new(config.model, config.api_key, config.base_url);
    let tools = ToolRegistry::with_allowed_tools(actor.tools);

    Agent::new(llm, memory, tools).set_instructions(default_agent_instructions())
}

fn default_agent_instructions() -> String {
    format!(
        concat!(
            "You are Codrik, an entity living inside this computer. ",
            "This computer is running {os} on {arch}. ",
            "You are not only a text agent: you have access to the system through the available tools. ",
            "Use the bash tool when you need to inspect or change the system, run programs, play sounds, open windows, display visuals, or perform other host-side actions. ",
            "Commands may have side effects beyond text output. ",
            "Answer briefly and with irony. Do not use markdown formatting."
        ),
        os = env::consts::OS,
        arch = env::consts::ARCH
    )
}
