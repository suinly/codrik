use crate::{
    agent::Agent,
    auth::AuthorizedActor,
    config::{AppConfig, codrik_dir},
    llm::{
        client::{LlmStreamSink, RunContext},
        openai::OpenAiClient,
    },
    memory::{file::FileMemoryStore, in_memory::InMemoryStore, store::MemoryStore},
    tools::{ToolRegistry, ToolRegistryConfig},
};

use std::path::PathBuf;

use anyhow::{Context, Result, bail};

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

pub async fn run_once_with_actor_session_in_root_and_context(
    query: String,
    config: AppConfig,
    actor: AuthorizedActor,
    session_root: PathBuf,
    session_id: impl AsRef<str>,
    context: &RunContext,
) -> Result<String> {
    let memory = FileMemoryStore::new(session_root, session_id)?;
    let agent = build_agent_for_actor(config, memory, actor)?;

    agent.execute_with_context(query, context).await
}

pub async fn run_once_with_actor_session_streaming_in_root_and_context(
    query: String,
    config: AppConfig,
    actor: AuthorizedActor,
    session_root: PathBuf,
    session_id: impl AsRef<str>,
    sink: &mut dyn LlmStreamSink,
    context: &RunContext,
) -> Result<String> {
    let memory = FileMemoryStore::new(session_root, session_id)?;
    let agent = build_agent_for_actor(config, memory, actor)?;

    agent
        .execute_streaming_with_context(query, sink, context)
        .await
}

fn build_agent_for_actor<M>(
    config: AppConfig,
    memory: M,
    actor: AuthorizedActor,
) -> Result<Agent<OpenAiClient, M, ToolRegistry>>
where
    M: MemoryStore,
{
    let llm = OpenAiClient::new(config.model, config.api_key, config.base_url);
    let workspace = actor_workspace_path(&actor.id)?;
    let tools = ToolRegistry::with_allowed_tools_and_config(
        actor.tools,
        ToolRegistryConfig {
            bashkit_workspace: Some(workspace),
        },
    );

    Ok(Agent::new(llm, memory, tools).set_instructions(default_agent_instructions()))
}

fn actor_workspace_path(actor_id: &str) -> Result<std::path::PathBuf> {
    if actor_id.is_empty()
        || actor_id == "."
        || actor_id == ".."
        || actor_id.contains('/')
        || actor_id.contains('\\')
    {
        bail!("unsafe actor id for workspace path: {actor_id}");
    }

    Ok(codrik_dir()
        .context("failed to resolve codrik directory for actor workspace")?
        .join("workspaces")
        .join(actor_id))
}

fn default_agent_instructions() -> String {
    include_str!("../agent_instructions.md")
        .trim_end()
        .to_string()
}
