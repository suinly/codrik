use crate::{
    agent::Agent,
    auth::AuthorizedActor,
    config::{AppConfig, codrik_dir},
    llm::{
        client::{AgentActivitySink, LlmStreamSink, NoopAgentActivitySink, RunContext},
        openai::OpenAiClient,
    },
    memory::{file::FileMemoryStore, in_memory::InMemoryStore, store::MemoryStore},
    skills::{Skill, SkillRegistry, SkillRoot},
    tools::{ToolRegistry, ToolRegistryConfig},
};

use std::path::PathBuf;

use anyhow::{Context, Result, bail};

const MAX_SKILL_INDEX_CHARS: usize = 8_000;

pub type AppAgent = Agent<OpenAiClient, InMemoryStore, ToolRegistry>;

pub fn build_agent(config: AppConfig) -> AppAgent {
    build_agent_with_memory(config, InMemoryStore::new())
}

fn build_agent_with_memory<M>(config: AppConfig, memory: M) -> Agent<OpenAiClient, M, ToolRegistry>
where
    M: MemoryStore,
{
    let llm = OpenAiClient::new(config.model, config.api_key, config.base_url);
    let tool_config = default_tool_config().expect("failed to build default tool config");
    let instructions = agent_instructions_for_tool_config(&tool_config);
    let tools = ToolRegistry::with_config(tool_config);

    Agent::new(llm, memory, tools).set_instructions(instructions)
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
    let mut activity = NoopAgentActivitySink;
    run_once_with_actor_session_streaming_and_activity_in_root_and_context(
        query,
        config,
        actor,
        session_root,
        session_id,
        sink,
        &mut activity,
        context,
    )
    .await
}

pub async fn run_once_with_actor_session_streaming_and_activity_in_root_and_context(
    query: String,
    config: AppConfig,
    actor: AuthorizedActor,
    session_root: PathBuf,
    session_id: impl AsRef<str>,
    sink: &mut dyn LlmStreamSink,
    activity: &mut dyn AgentActivitySink,
    context: &RunContext,
) -> Result<String> {
    let memory = FileMemoryStore::new(session_root, session_id)?;
    let agent = build_agent_for_actor(config, memory, actor)?;

    agent
        .execute_streaming_with_context_and_activity(query, sink, activity, context)
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
    let tool_config = actor_tool_config(&actor)?;
    let instructions = agent_instructions_for_tool_config(&tool_config);
    let tools = ToolRegistry::with_allowed_tools_and_config(actor.tools, tool_config);

    Ok(Agent::new(llm, memory, tools).set_instructions(instructions))
}

fn default_tool_config() -> Result<ToolRegistryConfig> {
    Ok(ToolRegistryConfig {
        bashkit_workspace: None,
        skill_roots: default_skill_roots()?,
    })
}

fn actor_tool_config(actor: &AuthorizedActor) -> Result<ToolRegistryConfig> {
    Ok(ToolRegistryConfig {
        bashkit_workspace: Some(actor_workspace_path(&actor.id)?),
        skill_roots: default_skill_roots()?,
    })
}

fn default_skill_roots() -> Result<Vec<SkillRoot>> {
    Ok(vec![
        SkillRoot::read_only(PathBuf::from(".codrik").join("skills"), "project"),
        SkillRoot::writable(codrik_dir()?.join("skills"), "user"),
    ])
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

fn agent_instructions_for_tool_config(tool_config: &ToolRegistryConfig) -> String {
    let mut instructions = default_agent_instructions();
    let Ok(skills) = SkillRegistry::new(tool_config.skill_roots.clone()).list() else {
        return instructions;
    };

    if let Some(skill_index) = skill_index_section(&skills) {
        instructions.push_str("\n\n");
        instructions.push_str(&skill_index);
    }

    instructions
}

fn skill_index_section(skills: &[Skill]) -> Option<String> {
    if skills.is_empty() {
        return None;
    }

    let mut section = String::from("## Available Skills\n\n");
    section.push_str(
        "These local skills are available for implicit matching. Use `skills_read` to load the full `SKILL.md` before following a selected skill.\n\n",
    );

    let mut omitted = 0;
    for skill in skills {
        let line = format!(
            "- {} ({}): {}\n",
            skill.name, skill.source, skill.description
        );
        if section.len() + line.len() > MAX_SKILL_INDEX_CHARS {
            omitted += 1;
            continue;
        }

        section.push_str(&line);
    }

    if omitted > 0 {
        let line = format!("- ... {omitted} more skills omitted from the compact index.\n");
        if section.len() + line.len() <= MAX_SKILL_INDEX_CHARS {
            section.push_str(&line);
        }
    }

    Some(section.trim_end().to_string())
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::Path,
        sync::atomic::{AtomicU64, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::*;

    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn default_skill_roots_prefer_project_then_user() -> Result<()> {
        let roots = default_skill_roots()?;

        assert_eq!(
            roots,
            vec![
                SkillRoot::read_only(PathBuf::from(".codrik").join("skills"), "project"),
                SkillRoot::writable(codrik_dir()?.join("skills"), "user"),
            ]
        );
        Ok(())
    }

    #[test]
    fn agent_instructions_include_available_skill_metadata() -> Result<()> {
        let root = temp_root("skill-index")?;
        write_skill(
            &root,
            "meduza_daily_summary",
            "---\nname: meduza_daily_summary\ndescription: Use for Meduza news digests and news today requests.\n---\n\n# Secret full instructions\n",
        )?;
        let tool_config = ToolRegistryConfig {
            bashkit_workspace: None,
            skill_roots: vec![SkillRoot::read_only(&root, "test")],
        };

        let instructions = agent_instructions_for_tool_config(&tool_config);

        assert!(instructions.contains("## Available Skills"));
        assert!(instructions.contains(
            "- meduza_daily_summary (test): Use for Meduza news digests and news today requests."
        ));
        assert!(!instructions.contains("# Secret full instructions"));
        Ok(())
    }

    #[test]
    fn agent_instructions_omit_skill_index_when_no_skills_exist() -> Result<()> {
        let tool_config = ToolRegistryConfig {
            bashkit_workspace: None,
            skill_roots: vec![SkillRoot::read_only(temp_root("empty")?, "test")],
        };

        let instructions = agent_instructions_for_tool_config(&tool_config);

        assert!(!instructions.contains("## Available Skills"));
        Ok(())
    }

    fn write_skill(root: &Path, name: &str, content: &str) -> Result<()> {
        let dir = root.join(name);
        fs::create_dir_all(&dir)?;
        fs::write(dir.join("SKILL.md"), content)?;
        Ok(())
    }

    fn temp_root(label: &str) -> Result<PathBuf> {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)?
            .as_nanos()
            .to_string();
        let path = std::env::temp_dir().join(format!(
            "codrik-app-skills-{label}-{}-{unique}",
            TEMP_COUNTER.fetch_add(1, Ordering::SeqCst)
        ));
        fs::create_dir_all(&path)?;
        Ok(path)
    }
}
