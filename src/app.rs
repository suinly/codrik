use crate::{
    config::{AppConfig, RuntimePaths, codrik_dir},
    llm::{client::LlmStreamClient, openai::OpenAiClient},
    runtime::{
        artifacts::ArtifactManager,
        dispatcher::ActorDispatcher,
        hooks::{NoopRuntimeBoundaryHooks, RuntimeBoundaryHooks},
        identity_link::{IdentityLinkManager, IdentityLinkService, SystemLinkCodeGenerator},
        instance_lock::InstanceLock,
        ipc::{
            security::{create_secure_directory, validate_secure_directory},
            server::LocalIpcServer,
        },
        model::{ActorId, Clock, SystemClock},
        observability::{
            RuntimeComponent, RuntimeLogEvent, RuntimeLogger, RuntimeRecoveryCounts,
            RuntimeTransition, StderrRuntimeLogger,
        },
        outbox_worker::OutboxWorker,
        runner::{ActorRunner, RunnerLimits},
        signals::ActorSignals,
        sqlite::{RUNTIME_SCHEMA_VERSION, SqliteRuntimeStore},
        store::{ActorStore, RuntimeStore},
        stream_hub::StreamHub,
        supervisor::{ServeRuntime, Supervisor},
    },
    skills::{Skill, SkillRegistry, SkillRoot, builtin_skill_root},
    tools::{FileRoot, ToolRegistry, ToolRegistryConfig},
};

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result, bail};

const MAX_SKILL_INDEX_CHARS: usize = 8_000;
const ARTIFACT_GC_INTERVAL: std::time::Duration = std::time::Duration::from_secs(300);
const IDENTITY_LINK_GC_INTERVAL: std::time::Duration = std::time::Duration::from_secs(300);
const IDENTITY_LINK_GC_BATCH: usize = 256;

pub async fn serve(config: AppConfig) -> Result<()> {
    let home = codrik_dir()?;
    let llm = OpenAiClient::new(
        config.model.clone(),
        config.api_key.clone(),
        config.base_url.clone(),
    );
    serve_at_until(
        config,
        Arc::new(StderrRuntimeLogger::default()),
        &NoopStartupTrace,
        home,
        SystemClock,
        llm,
        shutdown_signal(),
    )
    .await
}

#[doc(hidden)]
pub async fn serve_with_dependencies<C, L, F>(
    config: AppConfig,
    home: PathBuf,
    clock: C,
    llm: L,
    shutdown: F,
) -> Result<()>
where
    C: Clock,
    L: LlmStreamClient + Send + Sync + 'static,
    F: std::future::Future<Output = ()>,
{
    serve_at_until(
        config,
        Arc::new(crate::runtime::observability::NoopRuntimeLogger),
        &NoopStartupTrace,
        home,
        clock,
        llm,
        shutdown,
    )
    .await
}

#[doc(hidden)]
pub async fn serve_with_dependencies_and_hooks<C, L, F>(
    config: AppConfig,
    home: PathBuf,
    clock: C,
    llm: L,
    hooks: Arc<dyn RuntimeBoundaryHooks>,
    shutdown: F,
) -> Result<()>
where
    C: Clock,
    L: LlmStreamClient + Send + Sync + 'static,
    F: std::future::Future<Output = ()>,
{
    serve_at_until_with_hooks(
        config,
        Arc::new(crate::runtime::observability::NoopRuntimeLogger),
        &NoopStartupTrace,
        home,
        clock,
        llm,
        hooks,
        shutdown,
    )
    .await
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StartupPhase {
    PathsValidated,
    LockAcquired,
    Migrated,
    ActorBootstrapped,
    ActorVerified,
    ParentsValidated,
    StaleSocketRemoved,
    SocketBound,
    Recovered,
    ArtifactsCollected,
    Ready,
}

trait StartupTrace: Sync {
    fn record(&self, phase: StartupPhase);
}

struct NoopStartupTrace;

impl StartupTrace for NoopStartupTrace {
    fn record(&self, _phase: StartupPhase) {}
}

async fn serve_at_until<C, L, F>(
    config: AppConfig,
    logger: Arc<dyn RuntimeLogger>,
    trace: &dyn StartupTrace,
    home: PathBuf,
    clock: C,
    llm: L,
    shutdown: F,
) -> Result<()>
where
    C: Clock,
    L: LlmStreamClient + Send + Sync + 'static,
    F: std::future::Future<Output = ()>,
{
    serve_at_until_with_hooks(
        config,
        logger,
        trace,
        home,
        clock,
        llm,
        Arc::new(NoopRuntimeBoundaryHooks),
        shutdown,
    )
    .await
}

async fn serve_at_until_with_hooks<C, L, F>(
    config: AppConfig,
    logger: Arc<dyn RuntimeLogger>,
    trace: &dyn StartupTrace,
    home: PathBuf,
    clock: C,
    llm: L,
    hooks: Arc<dyn RuntimeBoundaryHooks>,
    shutdown: F,
) -> Result<()>
where
    C: Clock,
    L: LlmStreamClient + Send + Sync + 'static,
    F: std::future::Future<Output = ()>,
{
    let runtime = config.required_runtime()?.clone();
    let paths = runtime.resolve_paths(&home)?;
    prepare_paths(&home, &paths)?;
    trace.record(StartupPhase::PathsValidated);
    let lock = InstanceLock::acquire(&paths.lock, &paths.socket)?;
    trace.record(StartupPhase::LockAcquired);
    let store = SqliteRuntimeStore::open(&paths.database).await?;
    trace.record(StartupPhase::Migrated);
    let actor_id = ActorId::parse_workspace_safe(&runtime.actor_id)?;
    store
        .ensure_initial_actor(&actor_id, &["*".to_string()], clock.now())
        .await?;
    trace.record(StartupPhase::ActorBootstrapped);
    let actor = store
        .load_actor(&actor_id)
        .await?
        .with_context(|| format!("configured runtime actor {actor_id} does not exist"))?;
    if !actor.enabled {
        bail!("configured runtime actor {actor_id} is disabled");
    }
    trace.record(StartupPhase::ActorVerified);
    validate_runtime_paths(&home, &paths)?;
    trace.record(StartupPhase::ParentsValidated);
    lock.remove_stale_socket()?;
    trace.record(StartupPhase::StaleSocketRemoved);

    let signals = ActorSignals::default();
    let hub = Arc::new(StreamHub::default());
    let outbox_owner = format!("outbox-{}", std::process::id());
    let dispatcher_owner = format!("dispatcher-{}", std::process::id());
    let outbox = Arc::new(OutboxWorker::new(
        Arc::new(store.clone()),
        hub.clone(),
        clock.clone(),
        outbox_owner.clone(),
    ));
    let identity_linking: Arc<dyn IdentityLinkManager> = Arc::new(IdentityLinkService::new(
        store.clone(),
        clock.clone(),
        SystemLinkCodeGenerator,
    ));
    let server = LocalIpcServer::bind_with_hooks(
        &paths.socket,
        actor_id.clone(),
        Arc::new(store.clone()),
        outbox.clone(),
        hub.clone(),
        hooks.clone(),
    )?
    .with_actor_signals(signals.clone())
    .with_identity_linking(identity_linking.clone());
    trace.record(StartupPhase::SocketBound);
    let recovery = store.recover_startup(clock.now()).await?;
    trace.record(StartupPhase::Recovered);
    let artifacts = ArtifactManager::new(paths.artifacts.clone(), store.clone(), clock.clone());
    artifacts.collect_garbage(clock.now()).await?;
    identity_linking
        .collect_expired(IDENTITY_LINK_GC_BATCH)
        .await?;
    trace.record(StartupPhase::ArtifactsCollected);
    let tool_config =
        tool_config_for_actor_workspace(actor_workspace_path_in(&home, actor.id.as_str())?)?;
    let instructions = agent_instructions_for_tool_config(&tool_config);
    let tools = ToolRegistry::with_allowed_tools_and_config(actor.tools, tool_config);
    let runner = ActorRunner::new(
        llm,
        tools,
        signals.clone(),
        hub.clone(),
        RunnerLimits::default(),
        artifacts.clone(),
    )
    .with_system_instructions(instructions)
    .with_logger(logger.clone())
    .with_boundary_hooks(hooks);
    let dispatcher = ActorDispatcher::new(
        actor_id.clone(),
        dispatcher_owner.clone(),
        signals,
        runner,
        clock.clone(),
    );

    let mut startup =
        RuntimeLogEvent::transition(RuntimeComponent::Startup, RuntimeTransition::Recovered);
    startup.actor_id = Some(actor_id);
    startup.database_path = Some(paths.database.clone());
    startup.socket_path = Some(paths.socket.clone());
    startup.schema_version = Some(RUNTIME_SCHEMA_VERSION);
    startup.recovery = Some(RuntimeRecoveryCounts {
        expired_actor_leases: recovery.expired_actor_leases,
        expired_bundle_claims: recovery.expired_bundle_claims,
        orphaned_running_attempts: recovery.orphaned_running_attempts,
    });
    logger.log(&startup)?;
    for unknown in &recovery.unknown_outcomes {
        let mut event = RuntimeLogEvent::transition(
            RuntimeComponent::Recovery,
            RuntimeTransition::OutcomeUnknown,
        );
        event.actor_id = Some(unknown.actor_id.clone());
        event.work_item_id = Some(unknown.work_item_id.clone());
        event.run_id = Some(unknown.run_id.clone());
        event.attempt_id = Some(unknown.attempt_id.clone());
        event.lease_generation = Some(unknown.lease_generation);
        event.error_class =
            Some(crate::runtime::observability::RuntimeErrorClass::UnknownExternalOutcome);
        logger.log(&event)?;
    }

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let mut service = ServeRuntime::new(Supervisor::default());
    service.component("ipc", server.run(shutdown_rx.clone()));
    service.component("outbox", {
        let outbox = outbox.clone();
        let shutdown = shutdown_rx.clone();
        async move { outbox.run(shutdown).await }
    });
    service.component("artifact-gc", {
        let artifacts = artifacts.clone();
        let clock = clock.clone();
        let shutdown = shutdown_rx.clone();
        async move { run_artifact_gc(artifacts, clock, shutdown).await }
    });
    service.component("identity-link-gc", {
        let identity_linking = identity_linking.clone();
        let shutdown = shutdown_rx.clone();
        async move { run_identity_link_gc(identity_linking, shutdown).await }
    });
    service.component("dispatcher", async move {
        dispatcher.run_with_shutdown(shutdown_rx).await
    });
    let ready_logger = logger.clone();
    let shutdown_logger = logger.clone();
    let result = service
        .run_until_started(
            async move {
                shutdown.await;
                let _ = shutdown_logger.log(&RuntimeLogEvent::transition(
                    RuntimeComponent::Supervisor,
                    RuntimeTransition::ShuttingDown,
                ));
                shutdown_tx.send_replace(true);
            },
            move || {
                trace.record(StartupPhase::Ready);
                ready_logger.log(&RuntimeLogEvent::transition(
                    RuntimeComponent::Startup,
                    RuntimeTransition::Ready,
                ))
            },
        )
        .await;
    if result.is_err() {
        let mut event = RuntimeLogEvent::transition(
            RuntimeComponent::Supervisor,
            RuntimeTransition::FailedTerminal,
        );
        event.error_class = Some(crate::runtime::observability::RuntimeErrorClass::ComponentExit);
        let _ = logger.log(&event);
    }
    let recovery = store
        .recover_shutdown(&dispatcher_owner, &outbox_owner, clock.now())
        .await;
    let cleanup = lock.remove_stale_socket();
    result.and(recovery).and(cleanup)
}

async fn run_artifact_gc<S, C>(
    manager: ArtifactManager<S, C>,
    clock: C,
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> Result<()>
where
    S: RuntimeStore + Clone + 'static,
    C: Clock,
{
    run_artifact_gc_at_interval(manager, clock, shutdown, ARTIFACT_GC_INTERVAL).await
}

async fn run_artifact_gc_at_interval<S, C>(
    manager: ArtifactManager<S, C>,
    clock: C,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
    interval: std::time::Duration,
) -> Result<()>
where
    S: RuntimeStore + Clone + 'static,
    C: Clock,
{
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return Ok(());
                }
            }
            _ = tokio::time::sleep(interval) => {
                manager.collect_garbage(clock.now()).await?;
            }
        }
    }
}

async fn run_identity_link_gc(
    manager: Arc<dyn IdentityLinkManager>,
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> Result<()> {
    run_identity_link_gc_at_interval(manager, shutdown, IDENTITY_LINK_GC_INTERVAL).await
}

async fn run_identity_link_gc_at_interval(
    manager: Arc<dyn IdentityLinkManager>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
    interval: std::time::Duration,
) -> Result<()> {
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return Ok(());
                }
            }
            _ = tokio::time::sleep(interval) => {
                manager.collect_expired(IDENTITY_LINK_GC_BATCH).await?;
            }
        }
    }
}

fn prepare_paths(home: &Path, paths: &RuntimePaths) -> Result<()> {
    create_secure_directory(home)?;
    for parent in required_parents(paths)? {
        validate_secure_directory(parent)?;
    }
    create_secure_directory(&paths.artifacts)?;
    Ok(())
}

fn validate_runtime_paths(home: &Path, paths: &RuntimePaths) -> Result<()> {
    validate_secure_directory(home)?;
    for parent in required_parents(paths)? {
        validate_secure_directory(parent)?;
    }
    validate_secure_directory(&paths.artifacts)
}

fn required_parents(paths: &RuntimePaths) -> Result<Vec<&Path>> {
    [
        &paths.database,
        &paths.lock,
        &paths.socket,
        &paths.artifacts,
    ]
    .into_iter()
    .map(|path| {
        path.parent()
            .with_context(|| format!("runtime path has no parent: {}", path.display()))
    })
    .collect()
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("failed to install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = terminate.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

#[cfg(test)]
fn default_tool_config() -> Result<ToolRegistryConfig> {
    Ok(ToolRegistryConfig {
        actor_workspace: None,
        skill_roots: default_skill_roots()?,
        file_roots: Vec::new(),
    })
}

fn tool_config_for_actor_workspace(workspace: PathBuf) -> Result<ToolRegistryConfig> {
    std::fs::create_dir_all(&workspace)
        .with_context(|| format!("failed to create actor workspace: {}", workspace.display()))?;
    Ok(ToolRegistryConfig {
        actor_workspace: Some(workspace.clone()),
        skill_roots: default_skill_roots()?,
        file_roots: vec![FileRoot::new("workspace", workspace)],
    })
}

fn default_skill_roots() -> Result<Vec<SkillRoot>> {
    Ok(vec![
        SkillRoot::read_only(PathBuf::from(".codrik").join("skills"), "project"),
        SkillRoot::writable(codrik_dir()?.join("skills"), "user"),
        builtin_skill_root(),
    ])
}

fn actor_workspace_path_in(home: &Path, actor_id: &str) -> Result<std::path::PathBuf> {
    let actor_id = ActorId::parse_workspace_safe(actor_id)?;
    Ok(home.join("workspaces").join(actor_id.as_str()))
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
        os::unix::fs::PermissionsExt,
        path::Path,
        sync::{
            Mutex,
            atomic::{AtomicU64, Ordering},
        },
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::*;

    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);
    type RuntimeWorkCounts = (i64, i64, i64, i64, i64, i64);
    type LinkRuntimeSnapshot = (Vec<u8>, RuntimeWorkCounts);

    #[derive(Default)]
    struct RecordingStartupTrace(Mutex<Vec<StartupPhase>>);

    #[derive(Default)]
    struct CountingLinkManager(std::sync::atomic::AtomicUsize);

    #[async_trait::async_trait]
    impl IdentityLinkManager for CountingLinkManager {
        async fn issue_code(
            &self,
            _actor: &ActorId,
        ) -> Result<crate::runtime::identity_link::IssuedLinkCode> {
            unreachable!("GC test never issues codes")
        }

        async fn redeem_code(
            &self,
            _identity: crate::runtime::store::LinkIdentity,
            _code: &str,
        ) -> Result<crate::runtime::identity_link::LinkRedemption> {
            unreachable!("GC test never redeems codes")
        }

        async fn redeem_code_once(
            &self,
            _key: crate::runtime::gateway::GatewayCommandKey,
            _identity: crate::runtime::store::LinkIdentity,
            _code: &str,
        ) -> Result<crate::runtime::identity_link::LinkRedemption> {
            unreachable!("GC test never redeems codes")
        }

        async fn collect_expired(&self, limit: usize) -> Result<usize> {
            assert_eq!(limit, IDENTITY_LINK_GC_BATCH);
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(0)
        }
    }

    impl StartupTrace for RecordingStartupTrace {
        fn record(&self, phase: StartupPhase) {
            self.0.lock().unwrap().push(phase);
        }
    }

    #[test]
    fn default_skill_roots_order_project_user_then_builtin() -> Result<()> {
        let roots = default_skill_roots()?;

        assert_eq!(
            roots,
            vec![
                SkillRoot::read_only(PathBuf::from(".codrik").join("skills"), "project"),
                SkillRoot::writable(codrik_dir()?.join("skills"), "user"),
                crate::skills::builtin_skill_root(),
            ]
        );
        Ok(())
    }

    #[test]
    fn default_instructions_index_builtin_skill_creator() -> Result<()> {
        let tool_config = default_tool_config()?;

        let instructions = agent_instructions_for_tool_config(&tool_config);

        assert!(instructions.contains(
            "- skill-creator (built-in): Use when creating, writing, saving, or updating reusable skills."
        ));
        assert!(!instructions.contains("# Skill Creator"));
        Ok(())
    }

    #[test]
    fn project_and_user_skills_override_builtin_by_order() -> Result<()> {
        let project = temp_root("project-builtin-override")?;
        let user = temp_root("user-builtin-override")?;
        write_skill(
            &project,
            "skill-creator",
            "---\nname: skill-creator\ndescription: Project creator.\n---\n# Project\n",
        )?;
        write_skill(
            &user,
            "skill-creator",
            "---\nname: skill-creator\ndescription: User creator.\n---\n# User\n",
        )?;
        let registry = SkillRegistry::new(vec![
            SkillRoot::read_only(&project, "project"),
            SkillRoot::writable(&user, "user"),
            crate::skills::builtin_skill_root(),
        ]);

        let skills = registry.list()?;

        assert_eq!(skills.len(), 1);
        assert_eq!(skills[0].source, "project");
        assert_eq!(
            registry.read("skill-creator", None)?,
            "---\nname: skill-creator\ndescription: Project creator.\n---\n# Project\n"
        );

        let registry = SkillRegistry::new(vec![
            SkillRoot::writable(&user, "user"),
            crate::skills::builtin_skill_root(),
        ]);
        let skills = registry.list()?;
        assert_eq!(skills.len(), 1);
        assert_eq!(skills[0].source, "user");
        assert_eq!(
            registry.read("skill-creator", None)?,
            "---\nname: skill-creator\ndescription: User creator.\n---\n# User\n"
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
            actor_workspace: None,
            skill_roots: vec![SkillRoot::read_only(&root, "test")],
            file_roots: Vec::new(),
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
            actor_workspace: None,
            skill_roots: vec![SkillRoot::read_only(temp_root("empty")?, "test")],
            file_roots: Vec::new(),
        };

        let instructions = agent_instructions_for_tool_config(&tool_config);

        assert!(!instructions.contains("## Available Skills"));
        Ok(())
    }

    #[test]
    fn actor_tool_config_creates_shared_shell_workspace() -> Result<()> {
        let workspace = temp_root("actor-workspace")?;
        std::fs::remove_dir_all(&workspace)?;

        let config = tool_config_for_actor_workspace(workspace.clone())?;

        assert!(workspace.is_dir());
        assert_eq!(config.actor_workspace, Some(workspace.clone()));
        assert_eq!(config.file_roots[0], FileRoot::new("workspace", &workspace));
        std::fs::remove_dir_all(workspace).ok();
        Ok(())
    }

    #[tokio::test]
    async fn serve_dependency_seam_uses_injected_clock_for_runtime_state() -> Result<()> {
        let home = short_runtime_root("injected-clock")?;
        fs::set_permissions(&home, fs::Permissions::from_mode(0o700))?;
        let config: AppConfig = yaml_serde::from_str(
            "api_key: key\nbase_url: https://example.test/v1\nmodel: test\nruntime:\n  actor_id: actor:local:owner\n",
        )?;
        let database = home.join("runtime.sqlite");
        serve_with_dependencies(
            config.clone(),
            home,
            crate::runtime::model::ManualClock::new(12_345),
            OpenAiClient::new(config.model, config.api_key, config.base_url),
            async {},
        )
        .await?;
        let connection = tokio_rusqlite::Connection::open(database).await?;
        let created_at: i64 = connection
            .call(|db| {
                db.query_row(
                    "SELECT created_at FROM actors WHERE id='actor:local:owner'",
                    [],
                    |row| row.get(0),
                )
            })
            .await?;
        assert_eq!(created_at, 12_345);
        Ok(())
    }

    #[tokio::test]
    async fn serving_runtime_issues_link_code_without_creating_agent_work() -> Result<()> {
        let home = short_runtime_root("link-ipc")?;
        fs::set_permissions(&home, fs::Permissions::from_mode(0o700))?;
        let config: AppConfig = yaml_serde::from_str(
            "api_key: key\nbase_url: https://example.test/v1\nmodel: test\nruntime:\n  actor_id: actor:local:owner\n",
        )?;
        let paths = config.required_runtime()?.resolve_paths(&home)?;
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let shutdown_waiter = shutdown.clone();
        let serve_config = config.clone();
        let serve_home = home.clone();
        let server = tokio::spawn(async move {
            serve_with_dependencies(
                serve_config.clone(),
                serve_home,
                crate::runtime::model::ManualClock::new(12_345),
                OpenAiClient::new(
                    serve_config.model,
                    serve_config.api_key,
                    serve_config.base_url,
                ),
                async move { shutdown_waiter.notified().await },
            )
            .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while !paths.socket.exists() {
                tokio::task::yield_now().await;
            }
        })
        .await?;

        let client = crate::runtime::ipc::client::LocalIpcClient::new(paths.socket.clone());
        let issued = client
            .issue_link_code(crate::runtime::RequestId::new())
            .await?;
        assert_eq!(issued.code.len(), 9);
        assert_eq!(issued.expires_at.0, 612_345);
        let connection = tokio_rusqlite::Connection::open(&paths.database).await?;
        let first_hash: Vec<u8> = connection
            .call(|database| {
                database.query_row("SELECT code_hash FROM identity_link_codes", [], |row| {
                    row.get(0)
                })
            })
            .await?;
        assert_eq!(first_hash.len(), 32);

        let replacement = client
            .issue_link_code(crate::runtime::RequestId::new())
            .await?;
        assert_ne!(replacement.code, issued.code);

        shutdown.notify_one();
        server.await??;
        let (replacement_hash, counts): LinkRuntimeSnapshot = connection
            .call(
                |database| -> tokio_rusqlite::rusqlite::Result<LinkRuntimeSnapshot> {
                    Ok((
                        database.query_row(
                            "SELECT code_hash FROM identity_link_codes",
                            [],
                            |row| row.get(0),
                        )?,
                        (
                            database
                                .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))?,
                            database.query_row("SELECT COUNT(*) FROM work_items", [], |row| {
                                row.get(0)
                            })?,
                            database
                                .query_row("SELECT COUNT(*) FROM runs", [], |row| row.get(0))?,
                            database
                                .query_row("SELECT COUNT(*) FROM outbox", [], |row| row.get(0))?,
                            database.query_row(
                                "SELECT COUNT(*) FROM result_bundles",
                                [],
                                |row| row.get(0),
                            )?,
                            database.query_row(
                                "SELECT COUNT(*) FROM local_requests",
                                [],
                                |row| row.get(0),
                            )?,
                        ),
                    ))
                },
            )
            .await?;
        assert_ne!(replacement_hash, first_hash);
        assert_eq!(counts, (0, 0, 0, 0, 0, 0));
        let database_bytes = std::fs::read(&paths.database)?;
        assert!(
            !database_bytes
                .windows(issued.code.len())
                .any(|window| window == issued.code.as_bytes())
        );
        assert!(
            !database_bytes
                .windows(replacement.code.len())
                .any(|window| window == replacement.code.as_bytes())
        );
        std::fs::remove_dir_all(home)?;
        Ok(())
    }

    #[tokio::test]
    async fn production_startup_is_ordered_and_ready_only_after_recovery() -> Result<()> {
        let home = short_runtime_root("order")?;
        fs::set_permissions(&home, fs::Permissions::from_mode(0o700))?;
        let stale = std::os::unix::net::UnixListener::bind(home.join("codrik.sock"))?;
        drop(stale);
        let config: AppConfig = yaml_serde::from_str(
            "api_key: key\nbase_url: https://example.test/v1\nmodel: test\nruntime:\n  actor_id: actor:local:owner\n",
        )?;
        let trace = RecordingStartupTrace::default();
        serve_at_until(
            config,
            Arc::new(crate::runtime::observability::NoopRuntimeLogger),
            &trace,
            home,
            SystemClock,
            OpenAiClient::new("test", "key", "https://example.test/v1"),
            async {},
        )
        .await?;
        assert_eq!(
            *trace.0.lock().unwrap(),
            vec![
                StartupPhase::PathsValidated,
                StartupPhase::LockAcquired,
                StartupPhase::Migrated,
                StartupPhase::ActorBootstrapped,
                StartupPhase::ActorVerified,
                StartupPhase::ParentsValidated,
                StartupPhase::StaleSocketRemoved,
                StartupPhase::SocketBound,
                StartupPhase::Recovered,
                StartupPhase::ArtifactsCollected,
                StartupPhase::Ready,
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn periodic_artifact_gc_propagates_authority_failure() -> Result<()> {
        let root = short_runtime_root("gc-authority")?;
        let database = root.join("runtime.sqlite");
        let store = SqliteRuntimeStore::open(&database).await?;
        let manager = ArtifactManager::new(
            root.join("artifacts"),
            store,
            crate::runtime::model::ManualClock::new(1),
        );
        let connection = tokio_rusqlite::Connection::open(&database).await?;
        connection
            .call(|database| database.execute_batch("DROP TABLE artifacts;"))
            .await?;
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let error = run_artifact_gc_at_interval(
            manager,
            crate::runtime::model::ManualClock::new(2),
            shutdown_rx,
            std::time::Duration::from_millis(1),
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("artifacts"));
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[tokio::test]
    async fn periodic_identity_link_gc_propagates_authority_failure() -> Result<()> {
        let root = short_runtime_root("link-gc-authority")?;
        let database = root.join("runtime.sqlite");
        let store = SqliteRuntimeStore::open(&database).await?;
        let manager: Arc<dyn IdentityLinkManager> = Arc::new(IdentityLinkService::new(
            store,
            crate::runtime::model::ManualClock::new(1),
            SystemLinkCodeGenerator,
        ));
        let connection = tokio_rusqlite::Connection::open(&database).await?;
        connection
            .call(|database| database.execute_batch("DROP TABLE identity_link_codes;"))
            .await?;
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let error = run_identity_link_gc_at_interval(
            manager,
            shutdown_rx,
            std::time::Duration::from_millis(1),
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("identity_link_codes"));
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn identity_link_gc_runs_after_interval_and_exits_on_shutdown() -> Result<()> {
        let manager = Arc::new(CountingLinkManager::default());
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let gc_manager: Arc<dyn IdentityLinkManager> = manager.clone();
        let task = tokio::spawn(run_identity_link_gc_at_interval(
            gc_manager,
            shutdown_rx,
            std::time::Duration::from_secs(5),
        ));
        tokio::task::yield_now().await;
        tokio::time::advance(std::time::Duration::from_secs(5)).await;
        tokio::task::yield_now().await;
        assert_eq!(manager.0.load(Ordering::SeqCst), 1);
        shutdown_tx.send_replace(true);
        task.await??;
        Ok(())
    }

    #[tokio::test]
    async fn wrong_or_disabled_actor_fails_before_socket_cleanup() -> Result<()> {
        for (configured, enabled) in [("actor:missing", true), ("actor:local:owner", false)] {
            let home = short_runtime_root("actor")?;
            fs::set_permissions(&home, fs::Permissions::from_mode(0o700))?;
            let database_path = home.join("runtime.sqlite");
            let store = SqliteRuntimeStore::open(&database_path).await?;
            let owner = ActorId::parse_workspace_safe("actor:local:owner")?;
            store
                .ensure_initial_actor(&owner, &[], crate::runtime::model::Timestamp(1))
                .await?;
            drop(store);
            if !enabled {
                tokio_rusqlite::Connection::open(&database_path)
                    .await?
                    .call(|database| {
                        database.execute(
                            "UPDATE actors SET enabled = 0 WHERE id = 'actor:local:owner'",
                            [],
                        )
                    })
                    .await?;
            }
            let stale_path = home.join("codrik.sock");
            let stale = std::os::unix::net::UnixListener::bind(&stale_path)?;
            drop(stale);
            let config: AppConfig = yaml_serde::from_str(&format!(
                "api_key: key\nbase_url: https://example.test/v1\nmodel: test\nruntime:\n  actor_id: {configured}\n"
            ))?;
            let trace = RecordingStartupTrace::default();
            let error = serve_at_until(
                config,
                Arc::new(crate::runtime::observability::NoopRuntimeLogger),
                &trace,
                home,
                SystemClock,
                OpenAiClient::new("test", "key", "https://example.test/v1"),
                async {},
            )
            .await
            .unwrap_err();
            let expected = if configured == "actor:missing" {
                "configured runtime actor actor:missing does not exist"
            } else {
                "configured runtime actor actor:local:owner is disabled"
            };
            assert!(error.to_string().contains(expected), "{error:#}");
            assert!(stale_path.exists());
            assert!(
                !trace
                    .0
                    .lock()
                    .unwrap()
                    .contains(&StartupPhase::StaleSocketRemoved)
            );
        }
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

    fn short_runtime_root(label: &str) -> Result<PathBuf> {
        #[cfg(target_os = "macos")]
        let base = Path::new("/private/tmp");
        #[cfg(target_os = "linux")]
        let base = Path::new("/tmp");
        let path = base.join(format!("cs-{label}-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&path)?;
        Ok(path)
    }
}
