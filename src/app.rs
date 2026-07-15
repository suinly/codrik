use crate::{
    auth::{AuthorizationStore, AuthorizedActor},
    config::{AppConfig, RuntimePaths, codrik_dir},
    llm::openai::OpenAiClient,
    runtime::{
        artifacts::ArtifactManager,
        dispatcher::ActorDispatcher,
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
        sqlite::SqliteRuntimeStore,
        store::{LocalIngressStore, RuntimeAuthorizationStore},
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

pub async fn serve(config: AppConfig) -> Result<()> {
    let home = codrik_dir()?;
    serve_at_until(
        config,
        Arc::new(StderrRuntimeLogger::default()),
        &NoopStartupTrace,
        home,
        shutdown_signal(),
    )
    .await
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StartupPhase {
    PathsValidated,
    LockAcquired,
    Migrated,
    AuthImported,
    ActorVerified,
    ParentsValidated,
    StaleSocketRemoved,
    SocketBound,
    Recovered,
    Ready,
}

trait StartupTrace: Sync {
    fn record(&self, phase: StartupPhase);
}

struct NoopStartupTrace;

impl StartupTrace for NoopStartupTrace {
    fn record(&self, _phase: StartupPhase) {}
}

async fn serve_at_until<F>(
    config: AppConfig,
    logger: Arc<dyn RuntimeLogger>,
    trace: &dyn StartupTrace,
    home: PathBuf,
    shutdown: F,
) -> Result<()>
where
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
    import_legacy_authorization_once(&store, &home.join("users.json"), SystemClock.now()).await?;
    trace.record(StartupPhase::AuthImported);
    let actor_id = ActorId::from_string(runtime.actor_id);
    let actor = store.load_actor(&actor_id).await?.with_context(|| {
        format!("configured runtime actor {actor_id} does not exist; authorize an actor and set runtime.actor_id")
    })?;
    if !actor.enabled {
        bail!("configured runtime actor {actor_id} is disabled");
    }
    trace.record(StartupPhase::ActorVerified);
    validate_runtime_paths(&home, &paths)?;
    trace.record(StartupPhase::ParentsValidated);
    lock.remove_stale_socket()?;
    trace.record(StartupPhase::StaleSocketRemoved);

    let clock = SystemClock;
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
    let server = LocalIpcServer::bind(
        &paths.socket,
        actor_id.clone(),
        Arc::new(store.clone()),
        outbox.clone(),
        hub.clone(),
    )?;
    trace.record(StartupPhase::SocketBound);
    let recovery = store.recover_startup(clock.now()).await?;
    trace.record(StartupPhase::Recovered);
    let tool_config =
        tool_config_for_actor_workspace(actor_workspace_path_in(&home, actor.id.as_str())?)?;
    let instructions = agent_instructions_for_tool_config(&tool_config);
    let tools = ToolRegistry::with_allowed_tools_and_config(actor.tools, tool_config);
    let llm = OpenAiClient::new(config.model, config.api_key, config.base_url);
    let artifacts = ArtifactManager::new(paths.artifacts.clone(), store.clone(), clock.clone());
    let runner = ActorRunner::new(
        llm,
        tools,
        signals.clone(),
        hub.clone(),
        RunnerLimits::default(),
        artifacts,
    )
    .with_system_instructions(instructions)
    .with_logger(logger.clone());
    let dispatcher = ActorDispatcher::new(
        actor_id.clone(),
        dispatcher_owner.clone(),
        signals,
        runner,
        clock,
    );

    let mut startup =
        RuntimeLogEvent::transition(RuntimeComponent::Startup, RuntimeTransition::Recovered);
    startup.actor_id = Some(actor_id);
    startup.database_path = Some(paths.database.clone());
    startup.socket_path = Some(paths.socket.clone());
    startup.schema_version = Some(2);
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
        .recover_shutdown(&dispatcher_owner, &outbox_owner, SystemClock.now())
        .await;
    let cleanup = lock.remove_stale_socket();
    result.and(recovery).and(cleanup)
}

async fn import_legacy_authorization_once(
    store: &SqliteRuntimeStore,
    users_path: &Path,
    now: crate::runtime::model::Timestamp,
) -> Result<crate::runtime::store::ImportOutcome> {
    if store.legacy_authorization_imported().await? {
        return Ok(crate::runtime::store::ImportOutcome::AlreadyImported);
    }
    let snapshot = AuthorizationStore::new(users_path).snapshot().await?;
    store.import_legacy_authorization(snapshot, now).await
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

fn default_tool_config() -> Result<ToolRegistryConfig> {
    Ok(ToolRegistryConfig {
        actor_workspace: None,
        skill_roots: default_skill_roots()?,
        file_roots: Vec::new(),
    })
}

fn actor_tool_config(actor: &AuthorizedActor) -> Result<ToolRegistryConfig> {
    let workspace = actor_workspace_path(&actor.id)?;
    tool_config_for_actor_workspace(workspace)
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

fn actor_workspace_path(actor_id: &str) -> Result<std::path::PathBuf> {
    actor_workspace_path_in(
        &codrik_dir().context("failed to resolve codrik directory for actor workspace")?,
        actor_id,
    )
}

fn actor_workspace_path_in(home: &Path, actor_id: &str) -> Result<std::path::PathBuf> {
    if actor_id.is_empty()
        || actor_id == "."
        || actor_id == ".."
        || actor_id.contains('/')
        || actor_id.contains('\\')
    {
        bail!("unsafe actor id for workspace path: {actor_id}");
    }

    Ok(home.join("workspaces").join(actor_id))
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

    #[derive(Default)]
    struct RecordingStartupTrace(Mutex<Vec<StartupPhase>>);

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
    async fn legacy_auth_marker_is_checked_before_reading_corrupt_file() -> Result<()> {
        let root = temp_root("auth-marker")?;
        let users = root.join("users.json");
        fs::write(&users, r#"{"version":1,"actors":{}}"#)?;
        let store = SqliteRuntimeStore::open_in_memory().await?;
        assert_eq!(
            import_legacy_authorization_once(&store, &users, crate::runtime::model::Timestamp(1))
                .await?,
            crate::runtime::store::ImportOutcome::Imported
        );
        fs::write(&users, "not json and must not be read")?;
        assert_eq!(
            import_legacy_authorization_once(&store, &users, crate::runtime::model::Timestamp(2))
                .await?,
            crate::runtime::store::ImportOutcome::AlreadyImported
        );
        Ok(())
    }

    #[tokio::test]
    async fn failed_legacy_auth_parse_does_not_set_marker() -> Result<()> {
        let root = temp_root("auth-failure")?;
        let users = root.join("users.json");
        fs::write(&users, "not json")?;
        let store = SqliteRuntimeStore::open_in_memory().await?;
        assert!(
            import_legacy_authorization_once(&store, &users, crate::runtime::model::Timestamp(1))
                .await
                .is_err()
        );
        assert!(!store.legacy_authorization_imported().await?);
        Ok(())
    }

    #[tokio::test]
    async fn production_startup_is_ordered_and_ready_only_after_recovery() -> Result<()> {
        let home = short_runtime_root("order")?;
        fs::set_permissions(&home, fs::Permissions::from_mode(0o700))?;
        fs::write(
            home.join("users.json"),
            r#"{"version":1,"actors":{"actor:local:owner":{"enabled":true,"display_name":null,"identities":[],"tools":["*"]}}}"#,
        )?;
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
            async {},
        )
        .await?;
        assert_eq!(
            *trace.0.lock().unwrap(),
            vec![
                StartupPhase::PathsValidated,
                StartupPhase::LockAcquired,
                StartupPhase::Migrated,
                StartupPhase::AuthImported,
                StartupPhase::ActorVerified,
                StartupPhase::ParentsValidated,
                StartupPhase::StaleSocketRemoved,
                StartupPhase::SocketBound,
                StartupPhase::Recovered,
                StartupPhase::Ready,
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn wrong_or_disabled_actor_fails_before_socket_cleanup() -> Result<()> {
        for (configured, enabled) in [("actor:missing", true), ("actor:local:owner", false)] {
            let home = short_runtime_root("actor")?;
            fs::set_permissions(&home, fs::Permissions::from_mode(0o700))?;
            fs::write(
                home.join("users.json"),
                format!(
                    r#"{{"version":1,"actors":{{"actor:local:owner":{{"enabled":{enabled},"display_name":null,"identities":[],"tools":[]}}}}}}"#
                ),
            )?;
            let stale_path = home.join("codrik.sock");
            let stale = std::os::unix::net::UnixListener::bind(&stale_path)?;
            drop(stale);
            let config: AppConfig = yaml_serde::from_str(&format!(
                "api_key: key\nbase_url: https://example.test/v1\nmodel: test\nruntime:\n  actor_id: {configured}\n"
            ))?;
            let trace = RecordingStartupTrace::default();
            assert!(
                serve_at_until(
                    config,
                    Arc::new(crate::runtime::observability::NoopRuntimeLogger),
                    &trace,
                    home,
                    async {},
                )
                .await
                .is_err()
            );
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
