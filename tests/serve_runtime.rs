#![cfg(unix)]

use std::{
    collections::VecDeque,
    fs,
    io::{Read, Write},
    os::unix::fs::{MetadataExt, PermissionsExt},
    os::unix::net::UnixStream,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use serde_json::{Value, json};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::{Notify, oneshot},
    task::JoinHandle,
};
use uuid::Uuid;

use codrik::{
    interfaces::local_renderer::{FinalBundleVerifier, VerifiedFinalBundle},
    runtime::{
        hooks::RuntimeBoundaryHooks,
        ipc::protocol::ServerEvent,
        model::{ActorId, RequestId, Timestamp},
        sqlite::SqliteRuntimeStore,
        store::{ActorStore, FinalPayload},
    },
};

const ACTOR: &str = "actor:local:owner";
static ACCEPTANCE_LOCK: Mutex<()> = Mutex::new(());

fn serialize_acceptance() -> std::sync::MutexGuard<'static, ()> {
    ACCEPTANCE_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[derive(Clone)]
enum ProviderReply {
    Text { text: String, pause_ms: u64 },
    Files(usize),
}

struct ScriptedProvider {
    endpoint: String,
    calls: Arc<AtomicUsize>,
    task: JoinHandle<()>,
}

impl ScriptedProvider {
    async fn start(replies: Vec<ProviderReply>) -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .context("bind scripted loopback provider")?;
        let endpoint = format!("http://{}/v1", listener.local_addr()?);
        let calls = Arc::new(AtomicUsize::new(0));
        let queued = Arc::new(Mutex::new(VecDeque::<ProviderReply>::from(replies)));
        let observed_calls = calls.clone();
        let observed_replies = queued.clone();
        let task = tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let calls = observed_calls.clone();
                let replies = observed_replies.clone();
                tokio::spawn(async move {
                    let mut request = Vec::new();
                    let mut buf = [0_u8; 4096];
                    loop {
                        let Ok(read) = socket.read(&mut buf).await else {
                            return;
                        };
                        if read == 0 {
                            return;
                        }
                        request.extend_from_slice(&buf[..read]);
                        if request.windows(4).any(|window| window == b"\r\n\r\n") {
                            break;
                        }
                    }
                    let header_end = request
                        .windows(4)
                        .position(|window| window == b"\r\n\r\n")
                        .unwrap()
                        + 4;
                    let headers = String::from_utf8_lossy(&request[..header_end]);
                    let content_length = headers
                        .lines()
                        .find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            name.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().ok())
                                .flatten()
                        })
                        .unwrap_or(0);
                    while request.len() < header_end + content_length {
                        let Ok(read) = socket.read(&mut buf).await else {
                            return;
                        };
                        if read == 0 {
                            return;
                        }
                        request.extend_from_slice(&buf[..read]);
                    }
                    calls.fetch_add(1, Ordering::SeqCst);
                    let reply = {
                        let mut replies = replies.lock().unwrap();
                        if replies.len() > 1 {
                            replies.pop_front().unwrap()
                        } else {
                            replies.front().cloned().unwrap_or(ProviderReply::Text {
                                text: "scripted final".into(),
                                pause_ms: 0,
                            })
                        }
                    };
                    match reply {
                        ProviderReply::Text { text, pause_ms } => {
                            let headers = "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\nconnection: close\r\n\r\n";
                            if socket.write_all(headers.as_bytes()).await.is_err() {
                                return;
                            }
                            let split = text.len().min(
                                text.char_indices()
                                    .nth(text.chars().count() / 2)
                                    .map(|(i, _)| i)
                                    .unwrap_or(text.len()),
                            );
                            let (first, second) = text.split_at(split);
                            for (sequence, delta) in [(1, first), (2, second)] {
                                if delta.is_empty() {
                                    continue;
                                }
                                let event = json!({"type":"response.output_text.delta","sequence_number":sequence,"item_id":"msg_1","output_index":0,"content_index":0,"delta":delta});
                                let frame =
                                    format!("data: {}\n\n", serde_json::to_string(&event).unwrap());
                                if socket.write_all(frame.as_bytes()).await.is_err() {
                                    return;
                                }
                                let _ = socket.flush().await;
                                if pause_ms > 0 {
                                    tokio::time::sleep(Duration::from_millis(pause_ms)).await;
                                }
                            }
                            let completed = json!({
                                "type":"response.completed", "sequence_number":3,
                                "response": {"id":"resp_1","object":"response","created_at":1,"model":"scripted","status":"completed","output":[{"type":"message","id":"msg_1","role":"assistant","status":"completed","content":[{"type":"output_text","text":text,"annotations":[]}]}]}
                            });
                            let frame =
                                format!("data: {}\n\n", serde_json::to_string(&completed).unwrap());
                            let _ = socket.write_all(frame.as_bytes()).await;
                            let _ = socket.flush().await;
                        }
                        ProviderReply::Files(count) => {
                            let headers = "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\nconnection: close\r\n\r\n";
                            if socket.write_all(headers.as_bytes()).await.is_err() {
                                return;
                            }
                            let output = (0..count).map(|index| json!({
                                "type":"function_call",
                                "id":format!("item_{index}"),
                                "call_id":format!("call_{index}"),
                                "name":"send_file",
                                "arguments":serde_json::to_string(&json!({"path":format!("workspace/file-{index}.txt")})).unwrap(),
                                "status":"completed"
                            })).collect::<Vec<_>>();
                            let completed = json!({
                                "type":"response.completed", "sequence_number":1,
                                "response": {"id":"resp_files","object":"response","created_at":1,"model":"scripted","status":"completed","output":output}
                            });
                            let frame =
                                format!("data: {}\n\n", serde_json::to_string(&completed).unwrap());
                            let _ = socket.write_all(frame.as_bytes()).await;
                            let _ = socket.flush().await;
                        }
                    }
                });
            }
        });
        Ok(Self {
            endpoint,
            calls,
            task,
        })
    }
}

impl Drop for ScriptedProvider {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct RuntimeHarness {
    test_roots: PathBuf,
    root: PathBuf,
    config: PathBuf,
    socket: PathBuf,
    database: PathBuf,
    log: PathBuf,
    ready_log_offset: usize,
    child: Option<Child>,
    provider: ScriptedProvider,
}

impl RuntimeHarness {
    async fn start(replies: Vec<ProviderReply>) -> Result<Self> {
        Self::start_with_actor(replies, true).await
    }

    async fn start_with_actor(replies: Vec<ProviderReply>, enabled: bool) -> Result<Self> {
        let provider = ScriptedProvider::start(replies).await?;
        let test_roots = std::env::current_dir()?.join(".t");
        fs::create_dir_all(&test_roots)?;
        fs::set_permissions(&test_roots, fs::Permissions::from_mode(0o700))?;
        let root = test_roots.join(&Uuid::new_v4().simple().to_string()[..8]);
        fs::create_dir(&root).context("create harness root")?;
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700))
            .context("secure harness root")?;
        let config = root.join("c");
        let socket = root.join("s");
        let database = root.join("d");
        let log = root.join("l");
        if !enabled {
            let store = SqliteRuntimeStore::open(&database).await?;
            store
                .ensure_initial_actor(
                    &ActorId::parse_workspace_safe(ACTOR)?,
                    &["*".into()],
                    Timestamp(1),
                )
                .await?;
            drop(store);
            tokio_rusqlite::Connection::open(&database)
                .await?
                .call(|connection| {
                    connection.execute(
                        "UPDATE actors SET enabled = 0 WHERE id = 'actor:local:owner'",
                        [],
                    )
                })
                .await?;
        }
        fs::write(&config, format!("api_key: test\nbase_url: {}\nmodel: scripted\nruntime:\n  actor_id: {ACTOR}\n  database_path: {}\n  socket_path: {}\n  lock_path: {}\n  artifact_path: {}\n", provider.endpoint, database.display(), socket.display(), root.join("k").display(), root.join("a").display())).context("write config")?;
        let mut harness = Self {
            test_roots,
            root,
            config,
            socket,
            database,
            log,
            ready_log_offset: 0,
            child: None,
            provider,
        };
        harness.spawn().context("spawn serve")?;
        if enabled {
            harness
                .wait_ready(Duration::from_secs(12))
                .context("wait for spawned serve readiness")?;
        }
        Ok(harness)
    }

    fn spawn(&mut self) -> Result<()> {
        self.ready_log_offset = fs::metadata(&self.log)
            .map(|metadata| metadata.len() as usize)
            .unwrap_or(0);
        let stderr = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log)?;
        let child = Command::new(env!("CARGO_BIN_EXE_codrik"))
            .arg("serve")
            .env("CODRIK_CONFIG", &self.config)
            .env("CODRIK_HOME", &self.root)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(stderr)
            .spawn()?;
        self.child = Some(child);
        Ok(())
    }

    fn wait_ready(&mut self, timeout: Duration) -> Result<()> {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if let Some(status) = self
                .child
                .as_mut()
                .unwrap()
                .try_wait()
                .context("poll serve child")?
            {
                bail!(
                    "serve exited before readiness ({status}): {}",
                    fs::read_to_string(&self.log).unwrap_or_default()
                );
            }
            let log = fs::read_to_string(&self.log).unwrap_or_default();
            if self.socket.exists()
                && log
                    .get(self.ready_log_offset..)
                    .unwrap_or_default()
                    .contains("\"ready\"")
            {
                return Ok(());
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        bail!(
            "timed out waiting for readiness: {}",
            fs::read_to_string(&self.log).unwrap_or_default()
        )
    }

    fn kill(&mut self) -> Result<()> {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
        Ok(())
    }

    fn terminate(&mut self) -> Result<()> {
        if let Some(child) = self.child.as_mut() {
            unsafe {
                libc::kill(child.id() as i32, libc::SIGTERM);
            }
            let deadline = Instant::now() + Duration::from_secs(8);
            while Instant::now() < deadline {
                if child.try_wait()?.is_some() {
                    self.child = None;
                    return Ok(());
                }
                std::thread::sleep(Duration::from_millis(20));
            }
        }
        self.kill()
    }

    async fn count(&self, table: &'static str) -> Result<i64> {
        let connection = tokio_rusqlite::Connection::open(&self.database).await?;
        connection
            .call(move |db| {
                db.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                    row.get(0)
                })
            })
            .await
            .map_err(Into::into)
    }

    async fn scalar(&self, sql: &'static str) -> Result<String> {
        self.scalar_owned(sql.to_owned()).await
    }

    async fn scalar_owned(&self, sql: String) -> Result<String> {
        let connection = tokio_rusqlite::Connection::open(&self.database).await?;
        connection
            .call(move |db| db.query_row(&sql, [], |row| row.get(0)))
            .await
            .map_err(Into::into)
    }

    async fn wait_scalar(&self, sql: &str, expected: &str) -> Result<()> {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if matches!(self.scalar_owned(sql.to_owned()).await, Ok(value) if value == expected)
                {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .with_context(|| format!("database barrier `{sql}` did not reach `{expected}`"))?;
        Ok(())
    }
}

impl Drop for RuntimeHarness {
    fn drop(&mut self) {
        let _ = self.kill();
        let _ = fs::remove_dir_all(&self.root);
        // `remove_dir` is deliberately non-recursive: it removes only the suite's
        // now-empty shared parent and cannot race away another live harness.
        let _ = fs::remove_dir(&self.test_roots);
    }
}

#[derive(Clone)]
enum InjectedReply {
    Text(String),
    PartialThenFinal { delta: String, final_text: String },
    ToolCall,
    Failure,
}

#[derive(Clone)]
struct InjectedLlm {
    replies: Arc<Mutex<VecDeque<InjectedReply>>>,
    calls: Arc<AtomicUsize>,
    called: Arc<Notify>,
    block_call: Option<usize>,
    release: Arc<Notify>,
}

impl InjectedLlm {
    fn new(replies: Vec<InjectedReply>) -> Self {
        Self {
            replies: Arc::new(Mutex::new(replies.into())),
            calls: Arc::new(AtomicUsize::new(0)),
            called: Arc::new(Notify::new()),
            block_call: None,
            release: Arc::new(Notify::new()),
        }
    }

    fn blocking(replies: Vec<InjectedReply>, block_call: usize) -> Self {
        let mut llm = Self::new(replies);
        llm.block_call = Some(block_call);
        llm
    }

    async fn wait_calls(&self, expected: usize) {
        while self.calls.load(Ordering::SeqCst) < expected {
            let called = self.called.notified();
            if self.calls.load(Ordering::SeqCst) >= expected {
                break;
            }
            called.await;
        }
    }
}

#[async_trait]
impl codrik::llm::client::LlmStreamClient for InjectedLlm {
    async fn stream(
        &self,
        _request: codrik::llm::client::LlmRequest,
        sink: &mut dyn codrik::llm::client::LlmStreamSink,
        _context: &codrik::llm::client::RunContext,
    ) -> Result<codrik::llm::client::LlmResponse> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        self.called.notify_waiters();
        if self.block_call == Some(call) {
            self.release.notified().await;
        }
        let reply = {
            self.replies
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(InjectedReply::Text("scripted final".into()))
        };
        match reply {
            InjectedReply::Failure => bail!("scripted model failure"),
            InjectedReply::ToolCall => Ok(codrik::llm::client::LlmResponse {
                content: String::new(),
                tool_calls: vec![codrik::llm::client::LlmToolCall {
                    id: "call-datetime".into(),
                    name: "datetime".into(),
                    arguments: "{}".into(),
                }],
            }),
            InjectedReply::Text(text) => {
                sink.on_event(codrik::llm::client::LlmStreamEvent::TextDelta(text.clone()))
                    .await?;
                Ok(codrik::llm::client::LlmResponse {
                    content: text,
                    tool_calls: Vec::new(),
                })
            }
            InjectedReply::PartialThenFinal { delta, final_text } => {
                sink.on_event(codrik::llm::client::LlmStreamEvent::TextDelta(delta))
                    .await?;
                Ok(codrik::llm::client::LlmResponse {
                    content: final_text,
                    tool_calls: Vec::new(),
                })
            }
        }
    }
}

#[derive(Default)]
struct BoundaryHooks {
    block_dispatch: std::sync::atomic::AtomicBool,
    block_incorporation: std::sync::atomic::AtomicBool,
    ingress_seen: std::sync::atomic::AtomicBool,
    incorporation_seen: std::sync::atomic::AtomicBool,
    ingress_reached: Notify,
    incorporation_reached: Notify,
    release_dispatch: Notify,
    release_incorporation: Notify,
}

impl BoundaryHooks {
    fn ingress_gate() -> Arc<Self> {
        Arc::new(Self {
            block_dispatch: std::sync::atomic::AtomicBool::new(true),
            ..Self::default()
        })
    }

    fn incorporation_gate() -> Arc<Self> {
        Arc::new(Self {
            block_incorporation: std::sync::atomic::AtomicBool::new(true),
            ..Self::default()
        })
    }

    async fn wait_ingress(&self) {
        wait_latched(&self.ingress_seen, &self.ingress_reached).await;
    }

    async fn wait_incorporation(&self) {
        wait_latched(&self.incorporation_seen, &self.incorporation_reached).await;
    }

    fn disable(&self) {
        self.block_dispatch.store(false, Ordering::SeqCst);
        self.block_incorporation.store(false, Ordering::SeqCst);
        self.release_dispatch.notify_waiters();
        self.release_incorporation.notify_waiters();
    }
}

async fn wait_latched(seen: &std::sync::atomic::AtomicBool, changed: &Notify) {
    loop {
        if seen.load(Ordering::Acquire) {
            return;
        }
        let notified = changed.notified();
        if seen.load(Ordering::Acquire) {
            return;
        }
        notified.await;
    }
}

#[async_trait]
impl RuntimeBoundaryHooks for BoundaryHooks {
    async fn before_dispatch(&self) {
        if self.block_dispatch.load(Ordering::SeqCst) {
            self.release_dispatch.notified().await;
        }
    }

    async fn ingress_committed(&self, _request_id: &RequestId) {
        self.ingress_seen.store(true, Ordering::Release);
        self.ingress_reached.notify_waiters();
    }

    async fn incorporation_committed(&self, _request_ids: &[RequestId]) {
        self.incorporation_seen.store(true, Ordering::Release);
        self.incorporation_reached.notify_waiters();
        if self.block_incorporation.load(Ordering::SeqCst) {
            self.release_incorporation.notified().await;
        }
    }
}

struct InjectedHarness {
    test_roots: PathBuf,
    root: PathBuf,
    config: codrik::config::AppConfig,
    socket: PathBuf,
    database: PathBuf,
    clock: codrik::runtime::model::ManualClock,
    llm: InjectedLlm,
    hooks: Arc<dyn RuntimeBoundaryHooks>,
    shutdown: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<Result<()>>>,
}

impl InjectedHarness {
    async fn start(replies: Vec<InjectedReply>) -> Result<Self> {
        Self::start_with_llm_and_hooks(
            InjectedLlm::new(replies),
            Arc::new(codrik::runtime::hooks::NoopRuntimeBoundaryHooks),
        )
        .await
    }

    async fn start_with_llm(llm: InjectedLlm) -> Result<Self> {
        Self::start_with_llm_and_hooks(
            llm,
            Arc::new(codrik::runtime::hooks::NoopRuntimeBoundaryHooks),
        )
        .await
    }

    async fn start_with_hooks(
        replies: Vec<InjectedReply>,
        hooks: Arc<dyn RuntimeBoundaryHooks>,
    ) -> Result<Self> {
        Self::start_with_llm_and_hooks(InjectedLlm::new(replies), hooks).await
    }

    async fn start_with_llm_and_hooks(
        llm: InjectedLlm,
        hooks: Arc<dyn RuntimeBoundaryHooks>,
    ) -> Result<Self> {
        let test_roots = std::env::current_dir()?.join(".t");
        fs::create_dir_all(&test_roots)?;
        fs::set_permissions(&test_roots, fs::Permissions::from_mode(0o700))?;
        let root = test_roots.join(&Uuid::new_v4().simple().to_string()[..8]);
        fs::create_dir(&root)?;
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700))?;
        let socket = root.join("s");
        let database = root.join("d");
        let config = yaml_serde::from_str(&format!(
            "api_key: injected\nbase_url: https://unused.invalid/v1\nmodel: injected\nruntime:\n  actor_id: {ACTOR}\n  database_path: {}\n  socket_path: {}\n  lock_path: {}\n  artifact_path: {}\n",
            database.display(),
            socket.display(),
            root.join("k").display(),
            root.join("a").display()
        ))?;
        let mut harness = Self {
            test_roots,
            root,
            config,
            socket,
            database,
            clock: codrik::runtime::model::ManualClock::new(1_000),
            llm,
            hooks,
            shutdown: None,
            task: None,
        };
        harness.spawn().await?;
        Ok(harness)
    }

    async fn spawn(&mut self) -> Result<()> {
        let (shutdown, stopped) = oneshot::channel();
        self.shutdown = Some(shutdown);
        let config = self.config.clone();
        let root = self.root.clone();
        let clock = self.clock.clone();
        let llm = self.llm.clone();
        let hooks = self.hooks.clone();
        self.task = Some(tokio::spawn(async move {
            codrik::app::serve_with_dependencies_and_hooks(
                config,
                root,
                clock,
                llm,
                hooks,
                async move {
                    let _ = stopped.await;
                },
            )
            .await
        }));
        let probe_request = request_id();
        let readiness = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if let Ok(mut stream) = UnixStream::connect(&self.socket) {
                    let _ = stream.set_read_timeout(Some(Duration::from_millis(250)));
                    let _ = stream.set_write_timeout(Some(Duration::from_millis(250)));
                    if send_frame(
                        &mut stream,
                        json!({"type":"resume","request_id":probe_request.clone()}),
                    )
                    .is_ok()
                        && wait_for_type(&mut stream, "request_error").is_ok()
                    {
                        return;
                    }
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        if readiness.is_err() {
            if self
                .task
                .as_ref()
                .is_some_and(tokio::task::JoinHandle::is_finished)
            {
                let result = self.task.take().expect("finished task").await;
                return match result {
                    Ok(Ok(())) => bail!("injected runtime exited before readiness"),
                    Ok(Err(error)) => Err(error).context("injected runtime startup failed"),
                    Err(error) => Err(error).context("injected runtime task failed"),
                };
            }
            readiness.context("injected runtime readiness")?;
        }
        Ok(())
    }

    async fn crash(&mut self) {
        self.shutdown.take();
        if let Some(task) = self.task.take() {
            task.abort();
            let _ = task.await;
        }
    }

    async fn scalar(&self, sql: impl Into<String>) -> Result<String> {
        let connection = tokio_rusqlite::Connection::open(&self.database).await?;
        let sql = sql.into();
        connection
            .call(move |db| db.query_row(&sql, [], |row| row.get(0)))
            .await
            .map_err(Into::into)
    }

    async fn wait_scalar(&self, sql: &str, expected: &str) -> Result<()> {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if matches!(self.scalar(sql.to_owned()).await, Ok(value) if value == expected) {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .with_context(|| format!("database barrier `{sql}` did not reach `{expected}`"))?;
        Ok(())
    }
}

impl Drop for InjectedHarness {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
        let _ = fs::remove_dir_all(&self.root);
        let _ = fs::remove_dir(&self.test_roots);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn clean_runtime_bootstraps_configured_actor_without_users_file() -> Result<()> {
    let harness = InjectedHarness::start(vec![]).await?;
    assert!(!harness.root.join("users.json").exists());
    assert_eq!(
        harness
            .scalar(format!(
                "SELECT CAST(enabled AS TEXT) FROM actors WHERE id = '{ACTOR}'"
            ))
            .await?,
        "1"
    );
    assert_eq!(
        harness
            .scalar(format!(
                "SELECT tools_json FROM actors WHERE id = '{ACTOR}'"
            ))
            .await?,
        r#"["*"]"#
    );
    drop(harness);
    Ok(())
}

fn request_id() -> String {
    Uuid::new_v4().to_string()
}

fn connect(path: &Path) -> Result<UnixStream> {
    let stream = UnixStream::connect(path)
        .with_context(|| format!("connect Unix socket {}", path.display()))?;
    stream
        .set_read_timeout(Some(Duration::from_secs(20)))
        .context("set IPC read timeout")?;
    stream
        .set_write_timeout(Some(Duration::from_secs(5)))
        .context("set IPC write timeout")?;
    Ok(stream)
}

fn send_frame(stream: &mut UnixStream, body: Value) -> Result<()> {
    let bytes = serde_json::to_vec(&json!({"version":1,"body":body}))?;
    stream.write_all(&(bytes.len() as u32).to_be_bytes())?;
    stream.write_all(&bytes)?;
    Ok(())
}

fn read_frame(stream: &mut UnixStream) -> Result<Value> {
    let mut header = [0_u8; 4];
    stream.read_exact(&mut header)?;
    let mut body = vec![0; u32::from_be_bytes(header) as usize];
    stream.read_exact(&mut body)?;
    Ok(serde_json::from_slice(&body)?)
}

fn submit(socket: &Path, request: &str, text: &str) -> Result<UnixStream> {
    let mut stream = connect(socket)?;
    send_frame(
        &mut stream,
        json!({"type":"submit","request_id":request,"text":text}),
    )?;
    Ok(stream)
}

fn resume(socket: &Path, request: &str) -> Result<UnixStream> {
    let mut stream = connect(socket)?;
    send_frame(&mut stream, json!({"type":"resume","request_id":request}))?;
    Ok(stream)
}

fn wait_for_type(stream: &mut UnixStream, expected: &str) -> Result<Value> {
    loop {
        let event = read_frame(stream)?;
        if event["body"]["type"] == expected {
            return Ok(event);
        }
    }
}

#[derive(Debug)]
struct VerifiedBundle {
    verified: VerifiedFinalBundle,
    chunk_frames: usize,
}

impl std::ops::Deref for VerifiedBundle {
    type Target = VerifiedFinalBundle;

    fn deref(&self) -> &Self::Target {
        &self.verified
    }
}

impl VerifiedBundle {
    fn delivery_ids(&self) -> Vec<codrik::runtime::DeliveryId> {
        self.verified.delivery_ids()
    }

    fn text(&self) -> Option<&str> {
        self.deliveries
            .iter()
            .find_map(|delivery| match &delivery.payload {
                FinalPayload::Text { text } => Some(text.as_str()),
                _ => None,
            })
    }
}

fn final_bundle(stream: &mut UnixStream, expected_request: &str) -> Result<VerifiedBundle> {
    let request = codrik::runtime::RequestId::parse(expected_request)?;
    let mut verifier = FinalBundleVerifier::for_request(request);
    let mut chunk_frames = 0;
    loop {
        let wire = read_frame(stream)?;
        if wire["body"]["type"] == "final_chunk" {
            chunk_frames += 1;
        }
        if !matches!(
            wire["body"]["type"].as_str(),
            Some("final_begin" | "final_chunk" | "final_end")
        ) {
            if verifier.is_in_progress() {
                bail!("non-final event arrived while final verification was in progress");
            }
            continue;
        }
        let event: ServerEvent = serde_json::from_value(wire)?;
        if let Some(verified) = verifier.handle(event)? {
            return Ok(VerifiedBundle {
                verified,
                chunk_frames,
            });
        }
    }
}

fn ack(
    socket: &Path,
    request: &str,
    bundle: &codrik::runtime::BundleId,
    deliveries: &[codrik::runtime::DeliveryId],
) -> Result<()> {
    let mut stream = connect(socket)?;
    send_frame(
        &mut stream,
        json!({"type":"ack_final","request_id":request,"bundle_id":bundle,"delivery_ids":deliveries}),
    )?;
    let event = wait_for_type(&mut stream, "ack_accepted")?;
    assert_eq!(event["body"]["bundle_id"].as_str(), Some(bundle.as_str()));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scenario_01_submit_streams_verified_final_and_acks_delivery() -> Result<()> {
    let _serial = serialize_acceptance();
    let harness = RuntimeHarness::start(vec![ProviderReply::Text {
        text: "hello world".into(),
        pause_ms: 5,
    }])
    .await?;
    let request = request_id();
    let mut stream = submit(&harness.socket, &request, "hello")?;
    wait_for_type(&mut stream, "accepted")?;
    wait_for_type(&mut stream, "text_delta").with_context(|| {
        format!(
            "provider calls={} daemon log:\n{}",
            harness.provider.calls.load(Ordering::SeqCst),
            fs::read_to_string(&harness.log).unwrap_or_default()
        )
    })?;
    let verified = final_bundle(&mut stream, &request).with_context(|| {
        format!(
            "daemon log:\n{}",
            fs::read_to_string(&harness.log).unwrap_or_default()
        )
    })?;
    assert_eq!(verified.request_id.as_str(), request);
    assert_eq!(verified.text(), Some("hello world"));
    assert!(!verified.replay);
    assert_eq!(
        harness
            .scalar("SELECT id FROM result_bundles LIMIT 1")
            .await?,
        verified.bundle_id.as_str()
    );
    ack(
        &harness.socket,
        &request,
        &verified.bundle_id,
        &verified.delivery_ids(),
    )?;
    assert_eq!(
        harness
            .scalar("SELECT state FROM result_bundles LIMIT 1")
            .await?,
        "delivered"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scenario_02_duplicate_submit_creates_one_durable_execution() -> Result<()> {
    let _serial = serialize_acceptance();
    let harness = RuntimeHarness::start(vec![ProviderReply::Text {
        text: "once".into(),
        pause_ms: 100,
    }])
    .await?;
    let request = request_id();
    let mut first = submit(&harness.socket, &request, "same")?;
    wait_for_type(&mut first, "accepted")?;
    let mut duplicate = submit(&harness.socket, &request, "same")?;
    wait_for_type(&mut duplicate, "accepted")?;
    let _ = final_bundle(&mut duplicate, &request)?;
    assert_eq!(harness.count("local_requests").await?, 1);
    assert_eq!(harness.count("events").await?, 1);
    assert_eq!(harness.provider.calls.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scenario_03_conflicting_request_id_is_rejected() -> Result<()> {
    let _serial = serialize_acceptance();
    let harness = RuntimeHarness::start(vec![ProviderReply::Text {
        text: "done".into(),
        pause_ms: 100,
    }])
    .await?;
    let request = request_id();
    let mut first = submit(&harness.socket, &request, "one")?;
    wait_for_type(&mut first, "accepted")?;
    let mut conflict = submit(&harness.socket, &request, "two")?;
    let error = wait_for_type(&mut conflict, "request_error")?;
    assert_eq!(error["body"]["code"], "request_conflict");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scenario_04_disconnect_during_streaming_does_not_cancel_work() -> Result<()> {
    let _serial = serialize_acceptance();
    let harness = RuntimeHarness::start(vec![ProviderReply::Text {
        text: "durable".into(),
        pause_ms: 100,
    }])
    .await?;
    let request = request_id();
    let mut stream = submit(&harness.socket, &request, "disconnect")?;
    wait_for_type(&mut stream, "accepted")?;
    drop(stream);
    tokio::time::sleep(Duration::from_millis(500)).await;
    let mut resumed = resume(&harness.socket, &request)?;
    let _ = final_bundle(&mut resumed, &request)?;
    assert_eq!(
        harness
            .scalar("SELECT state FROM local_requests LIMIT 1")
            .await?,
        "completed"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scenario_05_resume_joins_live_run_or_replays_completion() -> Result<()> {
    let _serial = serialize_acceptance();
    let harness = RuntimeHarness::start(vec![ProviderReply::Text {
        text: "resume".into(),
        pause_ms: 100,
    }])
    .await?;
    let request = request_id();
    let mut submitted = submit(&harness.socket, &request, "resume me")?;
    wait_for_type(&mut submitted, "accepted")?;
    let mut live = resume(&harness.socket, &request)?;
    let bundle = final_bundle(&mut live, &request)?;
    ack(
        &harness.socket,
        &request,
        &bundle.bundle_id,
        &bundle.delivery_ids(),
    )?;
    let mut replay = resume(&harness.socket, &request)?;
    let replayed = final_bundle(&mut replay, &request)?;
    assert!(replayed.replay);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scenario_06_multiple_incorporated_requests_receive_final_rows() -> Result<()> {
    let _serial = serialize_acceptance();
    let harness = RuntimeHarness::start(vec![ProviderReply::Text {
        text: "shared".into(),
        pause_ms: 1_000,
    }])
    .await?;
    let first_id = request_id();
    let second_id = request_id();
    let mut first = submit(&harness.socket, &first_id, "first")?;
    wait_for_type(&mut first, "accepted")?;
    let mut second = submit(&harness.socket, &second_id, "second")?;
    wait_for_type(&mut second, "accepted")?;
    harness
        .wait_scalar(
            "SELECT CASE WHEN runs.state='active' AND COUNT(run_events.event_id)=2 AND SUM(run_events.incorporated)=2 THEN 'shared-active' ELSE 'not-ready' END FROM runs JOIN run_events ON run_events.run_id=runs.id GROUP BY runs.id",
            "shared-active",
        )
        .await?;
    let first_bundle = final_bundle(&mut first, &first_id)?;
    let second_bundle = final_bundle(&mut second, &second_id)?;
    assert_eq!(harness.count("result_bundles").await?, 2);
    assert_eq!(
        harness
            .scalar("SELECT CAST(COUNT(DISTINCT work_item_id) AS TEXT) FROM local_requests")
            .await?,
        "1"
    );
    assert_eq!(
        harness
            .scalar("SELECT CAST(COUNT(DISTINCT run_id) AS TEXT) FROM events")
            .await?,
        "1"
    );
    assert_eq!(
        harness
            .scalar("SELECT CAST(COUNT(*) AS TEXT) FROM run_events WHERE incorporated=1")
            .await?,
        "2"
    );
    assert_eq!(
        harness.scalar("SELECT state FROM runs LIMIT 1").await?,
        "completed"
    );
    assert_eq!(
        harness
            .scalar("SELECT state FROM work_items LIMIT 1")
            .await?,
        "completed"
    );
    assert_eq!(
        harness
            .scalar_owned(format!(
                "SELECT result_bundle_id FROM local_requests WHERE request_id='{first_id}'"
            ))
            .await?,
        first_bundle.bundle_id.as_str()
    );
    assert_eq!(
        harness
            .scalar_owned(format!(
                "SELECT result_bundle_id FROM local_requests WHERE request_id='{second_id}'"
            ))
            .await?,
        second_bundle.bundle_id.as_str()
    );
    assert_eq!(harness.count("outbox_deliveries").await?, 2);
    assert_eq!(harness.count("outbox").await?, 1);
    assert_eq!(harness.provider.calls.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scenario_07_restart_after_ingress_preserves_durable_state() -> Result<()> {
    let _serial = serialize_acceptance();

    // Boundary rendezvous are latched: a production callback that wins the
    // race before the test registers its waiter must still be observed.
    let early_ingress = BoundaryHooks::ingress_gate();
    early_ingress.ingress_committed(&RequestId::new()).await;
    tokio::time::timeout(Duration::from_millis(50), early_ingress.wait_ingress()).await?;
    let early_incorporation = BoundaryHooks::incorporation_gate();
    let callback = {
        let hooks = early_incorporation.clone();
        tokio::spawn(async move {
            hooks.incorporation_committed(&[RequestId::new()]).await;
        })
    };
    tokio::task::yield_now().await;
    tokio::time::timeout(
        Duration::from_millis(50),
        early_incorporation.wait_incorporation(),
    )
    .await?;
    early_incorporation.disable();
    callback.await?;

    // Boundary A: ingress is durable, but no run has attached it yet.
    let ingress_hooks = BoundaryHooks::ingress_gate();
    let mut ingress = InjectedHarness::start_with_hooks(
        vec![InjectedReply::Text("after ingress".into())],
        ingress_hooks.clone(),
    )
    .await?;
    let request = request_id();
    let _submitted = submit(&ingress.socket, &request, "durable ingress")?;
    tokio::time::timeout(Duration::from_secs(10), ingress_hooks.wait_ingress()).await?;
    ingress
        .wait_scalar(
            "SELECT CASE WHEN COUNT(*)=1 AND (SELECT COUNT(*) FROM runs)=0 THEN 'ingress-only' ELSE 'not-ready' END FROM local_requests",
            "ingress-only",
        )
        .await?;
    assert_eq!(ingress.llm.calls.load(Ordering::SeqCst), 0);
    ingress.crash().await;
    ingress_hooks.disable();
    ingress.spawn().await.context("boundary A restart")?;
    let mut resumed = resume(&ingress.socket, &request)?;
    assert_eq!(
        final_bundle(&mut resumed, &request)?.text(),
        Some("after ingress")
    );
    assert_eq!(ingress.llm.calls.load(Ordering::SeqCst), 1);

    // Boundary B: a run is attached and its source event is incorporated, but
    // no model checkpoint or terminal result exists.
    let incorporation_hooks = BoundaryHooks::incorporation_gate();
    let mut incorporated = InjectedHarness::start_with_hooks(
        vec![InjectedReply::Text("after incorporation".into())],
        incorporation_hooks.clone(),
    )
    .await?;
    let request = request_id();
    let mut submitted = submit(&incorporated.socket, &request, "incorporated")?;
    wait_for_type(&mut submitted, "accepted")?;
    tokio::time::timeout(
        Duration::from_secs(10),
        incorporation_hooks.wait_incorporation(),
    )
    .await?;
    incorporated
        .wait_scalar(
            "SELECT CASE WHEN runs.state='active' AND COUNT(run_events.event_id)=1 AND SUM(run_events.incorporated)=1 THEN 'incorporated' ELSE 'not-ready' END FROM runs JOIN run_events ON run_events.run_id=runs.id GROUP BY runs.id",
            "incorporated",
        )
        .await?;
    assert_eq!(incorporated.llm.calls.load(Ordering::SeqCst), 0);
    incorporated.crash().await;
    incorporation_hooks.disable();
    incorporated.clock.advance(31_000);
    incorporated.spawn().await.context("boundary B restart")?;
    let mut resumed = resume(&incorporated.socket, &request)?;
    assert_eq!(
        final_bundle(&mut resumed, &request)?.text(),
        Some("after incorporation")
    );
    assert_eq!(incorporated.llm.calls.load(Ordering::SeqCst), 1);

    // Boundary C: the first model/tool checkpoint is committed. The second
    // model call is deliberately held in-flight, then the supervisor crashes;
    // restart conservatively repeats that ambiguous call.
    let llm = InjectedLlm::blocking(
        vec![
            InjectedReply::ToolCall,
            InjectedReply::Text("after checkpoint".into()),
        ],
        2,
    );
    let mut checkpoint = InjectedHarness::start_with_llm(llm).await?;
    let request = request_id();
    let mut submitted = submit(&checkpoint.socket, &request, "checkpoint boundary")?;
    wait_for_type(&mut submitted, "accepted")?;
    checkpoint.llm.wait_calls(2).await;
    checkpoint
        .wait_scalar(
            "SELECT CASE WHEN COUNT(*) > 0 THEN 'committed' ELSE 'missing' END FROM recent_messages",
            "committed",
        )
        .await?;
    checkpoint.crash().await;
    checkpoint.clock.advance(31_000);
    checkpoint.spawn().await.context("boundary C restart")?;
    let mut resumed = resume(&checkpoint.socket, &request)?;
    assert_eq!(
        final_bundle(&mut resumed, &request)?.text(),
        Some("after checkpoint")
    );
    assert_eq!(checkpoint.llm.calls.load(Ordering::SeqCst), 3);

    // Boundary D: terminal finalization is durable before delivery ACK.
    let mut terminal =
        InjectedHarness::start(vec![InjectedReply::Text("terminal durable".into())]).await?;
    let request = request_id();
    let mut submitted = submit(&terminal.socket, &request, "terminal boundary")?;
    wait_for_type(&mut submitted, "accepted")?;
    let original = final_bundle(&mut submitted, &request)?;
    assert_eq!(
        terminal.scalar("SELECT state FROM local_requests").await?,
        "completed"
    );
    terminal.crash().await;
    terminal.clock.advance(60_000);
    terminal.spawn().await.context("boundary D restart")?;
    let mut resumed = resume(&terminal.socket, &request)?;
    let replayed = final_bundle(&mut resumed, &request)?;
    assert_eq!(replayed.bundle_id, original.bundle_id);
    assert_eq!(replayed.manifest_sha256, original.manifest_sha256);
    assert_eq!(terminal.llm.calls.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scenario_08_final_end_without_ack_redelivers_without_recomputation() -> Result<()> {
    let _serial = serialize_acceptance();
    let mut harness = InjectedHarness::start(vec![InjectedReply::Text("immutable".into())]).await?;
    let request = request_id();
    let mut stream = submit(&harness.socket, &request, "lost ack")?;
    wait_for_type(&mut stream, "accepted")?;
    let original_bundle = final_bundle(&mut stream, &request)?;
    harness.crash().await;
    harness.clock.advance(60_000);
    harness.spawn().await?;
    let mut resumed = resume(&harness.socket, &request)?;
    let redelivered_bundle = final_bundle(&mut resumed, &request)?;
    assert_eq!(redelivered_bundle.bundle_id, original_bundle.bundle_id);
    assert_eq!(
        redelivered_bundle.manifest_sha256,
        original_bundle.manifest_sha256
    );
    assert_eq!(harness.llm.calls.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scenario_09_second_daemon_cannot_remove_live_socket() -> Result<()> {
    let _serial = serialize_acceptance();
    let mut harness = RuntimeHarness::start(vec![]).await?;
    let socket_metadata = fs::symlink_metadata(&harness.socket)?;
    let stderr = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&harness.log)?;
    let status = Command::new(env!("CARGO_BIN_EXE_codrik"))
        .arg("serve")
        .env("CODRIK_CONFIG", &harness.config)
        .env("CODRIK_HOME", &harness.root)
        .stdout(Stdio::null())
        .stderr(stderr)
        .status()?;
    assert!(!status.success());
    assert!(harness.socket.exists());
    assert_eq!(
        fs::symlink_metadata(&harness.socket)?.ino(),
        socket_metadata.ino()
    );
    harness.kill()?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scenario_10_sigterm_preserves_resumable_active_state() -> Result<()> {
    let _serial = serialize_acceptance();
    let mut harness = RuntimeHarness::start(vec![ProviderReply::Text {
        text: "signal".into(),
        pause_ms: 500,
    }])
    .await?;
    let request = request_id();
    let mut stream = submit(&harness.socket, &request, "sigterm")?;
    wait_for_type(&mut stream, "accepted")?;
    harness.terminate()?;
    harness.spawn()?;
    harness.wait_ready(Duration::from_secs(12))?;
    let mut resumed = resume(&harness.socket, &request)?;
    let _ = final_bundle(&mut resumed, &request)?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scenario_11_orphaned_running_tools_are_not_reinvoked() -> Result<()> {
    let _serial = serialize_acceptance();
    let mut harness = RuntimeHarness::start(vec![]).await?;
    harness.kill()?;
    let db = tokio_rusqlite::Connection::open(&harness.database).await?;
    db.call(|db| {
        db.execute("INSERT INTO work_items(id,actor_id,kind,audience_kind,state,created_at,updated_at) VALUES('orphan-work',?1,'interactive','actor_private','ready',1,1)", [ACTOR])?;
        db.execute("INSERT INTO runs(id,actor_id,work_item_id,state,lease_generation,observed_sequence,created_at,updated_at) VALUES('orphan-run',?1,'orphan-work','active',1,0,1,1)", [ACTOR])?;
        db.execute("INSERT INTO tool_attempts(id,run_id,tool_call_id,tool_name,arguments_json,capabilities_json,state,created_at,updated_at) VALUES('orphan-attempt','orphan-run','call','datetime','{}','{}','running',1,1)", [])?;
        Ok::<(), tokio_rusqlite::rusqlite::Error>(())
    }).await?;
    harness.spawn()?;
    harness.wait_ready(Duration::from_secs(12))?;
    assert_eq!(
        harness
            .scalar("SELECT state FROM tool_attempts WHERE id='orphan-attempt'")
            .await?,
        "outcome_unknown"
    );
    assert_eq!(harness.provider.calls.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scenario_12_fifth_runtime_failure_delivers_terminal_error() -> Result<()> {
    let _serial = serialize_acceptance();
    let mut harness = InjectedHarness::start(vec![
        InjectedReply::Failure,
        InjectedReply::Failure,
        InjectedReply::Failure,
        InjectedReply::Failure,
        InjectedReply::Failure,
    ])
    .await?;
    let request = request_id();
    let mut stream = submit(&harness.socket, &request, "fail five times")?;
    wait_for_type(&mut stream, "accepted")?;
    harness.llm.wait_calls(1).await;
    harness
        .wait_scalar("SELECT CAST(failure_count AS TEXT) FROM work_items", "1")
        .await?;
    harness.clock.advance(1_000);
    harness.llm.wait_calls(2).await;
    harness
        .wait_scalar("SELECT CAST(failure_count AS TEXT) FROM work_items", "2")
        .await?;

    // Restart after durable retry history exists; advancing beyond the actor
    // lease is the deterministic recovery barrier for the new supervisor.
    harness.crash().await;
    harness.clock.advance(30_000);
    harness.spawn().await?;
    harness.llm.wait_calls(3).await;
    harness
        .wait_scalar("SELECT CAST(failure_count AS TEXT) FROM work_items", "3")
        .await?;
    harness.clock.advance(4_000);
    harness.llm.wait_calls(4).await;
    harness
        .wait_scalar("SELECT CAST(failure_count AS TEXT) FROM work_items", "4")
        .await?;
    harness.clock.advance(8_000);
    harness.llm.wait_calls(5).await;
    let mut resumed = resume(&harness.socket, &request)?;
    let terminal = final_bundle(&mut resumed, &request)?;
    assert_eq!(terminal.deliveries.len(), 1);
    assert!(matches!(
        &terminal.deliveries[0].payload,
        FinalPayload::TerminalError { code, .. } if code == "dispatcher_failure_limit"
    ));
    assert_eq!(
        harness
            .scalar("SELECT state FROM local_requests LIMIT 1")
            .await?,
        "failed_terminal"
    );
    assert_eq!(harness.llm.calls.load(Ordering::SeqCst), 5);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scenario_13_disabled_configured_actor_prevents_readiness() -> Result<()> {
    let _serial = serialize_acceptance();
    let mut harness = RuntimeHarness::start_with_actor(vec![], false).await?;
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if harness.child.as_mut().unwrap().try_wait()?.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(harness.child.as_mut().unwrap().try_wait()?.is_some());
    assert!(!harness.socket.exists());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scenario_14_disconnect_before_accepted_never_races_false_missing() -> Result<()> {
    let _serial = serialize_acceptance();
    let harness = RuntimeHarness::start(vec![ProviderReply::Text {
        text: "registered".into(),
        pause_ms: 0,
    }])
    .await?;
    let request = request_id();
    drop(submit(&harness.socket, &request, "early disconnect")?);
    let mut resumed = resume(&harness.socket, &request)?;
    let _ = final_bundle(&mut resumed, &request)?;
    assert_eq!(harness.count("local_requests").await?, 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scenario_15_large_text_and_more_than_32_routes_replay_completely() -> Result<()> {
    let _serial = serialize_acceptance();
    let text = "x".repeat(300_000);
    let harness = RuntimeHarness::start(vec![
        ProviderReply::Files(33),
        ProviderReply::Text {
            text: text.clone(),
            pause_ms: 0,
        },
    ])
    .await?;
    let workspace = harness.root.join("workspaces").join(ACTOR);
    fs::create_dir_all(&workspace)?;
    for index in 0..33 {
        fs::write(
            workspace.join(format!("file-{index}.txt")),
            format!("file {index}"),
        )?;
    }
    let request = request_id();
    let mut stream = submit(&harness.socket, &request, "large bundle")?;
    wait_for_type(&mut stream, "accepted")?;
    let delivered = final_bundle(&mut stream, &request)?;
    assert_eq!(delivered.deliveries.len(), 34);
    assert_eq!(
        delivered
            .deliveries
            .iter()
            .filter(|delivery| matches!(delivery.payload, FinalPayload::File { .. }))
            .count(),
        33
    );
    assert_eq!(delivered.text(), Some(text.as_str()));
    assert!(delivered.chunk_frames > delivered.deliveries.len());
    assert_eq!(harness.count("outbox_deliveries").await?, 34);
    assert_eq!(harness.provider.calls.load(Ordering::SeqCst), 2);
    ack(
        &harness.socket,
        &request,
        &delivered.bundle_id,
        &delivered.delivery_ids(),
    )?;
    let mut replay = resume(&harness.socket, &request)?;
    let replayed = final_bundle(&mut replay, &request)?;
    assert!(replayed.replay);
    assert_eq!(replayed.bundle_id, delivered.bundle_id);
    assert_eq!(replayed.manifest_sha256, delivered.manifest_sha256);
    assert_eq!(replayed.text(), Some(text.as_str()));
    assert_eq!(replayed.delivery_ids(), delivered.delivery_ids());
    for (actual, expected) in replayed.deliveries.iter().zip(&delivered.deliveries) {
        assert_eq!(actual.payload, expected.payload);
    }
    assert_eq!(harness.provider.calls.load(Ordering::SeqCst), 2);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scenario_16_cancel_terminalizes_every_active_request_on_work_item() -> Result<()> {
    let _serial = serialize_acceptance();
    let harness = RuntimeHarness::start(vec![ProviderReply::Text {
        text: "too late".into(),
        pause_ms: 500,
    }])
    .await?;
    let first_id = request_id();
    let second_id = request_id();
    let mut first = submit(&harness.socket, &first_id, "cancel group")?;
    wait_for_type(&mut first, "accepted")?;
    let mut second = submit(&harness.socket, &second_id, "joined")?;
    wait_for_type(&mut second, "accepted")?;
    let mut cancel = connect(&harness.socket)?;
    send_frame(
        &mut cancel,
        json!({"type":"cancel","request_id":first_id,"cancel_id":Uuid::new_v4().to_string()}),
    )?;
    let accepted = wait_for_type(&mut cancel, "cancel_accepted")?;
    assert_eq!(
        accepted["body"]["affected_request_ids"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    let first_bundle = final_bundle(&mut first, &first_id)?;
    let second_bundle = final_bundle(&mut second, &second_id)?;
    for bundle in [&first_bundle, &second_bundle] {
        assert_eq!(bundle.deliveries.len(), 1);
        assert!(matches!(
            &bundle.deliveries[0].payload,
            FinalPayload::TerminalError { code, .. } if code == "cancelled"
        ));
    }
    assert_eq!(
        harness
            .scalar("SELECT CAST(COUNT(*) AS TEXT) FROM local_requests WHERE state='cancelled'")
            .await?,
        "2"
    );
    assert_eq!(
        harness
            .scalar_owned(format!(
                "SELECT result_bundle_id FROM local_requests WHERE request_id='{first_id}'"
            ))
            .await?,
        first_bundle.bundle_id.as_str()
    );
    assert_eq!(
        harness
            .scalar_owned(format!(
                "SELECT result_bundle_id FROM local_requests WHERE request_id='{second_id}'"
            ))
            .await?,
        second_bundle.bundle_id.as_str()
    );
    ack(
        &harness.socket,
        &first_id,
        &first_bundle.bundle_id,
        &first_bundle.delivery_ids(),
    )?;
    ack(
        &harness.socket,
        &second_id,
        &second_bundle.bundle_id,
        &second_bundle.delivery_ids(),
    )?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scenario_17_slow_or_malformed_clients_do_not_exhaust_daemon() -> Result<()> {
    let _serial = serialize_acceptance();
    let harness = RuntimeHarness::start(vec![ProviderReply::Text {
        text: "healthy".into(),
        pause_ms: 0,
    }])
    .await?;
    let mut slow = Vec::new();
    for _ in 0..96 {
        let mut stream = connect(&harness.socket)?;
        stream.write_all(&[0, 0])?;
        slow.push(stream);
    }
    for _ in 0..32 {
        let mut stream = connect(&harness.socket)?;
        stream.write_all(&4_u32.to_be_bytes())?;
        stream.write_all(b"nope")?;
    }
    let request = request_id();
    let mut valid = submit(&harness.socket, &request, "still responsive")?;
    wait_for_type(&mut valid, "accepted")?;
    let _ = final_bundle(&mut valid, &request)?;
    drop(slow);
    Ok(())
}

mod telegram_acceptance {
    use std::sync::{Arc, Mutex};

    use anyhow::Result;
    use async_trait::async_trait;

    use super::{ACTOR, InjectedLlm, InjectedReply};
    use codrik::{
        config::{ValidatedTelegramConfig, ValidatedTelegramIngressConfig},
        interfaces::telegram::{
            activity::TelegramActivityWorker,
            api::{
                EditMessageText, SendChatAction, SendFile, SendMessage, SendRichMessage,
                SetWebhook, TelegramApi, TelegramApiError, TelegramMessageRef, WebhookInfo,
            },
            delivery::TelegramDeliveryWorker,
            prepare_with_api,
        },
        runtime::{
            artifacts::ArtifactManager,
            gateway_activity::GatewayActivityHub,
            identity_link::{IdentityLinkManager, IdentityLinkService, SystemLinkCodeGenerator},
            model::{ActorId, Clock, ManualClock},
            runner::{ActorRunner, RunOnceOutcome, RunnerLimits},
            signals::ActorSignals,
            sqlite::SqliteRuntimeStore,
            store::ActorStore,
            stream_hub::{CompositeRuntimeEventPublisher, NoopRuntimeEventPublisher},
        },
        tools::ToolRegistry,
    };

    #[derive(Clone, Default)]
    struct TelegramApiMock {
        sent: Arc<Mutex<Vec<String>>>,
        rich_sent: Arc<Mutex<Vec<String>>>,
        edited: Arc<Mutex<Vec<String>>>,
        actions: Arc<Mutex<Vec<String>>>,
        reply_message_ids: Arc<Mutex<Vec<i64>>>,
    }

    #[async_trait]
    impl TelegramApi for TelegramApiMock {
        async fn get_me(
            &self,
        ) -> std::result::Result<codrik::interfaces::telegram::types::TelegramBot, TelegramApiError>
        {
            Ok(codrik::interfaces::telegram::types::TelegramBot {
                id: 900,
                is_bot: true,
                username: Some("codrik_bot".into()),
            })
        }

        async fn set_webhook(
            &self,
            _command: SetWebhook,
        ) -> std::result::Result<(), TelegramApiError> {
            Ok(())
        }

        async fn get_webhook_info(&self) -> std::result::Result<WebhookInfo, TelegramApiError> {
            Ok(WebhookInfo {
                url: "https://agent.example/webhooks/telegram".into(),
                allowed_updates: vec!["message".into()],
                pending_update_count: 0,
            })
        }

        async fn send_message(
            &self,
            command: SendMessage,
        ) -> std::result::Result<TelegramMessageRef, TelegramApiError> {
            if let Some(reply) = command.reply_parameters {
                self.reply_message_ids
                    .lock()
                    .unwrap()
                    .push(reply.message_id);
            }
            self.sent.lock().unwrap().push(command.text);
            Ok(TelegramMessageRef { message_id: 77 })
        }

        async fn send_rich_message(
            &self,
            command: SendRichMessage,
        ) -> std::result::Result<TelegramMessageRef, TelegramApiError> {
            self.rich_sent
                .lock()
                .unwrap()
                .push(command.rich_message.markdown);
            Ok(TelegramMessageRef { message_id: 78 })
        }

        async fn send_chat_action(
            &self,
            command: SendChatAction,
        ) -> std::result::Result<(), TelegramApiError> {
            self.actions.lock().unwrap().push(command.chat_id);
            Ok(())
        }

        async fn edit_message_text(
            &self,
            command: EditMessageText,
        ) -> std::result::Result<(), TelegramApiError> {
            self.edited.lock().unwrap().push(command.text);
            Ok(())
        }

        async fn send_photo(
            &self,
            _command: SendFile,
        ) -> std::result::Result<TelegramMessageRef, TelegramApiError> {
            unreachable!()
        }

        async fn send_document(
            &self,
            _command: SendFile,
        ) -> std::result::Result<TelegramMessageRef, TelegramApiError> {
            unreachable!()
        }
    }

    fn update(update_id: i64, message_id: i64, text: &str) -> serde_json::Value {
        serde_json::json!({
            "update_id": update_id,
            "message": {
                "message_id": message_id,
                "from": {
                    "id": 4242,
                    "is_bot": false,
                    "username": "owner"
                },
                "chat": {
                    "id": 4242,
                    "type": "private"
                },
                "text": text
            }
        })
    }

    const RICH_FINAL: &str = concat!(
        "# Result\n\n",
        "**Bold** and [link](https://example.com)\n\n",
        "- first\n- second\n\n",
        "| Name | Value |\n| --- | --- |\n| time | 22:45 |\n\n",
        "```rust\nlet ready = true;\n```\n\n",
        "> quoted\n\n",
        "||spoiler|| and $x^2$\n\n",
        "<details><summary>More</summary>Details</details>",
    );

    async fn counts(path: &std::path::Path) -> Result<(i64, i64, i64, i64, i64)> {
        let connection = tokio_rusqlite::Connection::open(path).await?;
        connection
            .call(
                |database| -> tokio_rusqlite::rusqlite::Result<(i64, i64, i64, i64, i64)> {
                    Ok((
                        database.query_row("SELECT COUNT(*) FROM gateway_commands", [], |row| {
                            row.get(0)
                        })?,
                        database.query_row(
                            "SELECT COUNT(*) FROM identities WHERE provider = 'telegram:900'",
                            [],
                            |row| row.get(0),
                        )?,
                        database.query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))?,
                        database
                            .query_row("SELECT COUNT(*) FROM work_items", [], |row| row.get(0))?,
                        database.query_row(
                            "SELECT COUNT(*) FROM gateway_deliveries",
                            [],
                            |row| row.get(0),
                        )?,
                    ))
                },
            )
            .await
            .map_err(Into::into)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn telegram_webhook_links_runs_and_delivers_without_duplicates() -> Result<()> {
        let root = std::env::current_dir()?
            .join(".t")
            .join(uuid::Uuid::new_v4().simple().to_string());
        std::fs::create_dir_all(&root)?;
        let database = root.join("runtime.sqlite");
        let artifacts = root.join("artifacts");
        let store = SqliteRuntimeStore::open(&database).await?;
        let actor = ActorId::parse_workspace_safe(ACTOR)?;
        let clock = ManualClock::new(1_000);
        store
            .ensure_initial_actor(&actor, &["*".into()], clock.now())
            .await?;
        let linking: Arc<dyn IdentityLinkManager> = Arc::new(IdentityLinkService::new(
            store.clone(),
            clock.clone(),
            SystemLinkCodeGenerator,
        ));
        let code = linking.issue_code(&actor).await?.code;
        let signals = ActorSignals::default();
        let api = TelegramApiMock::default();
        let activity = GatewayActivityHub::default();
        let probe = std::net::TcpListener::bind("127.0.0.1:0")?;
        let webhook_address = probe.local_addr()?;
        drop(probe);
        let prepared = Arc::new(
            prepare_with_api(
                ValidatedTelegramConfig {
                    token: "test-token".into(),
                    ingress: ValidatedTelegramIngressConfig::Webhook {
                        public_url: url::Url::parse("https://agent.example/webhooks/telegram")?,
                        listen: webhook_address,
                        webhook_secret: "acceptance_secret".into(),
                    },
                },
                store.clone(),
                linking,
                signals.clone(),
                activity.clone(),
                clock.clone(),
                artifacts.clone(),
                api.clone(),
            )
            .await?,
        );
        let (webhook_shutdown_tx, webhook_shutdown_rx) = tokio::sync::watch::channel(false);
        let webhook_task = {
            let prepared = prepared.clone();
            tokio::spawn(async move { prepared.webhook(webhook_shutdown_rx).await })
        };
        let client = reqwest::Client::new();
        let delivery = TelegramDeliveryWorker::new(
            store.clone(),
            api.clone(),
            clock.clone(),
            "telegram:900",
            "acceptance-delivery",
            artifacts.clone(),
        );

        let link_update = update(10, 100, &format!("/link {code}"));
        let link_response = client
            .post(format!("http://{webhook_address}/webhooks/telegram"))
            .header("content-type", "application/json")
            .header("x-telegram-bot-api-secret-token", "acceptance_secret")
            .json(&link_update)
            .send()
            .await?;
        assert_eq!(link_response.status(), reqwest::StatusCode::OK);
        assert_eq!(counts(&database).await?, (1, 1, 0, 0, 1));
        assert_eq!(delivery.run_once().await?, 1);
        assert!(api.sent.lock().unwrap().is_empty());
        assert_eq!(
            *api.rich_sent.lock().unwrap(),
            vec!["This channel is now linked."]
        );

        let text_update = update(11, 101, "remember this across channels");
        let text_response = client
            .post(format!("http://{webhook_address}/webhooks/telegram"))
            .header("content-type", "application/json")
            .header("x-telegram-bot-api-secret-token", "acceptance_secret")
            .json(&text_update)
            .send()
            .await?;
        assert_eq!(text_response.status(), reqwest::StatusCode::OK);
        assert_eq!(counts(&database).await?, (1, 1, 1, 1, 1));

        let streaming = Arc::new(TelegramActivityWorker::new(api.clone(), "telegram:900"));
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let streaming_task = {
            let streaming = streaming.clone();
            let receiver = activity.subscribe();
            tokio::spawn(async move { streaming.run(receiver, shutdown_rx).await })
        };
        let events = Arc::new(CompositeRuntimeEventPublisher::new(
            Arc::new(NoopRuntimeEventPublisher),
            activity,
        ));
        let runner = ActorRunner::new(
            InjectedLlm::new(vec![InjectedReply::PartialThenFinal {
                delta: "Пр".into(),
                final_text: RICH_FINAL.into(),
            }]),
            ToolRegistry::new(),
            signals,
            events,
            RunnerLimits::default(),
            ArtifactManager::new(artifacts, store.clone(), clock.clone()),
        );
        assert_eq!(
            runner.run_once("telegram-acceptance").await?,
            RunOnceOutcome::Completed
        );
        let restarted_store = SqliteRuntimeStore::open(&database).await?;
        let restarted_delivery = TelegramDeliveryWorker::new(
            restarted_store,
            api.clone(),
            clock.clone(),
            "telegram:900",
            "acceptance-delivery-restarted",
            root.join("artifacts"),
        );
        assert_eq!(restarted_delivery.run_once().await?, 1);
        let sent = api.sent.lock().unwrap().clone();
        let rich_sent = api.rich_sent.lock().unwrap().clone();
        assert!(sent.is_empty());
        assert_eq!(
            rich_sent,
            vec![
                "This channel is now linked.".to_owned(),
                RICH_FINAL.to_owned()
            ]
        );
        assert!(
            !rich_sent
                .iter()
                .any(|text| text == "Пр" || text == "Thinking…")
        );
        assert!(api.edited.lock().unwrap().is_empty());
        assert!(api.reply_message_ids.lock().unwrap().is_empty());
        assert!(!api.actions.lock().unwrap().is_empty());

        for replay in [&link_update, &text_update] {
            let response = client
                .post(format!("http://{webhook_address}/webhooks/telegram"))
                .header("content-type", "application/json")
                .header("x-telegram-bot-api-secret-token", "acceptance_secret")
                .json(replay)
                .send()
                .await?;
            assert_eq!(response.status(), reqwest::StatusCode::OK);
        }
        assert_eq!(delivery.run_once().await?, 0);
        assert_eq!(counts(&database).await?, (1, 1, 1, 1, 2));

        shutdown_tx.send_replace(true);
        streaming_task.await??;
        webhook_shutdown_tx.send_replace(true);
        webhook_task.await??;
        drop(store);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }
}
