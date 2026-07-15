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
use serde_json::{Value, json};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    task::JoinHandle,
};
use uuid::Uuid;

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
    HttpError,
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
                        ProviderReply::HttpError => {
                            let body = br#"{"error":{"message":"scripted failure","type":"server_error"}}"#;
                            let response = format!(
                                "HTTP/1.1 500 Internal Server Error\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                                body.len()
                            );
                            let _ = socket.write_all(response.as_bytes()).await;
                            let _ = socket.write_all(body).await;
                        }
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
        fs::write(root.join("users.json"), format!(r#"{{"version":1,"actors":{{"{ACTOR}":{{"enabled":{enabled},"display_name":null,"identities":[],"tools":["*"]}}}}}}"#)).context("write users")?;
        fs::set_permissions(root.join("users.json"), fs::Permissions::from_mode(0o600))
            .context("secure users")?;
        fs::write(&config, format!("api_key: test\nbase_url: {}\nmodel: scripted\nruntime:\n  actor_id: {ACTOR}\n  database_path: {}\n  socket_path: {}\n  lock_path: {}\n  artifact_path: {}\n", provider.endpoint, database.display(), socket.display(), root.join("k").display(), root.join("a").display())).context("write config")?;
        let mut harness = Self {
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

    fn restart(&mut self) -> Result<()> {
        self.kill()?;
        self.spawn()?;
        self.wait_ready(Duration::from_secs(12))
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
        let connection = tokio_rusqlite::Connection::open(&self.database).await?;
        connection
            .call(move |db| db.query_row(sql, [], |row| row.get(0)))
            .await
            .map_err(Into::into)
    }
}

impl Drop for RuntimeHarness {
    fn drop(&mut self) {
        let _ = self.kill();
        let _ = fs::remove_dir_all(&self.root);
    }
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

fn final_bundle(stream: &mut UnixStream) -> Result<(String, Vec<String>, bool)> {
    let begin = wait_for_type(stream, "final_begin")?;
    let bundle = begin["body"]["bundle_id"]
        .as_str()
        .context("bundle id")?
        .to_owned();
    let replay = begin["body"]["replay"].as_bool().unwrap_or(false);
    let deliveries = begin["body"]["manifest"]
        .as_array()
        .context("manifest")?
        .iter()
        .map(|entry| entry["delivery_id"].as_str().unwrap().to_owned())
        .collect();
    loop {
        let event = read_frame(stream)?;
        if event["body"]["type"] == "final_end" {
            return Ok((bundle, deliveries, replay));
        }
    }
}

fn ack(socket: &Path, request: &str, bundle: &str, deliveries: &[String]) -> Result<()> {
    let mut stream = connect(socket)?;
    send_frame(
        &mut stream,
        json!({"type":"ack_final","request_id":request,"bundle_id":bundle,"delivery_ids":deliveries}),
    )?;
    let event = wait_for_type(&mut stream, "ack_accepted")?;
    assert_eq!(event["body"]["bundle_id"], bundle);
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
    let (bundle, deliveries, _) = final_bundle(&mut stream).with_context(|| {
        format!(
            "daemon log:\n{}",
            fs::read_to_string(&harness.log).unwrap_or_default()
        )
    })?;
    ack(&harness.socket, &request, &bundle, &deliveries)?;
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
    let _ = final_bundle(&mut duplicate)?;
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
    let _ = final_bundle(&mut resumed)?;
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
    let (bundle, deliveries, _) = final_bundle(&mut live)?;
    ack(&harness.socket, &request, &bundle, &deliveries)?;
    let mut replay = resume(&harness.socket, &request)?;
    let (_, _, replayed) = final_bundle(&mut replay)?;
    assert!(replayed);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scenario_06_multiple_incorporated_requests_receive_final_rows() -> Result<()> {
    let _serial = serialize_acceptance();
    let harness = RuntimeHarness::start(vec![ProviderReply::Text {
        text: "shared".into(),
        pause_ms: 150,
    }])
    .await?;
    let first_id = request_id();
    let second_id = request_id();
    let mut first = submit(&harness.socket, &first_id, "first")?;
    wait_for_type(&mut first, "accepted")?;
    let mut second = submit(&harness.socket, &second_id, "second")?;
    wait_for_type(&mut second, "accepted")?;
    let _ = final_bundle(&mut first)?;
    let _ = final_bundle(&mut second)?;
    assert_eq!(harness.count("result_bundles").await?, 2);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scenario_07_restart_after_ingress_preserves_durable_state() -> Result<()> {
    let _serial = serialize_acceptance();
    let mut harness = RuntimeHarness::start(vec![ProviderReply::Text {
        text: "after restart".into(),
        pause_ms: 300,
    }])
    .await?;
    let request = request_id();
    let mut stream = submit(&harness.socket, &request, "crash boundary")?;
    wait_for_type(&mut stream, "accepted")?;
    harness.restart()?;
    let mut resumed = resume(&harness.socket, &request)?;
    let _ = final_bundle(&mut resumed)?;
    assert_eq!(harness.count("local_requests").await?, 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scenario_08_final_end_without_ack_redelivers_without_recomputation() -> Result<()> {
    let _serial = serialize_acceptance();
    let mut harness = RuntimeHarness::start(vec![ProviderReply::Text {
        text: "immutable".into(),
        pause_ms: 0,
    }])
    .await?;
    let request = request_id();
    let mut stream = submit(&harness.socket, &request, "lost ack")?;
    wait_for_type(&mut stream, "accepted")?;
    let (original_bundle, _, _) = final_bundle(&mut stream)?;
    harness.kill()?;
    let db = tokio_rusqlite::Connection::open(&harness.database).await?;
    db.call(|db| {
        db.execute(
            "UPDATE result_bundles SET claim_expires_at=0 WHERE state='delivering'",
            [],
        )?;
        Ok::<(), tokio_rusqlite::rusqlite::Error>(())
    })
    .await?;
    harness.spawn()?;
    harness.wait_ready(Duration::from_secs(12))?;
    let mut resumed = resume(&harness.socket, &request)?;
    let (redelivered_bundle, _, _) = final_bundle(&mut resumed)?;
    assert_eq!(redelivered_bundle, original_bundle);
    assert_eq!(harness.provider.calls.load(Ordering::SeqCst), 1);
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
    let _ = final_bundle(&mut resumed)?;
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
    let harness = RuntimeHarness::start(vec![
        ProviderReply::HttpError,
        ProviderReply::HttpError,
        ProviderReply::HttpError,
        ProviderReply::HttpError,
        ProviderReply::HttpError,
    ])
    .await?;
    let request = request_id();
    let mut stream = submit(&harness.socket, &request, "fail five times")?;
    wait_for_type(&mut stream, "accepted")?;
    let _ = final_bundle(&mut stream)?;
    assert_eq!(
        harness
            .scalar("SELECT state FROM local_requests LIMIT 1")
            .await?,
        "failed_terminal"
    );
    assert!(harness.provider.calls.load(Ordering::SeqCst) >= 5);
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
    let _ = final_bundle(&mut resumed)?;
    assert_eq!(harness.count("local_requests").await?, 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scenario_15_large_text_and_more_than_32_routes_replay_completely() -> Result<()> {
    let _serial = serialize_acceptance();
    let text = "x".repeat(300_000);
    let harness = RuntimeHarness::start(vec![
        ProviderReply::Files(33),
        ProviderReply::Text { text, pause_ms: 0 },
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
    let (_, deliveries, _) = final_bundle(&mut stream)?;
    assert_eq!(deliveries.len(), 34);
    assert_eq!(harness.count("outbox_deliveries").await?, 34);
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
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if harness
            .scalar("SELECT CAST(COUNT(*) AS TEXT) FROM local_requests WHERE state='cancelled'")
            .await?
            == "2"
        {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    bail!("requests did not both cancel")
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
    let _ = final_bundle(&mut valid)?;
    drop(slow);
    Ok(())
}
