use std::{
    fs,
    io::Write,
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::Path,
    process::Stdio,
    time::Duration,
};

use anyhow::{Context, Result, bail};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter},
    process::{Child, ChildStdin, ChildStdout, Command},
    sync::mpsc,
};

use crate::{
    config::ValidatedReticulumConfig,
    interfaces::reticulum::protocol::{
        BridgeCommand, BridgeEvent, MAX_PROTOCOL_LINE_BYTES, decode_event, encode_command,
    },
};

const BRIDGE_SOURCE: &str = include_str!("bridge.py");
const START_TIMEOUT: Duration = Duration::from_secs(30);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_STDERR_LINE_BYTES: usize = 4096;

pub struct BridgeProcess {
    child: Option<Child>,
    stdin: BufWriter<ChildStdin>,
    stdout: BufReader<ChildStdout>,
    start: BridgeCommand,
    stderr: Option<mpsc::Receiver<String>>,
}

impl BridgeProcess {
    pub async fn spawn(config: &ValidatedReticulumConfig, state_dir: &Path) -> Result<Self> {
        let bridge_path = materialize_bridge(state_dir)?;
        let mut child = Command::new(&config.python)
            .arg(&bridge_path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .with_context(|| {
                format!(
                    "failed to start Reticulum bridge with {}",
                    config.python.display()
                )
            })?;
        let stdin = child
            .stdin
            .take()
            .context("Reticulum bridge stdin missing")?;
        let stdout = child
            .stdout
            .take()
            .context("Reticulum bridge stdout missing")?;
        let stderr = child
            .stderr
            .take()
            .context("Reticulum bridge stderr missing")?;
        let (stderr_tx, stderr_rx) = mpsc::channel(32);
        tokio::spawn(drain_stderr(stderr, stderr_tx));
        Ok(Self {
            child: Some(child),
            stdin: BufWriter::new(stdin),
            stdout: BufReader::new(stdout),
            start: BridgeCommand::Start {
                state_dir: state_dir.to_owned(),
                rns_host: config.host.clone(),
                rns_port: config.port,
            },
            stderr: Some(stderr_rx),
        })
    }

    pub async fn start(&mut self) -> Result<String> {
        let start = self.start.clone();
        self.send(&start).await?;
        let event = tokio::time::timeout(START_TIMEOUT, self.next_event())
            .await
            .context("Reticulum bridge readiness timed out")??;
        match event {
            BridgeEvent::Ready { destination } => Ok(destination),
            BridgeEvent::Fatal { error } => bail!("Reticulum bridge failed: {error}"),
            _ => bail!("Reticulum bridge emitted an event before readiness"),
        }
    }

    pub async fn send(&mut self, command: &BridgeCommand) -> Result<()> {
        self.stdin.write_all(&encode_command(command)?).await?;
        self.stdin.flush().await?;
        Ok(())
    }

    pub async fn next_event(&mut self) -> Result<BridgeEvent> {
        let line = read_protocol_line(&mut self.stdout).await?;
        decode_event(&line)
    }

    pub fn take_stderr(&mut self) -> mpsc::Receiver<String> {
        self.stderr
            .take()
            .expect("Reticulum bridge stderr receiver already taken")
    }

    pub async fn shutdown(mut self) -> Result<()> {
        let _ = self.send(&BridgeCommand::Shutdown).await;
        let mut child = self
            .child
            .take()
            .context("Reticulum bridge child missing")?;
        match tokio::time::timeout(SHUTDOWN_TIMEOUT, child.wait()).await {
            Ok(status) => {
                status?;
            }
            Err(_) => {
                child.kill().await?;
                child.wait().await?;
            }
        }
        Ok(())
    }
}

impl Drop for BridgeProcess {
    fn drop(&mut self) {
        let Some(mut child) = self.child.take() else {
            return;
        };
        let _ = child.start_kill();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                let _ = child.wait().await;
            });
        }
    }
}

async fn drain_stderr(stderr: tokio::process::ChildStderr, sender: mpsc::Sender<String>) {
    let mut reader = BufReader::new(stderr);
    let mut line = Vec::with_capacity(MAX_STDERR_LINE_BYTES);
    loop {
        let available = match reader.fill_buf().await {
            Ok(available) => available,
            Err(_) => return,
        };
        if available.is_empty() {
            if !line.is_empty() {
                send_stderr_line(&sender, &line);
            }
            return;
        }
        let consumed = available
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(available.len(), |index| index + 1);
        let remaining = MAX_STDERR_LINE_BYTES.saturating_sub(line.len());
        line.extend_from_slice(&available[..consumed.min(remaining)]);
        let complete = available[..consumed].last() == Some(&b'\n');
        reader.consume(consumed);
        if complete {
            if line.last() == Some(&b'\n') {
                line.pop();
            }
            send_stderr_line(&sender, &line);
            line.clear();
        }
    }
}

fn send_stderr_line(sender: &mpsc::Sender<String>, bytes: &[u8]) {
    let sanitized: String = String::from_utf8_lossy(bytes)
        .chars()
        .filter(|character| *character == '\t' || !character.is_control())
        .collect();
    if !sanitized.is_empty() {
        eprintln!("reticulum bridge: {sanitized}");
    }
    let _ = sender.try_send(sanitized);
}

async fn read_protocol_line(reader: &mut BufReader<ChildStdout>) -> Result<Vec<u8>> {
    let mut line = Vec::new();
    loop {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            bail!("Reticulum bridge stdout closed");
        }
        let consumed = match available.iter().position(|byte| *byte == b'\n') {
            Some(index) => index + 1,
            None => available.len(),
        };
        if line.len() + consumed > MAX_PROTOCOL_LINE_BYTES {
            bail!("Reticulum bridge protocol line is too large");
        }
        line.extend_from_slice(&available[..consumed]);
        reader.consume(consumed);
        if line.last() == Some(&b'\n') {
            line.pop();
            return Ok(line);
        }
    }
}

fn materialize_bridge(state_dir: &Path) -> Result<std::path::PathBuf> {
    let path = state_dir.join("bridge.py");
    if let Ok(metadata) = fs::symlink_metadata(&path) {
        if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
            bail!("Reticulum bridge path is not a regular file");
        }
        if fs::read(&path)? == BRIDGE_SOURCE.as_bytes() {
            fs::set_permissions(&path, fs::Permissions::from_mode(0o600))?;
            return Ok(path);
        }
    }
    let temporary = state_dir.join(format!(".bridge-{}.tmp", std::process::id()));
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true).mode(0o600);
    let mut file = options.open(&temporary).or_else(|error| {
        if error.kind() == std::io::ErrorKind::AlreadyExists {
            fs::remove_file(&temporary)?;
            options.open(&temporary)
        } else {
            Err(error)
        }
    })?;
    file.write_all(BRIDGE_SOURCE.as_bytes())?;
    file.sync_all()?;
    drop(file);
    fs::rename(&temporary, &path)?;
    Ok(path)
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        os::unix::fs::{PermissionsExt, symlink},
        path::PathBuf,
    };

    use anyhow::Result;

    use super::BridgeProcess;
    use crate::{
        config::ValidatedReticulumConfig,
        interfaces::reticulum::protocol::{BridgeCommand, BridgeDeliveryOutcome, BridgeEvent},
        runtime::ipc::security::create_secure_directory,
    };

    const DESTINATION: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    fn fake_bridge(script: &str) -> Result<(PathBuf, PathBuf)> {
        let root = std::env::temp_dir()
            .canonicalize()?
            .join(format!("codrik-reticulum-{}", uuid::Uuid::new_v4()));
        create_secure_directory(&root)?;
        let executable = root.join("fake-bridge");
        fs::write(&executable, script)?;
        fs::set_permissions(&executable, fs::Permissions::from_mode(0o700))?;
        Ok((root, executable))
    }

    #[tokio::test]
    async fn bridge_starts_reads_ready_and_shuts_down() -> Result<()> {
        let script = format!(
            "#!/bin/sh\nread start\nprintf '%s\\n' '{{\"type\":\"ready\",\"destination\":\"{DESTINATION}\"}}'\nread send\nprintf '%s\\n' '{{\"type\":\"delivery\",\"delivery_id\":\"delivery-1\",\"outcome\":\"delivered\",\"retry_after_ms\":null}}'\nread shutdown\n"
        );
        let (root, executable) = fake_bridge(&script)?;
        let config = ValidatedReticulumConfig {
            host: "mesh.example".into(),
            port: 4242,
            python: executable,
        };
        let mut bridge = BridgeProcess::spawn(&config, &root).await?;
        assert_eq!(bridge.start().await?, DESTINATION);
        bridge
            .send(&BridgeCommand::Send {
                delivery_id: "delivery-1".into(),
                destination: DESTINATION.into(),
                text: "hello".into(),
            })
            .await?;
        assert!(matches!(
            bridge.next_event().await?,
            BridgeEvent::Delivery {
                outcome: BridgeDeliveryOutcome::Delivered,
                ..
            }
        ));
        bridge.shutdown().await?;
        assert!(root.join("bridge.py").is_file());
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[tokio::test]
    async fn bridge_rejects_oversized_stdout() -> Result<()> {
        let (root, executable) = fake_bridge(
            "#!/bin/sh\nread start\ni=0\nwhile [ $i -lt 1048580 ]; do printf x; i=$((i+1)); done\nprintf '\\n'\n",
        )?;
        let config = ValidatedReticulumConfig {
            host: "mesh.example".into(),
            port: 4242,
            python: executable,
        };
        let mut bridge = BridgeProcess::spawn(&config, &root).await?;
        assert!(bridge.start().await.is_err());
        bridge.shutdown().await?;
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[tokio::test]
    async fn bridge_drains_and_bounds_stderr() -> Result<()> {
        let script = format!(
            "#!/bin/sh\nread start\ni=0\nwhile [ $i -lt 5000 ]; do printf x >&2; i=$((i+1)); done\nprintf '\\n' >&2\nprintf '%s\\n' '{{\"type\":\"ready\",\"destination\":\"{DESTINATION}\"}}'\nread shutdown\n"
        );
        let (root, executable) = fake_bridge(&script)?;
        let config = ValidatedReticulumConfig {
            host: "mesh.example".into(),
            port: 4242,
            python: executable,
        };
        let mut bridge = BridgeProcess::spawn(&config, &root).await?;
        let mut stderr = bridge.take_stderr();
        assert_eq!(bridge.start().await?, DESTINATION);
        assert!(stderr.recv().await.unwrap().len() <= 4096);
        bridge.shutdown().await?;
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[tokio::test]
    async fn bridge_rejects_existing_symlink_at_materialized_path() -> Result<()> {
        let (root, executable) = fake_bridge("#!/bin/sh\nexit 0\n")?;
        symlink(&executable, root.join("bridge.py"))?;
        let config = ValidatedReticulumConfig {
            host: "mesh.example".into(),
            port: 4242,
            python: executable,
        };
        assert!(BridgeProcess::spawn(&config, &root).await.is_err());
        fs::remove_dir_all(root)?;
        Ok(())
    }
}
