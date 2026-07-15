use std::{env, future::Future, io::Write};

use anyhow::{Context, Result, bail};

use crate::{
    config::{AppConfig, codrik_dir},
    interfaces::{
        local_renderer::{LocalRenderer, RenderAction},
        request_metadata::{RequestMetadataState, RequestMetadataStore, recovery_command},
    },
    runtime::{
        RequestId,
        ipc::client::{ClientEventStream, LocalIpcClient},
        model::{Clock, SystemClock},
    },
    updater,
};

pub async fn run() -> Result<()> {
    match CliCommand::parse(env::args().skip(1))? {
        CliCommand::Update => updater::update().await,
        CliCommand::Serve => crate::app::serve(AppConfig::load_default()?).await,
        CliCommand::Submit(prompt) => submit(prompt).await,
        CliCommand::Resume(request) => resume(request).await,
        CliCommand::Cancel(request) => cancel(request).await,
    }
}

async fn submit(prompt: String) -> Result<()> {
    let (client, metadata) = local_context()?;
    let request = RequestId::new();
    metadata.create(&request, SystemClock.now().0, &prompt)?;
    let stream = client.submit(request.clone(), prompt).await?;
    metadata.set_state(&request, RequestMetadataState::SentUnconfirmed)?;
    run_rendered(&client, &metadata, request, stream).await
}

async fn resume(request: RequestId) -> Result<()> {
    let (client, metadata) = local_context()?;
    let stream = client.resume(request.clone()).await?;
    run_rendered(&client, &metadata, request, stream).await
}

async fn cancel(request: RequestId) -> Result<()> {
    let (client, _) = local_context()?;
    let mut stream = client.cancel(request.clone()).await?;
    loop {
        let Some(event) = stream.next_event().await? else {
            bail!("daemon closed before confirming cancellation for {request}")
        };
        match event.body {
            crate::runtime::ipc::protocol::ServerEventBody::CancelAccepted {
                request_id, ..
            } if request_id == request => return Ok(()),
            crate::runtime::ipc::protocol::ServerEventBody::RequestError {
                code, message, ..
            } => bail!("daemon rejected cancellation ({code}): {message}"),
            crate::runtime::ipc::protocol::ServerEventBody::ProtocolError { code, message } => {
                bail!("daemon protocol error ({code:?}): {message}")
            }
            _ => {}
        }
    }
}

fn local_context() -> Result<(LocalIpcClient, RequestMetadataStore)> {
    let config = AppConfig::load_default()?;
    let paths = config.required_runtime()?.resolve_paths(&codrik_dir()?)?;
    Ok((
        LocalIpcClient::new(paths.socket),
        RequestMetadataStore::new(paths.client_requests),
    ))
}

async fn run_rendered(
    client: &LocalIpcClient,
    metadata: &RequestMetadataStore,
    request: RequestId,
    stream: ClientEventStream,
) -> Result<()> {
    let mut renderer = LocalRenderer::stdout(request.clone());
    let mut recovery = std::io::stderr();
    drive_operation(
        client,
        &request,
        stream,
        metadata,
        &mut renderer,
        &mut recovery,
        async {
            let _ = tokio::signal::ctrl_c().await;
        },
    )
    .await
}

async fn drive_operation<W, R, F>(
    client: &LocalIpcClient,
    request: &RequestId,
    mut stream: ClientEventStream,
    metadata: &RequestMetadataStore,
    renderer: &mut LocalRenderer<W>,
    recovery: &mut R,
    interrupt: F,
) -> Result<()>
where
    W: Write,
    R: Write,
    F: Future<Output = ()>,
{
    tokio::pin!(interrupt);
    loop {
        let event = tokio::select! {
            _ = &mut interrupt => {
                writeln!(recovery, "{}", recovery_command(request))?;
                return Ok(());
            }
            event = stream.next_event() => event?,
        };
        let Some(event) = event else {
            writeln!(recovery, "{}", recovery_command(request))?;
            return Ok(());
        };
        match renderer.handle(event)? {
            RenderAction::Continue => {}
            RenderAction::Accepted => {
                metadata.set_state_if_present(request, RequestMetadataState::Accepted)?;
            }
            RenderAction::FinalVerified {
                request_id,
                bundle_id,
                delivery_ids,
            } => {
                let acknowledgement = client.acknowledge_final(request_id, bundle_id, delivery_ids);
                tokio::pin!(acknowledgement);
                let acknowledgement = tokio::select! {
                    _ = &mut interrupt => {
                        writeln!(recovery, "{}", recovery_command(request))?;
                        return Ok(())
                    }
                    result = &mut acknowledgement => result,
                };
                if let Err(error) = acknowledgement {
                    writeln!(recovery, "{}", recovery_command(request))?;
                    return Err(error);
                }
                metadata.set_state_if_present(request, RequestMetadataState::Terminal)?;
                return Ok(());
            }
            RenderAction::Recover => {
                writeln!(recovery, "{}", recovery_command(request))?;
                return Ok(());
            }
            RenderAction::DaemonError(message) => bail!(message),
            RenderAction::CancelAccepted => bail!("unexpected cancellation response"),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
enum CliCommand {
    Update,
    Serve,
    Resume(RequestId),
    Cancel(RequestId),
    Submit(String),
}

impl CliCommand {
    fn parse(args: impl IntoIterator<Item = String>) -> Result<Self> {
        let mut args = args.into_iter();
        let command = args.next().context("missing query or command")?;

        let parsed = match command.as_str() {
            "update" => Self::Update,
            "serve" => Self::Serve,
            "resume" => {
                let request_id = args.next().context("missing request id")?;
                Self::Resume(RequestId::parse(&request_id)?)
            }
            "cancel" => {
                let request_id = args.next().context("missing request id")?;
                Self::Cancel(RequestId::parse(&request_id)?)
            }
            "gateway" | "--session" | "--stream" => {
                bail!("legacy local command is unsupported; use serve, resume, or cancel")
            }
            _ if command.starts_with('-') => bail!("unknown option: {command}"),
            _ => Self::Submit(command),
        };
        if args.next().is_some() {
            bail!("unexpected extra argument")
        }
        Ok(parsed)
    }
}

#[cfg(test)]
mod tests {
    use std::{future::pending, path::PathBuf, sync::Arc, time::Duration};

    use anyhow::Result;
    use tokio::{io::AsyncReadExt, net::UnixListener, sync::Notify};

    use super::{CliCommand, drive_operation};
    use crate::{
        interfaces::{
            local_renderer::LocalRenderer,
            request_metadata::{RequestMetadataState, RequestMetadataStore},
        },
        runtime::{
            BundleId, BundleState, DeliveryId, RequestId,
            ipc::{
                client::LocalIpcClient,
                protocol::{ClientRequestBody, FrameReader, FrameWriter, encode_bundle},
            },
            store::{BundleManifest, FinalPayload, ResultBundle},
        },
    };

    #[test]
    fn parses_supported_commands() -> Result<()> {
        const UUID: &str = "0190f2ef-0000-7000-8000-000000000001";
        let parse = |args: &[&str]| CliCommand::parse(args.iter().copied().map(String::from));

        assert_eq!(parse(&["serve"])?, CliCommand::Serve);
        assert_eq!(parse(&["update"])?, CliCommand::Update);
        assert_eq!(
            parse(&["resume", UUID])?,
            CliCommand::Resume(RequestId::parse(UUID)?)
        );
        assert_eq!(
            parse(&["cancel", UUID])?,
            CliCommand::Cancel(RequestId::parse(UUID)?)
        );
        assert_eq!(parse(&["hello"])?, CliCommand::Submit("hello".into()));
        assert!(parse(&["gateway", "telegram"]).is_err());
        assert!(parse(&["--session", "x", "hello"]).is_err());
        assert!(parse(&["--stream", "hello"]).is_err());
        assert!(parse(&["update", "extra"]).is_err());
        assert!(parse(&["serve", "extra"]).is_err());
        assert!(parse(&["resume", UUID, "extra"]).is_err());
        assert!(parse(&["cancel", UUID, "extra"]).is_err());
        assert!(parse(&["hello", "extra"]).is_err());

        Ok(())
    }

    #[tokio::test]
    async fn verified_final_is_acked_before_metadata_becomes_terminal() -> Result<()> {
        let socket = temp_socket();
        let listener = UnixListener::bind(&socket)?;
        let request = RequestId::new();
        let bundle = BundleId::new();
        let delivery = DeliveryId::new();
        let expected_request = request.clone();
        let expected_bundle = bundle.clone();
        let expected_delivery = delivery.clone();
        let server = tokio::spawn(async move {
            let (mut operation, _) = listener.accept().await?;
            let request_frame = FrameReader::new(&mut operation)
                .read_client_request()
                .await?;
            assert!(
                matches!(request_frame.body, ClientRequestBody::Resume { request_id } if request_id == expected_request)
            );
            let events = encode_bundle(
                &ResultBundle {
                    id: expected_bundle.clone(),
                    request_id: expected_request.clone(),
                    state: BundleState::Delivered,
                    manifest: BundleManifest {
                        entries: vec![],
                        sha256: String::new(),
                    },
                    deliveries: vec![(
                        expected_delivery.clone(),
                        FinalPayload::Text {
                            text: "done".into(),
                        },
                    )],
                },
                true,
            )?;
            let mut writer = FrameWriter::new(&mut operation);
            for event in events {
                writer.write_server_event(&event).await?;
            }

            let (mut ack, _) = listener.accept().await?;
            let ack_request = FrameReader::new(&mut ack).read_client_request().await?;
            assert!(
                matches!(ack_request.body, ClientRequestBody::AckFinal { request_id, bundle_id, delivery_ids }
                if request_id == expected_request && bundle_id == expected_bundle && delivery_ids == vec![expected_delivery])
            );
            FrameWriter::new(&mut ack)
                .write_server_event(&crate::runtime::ipc::protocol::ServerEvent::new(
                    crate::runtime::ipc::protocol::ServerEventBody::AckAccepted {
                        request_id: expected_request,
                        bundle_id: expected_bundle,
                    },
                ))
                .await?;
            anyhow::Ok(())
        });

        let metadata_root = temp_root();
        let metadata = RequestMetadataStore::new(metadata_root.clone());
        metadata.create(&request, 1, "secret")?;
        metadata.set_state(&request, RequestMetadataState::Accepted)?;
        let client = LocalIpcClient::new(socket.clone());
        let stream = client.resume(request.clone()).await?;
        let mut renderer = LocalRenderer::for_request(Vec::new(), false, request.clone());
        let mut recovery = Vec::new();
        drive_operation(
            &client,
            &request,
            stream,
            &metadata,
            &mut renderer,
            &mut recovery,
            pending(),
        )
        .await?;
        server.await??;
        assert_eq!(
            metadata.load(&request)?.unwrap().state,
            RequestMetadataState::Terminal
        );
        assert_eq!(String::from_utf8(renderer.into_inner())?, "done\n");
        assert!(recovery.is_empty());
        std::fs::remove_file(socket)?;
        std::fs::remove_dir_all(metadata_root)?;
        Ok(())
    }

    #[tokio::test]
    async fn eof_keeps_metadata_nonterminal_and_prints_exact_resume_command() -> Result<()> {
        let socket = temp_socket();
        let listener = UnixListener::bind(&socket)?;
        let request = RequestId::new();
        let server = tokio::spawn(async move {
            let (mut operation, _) = listener.accept().await?;
            FrameReader::new(&mut operation)
                .read_client_request()
                .await?;
            anyhow::Ok(())
        });
        let metadata_root = temp_root();
        let metadata = RequestMetadataStore::new(metadata_root.clone());
        metadata.create(&request, 1, "secret")?;
        metadata.set_state(&request, RequestMetadataState::SentUnconfirmed)?;
        let client = LocalIpcClient::new(socket.clone());
        let stream = client.resume(request.clone()).await?;
        let mut renderer = LocalRenderer::for_request(Vec::new(), false, request.clone());
        let mut recovery = Vec::new();
        drive_operation(
            &client,
            &request,
            stream,
            &metadata,
            &mut renderer,
            &mut recovery,
            pending(),
        )
        .await?;
        server.await??;
        assert_eq!(
            metadata.load(&request)?.unwrap().state,
            RequestMetadataState::SentUnconfirmed
        );
        assert_eq!(
            String::from_utf8(recovery)?,
            format!("codrik resume {request}\n")
        );
        std::fs::remove_file(socket)?;
        std::fs::remove_dir_all(metadata_root)?;
        Ok(())
    }

    #[tokio::test]
    async fn post_final_ack_failure_prints_recovery_and_keeps_metadata_nonterminal() -> Result<()> {
        let socket = temp_socket();
        let listener = UnixListener::bind(&socket)?;
        let request = RequestId::new();
        let expected_request = request.clone();
        let server = tokio::spawn(async move {
            let (mut operation, _) = listener.accept().await?;
            FrameReader::new(&mut operation)
                .read_client_request()
                .await?;
            let events = encode_bundle(
                &ResultBundle {
                    id: BundleId::new(),
                    request_id: expected_request,
                    state: BundleState::Delivered,
                    manifest: BundleManifest {
                        entries: vec![],
                        sha256: String::new(),
                    },
                    deliveries: vec![(
                        DeliveryId::new(),
                        FinalPayload::Text {
                            text: "done".into(),
                        },
                    )],
                },
                true,
            )?;
            for event in events {
                FrameWriter::new(&mut operation)
                    .write_server_event(&event)
                    .await?;
            }
            let (mut ack, _) = listener.accept().await?;
            FrameReader::new(&mut ack).read_client_request().await?;
            // EOF without AckAccepted is an ambiguous failure.
            anyhow::Ok(())
        });
        let metadata_root = temp_root();
        let metadata = RequestMetadataStore::new(metadata_root.clone());
        metadata.create(&request, 1, "secret")?;
        metadata.set_state(&request, RequestMetadataState::Accepted)?;
        let client = LocalIpcClient::new(socket.clone());
        let stream = client.resume(request.clone()).await?;
        let mut renderer = LocalRenderer::for_request(Vec::new(), false, request.clone());
        let mut recovery = Vec::new();
        assert!(
            drive_operation(
                &client,
                &request,
                stream,
                &metadata,
                &mut renderer,
                &mut recovery,
                pending(),
            )
            .await
            .is_err()
        );
        server.await??;
        assert_eq!(
            metadata.load(&request)?.unwrap().state,
            RequestMetadataState::Accepted
        );
        assert_eq!(
            String::from_utf8(recovery)?,
            format!("codrik resume {request}\n")
        );
        std::fs::remove_file(socket)?;
        std::fs::remove_dir_all(metadata_root)?;
        Ok(())
    }

    #[tokio::test]
    async fn interrupt_closes_connection_without_sending_cancel() -> Result<()> {
        let socket = temp_socket();
        let listener = UnixListener::bind(&socket)?;
        let request = RequestId::new();
        let server = tokio::spawn(async move {
            let (mut operation, _) = listener.accept().await?;
            FrameReader::new(&mut operation)
                .read_client_request()
                .await?;
            let mut remainder = Vec::new();
            operation.read_to_end(&mut remainder).await?;
            assert!(remainder.is_empty());
            anyhow::Ok(())
        });
        let metadata_root = temp_root();
        let metadata = RequestMetadataStore::new(metadata_root.clone());
        metadata.create(&request, 1, "secret")?;
        metadata.set_state(&request, RequestMetadataState::Accepted)?;
        let client = LocalIpcClient::new(socket.clone());
        let stream = client.resume(request.clone()).await?;
        let mut renderer = LocalRenderer::for_request(Vec::new(), false, request.clone());
        let mut recovery = Vec::new();
        drive_operation(
            &client,
            &request,
            stream,
            &metadata,
            &mut renderer,
            &mut recovery,
            async {},
        )
        .await?;
        server.await??;
        assert_eq!(
            metadata.load(&request)?.unwrap().state,
            RequestMetadataState::Accepted
        );
        assert_eq!(
            String::from_utf8(recovery)?,
            format!("codrik resume {request}\n")
        );
        std::fs::remove_file(socket)?;
        std::fs::remove_dir_all(metadata_root)?;
        Ok(())
    }

    #[tokio::test]
    async fn interrupt_drops_blocked_ack_without_cancel_and_keeps_recovery() -> Result<()> {
        let socket = temp_socket();
        let listener = UnixListener::bind(&socket)?;
        let request = RequestId::new();
        let expected_request = request.clone();
        let ack_started = Arc::new(Notify::new());
        let server_notice = ack_started.clone();
        let release_server = Arc::new(Notify::new());
        let server_release = release_server.clone();
        let server = tokio::spawn(async move {
            let (mut operation, _) = listener.accept().await?;
            FrameReader::new(&mut operation)
                .read_client_request()
                .await?;
            let events = encode_bundle(
                &ResultBundle {
                    id: BundleId::new(),
                    request_id: expected_request,
                    state: BundleState::Delivered,
                    manifest: BundleManifest {
                        entries: vec![],
                        sha256: String::new(),
                    },
                    deliveries: vec![(
                        DeliveryId::new(),
                        FinalPayload::Text {
                            text: "done".into(),
                        },
                    )],
                },
                true,
            )?;
            for event in events {
                FrameWriter::new(&mut operation)
                    .write_server_event(&event)
                    .await?;
            }

            let (mut ack, _) = listener.accept().await?;
            let request = FrameReader::new(&mut ack).read_client_request().await?;
            assert!(matches!(request.body, ClientRequestBody::AckFinal { .. }));
            server_notice.notify_one();
            server_release.notified().await;
            anyhow::Ok(())
        });

        let metadata_root = temp_root();
        let metadata = RequestMetadataStore::new(metadata_root.clone());
        metadata.create(&request, 1, "secret")?;
        metadata.set_state(&request, RequestMetadataState::Accepted)?;
        let client = LocalIpcClient::new(socket.clone());
        let stream = client.resume(request.clone()).await?;
        let mut renderer = LocalRenderer::for_request(Vec::new(), false, request.clone());
        let mut recovery = Vec::new();
        tokio::time::timeout(
            Duration::from_secs(2),
            drive_operation(
                &client,
                &request,
                stream,
                &metadata,
                &mut renderer,
                &mut recovery,
                ack_started.notified(),
            ),
        )
        .await
        .expect("interrupt must remain selectable during ACK")?;
        release_server.notify_one();
        server.await??;
        assert_eq!(
            metadata.load(&request)?.unwrap().state,
            RequestMetadataState::Accepted
        );
        assert_eq!(
            String::from_utf8(recovery)?,
            format!("codrik resume {request}\n")
        );
        std::fs::remove_file(socket)?;
        std::fs::remove_dir_all(metadata_root)?;
        Ok(())
    }

    fn temp_socket() -> PathBuf {
        PathBuf::from("/tmp").join(format!("c11-cli-{}.sock", uuid::Uuid::new_v4()))
    }

    fn temp_root() -> PathBuf {
        std::env::temp_dir().join(format!("codrik-cli-metadata-{}", uuid::Uuid::new_v4()))
    }
}
