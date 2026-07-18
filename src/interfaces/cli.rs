use std::{env, future::Future, io::Write, path::PathBuf};

use anyhow::{Context, Result, bail};

use crate::{
    config::{AppConfig, codrik_dir},
    interfaces::{
        local_renderer::{LocalRenderer, RenderAction},
        request_metadata::{RequestMetadataState, RequestMetadataStore, recovery_command},
    },
    runtime::{
        RequestId,
        actor_admin::{ActorAdminCommand, ActorAdminResult},
        ipc::client::{ClientEventStream, LocalIpcClient},
        model::{ActorId, Clock, SystemClock},
    },
    updater,
};

pub async fn run() -> Result<()> {
    match CliCommand::parse(env::args().skip(1))? {
        CliCommand::Update => updater::update().await,
        CliCommand::Serve => crate::app::serve(AppConfig::load_default()?).await,
        CliCommand::Link(actor) => link(actor).await,
        CliCommand::Actors(command) => actors(command).await,
        CliCommand::Submit(prompt) => submit(prompt).await,
        CliCommand::Resume(request) => resume(request).await,
        CliCommand::Cancel(request) => cancel(request).await,
        CliCommand::InstallerValidateConfig { config } => {
            let config = AppConfig::load(config)?;
            let actor = ActorId::parse_workspace_safe(&config.required_runtime()?.actor_id)?;
            println!("{actor}");
            Ok(())
        }
    }
}

async fn submit(prompt: String) -> Result<()> {
    let (client, metadata) = local_context()?;
    let request = RequestId::new();
    metadata.create(&request, SystemClock.now().0, &prompt)?;
    let stream = match client.submit(request.clone(), prompt).await {
        Ok(stream) => stream,
        Err(error) => {
            eprintln!("request id: {request}");
            eprintln!("{}", recovery_command(&request));
            return Err(error);
        }
    };
    if let Err(error) = metadata.set_state(&request, RequestMetadataState::SentUnconfirmed) {
        eprintln!("request id: {request}");
        eprintln!("{}", recovery_command(&request));
        return Err(error);
    }
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
                request_id,
                affected_request_ids,
                ..
            } if request_id == request => {
                for affected in affected_request_ids {
                    println!("{affected}");
                }
                return Ok(());
            }
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

async fn link(actor: Option<ActorId>) -> Result<()> {
    let issued = local_client()?
        .issue_link_code_for(RequestId::new(), actor)
        .await?;
    write_link_instructions(&mut std::io::stdout(), &issued.code)
}

async fn actors(command: ActorAdminCommand) -> Result<()> {
    let result = local_client()?
        .actor_admin(RequestId::new(), command.clone())
        .await?;
    render_actor_admin(&mut std::io::stdout(), &command, result)
}

fn render_actor_admin(
    output: &mut impl Write,
    command: &ActorAdminCommand,
    result: ActorAdminResult,
) -> Result<()> {
    match (command, result) {
        (ActorAdminCommand::List, ActorAdminResult::Actors { mut actors }) => {
            actors.sort_by(|left, right| left.id.as_str().cmp(right.id.as_str()));
            for actor in actors {
                let mut tools = actor.tools;
                tools.sort();
                let tools = if tools.is_empty() {
                    "-".into()
                } else {
                    tools.join(", ")
                };
                writeln!(
                    output,
                    "{}\t{}\t{}",
                    actor.id,
                    if actor.enabled { "enabled" } else { "disabled" },
                    tools
                )?;
            }
        }
        (ActorAdminCommand::Show { .. }, ActorAdminResult::Actor { mut details, .. }) => {
            details.actor.tools.sort();
            details.identities.sort_by(|left, right| {
                (&left.provider, &left.username).cmp(&(&right.provider, &right.username))
            });
            writeln!(output, "Actor: {}", details.actor.id)?;
            writeln!(
                output,
                "Status: {}",
                if details.actor.enabled {
                    "enabled"
                } else {
                    "disabled"
                }
            )?;
            writeln!(
                output,
                "Active work: {}",
                if details.has_active_work { "yes" } else { "no" }
            )?;
            writeln!(
                output,
                "Tools: {}",
                if details.actor.tools.is_empty() {
                    "-".into()
                } else {
                    details.actor.tools.join(", ")
                }
            )?;
            writeln!(output, "Identities:")?;
            for identity in details.identities {
                match identity.username {
                    Some(username) => writeln!(output, "  {} @{}", identity.provider, username)?,
                    None => writeln!(output, "  {}", identity.provider)?,
                }
            }
        }
        (ActorAdminCommand::ToolsList { .. }, ActorAdminResult::Tools { mut tools, .. }) => {
            tools.sort();
            for tool in tools {
                writeln!(output, "{tool}")?;
            }
        }
        (ActorAdminCommand::Delete { .. }, ActorAdminResult::Deleted { actor_id }) => {
            writeln!(output, "{actor_id} deleted")?;
        }
        (_, ActorAdminResult::Actor { details, changed }) => {
            writeln!(
                output,
                "{} {}",
                details.actor.id,
                if changed { "updated" } else { "unchanged" }
            )?;
        }
        _ => bail!("daemon returned an unexpected actor administration result"),
    }
    Ok(())
}

fn write_link_instructions(output: &mut impl Write, code: &str) -> Result<()> {
    writeln!(output, "Link code: {code}")?;
    writeln!(output, "Expires in 10 minutes.")?;
    writeln!(output, "In the new channel, send: /link {code}")?;
    Ok(())
}

fn local_client() -> Result<LocalIpcClient> {
    let config = AppConfig::load_default()?;
    let paths = config.required_runtime()?.resolve_paths(&codrik_dir()?)?;
    Ok(LocalIpcClient::new(paths.socket))
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
            event = stream.next_event() => match event {
                Ok(event) => event,
                Err(error) => {
                    writeln!(recovery, "{}", recovery_command(request))?;
                    return Err(error);
                }
            },
        };
        let Some(event) = event else {
            writeln!(recovery, "{}", recovery_command(request))?;
            return Ok(());
        };
        let action = match renderer.handle(event) {
            Ok(action) => action,
            Err(error) => {
                writeln!(recovery, "{}", recovery_command(request))?;
                return Err(error);
            }
        };
        match action {
            RenderAction::Continue => {}
            RenderAction::Accepted => {
                if let Err(error) =
                    metadata.set_state_if_present(request, RequestMetadataState::Accepted)
                {
                    writeln!(recovery, "{}", recovery_command(request))?;
                    return Err(error);
                }
            }
            RenderAction::FinalVerified {
                request_id,
                bundle_id,
                delivery_ids,
            } => {
                if let Err(error) = stream.close_write().await {
                    writeln!(recovery, "{}", recovery_command(request))?;
                    return Err(error);
                }
                drop(stream);
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
                if let Err(error) =
                    metadata.set_state_if_present(request, RequestMetadataState::Terminal)
                {
                    writeln!(recovery, "{}", recovery_command(request))?;
                    return Err(error);
                }
                return Ok(());
            }
            RenderAction::Recover => {
                writeln!(recovery, "{}", recovery_command(request))?;
                return Ok(());
            }
            RenderAction::DaemonError { message, recover } => {
                if recover {
                    writeln!(recovery, "{}", recovery_command(request))?;
                }
                bail!(message)
            }
            RenderAction::CancelAccepted => bail!("unexpected cancellation response"),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
enum CliCommand {
    Update,
    Serve,
    Link(Option<ActorId>),
    Actors(ActorAdminCommand),
    Resume(RequestId),
    Cancel(RequestId),
    Submit(String),
    InstallerValidateConfig { config: PathBuf },
}

impl CliCommand {
    fn parse(args: impl IntoIterator<Item = String>) -> Result<Self> {
        let mut args = args.into_iter();
        let command = args.next().context("missing query or command")?;

        let parsed = match command.as_str() {
            "update" => Self::Update,
            "serve" => Self::Serve,
            "link" => Self::Link(
                args.next()
                    .map(|actor| ActorId::parse_workspace_safe(&actor))
                    .transpose()?,
            ),
            "actors" => Self::Actors(parse_actor_command(&mut args)?),
            "resume" => {
                let request_id = args.next().context("missing request id")?;
                Self::Resume(RequestId::parse(&request_id)?)
            }
            "cancel" => {
                let request_id = args.next().context("missing request id")?;
                Self::Cancel(RequestId::parse(&request_id)?)
            }
            "__installer_validate_config" => {
                let config = PathBuf::from(args.next().context("missing config path")?);
                Self::InstallerValidateConfig { config }
            }
            "gateway" | "--session" | "--stream" => {
                bail!("legacy local command is unsupported; use serve, resume, or cancel")
            }
            _ if command.starts_with("__installer_") => {
                bail!("unknown internal command: {command}")
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

fn parse_actor_command(args: &mut impl Iterator<Item = String>) -> Result<ActorAdminCommand> {
    let command = args.next().context("missing actors command")?;
    let actor =
        |raw: Option<String>| ActorId::parse_workspace_safe(&raw.context("missing actor id")?);
    Ok(match command.as_str() {
        "list" => ActorAdminCommand::List,
        "show" => ActorAdminCommand::Show {
            actor_id: actor(args.next())?,
        },
        "create" => ActorAdminCommand::Create {
            actor_id: actor(args.next())?,
        },
        "enable" => ActorAdminCommand::Enable {
            actor_id: actor(args.next())?,
        },
        "disable" => ActorAdminCommand::Disable {
            actor_id: actor(args.next())?,
        },
        "delete" => {
            let actor_id = actor(args.next())?;
            let force = match args.next().as_deref() {
                None => false,
                Some("--force") => true,
                Some(option) => bail!("unknown actors delete option: {option}"),
            };
            ActorAdminCommand::Delete { actor_id, force }
        }
        "tools" => {
            let action = args.next().context("missing actors tools command")?;
            match action.as_str() {
                "list" => ActorAdminCommand::ToolsList {
                    actor_id: actor(args.next())?,
                },
                "grant" | "revoke" => {
                    let actor_id = actor(args.next())?;
                    let tool = args.next().context("missing tool name")?;
                    if action == "grant" {
                        ActorAdminCommand::ToolsGrant { actor_id, tool }
                    } else {
                        ActorAdminCommand::ToolsRevoke { actor_id, tool }
                    }
                }
                _ => bail!("unknown actors tools command: {action}"),
            }
        }
        _ => bail!("unknown actors command: {command}"),
    })
}

#[cfg(test)]
mod tests {
    use std::{future::pending, path::PathBuf, sync::Arc, time::Duration};

    use anyhow::Result;
    use tokio::{io::AsyncReadExt, net::UnixListener, sync::Notify};

    use super::{CliCommand, drive_operation, render_actor_admin, write_link_instructions};
    use crate::{
        interfaces::{
            local_renderer::LocalRenderer,
            request_metadata::{RequestMetadataState, RequestMetadataStore},
        },
        runtime::{
            BundleId, BundleState, DeliveryId, RequestId,
            actor_admin::{ActorAdminCommand, ActorAdminResult},
            ipc::{
                client::LocalIpcClient,
                protocol::{ClientRequestBody, FrameReader, FrameWriter, encode_bundle},
                server::MAX_CONNECTIONS,
            },
            model::ActorId,
            store::{
                ActorDetails, BundleManifest, FinalPayload, LinkIdentity, ResultBundle,
                RuntimeActor,
            },
        },
    };

    #[test]
    fn parses_supported_commands() -> Result<()> {
        const UUID: &str = "0190f2ef-0000-7000-8000-000000000001";
        let parse = |args: &[&str]| CliCommand::parse(args.iter().copied().map(String::from));

        assert_eq!(parse(&["serve"])?, CliCommand::Serve);
        assert_eq!(parse(&["link"])?, CliCommand::Link(None));
        assert_eq!(
            parse(&["link", "alice"])?,
            CliCommand::Link(Some(ActorId::parse_workspace_safe("alice")?))
        );
        assert_eq!(
            parse(&["actors", "list"])?,
            CliCommand::Actors(ActorAdminCommand::List)
        );
        assert_eq!(
            parse(&["actors", "show", "alice"])?,
            CliCommand::Actors(ActorAdminCommand::Show {
                actor_id: ActorId::parse_workspace_safe("alice")?,
            })
        );
        for (verb, expected) in [
            (
                "create",
                ActorAdminCommand::Create {
                    actor_id: ActorId::parse_workspace_safe("alice")?,
                },
            ),
            (
                "enable",
                ActorAdminCommand::Enable {
                    actor_id: ActorId::parse_workspace_safe("alice")?,
                },
            ),
            (
                "disable",
                ActorAdminCommand::Disable {
                    actor_id: ActorId::parse_workspace_safe("alice")?,
                },
            ),
        ] {
            assert_eq!(
                parse(&["actors", verb, "alice"])?,
                CliCommand::Actors(expected)
            );
        }
        assert_eq!(
            parse(&["actors", "delete", "alice"])?,
            CliCommand::Actors(ActorAdminCommand::Delete {
                actor_id: ActorId::parse_workspace_safe("alice")?,
                force: false,
            })
        );
        assert_eq!(
            parse(&["actors", "tools", "list", "alice"])?,
            CliCommand::Actors(ActorAdminCommand::ToolsList {
                actor_id: ActorId::parse_workspace_safe("alice")?,
            })
        );
        assert_eq!(
            parse(&["actors", "tools", "grant", "alice", "bash"])?,
            CliCommand::Actors(ActorAdminCommand::ToolsGrant {
                actor_id: ActorId::parse_workspace_safe("alice")?,
                tool: "bash".into(),
            })
        );
        assert_eq!(
            parse(&["actors", "delete", "alice", "--force"])?,
            CliCommand::Actors(ActorAdminCommand::Delete {
                actor_id: ActorId::parse_workspace_safe("alice")?,
                force: true,
            })
        );
        assert_eq!(
            parse(&["actors", "tools", "revoke", "alice", "bash"])?,
            CliCommand::Actors(ActorAdminCommand::ToolsRevoke {
                actor_id: ActorId::parse_workspace_safe("alice")?,
                tool: "bash".into(),
            })
        );
        assert_eq!(parse(&["update"])?, CliCommand::Update);
        assert_eq!(
            parse(&["resume", UUID])?,
            CliCommand::Resume(RequestId::parse(UUID)?)
        );
        assert_eq!(
            parse(&["cancel", UUID])?,
            CliCommand::Cancel(RequestId::parse(UUID)?)
        );
        assert_eq!(
            parse(&["__installer_validate_config", "/tmp/config.yml"])?,
            CliCommand::InstallerValidateConfig {
                config: PathBuf::from("/tmp/config.yml"),
            }
        );
        assert_eq!(parse(&["hello"])?, CliCommand::Submit("hello".into()));
        for removed in ["validate", "has_actors", "validate_actor"] {
            assert!(CliCommand::parse([format!("__installer_{removed}")]).is_err());
        }
        assert!(parse(&["gateway", "telegram"]).is_err());
        assert!(parse(&["--session", "x", "hello"]).is_err());
        assert!(parse(&["--stream", "hello"]).is_err());
        assert!(parse(&["update", "extra"]).is_err());
        assert!(parse(&["serve", "extra"]).is_err());
        assert!(parse(&["link", "alice", "extra"]).is_err());
        assert!(parse(&["actors", "delete", "alice", "--unknown"]).is_err());
        assert!(parse(&["actors", "tools", "grant", "alice"]).is_err());
        assert!(parse(&["resume", UUID, "extra"]).is_err());
        assert!(parse(&["cancel", UUID, "extra"]).is_err());
        assert!(parse(&["hello", "extra"]).is_err());

        Ok(())
    }

    #[test]
    fn renders_actor_results_without_identity_subjects() -> Result<()> {
        let alice = ActorId::parse_workspace_safe("alice")?;
        let bob = ActorId::parse_workspace_safe("bob")?;
        let mut output = Vec::new();
        render_actor_admin(
            &mut output,
            &ActorAdminCommand::List,
            ActorAdminResult::Actors {
                actors: vec![
                    RuntimeActor {
                        id: bob,
                        enabled: false,
                        tools: vec!["bash".into(), "*".into()],
                    },
                    RuntimeActor {
                        id: alice.clone(),
                        enabled: true,
                        tools: vec![],
                    },
                ],
            },
        )?;
        assert_eq!(
            String::from_utf8(output)?,
            "alice\tenabled\t-\nbob\tdisabled\t*, bash\n"
        );

        let mut output = Vec::new();
        render_actor_admin(
            &mut output,
            &ActorAdminCommand::Show {
                actor_id: alice.clone(),
            },
            ActorAdminResult::Actor {
                details: ActorDetails {
                    actor: RuntimeActor {
                        id: alice,
                        enabled: true,
                        tools: vec!["datetime".into(), "bash".into()],
                    },
                    identities: vec![LinkIdentity {
                        provider: "telegram".into(),
                        subject: "secret-subject".into(),
                        username: Some("daniel".into()),
                    }],
                    has_active_work: true,
                },
                changed: false,
            },
        )?;
        let rendered = String::from_utf8(output)?;
        assert_eq!(
            rendered,
            "Actor: alice\nStatus: enabled\nActive work: yes\nTools: bash, datetime\nIdentities:\n  telegram @daniel\n"
        );
        assert!(!rendered.contains("secret-subject"));
        Ok(())
    }

    #[test]
    fn renders_actor_mutation_tools_and_deletion() -> Result<()> {
        let alice = ActorId::parse_workspace_safe("alice")?;
        let details = ActorDetails {
            actor: RuntimeActor {
                id: alice.clone(),
                enabled: true,
                tools: vec![],
            },
            identities: vec![],
            has_active_work: false,
        };
        for (changed, expected) in [(true, "alice updated\n"), (false, "alice unchanged\n")] {
            let mut output = Vec::new();
            render_actor_admin(
                &mut output,
                &ActorAdminCommand::Enable {
                    actor_id: alice.clone(),
                },
                ActorAdminResult::Actor {
                    details: details.clone(),
                    changed,
                },
            )?;
            assert_eq!(String::from_utf8(output)?, expected);
        }

        let mut output = Vec::new();
        render_actor_admin(
            &mut output,
            &ActorAdminCommand::ToolsList {
                actor_id: alice.clone(),
            },
            ActorAdminResult::Tools {
                actor_id: alice.clone(),
                tools: vec!["datetime".into(), "*".into(), "bash".into()],
            },
        )?;
        assert_eq!(String::from_utf8(output)?, "*\nbash\ndatetime\n");

        let mut output = Vec::new();
        render_actor_admin(
            &mut output,
            &ActorAdminCommand::Delete {
                actor_id: alice.clone(),
                force: true,
            },
            ActorAdminResult::Deleted { actor_id: alice },
        )?;
        assert_eq!(String::from_utf8(output)?, "alice deleted\n");
        Ok(())
    }

    #[test]
    fn link_instructions_are_concise_and_channel_ready() -> Result<()> {
        let mut output = Vec::new();
        write_link_instructions(&mut output, "ABCD-EFGH")?;
        assert_eq!(
            String::from_utf8(output)?,
            "Link code: ABCD-EFGH\nExpires in 10 minutes.\nIn the new channel, send: /link ABCD-EFGH\n"
        );
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
            let mut eof = [0_u8; 1];
            assert_eq!(
                tokio::time::timeout(std::time::Duration::from_secs(1), operation.read(&mut eof),)
                    .await??,
                0,
                "operation connection must close before the separate ACK connection"
            );

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
    async fn sixty_four_final_streams_release_operation_connections_before_ack() -> Result<()> {
        let socket = temp_socket();
        let listener = UnixListener::bind(&socket)?;
        let coordinates = (0..MAX_CONNECTIONS)
            .map(|_| (RequestId::new(), BundleId::new(), DeliveryId::new()))
            .collect::<Vec<_>>();
        let server_coordinates = coordinates.clone();
        let server = tokio::spawn(async move {
            let mut operations = Vec::with_capacity(MAX_CONNECTIONS);
            for (request, bundle, delivery) in &server_coordinates {
                let (mut operation, _) = listener.accept().await?;
                let request_frame = FrameReader::new(&mut operation)
                    .read_client_request()
                    .await?;
                assert!(matches!(
                    request_frame.body,
                    ClientRequestBody::Resume { request_id } if request_id == *request
                ));
                let events = encode_bundle(
                    &ResultBundle {
                        id: bundle.clone(),
                        request_id: request.clone(),
                        state: BundleState::Delivered,
                        manifest: BundleManifest {
                            entries: vec![],
                            sha256: String::new(),
                        },
                        deliveries: vec![(
                            delivery.clone(),
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
                operations.push(operation);
            }
            for operation in &mut operations {
                let mut eof = [0_u8; 1];
                assert_eq!(
                    tokio::time::timeout(Duration::from_secs(2), operation.read(&mut eof))
                        .await??,
                    0
                );
            }
            drop(operations);
            for (request, bundle, delivery) in server_coordinates {
                let (mut ack, _) = listener.accept().await?;
                let ack_request = FrameReader::new(&mut ack).read_client_request().await?;
                assert!(matches!(
                    ack_request.body,
                    ClientRequestBody::AckFinal { request_id, bundle_id, delivery_ids }
                        if request_id == request
                            && bundle_id == bundle
                            && delivery_ids == vec![delivery]
                ));
                FrameWriter::new(&mut ack)
                    .write_server_event(&crate::runtime::ipc::protocol::ServerEvent::new(
                        crate::runtime::ipc::protocol::ServerEventBody::AckAccepted {
                            request_id: request,
                            bundle_id: bundle,
                        },
                    ))
                    .await?;
            }
            anyhow::Ok(())
        });

        let metadata_root = temp_root();
        let metadata = Arc::new(RequestMetadataStore::new(metadata_root.clone()));
        let client = Arc::new(LocalIpcClient::new(socket.clone()));
        let mut clients = Vec::new();
        for (request, _, _) in &coordinates {
            metadata.create(request, 1, "secret")?;
            metadata.set_state(request, RequestMetadataState::Accepted)?;
            let client = client.clone();
            let metadata = metadata.clone();
            let request = request.clone();
            clients.push(tokio::spawn(async move {
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
                anyhow::Ok(())
            }));
        }
        for client in clients {
            client.await??;
        }
        server.await??;
        assert!(coordinates.iter().all(|(request, _, _)| {
            metadata
                .load(request)
                .ok()
                .flatten()
                .is_some_and(|entry| entry.state == RequestMetadataState::Terminal)
        }));
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
    async fn accepted_metadata_write_failure_prints_exact_resume_command() -> Result<()> {
        use std::os::unix::fs::PermissionsExt;

        let socket = temp_socket();
        let listener = UnixListener::bind(&socket)?;
        let request = RequestId::new();
        let expected = request.clone();
        let server = tokio::spawn(async move {
            let (mut operation, _) = listener.accept().await?;
            FrameReader::new(&mut operation)
                .read_client_request()
                .await?;
            FrameWriter::new(&mut operation)
                .write_server_event(&crate::runtime::ipc::protocol::ServerEvent::new(
                    crate::runtime::ipc::protocol::ServerEventBody::Accepted {
                        request_id: expected,
                        work_item_id: crate::runtime::model::WorkItemId::new(),
                        sequence: 1,
                    },
                ))
                .await?;
            anyhow::Ok(())
        });
        let metadata_root = temp_root();
        let metadata = RequestMetadataStore::new(metadata_root.clone());
        metadata.create(&request, 1, "secret")?;
        std::fs::set_permissions(
            metadata.path(&request),
            std::fs::Permissions::from_mode(0o400),
        )?;
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
        assert_eq!(
            String::from_utf8(recovery)?,
            format!("codrik resume {request}\n")
        );
        server.await??;
        std::fs::set_permissions(
            metadata.path(&request),
            std::fs::Permissions::from_mode(0o600),
        )?;
        std::fs::remove_file(socket)?;
        std::fs::remove_dir_all(metadata_root)?;
        Ok(())
    }

    #[tokio::test]
    async fn definitive_missing_request_does_not_print_resume_recovery() -> Result<()> {
        let socket = temp_socket();
        let listener = UnixListener::bind(&socket)?;
        let request = RequestId::new();
        let expected = request.clone();
        let server = tokio::spawn(async move {
            let (mut operation, _) = listener.accept().await?;
            FrameReader::new(&mut operation)
                .read_client_request()
                .await?;
            FrameWriter::new(&mut operation)
                .write_server_event(&crate::runtime::ipc::protocol::ServerEvent::new(
                    crate::runtime::ipc::protocol::ServerEventBody::RequestError {
                        request_id: expected,
                        code: "missing_request".into(),
                        message: "request does not exist".into(),
                    },
                ))
                .await?;
            anyhow::Ok(())
        });
        let metadata_root = temp_root();
        let metadata = RequestMetadataStore::new(metadata_root.clone());
        metadata.create(&request, 1, "secret")?;
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
        assert!(recovery.is_empty());
        server.await??;
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
