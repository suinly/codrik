#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use anyhow::Result;
    use tokio::{net::UnixListener, time::advance};

    use super::{LocalIpcClient, write_operation};
    use crate::runtime::{
        BundleId, DeliveryId, RequestId,
        ipc::protocol::{
            ClientRequestBody, FrameReader, FrameWriter, ProtocolErrorCode, ServerEvent,
            ServerEventBody,
        },
        model::WorkItemId,
    };

    #[tokio::test]
    async fn submit_sends_one_operation_and_streams_events() -> Result<()> {
        let socket = temp_socket();
        let listener = UnixListener::bind(&socket)?;
        let request = RequestId::new();
        let expected = request.clone();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await?;
            let received = FrameReader::new(&mut stream).read_client_request().await?;
            assert_eq!(
                received.body,
                ClientRequestBody::Submit {
                    request_id: expected.clone(),
                    text: "hello".into(),
                }
            );
            FrameWriter::new(&mut stream)
                .write_server_event(&ServerEvent::new(ServerEventBody::Accepted {
                    request_id: expected,
                    work_item_id: WorkItemId::from_string(uuid::Uuid::new_v4().to_string()),
                    sequence: 1,
                }))
                .await?;
            anyhow::Ok(())
        });

        let client = LocalIpcClient::new(socket.clone());
        let mut events = client.submit(request.clone(), "hello".into()).await?;
        assert!(matches!(
            events.next_event().await?,
            Some(ServerEvent { body: ServerEventBody::Accepted { request_id, .. }, .. }) if request_id == request
        ));
        server.await??;
        drop(events);
        std::fs::remove_file(socket)?;
        Ok(())
    }

    #[tokio::test]
    async fn unavailable_daemon_error_names_socket_and_serve_command() {
        let socket = temp_socket();
        let error = LocalIpcClient::new(socket.clone())
            .resume(RequestId::new())
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains(&socket.display().to_string()));
        assert!(error.contains("codrik serve"));
    }

    #[tokio::test]
    async fn acknowledgement_requires_matching_positive_response() -> Result<()> {
        let request = RequestId::new();
        let bundle = BundleId::new();
        let socket = temp_socket();
        let listener = UnixListener::bind(&socket)?;
        let wrong = BundleId::new();
        let expected_request = request.clone();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await?;
            FrameReader::new(&mut stream).read_client_request().await?;
            FrameWriter::new(&mut stream)
                .write_server_event(&ServerEvent::new(ServerEventBody::AckAccepted {
                    request_id: expected_request,
                    bundle_id: wrong,
                }))
                .await?;
            anyhow::Ok(())
        });
        let error = LocalIpcClient::new(socket.clone())
            .acknowledge_final(request, bundle, vec![DeliveryId::new()])
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("unexpected"));
        server.await??;
        std::fs::remove_file(socket)?;
        Ok(())
    }

    #[tokio::test]
    async fn acknowledgement_rejects_error_and_eof_responses() -> Result<()> {
        for response in [Some("rejected"), None] {
            let request = RequestId::new();
            let bundle = BundleId::new();
            let socket = temp_socket();
            let listener = UnixListener::bind(&socket)?;
            let expected_request = request.clone();
            let server = tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await?;
                FrameReader::new(&mut stream).read_client_request().await?;
                if let Some(message) = response {
                    FrameWriter::new(&mut stream)
                        .write_server_event(&ServerEvent::new(ServerEventBody::RequestError {
                            request_id: expected_request,
                            code: "ack_failed".into(),
                            message: message.into(),
                        }))
                        .await?;
                }
                anyhow::Ok(())
            });
            assert!(
                LocalIpcClient::new(socket.clone())
                    .acknowledge_final(request, bundle, vec![DeliveryId::new()])
                    .await
                    .is_err()
            );
            server.await??;
            std::fs::remove_file(socket)?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn issue_link_code_requires_matching_terminal_response() -> Result<()> {
        let socket = temp_socket();
        let listener = UnixListener::bind(&socket)?;
        let request = RequestId::new();
        let expected = request.clone();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await?;
            let received = FrameReader::new(&mut stream).read_client_request().await?;
            assert_eq!(
                received.body,
                ClientRequestBody::IssueLinkCode {
                    request_id: expected.clone(),
                }
            );
            FrameWriter::new(&mut stream)
                .write_server_event(&ServerEvent::new(ServerEventBody::LinkCodeIssued {
                    request_id: expected,
                    code: "ABCD-EFGH".into(),
                    expires_at: 1_721_234_567_890,
                }))
                .await?;
            anyhow::Ok(())
        });

        let issued = LocalIpcClient::new(socket.clone())
            .issue_link_code(request)
            .await?;
        assert_eq!(issued.code, "ABCD-EFGH");
        assert_eq!(issued.expires_at.0, 1_721_234_567_890);
        server.await??;
        std::fs::remove_file(socket)?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn client_operation_write_uses_protocol_deadline() {
        let (mut writer, _reader) = tokio::io::duplex(1);
        let request = RequestId::new();
        let task = tokio::spawn(async move {
            write_operation(
                &mut writer,
                ClientRequestBody::Resume {
                    request_id: request,
                },
            )
            .await
        });
        tokio::task::yield_now().await;
        advance(std::time::Duration::from_secs(30)).await;
        assert_eq!(
            task.await.unwrap().unwrap_err().code(),
            ProtocolErrorCode::WriteTimeout
        );
    }

    #[tokio::test(start_paused = true)]
    async fn daemon_events_have_no_header_or_body_response_deadline() -> Result<()> {
        use tokio::io::AsyncWriteExt;

        let socket = temp_socket();
        let listener = UnixListener::bind(&socket)?;
        let request = RequestId::new();
        let expected = request.clone();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await?;
            FrameReader::new(&mut stream).read_client_request().await?;
            tokio::time::sleep(std::time::Duration::from_secs(6)).await;
            FrameWriter::new(&mut stream)
                .write_server_event(&ServerEvent::new(ServerEventBody::TextDelta {
                    request_id: expected.clone(),
                    delta: "after idle".into(),
                }))
                .await?;
            let event = ServerEvent::new(ServerEventBody::TextDelta {
                request_id: expected,
                delta: "slow body".into(),
            });
            let bytes = serde_json::to_vec(&event)?;
            stream
                .write_all(&(bytes.len() as u32).to_be_bytes())
                .await?;
            stream.write_all(&bytes[..1]).await?;
            tokio::time::sleep(std::time::Duration::from_secs(31)).await;
            stream.write_all(&bytes[1..]).await?;
            anyhow::Ok(())
        });

        let client = LocalIpcClient::new(socket.clone());
        let mut events = client.resume(request).await?;
        let first_event = {
            let first = events.next_event();
            tokio::pin!(first);
            tokio::task::yield_now().await;
            advance(std::time::Duration::from_secs(6)).await;
            first.await?.unwrap()
        };
        assert!(
            matches!(first_event.body, ServerEventBody::TextDelta { delta, .. } if delta == "after idle")
        );
        let second_event = {
            let second = events.next_event();
            tokio::pin!(second);
            tokio::task::yield_now().await;
            advance(std::time::Duration::from_secs(31)).await;
            second.await?.unwrap()
        };
        assert!(
            matches!(second_event.body, ServerEventBody::TextDelta { delta, .. } if delta == "slow body")
        );
        server.await??;
        std::fs::remove_file(socket)?;
        Ok(())
    }

    fn temp_socket() -> PathBuf {
        PathBuf::from("/tmp").join(format!("c11-{}.sock", uuid::Uuid::new_v4()))
    }
}
use std::{
    fmt,
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{Result, anyhow};
use tokio::{
    io::AsyncWrite,
    net::{
        UnixStream,
        unix::{OwnedReadHalf, OwnedWriteHalf},
    },
    time::timeout,
};

use crate::runtime::{
    BundleId, CancelId, DeliveryId, RequestId,
    identity_link::IssuedLinkCode,
    ipc::protocol::{
        ClientRequest, ClientRequestBody, FrameReader, FrameWriter, ProtocolErrorCode,
        ProtocolFailure, ServerEvent, ServerEventBody,
    },
    model::Timestamp,
};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

async fn write_operation<W>(write: &mut W, body: ClientRequestBody) -> Result<(), ProtocolFailure>
where
    W: AsyncWrite + Unpin,
{
    FrameWriter::new(write)
        .write_client_request(&ClientRequest::new(body))
        .await
}

#[derive(Clone, Debug)]
pub struct LocalIpcClient {
    socket: PathBuf,
}

impl LocalIpcClient {
    pub fn new(socket: PathBuf) -> Self {
        Self { socket }
    }

    pub fn socket(&self) -> &Path {
        &self.socket
    }

    pub async fn submit(&self, request_id: RequestId, text: String) -> Result<ClientEventStream> {
        self.open(ClientRequestBody::Submit { request_id, text })
            .await
    }

    pub async fn resume(&self, request_id: RequestId) -> Result<ClientEventStream> {
        self.open(ClientRequestBody::Resume { request_id }).await
    }

    pub async fn cancel(&self, request_id: RequestId) -> Result<ClientEventStream> {
        self.open(ClientRequestBody::Cancel {
            request_id,
            cancel_id: CancelId::new(),
        })
        .await
    }

    pub async fn acknowledge_final(
        &self,
        request_id: RequestId,
        bundle_id: BundleId,
        delivery_ids: Vec<DeliveryId>,
    ) -> Result<()> {
        let expected_request = request_id.clone();
        let expected_bundle = bundle_id.clone();
        let mut stream = self
            .open(ClientRequestBody::AckFinal {
                request_id,
                bundle_id,
                delivery_ids,
            })
            .await?;
        stream.close_write().await?;
        match stream.next_event().await? {
            Some(ServerEvent {
                body:
                    crate::runtime::ipc::protocol::ServerEventBody::AckAccepted {
                        request_id,
                        bundle_id,
                    },
                ..
            }) if request_id == expected_request && bundle_id == expected_bundle => Ok(()),
            Some(ServerEvent {
                body:
                    crate::runtime::ipc::protocol::ServerEventBody::RequestError {
                        code, message, ..
                    },
                ..
            }) => Err(anyhow!(
                "daemon rejected final acknowledgement ({code}): {message}"
            )),
            Some(event) => Err(anyhow!(
                "daemon returned an unexpected final acknowledgement response: {:?}",
                event.body
            )),
            None => Err(anyhow!(
                "daemon closed before confirming final acknowledgement for {expected_request}"
            )),
        }
    }

    pub async fn issue_link_code(&self, request_id: RequestId) -> Result<IssuedLinkCode> {
        let expected_request = request_id.clone();
        let mut stream = self
            .open(ClientRequestBody::IssueLinkCode { request_id })
            .await?;
        stream.close_write().await?;
        match stream.next_event().await? {
            Some(ServerEvent {
                body:
                    ServerEventBody::LinkCodeIssued {
                        request_id,
                        code,
                        expires_at,
                    },
                ..
            }) if request_id == expected_request => Ok(IssuedLinkCode {
                code,
                expires_at: Timestamp(expires_at),
            }),
            Some(ServerEvent {
                body: ServerEventBody::RequestError { code, message, .. },
                ..
            }) => Err(anyhow!(
                "daemon rejected identity link code request ({code}): {message}"
            )),
            Some(event) => Err(anyhow!(
                "daemon returned an unexpected identity link code response: {:?}",
                event.body
            )),
            None => Err(anyhow!(
                "daemon closed before issuing an identity link code for {expected_request}"
            )),
        }
    }

    async fn open(&self, body: ClientRequestBody) -> Result<ClientEventStream> {
        let stream = timeout(CONNECT_TIMEOUT, UnixStream::connect(&self.socket))
            .await
            .map_err(|_| self.unavailable("connection deadline exceeded"))?
            .map_err(|error| self.unavailable(&error.to_string()))?;
        let (read, mut write) = stream.into_split();
        write_operation(&mut write, body)
            .await
            .map_err(|error| self.unavailable(&format!("failed to write operation: {error}")))?;
        Ok(ClientEventStream {
            reader: FrameReader::new(read),
            write: Some(write),
        })
    }

    fn unavailable(&self, reason: &str) -> anyhow::Error {
        anyhow!(
            "codrik daemon is unavailable at {} ({reason}); start it with `codrik serve`",
            self.socket.display()
        )
    }
}

pub struct ClientEventStream {
    reader: FrameReader<OwnedReadHalf>,
    // Keeping the write side open tells the server the subscription is still live.
    write: Option<OwnedWriteHalf>,
}

impl fmt::Debug for ClientEventStream {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ClientEventStream")
            .finish_non_exhaustive()
    }
}

impl ClientEventStream {
    pub async fn next_event(&mut self) -> Result<Option<ServerEvent>> {
        match self.reader.read_server_event_without_deadline().await {
            Ok(event) => Ok(Some(event)),
            Err(error) if error.code() == ProtocolErrorCode::IncompleteFrame => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    pub async fn close_write(&mut self) -> Result<()> {
        if let Some(mut write) = self.write.take() {
            use tokio::io::AsyncWriteExt;
            write.shutdown().await?;
        }
        Ok(())
    }
}
