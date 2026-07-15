#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use anyhow::Result;
    use tokio::net::UnixListener;

    use super::LocalIpcClient;
    use crate::runtime::{
        RequestId,
        ipc::protocol::{
            ClientRequestBody, FrameReader, FrameWriter, ServerEvent, ServerEventBody,
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
    net::{
        UnixStream,
        unix::{OwnedReadHalf, OwnedWriteHalf},
    },
    time::timeout,
};

use crate::runtime::{
    BundleId, CancelId, DeliveryId, RequestId,
    ipc::protocol::{
        ClientRequest, ClientRequestBody, FrameReader, FrameWriter, ProtocolErrorCode, ServerEvent,
    },
};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

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
        let mut stream = self
            .open(ClientRequestBody::AckFinal {
                request_id,
                bundle_id,
                delivery_ids,
            })
            .await?;
        stream.close_write().await?;
        while stream.next_event().await?.is_some() {}
        Ok(())
    }

    async fn open(&self, body: ClientRequestBody) -> Result<ClientEventStream> {
        let stream = timeout(CONNECT_TIMEOUT, UnixStream::connect(&self.socket))
            .await
            .map_err(|_| self.unavailable("connection deadline exceeded"))?
            .map_err(|error| self.unavailable(&error.to_string()))?;
        let (read, mut write) = stream.into_split();
        FrameWriter::new(&mut write)
            .write_client_request(&ClientRequest::new(body))
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
        match self.reader.read_server_event().await {
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
