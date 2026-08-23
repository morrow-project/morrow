use super::*;
use tracing::Instrument;

#[derive(Clone)]
pub(super) struct NetworkFactory {
    pub(super) nodes: HashMap<u64, ClusterNode>,
    pub(super) auth_token: String,
    pub(super) node_id: u64,
    pub(super) tls: Option<RaftTlsRuntime>,
}
impl RaftNetworkFactory<BrokerRaftConfig> for NetworkFactory {
    type Network = NetworkClient;

    async fn new_client(&mut self, target: u64, node: &BasicNode) -> Self::Network {
        let addr = self
            .nodes
            .get(&target)
            .map(|node| node.raft_addr.to_string())
            .unwrap_or_else(|| node.addr.clone());
        NetworkClient {
            addr,
            auth_token: self.auth_token.clone(),
            node_id: self.node_id,
            target,
            tls: self.tls.clone(),
            connection: Arc::new(tokio::sync::Mutex::new(None)),
        }
    }
}
#[derive(Clone)]
pub(super) struct NetworkClient {
    pub(super) addr: String,
    pub(super) auth_token: String,
    pub(super) node_id: u64,
    pub(super) target: u64,
    pub(super) tls: Option<RaftTlsRuntime>,
    pub(super) connection: Arc<tokio::sync::Mutex<Option<RaftConnection>>>,
}

pub(super) enum RaftConnection {
    Plain(TcpStream),
    Tls(tokio_rustls::client::TlsStream<TcpStream>),
}
impl RaftNetwork<BrokerRaftConfig> for NetworkClient {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<BrokerRaftConfig>,
        _option: RPCOption,
    ) -> std::result::Result<
        AppendEntriesResponse<u64>,
        RPCError<u64, BasicNode, openraft::error::RaftError<u64>>,
    > {
        match self.request(RaftRequest::AppendEntries(rpc)).await? {
            RaftResponse::AppendEntries(response) => Ok(response),
            RaftResponse::Error(message) => Err(network_error(message)),
            _ => Err(network_error("unexpected append_entries response")),
        }
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        _option: RPCOption,
    ) -> std::result::Result<
        VoteResponse<u64>,
        RPCError<u64, BasicNode, openraft::error::RaftError<u64>>,
    > {
        match self.request(RaftRequest::Vote(rpc)).await? {
            RaftResponse::Vote(response) => Ok(response),
            RaftResponse::Error(message) => Err(network_error(message)),
            _ => Err(network_error("unexpected vote response")),
        }
    }

    async fn full_snapshot(
        &mut self,
        vote: Vote<u64>,
        snapshot: Snapshot<BrokerRaftConfig>,
        _cancel: impl std::future::Future<Output = ReplicationClosed> + Send + 'static,
        _option: RPCOption,
    ) -> std::result::Result<SnapshotResponse<u64>, StreamingError<BrokerRaftConfig, Fatal<u64>>>
    {
        match self
            .request(RaftRequest::FullSnapshot {
                vote,
                meta: snapshot.meta,
                data: *snapshot.snapshot,
            })
            .await
            .map_err(|err| match err {
                RPCError::Unreachable(err) => StreamingError::Unreachable(err),
                RPCError::Network(err) => StreamingError::Network(err),
                RPCError::Timeout(err) => StreamingError::Timeout(err),
                RPCError::PayloadTooLarge(err) => {
                    StreamingError::Unreachable(Unreachable::new(&err))
                }
                RPCError::RemoteError(err) => StreamingError::Unreachable(Unreachable::new(&err)),
            })? {
            RaftResponse::FullSnapshot(response) => Ok(response),
            RaftResponse::Error(message) => Err(StreamingError::Network(NetworkError::new(
                &SimpleError(message),
            ))),
            _ => Err(StreamingError::Network(NetworkError::new(&SimpleError(
                "unexpected full_snapshot response".to_string(),
            )))),
        }
    }
}

impl NetworkClient {
    pub(super) async fn request(
        &self,
        request: RaftRequest,
    ) -> std::result::Result<RaftResponse, RPCError<u64, BasicNode, openraft::error::RaftError<u64>>>
    {
        async {
            let mut connection = self.connection.lock().await;
            if connection.is_none() {
                *connection = Some(self.connect().await?);
            }
            let result = match connection.as_mut().expect("Raft connection initialized") {
                RaftConnection::Plain(stream) => self.exchange(&mut *stream, request).await,
                RaftConnection::Tls(stream) => self.exchange(&mut *stream, request).await,
            };
            if result.is_err() {
                *connection = None;
            }
            result
        }
        .instrument(tracing::info_span!(
            "morrow.raft.rpc",
            target_node_id = self.target,
        ))
        .await
    }

    async fn connect(
        &self,
    ) -> std::result::Result<
        RaftConnection,
        RPCError<u64, BasicNode, openraft::error::RaftError<u64>>,
    > {
        let stream = TcpStream::connect(&self.addr).await.map_err(|err| {
            RPCError::Unreachable(Unreachable::new(&io::Error::new(
                err.kind(),
                err.to_string(),
            )))
        })?;
        if let Some(tls) = &self.tls {
            let server_name = tls
                .server_names
                .get(&self.target)
                .ok_or_else(|| network_error("missing Raft TLS server name"))?;
            let server_name = rustls::pki_types::ServerName::try_from(server_name.clone())
                .map_err(|err| network_error(err.to_string()))?;
            let stream = tls
                .connector
                .connect(server_name, stream)
                .await
                .map_err(|err| RPCError::Network(NetworkError::new(&err)))?;
            let peer_id = crate::tls::identify_peer(
                stream.get_ref().1.peer_certificates(),
                &tls.peer_identities,
            )
            .map_err(|err| RPCError::Network(NetworkError::new(&err)))?;
            if peer_id != self.target {
                return Err(network_error(
                    "Raft TLS certificate belongs to a different node",
                ));
            }
            Ok(RaftConnection::Tls(stream))
        } else {
            Ok(RaftConnection::Plain(stream))
        }
    }

    async fn exchange<S>(
        &self,
        stream: &mut S,
        request: RaftRequest,
    ) -> std::result::Result<RaftResponse, RPCError<u64, BasicNode, openraft::error::RaftError<u64>>>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        write_frame(
            stream,
            &AuthenticatedRaftRequest {
                node_id: self.node_id,
                auth_token: self.auth_token.clone(),
                request,
            },
        )
        .await
        .map_err(|err| RPCError::Network(NetworkError::new(&err)))?;
        read_frame(stream)
            .await
            .map_err(|err| RPCError::Network(NetworkError::new(&err)))
    }
}
