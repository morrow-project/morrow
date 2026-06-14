use super::*;

#[derive(Clone)]
pub(super) struct NetworkFactory {
    pub(super) nodes: HashMap<u64, ClusterNode>,
}
impl RaftNetworkFactory<BrokerRaftConfig> for NetworkFactory {
    type Network = NetworkClient;

    async fn new_client(&mut self, target: u64, node: &BasicNode) -> Self::Network {
        let addr = self
            .nodes
            .get(&target)
            .map(|node| node.raft_addr.to_string())
            .unwrap_or_else(|| node.addr.clone());
        let _ = target;
        NetworkClient { addr }
    }
}
#[derive(Clone)]
pub(super) struct NetworkClient {
    pub(super) addr: String,
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
    pub(super) async fn client_write(&self, command: BrokerCommand) -> Result<BrokerResponse> {
        match self
            .request(RaftRequest::ClientWrite(command))
            .await
            .map_err(|err| BrokerError::with_source("forwarding Raft client write", err))?
        {
            RaftResponse::ClientWrite(response) => Ok(response),
            RaftResponse::Error(message) => Err(BrokerError::msg(message)),
            _ => Err(BrokerError::msg("unexpected client_write response")),
        }
    }

    async fn request(
        &self,
        request: RaftRequest,
    ) -> std::result::Result<RaftResponse, RPCError<u64, BasicNode, openraft::error::RaftError<u64>>>
    {
        let mut stream = TcpStream::connect(&self.addr).await.map_err(|err| {
            RPCError::Unreachable(Unreachable::new(&io::Error::new(
                err.kind(),
                err.to_string(),
            )))
        })?;
        write_frame(&mut stream, &request)
            .await
            .map_err(|err| RPCError::Network(NetworkError::new(&err)))?;
        read_frame(&mut stream)
            .await
            .map_err(|err| RPCError::Network(NetworkError::new(&err)))
    }
}
