use super::*;

#[derive(Debug, Serialize, Deserialize)]
pub(super) enum RaftRequest {
    AppendEntries(AppendEntriesRequest<BrokerRaftConfig>),
    Vote(VoteRequest<u64>),
    FullSnapshot {
        vote: Vote<u64>,
        meta: SnapshotMeta<u64, BasicNode>,
        data: Vec<u8>,
    },
    DataAppend(DataAppendRequest),
    DataProgress(DataProgressRequest),
    DataManifest(DataManifestRequest),
    DataHeartbeat(DataHeartbeatRequest),
    DataSnapshotChunk(DataSnapshotChunk),
}

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct AuthenticatedRaftRequest {
    pub(super) node_id: u64,
    pub(super) auth_token: String,
    pub(super) request: RaftRequest,
}

#[derive(Debug, Serialize, Deserialize)]
pub(super) enum RaftResponse {
    AppendEntries(AppendEntriesResponse<u64>),
    Vote(VoteResponse<u64>),
    FullSnapshot(SnapshotResponse<u64>),
    DataAppend(DataAppendResponse),
    DataProgress(Option<u64>),
    DataManifest(DataManifestResponse),
    DataHeartbeat(DataHeartbeatResponse),
    DataSnapshotChunkAck { offset: u64, checksum: u32 },
    Error(String),
}

pub(super) async fn serve_raft(
    raft: BrokerRaft,
    state_machine: StateMachineStore,
    listener: TcpListener,
    auth_token: String,
    partition_data: SharedReplicaData,
    tls: Option<RaftTlsRuntime>,
    quotas: Arc<crate::quota::QuotaRuntime>,
) -> Result<()> {
    loop {
        let (stream, _) = listener.accept().await.context("accepting Raft RPC")?;
        let Some(permit) = quotas.try_raft() else {
            continue;
        };
        let raft = raft.clone();
        let state_machine = state_machine.clone();
        let auth_token = auth_token.clone();
        let partition_data = partition_data.clone();
        let tls = tls.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let result = if let Some(tls) = tls {
                match tokio::time::timeout(
                    Duration::from_millis(tls.handshake_timeout_ms),
                    tls.acceptor.accept(stream),
                )
                .await
                {
                    Ok(Ok(stream)) => match crate::tls::identify_peer(
                        stream.get_ref().1.peer_certificates(),
                        &tls.peer_identities,
                    ) {
                        Ok(peer_id) => {
                            handle_raft_stream(
                                raft,
                                state_machine,
                                stream,
                                &auth_token,
                                partition_data,
                                Some(peer_id),
                            )
                            .await
                        }
                        Err(err) => Err(err),
                    },
                    Ok(Err(err)) => Err(BrokerError::with_source("accepting Raft TLS", err)),
                    Err(_) => Err(BrokerError::msg("Raft TLS handshake timed out")),
                }
            } else {
                handle_raft_stream(
                    raft,
                    state_machine,
                    stream,
                    &auth_token,
                    partition_data,
                    None,
                )
                .await
            };
            if let Err(err) = result {
                error!(error = ?err, "raft RPC error");
            }
        });
    }
}

pub(super) async fn handle_raft_stream<S>(
    raft: BrokerRaft,
    state_machine: StateMachineStore,
    mut stream: S,
    auth_token: &str,
    partition_data: SharedReplicaData,
    tls_peer_id: Option<u64>,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    loop {
        let request = read_authenticated_request(&mut stream, auth_token, tls_peer_id).await?;
        let response = match request {
            RaftRequest::AppendEntries(rpc) => match raft.append_entries(rpc).await {
                Ok(response) => RaftResponse::AppendEntries(response),
                Err(err) => RaftResponse::Error(err.to_string()),
            },
            RaftRequest::Vote(rpc) => match raft.vote(rpc).await {
                Ok(response) => RaftResponse::Vote(response),
                Err(err) => RaftResponse::Error(err.to_string()),
            },
            RaftRequest::FullSnapshot { vote, meta, data } => {
                let snapshot: Snapshot<BrokerRaftConfig> = Snapshot {
                    meta,
                    snapshot: Box::new(data),
                };
                match raft.install_full_snapshot(vote, snapshot).await {
                    Ok(response) => RaftResponse::FullSnapshot(response),
                    Err(err) => RaftResponse::Error(err.to_string()),
                }
            }
            RaftRequest::DataAppend(mut request) => {
                let metadata = state_machine.durable_state();
                let key = partition_key(
                    request.envelope.stream.as_str(),
                    request.envelope.partition.0,
                );
                let committed = metadata.partition_commits.get(&key);
                let assignment = metadata.partition_assignments.get(&key);
                if raft.current_leader().await != Some(request.leader_id)
                    || assignment.is_none_or(|assignment| {
                        assignment.leader_id != request.leader_id
                            || assignment.leader_epoch != request.leader_epoch
                            || assignment.replica_set_generation != request.replica_set_generation
                    })
                {
                    RaftResponse::Error("fenced partition leader epoch".to_string())
                } else if request.batch_digest
                    != crate::partition_log::committed_envelope_checksum(&request.envelope)
                        .unwrap_or_default()
                {
                    RaftResponse::Error(
                        "partition predecessor or batch digest mismatch".to_string(),
                    )
                } else {
                    request.committed_high_watermark =
                        committed.map(|commit| commit.high_watermark);
                    match partition_data.lock().unwrap().append(&request) {
                        Ok(response) => RaftResponse::DataAppend(response),
                        Err(err) => RaftResponse::Error(err.to_string()),
                    }
                }
            }
            RaftRequest::DataProgress(request) => {
                RaftResponse::DataProgress(partition_data.lock().unwrap().progress(&request))
            }
            RaftRequest::DataManifest(request) => {
                let metadata = state_machine.durable_state();
                RaftResponse::DataManifest(
                    partition_data.lock().unwrap().manifest(&request, &metadata),
                )
            }
            RaftRequest::DataHeartbeat(request) => {
                let metadata = state_machine.durable_state();
                let key = partition_key(&request.stream, request.partition.0);
                let assignment = metadata.partition_assignments.get(&key);
                let commit = metadata.partition_commits.get(&key);
                if assignment.is_none_or(|assignment| {
                    assignment.replica_set_generation != request.replica_set_generation
                        || assignment.leader_id != request.leader_id
                        || assignment.leader_epoch != request.leader_epoch
                }) {
                    RaftResponse::Error("fenced partition heartbeat".to_string())
                } else {
                    RaftResponse::DataHeartbeat(DataHeartbeatResponse {
                        replica_set_generation: request.replica_set_generation,
                        leader_id: request.leader_id,
                        leader_epoch: request.leader_epoch,
                        high_watermark: commit.map(|commit| commit.high_watermark),
                    })
                }
            }
            RaftRequest::DataSnapshotChunk(chunk) => {
                crate::broker_ensure!(
                    chunk.data.len() <= MAX_RAFT_SNAPSHOT_CHUNK,
                    "partition snapshot chunk exceeds maximum size"
                );
                let checksum = crc32fast::hash(&chunk.data);
                if checksum != chunk.checksum {
                    RaftResponse::Error("partition snapshot chunk checksum mismatch".to_string())
                } else {
                    RaftResponse::DataSnapshotChunkAck {
                        offset: chunk.offset,
                        checksum,
                    }
                }
            }
        };
        write_frame(&mut stream, &response).await?;
    }
}

pub(super) async fn read_authenticated_request<R>(
    reader: &mut R,
    auth_token: &str,
    tls_peer_id: Option<u64>,
) -> Result<RaftRequest>
where
    R: AsyncRead + Unpin,
{
    let envelope: AuthenticatedRaftRequest = read_frame(reader).await?;
    if let Some(peer_id) = tls_peer_id {
        crate::broker_ensure!(
            envelope.node_id == peer_id,
            "Raft request node ID does not match peer certificate"
        );
    }
    crate::broker_ensure!(
        crate::security::constant_time_eq(&envelope.auth_token, auth_token),
        "invalid Raft auth token"
    );
    Ok(envelope.request)
}

pub(super) async fn write_frame<W, T>(writer: &mut W, value: &T) -> Result<()>
where
    W: AsyncWrite + Unpin,
    T: Serialize,
{
    // CBOR preserves byte strings as bytes (rather than JSON integer arrays)
    // while retaining deterministic, length-delimited incremental framing.
    let mut body = Vec::new();
    ciborium::into_writer(value, &mut body).context("serializing Raft frame")?;
    body.insert(0, RAFT_PROTOCOL_VERSION);
    crate::broker_ensure!(
        body.len() <= MAX_RAFT_FRAME,
        "Raft frame exceeds maximum size"
    );
    let len: u32 = body.len().try_into().context("Raft frame too large")?;
    let mut frame = Vec::with_capacity(4 + body.len());
    frame.extend_from_slice(&len.to_le_bytes());
    frame.extend_from_slice(&body);
    writer.write_all(&frame).await?;
    Ok(())
}

pub(super) async fn read_frame<R, T>(reader: &mut R) -> Result<T>
where
    R: AsyncRead + Unpin,
    T: for<'de> Deserialize<'de>,
{
    let mut len = [0; 4];
    tokio::time::timeout(
        Duration::from_millis(RAFT_FRAME_READ_TIMEOUT_MS),
        reader.read_exact(&mut len),
    )
    .await
    .map_err(|_| BrokerError::msg("Raft frame read timed out"))??;
    let len = u32::from_le_bytes(len) as usize;
    crate::broker_ensure!(len <= MAX_RAFT_FRAME, "Raft frame exceeds maximum size");
    let mut body = vec![0; len];
    tokio::time::timeout(
        Duration::from_millis(RAFT_FRAME_READ_TIMEOUT_MS),
        reader.read_exact(&mut body),
    )
    .await
    .map_err(|_| BrokerError::msg("Raft frame read timed out"))??;
    let (version, payload) = body
        .split_first()
        .ok_or_else(|| BrokerError::msg("empty Raft frame"))?;
    crate::broker_ensure!(
        *version == RAFT_PROTOCOL_VERSION,
        "unsupported Raft protocol version"
    );
    ciborium::from_reader(payload).context("decoding Raft frame")
}
