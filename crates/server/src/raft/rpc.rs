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
    DataAppendBatch(Vec<DataAppendRequest>),
    DataCommit(DataCommitRequest),
    DataProgress(DataProgressRequest),
    DataManifest(DataManifestRequest),
    DataHeartbeat(DataHeartbeatRequest),
    DataSnapshotChunk(DataSnapshotChunk),
    BrokerControl(protocol::broker_control::BrokerControlFrame),
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
    DataAppendBatch(Vec<DataAppendResponse>),
    DataCommit(DataCommitResponse),
    DataProgress(Option<u64>),
    DataManifest(DataManifestResponse),
    DataHeartbeat(DataHeartbeatResponse),
    DataSnapshotChunkAck { offset: u64, checksum: u32 },
    BrokerControl(protocol::broker_control::BrokerControlFrame),
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
    broker_control: crate::broker::BrokerControlRegistry,
    accepts_broker_control: bool,
) -> Result<()> {
    let io_gate = Arc::new(tokio::sync::Semaphore::new(64));
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
        let io_gate = io_gate.clone();
        let broker_control = broker_control.clone();
        let accepts_broker_control = accepts_broker_control;
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
                                io_gate,
                                broker_control,
                                accepts_broker_control,
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
                    io_gate,
                    broker_control,
                    accepts_broker_control,
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
    io_gate: Arc<tokio::sync::Semaphore>,
    broker_control: crate::broker::BrokerControlRegistry,
    accepts_broker_control: bool,
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
                if assignment.is_none_or(|assignment| {
                    assignment.leader_id != request.leader_id
                        || assignment.leader_epoch != request.leader_epoch
                        || assignment.replica_set_generation != request.replica_set_generation
                }) {
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
                    match run_replica_io(io_gate.clone(), partition_data.clone(), move |store| {
                        store.append(&request)
                    })
                    .await
                    {
                        Ok(response) => RaftResponse::DataAppend(response),
                        Err(err) => RaftResponse::Error(err.to_string()),
                    }
                }
            }
            RaftRequest::DataAppendBatch(requests) => {
                let metadata = state_machine.durable_state();
                let valid = requests.iter().all(|request| {
                    let key = partition_key(
                        request.envelope.stream.as_str(),
                        request.envelope.partition.0,
                    );
                    metadata
                        .partition_assignments
                        .get(&key)
                        .is_some_and(|assignment| {
                            assignment.leader_id == request.leader_id
                                && assignment.leader_epoch == request.leader_epoch
                                && assignment.replica_set_generation
                                    == request.replica_set_generation
                        })
                        && request.batch_digest
                            == crate::partition_log::committed_envelope_checksum(&request.envelope)
                                .unwrap_or_default()
                });
                if !valid || !valid_data_append_batch(&requests) {
                    RaftResponse::Error("invalid partition append batch".to_string())
                } else {
                    match run_replica_io(io_gate.clone(), partition_data.clone(), move |store| {
                        store.append_batch(&requests)
                    })
                    .await
                    {
                        Ok(response) => RaftResponse::DataAppendBatch(response),
                        Err(err) => RaftResponse::Error(err.to_string()),
                    }
                }
            }
            RaftRequest::DataCommit(request) => {
                let metadata = state_machine.durable_state();
                let key = partition_key(&request.stream, request.partition.0);
                let assignment = metadata.partition_assignments.get(&key);
                if assignment.is_none_or(|assignment| {
                    assignment.leader_id != request.leader_id
                        || assignment.leader_epoch != request.leader_epoch
                        || assignment.replica_set_generation != request.replica_set_generation
                }) {
                    RaftResponse::Error("fenced partition commit".to_string())
                } else {
                    match run_replica_io(io_gate.clone(), partition_data.clone(), move |store| {
                        store.commit(&request)
                    })
                    .await
                    {
                        Ok(response) => RaftResponse::DataCommit(response),
                        Err(err) => RaftResponse::Error(err.to_string()),
                    }
                }
            }
            RaftRequest::DataProgress(request) => {
                match run_replica_io(io_gate.clone(), partition_data.clone(), move |store| {
                    Ok(store.progress(&request))
                })
                .await
                {
                    Ok(progress) => RaftResponse::DataProgress(progress),
                    Err(err) => RaftResponse::Error(err.to_string()),
                }
            }
            RaftRequest::DataManifest(request) => {
                let metadata = state_machine.durable_state();
                match run_replica_io(io_gate.clone(), partition_data.clone(), move |store| {
                    Ok(store.manifest(&request, &metadata))
                })
                .await
                {
                    Ok(manifest) => RaftResponse::DataManifest(manifest),
                    Err(err) => RaftResponse::Error(err.to_string()),
                }
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
                    let local_commit = partition_data.lock().ok().and_then(|store| {
                        store.commit_metadata(&request.stream, request.partition)
                    });
                    RaftResponse::DataHeartbeat(DataHeartbeatResponse {
                        replica_set_generation: request.replica_set_generation,
                        leader_id: request.leader_id,
                        leader_epoch: request.leader_epoch,
                        high_watermark: local_commit
                            .as_ref()
                            .map(|commit| commit.high_watermark)
                            .or_else(|| commit.map(|commit| commit.high_watermark)),
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
            RaftRequest::BrokerControl(frame) => {
                use protocol::broker_control::BrokerControlFrame;
                if !accepts_broker_control {
                    RaftResponse::BrokerControl(BrokerControlFrame::Error(
                        protocol::broker_control::ControlError {
                            code: "control_plane_unavailable".to_string(),
                            message: "broker-only nodes do not host the control plane".to_string(),
                        },
                    ))
                } else {
                    RaftResponse::BrokerControl(match frame {
                        BrokerControlFrame::Register(registration) => {
                            match broker_control.register(registration).await {
                                Ok(result) => BrokerControlFrame::RegisterAccepted(result.accepted),
                                Err(error) => BrokerControlFrame::Error(
                                    protocol::broker_control::ControlError {
                                        code: "registration_rejected".to_string(),
                                        message: error.to_string(),
                                    },
                                ),
                            }
                        }
                        BrokerControlFrame::Heartbeat(heartbeat) => {
                            match broker_control.heartbeat(heartbeat).await {
                                Ok(()) => BrokerControlFrame::HeartbeatAccepted,
                                Err(error) => BrokerControlFrame::Error(
                                    protocol::broker_control::ControlError {
                                        code: "heartbeat_rejected".to_string(),
                                        message: error.to_string(),
                                    },
                                ),
                            }
                        }
                        _ => BrokerControlFrame::Error(protocol::broker_control::ControlError {
                            code: "unsupported_control_request".to_string(),
                            message: "control frame is not a request".to_string(),
                        }),
                    })
                }
            }
        };
        write_frame(&mut stream, &response).await?;
    }
}

async fn run_replica_io<T, F>(
    io_gate: Arc<tokio::sync::Semaphore>,
    partition_data: SharedReplicaData,
    operation: F,
) -> Result<T>
where
    T: Send + 'static,
    F: FnOnce(&mut ReplicaDataStore) -> Result<T> + Send + 'static,
{
    let permit = io_gate
        .try_acquire_owned()
        .map_err(|_| BrokerError::msg("partition I/O queue is full"))?;
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let mut store = partition_data
            .lock()
            .map_err(|_| BrokerError::msg("partition I/O lock poisoned"))?;
        operation(&mut store)
    })
    .await
    .map_err(|err| BrokerError::with_source("partition I/O worker failed", err))?
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
