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
}

#[derive(Debug, Serialize, Deserialize)]
pub(super) enum RaftResponse {
    AppendEntries(AppendEntriesResponse<u64>),
    Vote(VoteResponse<u64>),
    FullSnapshot(SnapshotResponse<u64>),
    Error(String),
}

pub(super) async fn serve_raft(raft: BrokerRaft, listen: SocketAddr) -> Result<()> {
    let listener = TcpListener::bind(listen)
        .await
        .with_context(|| format!("binding Raft listener {listen}"))?;
    loop {
        let (stream, _) = listener.accept().await.context("accepting Raft RPC")?;
        let raft = raft.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_raft_stream(raft, stream).await {
                error!(error = ?err, "raft RPC error");
            }
        });
    }
}

pub(super) async fn handle_raft_stream(raft: BrokerRaft, mut stream: TcpStream) -> Result<()> {
    let request: RaftRequest = read_frame(&mut stream).await?;
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
    };
    write_frame(&mut stream, &response).await?;
    Ok(())
}

pub(super) async fn write_frame<W, T>(writer: &mut W, value: &T) -> Result<()>
where
    W: AsyncWrite + Unpin,
    T: Serialize,
{
    let body = serde_json::to_vec(value).context("serializing Raft frame")?;
    crate::broker_ensure!(
        body.len() <= MAX_RAFT_FRAME,
        "Raft frame exceeds maximum size"
    );
    let len: u32 = body.len().try_into().context("Raft frame too large")?;
    writer.write_all(&len.to_le_bytes()).await?;
    writer.write_all(&body).await?;
    Ok(())
}

pub(super) async fn read_frame<R, T>(reader: &mut R) -> Result<T>
where
    R: AsyncRead + Unpin,
    T: for<'de> Deserialize<'de>,
{
    let mut len = [0; 4];
    reader.read_exact(&mut len).await?;
    let len = u32::from_le_bytes(len) as usize;
    crate::broker_ensure!(len <= MAX_RAFT_FRAME, "Raft frame exceeds maximum size");
    let mut body = vec![0; len];
    reader.read_exact(&mut body).await?;
    serde_json::from_slice(&body).context("decoding Raft frame")
}
