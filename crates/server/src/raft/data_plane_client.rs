use super::*;

pub(super) async fn send_data_append(
    addr: &str,
    auth_token: String,
    node_id: u64,
    target: u64,
    tls: Option<RaftTlsRuntime>,
    request: DataAppendRequest,
) -> Result<DataAppendResponse> {
    let client = NetworkClient {
        addr: addr.to_string(),
        auth_token,
        node_id,
        target,
        tls,
        connection: Arc::new(tokio::sync::Mutex::new(None)),
    };
    send_data_append_on_client(&client, request).await
}

pub(super) async fn send_data_append_on_client(
    client: &NetworkClient,
    request: DataAppendRequest,
) -> Result<DataAppendResponse> {
    match client
        .request(RaftRequest::DataAppend(request))
        .await
        .map_err(|err| BrokerError::msg(err.to_string()))?
    {
        RaftResponse::DataAppend(response) => Ok(response),
        RaftResponse::Error(message) => Err(BrokerError::msg(message)),
        _ => Err(BrokerError::msg("unexpected partition replica response")),
    }
}

pub(super) async fn send_data_append_batch_on_client(
    client: &NetworkClient,
    requests: Vec<DataAppendRequest>,
) -> Result<Vec<DataAppendResponse>> {
    match client
        .request(RaftRequest::DataAppendBatch(requests))
        .await
        .map_err(|err| BrokerError::msg(err.to_string()))?
    {
        RaftResponse::DataAppendBatch(responses) => Ok(responses),
        RaftResponse::Error(message) => Err(BrokerError::msg(message)),
        _ => Err(BrokerError::msg(
            "unexpected partition append batch response",
        )),
    }
}

pub(super) async fn send_data_commit_on_client(
    client: &NetworkClient,
    request: DataCommitRequest,
) -> Result<DataCommitResponse> {
    match client
        .request(RaftRequest::DataCommit(request))
        .await
        .map_err(|err| BrokerError::msg(err.to_string()))?
    {
        RaftResponse::DataCommit(response) => Ok(response),
        RaftResponse::Error(message) => Err(BrokerError::msg(message)),
        _ => Err(BrokerError::msg("unexpected partition commit response")),
    }
}

pub(super) async fn commit_on_replicas(
    clients: Vec<(NetworkClient, DataCommitRequest)>,
) -> Result<()> {
    let mut joins = tokio::task::JoinSet::new();
    for (client, request) in clients {
        joins.spawn(async move { send_data_commit_on_client(&client, request).await });
    }
    while let Some(response) = joins.join_next().await {
        response
            .map_err(|err| BrokerError::with_source("partition commit worker failed", err))??;
    }
    Ok(())
}

pub(super) async fn send_data_progress(
    addr: &str,
    auth_token: String,
    node_id: u64,
    target: u64,
    tls: Option<RaftTlsRuntime>,
    request: DataProgressRequest,
) -> Result<Option<u64>> {
    let client = NetworkClient {
        addr: addr.to_string(),
        auth_token,
        node_id,
        target,
        tls,
        connection: Arc::new(tokio::sync::Mutex::new(None)),
    };
    send_data_progress_on_client(&client, request).await
}

pub(super) async fn send_data_progress_on_client(
    client: &NetworkClient,
    request: DataProgressRequest,
) -> Result<Option<u64>> {
    match client
        .request(RaftRequest::DataProgress(request))
        .await
        .map_err(|err| BrokerError::msg(err.to_string()))?
    {
        RaftResponse::DataProgress(progress) => Ok(progress),
        RaftResponse::Error(message) => Err(BrokerError::msg(message)),
        _ => Err(BrokerError::msg("unexpected partition progress response")),
    }
}

pub(super) async fn send_data_manifest(
    addr: &str,
    auth_token: String,
    node_id: u64,
    target: u64,
    tls: Option<RaftTlsRuntime>,
    request: DataManifestRequest,
) -> Result<DataManifestResponse> {
    let client = NetworkClient {
        addr: addr.to_string(),
        auth_token,
        node_id,
        target,
        tls,
        connection: Arc::new(tokio::sync::Mutex::new(None)),
    };
    match client
        .request(RaftRequest::DataManifest(request))
        .await
        .map_err(|err| BrokerError::msg(err.to_string()))?
    {
        RaftResponse::DataManifest(response) => Ok(response),
        RaftResponse::Error(message) => Err(BrokerError::msg(message)),
        _ => Err(BrokerError::msg("unexpected partition manifest response")),
    }
}
