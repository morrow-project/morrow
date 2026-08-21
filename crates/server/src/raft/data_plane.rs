use super::*;
use crate::{
    partition_log::{MessageEnvelope, PartitionLogSet},
    stream::{PartitionId, StreamCatalog},
};
use std::sync::Mutex as StdMutex;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct DataAppendRequest {
    pub(super) leader_id: u64,
    pub(super) leader_epoch: u64,
    pub(super) fsync: bool,
    pub(super) committed_high_watermark: Option<u64>,
    pub(super) envelope: MessageEnvelope,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub(super) struct DataAppendResponse {
    pub(super) match_offset: u64,
    pub(super) flushed_offset: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct DataProgressRequest {
    pub(super) stream: String,
    pub(super) partition: PartitionId,
}

pub(super) struct ReplicaDataStore {
    logs: PartitionLogSet,
    records: HashMap<(String, PartitionId), BTreeMap<u64, MessageEnvelope>>,
}

impl ReplicaDataStore {
    pub(super) fn open(root: &Path, streams: &StreamCatalog, segment_bytes: u64) -> Result<Self> {
        let (logs, replay) = PartitionLogSet::open(root, streams, segment_bytes)?;
        let mut records: HashMap<_, BTreeMap<_, _>> = HashMap::new();
        for envelope in replay {
            records
                .entry((envelope.stream.as_str().to_string(), envelope.partition))
                .or_default()
                .insert(envelope.offset, envelope);
        }
        Ok(Self { logs, records })
    }

    pub(super) fn append(&mut self, request: &DataAppendRequest) -> Result<DataAppendResponse> {
        crate::broker_ensure!(
            request.envelope.leader_epoch <= request.leader_epoch,
            "data append is from a stale leader epoch"
        );
        let key = (
            request.envelope.stream.as_str().to_string(),
            request.envelope.partition,
        );
        let must_truncate = self
            .records
            .get(&key)
            .and_then(|records| records.get(&request.envelope.offset))
            .is_some_and(|existing| existing != &request.envelope);
        if must_truncate {
            crate::broker_ensure!(
                request
                    .committed_high_watermark
                    .is_none_or(|committed| request.envelope.offset > committed),
                "replica diverges within committed history"
            );
            let records = self.records.entry(key.clone()).or_default();
            records.split_off(&request.envelope.offset);
            let retained = records.values().cloned().collect::<Vec<_>>();
            self.logs.rewrite_partition(&key.0, key.1, &retained)?;
        }
        if let Some(existing) = self
            .records
            .get(&key)
            .and_then(|records| records.get(&request.envelope.offset))
        {
            crate::broker_ensure!(existing == &request.envelope, "divergent replica suffix");
        } else {
            self.logs.append_committed(request.envelope.clone())?;
            self.records
                .entry(key)
                .or_default()
                .insert(request.envelope.offset, request.envelope.clone());
        }
        if request.fsync {
            self.logs.flush()?;
        }
        Ok(DataAppendResponse {
            match_offset: request.envelope.offset,
            flushed_offset: request.fsync.then_some(request.envelope.offset),
        })
    }

    pub(super) fn committed_records(&self, metadata: &DurableState) -> Vec<MessageEnvelope> {
        let mut committed = Vec::new();
        for ((stream, partition), records) in &self.records {
            let Some(high_watermark) = metadata
                .partition_commits
                .get(&partition_key(stream, partition.0))
                .map(|commit| commit.high_watermark)
            else {
                continue;
            };
            committed.extend(
                records
                    .range(..=high_watermark)
                    .map(|(_, envelope)| envelope.clone()),
            );
        }
        committed
    }

    pub(super) fn progress(&self, request: &DataProgressRequest) -> Option<u64> {
        self.records
            .get(&(request.stream.clone(), request.partition))
            .and_then(|records| records.keys().next_back().copied())
    }

    pub(super) fn catch_up_records(
        &self,
        metadata: &DurableState,
        stream: &str,
        partition: PartitionId,
        after: Option<u64>,
    ) -> Vec<MessageEnvelope> {
        let Some(high_watermark) = metadata
            .partition_commits
            .get(&partition_key(stream, partition.0))
            .map(|commit| commit.high_watermark)
        else {
            return Vec::new();
        };
        let start = after.map_or(0, |offset| offset.saturating_add(1));
        self.records
            .get(&(stream.to_string(), partition))
            .into_iter()
            .flat_map(|records| records.range(start..=high_watermark))
            .map(|(_, envelope)| envelope.clone())
            .collect()
    }

    pub(super) fn has_committed_prefix(&self, metadata: &DurableState) -> bool {
        metadata.partition_commits.iter().all(|(key, commit)| {
            let Some((stream, partition)) = key.rsplit_once(':') else {
                return false;
            };
            let Ok(partition) = partition.parse::<u32>() else {
                return false;
            };
            self.records
                .get(&(stream.to_string(), PartitionId(partition)))
                .and_then(|records| records.get(&commit.high_watermark))
                .and_then(|record| crate::partition_log::committed_envelope_checksum(record).ok())
                == Some(commit.checksum)
        })
    }
}

pub(super) type SharedReplicaData = Arc<StdMutex<ReplicaDataStore>>;

pub(super) async fn send_data_append(
    addr: SocketAddr,
    auth_token: String,
    request: DataAppendRequest,
) -> Result<DataAppendResponse> {
    let mut stream = TcpStream::connect(addr)
        .await
        .with_context(|| format!("connecting partition replica {addr}"))?;
    write_frame(
        &mut stream,
        &AuthenticatedRaftRequest {
            auth_token,
            request: RaftRequest::DataAppend(request),
        },
    )
    .await?;
    match read_frame(&mut stream).await? {
        RaftResponse::DataAppend(response) => Ok(response),
        RaftResponse::Error(message) => Err(BrokerError::msg(message)),
        _ => Err(BrokerError::msg("unexpected partition replica response")),
    }
}

pub(super) async fn send_data_progress(
    addr: SocketAddr,
    auth_token: String,
    request: DataProgressRequest,
) -> Result<Option<u64>> {
    let mut stream = TcpStream::connect(addr)
        .await
        .with_context(|| format!("connecting partition replica {addr}"))?;
    write_frame(
        &mut stream,
        &AuthenticatedRaftRequest {
            auth_token,
            request: RaftRequest::DataProgress(request),
        },
    )
    .await?;
    match read_frame(&mut stream).await? {
        RaftResponse::DataProgress(progress) => Ok(progress),
        RaftResponse::Error(message) => Err(BrokerError::msg(message)),
        _ => Err(BrokerError::msg("unexpected partition progress response")),
    }
}
