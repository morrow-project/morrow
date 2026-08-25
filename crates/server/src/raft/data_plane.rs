use super::*;
use crate::{
    partition_log::{MessageEnvelope, PartitionLogSet},
    stream::{PartitionId, StreamCatalog, StreamDefinition},
};
use std::sync::Mutex as StdMutex;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct DataAppendRequest {
    pub(super) leader_id: u64,
    pub(super) leader_epoch: u64,
    pub(super) replica_set_generation: u64,
    pub(super) fsync: bool,
    pub(super) committed_high_watermark: Option<u64>,
    pub(super) predecessor_offset: Option<u64>,
    pub(super) predecessor_checksum: Option<u32>,
    pub(super) batch_digest: u32,
    pub(super) durability: DurabilityBoundary,
    pub(super) envelope: MessageEnvelope,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub(super) enum DurabilityBoundary {
    Memory,
    LocalFlush,
    QuorumFlush,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct DataManifestRequest {
    pub(super) stream: String,
    pub(super) partition: PartitionId,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub(super) struct DataManifestResponse {
    pub(super) replica_set_generation: u64,
    pub(super) leader_id: u64,
    pub(super) leader_epoch: u64,
    pub(super) high_watermark: Option<u64>,
    pub(super) last_offset: Option<u64>,
    pub(super) last_checksum: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct DataHeartbeatRequest {
    pub(super) stream: String,
    pub(super) partition: PartitionId,
    pub(super) replica_set_generation: u64,
    pub(super) leader_id: u64,
    pub(super) leader_epoch: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub(super) struct DataHeartbeatResponse {
    pub(super) replica_set_generation: u64,
    pub(super) leader_id: u64,
    pub(super) leader_epoch: u64,
    pub(super) high_watermark: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct DataSnapshotChunk {
    pub(super) stream: String,
    pub(super) partition: PartitionId,
    pub(super) replica_set_generation: u64,
    pub(super) leader_epoch: u64,
    pub(super) offset: u64,
    pub(super) final_chunk: bool,
    pub(super) checksum: u32,
    pub(super) data: Vec<u8>,
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
        let current_offset = self
            .records
            .get(&key)
            .and_then(|records| records.keys().next_back().copied());
        if let Some(predecessor_offset) = request.predecessor_offset {
            crate::broker_ensure!(
                current_offset == Some(predecessor_offset),
                "partition append has a gap or stale predecessor"
            );
        }
        let existing = self
            .records
            .get(&key)
            .and_then(|records| records.get(&request.envelope.offset));
        let must_truncate = existing
            .map(|existing| {
                self.logs
                    .load_envelope(&key.0, key.1, existing.offset)
                    .and_then(|stored| {
                        stored.ok_or_else(|| BrokerError::msg("replica record is unavailable"))
                    })
                    .map(|stored| stored != request.envelope)
            })
            .transpose()?
            .unwrap_or(false);
        if must_truncate {
            crate::broker_ensure!(
                request
                    .committed_high_watermark
                    .is_none_or(|committed| request.envelope.offset > committed),
                "replica diverges within committed history"
            );
            let records = self.records.entry(key.clone()).or_default();
            records.split_off(&request.envelope.offset);
            let retained = records
                .values()
                .map(|record| {
                    self.logs
                        .load_envelope(&key.0, key.1, record.offset)?
                        .ok_or_else(|| BrokerError::msg("replica record is unavailable"))
                })
                .collect::<Result<Vec<_>>>()?;
            self.logs.rewrite_partition(&key.0, key.1, &retained)?;
        }
        if let Some(existing) = self
            .records
            .get(&key)
            .and_then(|records| records.get(&request.envelope.offset))
        {
            let stored = self
                .logs
                .load_envelope(&key.0, key.1, existing.offset)?
                .ok_or_else(|| BrokerError::msg("replica record is unavailable"))?;
            crate::broker_ensure!(stored == request.envelope, "divergent replica suffix");
        } else {
            self.logs.append_committed(request.envelope.clone())?;
            self.records.entry(key).or_default().insert(
                request.envelope.offset,
                request.envelope.clone().into_resident_metadata(),
            );
        }
        if request.fsync {
            self.logs.flush()?;
        }
        Ok(DataAppendResponse {
            match_offset: request.envelope.offset,
            flushed_offset: request.fsync.then_some(request.envelope.offset),
        })
    }

    pub(super) fn committed_records(
        &self,
        metadata: &DurableState,
    ) -> Result<Vec<MessageEnvelope>> {
        let mut committed = Vec::new();
        for ((stream, partition), records) in &self.records {
            let Some(high_watermark) = metadata
                .partition_commits
                .get(&partition_key(stream, partition.0))
                .map(|commit| commit.high_watermark)
            else {
                continue;
            };
            for (_, envelope) in records.range(..=high_watermark) {
                committed.push(
                    self.logs
                        .load_envelope(stream, *partition, envelope.offset)?
                        .ok_or_else(|| {
                            BrokerError::msg("committed replica record is unavailable")
                        })?,
                );
            }
        }
        Ok(committed)
    }

    pub(super) fn record(
        &self,
        stream: &str,
        partition: PartitionId,
        offset: u64,
    ) -> Result<Option<MessageEnvelope>> {
        let Some(metadata) = self
            .records
            .get(&(stream.to_string(), partition))
            .and_then(|records| records.get(&offset))
        else {
            return Ok(None);
        };
        self.logs.load_envelope(stream, partition, metadata.offset)
    }

    pub(super) fn progress(&self, request: &DataProgressRequest) -> Option<u64> {
        self.records
            .get(&(request.stream.clone(), request.partition))
            .and_then(|records| records.keys().next_back().copied())
    }

    pub(super) fn manifest(
        &self,
        request: &DataManifestRequest,
        metadata: &DurableState,
    ) -> DataManifestResponse {
        let key = partition_key(&request.stream, request.partition.0);
        let assignment = metadata.partition_assignments.get(&key);
        let commit = metadata.partition_commits.get(&key);
        let last_offset = self.progress(&DataProgressRequest {
            stream: request.stream.clone(),
            partition: request.partition,
        });
        let last_checksum = last_offset.and_then(|offset| {
            self.record(&request.stream, request.partition, offset)
                .ok()
                .flatten()
                .and_then(|record| crate::partition_log::committed_envelope_checksum(&record).ok())
        });
        DataManifestResponse {
            replica_set_generation: assignment
                .map(|assignment| assignment.replica_set_generation)
                .unwrap_or_default(),
            leader_id: assignment
                .map(|assignment| assignment.leader_id)
                .unwrap_or_default(),
            leader_epoch: assignment
                .map(|assignment| assignment.leader_epoch)
                .unwrap_or_default(),
            high_watermark: commit.map(|commit| commit.high_watermark),
            last_offset,
            last_checksum,
        }
    }

    pub(super) fn catch_up_records(
        &self,
        metadata: &DurableState,
        stream: &str,
        partition: PartitionId,
        after: Option<u64>,
    ) -> Result<Vec<MessageEnvelope>> {
        let Some(high_watermark) = metadata
            .partition_commits
            .get(&partition_key(stream, partition.0))
            .map(|commit| commit.high_watermark)
        else {
            return Ok(Vec::new());
        };
        let start = after.map_or(0, |offset| offset.saturating_add(1));
        self.records
            .get(&(stream.to_string(), partition))
            .into_iter()
            .flat_map(|records| records.range(start..=high_watermark))
            .map(|(_, envelope)| {
                self.logs
                    .load_envelope(stream, partition, envelope.offset)?
                    .ok_or_else(|| BrokerError::msg("replica catch-up record is unavailable"))
            })
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
                .and_then(|record| {
                    self.logs
                        .load_envelope(stream, PartitionId(partition), record.offset)
                        .ok()
                        .flatten()
                })
                .and_then(|record| crate::partition_log::committed_envelope_checksum(&record).ok())
                == Some(commit.checksum)
        })
    }

    pub(super) fn enforce_retention(
        &mut self,
        streams: &[StreamDefinition],
        now_ms: u64,
    ) -> Result<()> {
        let changes = self.logs.retention_changes(streams, now_ms);
        for change in changes {
            let records = self
                .records
                .entry((change.stream.clone(), change.partition))
                .or_default();
            records.retain(|offset, _| *offset >= change.earliest_offset);
            let retained = records
                .values()
                .map(|record| {
                    self.logs
                        .load_envelope(&change.stream, change.partition, record.offset)?
                        .ok_or_else(|| BrokerError::msg("retained replica record is unavailable"))
                })
                .collect::<Result<Vec<_>>>()?;
            self.logs.retain_partition(&change, &retained)?;
        }
        Ok(())
    }
}

pub(super) type SharedReplicaData = Arc<StdMutex<ReplicaDataStore>>;

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
