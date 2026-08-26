use super::*;
use crate::{
    partition_log::{MessageEnvelope, PartitionLogSet},
    stream::{PartitionId, StreamCatalog, StreamDefinition},
};
use std::sync::Mutex as StdMutex;

const PARTITION_COMMIT_JOURNAL: &str = "commit-state.journal";
const COMMIT_CHECKPOINT_RECORDS: u64 = 1_024;
const COMMIT_CHECKPOINT_BYTES: u64 = 8 * 1024 * 1024;
pub(super) const MAX_DATA_APPEND_BATCH_RECORDS: usize = 256;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct PartitionCommitRecord {
    pub(super) stream: String,
    pub(super) partition: PartitionId,
    pub(super) replica_set_generation: u64,
    pub(super) leader_id: u64,
    pub(super) leader_epoch: u64,
    pub(super) high_watermark: u64,
    pub(super) checksum: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct DataCommitRequest {
    pub(super) leader_id: u64,
    pub(super) leader_epoch: u64,
    pub(super) replica_set_generation: u64,
    pub(super) stream: String,
    pub(super) partition: PartitionId,
    pub(super) high_watermark: u64,
    pub(super) checksum: u32,
    pub(super) fsync: bool,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub(super) struct DataCommitResponse {
    pub(super) high_watermark: u64,
    pub(super) flushed: bool,
}

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
    commits: HashMap<String, PartitionCommitMetadata>,
    commit_journal: PathBuf,
    commit_records: u64,
    commit_bytes: u64,
    unsafe_commits: bool,
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
        let commit_journal = root.join(PARTITION_COMMIT_JOURNAL);
        let commit_bytes = commit_journal
            .metadata()
            .map(|meta| meta.len())
            .unwrap_or_default();
        let mut commits: HashMap<String, PartitionCommitMetadata> = HashMap::new();
        let commit_history = crate::raft::read_journal::<PartitionCommitRecord>(&commit_journal)?;
        let mut journal_conflict = false;
        for record in commit_history.iter().cloned() {
            let key = partition_key(record.stream.as_str(), record.partition.0);
            let candidate = PartitionCommitMetadata {
                replica_set_generation: record.replica_set_generation,
                high_watermark: record.high_watermark,
                checksum: record.checksum,
                leader_id: record.leader_id,
                leader_epoch: record.leader_epoch,
            };
            if let Some(current) = commits.get(&key) {
                if candidate.leader_epoch == current.leader_epoch
                    && candidate.high_watermark == current.high_watermark
                    && (candidate.checksum != current.checksum
                        || candidate.replica_set_generation != current.replica_set_generation)
                {
                    journal_conflict = true;
                }
            }
            if commits.get(&key).is_none_or(|current| {
                candidate.leader_epoch > current.leader_epoch
                    || (candidate.leader_epoch == current.leader_epoch
                        && candidate.high_watermark > current.high_watermark)
            }) {
                commits.insert(key, candidate);
            }
        }
        let mut store = Self {
            logs,
            records,
            commits,
            commit_journal,
            commit_records: commit_history.len() as u64,
            commit_bytes,
            unsafe_commits: journal_conflict,
        };
        store.unsafe_commits = store.commits.iter().any(|(key, commit)| {
            let Some((stream, partition)) = key.rsplit_once(':') else {
                return true;
            };
            let Ok(partition) = partition.parse::<u32>() else {
                return true;
            };
            let Some(record) = store
                .records
                .get(&(stream.to_string(), PartitionId(partition)))
                .and_then(|records| records.get(&commit.high_watermark))
            else {
                return false;
            };
            store
                .logs
                .load_envelope(stream, PartitionId(partition), record.offset)
                .ok()
                .flatten()
                .and_then(|envelope| {
                    crate::partition_log::committed_envelope_checksum(&envelope).ok()
                })
                .is_some_and(|checksum| checksum != commit.checksum)
        });
        Ok(store)
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
            if let Some(expected_checksum) = request.predecessor_checksum {
                let predecessor = self
                    .logs
                    .load_envelope(&key.0, key.1, predecessor_offset)?
                    .ok_or_else(|| BrokerError::msg("partition predecessor is unavailable"))?;
                crate::broker_ensure!(
                    crate::partition_log::committed_envelope_checksum(&predecessor)?
                        == expected_checksum,
                    "partition predecessor checksum mismatch"
                );
            }
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

    pub(super) fn append_batch(
        &mut self,
        requests: &[DataAppendRequest],
    ) -> Result<Vec<DataAppendResponse>> {
        crate::broker_ensure!(
            !requests.is_empty() && requests.len() <= MAX_DATA_APPEND_BATCH_RECORDS,
            "partition append batch size is outside the supported bound"
        );
        let mut responses = Vec::with_capacity(requests.len());
        for request in requests {
            responses.push(self.append(request)?);
        }
        Ok(responses)
    }

    pub(super) fn commit(&mut self, request: &DataCommitRequest) -> Result<DataCommitResponse> {
        let key = (request.stream.clone(), request.partition);
        let record = self
            .records
            .get(&key)
            .and_then(|records| records.get(&request.high_watermark))
            .ok_or_else(|| BrokerError::msg("partition commit record is unavailable"))?;
        let envelope = self
            .logs
            .load_envelope(&key.0, key.1, record.offset)?
            .ok_or_else(|| BrokerError::msg("partition commit envelope is unavailable"))?;
        crate::broker_ensure!(
            crate::partition_log::committed_envelope_checksum(&envelope)? == request.checksum,
            "partition commit checksum mismatch"
        );
        let metadata = PartitionCommitMetadata {
            replica_set_generation: request.replica_set_generation,
            high_watermark: request.high_watermark,
            checksum: request.checksum,
            leader_id: request.leader_id,
            leader_epoch: request.leader_epoch,
        };
        let key_string = partition_key(key.0.as_str(), key.1.0);
        if let Some(current) = self.commits.get(&key_string) {
            crate::broker_ensure!(
                request.leader_epoch >= current.leader_epoch,
                "stale partition commit epoch"
            );
            crate::broker_ensure!(
                request.replica_set_generation >= current.replica_set_generation,
                "stale partition commit generation"
            );
            if request.leader_epoch == current.leader_epoch {
                crate::broker_ensure!(
                    request.high_watermark >= current.high_watermark,
                    "partition commit watermark regressed"
                );
                if request.high_watermark == current.high_watermark {
                    crate::broker_ensure!(
                        request.checksum == current.checksum,
                        "partition commit bytes conflict"
                    );
                    return Ok(DataCommitResponse {
                        high_watermark: request.high_watermark,
                        flushed: true,
                    });
                }
            }
            crate::broker_ensure!(
                request.high_watermark == current.high_watermark.saturating_add(1),
                "partition commit has a gap"
            );
        }
        let appended_bytes = crate::raft::append_journal(
            &self.commit_journal,
            &PartitionCommitRecord {
                stream: key.0,
                partition: key.1,
                replica_set_generation: request.replica_set_generation,
                leader_id: request.leader_id,
                leader_epoch: request.leader_epoch,
                high_watermark: request.high_watermark,
                checksum: request.checksum,
            },
        )?;
        self.commits.insert(key_string, metadata);
        self.commit_records = self.commit_records.saturating_add(1);
        self.commit_bytes = self.commit_bytes.saturating_add(appended_bytes);
        if self.commit_records >= COMMIT_CHECKPOINT_RECORDS
            || self.commit_bytes >= COMMIT_CHECKPOINT_BYTES
        {
            let checkpoint = self
                .commits
                .iter()
                .filter_map(|(key, commit)| {
                    let (stream, partition) = key.rsplit_once(':')?;
                    Some(PartitionCommitRecord {
                        stream: stream.to_string(),
                        partition: PartitionId(partition.parse().ok()?),
                        replica_set_generation: commit.replica_set_generation,
                        leader_id: commit.leader_id,
                        leader_epoch: commit.leader_epoch,
                        high_watermark: commit.high_watermark,
                        checksum: commit.checksum,
                    })
                })
                .collect::<Vec<_>>();
            crate::raft::rewrite_journal(&self.commit_journal, &checkpoint)?;
            self.commit_records = checkpoint.len() as u64;
            self.commit_bytes = self
                .commit_journal
                .metadata()
                .map(|meta| meta.len())
                .unwrap_or_default();
        }
        Ok(DataCommitResponse {
            high_watermark: request.high_watermark,
            flushed: true,
        })
    }

    pub(super) fn local_commits(
        &self,
    ) -> impl Iterator<Item = (&String, &PartitionCommitMetadata)> {
        self.commits.iter()
    }

    pub(super) fn commit_metadata(
        &self,
        stream: &str,
        partition: PartitionId,
    ) -> Option<PartitionCommitMetadata> {
        self.commits
            .get(&partition_key(stream, partition.0))
            .cloned()
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
        let commit = self
            .commits
            .get(&key)
            .or_else(|| metadata.partition_commits.get(&key));
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
        if start > high_watermark {
            return Ok(Vec::new());
        }
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
        if self.unsafe_commits {
            return false;
        }
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
