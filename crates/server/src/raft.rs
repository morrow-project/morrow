use crate::{
    config::ClusterConfig,
    error::{BrokerError, Result, ResultExt},
    wal::{ConsumerRecord, PublishRecord},
};
use openraft::{
    BasicNode, Entry, EntryPayload, ErrorSubject, ErrorVerb, LogId, LogState, Membership,
    RaftLogReader, RaftNetwork, RaftNetworkFactory, RaftSnapshotBuilder, Snapshot, SnapshotMeta,
    SnapshotPolicy, StorageError, StoredMembership, Vote,
    entry::RaftPayload,
    error::{Fatal, NetworkError, RPCError, ReplicationClosed, StreamingError, Unreachable},
    network::RPCOption,
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse,
    },
    storage::{LogFlushed, RaftLogStorage, RaftStateMachine},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, VecDeque},
    fmt, io,
    ops::RangeBounds,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tracing::error;
const LOG_FILE: &str = "raft-log.journal";
const STATE_FILE: &str = "raft-state.journal";
const SNAPSHOT_FILE: &str = "raft-snapshot.v1.json";
const LEGACY_LOG_FILE: &str = "raft-log.json";
const LEGACY_STATE_FILE: &str = "raft-state.json";
const LEGACY_SNAPSHOT_FILE: &str = "raft-snapshot.json";
const MAX_RAFT_FRAME: usize = 16 * 1024 * 1024;
const MAX_RAFT_SNAPSHOT_CHUNK: usize = 1024 * 1024;
const RAFT_FRAME_READ_TIMEOUT_MS: u64 = 5_000;
/// Versioned binary peer protocol. Unknown versions are rejected rather than
/// silently falling back to a weaker JSON contract.
const RAFT_PROTOCOL_VERSION: u8 = 1;
openraft::declare_raft_types!(
    pub BrokerRaftConfig:
        D = BrokerCommand,
        R = BrokerResponse,
        NodeId = u64,
        Node = BasicNode,
        Entry = Entry<BrokerRaftConfig>,
        SnapshotData = Vec<u8>,
        AsyncRuntime = openraft::TokioRuntime,
);
pub type BrokerRaft = openraft::Raft<BrokerRaftConfig>;
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BrokerCommand {
    MetadataBootstrap {
        streams: Vec<crate::stream::StreamDefinition>,
        assignments: HashMap<String, PartitionAssignmentMetadata>,
        security_references: BTreeSet<String>,
        feature_gates: BTreeSet<String>,
    },
    PartitionLeaderUpdate {
        stream: String,
        partition: u32,
        leader_id: u64,
        leader_epoch: u64,
    },
    PartitionReconfiguration {
        stream: String,
        partition: u32,
        generation: u64,
        phase: PartitionReconfigurationPhase,
        replicas: BTreeSet<u64>,
        active_commit_set: BTreeSet<u64>,
        leader_id: u64,
        leader_epoch: u64,
        committed_offset: Option<u64>,
        committed_checksum: Option<u32>,
    },
    PartitionCommit {
        stream: String,
        partition: u32,
        offset: u64,
        checksum: u32,
        leader_id: u64,
        leader_epoch: u64,
    },
    ConsumerUpsert {
        record: ConsumerRecord,
    },
    CursorConsumerUpsert {
        record: ConsumerRecord,
        cursors: crate::consumer_cursor::ConsumerCursorSet,
    },
    ConsumerDelete {
        consumer_id: String,
    },
    GroupUpsert {
        group: String,
        record: crate::consumer_group::GroupRecord,
    },
    PolicyReplace {
        snapshot: crate::tenancy::PolicySnapshot,
    },
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BrokerResponse {
    MetadataBootstrap,
    PartitionLeaderUpdate {
        leader_id: u64,
        leader_epoch: u64,
    },
    PartitionReconfiguration {
        generation: u64,
        phase: PartitionReconfigurationPhase,
    },
    PartitionCommit {
        high_watermark: u64,
        leader_epoch: u64,
    },
    ConsumerUpsert,
    ConsumerDelete,
    GroupUpsert,
    PolicyReplace {
        generation: u64,
    },
    Noop,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DurableConsumer {
    pub record: ConsumerRecord,
    #[serde(default)]
    pub cursors: crate::consumer_cursor::ConsumerCursorSet,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DurableState {
    #[serde(default)]
    pub stream_definitions: HashMap<String, crate::stream::StreamDefinition>,
    #[serde(default)]
    pub partition_assignments: HashMap<String, PartitionAssignmentMetadata>,
    #[serde(default)]
    pub security_references: BTreeSet<String>,
    #[serde(default)]
    pub feature_gates: BTreeSet<String>,
    pub messages: HashMap<u64, PublishRecord>,
    pub consumers: HashMap<String, DurableConsumer>,
    #[serde(default)]
    pub groups: HashMap<String, crate::consumer_group::GroupRecord>,
    #[serde(default)]
    pub next_partition_offsets: HashMap<String, u64>,
    #[serde(default)]
    pub partition_commits: HashMap<String, PartitionCommitMetadata>,
    #[serde(default)]
    pub policy: Option<crate::tenancy::PolicySnapshot>,
    pub last_applied: Option<LogId<u64>>,
    pub last_membership: StoredMembership<u64, BasicNode>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionCommitMetadata {
    #[serde(default)]
    pub replica_set_generation: u64,
    pub high_watermark: u64,
    pub checksum: u32,
    pub leader_id: u64,
    pub leader_epoch: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionAssignmentMetadata {
    pub replicas: BTreeSet<u64>,
    #[serde(default)]
    pub active_commit_set: BTreeSet<u64>,
    #[serde(default = "default_replica_set_generation")]
    pub replica_set_generation: u64,
    #[serde(default)]
    pub phase: PartitionReconfigurationPhase,
    pub leader_id: u64,
    pub leader_epoch: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum PartitionReconfigurationPhase {
    #[default]
    Stable,
    Adding {
        candidate: u64,
    },
    CatchingUp {
        candidate: u64,
        committed_offset: u64,
        digest: u32,
    },
    Activating {
        candidate: u64,
    },
    Demoting {
        member: u64,
    },
    RolledBack {
        reason: String,
    },
}

fn default_replica_set_generation() -> u64 {
    1
}

impl PartitionAssignmentMetadata {
    fn normalize(&mut self) {
        if self.active_commit_set.is_empty() {
            self.active_commit_set = self.replicas.clone();
        }
        if self.replica_set_generation == 0 {
            self.replica_set_generation = 1;
        }
    }

    pub fn active_members(&self) -> &BTreeSet<u64> {
        &self.active_commit_set
    }
}
impl DurableState {
    pub fn new(nodes: BTreeMap<u64, BasicNode>) -> Self {
        let voters = nodes.keys().copied().collect::<BTreeSet<_>>();
        let membership = Membership::new(vec![voters], nodes);
        Self {
            stream_definitions: HashMap::new(),
            partition_assignments: HashMap::new(),
            security_references: BTreeSet::new(),
            feature_gates: BTreeSet::new(),
            messages: HashMap::new(),
            consumers: HashMap::new(),
            groups: HashMap::new(),
            next_partition_offsets: HashMap::new(),
            partition_commits: HashMap::new(),
            policy: None,
            last_applied: None,
            last_membership: StoredMembership::new(None, membership),
        }
    }

    pub fn apply_command(&mut self, command: BrokerCommand) -> BrokerResponse {
        match command {
            BrokerCommand::MetadataBootstrap {
                streams,
                assignments,
                security_references,
                feature_gates,
            } => {
                if self.stream_definitions.is_empty() {
                    self.stream_definitions = streams
                        .into_iter()
                        .map(|stream| (stream.name.as_str().to_string(), stream))
                        .collect();
                    self.partition_assignments = assignments;
                    self.partition_assignments
                        .values_mut()
                        .for_each(PartitionAssignmentMetadata::normalize);
                    self.security_references = security_references;
                    self.feature_gates = feature_gates;
                    BrokerResponse::MetadataBootstrap
                } else {
                    BrokerResponse::Noop
                }
            }
            BrokerCommand::PolicyReplace { snapshot } => {
                if self
                    .policy
                    .as_ref()
                    .is_some_and(|current| snapshot.generation < current.generation)
                {
                    return BrokerResponse::Noop;
                }
                let generation = snapshot.generation;
                self.policy = Some(snapshot);
                BrokerResponse::PolicyReplace { generation }
            }
            BrokerCommand::PartitionCommit {
                stream,
                partition,
                offset,
                checksum,
                leader_id,
                leader_epoch,
            } => {
                let key = partition_key(&stream, partition);
                let Some(assignment) = self.partition_assignments.get(&key) else {
                    return BrokerResponse::Noop;
                };
                let active_members = if assignment.active_commit_set.is_empty() {
                    &assignment.replicas
                } else {
                    &assignment.active_commit_set
                };
                if assignment.leader_id != leader_id
                    || assignment.leader_epoch != leader_epoch
                    || !active_members.contains(&leader_id)
                    || !matches!(assignment.phase, PartitionReconfigurationPhase::Stable)
                {
                    return BrokerResponse::Noop;
                }
                let next_offset = self.next_partition_offsets.entry(key.clone()).or_default();
                if let Some(committed) = self.partition_commits.get(&key) {
                    if committed.high_watermark == offset
                        && committed.leader_id == leader_id
                        && committed.leader_epoch == leader_epoch
                        && committed.checksum == checksum
                    {
                        return BrokerResponse::PartitionCommit {
                            high_watermark: offset,
                            leader_epoch,
                        };
                    }
                    if leader_epoch < committed.leader_epoch {
                        return BrokerResponse::Noop;
                    }
                }
                if *next_offset != offset {
                    return BrokerResponse::Noop;
                }
                *next_offset = offset.saturating_add(1);
                self.partition_commits.insert(
                    key.clone(),
                    PartitionCommitMetadata {
                        replica_set_generation: assignment.replica_set_generation,
                        high_watermark: offset,
                        checksum,
                        leader_id,
                        leader_epoch,
                    },
                );
                BrokerResponse::PartitionCommit {
                    high_watermark: offset,
                    leader_epoch,
                }
            }
            BrokerCommand::PartitionLeaderUpdate {
                stream,
                partition,
                leader_id,
                leader_epoch,
            } => {
                let key = partition_key(&stream, partition);
                let Some(assignment) = self.partition_assignments.get_mut(&key) else {
                    return BrokerResponse::Noop;
                };
                assignment.normalize();
                if assignment.leader_id == leader_id && assignment.leader_epoch == leader_epoch {
                    return BrokerResponse::PartitionLeaderUpdate {
                        leader_id,
                        leader_epoch,
                    };
                }
                if leader_epoch != assignment.leader_epoch.saturating_add(1)
                    || !assignment.active_members().contains(&leader_id)
                    || !matches!(assignment.phase, PartitionReconfigurationPhase::Stable)
                {
                    return BrokerResponse::Noop;
                }
                assignment.leader_id = leader_id;
                assignment.leader_epoch = leader_epoch;
                BrokerResponse::PartitionLeaderUpdate {
                    leader_id,
                    leader_epoch,
                }
            }
            BrokerCommand::PartitionReconfiguration {
                stream,
                partition,
                generation,
                phase,
                replicas,
                active_commit_set,
                leader_id,
                leader_epoch,
                committed_offset,
                committed_checksum,
            } => {
                let key = partition_key(&stream, partition);
                let Some(assignment) = self.partition_assignments.get_mut(&key) else {
                    return BrokerResponse::Noop;
                };
                assignment.normalize();
                if generation < assignment.replica_set_generation {
                    return BrokerResponse::Noop;
                }
                if generation > assignment.replica_set_generation.saturating_add(1) {
                    return BrokerResponse::Noop;
                }
                if active_commit_set.is_empty()
                    || !active_commit_set.is_subset(&replicas)
                    || !replicas.contains(&leader_id)
                    || !active_commit_set.contains(&leader_id)
                    || (matches!(phase, PartitionReconfigurationPhase::Stable)
                        && active_commit_set.len().saturating_mul(2) <= replicas.len())
                {
                    return BrokerResponse::Noop;
                }
                if matches!(phase, PartitionReconfigurationPhase::Activating { .. }) {
                    let Some(expected_offset) = committed_offset else {
                        return BrokerResponse::Noop;
                    };
                    let Some(expected_checksum) = committed_checksum else {
                        return BrokerResponse::Noop;
                    };
                    let Some(committed) = self.partition_commits.get(&key) else {
                        return BrokerResponse::Noop;
                    };
                    if committed.high_watermark != expected_offset
                        || committed.checksum != expected_checksum
                    {
                        return BrokerResponse::Noop;
                    }
                }
                if generation == assignment.replica_set_generation
                    && assignment.phase == phase
                    && assignment.replicas == replicas
                    && assignment.active_commit_set == active_commit_set
                    && assignment.leader_id == leader_id
                    && assignment.leader_epoch == leader_epoch
                {
                    return BrokerResponse::PartitionReconfiguration { generation, phase };
                }
                if leader_epoch < assignment.leader_epoch
                    || (leader_epoch == assignment.leader_epoch
                        && leader_id != assignment.leader_id)
                {
                    return BrokerResponse::Noop;
                }
                assignment.replicas = replicas;
                assignment.active_commit_set = active_commit_set;
                assignment.leader_id = leader_id;
                assignment.leader_epoch = leader_epoch;
                assignment.replica_set_generation = generation;
                assignment.phase = phase.clone();
                BrokerResponse::PartitionReconfiguration { generation, phase }
            }
            BrokerCommand::ConsumerUpsert { record } => {
                self.consumers
                    .entry(record.consumer_id.clone())
                    .and_modify(|consumer| consumer.record = record.clone())
                    .or_insert_with(|| DurableConsumer {
                        record,
                        cursors: Default::default(),
                    });
                BrokerResponse::ConsumerUpsert
            }
            BrokerCommand::CursorConsumerUpsert { record, cursors } => {
                self.consumers
                    .entry(record.consumer_id.clone())
                    .and_modify(|consumer| consumer.record = record.clone())
                    .or_insert_with(|| DurableConsumer { record, cursors });
                BrokerResponse::ConsumerUpsert
            }
            BrokerCommand::ConsumerDelete { consumer_id } => {
                self.consumers.remove(&consumer_id);
                BrokerResponse::ConsumerDelete
            }
            BrokerCommand::GroupUpsert { group, record } => {
                self.groups.insert(group, record);
                BrokerResponse::GroupUpsert
            }
        }
    }
}

pub(crate) fn partition_key(stream: &str, partition: u32) -> String {
    format!("{stream}:{partition}")
}

mod data_plane;
mod data_plane_client;
mod log_store;
mod network;
mod partition_runtime;
mod proxy;
mod rpc;
mod runtime;
mod state_machine;
mod storage_io;

pub(crate) use self::proxy::proxy_stream_to_leader;
use self::runtime::RaftTlsRuntime;
pub(crate) use self::state_machine::{CommittedDelta, DeltaBatch};
use self::{
    data_plane::*, data_plane_client::*, log_store::*, network::*, rpc::*, state_machine::*,
    storage_io::*,
};
pub use self::{
    proxy::proxy_to_leader,
    runtime::{ClusterNode, RaftRuntime},
};

#[cfg(test)]
mod storage_tests;
#[cfg(test)]
mod tests;
