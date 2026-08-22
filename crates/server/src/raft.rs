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
    collections::{BTreeMap, BTreeSet, HashMap},
    fmt, io,
    net::SocketAddr,
    ops::RangeBounds,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tracing::error;
const LOG_FILE: &str = "raft-log.json";
const STATE_FILE: &str = "raft-state.json";
const SNAPSHOT_FILE: &str = "raft-snapshot.json";
const MAX_RAFT_FRAME: usize = 16 * 1024 * 1024;
const RAFT_FRAME_READ_TIMEOUT_MS: u64 = 5_000;
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
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BrokerResponse {
    MetadataBootstrap,
    PartitionLeaderUpdate {
        leader_id: u64,
        leader_epoch: u64,
    },
    PartitionCommit {
        high_watermark: u64,
        leader_epoch: u64,
    },
    ConsumerUpsert,
    ConsumerDelete,
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
    pub next_partition_offsets: HashMap<String, u64>,
    #[serde(default)]
    pub partition_commits: HashMap<String, PartitionCommitMetadata>,
    pub last_applied: Option<LogId<u64>>,
    pub last_membership: StoredMembership<u64, BasicNode>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionCommitMetadata {
    pub high_watermark: u64,
    pub checksum: u32,
    pub leader_id: u64,
    pub leader_epoch: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionAssignmentMetadata {
    pub replicas: BTreeSet<u64>,
    pub leader_id: u64,
    pub leader_epoch: u64,
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
            next_partition_offsets: HashMap::new(),
            partition_commits: HashMap::new(),
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
                    self.security_references = security_references;
                    self.feature_gates = feature_gates;
                    BrokerResponse::MetadataBootstrap
                } else {
                    BrokerResponse::Noop
                }
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
                if assignment.leader_id != leader_id || assignment.leader_epoch != leader_epoch {
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
                if assignment.leader_id == leader_id && assignment.leader_epoch == leader_epoch {
                    return BrokerResponse::PartitionLeaderUpdate {
                        leader_id,
                        leader_epoch,
                    };
                }
                if leader_epoch != assignment.leader_epoch.saturating_add(1)
                    || !assignment.replicas.contains(&leader_id)
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
        }
    }
}

pub(crate) fn partition_key(stream: &str, partition: u32) -> String {
    format!("{stream}:{partition}")
}

mod data_plane;
mod log_store;
mod network;
mod proxy;
mod rpc;
mod runtime;
mod state_machine;
mod storage_io;

pub(crate) use self::proxy::proxy_stream_to_leader;
use self::runtime::RaftTlsRuntime;
use self::{data_plane::*, log_store::*, network::*, rpc::*, state_machine::*, storage_io::*};
pub use self::{
    proxy::proxy_to_leader,
    runtime::{ClusterNode, RaftRuntime},
};

#[cfg(test)]
mod tests;
