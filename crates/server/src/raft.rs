use crate::{
    config::ClusterConfig,
    error::{BrokerError, Result, ResultExt},
    wal::{ConsumerRecord, DeliveryAttemptRecord, PublishRecord},
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
use protocol::subject;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    fmt, io,
    net::SocketAddr,
    ops::RangeBounds,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tracing::error;
const LOG_FILE: &str = "raft-log.json";
const STATE_FILE: &str = "raft-state.json";
const SNAPSHOT_FILE: &str = "raft-snapshot.json";
const MAX_RAFT_FRAME: usize = 64 * 1024 * 1024;
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
    Publish {
        subject: String,
        reply_to: Option<String>,
        payload: Vec<u8>,
    },
    ConsumerUpsert {
        record: ConsumerRecord,
    },
    DeliveryAttempt {
        seq: u64,
        consumer_id: String,
        deadline_ms: u64,
        attempt: u32,
    },
    Ack {
        seq: u64,
        consumer_id: String,
        delivery_id: u64,
    },
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BrokerResponse {
    Publish {
        seq: Option<u64>,
        retained: bool,
    },
    ConsumerUpsert,
    DeliveryAttempt {
        record: Option<DeliveryAttemptRecord>,
    },
    Ack {
        accepted: bool,
    },
    Noop,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DurableConsumer {
    pub record: ConsumerRecord,
    pub pending: BTreeSet<u64>,
    pub pending_attempts: HashMap<u64, u32>,
    pub in_flight: HashMap<u64, DeliveryAttemptRecord>,
    pub acked: HashSet<u64>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DurableState {
    pub messages: HashMap<u64, PublishRecord>,
    pub consumers: HashMap<String, DurableConsumer>,
    pub next_seq: u64,
    pub next_delivery_id: u64,
    pub last_applied: Option<LogId<u64>>,
    pub last_membership: StoredMembership<u64, BasicNode>,
}
impl DurableState {
    pub fn new(nodes: BTreeMap<u64, BasicNode>) -> Self {
        let voters = nodes.keys().copied().collect::<BTreeSet<_>>();
        let membership = Membership::new(vec![voters], nodes);
        Self {
            messages: HashMap::new(),
            consumers: HashMap::new(),
            next_seq: 1,
            next_delivery_id: 1,
            last_applied: None,
            last_membership: StoredMembership::new(None, membership),
        }
    }

    pub fn apply_command(&mut self, command: BrokerCommand) -> BrokerResponse {
        match command {
            BrokerCommand::Publish {
                subject,
                reply_to,
                payload,
            } => {
                let matching_consumers = self
                    .consumers
                    .iter()
                    .filter(|(_, consumer)| {
                        subject::matches(&consumer.record.filter_subject, &subject)
                    })
                    .map(|(consumer_id, _)| consumer_id.clone())
                    .collect::<Vec<_>>();
                if matching_consumers.is_empty() {
                    return BrokerResponse::Publish {
                        seq: None,
                        retained: false,
                    };
                }

                let seq = self.next_seq;
                self.next_seq += 1;
                let record = PublishRecord {
                    seq,
                    subject,
                    reply_to,
                    payload,
                };
                self.messages.insert(seq, record);
                for consumer_id in matching_consumers {
                    if let Some(consumer) = self.consumers.get_mut(&consumer_id) {
                        consumer.pending.insert(seq);
                    }
                }
                BrokerResponse::Publish {
                    seq: Some(seq),
                    retained: true,
                }
            }
            BrokerCommand::ConsumerUpsert { record } => {
                self.consumers
                    .entry(record.consumer_id.clone())
                    .and_modify(|consumer| consumer.record = record.clone())
                    .or_insert_with(|| DurableConsumer {
                        record,
                        pending: BTreeSet::new(),
                        pending_attempts: HashMap::new(),
                        in_flight: HashMap::new(),
                        acked: HashSet::new(),
                    });
                BrokerResponse::ConsumerUpsert
            }
            BrokerCommand::DeliveryAttempt {
                seq,
                consumer_id,
                deadline_ms,
                attempt,
            } => {
                let Some(consumer) = self.consumers.get_mut(&consumer_id) else {
                    return BrokerResponse::DeliveryAttempt { record: None };
                };
                if !self.messages.contains_key(&seq) || consumer.acked.contains(&seq) {
                    consumer.pending.remove(&seq);
                    consumer.pending_attempts.remove(&seq);
                    consumer.in_flight.remove(&seq);
                    return BrokerResponse::DeliveryAttempt { record: None };
                }
                if !consumer.pending.contains(&seq) && !consumer.in_flight.contains_key(&seq) {
                    return BrokerResponse::DeliveryAttempt { record: None };
                }

                let delivery_id = self.next_delivery_id;
                self.next_delivery_id += 1;
                let record = DeliveryAttemptRecord {
                    seq,
                    consumer_id,
                    delivery_id,
                    deadline_ms,
                    attempt,
                };
                consumer.pending.remove(&seq);
                consumer.pending_attempts.remove(&seq);
                consumer.in_flight.insert(seq, record.clone());
                BrokerResponse::DeliveryAttempt {
                    record: Some(record),
                }
            }
            BrokerCommand::Ack {
                seq,
                consumer_id,
                delivery_id,
            } => {
                let valid = self
                    .consumers
                    .get(&consumer_id)
                    .and_then(|consumer| consumer.in_flight.get(&seq))
                    .is_some_and(|in_flight| in_flight.delivery_id == delivery_id);
                if valid {
                    let consumer = self.consumers.get_mut(&consumer_id).unwrap();
                    consumer.in_flight.remove(&seq);
                    consumer.pending.remove(&seq);
                    consumer.pending_attempts.remove(&seq);
                    consumer.acked.insert(seq);
                    self.cleanup_acked_messages();
                }
                BrokerResponse::Ack { accepted: valid }
            }
        }
    }

    fn cleanup_acked_messages(&mut self) {
        let removable = self
            .messages
            .keys()
            .copied()
            .filter(|seq| {
                let mut interested = false;
                for consumer in self.consumers.values() {
                    if consumer.pending.contains(seq)
                        || consumer.in_flight.contains_key(seq)
                        || consumer.acked.contains(seq)
                    {
                        interested = true;
                        if !consumer.acked.contains(seq) {
                            return false;
                        }
                    }
                }
                interested
            })
            .collect::<Vec<_>>();
        for seq in removable {
            self.messages.remove(&seq);
        }
    }
}

mod log_store;
mod network;
mod proxy;
mod rpc;
mod runtime;
mod state_machine;
mod storage_io;

pub(crate) use self::proxy::proxy_stream_to_leader;
use self::{log_store::*, network::*, rpc::*, state_machine::*, storage_io::*};
pub use self::{
    proxy::proxy_to_leader,
    runtime::{ClusterNode, RaftRuntime},
};

#[cfg(test)]
mod tests;
