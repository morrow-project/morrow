use std::{
    cmp::Reverse,
    collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap, HashSet},
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::{Mutex, Notify, mpsc},
};
use tokio_rustls::TlsAcceptor;
use tracing::{Instrument, error, info, warn};

#[cfg(test)]
use openraft::BasicNode;
use protocol::{AckSubject, Command, ConnectAuth, auth, subject};
#[cfg(test)]
use std::collections::VecDeque;
#[cfg(test)]
use tokio::sync::oneshot;

use crate::{
    config::Config,
    error::{BrokerError, Result, ResultExt},
    middleware::{MiddlewareDecision, MiddlewareMessage, MiddlewareRuntime, MiddlewareStage},
    partition_log::{
        AppendRequest, DEFAULT_NAMESPACE, MessageEnvelope, MessageHeader, PartitionLogSet,
        select_partition,
    },
    raft::{
        BrokerCommand, BrokerResponse, CommittedDelta, DeltaBatch, DurableState, RaftRuntime,
        proxy_stream_to_leader,
    },
    wal::{
        ConsumerCursorDeltaRecord, ConsumerCursorRecord, ConsumerRecord, DeadLetterRecord,
        DeliveryAttemptRecord, PartitionAppendRecord, ProducerSequenceRecord, PublishRecord,
        ReplayedConsumer, Wal, WalStatus,
    },
};

const DEFAULT_ACK_TIMEOUT_MS: u64 = 30_000;
pub(crate) const DEFAULT_MAX_IN_FLIGHT: usize = 1024;
const MAX_EXPIRED_LEASES_PER_TICK: usize = 1_024;
const RETENTION_TICK_INTERVAL_MS: u64 = 1_000;
const CLUSTER_LOG_SCAN_INTERVAL_MS: u64 = 500;
const UNAUTHENTICATED_READ_TIMEOUT_MS: u64 = 5_000;
const ROUTE_FRAME_READ_TIMEOUT_MS: u64 = 5_000;
const MAX_ROUTE_FRAME: usize = 2 * 1024 * 1024;
const MAX_BLOCKING_STORAGE_OPS: usize = 64;

mod broker;
mod broker_authorization;
mod broker_client;
mod broker_lifecycle;
mod broker_publish;
mod cluster_delta;
mod cluster_operations;
mod cluster_runtime;
mod compaction;
mod consumer;
mod consumer_group_commands;
mod delivery_index;
mod fake_cluster;
mod fake_cluster_types;
mod hooks;
mod http;
mod http_listener;
mod inner_admin;
mod inner_delivery;
mod manual_clock;
mod middleware_hooks;
mod producer_ack;
mod pull_consumer;
mod pull_delivery;
mod pull_waiters;
mod redelivery;
mod retention;
mod route_connection;
mod route_interests;
mod route_mesh;
mod route_state;
mod route_tls;
mod state;
mod subject_helpers;
mod wal_runtime;
mod websocket;

pub use self::broker::Morrow;
#[allow(unused_imports)]
use self::{
    cluster_runtime::*, compaction::*, consumer::*, consumer_group_commands::*, fake_cluster::*,
    fake_cluster_types::*, hooks::*, http::*, inner_admin::*, inner_delivery::*, manual_clock::*,
    pull_consumer::*, pull_waiters::*, retention::*, route_connection::*, route_interests::*,
    route_mesh::*, route_state::*, route_tls::*, state::*, subject_helpers::*, wal_runtime::*,
};

#[cfg(test)]
use crate::partition_replication::{
    Durability as PartitionDurability, PartitionAssignment, PartitionReplication,
};

#[cfg(test)]
mod tests;
