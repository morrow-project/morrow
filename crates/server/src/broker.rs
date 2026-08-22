use std::{
    collections::{BTreeSet, HashMap, HashSet},
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::{Mutex, mpsc},
};
use tokio_rustls::TlsAcceptor;
use tracing::{error, info};

#[cfg(test)]
use openraft::BasicNode;
use protocol::{AckSubject, Command, ConnectAuth, auth, subject};
#[cfg(test)]
use std::collections::{BTreeMap, VecDeque};
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
    raft::{BrokerCommand, BrokerResponse, DurableState, RaftRuntime, proxy_stream_to_leader},
    wal::{
        ConsumerCursorRecord, ConsumerRecord, DeliveryAttemptRecord, PartitionAppendRecord,
        PublishRecord, ReplayedConsumer, Wal, WalStatus,
    },
};

const DEFAULT_ACK_TIMEOUT_MS: u64 = 30_000;
pub(crate) const DEFAULT_MAX_IN_FLIGHT: usize = 1024;
const REDELIVERY_SCAN_INTERVAL_MS: u64 = 50;
const CLUSTER_LOG_SCAN_INTERVAL_MS: u64 = 500;
const UNAUTHENTICATED_READ_TIMEOUT_MS: u64 = 5_000;
const ROUTE_FRAME_READ_TIMEOUT_MS: u64 = 5_000;
const MAX_ROUTE_FRAME: usize = 2 * 1024 * 1024;

mod broker;
mod broker_client;
mod broker_lifecycle;
mod broker_publish;
mod cluster_operations;
mod cluster_runtime;
mod compaction;
mod consumer;
mod fake_cluster;
mod fake_cluster_types;
mod hooks;
mod http;
mod inner_admin;
mod inner_delivery;
mod manual_clock;
mod middleware_hooks;
mod producer_ack;
mod pull_consumer;
mod retention;
mod route_mesh;
mod route_state;
mod state;
mod subject_helpers;

pub use self::broker::Broker;
#[allow(unused_imports)]
use self::{
    cluster_runtime::*, compaction::*, consumer::*, fake_cluster::*, fake_cluster_types::*,
    hooks::*, http::*, inner_admin::*, inner_delivery::*, manual_clock::*, pull_consumer::*,
    retention::*, route_mesh::*, route_state::*, state::*, subject_helpers::*,
};

#[cfg(test)]
use crate::partition_replication::{
    Durability as PartitionDurability, PartitionAssignment, PartitionReplication,
};

#[cfg(test)]
mod tests;
