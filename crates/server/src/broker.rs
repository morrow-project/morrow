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
    raft::{BrokerCommand, BrokerResponse, DurableState, RaftRuntime, proxy_stream_to_leader},
    wal::{ConsumerRecord, PublishRecord, ReplayedConsumer, Wal},
};

const DEFAULT_ACK_TIMEOUT_MS: u64 = 30_000;
const DEFAULT_MAX_IN_FLIGHT: usize = 1024;
const REDELIVERY_SCAN_INTERVAL_MS: u64 = 50;
const CLUSTER_LOG_SCAN_INTERVAL_MS: u64 = 500;

mod broker;
mod broker_client;
mod broker_lifecycle;
mod broker_publish;
mod cluster_delivery;
mod cluster_runtime;
mod consumer;
mod fake_cluster;
mod fake_cluster_types;
mod hooks;
mod http;
mod inner_admin;
mod inner_delivery;
mod manual_clock;
mod route_mesh;
mod route_state;
mod state;
mod subject_helpers;

pub use self::broker::Broker;
#[allow(unused_imports)]
use self::{
    cluster_delivery::*, cluster_runtime::*, consumer::*, fake_cluster::*, fake_cluster_types::*,
    hooks::*, http::*, inner_admin::*, inner_delivery::*, manual_clock::*, route_mesh::*,
    route_state::*, state::*, subject_helpers::*,
};

#[cfg(test)]
mod tests;
