use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    fmt, io,
    net::SocketAddr,
    ops::RangeBounds,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
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
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tracing::error;

use crate::{
    config::ClusterConfig,
    error::{BrokerError, Result, ResultExt},
    wal::{ConsumerRecord, DeliveryAttemptRecord, PublishRecord},
};

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

#[derive(Clone)]
pub struct RaftRuntime {
    pub raft: BrokerRaft,
    pub state_machine: StateMachineStore,
    pub nodes: HashMap<u64, ClusterNode>,
    node_id: u64,
    tls_enabled: bool,
}

#[derive(Debug, Clone)]
pub struct ClusterNode {
    pub raft_addr: SocketAddr,
    pub client_addr: SocketAddr,
}

impl RaftRuntime {
    pub async fn open(config: &ClusterConfig, tls_enabled: bool) -> Result<Self> {
        std::fs::create_dir_all(&config.raft_dir)
            .with_context(|| format!("creating Raft directory {}", config.raft_dir.display()))?;

        let nodes = config
            .nodes
            .iter()
            .map(|node| {
                (
                    node.node_id,
                    ClusterNode {
                        raft_addr: node.raft_addr,
                        client_addr: node.client_addr,
                    },
                )
            })
            .collect::<HashMap<_, _>>();
        let raft_nodes = config
            .nodes
            .iter()
            .map(|node| (node.node_id, BasicNode::new(node.raft_addr)))
            .collect::<BTreeMap<_, _>>();

        let log_store = LogStore::open(config.raft_dir.join(LOG_FILE))?;
        let state_machine = StateMachineStore::open(
            config.raft_dir.join(STATE_FILE),
            config.raft_dir.join(SNAPSHOT_FILE),
            raft_nodes.clone(),
        )?;
        let network = NetworkFactory {
            nodes: nodes.clone(),
        };
        let raft_config = Arc::new(
            openraft::Config {
                cluster_name: "broker".to_string(),
                election_timeout_min: config.election_timeout_min_ms,
                election_timeout_max: config.election_timeout_max_ms,
                heartbeat_interval: config.heartbeat_interval_ms,
                snapshot_policy: SnapshotPolicy::LogsSinceLast(config.snapshot_threshold),
                ..Default::default()
            }
            .validate()
            .map_err(|err| BrokerError::with_source("invalid Raft config", err))?,
        );
        let raft = BrokerRaft::new(
            config.node_id,
            raft_config,
            network,
            log_store,
            state_machine.clone(),
        )
        .await
        .map_err(|err| BrokerError::with_source("starting Raft", err))?;

        if config.bootstrap {
            let initialized = raft
                .is_initialized()
                .await
                .map_err(|err| BrokerError::with_source("checking Raft initialization", err))?;
            if !initialized {
                raft.initialize(raft_nodes)
                    .await
                    .map_err(|err| BrokerError::with_source("initializing Raft cluster", err))?;
            }
        }

        Ok(Self {
            raft,
            state_machine,
            nodes,
            node_id: config.node_id,
            tls_enabled,
        })
    }

    pub fn spawn_listener(&self, listen: SocketAddr) {
        let raft = self.raft.clone();
        tokio::spawn(async move {
            if let Err(err) = serve_raft(raft, listen).await {
                error!(error = ?err, "raft transport error");
            }
        });
    }

    pub async fn client_write(&self, command: BrokerCommand) -> Result<BrokerResponse> {
        if let Some(leader) = self.raft.current_leader().await {
            if leader != self.node_id {
                if let Some(node) = self.nodes.get(&leader) {
                    return NetworkClient {
                        addr: node.raft_addr.to_string(),
                    }
                    .client_write(command)
                    .await;
                }
            }
        }
        let response = self
            .raft
            .client_write(command)
            .await
            .map_err(|err| BrokerError::with_source("proposing Raft command", err))?;
        Ok(response.data)
    }

    pub fn durable_state(&self) -> DurableState {
        self.state_machine.durable_state()
    }

    pub async fn is_leader(&self) -> bool {
        self.raft.current_leader().await == Some(self.node_id)
    }

    pub async fn current_leader(&self) -> Option<u64> {
        self.raft.current_leader().await
    }

    pub async fn leader_client_addr(&self) -> Option<SocketAddr> {
        let leader = self.raft.current_leader().await?;
        self.nodes.get(&leader).map(|node| node.client_addr)
    }

    pub fn tls_enabled(&self) -> bool {
        self.tls_enabled
    }

    pub fn cluster_size(&self) -> usize {
        self.nodes.len()
    }

    pub fn node_id(&self) -> u64 {
        self.node_id
    }
}

#[derive(Clone)]
pub struct LogStore {
    path: PathBuf,
    inner: Arc<Mutex<LogStoreData>>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct LogStoreData {
    vote: Option<Vote<u64>>,
    committed: Option<LogId<u64>>,
    last_purged_log_id: Option<LogId<u64>>,
    logs: BTreeMap<u64, Entry<BrokerRaftConfig>>,
}

impl LogStore {
    pub fn open(path: PathBuf) -> Result<Self> {
        let data = read_json_or_default(&path)?;
        Ok(Self {
            path,
            inner: Arc::new(Mutex::new(data)),
        })
    }

    fn persist(&self, data: &LogStoreData) -> io::Result<()> {
        write_json_atomically(&self.path, data)
    }
}

impl RaftLogReader<BrokerRaftConfig> for LogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + fmt::Debug + Send>(
        &mut self,
        range: RB,
    ) -> std::result::Result<Vec<Entry<BrokerRaftConfig>>, StorageError<u64>> {
        let data = self.inner.lock().unwrap();
        Ok(data
            .logs
            .range(range)
            .map(|(_, entry)| entry.clone())
            .collect())
    }
}

impl RaftLogStorage<BrokerRaftConfig> for LogStore {
    type LogReader = LogStore;

    async fn get_log_state(
        &mut self,
    ) -> std::result::Result<LogState<BrokerRaftConfig>, StorageError<u64>> {
        let data = self.inner.lock().unwrap();
        let last_log_id = data
            .logs
            .values()
            .next_back()
            .map(|entry| entry.log_id)
            .or(data.last_purged_log_id);
        Ok(LogState {
            last_purged_log_id: data.last_purged_log_id,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<u64>) -> std::result::Result<(), StorageError<u64>> {
        let mut data = self.inner.lock().unwrap();
        data.vote = Some(*vote);
        self.persist(&data)
            .map_err(|err| storage_error(ErrorSubject::Vote, ErrorVerb::Write, err))
    }

    async fn read_vote(&mut self) -> std::result::Result<Option<Vote<u64>>, StorageError<u64>> {
        Ok(self.inner.lock().unwrap().vote)
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<u64>>,
    ) -> std::result::Result<(), StorageError<u64>> {
        let mut data = self.inner.lock().unwrap();
        data.committed = committed;
        self.persist(&data)
            .map_err(|err| storage_error(ErrorSubject::Logs, ErrorVerb::Write, err))
    }

    async fn read_committed(
        &mut self,
    ) -> std::result::Result<Option<LogId<u64>>, StorageError<u64>> {
        Ok(self.inner.lock().unwrap().committed)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<BrokerRaftConfig>,
    ) -> std::result::Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<BrokerRaftConfig>> + Send,
        I::IntoIter: Send,
    {
        let result = {
            let mut data = self.inner.lock().unwrap();
            for entry in entries {
                data.logs.insert(entry.log_id.index, entry);
            }
            self.persist(&data)
        };
        match result {
            Ok(()) => {
                callback.log_io_completed(Ok(()));
                Ok(())
            }
            Err(err) => {
                callback.log_io_completed(Err(io::Error::new(err.kind(), err.to_string())));
                Err(storage_error(ErrorSubject::Logs, ErrorVerb::Write, err))
            }
        }
    }

    async fn truncate(&mut self, log_id: LogId<u64>) -> std::result::Result<(), StorageError<u64>> {
        let mut data = self.inner.lock().unwrap();
        data.logs.split_off(&log_id.index);
        self.persist(&data)
            .map_err(|err| storage_error(ErrorSubject::Log(log_id), ErrorVerb::Delete, err))
    }

    async fn purge(&mut self, log_id: LogId<u64>) -> std::result::Result<(), StorageError<u64>> {
        let mut data = self.inner.lock().unwrap();
        let keep = data.logs.split_off(&(log_id.index + 1));
        data.logs = keep;
        data.last_purged_log_id = Some(log_id);
        self.persist(&data)
            .map_err(|err| storage_error(ErrorSubject::Log(log_id), ErrorVerb::Delete, err))
    }
}

#[derive(Clone)]
pub struct StateMachineStore {
    path: PathBuf,
    snapshot_path: PathBuf,
    inner: Arc<Mutex<StateMachineData>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StateMachineData {
    state: DurableState,
    snapshot: Option<StoredSnapshot>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredSnapshot {
    meta: SnapshotMeta<u64, BasicNode>,
    data: Vec<u8>,
}

impl StateMachineStore {
    pub fn open(
        path: PathBuf,
        snapshot_path: PathBuf,
        nodes: BTreeMap<u64, BasicNode>,
    ) -> Result<Self> {
        let data = match read_json::<StateMachineData>(&path)? {
            Some(data) => data,
            None => StateMachineData {
                state: DurableState::new(nodes),
                snapshot: None,
            },
        };
        Ok(Self {
            path,
            snapshot_path,
            inner: Arc::new(Mutex::new(data)),
        })
    }

    pub fn durable_state(&self) -> DurableState {
        self.inner.lock().unwrap().state.clone()
    }

    fn persist(&self, data: &StateMachineData) -> io::Result<()> {
        write_json_atomically(&self.path, data)
    }
}

impl RaftStateMachine<BrokerRaftConfig> for StateMachineStore {
    type SnapshotBuilder = StateMachineStore;

    async fn applied_state(
        &mut self,
    ) -> std::result::Result<
        (Option<LogId<u64>>, StoredMembership<u64, BasicNode>),
        StorageError<u64>,
    > {
        let data = self.inner.lock().unwrap();
        Ok((data.state.last_applied, data.state.last_membership.clone()))
    }

    async fn apply<I>(
        &mut self,
        entries: I,
    ) -> std::result::Result<Vec<BrokerResponse>, StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<BrokerRaftConfig>> + Send,
        I::IntoIter: Send,
    {
        let mut responses = Vec::new();
        {
            let mut data = self.inner.lock().unwrap();
            for entry in entries {
                data.state.last_applied = Some(entry.log_id);
                if let Some(membership) = entry.payload.get_membership() {
                    data.state.last_membership =
                        StoredMembership::new(Some(entry.log_id), membership.clone());
                    responses.push(BrokerResponse::Noop);
                    continue;
                }
                match entry.payload {
                    EntryPayload::Blank => responses.push(BrokerResponse::Noop),
                    EntryPayload::Normal(command) => {
                        responses.push(data.state.apply_command(command));
                    }
                    EntryPayload::Membership(_) => unreachable!(),
                }
            }
            self.persist(&data)
                .map_err(|err| storage_error(ErrorSubject::StateMachine, ErrorVerb::Write, err))?;
        }
        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> std::result::Result<Box<Vec<u8>>, StorageError<u64>> {
        Ok(Box::new(Vec::new()))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, BasicNode>,
        snapshot: Box<Vec<u8>>,
    ) -> std::result::Result<(), StorageError<u64>> {
        let state: DurableState = serde_json::from_slice(&snapshot).map_err(|err| {
            storage_error(ErrorSubject::Snapshot(None), ErrorVerb::Read, json_io(err))
        })?;
        let snapshot: Snapshot<BrokerRaftConfig> = Snapshot {
            meta: meta.clone(),
            snapshot,
        };
        let mut data = self.inner.lock().unwrap();
        data.state = state;
        let snapshot_bytes = <Vec<u8> as Clone>::clone(&*snapshot.snapshot);
        data.snapshot = Some(StoredSnapshot {
            meta: snapshot.meta.clone(),
            data: snapshot_bytes,
        });
        self.persist(&data)
            .map_err(|err| storage_error(ErrorSubject::Snapshot(None), ErrorVerb::Write, err))?;
        write_json_atomically(&self.snapshot_path, data.snapshot.as_ref().unwrap())
            .map_err(|err| storage_error(ErrorSubject::Snapshot(None), ErrorVerb::Write, err))
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> std::result::Result<Option<Snapshot<BrokerRaftConfig>>, StorageError<u64>> {
        if let Some(snapshot) = &self.inner.lock().unwrap().snapshot {
            return Ok(Some(Snapshot {
                meta: snapshot.meta.clone(),
                snapshot: Box::new(snapshot.data.clone()),
            }));
        }
        let Some(snapshot): Option<StoredSnapshot> =
            read_json(&self.snapshot_path).map_err(|err| {
                storage_error(
                    ErrorSubject::Snapshot(None),
                    ErrorVerb::Read,
                    io::Error::new(io::ErrorKind::InvalidData, err.to_string()),
                )
            })?
        else {
            return Ok(None);
        };
        Ok(Some(Snapshot {
            meta: snapshot.meta,
            snapshot: Box::new(snapshot.data),
        }))
    }
}

impl RaftSnapshotBuilder<BrokerRaftConfig> for StateMachineStore {
    async fn build_snapshot(
        &mut self,
    ) -> std::result::Result<Snapshot<BrokerRaftConfig>, StorageError<u64>> {
        let (state, last_applied, last_membership) = {
            let data = self.inner.lock().unwrap();
            (
                data.state.clone(),
                data.state.last_applied,
                data.state.last_membership.clone(),
            )
        };
        let snapshot = serde_json::to_vec(&state).map_err(|err| {
            storage_error(ErrorSubject::Snapshot(None), ErrorVerb::Write, json_io(err))
        })?;
        let snapshot_id = match last_applied {
            Some(log_id) => format!("{}-{}", log_id.leader_id, log_id.index),
            None => "empty".to_string(),
        };
        let meta = SnapshotMeta {
            last_log_id: last_applied,
            last_membership,
            snapshot_id,
        };
        let snapshot: Snapshot<BrokerRaftConfig> = Snapshot {
            meta,
            snapshot: Box::new(snapshot),
        };
        {
            let mut data = self.inner.lock().unwrap();
            let snapshot_bytes = <Vec<u8> as Clone>::clone(&*snapshot.snapshot);
            data.snapshot = Some(StoredSnapshot {
                meta: snapshot.meta.clone(),
                data: snapshot_bytes,
            });
            self.persist(&data).map_err(|err| {
                storage_error(ErrorSubject::Snapshot(None), ErrorVerb::Write, err)
            })?;
        }
        let snapshot_bytes = <Vec<u8> as Clone>::clone(&*snapshot.snapshot);
        write_json_atomically(
            &self.snapshot_path,
            &StoredSnapshot {
                meta: snapshot.meta.clone(),
                data: snapshot_bytes,
            },
        )
        .map_err(|err| storage_error(ErrorSubject::Snapshot(None), ErrorVerb::Write, err))?;
        Ok(snapshot)
    }
}

#[derive(Clone)]
struct NetworkFactory {
    nodes: HashMap<u64, ClusterNode>,
}

impl RaftNetworkFactory<BrokerRaftConfig> for NetworkFactory {
    type Network = NetworkClient;

    async fn new_client(&mut self, target: u64, node: &BasicNode) -> Self::Network {
        let addr = self
            .nodes
            .get(&target)
            .map(|node| node.raft_addr.to_string())
            .unwrap_or_else(|| node.addr.clone());
        let _ = target;
        NetworkClient { addr }
    }
}

#[derive(Clone)]
struct NetworkClient {
    addr: String,
}

impl RaftNetwork<BrokerRaftConfig> for NetworkClient {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<BrokerRaftConfig>,
        _option: RPCOption,
    ) -> std::result::Result<
        AppendEntriesResponse<u64>,
        RPCError<u64, BasicNode, openraft::error::RaftError<u64>>,
    > {
        match self.request(RaftRequest::AppendEntries(rpc)).await? {
            RaftResponse::AppendEntries(response) => Ok(response),
            RaftResponse::Error(message) => Err(network_error(message)),
            _ => Err(network_error("unexpected append_entries response")),
        }
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        _option: RPCOption,
    ) -> std::result::Result<
        VoteResponse<u64>,
        RPCError<u64, BasicNode, openraft::error::RaftError<u64>>,
    > {
        match self.request(RaftRequest::Vote(rpc)).await? {
            RaftResponse::Vote(response) => Ok(response),
            RaftResponse::Error(message) => Err(network_error(message)),
            _ => Err(network_error("unexpected vote response")),
        }
    }

    async fn full_snapshot(
        &mut self,
        vote: Vote<u64>,
        snapshot: Snapshot<BrokerRaftConfig>,
        _cancel: impl std::future::Future<Output = ReplicationClosed> + Send + 'static,
        _option: RPCOption,
    ) -> std::result::Result<SnapshotResponse<u64>, StreamingError<BrokerRaftConfig, Fatal<u64>>>
    {
        match self
            .request(RaftRequest::FullSnapshot {
                vote,
                meta: snapshot.meta,
                data: *snapshot.snapshot,
            })
            .await
            .map_err(|err| match err {
                RPCError::Unreachable(err) => StreamingError::Unreachable(err),
                RPCError::Network(err) => StreamingError::Network(err),
                RPCError::Timeout(err) => StreamingError::Timeout(err),
                RPCError::PayloadTooLarge(err) => {
                    StreamingError::Unreachable(Unreachable::new(&err))
                }
                RPCError::RemoteError(err) => StreamingError::Unreachable(Unreachable::new(&err)),
            })? {
            RaftResponse::FullSnapshot(response) => Ok(response),
            RaftResponse::Error(message) => Err(StreamingError::Network(NetworkError::new(
                &SimpleError(message),
            ))),
            _ => Err(StreamingError::Network(NetworkError::new(&SimpleError(
                "unexpected full_snapshot response".to_string(),
            )))),
        }
    }
}

impl NetworkClient {
    async fn client_write(&self, command: BrokerCommand) -> Result<BrokerResponse> {
        match self
            .request(RaftRequest::ClientWrite(command))
            .await
            .map_err(|err| BrokerError::with_source("forwarding Raft client write", err))?
        {
            RaftResponse::ClientWrite(response) => Ok(response),
            RaftResponse::Error(message) => Err(BrokerError::msg(message)),
            _ => Err(BrokerError::msg("unexpected client_write response")),
        }
    }

    async fn request(
        &self,
        request: RaftRequest,
    ) -> std::result::Result<RaftResponse, RPCError<u64, BasicNode, openraft::error::RaftError<u64>>>
    {
        let mut stream = TcpStream::connect(&self.addr).await.map_err(|err| {
            RPCError::Unreachable(Unreachable::new(&io::Error::new(
                err.kind(),
                err.to_string(),
            )))
        })?;
        write_frame(&mut stream, &request)
            .await
            .map_err(|err| RPCError::Network(NetworkError::new(&err)))?;
        read_frame(&mut stream)
            .await
            .map_err(|err| RPCError::Network(NetworkError::new(&err)))
    }
}

#[derive(Debug, Serialize, Deserialize)]
enum RaftRequest {
    AppendEntries(AppendEntriesRequest<BrokerRaftConfig>),
    Vote(VoteRequest<u64>),
    ClientWrite(BrokerCommand),
    FullSnapshot {
        vote: Vote<u64>,
        meta: SnapshotMeta<u64, BasicNode>,
        data: Vec<u8>,
    },
}

#[derive(Debug, Serialize, Deserialize)]
enum RaftResponse {
    AppendEntries(AppendEntriesResponse<u64>),
    Vote(VoteResponse<u64>),
    ClientWrite(BrokerResponse),
    FullSnapshot(SnapshotResponse<u64>),
    Error(String),
}

async fn serve_raft(raft: BrokerRaft, listen: SocketAddr) -> Result<()> {
    let listener = TcpListener::bind(listen)
        .await
        .with_context(|| format!("binding Raft listener {listen}"))?;
    loop {
        let (stream, _) = listener.accept().await.context("accepting Raft RPC")?;
        let raft = raft.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_raft_stream(raft, stream).await {
                error!(error = ?err, "raft RPC error");
            }
        });
    }
}

async fn handle_raft_stream(raft: BrokerRaft, mut stream: TcpStream) -> Result<()> {
    let request: RaftRequest = read_frame(&mut stream).await?;
    let response = match request {
        RaftRequest::AppendEntries(rpc) => match raft.append_entries(rpc).await {
            Ok(response) => RaftResponse::AppendEntries(response),
            Err(err) => RaftResponse::Error(err.to_string()),
        },
        RaftRequest::Vote(rpc) => match raft.vote(rpc).await {
            Ok(response) => RaftResponse::Vote(response),
            Err(err) => RaftResponse::Error(err.to_string()),
        },
        RaftRequest::ClientWrite(command) => match raft.client_write(command).await {
            Ok(response) => RaftResponse::ClientWrite(response.data),
            Err(err) => RaftResponse::Error(err.to_string()),
        },
        RaftRequest::FullSnapshot { vote, meta, data } => {
            let snapshot: Snapshot<BrokerRaftConfig> = Snapshot {
                meta,
                snapshot: Box::new(data),
            };
            match raft.install_full_snapshot(vote, snapshot).await {
                Ok(response) => RaftResponse::FullSnapshot(response),
                Err(err) => RaftResponse::Error(err.to_string()),
            }
        }
    };
    write_frame(&mut stream, &response).await?;
    Ok(())
}

async fn write_frame<W, T>(writer: &mut W, value: &T) -> Result<()>
where
    W: AsyncWrite + Unpin,
    T: Serialize,
{
    let body = serde_json::to_vec(value).context("serializing Raft frame")?;
    crate::broker_ensure!(
        body.len() <= MAX_RAFT_FRAME,
        "Raft frame exceeds maximum size"
    );
    let len: u32 = body.len().try_into().context("Raft frame too large")?;
    writer.write_all(&len.to_le_bytes()).await?;
    writer.write_all(&body).await?;
    Ok(())
}

async fn read_frame<R, T>(reader: &mut R) -> Result<T>
where
    R: AsyncRead + Unpin,
    T: for<'de> Deserialize<'de>,
{
    let mut len = [0; 4];
    reader.read_exact(&mut len).await?;
    let len = u32::from_le_bytes(len) as usize;
    crate::broker_ensure!(len <= MAX_RAFT_FRAME, "Raft frame exceeds maximum size");
    let mut body = vec![0; len];
    reader.read_exact(&mut body).await?;
    serde_json::from_slice(&body).context("decoding Raft frame")
}

fn read_json_or_default<T>(path: &Path) -> Result<T>
where
    T: for<'de> Deserialize<'de> + Default,
{
    Ok(read_json(path)?.unwrap_or_default())
}

fn read_json<T>(path: &Path) -> Result<Option<T>>
where
    T: for<'de> Deserialize<'de>,
{
    if !path.exists() {
        return Ok(None);
    }
    let contents =
        std::fs::read(path).with_context(|| format!("reading Raft file {}", path.display()))?;
    let value = serde_json::from_slice(&contents)
        .with_context(|| format!("parsing Raft file {}", path.display()))?;
    Ok(Some(value))
}

fn write_json_atomically<T>(path: &Path, value: &T) -> io::Result<()>
where
    T: Serialize,
{
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("tmp");
    let body = serde_json::to_vec(value).map_err(json_io)?;
    std::fs::write(&tmp, body)?;
    let file = std::fs::OpenOptions::new().read(true).open(&tmp)?;
    file.sync_data()?;
    std::fs::rename(&tmp, path)?;
    if let Some(parent) = path.parent() {
        let dir = std::fs::File::open(parent)?;
        dir.sync_data()?;
    }
    Ok(())
}

fn storage_error(subject: ErrorSubject<u64>, verb: ErrorVerb, err: io::Error) -> StorageError<u64> {
    StorageError::from_io_error(subject, verb, err)
}

fn json_io(err: serde_json::Error) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, err)
}

fn network_error(
    message: impl Into<String>,
) -> RPCError<u64, BasicNode, openraft::error::RaftError<u64>> {
    RPCError::Network(NetworkError::new(&SimpleError(message.into())))
}

#[derive(Debug)]
struct SimpleError(String);

impl fmt::Display for SimpleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for SimpleError {}

pub(crate) async fn proxy_stream_to_leader<S>(mut inbound: S, leader: SocketAddr) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut outbound = TcpStream::connect(leader)
        .await
        .with_context(|| format!("connecting to leader {leader}"))?;
    tokio::io::copy_bidirectional(&mut inbound, &mut outbound)
        .await
        .context("proxying client connection to leader")?;
    Ok(())
}

pub async fn proxy_to_leader(inbound: TcpStream, leader: SocketAddr) -> Result<()> {
    proxy_stream_to_leader(inbound, leader).await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nodes() -> BTreeMap<u64, BasicNode> {
        [(1, BasicNode::new("127.0.0.1:5221"))]
            .into_iter()
            .collect()
    }

    #[test]
    fn applies_publish_attempt_and_ack() {
        let mut state = DurableState::new(nodes());
        let record = ConsumerRecord {
            consumer_id: "durable-client-sid".into(),
            filter_subject: "orders.*".into(),
            queue_group: None,
            ack_timeout_ms: 30_000,
            max_in_flight: 1024,
        };
        assert_eq!(
            state.apply_command(BrokerCommand::ConsumerUpsert { record }),
            BrokerResponse::ConsumerUpsert
        );
        assert_eq!(
            state.apply_command(BrokerCommand::Publish {
                subject: "orders.created".into(),
                reply_to: None,
                payload: b"ok".to_vec(),
            }),
            BrokerResponse::Publish {
                seq: Some(1),
                retained: true
            }
        );
        assert!(state.consumers["durable-client-sid"].pending.contains(&1));

        let response = state.apply_command(BrokerCommand::DeliveryAttempt {
            seq: 1,
            consumer_id: "durable-client-sid".into(),
            deadline_ms: 10,
            attempt: 1,
        });
        let BrokerResponse::DeliveryAttempt {
            record: Some(attempt),
        } = response
        else {
            panic!("expected delivery attempt");
        };
        assert_eq!(attempt.delivery_id, 1);
        assert!(
            state.consumers["durable-client-sid"]
                .in_flight
                .contains_key(&1)
        );

        assert_eq!(
            state.apply_command(BrokerCommand::Ack {
                seq: 1,
                consumer_id: "durable-client-sid".into(),
                delivery_id: 1,
            }),
            BrokerResponse::Ack { accepted: true }
        );
        assert!(state.messages.is_empty());
    }

    #[test]
    fn publish_without_matching_consumer_is_not_retained() {
        let mut state = DurableState::new(nodes());
        assert_eq!(
            state.apply_command(BrokerCommand::Publish {
                subject: "orders.created".into(),
                reply_to: None,
                payload: b"ok".to_vec(),
            }),
            BrokerResponse::Publish {
                seq: None,
                retained: false
            }
        );
        assert!(state.messages.is_empty());
    }

    #[test]
    fn delivery_attempts_allocate_monotonic_delivery_ids() {
        let mut state = DurableState::new(nodes());
        state.apply_command(BrokerCommand::ConsumerUpsert {
            record: ConsumerRecord {
                consumer_id: "durable-client-sid".into(),
                filter_subject: "orders.*".into(),
                queue_group: None,
                ack_timeout_ms: 30_000,
                max_in_flight: 1024,
            },
        });
        state.apply_command(BrokerCommand::Publish {
            subject: "orders.created".into(),
            reply_to: None,
            payload: b"one".to_vec(),
        });
        state.apply_command(BrokerCommand::Publish {
            subject: "orders.updated".into(),
            reply_to: None,
            payload: b"two".to_vec(),
        });

        let BrokerResponse::DeliveryAttempt {
            record: Some(first),
        } = state.apply_command(BrokerCommand::DeliveryAttempt {
            seq: 1,
            consumer_id: "durable-client-sid".into(),
            deadline_ms: 10,
            attempt: 1,
        })
        else {
            panic!("expected first delivery attempt");
        };
        let BrokerResponse::DeliveryAttempt {
            record: Some(second),
        } = state.apply_command(BrokerCommand::DeliveryAttempt {
            seq: 2,
            consumer_id: "durable-client-sid".into(),
            deadline_ms: 20,
            attempt: 1,
        })
        else {
            panic!("expected second delivery attempt");
        };

        assert_eq!(first.delivery_id, 1);
        assert_eq!(second.delivery_id, 2);
    }

    #[test]
    fn ack_rejects_stale_delivery_id() {
        let mut state = DurableState::new(nodes());
        state.apply_command(BrokerCommand::ConsumerUpsert {
            record: ConsumerRecord {
                consumer_id: "durable-client-sid".into(),
                filter_subject: "orders.*".into(),
                queue_group: None,
                ack_timeout_ms: 30_000,
                max_in_flight: 1024,
            },
        });
        state.apply_command(BrokerCommand::Publish {
            subject: "orders.created".into(),
            reply_to: None,
            payload: b"one".to_vec(),
        });
        state.apply_command(BrokerCommand::DeliveryAttempt {
            seq: 1,
            consumer_id: "durable-client-sid".into(),
            deadline_ms: 10,
            attempt: 1,
        });

        assert_eq!(
            state.apply_command(BrokerCommand::Ack {
                seq: 1,
                consumer_id: "durable-client-sid".into(),
                delivery_id: 2,
            }),
            BrokerResponse::Ack { accepted: false }
        );
        assert!(state.messages.contains_key(&1));
        assert!(
            state.consumers["durable-client-sid"]
                .in_flight
                .contains_key(&1)
        );
    }

    #[test]
    fn cleanup_waits_for_all_interested_consumers_to_ack() {
        let mut state = DurableState::new(nodes());
        for consumer_id in ["durable-a-sid", "durable-b-sid"] {
            state.apply_command(BrokerCommand::ConsumerUpsert {
                record: ConsumerRecord {
                    consumer_id: consumer_id.into(),
                    filter_subject: "orders.*".into(),
                    queue_group: None,
                    ack_timeout_ms: 30_000,
                    max_in_flight: 1024,
                },
            });
        }
        state.apply_command(BrokerCommand::Publish {
            subject: "orders.created".into(),
            reply_to: None,
            payload: b"one".to_vec(),
        });
        state.apply_command(BrokerCommand::DeliveryAttempt {
            seq: 1,
            consumer_id: "durable-a-sid".into(),
            deadline_ms: 10,
            attempt: 1,
        });
        state.apply_command(BrokerCommand::DeliveryAttempt {
            seq: 1,
            consumer_id: "durable-b-sid".into(),
            deadline_ms: 10,
            attempt: 1,
        });

        assert_eq!(
            state.apply_command(BrokerCommand::Ack {
                seq: 1,
                consumer_id: "durable-a-sid".into(),
                delivery_id: 1,
            }),
            BrokerResponse::Ack { accepted: true }
        );
        assert!(state.messages.contains_key(&1));

        assert_eq!(
            state.apply_command(BrokerCommand::Ack {
                seq: 1,
                consumer_id: "durable-b-sid".into(),
                delivery_id: 2,
            }),
            BrokerResponse::Ack { accepted: true }
        );
        assert!(state.messages.is_empty());
    }
}
