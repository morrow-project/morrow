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

#[derive(Clone)]
pub struct Broker {
    inner: Arc<Mutex<Inner>>,
    next_connection_id: Arc<AtomicU64>,
    config: Config,
    tls_acceptor: Option<TlsAcceptor>,
    cluster: Arc<Mutex<Option<ClusterRuntime>>>,
    route_mesh: Option<RouteMesh>,
    hooks: BrokerHooks,
}

#[derive(Clone)]
pub(crate) struct BrokerHooks {
    clock: Arc<dyn Clock>,
    start_redelivery_loop: bool,
    durable_publish_flush_mode: DurablePublishFlushMode,
    #[cfg(test)]
    initial_cluster: Option<ClusterRuntime>,
}

pub(crate) trait Clock: Send + Sync {
    fn now_ms(&self) -> u64;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DurablePublishFlushMode {
    SleepThenFlush,
    #[cfg(test)]
    FlushImmediately,
}

struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> u64 {
        now_ms()
    }
}

impl Default for BrokerHooks {
    fn default() -> Self {
        Self {
            clock: Arc::new(SystemClock),
            start_redelivery_loop: true,
            durable_publish_flush_mode: DurablePublishFlushMode::SleepThenFlush,
            #[cfg(test)]
            initial_cluster: None,
        }
    }
}

#[derive(Clone)]
enum ClusterRuntime {
    Real(RaftRuntime),
    #[cfg(test)]
    Fake(FakeClusterRuntime),
}

impl ClusterRuntime {
    fn real(runtime: RaftRuntime) -> Self {
        Self::Real(runtime)
    }

    async fn client_write(&self, command: BrokerCommand) -> Result<BrokerResponse> {
        match self {
            Self::Real(runtime) => runtime.client_write(command).await,
            #[cfg(test)]
            Self::Fake(runtime) => runtime.client_write(command).await,
        }
    }

    async fn client_write_forwarded(&self, command: BrokerCommand) -> Result<BrokerResponse> {
        match self {
            Self::Real(runtime) => runtime.client_write(command).await,
            #[cfg(test)]
            Self::Fake(runtime) => runtime.client_write_forwarded(command).await,
        }
    }

    fn durable_state(&self) -> DurableState {
        match self {
            Self::Real(runtime) => runtime.durable_state(),
            #[cfg(test)]
            Self::Fake(runtime) => runtime.durable_state(),
        }
    }

    async fn is_leader(&self) -> bool {
        match self {
            Self::Real(runtime) => runtime.is_leader().await,
            #[cfg(test)]
            Self::Fake(runtime) => runtime.is_leader().await,
        }
    }

    async fn current_leader(&self) -> Option<u64> {
        match self {
            Self::Real(runtime) => runtime.current_leader().await,
            #[cfg(test)]
            Self::Fake(runtime) => runtime.current_leader().await,
        }
    }

    async fn leader_client_addr(&self) -> Option<SocketAddr> {
        match self {
            Self::Real(runtime) => runtime.leader_client_addr().await,
            #[cfg(test)]
            Self::Fake(runtime) => runtime.leader_client_addr().await,
        }
    }

    fn tls_enabled(&self) -> bool {
        match self {
            Self::Real(runtime) => runtime.tls_enabled(),
            #[cfg(test)]
            Self::Fake(runtime) => runtime.tls_enabled(),
        }
    }

    fn cluster_size(&self) -> usize {
        match self {
            Self::Real(runtime) => runtime.cluster_size(),
            #[cfg(test)]
            Self::Fake(runtime) => runtime.node_count(),
        }
    }

    fn local_node_id(&self) -> u64 {
        match self {
            Self::Real(runtime) => runtime.node_id(),
            #[cfg(test)]
            Self::Fake(runtime) => runtime.local_node_id(),
        }
    }
}

#[derive(Clone)]
struct RouteMesh {
    inner: Arc<Mutex<RouteMeshState>>,
}

struct RouteMeshState {
    node_id: u64,
    route_addr: SocketAddr,
    client_addr: SocketAddr,
    seeds: Vec<SocketAddr>,
    reconnect_ms: u64,
    peers: HashMap<u64, RoutePeer>,
    known_peers: HashMap<u64, RoutePeerInfo>,
    local_interests: Vec<String>,
}

struct RoutePeer {
    info: RoutePeerInfo,
    sender: mpsc::Sender<RouteFrame>,
    direction: RouteDirection,
    state: &'static str,
    reconnect_attempts: u64,
    last_error: Option<String>,
    remote_interests: Vec<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct RoutePeerInfo {
    node_id: u64,
    route_addr: SocketAddr,
    client_addr: SocketAddr,
}

#[derive(Debug, Clone, Copy)]
enum RouteDirection {
    Inbound,
    Outbound,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum RouteFrame {
    Hello {
        node_id: u64,
        route_addr: SocketAddr,
        client_addr: SocketAddr,
    },
    PeerList {
        peers: Vec<RoutePeerInfo>,
    },
    Interests {
        subjects: Vec<String>,
    },
    Publish {
        subject: String,
        reply_to: Option<String>,
        payload: Vec<u8>,
    },
    Ping,
    Pong,
}

#[derive(Debug, serde::Serialize)]
struct RouteTopologyResponse {
    listen: String,
    seeds: Vec<String>,
    discovered: Vec<RouteDiscoveredPeerResponse>,
    connected: Vec<RoutePeerResponse>,
}

#[derive(Debug, serde::Serialize)]
struct RouteDiscoveredPeerResponse {
    node_id: u64,
    route_addr: String,
    client_addr: String,
    connected: bool,
}

#[derive(Debug, serde::Serialize)]
struct RoutePeerResponse {
    node_id: u64,
    route_addr: String,
    client_addr: String,
    direction: &'static str,
    state: &'static str,
    reconnect_attempts: u64,
    last_error: Option<String>,
    subscriptions: usize,
    subjects: Vec<String>,
}

impl RouteMesh {
    fn from_config(config: &Config) -> Option<Self> {
        let cluster = config.cluster.as_ref()?;
        let route_addr = cluster.route_listen?;
        Some(Self {
            inner: Arc::new(Mutex::new(RouteMeshState {
                node_id: cluster.node_id,
                route_addr,
                client_addr: config.listen,
                seeds: cluster.routes.clone(),
                reconnect_ms: cluster.route_reconnect_ms,
                peers: HashMap::new(),
                known_peers: HashMap::new(),
                local_interests: Vec::new(),
            })),
        })
    }

    async fn start(&self, broker: Broker) -> Result<()> {
        let (listen, reconnect_ms) = {
            let state = self.inner.lock().await;
            (state.route_addr, state.reconnect_ms)
        };
        let listener = TcpListener::bind(listen)
            .await
            .with_context(|| format!("binding route listener {listen}"))?;
        let accept_mesh = self.clone();
        let accept_broker = broker.clone();
        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, _)) => {
                        let mesh = accept_mesh.clone();
                        let broker = accept_broker.clone();
                        tokio::spawn(async move {
                            if let Err(err) =
                                handle_route_stream(mesh, broker, stream, RouteDirection::Inbound)
                                    .await
                            {
                                error!(error = ?err, "route connection error");
                            }
                        });
                    }
                    Err(err) => {
                        error!(error = ?err, "accepting route connection failed");
                    }
                }
            }
        });

        let dial_mesh = self.clone();
        let dial_broker = broker;
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(reconnect_ms));
            loop {
                interval.tick().await;
                let addrs = dial_mesh.dial_candidates().await;
                for addr in addrs {
                    match TcpStream::connect(addr).await {
                        Ok(stream) => {
                            let mesh = dial_mesh.clone();
                            let broker = dial_broker.clone();
                            tokio::spawn(async move {
                                if let Err(err) = handle_route_stream(
                                    mesh,
                                    broker,
                                    stream,
                                    RouteDirection::Outbound,
                                )
                                .await
                                {
                                    error!(error = ?err, "route connection error");
                                }
                            });
                        }
                        Err(err) => {
                            dial_mesh.note_dial_error(addr, err.to_string()).await;
                        }
                    }
                }
            }
        });
        Ok(())
    }

    async fn dial_candidates(&self) -> Vec<SocketAddr> {
        let state = self.inner.lock().await;
        let mut addrs = state.seeds.clone();
        addrs.extend(state.known_peers.values().map(|peer| peer.route_addr));
        addrs.sort();
        addrs.dedup();
        addrs
            .into_iter()
            .filter(|addr| *addr != state.route_addr)
            .filter(|addr| {
                !state
                    .peers
                    .values()
                    .any(|peer| peer.info.route_addr == *addr)
            })
            .collect()
    }

    async fn note_dial_error(&self, addr: SocketAddr, error: String) {
        let mut state = self.inner.lock().await;
        for peer in state.known_peers.values_mut() {
            if peer.route_addr == addr {
                continue;
            }
        }
        for peer in state.peers.values_mut() {
            if peer.info.route_addr == addr {
                peer.last_error = Some(error.clone());
                peer.reconnect_attempts = peer.reconnect_attempts.saturating_add(1);
            }
        }
    }

    async fn hello(&self) -> RouteFrame {
        let state = self.inner.lock().await;
        RouteFrame::Hello {
            node_id: state.node_id,
            route_addr: state.route_addr,
            client_addr: state.client_addr,
        }
    }

    async fn peer_list(&self) -> RouteFrame {
        let state = self.inner.lock().await;
        RouteFrame::PeerList {
            peers: state
                .known_peers
                .values()
                .cloned()
                .chain(std::iter::once(RoutePeerInfo {
                    node_id: state.node_id,
                    route_addr: state.route_addr,
                    client_addr: state.client_addr,
                }))
                .collect(),
        }
    }

    async fn interests(&self) -> RouteFrame {
        let state = self.inner.lock().await;
        RouteFrame::Interests {
            subjects: state.local_interests.clone(),
        }
    }

    async fn register_peer(
        &self,
        info: RoutePeerInfo,
        direction: RouteDirection,
        sender: mpsc::Sender<RouteFrame>,
    ) -> Option<bool> {
        let mut state = self.inner.lock().await;
        if info.node_id == state.node_id || info.route_addr == state.route_addr {
            return None;
        }
        let added = !state.known_peers.contains_key(&info.node_id);
        state.known_peers.insert(info.node_id, info.clone());
        state.peers.insert(
            info.node_id,
            RoutePeer {
                info,
                sender,
                direction,
                state: "connected",
                reconnect_attempts: 0,
                last_error: None,
                remote_interests: Vec::new(),
            },
        );
        Some(added)
    }

    async fn remove_peer(&self, node_id: u64) {
        let mut state = self.inner.lock().await;
        state.peers.remove(&node_id);
    }

    async fn merge_peers(&self, peers: Vec<RoutePeerInfo>) -> Vec<u64> {
        let mut state = self.inner.lock().await;
        let node_id = state.node_id;
        let route_addr = state.route_addr;
        let mut added = Vec::new();
        for peer in peers {
            if peer.node_id != node_id && peer.route_addr != route_addr {
                if !state.known_peers.contains_key(&peer.node_id) {
                    added.push(peer.node_id);
                }
                state.known_peers.insert(peer.node_id, peer);
            }
        }
        added
    }

    async fn connected_peer_count(&self) -> usize {
        self.inner.lock().await.peers.len()
    }

    async fn set_remote_interests(&self, node_id: u64, subjects: Vec<String>) {
        let mut state = self.inner.lock().await;
        if let Some(peer) = state.peers.get_mut(&node_id) {
            peer.remote_interests = subjects;
        }
    }

    async fn set_local_interests(&self, subjects: Vec<String>) {
        let senders = {
            let mut state = self.inner.lock().await;
            state.local_interests = subjects.clone();
            state
                .peers
                .values()
                .map(|peer| peer.sender.clone())
                .collect::<Vec<_>>()
        };
        for sender in senders {
            let _ = sender
                .send(RouteFrame::Interests {
                    subjects: subjects.clone(),
                })
                .await;
        }
    }

    async fn broadcast_peer_list(&self) {
        let frame = self.peer_list().await;
        let senders = {
            let state = self.inner.lock().await;
            state
                .peers
                .values()
                .map(|peer| peer.sender.clone())
                .collect::<Vec<_>>()
        };
        for sender in senders {
            let _ = sender.send(frame.clone()).await;
        }
    }

    async fn forward_publish(&self, subject: &str, reply_to: Option<&str>, payload: &[u8]) {
        let targets = {
            let state = self.inner.lock().await;
            state
                .peers
                .values()
                .filter(|peer| {
                    peer.remote_interests
                        .iter()
                        .any(|interest| subject::matches(interest, subject))
                })
                .map(|peer| peer.sender.clone())
                .collect::<Vec<_>>()
        };
        for sender in targets {
            let _ = sender
                .send(RouteFrame::Publish {
                    subject: subject.to_string(),
                    reply_to: reply_to.map(str::to_string),
                    payload: payload.to_vec(),
                })
                .await;
        }
    }

    async fn topology_response(&self) -> RouteTopologyResponse {
        let state = self.inner.lock().await;
        let mut discovered = state
            .known_peers
            .values()
            .map(|peer| RouteDiscoveredPeerResponse {
                node_id: peer.node_id,
                route_addr: peer.route_addr.to_string(),
                client_addr: peer.client_addr.to_string(),
                connected: state.peers.contains_key(&peer.node_id),
            })
            .collect::<Vec<_>>();
        discovered.sort_by_key(|peer| peer.node_id);
        let mut connected = state
            .peers
            .iter()
            .map(|(node_id, peer)| RoutePeerResponse {
                node_id: *node_id,
                route_addr: peer.info.route_addr.to_string(),
                client_addr: peer.info.client_addr.to_string(),
                direction: match peer.direction {
                    RouteDirection::Inbound => "inbound",
                    RouteDirection::Outbound => "outbound",
                },
                state: peer.state,
                reconnect_attempts: peer.reconnect_attempts,
                last_error: peer.last_error.clone(),
                subscriptions: peer.remote_interests.len(),
                subjects: peer.remote_interests.clone(),
            })
            .collect::<Vec<_>>();
        connected.sort_by_key(|peer| peer.node_id);
        RouteTopologyResponse {
            listen: state.route_addr.to_string(),
            seeds: state.seeds.iter().map(ToString::to_string).collect(),
            discovered,
            connected,
        }
    }
}

async fn handle_route_stream(
    mesh: RouteMesh,
    broker: Broker,
    stream: TcpStream,
    direction: RouteDirection,
) -> Result<()> {
    let (mut reader, mut writer) = stream.into_split();
    let (sender, mut receiver) = mpsc::channel::<RouteFrame>(256);
    sender
        .send(mesh.hello().await)
        .await
        .map_err(|_| BrokerError::msg("route writer closed"))?;
    sender
        .send(mesh.peer_list().await)
        .await
        .map_err(|_| BrokerError::msg("route writer closed"))?;
    sender
        .send(mesh.interests().await)
        .await
        .map_err(|_| BrokerError::msg("route writer closed"))?;
    let writer_task = tokio::spawn(async move {
        while let Some(frame) = receiver.recv().await {
            write_route_frame(&mut writer, &frame).await?;
        }
        Ok::<(), BrokerError>(())
    });

    let mut peer_id = None;
    loop {
        let Some(frame) = read_route_frame(&mut reader).await? else {
            break;
        };
        match frame {
            RouteFrame::Hello {
                node_id,
                route_addr,
                client_addr,
            } => {
                let info = RoutePeerInfo {
                    node_id,
                    route_addr,
                    client_addr,
                };
                let Some(added_peer) = mesh.register_peer(info, direction, sender.clone()).await
                else {
                    break;
                };
                peer_id = Some(node_id);
                if added_peer {
                    broker.log_cluster_event("cluster peer added").await;
                }
                mesh.broadcast_peer_list().await;
            }
            RouteFrame::PeerList { peers } => {
                for _ in mesh.merge_peers(peers).await {
                    broker.log_cluster_event("cluster peer added").await;
                }
            }
            RouteFrame::Interests { subjects } => {
                if let Some(node_id) = peer_id {
                    mesh.set_remote_interests(node_id, subjects).await;
                }
            }
            RouteFrame::Publish {
                subject,
                reply_to,
                payload,
            } => {
                broker
                    .deliver_route_publish(&subject, reply_to.as_deref(), &payload)
                    .await?;
            }
            RouteFrame::Ping => {
                let _ = sender.send(RouteFrame::Pong).await;
            }
            RouteFrame::Pong => {}
        }
    }
    if let Some(node_id) = peer_id {
        mesh.remove_peer(node_id).await;
    }
    writer_task.abort();
    Ok(())
}

async fn read_route_frame<R>(reader: &mut R) -> Result<Option<RouteFrame>>
where
    R: AsyncRead + Unpin,
{
    let mut len = [0_u8; 4];
    match reader.read_exact(&mut len).await {
        Ok(_) => {}
        Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(err) => return Err(err.into()),
    }
    let len = u32::from_be_bytes(len) as usize;
    crate::broker_ensure!(len <= 16 * 1024 * 1024, "route frame too large");
    let mut payload = vec![0; len];
    reader.read_exact(&mut payload).await?;
    serde_json::from_slice(&payload).context("decoding route frame")
}

async fn write_route_frame<W>(writer: &mut W, frame: &RouteFrame) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    let payload = serde_json::to_vec(frame).context("encoding route frame")?;
    crate::broker_ensure!(payload.len() <= u32::MAX as usize, "route frame too large");
    writer
        .write_all(&(payload.len() as u32).to_be_bytes())
        .await?;
    writer.write_all(&payload).await?;
    Ok(())
}

#[cfg(test)]
#[derive(Clone)]
struct FakeClusterRuntime {
    inner: Arc<std::sync::Mutex<FakeClusterState>>,
}

#[cfg(test)]
struct FakeClusterState {
    local_node_id: u64,
    leader: Option<u64>,
    tls_enabled: bool,
    nodes: HashMap<u64, SocketAddr>,
    available_nodes: BTreeSet<u64>,
    state: DurableState,
    writes: usize,
    delay_writes: bool,
    queued_writes: VecDeque<QueuedWrite>,
    next_write_id: u64,
}

#[cfg(test)]
struct QueuedWrite {
    id: u64,
    command: BrokerCommand,
    response: oneshot::Sender<BrokerResponse>,
}

#[cfg(test)]
impl FakeClusterRuntime {
    fn new(node_count: u64, local_node_id: u64, leader: Option<u64>) -> Self {
        assert!(node_count > 0);
        assert!(local_node_id > 0 && local_node_id <= node_count);
        if let Some(leader) = leader {
            assert!(leader > 0 && leader <= node_count);
        }
        let mut nodes = HashMap::new();
        let mut raft_nodes = BTreeMap::new();
        for node_id in 1..=node_count {
            let addr = SocketAddr::from(([127, 0, 0, 1], 10_000 + node_id as u16));
            nodes.insert(node_id, addr);
            raft_nodes.insert(node_id, BasicNode::new(addr));
        }
        Self {
            inner: Arc::new(std::sync::Mutex::new(FakeClusterState {
                local_node_id,
                leader,
                tls_enabled: false,
                available_nodes: nodes.keys().copied().collect(),
                nodes,
                state: DurableState::new(raft_nodes),
                writes: 0,
                delay_writes: false,
                queued_writes: VecDeque::new(),
                next_write_id: 1,
            })),
        }
    }

    async fn client_write(&self, command: BrokerCommand) -> Result<BrokerResponse> {
        let pending = {
            let mut inner = self.inner.lock().unwrap();
            inner.ensure_writable()?;
            if !inner.delay_writes {
                return Ok(inner.apply_command(command));
            }
            let (tx, rx) = oneshot::channel();
            let id = inner.next_write_id;
            inner.next_write_id += 1;
            inner.queued_writes.push_back(QueuedWrite {
                id,
                command,
                response: tx,
            });
            rx
        };
        pending
            .await
            .map_err(|_| BrokerError::msg("queued write canceled"))
    }

    async fn client_write_forwarded(&self, command: BrokerCommand) -> Result<BrokerResponse> {
        let pending = {
            let mut inner = self.inner.lock().unwrap();
            inner.ensure_forwardable()?;
            if !inner.delay_writes {
                return Ok(inner.apply_command(command));
            }
            let (tx, rx) = oneshot::channel();
            let id = inner.next_write_id;
            inner.next_write_id += 1;
            inner.queued_writes.push_back(QueuedWrite {
                id,
                command,
                response: tx,
            });
            rx
        };
        pending
            .await
            .map_err(|_| BrokerError::msg("queued write canceled"))
    }

    fn durable_state(&self) -> DurableState {
        self.inner.lock().unwrap().state.clone()
    }

    async fn is_leader(&self) -> bool {
        let inner = self.inner.lock().unwrap();
        inner.leader == Some(inner.local_node_id)
    }

    async fn current_leader(&self) -> Option<u64> {
        self.inner.lock().unwrap().leader
    }

    async fn leader_client_addr(&self) -> Option<SocketAddr> {
        let inner = self.inner.lock().unwrap();
        let leader = inner.leader?;
        inner.nodes.get(&leader).copied()
    }

    fn tls_enabled(&self) -> bool {
        self.inner.lock().unwrap().tls_enabled
    }

    fn write_count(&self) -> usize {
        self.inner.lock().unwrap().writes
    }

    fn node_count(&self) -> usize {
        self.inner.lock().unwrap().nodes.len()
    }

    fn local_node_id(&self) -> u64 {
        self.inner.lock().unwrap().local_node_id
    }

    fn queued_write_count(&self) -> usize {
        self.inner.lock().unwrap().queued_writes.len()
    }

    fn set_client_addr(&self, node_id: u64, addr: SocketAddr) {
        let mut inner = self.inner.lock().unwrap();
        assert!(inner.nodes.contains_key(&node_id));
        inner.nodes.insert(node_id, addr);
    }

    fn set_leader(&self, leader: Option<u64>) {
        let mut inner = self.inner.lock().unwrap();
        if let Some(leader) = leader {
            assert!(inner.nodes.contains_key(&leader));
        }
        inner.leader = leader;
    }

    fn partition_available(&self, nodes: impl IntoIterator<Item = u64>) {
        let mut inner = self.inner.lock().unwrap();
        let available = nodes.into_iter().collect::<BTreeSet<_>>();
        for node_id in &available {
            assert!(inner.nodes.contains_key(node_id));
        }
        inner.available_nodes = available;
    }

    fn restore_all_nodes(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.available_nodes = inner.nodes.keys().copied().collect();
    }

    fn set_delay_writes(&self, delay_writes: bool) {
        self.inner.lock().unwrap().delay_writes = delay_writes;
    }

    fn drain_one(&self) -> Option<u64> {
        let queued = self.inner.lock().unwrap().queued_writes.pop_front()?;
        let response = {
            let mut inner = self.inner.lock().unwrap();
            inner.apply_command(queued.command)
        };
        let _ = queued.response.send(response);
        Some(queued.id)
    }

    fn drain_all(&self) -> usize {
        let mut drained = 0;
        while self.drain_one().is_some() {
            drained += 1;
        }
        drained
    }
}

#[cfg(test)]
impl FakeClusterState {
    fn quorum_size(&self) -> usize {
        (self.nodes.len() / 2) + 1
    }

    fn ensure_writable(&self) -> Result<()> {
        if self.leader != Some(self.local_node_id) {
            crate::broker_bail!("not leader");
        }
        self.ensure_quorum()
    }

    fn ensure_forwardable(&self) -> Result<()> {
        crate::broker_ensure!(self.leader.is_some(), "not leader");
        self.ensure_quorum()
    }

    fn ensure_quorum(&self) -> Result<()> {
        if !self.available_nodes.contains(&self.local_node_id)
            || self.available_nodes.len() < self.quorum_size()
        {
            crate::broker_bail!("quorum unavailable");
        }
        Ok(())
    }

    fn apply_command(&mut self, command: BrokerCommand) -> BrokerResponse {
        self.writes += 1;
        self.state.apply_command(command)
    }
}

#[cfg(test)]
#[derive(Debug, Default)]
pub(crate) struct ManualClock {
    now_ms: AtomicU64,
}

#[cfg(test)]
impl ManualClock {
    fn new(now_ms: u64) -> Self {
        Self {
            now_ms: AtomicU64::new(now_ms),
        }
    }

    fn advance_ms(&self, millis: u64) {
        self.now_ms.fetch_add(millis, Ordering::Relaxed);
    }
}

#[cfg(test)]
impl Clock for ManualClock {
    fn now_ms(&self) -> u64 {
        self.now_ms.load(Ordering::Relaxed)
    }
}

struct Inner {
    wal: Wal,
    clients: HashMap<u64, Client>,
    consumers: HashMap<String, Consumer>,
    transient_subscriptions: HashMap<(u64, String), TransientSubscription>,
    messages: HashMap<u64, PublishRecord>,
}

struct Client {
    sender: mpsc::Sender<Vec<u8>>,
    remote_addr: Option<SocketAddr>,
    connected_at_ms: u64,
    verbose: bool,
    durable_id: Option<String>,
    authenticated: bool,
    auth_nonce: Option<String>,
    ack_timeout_ms: u64,
    max_in_flight: usize,
}

#[derive(Debug, Clone)]
struct Consumer {
    record: ConsumerRecord,
    members: HashMap<u64, SubscriptionMember>,
    pending: BTreeSet<u64>,
    pending_attempts: HashMap<u64, u32>,
    in_flight: HashMap<u64, InFlight>,
    acked: HashSet<u64>,
    delivered: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SubscriptionMember {
    sid: String,
    remaining_deliveries: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct InFlight {
    delivery_id: u64,
    deadline_ms: u64,
    attempt: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TransientSubscription {
    subject: String,
    sid: String,
    remaining_deliveries: Option<usize>,
}

struct Delivery {
    sender: mpsc::Sender<Vec<u8>>,
    frame: Vec<u8>,
}

#[derive(Debug, serde::Serialize)]
struct ClusterResponse {
    cluster_size: usize,
    cluster_status: &'static str,
    node_id: Option<u64>,
    role: &'static str,
    leader_id: Option<u64>,
    peers: Vec<ClusterPeerResponse>,
    routes: Option<RouteTopologyResponse>,
}

#[derive(Debug, serde::Serialize)]
struct ClusterPeerResponse {
    node_id: u64,
    client_addr: String,
    raft_addr: String,
    is_self: bool,
    is_leader: bool,
}

#[derive(Debug, serde::Serialize)]
struct ConnectionsResponse {
    count: usize,
    connections: Vec<ConnectionResponse>,
}

#[derive(Debug, serde::Serialize)]
struct ConnectionResponse {
    id: u64,
    remote_addr: Option<String>,
    durable_id: Option<String>,
    authenticated: bool,
    verbose: bool,
    connected_at_ms: u64,
    ack_timeout_ms: u64,
    max_in_flight: usize,
    subscriptions: usize,
    transient_subscriptions: usize,
}

#[derive(Debug, serde::Serialize)]
struct SubscriptionsResponse {
    durable_consumers: Vec<DurableConsumerResponse>,
    transient_subscriptions: Vec<TransientSubscriptionResponse>,
}

#[derive(Debug, serde::Serialize)]
struct DurableConsumerResponse {
    consumer_id: String,
    filter_subject: String,
    queue_group: Option<String>,
    members: Vec<ConsumerMemberResponse>,
    pending: usize,
    in_flight: usize,
    acked: usize,
    delivered: usize,
    ack_timeout_ms: u64,
    max_in_flight: usize,
}

#[derive(Debug, serde::Serialize)]
struct ConsumerMemberResponse {
    connection_id: u64,
    sid: String,
    remaining_deliveries: Option<usize>,
}

#[derive(Debug, serde::Serialize)]
struct TransientSubscriptionResponse {
    connection_id: u64,
    sid: String,
    subject: String,
    remaining_deliveries: Option<usize>,
}

impl Broker {
    pub fn open(config: Config) -> Result<Self> {
        Self::open_with_hooks(config, BrokerHooks::default())
    }

    pub(crate) fn open_with_hooks(config: Config, hooks: BrokerHooks) -> Result<Self> {
        config.validate()?;
        let (wal, replay) = Wal::open(&config.wal_dir, config.fsync_interval())?;
        let tls_acceptor = config
            .tls
            .as_ref()
            .map(crate::tls::load_acceptor)
            .transpose()?;
        let consumers = replay
            .consumers
            .into_iter()
            .map(|(id, consumer)| (id, Consumer::from_replay(consumer)))
            .collect();
        let cluster = {
            #[cfg(test)]
            {
                hooks.initial_cluster.clone()
            }
            #[cfg(not(test))]
            {
                None
            }
        };
        let route_mesh = RouteMesh::from_config(&config);
        Ok(Self {
            inner: Arc::new(Mutex::new(Inner {
                wal,
                clients: HashMap::new(),
                consumers,
                transient_subscriptions: HashMap::new(),
                messages: replay.messages,
            })),
            next_connection_id: Arc::new(AtomicU64::new(1)),
            config,
            tls_acceptor,
            cluster: Arc::new(Mutex::new(cluster)),
            route_mesh,
            hooks,
        })
    }

    pub async fn serve(self) -> Result<()> {
        let listener = TcpListener::bind(self.config.listen)
            .await
            .with_context(|| format!("binding {}", self.config.listen))?;
        self.serve_inner(listener, true).await
    }

    pub async fn serve_listener(self, listener: TcpListener) -> Result<()> {
        self.serve_inner(listener, false).await
    }

    async fn serve_inner(self, listener: TcpListener, handle_shutdown: bool) -> Result<()> {
        self.start_cluster().await?;
        self.start_route_mesh().await?;
        self.log_cluster_event("server started").await;
        self.spawn_cluster_log_monitor();
        self.spawn_http_status_listener();
        if self.hooks.start_redelivery_loop {
            let redeliver = self.clone();
            tokio::spawn(async move {
                redeliver.redelivery_loop().await;
            });
        }

        loop {
            if handle_shutdown {
                tokio::select! {
                    accepted = listener.accept() => {
                        self.spawn_accepted(accepted.context("accepting client connection")?.0);
                    }
                    signal = tokio::signal::ctrl_c() => {
                        signal.context("waiting for shutdown signal")?;
                        self.shutdown().await?;
                        return Ok(());
                    }
                }
            } else {
                let (stream, _) = listener
                    .accept()
                    .await
                    .context("accepting client connection")?;
                self.spawn_accepted(stream);
            }
        }
    }

    pub async fn shutdown(&self) -> Result<()> {
        let mut inner = self.inner.lock().await;
        inner.wal.flush()?;
        Ok(())
    }

    pub async fn cluster_leader(&self) -> Option<u64> {
        self.cluster_runtime().await?.current_leader().await
    }

    async fn cluster_response(&self) -> ClusterResponse {
        let cluster_config = self.config.cluster.as_ref();
        let cluster = self.cluster_runtime().await;
        let cluster_size = cluster
            .as_ref()
            .map(ClusterRuntime::cluster_size)
            .or_else(|| cluster_config.map(|cluster| cluster.nodes.len()))
            .unwrap_or(1);
        let node_id = cluster_config
            .map(|cluster| cluster.node_id)
            .or_else(|| cluster.as_ref().map(ClusterRuntime::local_node_id));
        let leader_id = match &cluster {
            Some(cluster) => cluster.current_leader().await,
            None => None,
        };
        let role = match (node_id, leader_id) {
            (None, _) => "standalone",
            (Some(node_id), Some(leader_id)) if node_id == leader_id => "leader",
            (Some(_), Some(_)) => "follower",
            (Some(_), None) => "unknown",
        };
        let cluster_status = if cluster_config.is_none() && cluster.is_none() {
            "standalone"
        } else if leader_id.is_some() {
            "ready"
        } else {
            "forming"
        };
        let peers = cluster_config
            .map(|cluster| {
                cluster
                    .nodes
                    .iter()
                    .map(|peer| ClusterPeerResponse {
                        node_id: peer.node_id,
                        client_addr: peer.client_addr.to_string(),
                        raft_addr: peer.raft_addr.to_string(),
                        is_self: Some(peer.node_id) == node_id,
                        is_leader: Some(peer.node_id) == leader_id,
                    })
                    .collect()
            })
            .unwrap_or_default();
        let routes = match &self.route_mesh {
            Some(route_mesh) => Some(route_mesh.topology_response().await),
            None => None,
        };
        ClusterResponse {
            cluster_size,
            cluster_status,
            node_id,
            role,
            leader_id,
            peers,
            routes,
        }
    }

    async fn connections_response(&self) -> ConnectionsResponse {
        self.inner.lock().await.connections_response()
    }

    async fn subscriptions_response(&self) -> SubscriptionsResponse {
        self.inner.lock().await.subscriptions_response()
    }

    fn spawn_http_status_listener(&self) {
        let Some(listen) = self.config.http_listen else {
            return;
        };
        let broker = self.clone();
        tokio::spawn(async move {
            if let Err(err) = broker.serve_http_status(listen).await {
                error!(error = ?err, "http status error");
            }
        });
    }

    async fn serve_http_status(&self, listen: SocketAddr) -> Result<()> {
        let listener = TcpListener::bind(listen)
            .await
            .with_context(|| format!("binding HTTP status listener {listen}"))?;
        loop {
            let (stream, _) = listener
                .accept()
                .await
                .context("accepting HTTP status connection")?;
            let broker = self.clone();
            tokio::spawn(async move {
                if let Err(err) = broker.handle_http_status(stream).await {
                    error!(error = ?err, "http status connection error");
                }
            });
        }
    }

    async fn handle_http_status(&self, mut stream: TcpStream) -> Result<()> {
        let mut request = Vec::new();
        let mut buf = [0_u8; 1024];
        loop {
            let read = stream
                .read(&mut buf)
                .await
                .context("reading HTTP status request")?;
            if read == 0 {
                return Ok(());
            }
            request.extend_from_slice(&buf[..read]);
            if request.windows(4).any(|window| window == b"\r\n\r\n") || request.len() >= 8 * 1024 {
                break;
            }
        }
        let request_line = request
            .split(|byte| *byte == b'\n')
            .next()
            .and_then(|line| std::str::from_utf8(line).ok())
            .map(str::trim_end)
            .unwrap_or("");
        let Some(path) = http_request_path(request_line) else {
            return write_http_not_found(&mut stream).await;
        };
        match path {
            "/cluster" => {
                let body = serde_json::to_vec(&self.cluster_response().await)
                    .context("serializing HTTP cluster response")?;
                write_http_response(&mut stream, "200 OK", "application/json", &body).await
            }
            "/connections" => {
                let body = serde_json::to_vec(&self.connections_response().await)
                    .context("serializing HTTP connections response")?;
                write_http_response(&mut stream, "200 OK", "application/json", &body).await
            }
            "/subscriptions" => {
                let body = serde_json::to_vec(&self.subscriptions_response().await)
                    .context("serializing HTTP subscriptions response")?;
                write_http_response(&mut stream, "200 OK", "application/json", &body).await
            }
            _ => write_http_not_found(&mut stream).await,
        }
    }

    #[cfg(test)]
    pub(crate) async fn tick_redelivery_for_test(&self) -> Result<()> {
        self.expire_and_redeliver().await
    }

    #[cfg(test)]
    pub(crate) async fn handle_client_for_test<S>(&self, stream: S) -> Result<()>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        self.handle_client(stream).await
    }

    #[cfg(test)]
    pub(crate) async fn handle_accepted_for_test<S>(&self, stream: S) -> Result<()>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let Some(stream) = self.route_cluster_stream(stream).await? else {
            return Ok(());
        };
        self.handle_client(stream).await
    }

    fn spawn_accepted(&self, stream: TcpStream) {
        let broker = self.clone();
        tokio::spawn(async move {
            if let Err(err) = broker.handle_accepted(stream).await {
                error!(error = ?err, "client error");
            }
        });
    }

    async fn handle_accepted(&self, stream: TcpStream) -> Result<()> {
        let remote_addr = stream.peer_addr().ok();
        let Some(stream) = self.route_cluster_stream(stream).await? else {
            return Ok(());
        };
        if let Some(acceptor) = &self.tls_acceptor {
            let timeout_ms = self
                .config
                .tls
                .as_ref()
                .map(|tls| tls.handshake_timeout_ms)
                .unwrap_or(2_000);
            let stream =
                tokio::time::timeout(Duration::from_millis(timeout_ms), acceptor.accept(stream))
                    .await
                    .map_err(|_| BrokerError::msg("TLS handshake timed out"))?
                    .context("accepting TLS client connection")?;
            self.handle_client_with_remote_addr(stream, remote_addr)
                .await
        } else {
            self.handle_client_with_remote_addr(stream, remote_addr)
                .await
        }
    }

    async fn route_cluster_stream<S>(&self, stream: S) -> Result<Option<S>>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        if self.route_mesh.is_some() {
            return Ok(Some(stream));
        }
        if let Some(cluster) = self.cluster_runtime().await {
            if !cluster.is_leader().await {
                if let Some(leader) = cluster.leader_client_addr().await {
                    proxy_stream_to_leader(stream, leader).await?;
                    return Ok(None);
                }
                if cluster.tls_enabled() {
                    return Ok(None);
                }
                let mut stream = stream;
                stream
                    .write_all(&protocol::err("no known leader"))
                    .await
                    .context("writing no-leader error")?;
                return Ok(None);
            }
        }
        Ok(Some(stream))
    }

    #[cfg(test)]
    async fn handle_client<S>(&self, stream: S) -> Result<()>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        self.handle_client_with_remote_addr(stream, None).await
    }

    async fn handle_client_with_remote_addr<S>(
        &self,
        stream: S,
        remote_addr: Option<SocketAddr>,
    ) -> Result<()>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let id = self.next_connection_id.fetch_add(1, Ordering::Relaxed);
        let (reader, mut writer) = tokio::io::split(stream);
        let (sender, mut receiver) = mpsc::channel::<Vec<u8>>(256);
        self.add_client(id, sender, remote_addr).await?;
        let nonce = {
            let inner = self.inner.lock().await;
            inner
                .clients
                .get(&id)
                .and_then(|client| client.auth_nonce.clone())
        };

        writer
            .write_all(&protocol::info_line(
                self.config.max_payload,
                nonce.as_deref(),
            ))
            .await?;
        let writer_task = tokio::spawn(async move {
            while let Some(frame) = receiver.recv().await {
                writer.write_all(&frame).await?;
            }
            Ok::<(), BrokerError>(())
        });

        let mut reader = BufReader::new(reader);
        loop {
            match protocol::read_command(&mut reader, self.config.max_payload).await {
                Ok(Some(command)) => {
                    if let Err(err) = self.handle_command(id, command).await {
                        let _ = self.send_to(id, protocol::err(&err.to_string())).await;
                    }
                }
                Ok(None) => break,
                Err(err) => {
                    let _ = self.send_to(id, protocol::err(&err.to_string())).await;
                    break;
                }
            }
        }

        self.remove_client(id).await?;
        writer_task.abort();
        Ok(())
    }

    async fn handle_command(&self, connection_id: u64, command: Command) -> Result<()> {
        match command {
            Command::Connect {
                verbose,
                durable_id,
                ack_timeout_ms,
                max_in_flight,
                auth,
            } => {
                self.configure_client(
                    connection_id,
                    verbose,
                    durable_id,
                    ack_timeout_ms,
                    max_in_flight,
                    auth,
                )
                .await
            }
            Command::Ping => self.send_to(connection_id, protocol::pong().to_vec()).await,
            Command::Pong => Ok(()),
            Command::Sub {
                subject,
                queue,
                sid,
            } => self.subscribe(connection_id, subject, queue, sid).await,
            Command::Unsub { sid, max_messages } => {
                self.unsubscribe(connection_id, &sid, max_messages).await
            }
            Command::Pub {
                subject,
                reply_to,
                payload,
            } => {
                self.publish(connection_id, subject, reply_to, payload)
                    .await
            }
        }
    }

    async fn add_client(
        &self,
        id: u64,
        sender: mpsc::Sender<Vec<u8>>,
        remote_addr: Option<SocketAddr>,
    ) -> Result<()> {
        let mut inner = self.inner.lock().await;
        inner.clients.insert(
            id,
            Client {
                sender,
                remote_addr,
                connected_at_ms: self.hooks.clock.now_ms(),
                verbose: self.config.verbose,
                durable_id: None,
                authenticated: false,
                auth_nonce: if self.config.auth.enabled {
                    Some(auth::nonce()?)
                } else {
                    None
                },
                ack_timeout_ms: DEFAULT_ACK_TIMEOUT_MS,
                max_in_flight: DEFAULT_MAX_IN_FLIGHT,
            },
        );
        Ok(())
    }

    async fn configure_client(
        &self,
        id: u64,
        verbose: bool,
        durable_id: Option<String>,
        ack_timeout_ms: Option<u64>,
        max_in_flight: Option<usize>,
        auth: Option<ConnectAuth>,
    ) -> Result<()> {
        let mut inner = self.inner.lock().await;
        let client = inner
            .clients
            .get_mut(&id)
            .ok_or_else(|| BrokerError::msg("unknown connection"))?;
        let durable_id = if self.config.auth.enabled {
            let nonce = client
                .auth_nonce
                .as_deref()
                .ok_or_else(|| BrokerError::msg("missing auth nonce"))?;
            let auth = auth
                .as_ref()
                .ok_or_else(|| BrokerError::msg("CONNECT client_id and signature are required"))?;
            let public_key = self
                .config
                .auth
                .clients
                .get(&auth.client_id)
                .ok_or_else(|| BrokerError::msg("unknown client_id"))?;
            let client_id = auth::verify(auth, nonce, public_key)?;
            if let Some(durable_id) = durable_id {
                crate::broker_ensure!(
                    durable_id == client_id,
                    "CONNECT durable_id must match authenticated client_id"
                );
            }
            client.authenticated = true;
            Some(client_id)
        } else {
            durable_id
        };
        client.verbose = verbose || self.config.verbose;
        client.durable_id = durable_id;
        client.ack_timeout_ms = ack_timeout_ms.unwrap_or(DEFAULT_ACK_TIMEOUT_MS);
        client.max_in_flight = max_in_flight.unwrap_or(DEFAULT_MAX_IN_FLIGHT);
        crate::broker_ensure!(
            client.ack_timeout_ms > 0,
            "ack_timeout_ms must be greater than zero"
        );
        crate::broker_ensure!(
            client.max_in_flight > 0,
            "max_in_flight must be greater than zero"
        );
        Ok(())
    }

    async fn subscribe(
        &self,
        connection_id: u64,
        sub_subject: String,
        queue: Option<String>,
        sid: String,
    ) -> Result<()> {
        crate::broker_ensure!(
            subject::validate_subscription(&sub_subject),
            "invalid subscription subject"
        );
        protocol::validate_identifier("sid", &sid)?;
        if let Some(queue) = &queue {
            protocol::validate_identifier("queue group", queue)?;
        }

        let durable_record = {
            let mut inner = self.inner.lock().await;
            let client = inner
                .clients
                .get(&connection_id)
                .ok_or_else(|| BrokerError::msg("unknown connection"))?;
            if is_inbox_subscription(&sub_subject) || client.durable_id.is_none() {
                crate::broker_ensure!(
                    queue.is_none(),
                    "transient subscriptions do not support queue groups"
                );
                inner.transient_subscriptions.insert(
                    (connection_id, sid.clone()),
                    TransientSubscription {
                        subject: sub_subject,
                        sid,
                        remaining_deliveries: None,
                    },
                );
                None
            } else {
                if let Some(durable_id) = &client.durable_id {
                    let consumer_id = consumer_id(durable_id, queue.as_deref(), &sub_subject, &sid);
                    let record = ConsumerRecord {
                        consumer_id: consumer_id.clone(),
                        filter_subject: sub_subject,
                        queue_group: queue,
                        ack_timeout_ms: client.ack_timeout_ms,
                        max_in_flight: client.max_in_flight,
                    };
                    Some((consumer_id, record, sid))
                } else {
                    crate::broker_bail!("CONNECT durable_id is required before SUB")
                }
            }
        };

        if let Some((_consumer_id, record, sid)) = durable_record {
            if let Some(cluster) = self.cluster_runtime().await {
                self.cluster_write(
                    &cluster,
                    BrokerCommand::ConsumerUpsert {
                        record: record.clone(),
                    },
                )
                .await?;
                self.sync_from_cluster(&cluster).await;
            } else {
                let mut inner = self.inner.lock().await;
                inner.wal.append_consumer_upsert(&record)?;
                inner.wal.flush_due()?;
                inner.upsert_consumer(record.clone());
            }
            let mut inner = self.inner.lock().await;
            let consumer = inner.upsert_consumer(record);
            consumer.members.insert(
                connection_id,
                SubscriptionMember {
                    sid,
                    remaining_deliveries: None,
                },
            );
            drop(inner);
            self.deliver_pending().await?;
        }
        self.sync_route_interests().await;
        Ok(())
    }

    async fn unsubscribe(
        &self,
        connection_id: u64,
        sid: &str,
        max_messages: Option<usize>,
    ) -> Result<()> {
        if let Some(max_messages) = max_messages {
            crate::broker_ensure!(
                max_messages > 0,
                "UNSUB max_messages must be greater than zero"
            );
        }
        let mut inner = self.inner.lock().await;
        let mut found = false;
        if let Some(subscription) = inner
            .transient_subscriptions
            .get_mut(&(connection_id, sid.to_string()))
        {
            found = true;
            if let Some(max_messages) = max_messages {
                subscription.remaining_deliveries = Some(max_messages);
            } else {
                inner
                    .transient_subscriptions
                    .remove(&(connection_id, sid.to_string()));
            }
        }
        for consumer in inner.consumers.values_mut() {
            if consumer
                .members
                .get(&connection_id)
                .is_some_and(|member| member.sid == sid)
            {
                if let Some(max_messages) = max_messages {
                    if let Some(member) = consumer.members.get_mut(&connection_id) {
                        member.remaining_deliveries = Some(max_messages);
                    }
                } else {
                    consumer.members.remove(&connection_id);
                }
                found = true;
            }
        }
        crate::broker_ensure!(found, "unknown sid");
        drop(inner);
        self.sync_route_interests().await;
        Ok(())
    }

    async fn publish(
        &self,
        publisher_id: u64,
        subject_name: String,
        reply_to: Option<String>,
        payload: Vec<u8>,
    ) -> Result<()> {
        if let Some(ack) = protocol::parse_ack_subject(&subject_name) {
            self.ack(ack).await?;
            self.send_verbose_ok(publisher_id).await?;
            return Ok(());
        }
        crate::broker_ensure!(
            !subject_name.starts_with("_BROKER."),
            "reserved broker subject"
        );
        crate::broker_ensure!(
            subject::validate_subject(&subject_name),
            "invalid publish subject"
        );
        crate::broker_ensure!(
            payload.len() <= self.config.max_payload,
            "payload exceeds max payload"
        );

        let (transient_deliveries, verbose) = {
            let mut inner = self.inner.lock().await;
            let verbose = inner
                .clients
                .get(&publisher_id)
                .map(|client| client.verbose)
                .unwrap_or(self.config.verbose);
            (
                inner.prepare_transient_deliveries(&subject_name, reply_to.as_deref(), &payload),
                verbose,
            )
        };
        for delivery in transient_deliveries {
            let _ = delivery.sender.send(delivery.frame).await;
        }
        self.sync_route_interests().await;
        if let Some(route_mesh) = &self.route_mesh {
            route_mesh
                .forward_publish(&subject_name, reply_to.as_deref(), &payload)
                .await;
        }

        if is_inbox_publish(&subject_name) {
            if verbose {
                self.send_to(publisher_id, protocol::ok().to_vec()).await?;
            }
            return Ok(());
        }

        if let Some(cluster) = self.cluster_runtime().await {
            let has_durable = {
                let inner = self.inner.lock().await;
                inner.has_matching_durable_consumer(&subject_name)
            };
            if !has_durable {
                if verbose {
                    self.send_to(publisher_id, protocol::ok().to_vec()).await?;
                }
                return Ok(());
            }
            self.cluster_write(
                &cluster,
                BrokerCommand::Publish {
                    subject: subject_name,
                    reply_to,
                    payload,
                },
            )
            .await?;
            self.sync_from_cluster(&cluster).await;
            self.deliver_pending().await?;
            if verbose {
                self.send_to(publisher_id, protocol::ok().to_vec()).await?;
            }
            return Ok(());
        }

        let has_durable = {
            let mut inner = self.inner.lock().await;
            let matching_consumers = inner.matching_durable_consumers(&subject_name);
            let has_durable = !matching_consumers.is_empty();
            if has_durable {
                let record =
                    inner
                        .wal
                        .append_publish(&subject_name, reply_to.as_deref(), &payload)?;
                for consumer_id in matching_consumers {
                    if let Some(consumer) = inner.consumers.get_mut(&consumer_id) {
                        consumer.pending.insert(record.seq);
                    }
                }
                inner.messages.insert(record.seq, record);
            } else {
                inner.wal.flush_due()?;
            }

            has_durable
        };

        if has_durable {
            match self.hooks.durable_publish_flush_mode {
                DurablePublishFlushMode::SleepThenFlush => {
                    tokio::time::sleep(self.config.fsync_interval()).await;
                    let mut inner = self.inner.lock().await;
                    inner.wal.flush()?;
                }
                #[cfg(test)]
                DurablePublishFlushMode::FlushImmediately => {
                    let mut inner = self.inner.lock().await;
                    inner.wal.flush()?;
                }
            }
        }

        self.deliver_pending().await?;

        if verbose {
            self.send_to(publisher_id, protocol::ok().to_vec()).await?;
        }

        Ok(())
    }

    async fn deliver_route_publish(
        &self,
        subject_name: &str,
        reply_to: Option<&str>,
        payload: &[u8],
    ) -> Result<()> {
        crate::broker_ensure!(
            subject::validate_subject(subject_name),
            "invalid route publish subject"
        );
        crate::broker_ensure!(
            payload.len() <= self.config.max_payload,
            "route payload exceeds max payload"
        );
        let deliveries = {
            let mut inner = self.inner.lock().await;
            inner.prepare_transient_deliveries(subject_name, reply_to, payload)
        };
        for delivery in deliveries {
            let _ = delivery.sender.send(delivery.frame).await;
        }
        self.sync_route_interests().await;
        Ok(())
    }

    async fn ack(&self, ack: AckSubject) -> Result<()> {
        if let Some(cluster) = self.cluster_runtime().await {
            self.cluster_write(
                &cluster,
                BrokerCommand::Ack {
                    seq: ack.seq,
                    consumer_id: ack.consumer_id,
                    delivery_id: ack.delivery_id,
                },
            )
            .await?;
            self.sync_from_cluster(&cluster).await;
            return Ok(());
        }
        let mut inner = self.inner.lock().await;
        let mut should_cleanup = false;
        let valid = inner
            .consumers
            .get(&ack.consumer_id)
            .and_then(|consumer| consumer.in_flight.get(&ack.seq))
            .is_some_and(|in_flight| in_flight.delivery_id == ack.delivery_id);
        if valid {
            inner
                .wal
                .append_ack(ack.seq, &ack.consumer_id, ack.delivery_id)?;
            let consumer = inner.consumers.get_mut(&ack.consumer_id).unwrap();
            consumer.in_flight.remove(&ack.seq);
            consumer.pending.remove(&ack.seq);
            consumer.pending_attempts.remove(&ack.seq);
            consumer.acked.insert(ack.seq);
            should_cleanup = true;
        }
        inner.wal.flush_due()?;
        if should_cleanup {
            inner.cleanup_acked_messages();
        }
        Ok(())
    }

    async fn send_verbose_ok(&self, publisher_id: u64) -> Result<()> {
        let verbose = {
            let inner = self.inner.lock().await;
            inner
                .clients
                .get(&publisher_id)
                .map(|client| client.verbose)
                .unwrap_or(self.config.verbose)
        };
        if verbose {
            self.send_to(publisher_id, protocol::ok().to_vec()).await?;
        }
        Ok(())
    }

    async fn deliver_pending(&self) -> Result<()> {
        if let Some(cluster) = self.cluster_runtime().await {
            return self.deliver_pending_clustered(cluster).await;
        }
        let deliveries = {
            let mut inner = self.inner.lock().await;
            inner.prepare_durable_deliveries(self.hooks.clock.now_ms())?
        };

        for delivery in deliveries {
            let _ = delivery.sender.send(delivery.frame).await;
        }
        Ok(())
    }

    async fn redelivery_loop(self) {
        let mut interval =
            tokio::time::interval(Duration::from_millis(REDELIVERY_SCAN_INTERVAL_MS));
        loop {
            interval.tick().await;
            if let Err(err) = self.expire_and_redeliver().await {
                error!(error = ?err, "redelivery error");
            }
        }
    }

    async fn expire_and_redeliver(&self) -> Result<()> {
        if self.cluster_runtime().await.is_some() {
            return self.deliver_pending().await;
        }
        {
            let mut inner = self.inner.lock().await;
            let now = self.hooks.clock.now_ms();
            for consumer in inner.consumers.values_mut() {
                let expired: Vec<_> = consumer
                    .in_flight
                    .iter()
                    .filter(|(_, in_flight)| in_flight.deadline_ms <= now)
                    .map(|(seq, _)| *seq)
                    .collect();
                for seq in expired {
                    if let Some(in_flight) = consumer.in_flight.remove(&seq) {
                        consumer.pending.insert(seq);
                        consumer
                            .pending_attempts
                            .insert(seq, in_flight.attempt.saturating_add(1));
                    }
                }
            }
        }
        self.deliver_pending().await
    }

    async fn send_to(&self, connection_id: u64, frame: Vec<u8>) -> Result<()> {
        let sender = {
            let inner = self.inner.lock().await;
            inner
                .clients
                .get(&connection_id)
                .map(|client| client.sender.clone())
                .ok_or_else(|| BrokerError::msg("unknown connection"))?
        };
        sender
            .send(frame)
            .await
            .map_err(|_| BrokerError::msg("connection closed"))
    }

    async fn remove_client(&self, connection_id: u64) -> Result<()> {
        let mut inner = self.inner.lock().await;
        inner.clients.remove(&connection_id);
        inner
            .transient_subscriptions
            .retain(|(client_id, _), _| *client_id != connection_id);
        for consumer in inner.consumers.values_mut() {
            consumer.members.remove(&connection_id);
        }
        drop(inner);
        self.sync_route_interests().await;
        Ok(())
    }

    async fn start_cluster(&self) -> Result<()> {
        let Some(cluster_config) = &self.config.cluster else {
            return Ok(());
        };
        let runtime = RaftRuntime::open(cluster_config, self.tls_acceptor.is_some()).await?;
        runtime.spawn_listener(cluster_config.raft_listen);
        let runtime = ClusterRuntime::real(runtime);
        self.sync_from_cluster(&runtime).await;
        *self.cluster.lock().await = Some(runtime);
        Ok(())
    }

    async fn start_route_mesh(&self) -> Result<()> {
        let Some(route_mesh) = &self.route_mesh else {
            return Ok(());
        };
        route_mesh.start(self.clone()).await?;
        self.sync_route_interests().await;
        Ok(())
    }

    fn spawn_cluster_log_monitor(&self) {
        if self.config.cluster.is_none() {
            return;
        }
        let broker = self.clone();
        tokio::spawn(async move {
            broker.cluster_log_monitor().await;
        });
    }

    async fn cluster_log_monitor(self) {
        let mut previous_leader = self.current_leader_for_log().await;
        let mut full_mesh_formed = false;
        let mut interval =
            tokio::time::interval(Duration::from_millis(CLUSTER_LOG_SCAN_INTERVAL_MS));
        loop {
            interval.tick().await;
            let leader = self.current_leader_for_log().await;
            if leader != previous_leader {
                self.log_cluster_event("cluster leader changed").await;
                previous_leader = leader;
            }

            let Some(route_mesh) = &self.route_mesh else {
                continue;
            };
            let cluster_size = self.cluster_size_for_log().await;
            let formed = cluster_size > 1
                && route_mesh.connected_peer_count().await >= cluster_size.saturating_sub(1);
            if formed && !full_mesh_formed {
                self.log_cluster_event("full member cluster formed").await;
            }
            full_mesh_formed = formed;
        }
    }

    async fn log_cluster_event(&self, event: &str) {
        let cluster_size = self.cluster_size_for_log().await;
        let leader_id = format_leader_id(self.current_leader_for_log().await);
        info!(event, cluster_size, leader_id, "cluster lifecycle");
    }

    async fn cluster_size_for_log(&self) -> usize {
        self.cluster_runtime()
            .await
            .map(|cluster| cluster.cluster_size())
            .or_else(|| {
                self.config
                    .cluster
                    .as_ref()
                    .map(|cluster| cluster.nodes.len())
            })
            .unwrap_or(1)
    }

    async fn current_leader_for_log(&self) -> Option<u64> {
        self.cluster_runtime().await?.current_leader().await
    }

    async fn cluster_runtime(&self) -> Option<ClusterRuntime> {
        self.cluster.lock().await.clone()
    }

    async fn sync_from_cluster(&self, cluster: &ClusterRuntime) {
        let state = cluster.durable_state();
        let mut inner = self.inner.lock().await;
        inner.sync_durable_state(state);
    }

    async fn sync_route_interests(&self) {
        let Some(route_mesh) = &self.route_mesh else {
            return;
        };
        let interests = {
            let inner = self.inner.lock().await;
            inner.route_interests()
        };
        route_mesh.set_local_interests(interests).await;
    }

    async fn cluster_write(
        &self,
        cluster: &ClusterRuntime,
        command: BrokerCommand,
    ) -> Result<BrokerResponse> {
        if self.route_mesh.is_some() {
            cluster.client_write_forwarded(command).await
        } else {
            cluster.client_write(command).await
        }
    }

    async fn deliver_pending_clustered(&self, cluster: ClusterRuntime) -> Result<()> {
        loop {
            let candidate = {
                let inner = self.inner.lock().await;
                inner.next_cluster_delivery(self.hooks.clock.now_ms())
            };
            let Some(candidate) = candidate else {
                break;
            };
            let response = self
                .cluster_write(
                    &cluster,
                    BrokerCommand::DeliveryAttempt {
                        seq: candidate.seq,
                        consumer_id: candidate.consumer_id.clone(),
                        deadline_ms: candidate.deadline_ms,
                        attempt: candidate.attempt,
                    },
                )
                .await?;
            self.sync_from_cluster(&cluster).await;
            let crate::raft::BrokerResponse::DeliveryAttempt {
                record: Some(record),
            } = response
            else {
                continue;
            };
            let delivery = {
                let mut inner = self.inner.lock().await;
                inner.delivery_for_record(&record, candidate.connection_id, &candidate.sid)
            };
            if let Some(delivery) = delivery {
                let _ = delivery.sender.send(delivery.frame).await;
            }
        }
        Ok(())
    }
}

impl Inner {
    fn connections_response(&self) -> ConnectionsResponse {
        let mut connections = self
            .clients
            .iter()
            .map(|(id, client)| {
                let subscriptions = self
                    .consumers
                    .values()
                    .filter(|consumer| consumer.members.contains_key(id))
                    .count();
                let transient_subscriptions = self
                    .transient_subscriptions
                    .keys()
                    .filter(|(connection_id, _)| connection_id == id)
                    .count();
                ConnectionResponse {
                    id: *id,
                    remote_addr: client.remote_addr.map(|addr| addr.to_string()),
                    durable_id: client.durable_id.clone(),
                    authenticated: client.authenticated,
                    verbose: client.verbose,
                    connected_at_ms: client.connected_at_ms,
                    ack_timeout_ms: client.ack_timeout_ms,
                    max_in_flight: client.max_in_flight,
                    subscriptions,
                    transient_subscriptions,
                }
            })
            .collect::<Vec<_>>();
        connections.sort_by_key(|connection| connection.id);
        ConnectionsResponse {
            count: connections.len(),
            connections,
        }
    }

    fn subscriptions_response(&self) -> SubscriptionsResponse {
        let mut durable_consumers = self
            .consumers
            .iter()
            .map(|(consumer_id, consumer)| {
                let mut members = consumer
                    .members
                    .iter()
                    .map(|(connection_id, member)| ConsumerMemberResponse {
                        connection_id: *connection_id,
                        sid: member.sid.clone(),
                        remaining_deliveries: member.remaining_deliveries,
                    })
                    .collect::<Vec<_>>();
                members.sort_by_key(|member| (member.connection_id, member.sid.clone()));
                DurableConsumerResponse {
                    consumer_id: consumer_id.clone(),
                    filter_subject: consumer.record.filter_subject.clone(),
                    queue_group: consumer.record.queue_group.clone(),
                    members,
                    pending: consumer.pending.len(),
                    in_flight: consumer.in_flight.len(),
                    acked: consumer.acked.len(),
                    delivered: consumer.delivered,
                    ack_timeout_ms: consumer.record.ack_timeout_ms,
                    max_in_flight: consumer.record.max_in_flight,
                }
            })
            .collect::<Vec<_>>();
        durable_consumers.sort_by(|left, right| left.consumer_id.cmp(&right.consumer_id));
        let mut transient_subscriptions = self
            .transient_subscriptions
            .iter()
            .map(
                |((connection_id, _), subscription)| TransientSubscriptionResponse {
                    connection_id: *connection_id,
                    sid: subscription.sid.clone(),
                    subject: subscription.subject.clone(),
                    remaining_deliveries: subscription.remaining_deliveries,
                },
            )
            .collect::<Vec<_>>();
        transient_subscriptions
            .sort_by_key(|subscription| (subscription.connection_id, subscription.sid.clone()));
        SubscriptionsResponse {
            durable_consumers,
            transient_subscriptions,
        }
    }

    fn route_interests(&self) -> Vec<String> {
        let mut interests = self
            .transient_subscriptions
            .values()
            .map(|subscription| subscription.subject.clone())
            .collect::<Vec<_>>();
        interests.sort();
        interests.dedup();
        interests
    }

    fn has_matching_durable_consumer(&self, subject_name: &str) -> bool {
        self.consumers
            .values()
            .any(|consumer| subject::matches(&consumer.record.filter_subject, subject_name))
    }

    fn matching_durable_consumers(&self, subject_name: &str) -> Vec<String> {
        self.consumers
            .iter()
            .filter(|(_, consumer)| subject::matches(&consumer.record.filter_subject, subject_name))
            .map(|(consumer_id, _)| consumer_id.clone())
            .collect()
    }

    fn upsert_consumer(&mut self, record: ConsumerRecord) -> &mut Consumer {
        let consumer_id = record.consumer_id.clone();
        let consumer = self
            .consumers
            .entry(consumer_id)
            .or_insert_with(|| Consumer {
                record: record.clone(),
                members: HashMap::new(),
                pending: BTreeSet::new(),
                pending_attempts: HashMap::new(),
                in_flight: HashMap::new(),
                acked: HashSet::new(),
                delivered: 0,
            });
        consumer.record = record;
        consumer
    }

    fn prepare_transient_deliveries(
        &mut self,
        subject_name: &str,
        reply_to: Option<&str>,
        payload: &[u8],
    ) -> Vec<Delivery> {
        let matched = self
            .transient_subscriptions
            .iter()
            .filter(|(_, subscription)| subject::matches(&subscription.subject, subject_name))
            .filter_map(|((connection_id, _), subscription)| {
                let client = self.clients.get(connection_id)?;
                Some((
                    *connection_id,
                    subscription.sid.clone(),
                    Delivery {
                        sender: client.sender.clone(),
                        frame: protocol::msg(subject_name, &subscription.sid, reply_to, payload),
                    },
                ))
            })
            .collect::<Vec<_>>();
        for (connection_id, sid, _) in &matched {
            self.decrement_transient_subscription(*connection_id, sid);
        }
        matched
            .into_iter()
            .map(|(_, _, delivery)| delivery)
            .collect()
    }

    fn prepare_durable_deliveries(&mut self, now: u64) -> Result<Vec<Delivery>> {
        let mut deliveries = Vec::new();
        let consumer_ids: Vec<_> = self.consumers.keys().cloned().collect();
        for consumer_id in consumer_ids {
            loop {
                let Some((seq, connection_id, sid, attempt, deadline_ms)) =
                    self.next_delivery_for(&consumer_id, now)
                else {
                    break;
                };
                let Some(message) = self.messages.get(&seq).cloned() else {
                    if let Some(consumer) = self.consumers.get_mut(&consumer_id) {
                        consumer.pending.remove(&seq);
                    }
                    continue;
                };
                let delivery =
                    self.wal
                        .append_delivery_attempt(seq, &consumer_id, deadline_ms, attempt)?;
                let ack_subject = protocol::ack_subject(&consumer_id, seq, delivery.delivery_id);
                if let Some(consumer) = self.consumers.get_mut(&consumer_id) {
                    consumer.pending.remove(&seq);
                    consumer.pending_attempts.remove(&seq);
                    consumer.in_flight.insert(
                        seq,
                        InFlight {
                            delivery_id: delivery.delivery_id,
                            deadline_ms: delivery.deadline_ms,
                            attempt: delivery.attempt,
                        },
                    );
                    consumer.delivered += 1;
                }
                if let Some(client) = self.clients.get(&connection_id) {
                    let frame = match message.reply_to.as_deref() {
                        Some(reply_to) => protocol::hmsg(
                            &message.subject,
                            &sid,
                            Some(reply_to),
                            &[("Broker-Ack", &ack_subject)],
                            &message.payload,
                        ),
                        None => protocol::msg(
                            &message.subject,
                            &sid,
                            Some(&ack_subject),
                            &message.payload,
                        ),
                    };
                    deliveries.push(Delivery {
                        sender: client.sender.clone(),
                        frame,
                    });
                    self.decrement_durable_member(&consumer_id, connection_id);
                }
            }
        }
        self.wal.flush_due()?;
        Ok(deliveries)
    }

    fn next_delivery_for(
        &self,
        consumer_id: &str,
        now: u64,
    ) -> Option<(u64, u64, String, u32, u64)> {
        let consumer = self.consumers.get(consumer_id)?;
        if consumer.in_flight.len() >= consumer.record.max_in_flight || consumer.members.is_empty()
        {
            return None;
        }
        let seq = *consumer.pending.iter().next()?;
        let (connection_id, member) = consumer
            .members
            .iter()
            .filter(|(connection_id, _)| self.clients.contains_key(connection_id))
            .min_by_key(|(connection_id, _)| **connection_id)?;
        let attempt = consumer.pending_attempts.get(&seq).copied().unwrap_or(1);
        let deadline_ms = now.saturating_add(consumer.record.ack_timeout_ms);
        Some((
            seq,
            *connection_id,
            member.sid.clone(),
            attempt,
            deadline_ms,
        ))
    }

    fn next_cluster_delivery(&self, now: u64) -> Option<ClusterDeliveryCandidate> {
        for consumer_id in self.consumers.keys() {
            let consumer = self.consumers.get(consumer_id)?;
            if consumer.in_flight.len() >= consumer.record.max_in_flight
                || consumer.members.is_empty()
            {
                continue;
            }
            let seq = consumer.pending.iter().next().copied().or_else(|| {
                consumer
                    .in_flight
                    .iter()
                    .filter(|(_, in_flight)| in_flight.deadline_ms <= now)
                    .map(|(seq, _)| *seq)
                    .min()
            })?;
            let (connection_id, member) = consumer
                .members
                .iter()
                .filter(|(connection_id, _)| self.clients.contains_key(connection_id))
                .min_by_key(|(connection_id, _)| **connection_id)?;
            let attempt = consumer
                .in_flight
                .get(&seq)
                .map(|in_flight| in_flight.attempt.saturating_add(1))
                .or_else(|| consumer.pending_attempts.get(&seq).copied())
                .unwrap_or(1);
            let deadline_ms = now.saturating_add(consumer.record.ack_timeout_ms);
            return Some(ClusterDeliveryCandidate {
                consumer_id: consumer_id.clone(),
                seq,
                connection_id: *connection_id,
                sid: member.sid.clone(),
                attempt,
                deadline_ms,
            });
        }
        None
    }

    fn delivery_for_record(
        &mut self,
        record: &crate::wal::DeliveryAttemptRecord,
        connection_id: u64,
        sid: &str,
    ) -> Option<Delivery> {
        let message = self.messages.get(&record.seq)?.clone();
        let client = self.clients.get(&connection_id)?;
        if let Some(consumer) = self.consumers.get_mut(&record.consumer_id) {
            consumer.delivered += 1;
        }
        let ack_subject =
            protocol::ack_subject(&record.consumer_id, record.seq, record.delivery_id);
        let frame = match message.reply_to.as_deref() {
            Some(reply_to) => protocol::hmsg(
                &message.subject,
                sid,
                Some(reply_to),
                &[("Broker-Ack", &ack_subject)],
                &message.payload,
            ),
            None => protocol::msg(&message.subject, sid, Some(&ack_subject), &message.payload),
        };
        let delivery = Delivery {
            sender: client.sender.clone(),
            frame,
        };
        self.decrement_durable_member(&record.consumer_id, connection_id);
        Some(delivery)
    }

    fn sync_durable_state(&mut self, state: DurableState) {
        self.messages = state.messages;
        let mut next = HashMap::new();
        for (consumer_id, durable) in state.consumers {
            let (members, delivered) = self
                .consumers
                .remove(&consumer_id)
                .map(|consumer| (consumer.members, consumer.delivered))
                .unwrap_or_default();
            next.insert(
                consumer_id,
                Consumer {
                    record: durable.record,
                    members,
                    pending: durable.pending,
                    pending_attempts: durable.pending_attempts,
                    in_flight: durable
                        .in_flight
                        .into_iter()
                        .map(|(seq, attempt)| {
                            (
                                seq,
                                InFlight {
                                    delivery_id: attempt.delivery_id,
                                    deadline_ms: attempt.deadline_ms,
                                    attempt: attempt.attempt,
                                },
                            )
                        })
                        .collect(),
                    acked: durable.acked,
                    delivered,
                },
            );
        }
        self.consumers = next;
    }

    fn cleanup_acked_messages(&mut self) {
        let removable: Vec<_> = self
            .messages
            .iter()
            .filter(|(seq, _)| {
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
            .map(|(seq, _)| *seq)
            .collect();
        for seq in removable {
            self.messages.remove(&seq);
        }
    }

    fn decrement_transient_subscription(&mut self, connection_id: u64, sid: &str) {
        let key = (connection_id, sid.to_string());
        let should_remove = self
            .transient_subscriptions
            .get_mut(&key)
            .and_then(|subscription| decrement_remaining(&mut subscription.remaining_deliveries))
            .unwrap_or(false);
        if should_remove {
            self.transient_subscriptions.remove(&key);
        }
    }

    fn decrement_durable_member(&mut self, consumer_id: &str, connection_id: u64) {
        let should_remove = self
            .consumers
            .get_mut(consumer_id)
            .and_then(|consumer| consumer.members.get_mut(&connection_id))
            .and_then(|member| decrement_remaining(&mut member.remaining_deliveries))
            .unwrap_or(false);
        if should_remove {
            if let Some(consumer) = self.consumers.get_mut(consumer_id) {
                consumer.members.remove(&connection_id);
            }
        }
    }
}

fn decrement_remaining(remaining: &mut Option<usize>) -> Option<bool> {
    let remaining = remaining.as_mut()?;
    *remaining = remaining.saturating_sub(1);
    Some(*remaining == 0)
}

fn http_request_path(request_line: &str) -> Option<&str> {
    let mut parts = request_line.split_whitespace();
    let method = parts.next()?;
    let path = parts.next()?;
    let version = parts.next()?;
    if parts.next().is_some() || method != "GET" || (version != "HTTP/1.1" && version != "HTTP/1.0")
    {
        return None;
    }
    Some(path)
}

async fn write_http_not_found<W>(writer: &mut W) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    write_http_response(
        writer,
        "404 Not Found",
        "application/json",
        br#"{"error":"not found"}"#,
    )
    .await
}

async fn write_http_response<W>(
    writer: &mut W,
    status: &str,
    content_type: &str,
    body: &[u8],
) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    let header = format!(
        "HTTP/1.1 {status}\r\ncontent-type: {content_type}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        body.len()
    );
    writer.write_all(header.as_bytes()).await?;
    writer.write_all(body).await?;
    Ok(())
}

struct ClusterDeliveryCandidate {
    consumer_id: String,
    seq: u64,
    connection_id: u64,
    sid: String,
    attempt: u32,
    deadline_ms: u64,
}

impl Consumer {
    fn from_replay(replay: ReplayedConsumer) -> Self {
        Self {
            record: replay.record,
            members: HashMap::new(),
            pending: replay.pending,
            pending_attempts: HashMap::new(),
            in_flight: replay
                .in_flight
                .into_iter()
                .map(|(seq, attempt)| {
                    (
                        seq,
                        InFlight {
                            delivery_id: attempt.delivery_id,
                            deadline_ms: attempt.deadline_ms,
                            attempt: attempt.attempt,
                        },
                    )
                })
                .collect(),
            acked: replay.acked,
            delivered: 0,
        }
    }
}

fn consumer_id(durable_id: &str, queue: Option<&str>, subject: &str, sid: &str) -> String {
    match queue {
        Some(queue) => format!("queue-{queue}-{}", hex(subject.as_bytes())),
        None => format!("durable-{durable_id}-{sid}"),
    }
}

fn is_inbox_subscription(subject: &str) -> bool {
    subject == "_INBOX.>" || subject.starts_with("_INBOX.")
}

fn is_inbox_publish(subject: &str) -> bool {
    subject.starts_with("_INBOX.")
}

fn hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn format_leader_id(leader_id: Option<u64>) -> String {
    leader_id
        .map(|leader_id| leader_id.to_string())
        .unwrap_or_else(|| "none".to_string())
}

#[cfg(test)]
mod tests {
    use std::{path::Path, sync::Arc, time::Duration};

    use crate::config::{ClusterConfig, ClusterNodeConfig};
    use tempfile::TempDir;
    use tokio::{
        io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader, DuplexStream},
        sync::mpsc,
        task::JoinHandle,
    };

    use super::*;

    struct Scenario {
        _dir: TempDir,
        clock: Arc<ManualClock>,
        broker: Broker,
        fake_cluster: Option<FakeClusterRuntime>,
    }

    impl Scenario {
        fn new() -> Self {
            let dir = TempDir::new().unwrap();
            let clock = Arc::new(ManualClock::new(1_000));
            let broker = deterministic_broker(test_config(dir.path()), clock.clone(), None);
            Self {
                _dir: dir,
                clock,
                broker,
                fake_cluster: None,
            }
        }

        fn new_fake_cluster(node_count: u64) -> Self {
            Self::new_fake_cluster_local_node(node_count, 1, Some(1))
        }

        fn new_fake_cluster_local_node(
            node_count: u64,
            local_node_id: u64,
            leader: Option<u64>,
        ) -> Self {
            Self::new_fake_cluster_local_node_with_routes(node_count, local_node_id, leader, false)
        }

        fn new_fake_route_cluster_local_node(
            node_count: u64,
            local_node_id: u64,
            leader: Option<u64>,
        ) -> Self {
            Self::new_fake_cluster_local_node_with_routes(node_count, local_node_id, leader, true)
        }

        fn new_fake_cluster_local_node_with_routes(
            node_count: u64,
            local_node_id: u64,
            leader: Option<u64>,
            route_mesh: bool,
        ) -> Self {
            let dir = TempDir::new().unwrap();
            let clock = Arc::new(ManualClock::new(1_000));
            let fake_cluster = FakeClusterRuntime::new(node_count, local_node_id, leader);
            let mut config = test_config(dir.path());
            if route_mesh {
                config.cluster = Some(fake_cluster_config(dir.path(), node_count, local_node_id));
            }
            let broker = deterministic_broker(
                config,
                clock.clone(),
                Some(ClusterRuntime::Fake(fake_cluster.clone())),
            );
            Self {
                _dir: dir,
                clock,
                broker,
                fake_cluster: Some(fake_cluster),
            }
        }

        fn broker(&self) -> &Broker {
            &self.broker
        }

        fn fake_cluster(&self) -> &FakeClusterRuntime {
            self.fake_cluster.as_ref().unwrap()
        }

        fn set_leader(&self, leader: Option<u64>) {
            self.fake_cluster().set_leader(leader);
        }

        fn partition_available(&self, nodes: impl IntoIterator<Item = u64>) {
            self.fake_cluster().partition_available(nodes);
        }

        fn restore_all_nodes(&self) {
            self.fake_cluster().restore_all_nodes();
        }

        fn set_delay_writes(&self, delay_writes: bool) {
            self.fake_cluster().set_delay_writes(delay_writes);
        }

        fn drain_one(&self) -> Option<u64> {
            self.fake_cluster().drain_one()
        }

        fn drain_all(&self) -> usize {
            self.fake_cluster().drain_all()
        }

        fn queued_write_count(&self) -> usize {
            self.fake_cluster().queued_write_count()
        }

        fn set_client_addr(&self, node_id: u64, addr: SocketAddr) {
            self.fake_cluster().set_client_addr(node_id, addr);
        }

        async fn connect(&self) -> TestClient {
            TestClient::connect(&self.broker).await
        }

        async fn connect_accepted(&self) -> TestClient {
            TestClient::connect_accepted(&self.broker).await
        }

        async fn connect_durable(&self, durable_id: &str, ack_timeout_ms: u64) -> TestClient {
            TestClient::connect_durable(&self.broker, durable_id, ack_timeout_ms).await
        }

        fn advance_ms(&self, millis: u64) {
            self.clock.advance_ms(millis);
        }

        async fn tick_redelivery(&self) {
            self.broker.tick_redelivery_for_test().await.unwrap();
        }

        async fn restart_broker(&mut self) {
            self.broker.shutdown().await.unwrap();
            self.broker = deterministic_broker(
                test_config(self._dir.path()),
                self.clock.clone(),
                self.fake_cluster.clone().map(ClusterRuntime::Fake),
            );
        }
    }

    struct TestClient {
        stream: Option<BufReader<DuplexStream>>,
        task: Option<JoinHandle<()>>,
    }

    impl Drop for TestClient {
        fn drop(&mut self) {
            if let Some(task) = self.task.take() {
                task.abort();
            }
        }
    }

    impl TestClient {
        async fn connect(broker: &Broker) -> Self {
            Self::connect_with(broker, false).await
        }

        async fn connect_accepted(broker: &Broker) -> Self {
            Self::connect_with(broker, true).await
        }

        async fn connect_with(broker: &Broker, accepted_path: bool) -> Self {
            let (client_stream, server_stream) = tokio::io::duplex(4096);
            let server = broker.clone();
            let task = tokio::spawn(async move {
                if accepted_path {
                    server
                        .handle_accepted_for_test(server_stream)
                        .await
                        .unwrap();
                } else {
                    server.handle_client_for_test(server_stream).await.unwrap();
                }
            });
            let mut client = Self {
                stream: Some(BufReader::new(client_stream)),
                task: Some(task),
            };
            let info = client.read_frame().await;
            assert!(info.starts_with("INFO "));
            client
        }

        async fn connect_durable(broker: &Broker, durable_id: &str, ack_timeout_ms: u64) -> Self {
            let mut client = Self::connect(broker).await;
            client
                .send_durable_connect(durable_id, ack_timeout_ms)
                .await;
            client
        }

        async fn send_durable_connect(&mut self, durable_id: &str, ack_timeout_ms: u64) {
            let payload = serde_json::json!({
                "durable_id": durable_id,
                "verbose": false,
                "ack_timeout_ms": ack_timeout_ms,
                "max_in_flight": 1024,
            });
            self.write_line(&format!("CONNECT {payload}")).await;
        }

        async fn disconnect(mut self) {
            drop(self.stream.take());
            let Some(task) = self.task.take() else {
                return;
            };
            match tokio::time::timeout(Duration::from_secs(1), task).await {
                Ok(joined) => joined.unwrap(),
                Err(_) => panic!("server task did not finish after client disconnect"),
            }
        }

        async fn write_line(&mut self, line: &str) {
            let stream = self.stream.as_mut().expect("client is disconnected");
            stream.get_mut().write_all(line.as_bytes()).await.unwrap();
            stream.get_mut().write_all(b"\r\n").await.unwrap();
        }

        async fn subscribe(&mut self, subject: &str, sid: &str) {
            self.write_line(&format!("SUB {subject} {sid}")).await;
        }

        async fn subscribe_queue(&mut self, subject: &str, queue: &str, sid: &str) {
            self.write_line(&format!("SUB {subject} {queue} {sid}"))
                .await;
        }

        async fn publish(&mut self, subject: &str, payload: &[u8]) {
            self.publish_with_reply(subject, None, payload).await;
        }

        async fn publish_with_reply(
            &mut self,
            subject: &str,
            reply_to: Option<&str>,
            payload: &[u8],
        ) {
            match reply_to {
                Some(reply_to) => {
                    self.write_line(&format!("PUB {subject} {reply_to} {}", payload.len()))
                        .await;
                }
                None => {
                    self.write_line(&format!("PUB {subject} {}", payload.len()))
                        .await;
                }
            }
            let stream = self.stream.as_mut().expect("client is disconnected");
            stream.get_mut().write_all(payload).await.unwrap();
            stream.get_mut().write_all(b"\r\n").await.unwrap();
        }

        async fn ping_roundtrip(&mut self) {
            self.write_line("PING").await;
            self.expect_pong().await;
        }

        async fn expect_pong(&mut self) {
            assert_eq!(self.read_frame().await, "PONG\r\n");
        }

        async fn expect_msg(&mut self) -> String {
            let frame = self.read_frame().await;
            assert!(frame.starts_with("MSG "), "expected MSG, got {frame:?}");
            frame
        }

        async fn expect_hmsg(&mut self) -> String {
            let frame = self.read_frame().await;
            assert!(frame.starts_with("HMSG "), "expected HMSG, got {frame:?}");
            frame
        }

        async fn expect_err_contains(&mut self, expected: &str) -> String {
            let frame = self.read_frame().await;
            assert!(frame.starts_with("-ERR "), "expected -ERR, got {frame:?}");
            assert!(
                frame.contains(expected),
                "expected error containing {expected:?}, got {frame:?}"
            );
            frame
        }

        async fn expect_no_frame_short(&mut self) {
            match tokio::time::timeout(Duration::from_millis(25), self.read_frame_inner()).await {
                Ok(frame) => panic!("expected no frame, got {frame:?}"),
                Err(_) => {}
            }
        }

        async fn read_frame(&mut self) -> String {
            tokio::time::timeout(Duration::from_secs(1), self.read_frame_inner())
                .await
                .expect("timed out reading frame")
        }

        async fn read_frame_inner(&mut self) -> String {
            let stream = self.stream.as_mut().expect("client is disconnected");
            let mut frame = Vec::new();
            stream.read_until(b'\n', &mut frame).await.unwrap();
            assert!(!frame.is_empty(), "connection closed before frame");
            let line = std::str::from_utf8(&frame).unwrap().to_string();
            let mut parts = line.split_whitespace();
            match parts.next() {
                Some("MSG") => {
                    let tokens = line.split_whitespace().collect::<Vec<_>>();
                    let size = tokens.last().unwrap().parse::<usize>().unwrap();
                    let mut body = vec![0; size + 2];
                    stream.read_exact(&mut body).await.unwrap();
                    frame.extend_from_slice(&body);
                }
                Some("HMSG") => {
                    let tokens = line.split_whitespace().collect::<Vec<_>>();
                    let total_size = tokens.last().unwrap().parse::<usize>().unwrap();
                    let mut body = vec![0; total_size + 2];
                    stream.read_exact(&mut body).await.unwrap();
                    frame.extend_from_slice(&body);
                }
                _ => {}
            }
            String::from_utf8(frame).unwrap()
        }
    }

    fn test_config(dir: &Path) -> Config {
        Config {
            listen: "127.0.0.1:0".parse().unwrap(),
            http_listen: None,
            wal_dir: dir.to_path_buf(),
            fsync_interval_ms: 1,
            max_payload: 1024,
            verbose: false,
            tls: None,
            auth: Default::default(),
            cluster: None,
        }
    }

    fn deterministic_broker(
        config: Config,
        clock: Arc<ManualClock>,
        initial_cluster: Option<ClusterRuntime>,
    ) -> Broker {
        Broker::open_with_hooks(
            config,
            BrokerHooks {
                clock,
                start_redelivery_loop: false,
                durable_publish_flush_mode: DurablePublishFlushMode::FlushImmediately,
                initial_cluster,
            },
        )
        .unwrap()
    }

    fn fake_cluster_config(dir: &Path, node_count: u64, local_node_id: u64) -> ClusterConfig {
        ClusterConfig {
            enabled: true,
            node_id: local_node_id,
            raft_listen: SocketAddr::from(([127, 0, 0, 1], 20_000 + local_node_id as u16)),
            route_listen: Some(SocketAddr::from((
                [127, 0, 0, 1],
                30_000 + local_node_id as u16,
            ))),
            routes: Vec::new(),
            route_reconnect_ms: 50,
            raft_dir: dir.join("raft"),
            bootstrap: local_node_id == 1,
            nodes: (1..=node_count)
                .map(|node_id| ClusterNodeConfig {
                    node_id,
                    raft_addr: SocketAddr::from(([127, 0, 0, 1], 20_000 + node_id as u16)),
                    client_addr: SocketAddr::from(([127, 0, 0, 1], 10_000 + node_id as u16)),
                })
                .collect(),
            election_timeout_min_ms: 150,
            election_timeout_max_ms: 300,
            heartbeat_interval_ms: 50,
            snapshot_threshold: 100,
        }
    }

    fn ack_subject(frame: &str) -> String {
        frame.split_whitespace().nth(3).unwrap().to_string()
    }

    async fn http_request(broker: &Broker, path: &str) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let broker = broker.clone();
        let server_task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            broker.handle_http_status(stream).await.unwrap();
        });
        let mut client = TcpStream::connect(addr).await.unwrap();
        client
            .write_all(format!("GET {path} HTTP/1.1\r\nhost: localhost\r\n\r\n").as_bytes())
            .await
            .unwrap();
        let mut response = Vec::new();
        client.read_to_end(&mut response).await.unwrap();
        server_task.await.unwrap();
        String::from_utf8(response).unwrap()
    }

    #[tokio::test]
    async fn auth_enabled_generates_fresh_nonce_per_connection() {
        let dir = TempDir::new().unwrap();
        let mut config = test_config(dir.path());
        config.auth.enabled = true;
        let broker = Broker::open(config).unwrap();
        let (tx1, _rx1) = mpsc::channel(8);
        let (tx2, _rx2) = mpsc::channel(8);

        broker.add_client(1, tx1, None).await.unwrap();
        broker.add_client(2, tx2, None).await.unwrap();

        let inner = broker.inner.lock().await;
        let first = inner.clients.get(&1).unwrap().auth_nonce.as_ref().unwrap();
        let second = inner.clients.get(&2).unwrap().auth_nonce.as_ref().unwrap();
        assert_ne!(first, second);
        assert_eq!(first.len(), 64);
        assert_eq!(second.len(), 64);
    }

    #[tokio::test]
    async fn non_durable_connect_subscribes_as_transient_core() {
        let scenario = Scenario::new();
        let mut subscriber = scenario.connect().await;
        let mut publisher = scenario.connect_durable("publisher1", 25).await;

        subscriber.write_line("CONNECT {}").await;
        subscriber.subscribe("orders.*", "sid1").await;
        subscriber.ping_roundtrip().await;
        publisher.publish("orders.created", b"hello").await;

        let frame = subscriber.expect_msg().await;
        assert_eq!(frame, "MSG orders.created sid1 5\r\nhello\r\n");
        let inner = scenario.broker().inner.lock().await;
        assert!(inner.consumers.is_empty());
        assert_eq!(inner.transient_subscriptions.len(), 1);
    }

    #[tokio::test]
    async fn durable_subscribe_publish_delivery_and_ack_are_deterministic() {
        let scenario = Scenario::new();
        let mut subscriber = scenario.connect_durable("client1", 25).await;
        let mut publisher = scenario.connect_durable("publisher1", 25).await;

        subscriber.subscribe("orders.*", "sid1").await;
        subscriber.ping_roundtrip().await;
        publisher.publish("orders.created", b"hello").await;

        let frame = subscriber.expect_msg().await;
        assert!(frame.starts_with("MSG orders.created sid1 _BROKER.ACK.durable-client1-sid1."));
        assert!(frame.ends_with("5\r\nhello\r\n"));
        publisher.publish(&ack_subject(&frame), b"").await;
        publisher.ping_roundtrip().await;

        let inner = scenario.broker().inner.lock().await;
        let consumer = inner.consumers.get("durable-client1-sid1").unwrap();
        assert!(consumer.pending.is_empty());
        assert!(consumer.in_flight.is_empty());
        assert!(consumer.acked.contains(&1));
        assert!(inner.messages.is_empty());
    }

    #[tokio::test]
    async fn redelivery_waits_for_manual_clock_deadline() {
        let scenario = Scenario::new();
        let mut subscriber = scenario.connect_durable("client1", 25).await;
        let mut publisher = scenario.connect_durable("publisher1", 25).await;

        subscriber.subscribe("orders.*", "sid1").await;
        subscriber.ping_roundtrip().await;
        publisher.publish("orders.created", b"hello").await;
        let first = subscriber.expect_msg().await;
        assert!(first.contains(".1.1 "));

        scenario.advance_ms(24);
        scenario.tick_redelivery().await;
        {
            let inner = scenario.broker().inner.lock().await;
            let consumer = inner.consumers.get("durable-client1-sid1").unwrap();
            assert!(consumer.pending.is_empty());
            assert_eq!(consumer.in_flight.get(&1).unwrap().delivery_id, 1);
        }
        subscriber.expect_no_frame_short().await;

        scenario.advance_ms(1);
        scenario.tick_redelivery().await;
        let second = subscriber.expect_msg().await;
        assert!(second.starts_with("MSG orders.created sid1 _BROKER.ACK.durable-client1-sid1.1.2"));
        assert!(second.ends_with("5\r\nhello\r\n"));
    }

    #[tokio::test]
    async fn acked_message_does_not_redeliver_after_manual_ticks() {
        let scenario = Scenario::new();
        let mut subscriber = scenario.connect_durable("client1", 25).await;
        let mut publisher = scenario.connect_durable("publisher1", 25).await;

        subscriber.subscribe("orders.*", "sid1").await;
        subscriber.ping_roundtrip().await;
        publisher.publish("orders.created", b"hello").await;
        let frame = subscriber.expect_msg().await;
        publisher.publish(&ack_subject(&frame), b"").await;
        publisher.ping_roundtrip().await;

        scenario.advance_ms(1_000);
        scenario.tick_redelivery().await;
        subscriber.expect_no_frame_short().await;
        let inner = scenario.broker().inner.lock().await;
        assert!(inner.messages.is_empty());
        let consumer = inner.consumers.get("durable-client1-sid1").unwrap();
        assert!(consumer.pending.is_empty());
        assert!(consumer.in_flight.is_empty());
    }

    #[tokio::test]
    async fn wal_replay_preserves_unacked_delivery_state_and_next_ids() {
        let mut scenario = Scenario::new();
        {
            let mut subscriber = scenario.connect_durable("client1", 25).await;
            let mut publisher = scenario.connect_durable("publisher1", 25).await;
            subscriber.subscribe("orders.*", "sid1").await;
            subscriber.ping_roundtrip().await;
            publisher.publish("orders.created", b"hello").await;
            let first = subscriber.expect_msg().await;
            assert!(first.contains(".1.1 "));
            subscriber.disconnect().await;
            publisher.disconnect().await;
        }

        scenario.restart_broker().await;
        scenario.advance_ms(25);
        let mut subscriber = scenario.connect_durable("client1", 25).await;
        subscriber.subscribe("orders.*", "sid1").await;
        scenario.tick_redelivery().await;

        let redelivery = subscriber.expect_msg().await;
        assert!(
            redelivery.starts_with("MSG orders.created sid1 _BROKER.ACK.durable-client1-sid1.1.2")
        );
    }

    #[tokio::test]
    async fn acked_message_does_not_redeliver_after_restart() {
        let mut scenario = Scenario::new();
        {
            let mut subscriber = scenario.connect_durable("client1", 25).await;
            let mut publisher = scenario.connect_durable("publisher1", 25).await;
            subscriber.subscribe("orders.*", "sid1").await;
            subscriber.ping_roundtrip().await;
            publisher.publish("orders.created", b"hello").await;
            let frame = subscriber.expect_msg().await;
            publisher.publish(&ack_subject(&frame), b"").await;
            publisher.ping_roundtrip().await;
            subscriber.disconnect().await;
            publisher.disconnect().await;
        }

        scenario.restart_broker().await;
        let mut subscriber = scenario.connect_durable("client1", 25).await;
        subscriber.subscribe("orders.*", "sid1").await;
        subscriber.ping_roundtrip().await;
        scenario.advance_ms(1_000);
        scenario.tick_redelivery().await;
        subscriber.expect_no_frame_short().await;
        let inner = scenario.broker().inner.lock().await;
        assert!(inner.consumers["durable-client1-sid1"].pending.is_empty());
        assert!(inner.consumers["durable-client1-sid1"].in_flight.is_empty());
        assert!(inner.consumers["durable-client1-sid1"].acked.contains(&1));
    }

    #[tokio::test]
    async fn request_reply_inbox_delivery_is_transient() {
        let scenario = Scenario::new();
        let mut responder = scenario.connect_durable("responder1", 25).await;
        let mut requester = scenario.connect_durable("requester1", 25).await;

        responder.subscribe("service.echo", "sid1").await;
        responder.ping_roundtrip().await;
        requester.subscribe("_INBOX.requester.1", "inbox1").await;
        requester.ping_roundtrip().await;
        requester
            .publish_with_reply("service.echo", Some("_INBOX.requester.1"), b"hello")
            .await;

        let request = responder.expect_hmsg().await;
        assert!(request.starts_with("HMSG service.echo sid1 _INBOX.requester.1 "));
        assert!(request.contains("\r\nBroker-Ack: _BROKER.ACK.durable-responder1-sid1."));
        responder.publish("_INBOX.requester.1", b"world").await;

        let response = requester.expect_msg().await;
        assert_eq!(response, "MSG _INBOX.requester.1 inbox1 5\r\nworld\r\n");
        let inner = scenario.broker().inner.lock().await;
        assert!(inner.messages.contains_key(&1));
        assert_eq!(inner.transient_subscriptions.len(), 1);
        assert_eq!(inner.consumers.len(), 1);
    }

    #[tokio::test]
    async fn publish_without_matching_durable_consumer_is_not_retained() {
        let scenario = Scenario::new();
        let mut publisher = scenario.connect_durable("publisher1", 25).await;

        publisher.publish("orders.created", b"hello").await;
        publisher.ping_roundtrip().await;
        assert!(scenario.broker().inner.lock().await.messages.is_empty());
    }

    #[tokio::test]
    async fn durable_queue_group_delivers_one_copy() {
        let scenario = Scenario::new();
        let mut first = scenario.connect_durable("client1", 25).await;
        let mut second = scenario.connect_durable("client2", 25).await;
        let mut publisher = scenario.connect_durable("publisher1", 25).await;

        first.subscribe_queue("orders.*", "workers", "a").await;
        first.ping_roundtrip().await;
        second.subscribe_queue("orders.*", "workers", "b").await;
        second.ping_roundtrip().await;
        publisher.publish("orders.created", b"hello").await;
        publisher.ping_roundtrip().await;

        let inner = scenario.broker().inner.lock().await;
        let consumer = inner
            .consumers
            .get("queue-workers-6f72646572732e2a")
            .unwrap();
        assert_eq!(consumer.delivered, 1);
        assert_eq!(consumer.in_flight.len(), 1);
    }

    #[tokio::test]
    async fn unsub_with_max_receives_one_more_durable_delivery_then_detaches() {
        let scenario = Scenario::new();
        let mut subscriber = scenario.connect_durable("client1", 25).await;
        let mut publisher = scenario.connect_durable("publisher1", 25).await;

        subscriber.subscribe("orders.*", "sid1").await;
        subscriber.ping_roundtrip().await;
        subscriber.write_line("UNSUB sid1 1").await;
        publisher.publish("orders.created", b"one").await;
        let first = subscriber.expect_msg().await;
        assert!(first.ends_with("3\r\none\r\n"));

        publisher.publish("orders.created", b"two").await;
        publisher.ping_roundtrip().await;
        subscriber.expect_no_frame_short().await;
        let inner = scenario.broker().inner.lock().await;
        assert!(inner.consumers["durable-client1-sid1"].members.is_empty());
    }

    #[tokio::test]
    async fn queue_unsub_with_max_detaches_only_that_member_after_count() {
        let scenario = Scenario::new();
        let mut first = scenario.connect_durable("client1", 25).await;
        let mut second = scenario.connect_durable("client2", 25).await;
        let mut publisher = scenario.connect_durable("publisher1", 25).await;

        first.subscribe_queue("orders.*", "workers", "a").await;
        first.ping_roundtrip().await;
        second.subscribe_queue("orders.*", "workers", "b").await;
        second.ping_roundtrip().await;
        first.write_line("UNSUB a 2").await;

        publisher.publish("orders.created", b"one").await;
        assert!(first.expect_msg().await.ends_with("3\r\none\r\n"));
        publisher.publish("orders.created", b"two").await;
        assert!(first.expect_msg().await.ends_with("3\r\ntwo\r\n"));
        publisher.publish("orders.created", b"three").await;
        assert!(second.expect_msg().await.ends_with("5\r\nthree\r\n"));
        first.expect_no_frame_short().await;

        let inner = scenario.broker().inner.lock().await;
        let consumer = inner
            .consumers
            .get("queue-workers-6f72646572732e2a")
            .unwrap();
        assert_eq!(consumer.members.len(), 1);
        assert!(consumer.members.values().any(|member| member.sid == "b"));
    }

    #[tokio::test]
    async fn transient_unsub_with_max_receives_one_more_live_message_then_detaches() {
        let scenario = Scenario::new();
        let mut subscriber = scenario.connect_durable("client1", 25).await;
        let mut publisher = scenario.connect_durable("publisher1", 25).await;

        subscriber.subscribe("_INBOX.client1.1", "inbox1").await;
        subscriber.ping_roundtrip().await;
        subscriber.write_line("UNSUB inbox1 1").await;
        publisher.publish("_INBOX.client1.1", b"one").await;
        let first = subscriber.expect_msg().await;
        assert_eq!(first, "MSG _INBOX.client1.1 inbox1 3\r\none\r\n");

        publisher.publish("_INBOX.client1.1", b"two").await;
        publisher.ping_roundtrip().await;
        subscriber.expect_no_frame_short().await;
        assert!(
            scenario
                .broker()
                .inner
                .lock()
                .await
                .transient_subscriptions
                .is_empty()
        );
    }

    #[tokio::test]
    async fn route_origin_publish_delivers_only_to_transient_subscribers() {
        let scenario = Scenario::new();
        let mut transient = scenario.connect().await;
        let mut durable = scenario.connect_durable("client1", 25).await;

        transient.write_line("CONNECT {}").await;
        transient.subscribe("orders.*", "sid1").await;
        durable.subscribe("orders.*", "durable1").await;
        transient.ping_roundtrip().await;
        durable.ping_roundtrip().await;

        scenario
            .broker()
            .deliver_route_publish("orders.created", None, b"hello")
            .await
            .unwrap();

        let frame = transient.expect_msg().await;
        assert_eq!(frame, "MSG orders.created sid1 5\r\nhello\r\n");
        durable.expect_no_frame_short().await;
        let inner = scenario.broker().inner.lock().await;
        assert!(inner.messages.is_empty());
        assert!(
            inner.consumers["durable-client1-durable1"]
                .pending
                .is_empty()
        );
    }

    #[tokio::test]
    async fn disconnected_in_flight_message_redelivers_after_reconnect() {
        let scenario = Scenario::new();
        let mut subscriber = scenario.connect_durable("client1", 25).await;
        let mut publisher = scenario.connect_durable("publisher1", 25).await;

        subscriber.subscribe("orders.*", "sid1").await;
        subscriber.ping_roundtrip().await;
        publisher.publish("orders.created", b"hello").await;
        let first = subscriber.expect_msg().await;
        assert!(first.starts_with("MSG orders.created sid1 _BROKER.ACK.durable-client1-sid1.1.1"));
        subscriber.disconnect().await;

        scenario.advance_ms(25);
        let mut subscriber = scenario.connect_durable("client1", 25).await;
        subscriber.subscribe("orders.*", "sid1").await;
        subscriber.ping_roundtrip().await;
        scenario.tick_redelivery().await;

        let redelivery = subscriber.expect_msg().await;
        assert!(
            redelivery.starts_with("MSG orders.created sid1 _BROKER.ACK.durable-client1-sid1.1.2")
        );
        assert!(redelivery.ends_with("5\r\nhello\r\n"));
    }

    #[tokio::test]
    async fn disconnected_in_flight_message_does_not_redeliver_before_deadline() {
        let scenario = Scenario::new();
        let mut subscriber = scenario.connect_durable("client1", 25).await;
        let mut publisher = scenario.connect_durable("publisher1", 25).await;

        subscriber.subscribe("orders.*", "sid1").await;
        subscriber.ping_roundtrip().await;
        publisher.publish("orders.created", b"hello").await;
        let first = subscriber.expect_msg().await;
        assert!(first.starts_with("MSG orders.created sid1 _BROKER.ACK.durable-client1-sid1.1.1"));
        subscriber.disconnect().await;

        scenario.advance_ms(24);
        let mut subscriber = scenario.connect_durable("client1", 25).await;
        subscriber.subscribe("orders.*", "sid1").await;
        subscriber.ping_roundtrip().await;
        scenario.tick_redelivery().await;
        subscriber.expect_no_frame_short().await;
        {
            let inner = scenario.broker().inner.lock().await;
            let consumer = inner.consumers.get("durable-client1-sid1").unwrap();
            assert!(consumer.pending.is_empty());
            assert_eq!(consumer.in_flight.get(&1).unwrap().delivery_id, 1);
        }

        scenario.advance_ms(1);
        scenario.tick_redelivery().await;
        let redelivery = subscriber.expect_msg().await;
        assert!(
            redelivery.starts_with("MSG orders.created sid1 _BROKER.ACK.durable-client1-sid1.1.2")
        );
    }

    #[tokio::test]
    async fn ack_after_reconnect_survives_restart() {
        let mut scenario = Scenario::new();
        let mut subscriber = scenario.connect_durable("client1", 25).await;
        let mut publisher = scenario.connect_durable("publisher1", 25).await;

        subscriber.subscribe("orders.*", "sid1").await;
        subscriber.ping_roundtrip().await;
        publisher.publish("orders.created", b"hello").await;
        let first = subscriber.expect_msg().await;
        assert!(first.starts_with("MSG orders.created sid1 _BROKER.ACK.durable-client1-sid1.1.1"));
        subscriber.disconnect().await;

        scenario.advance_ms(25);
        let mut subscriber = scenario.connect_durable("client1", 25).await;
        subscriber.subscribe("orders.*", "sid1").await;
        subscriber.ping_roundtrip().await;
        scenario.tick_redelivery().await;
        let redelivery = subscriber.expect_msg().await;
        assert!(
            redelivery.starts_with("MSG orders.created sid1 _BROKER.ACK.durable-client1-sid1.1.2")
        );
        publisher.publish(&ack_subject(&redelivery), b"").await;
        publisher.ping_roundtrip().await;
        subscriber.disconnect().await;
        publisher.disconnect().await;

        scenario.restart_broker().await;
        let mut subscriber = scenario.connect_durable("client1", 25).await;
        subscriber.subscribe("orders.*", "sid1").await;
        subscriber.ping_roundtrip().await;
        scenario.advance_ms(1_000);
        scenario.tick_redelivery().await;
        subscriber.expect_no_frame_short().await;

        let inner = scenario.broker().inner.lock().await;
        let consumer = inner.consumers.get("durable-client1-sid1").unwrap();
        assert!(consumer.pending.is_empty());
        assert!(consumer.in_flight.is_empty());
        assert!(consumer.acked.contains(&1));
    }

    #[tokio::test]
    async fn fake_cluster_runtime_drives_broker_flow_across_100_nodes() {
        let scenario = Scenario::new_fake_cluster(100);
        let mut subscriber = scenario.connect_durable("client1", 25).await;
        let mut publisher = scenario.connect_durable("publisher1", 25).await;

        assert_eq!(scenario.fake_cluster().node_count(), 100);
        assert_eq!(scenario.broker().cluster_leader().await, Some(1));

        subscriber.subscribe("orders.*", "sid1").await;
        subscriber.ping_roundtrip().await;
        publisher.publish("orders.created", b"hello").await;

        let delivery = subscriber.expect_msg().await;
        assert!(
            delivery.starts_with("MSG orders.created sid1 _BROKER.ACK.durable-client1-sid1.1.1")
        );
        assert!(delivery.ends_with("5\r\nhello\r\n"));

        publisher.publish(&ack_subject(&delivery), b"").await;
        publisher.ping_roundtrip().await;

        let durable = scenario.fake_cluster().durable_state();
        let consumer = durable.consumers.get("durable-client1-sid1").unwrap();
        assert!(consumer.pending.is_empty());
        assert!(consumer.in_flight.is_empty());
        assert!(consumer.acked.contains(&1));
        assert!(durable.messages.is_empty());
        assert_eq!(scenario.fake_cluster().write_count(), 4);
    }

    #[tokio::test]
    async fn http_cluster_endpoint_reports_standalone_node() {
        let scenario = Scenario::new();

        let response = http_request(scenario.broker(), "/cluster").await;

        assert!(response.starts_with("HTTP/1.1 200 OK\r\n"));
        assert!(response.contains("\"cluster_size\":1"));
        assert!(response.contains("\"cluster_status\":\"standalone\""));
        assert!(response.contains("\"node_id\":null"));
        assert!(response.contains("\"role\":\"standalone\""));
        assert!(response.contains("\"leader_id\":null"));
        assert!(response.contains("\"peers\":[]"));
    }

    #[tokio::test]
    async fn http_cluster_endpoint_reports_cluster_role_and_leader() {
        let scenario = Scenario::new_fake_cluster_local_node(3, 1, Some(1));

        let response = http_request(scenario.broker(), "/cluster").await;

        assert!(response.starts_with("HTTP/1.1 200 OK\r\n"));
        assert!(response.contains("\"cluster_size\":3"));
        assert!(response.contains("\"cluster_status\":\"ready\""));
        assert!(response.contains("\"node_id\":1"));
        assert!(response.contains("\"role\":\"leader\""));
        assert!(response.contains("\"leader_id\":1"));
    }

    #[tokio::test]
    async fn cluster_response_reports_follower_role_and_leader() {
        let scenario = Scenario::new_fake_cluster_local_node(3, 2, Some(1));

        let status = scenario.broker().cluster_response().await;

        assert_eq!(status.cluster_size, 3);
        assert_eq!(status.cluster_status, "ready");
        assert_eq!(status.node_id, Some(2));
        assert_eq!(status.role, "follower");
        assert_eq!(status.leader_id, Some(1));
    }

    #[tokio::test]
    async fn cluster_response_reports_forming_without_leader() {
        let scenario = Scenario::new_fake_cluster_local_node(3, 2, None);

        let status = scenario.broker().cluster_response().await;

        assert_eq!(status.cluster_size, 3);
        assert_eq!(status.cluster_status, "forming");
        assert_eq!(status.node_id, Some(2));
        assert_eq!(status.role, "unknown");
        assert_eq!(status.leader_id, None);
    }

    #[tokio::test]
    async fn http_connections_endpoint_reports_live_client_metadata() {
        let scenario = Scenario::new();
        let mut client = scenario.connect_durable("client1", 25).await;
        client.subscribe("orders.*", "sid1").await;
        client.subscribe("_INBOX.client1.1", "inbox1").await;
        client.ping_roundtrip().await;

        let response = http_request(scenario.broker(), "/connections").await;

        assert!(response.starts_with("HTTP/1.1 200 OK\r\n"));
        assert!(response.contains("\"count\":1"));
        assert!(response.contains("\"id\":1"));
        assert!(response.contains("\"remote_addr\":null"));
        assert!(response.contains("\"durable_id\":\"client1\""));
        assert!(response.contains("\"authenticated\":false"));
        assert!(response.contains("\"connected_at_ms\":1000"));
        assert!(response.contains("\"ack_timeout_ms\":25"));
        assert!(response.contains("\"max_in_flight\":1024"));
        assert!(response.contains("\"subscriptions\":1"));
        assert!(response.contains("\"transient_subscriptions\":1"));
    }

    #[tokio::test]
    async fn http_subscriptions_endpoint_reports_durable_and_transient_state() {
        let scenario = Scenario::new();
        let mut first = scenario.connect_durable("client1", 25).await;
        let mut second = scenario.connect_durable("client2", 50).await;
        first.subscribe("orders.*", "sid1").await;
        first.subscribe("_INBOX.client1.1", "inbox1").await;
        second
            .subscribe_queue("orders.*", "workers", "worker1")
            .await;
        first.ping_roundtrip().await;
        second.ping_roundtrip().await;

        let response = http_request(scenario.broker(), "/subscriptions").await;

        assert!(response.starts_with("HTTP/1.1 200 OK\r\n"));
        assert!(response.contains("\"durable_consumers\""));
        assert!(response.contains("\"consumer_id\":\"durable-client1-sid1\""));
        assert!(response.contains("\"filter_subject\":\"orders.*\""));
        assert!(response.contains("\"queue_group\":null"));
        assert!(response.contains("\"connection_id\":1"));
        assert!(response.contains("\"sid\":\"sid1\""));
        assert!(response.contains("\"consumer_id\":\"queue-workers-6f72646572732e2a\""));
        assert!(response.contains("\"queue_group\":\"workers\""));
        assert!(response.contains("\"sid\":\"worker1\""));
        assert!(response.contains("\"transient_subscriptions\""));
        assert!(response.contains("\"subject\":\"_INBOX.client1.1\""));
        assert!(response.contains("\"sid\":\"inbox1\""));
    }

    #[tokio::test]
    async fn http_status_and_unknown_paths_return_not_found() {
        let scenario = Scenario::new();

        let status = http_request(scenario.broker(), "/status").await;
        let unknown = http_request(scenario.broker(), "/nope").await;

        assert!(status.starts_with("HTTP/1.1 404 Not Found\r\n"));
        assert!(status.ends_with("{\"error\":\"not found\"}"));
        assert!(unknown.starts_with("HTTP/1.1 404 Not Found\r\n"));
        assert!(unknown.ends_with("{\"error\":\"not found\"}"));
    }

    #[tokio::test]
    async fn fake_cluster_follower_without_known_leader_returns_error() {
        let scenario = Scenario::new_fake_cluster_local_node(3, 1, None);
        let (client_stream, server_stream) = tokio::io::duplex(4096);
        let broker = scenario.broker().clone();
        let task = tokio::spawn(async move {
            broker
                .handle_accepted_for_test(server_stream)
                .await
                .unwrap();
        });
        let mut client = BufReader::new(client_stream);
        let mut frame = Vec::new();

        client.read_until(b'\n', &mut frame).await.unwrap();

        let frame = String::from_utf8(frame).unwrap();
        assert!(frame.starts_with("-ERR "), "expected -ERR, got {frame:?}");
        assert!(frame.contains("no known leader"));
        task.await.unwrap();
    }

    #[tokio::test]
    async fn fake_cluster_follower_proxies_raw_bytes_to_known_leader() {
        let scenario = Scenario::new_fake_cluster_local_node(3, 1, Some(2));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        scenario.set_client_addr(2, listener.local_addr().unwrap());
        let leader_task = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0; 5];
            stream.read_exact(&mut request).await.unwrap();
            assert_eq!(&request, b"ping!");
            stream.write_all(b"pong!").await.unwrap();
        });
        let (mut client_stream, server_stream) = tokio::io::duplex(4096);
        let broker = scenario.broker().clone();
        let broker_task = tokio::spawn(async move {
            broker
                .handle_accepted_for_test(server_stream)
                .await
                .unwrap();
        });

        client_stream.write_all(b"ping!").await.unwrap();
        let mut response = [0; 5];
        client_stream.read_exact(&mut response).await.unwrap();

        assert_eq!(&response, b"pong!");
        drop(client_stream);
        leader_task.await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), broker_task)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn fake_cluster_leader_change_from_remote_to_local_handles_protocol_locally() {
        let scenario = Scenario::new_fake_cluster_local_node(3, 1, Some(2));
        scenario.set_leader(Some(1));
        let mut subscriber = scenario.connect_accepted().await;
        subscriber.send_durable_connect("client1", 25).await;
        let mut publisher = scenario.connect_accepted().await;
        publisher.send_durable_connect("publisher1", 25).await;

        subscriber.subscribe("orders.*", "sid1").await;
        subscriber.ping_roundtrip().await;
        publisher.publish("orders.created", b"hello").await;

        let delivery = subscriber.expect_msg().await;
        assert!(
            delivery.starts_with("MSG orders.created sid1 _BROKER.ACK.durable-client1-sid1.1.1")
        );
        assert!(delivery.ends_with("5\r\nhello\r\n"));
    }

    #[tokio::test]
    async fn fake_cluster_local_leader_accepts_durable_flow_through_accepted_path() {
        let scenario = Scenario::new_fake_cluster_local_node(5, 1, Some(1));
        let mut subscriber = scenario.connect_accepted().await;
        subscriber.send_durable_connect("client1", 25).await;
        let mut publisher = scenario.connect_accepted().await;
        publisher.send_durable_connect("publisher1", 25).await;

        subscriber.subscribe("orders.*", "sid1").await;
        subscriber.ping_roundtrip().await;
        publisher.publish("orders.created", b"hello").await;

        let delivery = subscriber.expect_msg().await;
        assert!(delivery.ends_with("5\r\nhello\r\n"));
        publisher.publish(&ack_subject(&delivery), b"").await;
        publisher.ping_roundtrip().await;

        let durable = scenario.fake_cluster().durable_state();
        let consumer = durable.consumers.get("durable-client1-sid1").unwrap();
        assert!(consumer.in_flight.is_empty());
        assert!(consumer.acked.contains(&1));
    }

    #[tokio::test]
    async fn fake_cluster_quorum_loss_rejects_subscribe() {
        let scenario = Scenario::new_fake_cluster(5);
        scenario.partition_available([1, 2]);
        let mut subscriber = scenario.connect_durable("client1", 25).await;

        subscriber.subscribe("orders.*", "sid1").await;

        subscriber.expect_err_contains("quorum unavailable").await;
        assert!(scenario.fake_cluster().durable_state().consumers.is_empty());
        assert_eq!(scenario.fake_cluster().write_count(), 0);
    }

    #[tokio::test]
    async fn fake_cluster_not_leader_rejects_publish() {
        let scenario = Scenario::new_fake_cluster(5);
        let mut subscriber = scenario.connect_durable("client1", 25).await;
        let mut publisher = scenario.connect_durable("publisher1", 25).await;
        subscriber.subscribe("orders.*", "sid1").await;
        subscriber.ping_roundtrip().await;

        scenario.set_leader(Some(2));
        publisher.publish("orders.created", b"hello").await;

        publisher.expect_err_contains("not leader").await;
        subscriber.expect_no_frame_short().await;
        assert_eq!(scenario.fake_cluster().write_count(), 1);
    }

    #[tokio::test]
    async fn route_enabled_follower_forwards_durable_writes_to_leader() {
        let scenario = Scenario::new_fake_route_cluster_local_node(3, 2, Some(1));
        let mut subscriber = scenario.connect_durable("client1", 25).await;
        let mut publisher = scenario.connect_durable("publisher1", 25).await;

        subscriber.subscribe("orders.*", "sid1").await;
        subscriber.ping_roundtrip().await;
        publisher.publish("orders.created", b"hello").await;
        publisher.ping_roundtrip().await;

        let delivery = subscriber.expect_msg().await;
        assert!(delivery.starts_with("MSG orders.created sid1 _BROKER.ACK.durable-client1-sid1."));
        assert!(delivery.ends_with("5\r\nhello\r\n"));
        assert_eq!(scenario.fake_cluster().write_count(), 3);
    }

    #[tokio::test]
    async fn clustered_transient_publish_without_durable_match_does_not_propose_raft() {
        let scenario = Scenario::new_fake_cluster_local_node(3, 2, Some(1));
        let mut subscriber = scenario.connect().await;
        let mut publisher = scenario.connect().await;

        subscriber.write_line("CONNECT {}").await;
        publisher.write_line("CONNECT {}").await;
        subscriber.subscribe("topic", "sid1").await;
        subscriber.ping_roundtrip().await;

        publisher.publish("topic", b"hello").await;
        publisher.ping_roundtrip().await;

        let delivery = subscriber.expect_msg().await;
        assert_eq!(delivery, "MSG topic sid1 5\r\nhello\r\n");
        assert_eq!(scenario.fake_cluster().write_count(), 0);
    }

    #[tokio::test]
    async fn fake_cluster_partition_blocks_then_restore_allows_write() {
        let scenario = Scenario::new_fake_cluster(5);
        let mut subscriber = scenario.connect_durable("client1", 25).await;
        let mut publisher = scenario.connect_durable("publisher1", 25).await;
        subscriber.subscribe("orders.*", "sid1").await;
        subscriber.ping_roundtrip().await;

        scenario.partition_available([1, 2]);
        publisher.publish("orders.created", b"blocked").await;
        publisher.expect_err_contains("quorum unavailable").await;
        subscriber.expect_no_frame_short().await;

        scenario.restore_all_nodes();
        publisher.publish("orders.created", b"hello").await;
        let delivery = subscriber.expect_msg().await;
        assert!(delivery.ends_with("5\r\nhello\r\n"));
    }

    #[tokio::test]
    async fn fake_cluster_delays_consumer_upsert_until_drained() {
        let scenario = Scenario::new_fake_cluster(5);
        scenario.set_delay_writes(true);
        let mut subscriber = scenario.connect_durable("client1", 25).await;

        subscriber.subscribe("orders.*", "sid1").await;
        subscriber.write_line("PING").await;

        subscriber.expect_no_frame_short().await;
        assert_eq!(scenario.queued_write_count(), 1);
        assert!(
            !scenario
                .broker()
                .inner
                .lock()
                .await
                .consumers
                .contains_key("durable-client1-sid1")
        );

        assert!(scenario.drain_one().is_some());
        subscriber.expect_pong().await;
        assert!(
            scenario
                .broker()
                .inner
                .lock()
                .await
                .consumers
                .contains_key("durable-client1-sid1")
        );
    }

    #[tokio::test]
    async fn fake_cluster_delays_publish_delivery_until_drained() {
        let scenario = Scenario::new_fake_cluster(5);
        let mut subscriber = scenario.connect_durable("client1", 25).await;
        let mut publisher = scenario.connect_durable("publisher1", 25).await;
        subscriber.subscribe("orders.*", "sid1").await;
        subscriber.ping_roundtrip().await;

        scenario.set_delay_writes(true);
        publisher.publish("orders.created", b"hello").await;
        publisher.write_line("PING").await;

        subscriber.expect_no_frame_short().await;
        assert_eq!(scenario.queued_write_count(), 1);
        assert!(scenario.drain_one().is_some());
        tokio::task::yield_now().await;
        subscriber.expect_no_frame_short().await;
        assert_eq!(scenario.queued_write_count(), 1);
        assert!(scenario.drain_one().is_some());

        let delivery = subscriber.expect_msg().await;
        assert!(delivery.ends_with("5\r\nhello\r\n"));
        publisher.expect_pong().await;
    }

    #[tokio::test]
    async fn fake_cluster_delays_ack_until_drained() {
        let scenario = Scenario::new_fake_cluster(5);
        let mut subscriber = scenario.connect_durable("client1", 25).await;
        let mut publisher = scenario.connect_durable("publisher1", 25).await;
        subscriber.subscribe("orders.*", "sid1").await;
        subscriber.ping_roundtrip().await;
        publisher.publish("orders.created", b"hello").await;
        let delivery = subscriber.expect_msg().await;

        scenario.set_delay_writes(true);
        publisher.publish(&ack_subject(&delivery), b"").await;
        publisher.write_line("PING").await;
        publisher.expect_no_frame_short().await;
        assert_eq!(scenario.queued_write_count(), 1);
        {
            let durable = scenario.fake_cluster().durable_state();
            assert!(
                durable.consumers["durable-client1-sid1"]
                    .in_flight
                    .contains_key(&1)
            );
        }

        assert!(scenario.drain_one().is_some());
        publisher.expect_pong().await;
        let durable = scenario.fake_cluster().durable_state();
        let consumer = durable.consumers.get("durable-client1-sid1").unwrap();
        assert!(consumer.in_flight.is_empty());
        assert!(consumer.acked.contains(&1));
    }

    #[tokio::test]
    async fn fake_cluster_leader_change_back_to_local_allows_writes() {
        let scenario = Scenario::new_fake_cluster(5);
        scenario.set_leader(Some(2));
        let mut subscriber = scenario.connect_durable("client1", 25).await;

        subscriber.subscribe("orders.*", "sid1").await;
        subscriber.expect_err_contains("not leader").await;
        assert!(scenario.fake_cluster().durable_state().consumers.is_empty());

        scenario.set_leader(Some(1));
        subscriber.subscribe("orders.*", "sid1").await;
        subscriber.ping_roundtrip().await;
        assert!(
            scenario
                .fake_cluster()
                .durable_state()
                .consumers
                .contains_key("durable-client1-sid1")
        );
    }
}
