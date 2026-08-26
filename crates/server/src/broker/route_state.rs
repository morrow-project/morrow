use super::*;

#[derive(Clone)]
pub(super) struct RouteMesh {
    pub(super) inner: Arc<Mutex<RouteMeshState>>,
    pub(super) auth_token: String,
    pub(super) tls: Option<RouteTlsRuntime>,
    pub(super) configured_route_nodes: Arc<HashMap<String, u64>>,
    pub(super) quotas: Arc<crate::quota::QuotaRuntime>,
}

#[derive(Clone)]
pub(super) struct RouteTlsRuntime {
    pub(super) acceptor: tokio_rustls::TlsAcceptor,
    pub(super) connector: tokio_rustls::TlsConnector,
    pub(super) peer_identities: Arc<HashMap<Vec<u8>, u64>>,
    pub(super) server_names: Arc<HashMap<u64, String>>,
    pub(super) handshake_timeout_ms: u64,
}

pub(super) struct RouteMeshState {
    pub(super) node_id: u64,
    pub(super) route_listen: SocketAddr,
    pub(super) route_advertise: String,
    pub(super) client_addr: SocketAddr,
    pub(super) seeds: Vec<String>,
    pub(super) reconnect_ms: u64,
    pub(super) peers: HashMap<u64, RoutePeer>,
    pub(super) known_peers: HashMap<u64, RoutePeerInfo>,
    pub(super) local_interests: BTreeSet<String>,
    pub(super) local_interest_version: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(super) struct AuthenticatedRouteFrame {
    pub(super) auth_token: String,
    pub(super) frame: RouteFrame,
}

pub(super) struct RoutePeer {
    pub(super) info: RoutePeerInfo,
    pub(super) sender: mpsc::Sender<RouteFrame>,
    pub(super) direction: RouteDirection,
    pub(super) state: &'static str,
    pub(super) reconnect_attempts: u64,
    pub(super) last_error: Option<String>,
    pub(super) remote_interests: BTreeSet<String>,
    pub(super) remote_interest_version: u64,
    pub(super) remote_interest_index: subject::SubjectTrie<()>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(super) struct RoutePeerInfo {
    pub(super) node_id: u64,
    pub(super) route_addr: String,
    pub(super) client_addr: SocketAddr,
}

#[derive(Debug, Clone, Copy)]
pub(super) enum RouteDirection {
    Inbound,
    Outbound,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(super) enum RouteFrame {
    Hello {
        node_id: u64,
        route_addr: String,
        client_addr: SocketAddr,
    },
    PeerList {
        peers: Vec<RoutePeerInfo>,
    },
    Interests {
        version: u64,
        subjects: Vec<String>,
    },
    InterestDelta {
        version: u64,
        added: Vec<String>,
        removed: Vec<String>,
    },
    InterestResync,
    Publish {
        subject: String,
        reply_to: Option<String>,
        payload: Arc<Vec<u8>>,
    },
    Ping,
    Pong,
}

#[derive(Debug, serde::Serialize)]
pub(super) struct RouteTopologyResponse {
    pub(super) listen: String,
    pub(super) seeds: Vec<String>,
    pub(super) discovered: Vec<RouteDiscoveredPeerResponse>,
    pub(super) connected: Vec<RoutePeerResponse>,
}

#[derive(Debug, serde::Serialize)]
pub(super) struct RouteDiscoveredPeerResponse {
    pub(super) node_id: u64,
    pub(super) route_addr: String,
    pub(super) client_addr: String,
    pub(super) connected: bool,
}

#[derive(Debug, serde::Serialize)]
pub(super) struct RoutePeerResponse {
    pub(super) node_id: u64,
    pub(super) route_addr: String,
    pub(super) client_addr: String,
    pub(super) direction: &'static str,
    pub(super) state: &'static str,
    pub(super) reconnect_attempts: u64,
    pub(super) last_error: Option<String>,
    pub(super) subscriptions: usize,
    pub(super) subjects: Vec<String>,
}
