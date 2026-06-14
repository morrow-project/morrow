use super::*;

#[derive(Clone)]
pub struct Broker {
    pub(super) inner: Arc<Mutex<Inner>>,
    pub(super) next_connection_id: Arc<AtomicU64>,
    pub(super) config: Config,
    pub(super) tls_acceptor: Option<TlsAcceptor>,
    pub(super) cluster: Arc<Mutex<Option<ClusterRuntime>>>,
    pub(super) route_mesh: Option<RouteMesh>,
    pub(super) hooks: BrokerHooks,
}
