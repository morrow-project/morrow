use super::*;

#[derive(Clone)]
pub struct Broker {
    pub(super) inner: Arc<Mutex<DurableBrokerState>>,
    pub(super) wal: WalRuntime,
    pub(super) partition_logs: Arc<PartitionLogSet>,
    pub(super) storage_permits: Arc<tokio::sync::Semaphore>,
    pub(super) storage_gate: Arc<tokio::sync::RwLock<()>>,
    pub(super) connections: Arc<Mutex<ConnectionState>>,
    pub(super) transient: Arc<Mutex<TransientState>>,
    pub(super) next_connection_id: Arc<AtomicU64>,
    pub(super) config: Config,
    pub(super) tls_acceptor: Option<TlsAcceptor>,
    pub(super) admin_tls_acceptor: Option<TlsAcceptor>,
    pub(super) quotas: Arc<crate::quota::QuotaRuntime>,
    pub(super) cluster: Arc<Mutex<Option<ClusterRuntime>>>,
    pub(super) cluster_applied_index: Arc<AtomicU64>,
    pub(super) cluster_delta_gate: Arc<Mutex<()>>,
    pub(super) cluster_application_metrics: Arc<ClusterApplicationMetrics>,
    pub(super) redelivery_notify: Arc<Notify>,
    pub(super) compaction_running: Arc<AtomicBool>,
    pub(super) route_mesh: Option<RouteMesh>,
    pub(super) middleware: MiddlewareRuntime,
    pub(super) hooks: BrokerHooks,
}

impl Broker {
    pub fn middleware_runtime(&self) -> MiddlewareRuntime {
        self.middleware.clone()
    }
}
