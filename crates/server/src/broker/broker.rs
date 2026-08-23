use super::*;
use crate::consumer_group::GroupCoordinator;

#[derive(Clone)]
pub struct Morrow {
    pub(super) inner: Arc<Mutex<DurableBrokerState>>,
    pub(super) wal: WalRuntime,
    pub(super) partition_logs: Arc<PartitionLogSet>,
    pub(super) storage_permits: Arc<tokio::sync::Semaphore>,
    pub(super) storage_gate: Arc<tokio::sync::RwLock<()>>,
    pub(super) connections: Arc<Mutex<ConnectionState>>,
    pub(super) transient: Arc<Mutex<TransientState>>,
    pub(super) groups: Arc<Mutex<HashMap<String, GroupCoordinator>>>,
    pub(super) group_sessions: Arc<Mutex<HashMap<u64, GroupMemberSession>>>,
    pub(super) next_connection_id: Arc<AtomicU64>,
    pub(super) config: Config,
    pub(super) tls_acceptor: Option<TlsAcceptor>,
    pub(super) admin_tls_acceptor: Option<TlsAcceptor>,
    pub(super) quotas: Arc<crate::quota::QuotaRuntime>,
    pub(super) tenant_quotas: crate::quota::TenantQuotaRuntime,
    pub(super) policy: Arc<crate::tenancy::PolicyStore>,
    pub(super) audit: Arc<std::sync::Mutex<crate::tenancy::AuditLog>>,
    pub(super) cluster: Arc<Mutex<Option<ClusterRuntime>>>,
    pub(super) cluster_applied_index: Arc<AtomicU64>,
    pub(super) cluster_delta_gate: Arc<Mutex<()>>,
    pub(super) cluster_application_metrics: Arc<ClusterApplicationMetrics>,
    pub(super) metrics: Arc<BrokerMetrics>,
    pub(super) storage_failure: Arc<AtomicBool>,
    pub(super) redelivery_notify: Arc<Notify>,
    pub(super) pull_waiters: PullWaiterRegistry,
    pub(super) compaction_running: Arc<AtomicBool>,
    pub(super) route_mesh: Option<RouteMesh>,
    pub(super) middleware: MiddlewareRuntime,
    pub(super) hooks: BrokerHooks,
}

impl Morrow {
    pub fn middleware_runtime(&self) -> MiddlewareRuntime {
        self.middleware.clone()
    }

    pub fn audit_records(&self) -> Vec<crate::tenancy::AuditRecord> {
        self.audit
            .lock()
            .expect("audit log lock poisoned")
            .records()
            .to_vec()
    }

    pub fn verify_audit_log(&self) -> Result<()> {
        self.audit.lock().expect("audit log lock poisoned").verify()
    }

    pub fn policy_snapshot(&self) -> crate::tenancy::PolicySnapshot {
        self.policy.snapshot()
    }

    pub fn replace_policy_snapshot(&self, snapshot: crate::tenancy::PolicySnapshot) -> Result<()> {
        let generation = snapshot.generation;
        self.policy.replace(snapshot)?;
        self.record_audit_event(crate::tenancy::AuditEvent {
            sequence: 0,
            timestamp_ms: self.hooks.clock.now_ms(),
            actor: "system".to_string(),
            tenant: None,
            action: "policy.replace".to_string(),
            resource: "cluster/policy".to_string(),
            outcome: "success".to_string(),
            details: [("generation".to_string(), generation.to_string())]
                .into_iter()
                .collect(),
        });
        Ok(())
    }
}
