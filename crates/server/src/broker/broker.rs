use super::*;
use crate::consumer_group::GroupCoordinator;

pub(super) struct ViewRuntime {
    pub(super) definition: crate::config::ViewConfig,
    pub(super) view: crate::materialized_view::MaterializedView,
    pub(super) paused: bool,
}

pub(super) fn view_update(
    definition: &crate::config::ViewConfig,
    record: &crate::wal::PublishRecord,
) -> Option<crate::materialized_view::ViewUpdate> {
    if record.stream.as_deref() != Some(definition.source_stream.as_str())
        || definition
            .source_subject
            .as_deref()
            .is_some_and(|subject| !protocol::subject::matches(subject, &record.subject))
    {
        return None;
    }
    let key = definition
        .key_header
        .as_deref()
        .and_then(|header| {
            record
                .headers
                .iter()
                .find(|header_value| header_value.name.eq_ignore_ascii_case(header))
                .map(|header_value| header_value.value.clone())
        })
        .unwrap_or_else(|| record.subject.clone());
    Some(crate::materialized_view::ViewUpdate {
        key,
        value: Some(record.payload.clone()),
        position: crate::materialized_view::ViewPosition {
            stream: record.stream.clone().unwrap_or_default(),
            partition: crate::stream::PartitionId(record.partition.unwrap_or_default()),
            offset: record.offset.unwrap_or_default(),
        },
    })
}

#[derive(Clone)]
pub struct Morrow {
    pub(super) inner: Arc<Mutex<DurableBrokerState>>,
    pub(super) wal: WalRuntime,
    pub(super) partition_logs: Arc<PartitionLogSet>,
    pub(super) storage_permits: Arc<tokio::sync::Semaphore>,
    pub(super) storage_gate: Arc<tokio::sync::RwLock<()>>,
    pub(super) state_shard_gates: Arc<Vec<tokio::sync::Mutex<()>>>,
    pub(super) connections: Arc<Mutex<ConnectionState>>,
    pub(super) transient: Arc<Mutex<TransientState>>,
    pub(super) groups: Arc<Mutex<HashMap<String, GroupCoordinator>>>,
    pub(super) group_sessions: Arc<Mutex<HashMap<u64, GroupMemberSession>>>,
    pub(super) next_connection_id: Arc<AtomicU64>,
    pub(super) config: Config,
    pub(super) tls_acceptor: Option<TlsAcceptor>,
    pub(super) admin_tls_acceptor: Option<TlsAcceptor>,
    pub(super) websocket_tls_acceptor: Option<TlsAcceptor>,
    pub(super) quotas: Arc<crate::quota::QuotaRuntime>,
    pub(super) tenant_quotas: crate::quota::TenantQuotaRuntime,
    pub(super) policy: Arc<crate::tenancy::PolicyStore>,
    pub(super) audit: Arc<std::sync::Mutex<crate::tenancy::AuditLog>>,
    pub(super) schema_registry: Arc<Mutex<crate::schema_registry::SchemaRegistry>>,
    pub(super) cluster: Arc<Mutex<Option<ClusterRuntime>>>,
    pub(super) cluster_applied_index: Arc<AtomicU64>,
    pub(super) local_partition_applied: Arc<Mutex<HashMap<String, u64>>>,
    pub(super) cluster_delta_gate: Arc<Mutex<()>>,
    pub(super) cluster_application_metrics: Arc<ClusterApplicationMetrics>,
    pub(super) metrics: Arc<BrokerMetrics>,
    pub(super) metrics_snapshot: Arc<tokio::sync::RwLock<Option<(std::time::Instant, Arc<str>)>>>,
    pub(super) metrics_refreshing: Arc<AtomicBool>,
    pub(super) storage_failure: Arc<AtomicBool>,
    pub(super) audit_failure: Arc<AtomicBool>,
    pub(super) shutting_down: Arc<AtomicBool>,
    pub(super) redelivery_notify: Arc<Notify>,
    pub(super) pull_waiters: PullWaiterRegistry,
    pub(super) broker_control: BrokerControlRegistry,
    pub(super) compaction_running: Arc<AtomicBool>,
    pub(super) work_scheduler: Arc<tokio::sync::Mutex<crate::work_scheduler::WorkScheduler>>,
    pub(super) route_mesh: Option<RouteMesh>,
    pub(super) middleware: MiddlewareRuntime,
    pub(super) hooks: BrokerHooks,
    pub(super) transactions: Arc<Mutex<crate::transaction::TransactionCoordinator>>,
    pub(super) views: Arc<Mutex<HashMap<String, ViewRuntime>>>,
    pub(super) reassignment: Arc<Mutex<crate::reassignment::ReassignmentController>>,
    pub(super) cross_region: Arc<Mutex<crate::cross_region::CrossRegionReplicator>>,
    pub(super) partition_expansions:
        Arc<Mutex<HashMap<String, crate::partition_expansion::PartitionExpansion>>>,
}

impl Morrow {
    fn persist_partition_expansions(
        &self,
        expansions: &HashMap<String, crate::partition_expansion::PartitionExpansion>,
    ) -> Result<()> {
        let path = self.config.wal_dir.join("partition-expansions.json");
        let temporary = path.with_extension("json.tmp");
        let body = serde_json::to_vec_pretty(expansions).map_err(|error| {
            BrokerError::msg(format!("serializing partition expansions: {error}"))
        })?;
        std::fs::write(&temporary, body)
            .map_err(|error| BrokerError::msg(format!("writing partition expansions: {error}")))?;
        std::fs::rename(&temporary, &path)
            .map_err(|error| BrokerError::msg(format!("installing partition expansions: {error}")))
    }

    pub(super) async fn reserve_retention_work(&self) -> bool {
        self.work_scheduler.lock().await.try_reserve(
            crate::work_scheduler::WorkClass::Retention,
            0,
            0,
        )
    }

    pub(super) async fn release_retention_work(&self) {
        self.work_scheduler
            .lock()
            .await
            .release(crate::work_scheduler::WorkClass::Retention, 0, 0);
    }

    pub async fn begin_partition_expansion(
        &self,
        stream: &str,
        partitions: u32,
    ) -> Result<crate::partition_expansion::ExpansionPlan> {
        let mut expansions = self.partition_expansions.lock().await;
        let plan = expansions
            .get_mut(stream)
            .ok_or_else(|| BrokerError::msg("unknown stream"))?
            .begin(partitions)
            .cloned()
            .ok_or_else(|| {
                BrokerError::msg("partition expansion is already pending or not larger")
            })?;
        self.persist_partition_expansions(&expansions)?;
        Ok(plan)
    }

    pub async fn mark_partition_expansion_prepared(
        &self,
        stream: &str,
        prepared_partitions: u32,
    ) -> Result<()> {
        let mut expansions = self.partition_expansions.lock().await;
        crate::broker_ensure!(
            expansions
                .get_mut(stream)
                .ok_or_else(|| BrokerError::msg("unknown stream"))?
                .mark_prepared(prepared_partitions),
            "invalid partition expansion preparation progress"
        );
        self.persist_partition_expansions(&expansions)?;
        Ok(())
    }

    pub async fn activate_partition_expansion(&self, stream: &str) -> Result<(u32, u64)> {
        let mut expansions = self.partition_expansions.lock().await;
        let expansion = expansions
            .get_mut(stream)
            .ok_or_else(|| BrokerError::msg("unknown stream"))?;
        let (current_partitions, _) = expansion.current();
        let target_partitions = expansion
            .pending()
            .map(|plan| plan.to_partitions)
            .ok_or_else(|| BrokerError::msg("partition expansion is not pending"))?;
        for partition in current_partitions..target_partitions {
            self.partition_logs
                .activate_partition(stream, crate::stream::PartitionId(partition))?;
        }
        crate::broker_ensure!(
            expansion.activate(),
            "partition expansion is not fully prepared"
        );
        let current = expansion.current();
        self.persist_partition_expansions(&expansions)?;
        Ok(current)
    }

    pub async fn partition_expansion_epoch(&self, stream: &str, epoch: u64) -> Result<bool> {
        let expansions = self.partition_expansions.lock().await;
        let expansion = expansions
            .get(stream)
            .ok_or_else(|| BrokerError::msg("unknown stream"))?;
        Ok(matches!(
            expansion.decide(epoch),
            crate::partition_expansion::EpochDecision::Current
        ))
    }

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

    pub fn export_audit_log(&self) -> Result<Vec<u8>> {
        self.audit
            .lock()
            .expect("audit log lock poisoned")
            .export_json()
    }

    pub fn verify_audit_log(&self) -> Result<()> {
        self.audit.lock().expect("audit log lock poisoned").verify()
    }

    pub fn audit_status(&self) -> crate::tenancy::AuditStatus {
        self.audit.lock().expect("audit log lock poisoned").status()
    }

    pub fn policy_snapshot(&self) -> crate::tenancy::PolicySnapshot {
        self.policy.snapshot()
    }

    pub fn policy_snapshot_for_scope(
        &self,
        scope: &crate::tenancy::ResourceScope,
    ) -> crate::tenancy::PolicySnapshot {
        self.policy.snapshot_for_scope(scope)
    }

    pub fn audit_records_for_tenant(
        &self,
        tenant: &crate::tenancy::TenantId,
    ) -> Vec<crate::tenancy::AuditRecord> {
        self.audit
            .lock()
            .expect("audit log lock poisoned")
            .records_for_tenant(tenant)
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
        })?;
        Ok(())
    }

    pub async fn replace_policy_snapshot_replicated(
        &self,
        snapshot: crate::tenancy::PolicySnapshot,
    ) -> Result<()> {
        let Some(cluster) = self.cluster_runtime().await else {
            return self.replace_policy_snapshot(snapshot);
        };
        let generation = snapshot.generation;
        let response = self
            .cluster_write(
                &cluster,
                crate::raft::BrokerCommand::PolicyReplace { snapshot },
            )
            .await?;
        crate::broker_ensure!(
            matches!(
                response,
                crate::raft::BrokerResponse::PolicyReplace { generation: applied }
                    if applied == generation
            ),
            "cluster rejected policy replacement"
        );
        Ok(())
    }
}
