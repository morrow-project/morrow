use super::delivery_index::scheduled_at_ms;
use super::*;

impl Morrow {
    pub(super) async fn rebuild_tenant_disk_usage(&self) -> Result<()> {
        let records = self
            .inner
            .lock()
            .await
            .messages
            .values()
            .filter(|record| record.stream.is_some())
            .cloned()
            .collect::<Vec<_>>();
        let mut disk_usage = HashMap::new();
        for record in records {
            let record = self.partition_logs.load_record(&record)?;
            let tenant = self
                .config
                .tenant_quotas
                .keys()
                .find(|tenant| record.subject.starts_with(&format!("{tenant}.")))
                .map_or(crate::quota::DEFAULT_TENANT, String::as_str);
            let entry = disk_usage.entry(tenant.to_string()).or_insert(0u64);
            *entry = entry.saturating_add(crate::quota::persistent_publish_record_bytes(&record));
        }
        self.tenant_quotas.replace_disk_usage(disk_usage);
        Ok(())
    }

    pub(super) async fn apply_materialized_views(
        &self,
        record: &crate::wal::PublishRecord,
    ) -> Result<()> {
        let mut views = self.views.lock().await;
        for runtime in views.values_mut() {
            if runtime.paused {
                continue;
            }
            if let Some(update) = crate::broker::broker::view_update(&runtime.definition, record) {
                runtime.view.apply(update)?;
            }
        }
        Ok(())
    }

    pub(super) async fn views_response(&self) -> Vec<crate::broker::state::ViewStatusResponse> {
        self.views
            .lock()
            .await
            .values()
            .map(|runtime| crate::broker::state::ViewStatusResponse {
                name: runtime.view.name().to_string(),
                tenant: runtime.view.tenant().to_string(),
                source_stream: runtime.definition.source_stream.clone(),
                paused: runtime.paused,
                entries: runtime.view.entry_count(),
                positions: runtime.view.consistency_positions(),
            })
            .collect()
    }

    pub(super) async fn view_query(
        &self,
        name: &str,
        key: &str,
    ) -> Option<crate::broker::state::ViewQueryResponse> {
        self.views
            .lock()
            .await
            .get(name)
            .map(|runtime| crate::broker::state::ViewQueryResponse {
                name: name.to_string(),
                tenant: runtime.view.tenant().to_string(),
                key: key.to_string(),
                value: runtime.view.point_read(key).map(ToOwned::to_owned),
                positions: runtime.view.consistency_positions(),
            })
    }

    pub(super) async fn view_watch(
        &self,
        name: &str,
        since: u64,
    ) -> Result<Option<crate::broker::state::ViewWatchResponse>> {
        let views = self.views.lock().await;
        let Some(runtime) = views.get(name) else {
            return Ok(None);
        };
        Ok(Some(crate::broker::state::ViewWatchResponse {
            name: name.to_string(),
            tenant: runtime.view.tenant().to_string(),
            since,
            events: runtime.view.watch_from(since)?,
            positions: runtime.view.consistency_positions(),
        }))
    }

    pub(super) async fn create_view(
        &self,
        name: &str,
        request: crate::broker::state::ViewCreateRequest,
    ) -> Result<bool> {
        let tenant = request.tenant.clone();
        let definition = crate::config::ViewConfig {
            tenant: request.tenant,
            source_stream: request.source_stream,
            source_subject: request.source_subject,
            key_header: request.key_header,
            max_entries: request.max_entries,
            max_value_bytes: request.max_value_bytes,
            watch_capacity: request.watch_capacity,
        };
        crate::tenancy::TenantId::new(definition.tenant.clone())?;
        crate::stream::StreamId::new(definition.source_stream.clone())?;
        crate::broker_ensure!(
            !name.is_empty()
                && name.len() <= 128
                && name.bytes().all(|byte| {
                    byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.')
                }),
            "invalid view name"
        );
        if let Some(subject) = &definition.source_subject {
            crate::broker_ensure!(
                protocol::subject::validate_subscription(subject),
                "invalid view source subject"
            );
        }
        crate::broker_ensure!(
            definition.max_entries > 0
                && definition.max_value_bytes > 0
                && definition.watch_capacity > 0,
            "view limits must be greater than zero"
        );
        crate::broker_ensure!(
            self.config
                .streams
                .definitions()
                .iter()
                .any(|stream| stream.name.as_str() == definition.source_stream),
            "view source stream is not configured"
        );
        let mut views = self.views.lock().await;
        if views.contains_key(name) {
            return Ok(false);
        }
        let path = self
            .config
            .wal_dir
            .join("views")
            .join(&definition.tenant)
            .join(format!("{name}.json"));
        let view = crate::materialized_view::MaterializedView::open(
            path,
            &definition.tenant,
            name,
            crate::materialized_view::ViewLimits {
                max_entries: definition.max_entries,
                max_value_bytes: definition.max_value_bytes,
                watch_capacity: definition.watch_capacity,
            },
        )?;
        let definition_path = self.view_definition_path(&definition.tenant, name);
        if let Some(parent) = definition_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let definition_body = serde_json::to_vec(&definition)
            .map_err(|error| BrokerError::with_source("encoding view definition", error))?;
        std::fs::write(&definition_path, definition_body)?;
        views.insert(
            name.to_string(),
            crate::broker::broker::ViewRuntime {
                definition,
                view,
                paused: false,
            },
        );
        self.record_audit_event(crate::tenancy::AuditEvent {
            sequence: 0,
            timestamp_ms: self.hooks.clock.now_ms(),
            actor: "admin".to_string(),
            tenant: crate::tenancy::TenantId::new(tenant).ok(),
            action: "view.create".to_string(),
            resource: format!("view/{name}"),
            outcome: "success".to_string(),
            details: std::collections::BTreeMap::new(),
        });
        Ok(true)
    }

    fn view_definition_path(&self, tenant: &str, name: &str) -> std::path::PathBuf {
        self.config
            .wal_dir
            .join("views")
            .join(tenant)
            .join(format!("{name}.definition.json"))
    }

    pub fn open(config: Config) -> Result<Self> {
        Self::open_with_hooks(config, BrokerHooks::default())
    }

    pub(crate) fn open_with_hooks(config: Config, hooks: BrokerHooks) -> Result<Self> {
        config.validate()?;
        let encryption = config.storage_encryption()?;
        let (mut wal, mut replay) = Wal::open_with_encryption(
            &config.wal_dir,
            config.fsync_interval(),
            config.wal_segment_bytes,
            encryption.clone(),
        )?;
        if let Some(cluster) = &config.cluster {
            wal.namespace_delivery_ids(cluster.node_id);
        }
        let (partition_logs, mut envelopes) = PartitionLogSet::open_with_encryption(
            &config.wal_dir,
            &config.streams,
            config.wal_segment_bytes,
            encryption,
        )?;
        partition_logs.enforce_retention(&mut envelopes, &config.streams, hooks.clock.now_ms())?;
        let mut envelope_seqs = envelopes
            .iter()
            .map(|envelope| envelope.legacy_seq)
            .collect::<HashSet<_>>();
        let legacy_stream_records = replay
            .messages
            .values()
            .filter(|record| record.stream.is_some())
            .cloned()
            .collect::<Vec<_>>();
        for record in legacy_stream_records {
            let stream_name = record.stream.as_deref().unwrap();
            let stream = config
                .streams
                .definitions()
                .iter()
                .find(|stream| stream.name.as_str() == stream_name)
                .ok_or_else(|| {
                    BrokerError::msg(format!(
                        "legacy WAL record {} references unconfigured stream {stream_name}",
                        record.seq
                    ))
                })?;
            if !envelope_seqs.contains(&record.seq) {
                let envelope = partition_logs.append(AppendRequest {
                    namespace: DEFAULT_NAMESPACE,
                    stream,
                    subject: &record.subject,
                    key: record.key.as_deref(),
                    partition_hint: record.partition.map(crate::stream::PartitionId),
                    headers: &record.headers,
                    timestamp_ms: record.timestamp_ms,
                    reply_to: record.reply_to.as_deref(),
                    payload: &record.payload,
                    leader_epoch: record.leader_epoch,
                    legacy_seq: Some(record.seq),
                })?;
                envelope_seqs.insert(record.seq);
                envelopes.push(envelope);
            }
        }
        partition_logs.flush()?;
        for envelope in &envelopes {
            wal.observe_publish_seq(envelope.legacy_seq);
            if !replay.partition_appends.contains_key(&envelope.legacy_seq) {
                let record = PartitionAppendRecord::from(envelope);
                wal.append_partition_append(&record)?;
                replay.partition_appends.insert(record.seq, record);
            }
        }
        let envelope_by_seq = envelopes
            .into_iter()
            .map(|envelope| (envelope.legacy_seq, envelope))
            .collect::<HashMap<_, _>>();
        let recovered_envelopes = envelope_by_seq.values().cloned().collect::<Vec<_>>();
        let compaction_latest = reconcile_replayed_compaction(
            &mut replay,
            envelope_by_seq,
            &partition_logs,
            &config.streams,
        )?;
        let tls_acceptor = config
            .tls
            .as_ref()
            .map(crate::tls::load_acceptor)
            .transpose()?;
        let admin_tls_acceptor = config
            .admin_tls
            .as_ref()
            .map(crate::tls::load_acceptor)
            .transpose()?;
        let websocket_tls_acceptor = config
            .websocket
            .as_ref()
            .and_then(|websocket| websocket.tls.as_ref())
            .map(crate::tls::load_acceptor)
            .transpose()?;
        let consumers: HashMap<_, _> = replay
            .consumers
            .into_iter()
            .map(|(id, consumer)| {
                (
                    id,
                    Consumer::from_replay(
                        consumer,
                        &config.streams,
                        &replay.messages,
                        &partition_logs,
                    ),
                )
            })
            .collect();
        let mut consumer_interest_index = subject::SubjectTrie::default();
        for (consumer_id, consumer) in &consumers {
            consumer_interest_index.insert(&consumer.record.filter_subject, consumer_id.clone());
        }
        let partition_sequences = replay
            .messages
            .values()
            .filter_map(|record| {
                Some((
                    (record.stream.clone()?, record.partition?, record.offset?),
                    record.seq,
                ))
            })
            .collect();
        let ready_consumers = consumers.keys().cloned().collect();
        let lease_deadlines = consumers
            .iter()
            .flat_map(|(consumer_id, consumer)| {
                consumer.in_flight.iter().map(|(seq, lease)| {
                    Reverse(LeaseDeadline {
                        deadline_ms: lease.deadline_ms,
                        consumer_id: consumer_id.clone(),
                        seq: *seq,
                        delivery_id: lease.delivery_id,
                    })
                })
            })
            .collect();
        let scheduled_deliveries = replay
            .messages
            .values()
            .filter_map(|record| {
                scheduled_at_ms(record).map(|scheduled_at_ms| {
                    Reverse(ScheduledDelivery {
                        scheduled_at_ms,
                        seq: record.seq,
                    })
                })
            })
            .collect();
        let dead_letters = replay.dead_letters.into_iter().collect();
        let mut producer_epochs: HashMap<String, u64> = HashMap::new();
        let producer_sequences = replay
            .producer_sequences
            .into_iter()
            .map(|(identity, record)| {
                producer_epochs
                    .entry(record.producer_id.clone())
                    .and_modify(|epoch| *epoch = (*epoch).max(record.epoch))
                    .or_insert(record.epoch);
                (
                    identity,
                    ProducerDedupEntry {
                        fingerprint: record.fingerprint,
                        record: record.record,
                    },
                )
            })
            .collect();
        let groups = replay
            .groups
            .into_iter()
            .map(|(group, record)| {
                crate::consumer_group::GroupCoordinator::from_record(record)
                    .map(|coordinator| (group, coordinator))
                    .with_context(|| "replaying consumer-group state".to_string())
            })
            .collect::<Result<HashMap<_, _>>>()?;
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
        let quotas = Arc::new(crate::quota::QuotaRuntime::new(&config.quotas));
        let tenant_quotas =
            crate::quota::TenantQuotaRuntime::new(crate::quota::TenantQuotaLimits {
                max_connections: config.quotas.max_connections,
                max_memory_bytes: (config.quotas.max_outbound_bytes_per_connection as u64)
                    .saturating_mul(config.quotas.max_connections as u64),
                max_disk_bytes: u64::MAX,
                max_tasks: config.quotas.max_durable_consumers,
                max_background_tasks: config.quotas.max_transient_subscriptions,
            });
        for (tenant, limits) in &config.tenant_quotas {
            tenant_quotas.set_tenant_limits(
                tenant,
                crate::quota::TenantQuotaLimits {
                    max_connections: limits.max_connections,
                    max_memory_bytes: limits.max_memory_bytes,
                    max_disk_bytes: limits.max_disk_bytes,
                    max_tasks: limits.max_tasks,
                    max_background_tasks: limits.max_background_tasks,
                },
            );
        }
        for record in &recovered_envelopes {
            let tenant = config
                .tenant_quotas
                .keys()
                .find(|tenant| record.subject.starts_with(&format!("{tenant}.")))
                .map_or(crate::quota::DEFAULT_TENANT, String::as_str);
            let bytes = crate::quota::persistent_record_bytes(record);
            if bytes > 0 {
                crate::broker_ensure!(
                    tenant_quotas.try_reserve(
                        tenant,
                        crate::quota::TenantQuotaUsage {
                            disk_bytes: bytes,
                            ..Default::default()
                        }
                    ),
                    "tenant durable disk quota exceeded while rebuilding usage"
                );
            }
        }
        for consumer in consumers.values() {
            let tenant = config
                .tenant_quotas
                .keys()
                .find(|tenant| {
                    consumer
                        .record
                        .filter_subject
                        .starts_with(&format!("{tenant}."))
                })
                .map_or(crate::quota::DEFAULT_TENANT, String::as_str);
            crate::broker_ensure!(
                tenant_quotas.try_reserve(
                    tenant,
                    crate::quota::TenantQuotaUsage {
                        background_tasks: 1,
                        ..Default::default()
                    }
                ),
                "tenant background task quota exceeded while rebuilding usage"
            );
        }
        let mut view_definitions = config.views.clone();
        let definitions_root = config.wal_dir.join("views");
        if definitions_root.is_dir() {
            for tenant_dir in std::fs::read_dir(&definitions_root)? {
                let tenant_dir = tenant_dir?.path();
                if !tenant_dir.is_dir() {
                    continue;
                }
                for definition_file in std::fs::read_dir(&tenant_dir)? {
                    let definition_file = definition_file?.path();
                    if definition_file
                        .extension()
                        .and_then(|extension| extension.to_str())
                        != Some("json")
                        || !definition_file
                            .file_name()
                            .and_then(|name| name.to_str())
                            .is_some_and(|name| name.ends_with(".definition.json"))
                    {
                        continue;
                    }
                    let name = definition_file
                        .file_name()
                        .and_then(|name| name.to_str())
                        .and_then(|name| name.strip_suffix(".definition.json"))
                        .ok_or_else(|| BrokerError::msg("invalid persisted view definition"))?;
                    let definition: crate::config::ViewConfig = serde_json::from_slice(
                        &std::fs::read(&definition_file)?,
                    )
                    .map_err(|error| {
                        BrokerError::with_source("decoding persisted view definition", error)
                    })?;
                    view_definitions.insert(name.to_string(), definition);
                }
            }
        }
        let mut views = HashMap::new();
        for (name, definition) in &view_definitions {
            let path = config
                .wal_dir
                .join("views")
                .join(&definition.tenant)
                .join(format!("{name}.json"));
            let mut view = crate::materialized_view::MaterializedView::open(
                path,
                &definition.tenant,
                name,
                crate::materialized_view::ViewLimits {
                    max_entries: definition.max_entries,
                    max_value_bytes: definition.max_value_bytes,
                    watch_capacity: definition.watch_capacity,
                },
            )?;
            let mut records = replay
                .messages
                .values()
                .filter(|record| {
                    record.stream.as_deref() == Some(definition.source_stream.as_str())
                })
                .cloned()
                .collect::<Vec<_>>();
            records.sort_by_key(|record| {
                (
                    record.partition.unwrap_or_default(),
                    record.offset.unwrap_or_default(),
                )
            });
            for record in records {
                let record = partition_logs.load_record(&record)?;
                if let Some(update) = crate::broker::broker::view_update(definition, &record) {
                    view.apply(update)?;
                }
            }
            views.insert(
                name.clone(),
                crate::broker::broker::ViewRuntime {
                    definition: definition.clone(),
                    view,
                    paused: false,
                },
            );
        }
        let policy = Arc::new(crate::tenancy::PolicyStore::default());
        let audit = Arc::new(std::sync::Mutex::new(
            crate::tenancy::AuditLog::open_with_segment_bytes(
                config.wal_dir.join("audit.log"),
                config.audit_max_records,
                config.audit_segment_bytes,
            )?,
        ));
        let transaction_limits = crate::transaction::TransactionLimits {
            max_messages: 10_000,
            max_bytes: 64 * 1_048_576,
            max_partitions: 128,
            max_duration_ms: 300_000,
            max_concurrent: 1_024,
        };
        let mut transactions = crate::transaction::TransactionCoordinator::open(
            config.wal_dir.join("transactions.json"),
            transaction_limits,
        )?;
        transactions.recover(hooks.clock.now_ms())?;
        let schema_registry =
            crate::schema_registry::SchemaRegistry::open(config.wal_dir.join("schemas.json"))?;
        for (index, (subject, client)) in config.auth.clients.iter().enumerate() {
            let role_name = format!("static-client-{index}");
            let mut permissions = std::collections::BTreeSet::new();
            permissions.insert(crate::tenancy::Permission::Publish);
            permissions.insert(crate::tenancy::Permission::Subscribe);
            policy.upsert_role(crate::tenancy::Role {
                name: role_name.clone(),
                permissions,
            })?;
            let scope = crate::tenancy::ResourceScope {
                tenant: crate::tenancy::TenantId::new(client.tenant.clone())?,
                namespace: crate::tenancy::NamespaceId::new(client.namespace.clone())?,
            };
            policy.bind(crate::tenancy::RoleBinding {
                subject: client
                    .external_subject
                    .clone()
                    .unwrap_or_else(|| subject.clone()),
                scope,
                role: role_name,
                expires_at_ms: None,
            })?;
        }
        let route_mesh = RouteMesh::from_config(&config, quotas.clone())?;
        let broker_capacities = config
            .cluster
            .as_ref()
            .map(|cluster| {
                cluster
                    .nodes
                    .iter()
                    .map(|node| crate::reassignment::BrokerCapacity {
                        node_id: node.node_id,
                        region: "default".to_string(),
                        zone: "default".to_string(),
                        disk_capacity_bytes: u64::MAX,
                        disk_used_bytes: 0,
                        partition_count: 0,
                        leader_count: 0,
                        throughput_bytes_per_second: u64::MAX,
                        max_concurrent_moves: 1,
                        lifecycle: crate::reassignment::BrokerLifecycle::Active,
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_else(|| {
                vec![crate::reassignment::BrokerCapacity {
                    node_id: 1,
                    region: "default".to_string(),
                    zone: "default".to_string(),
                    disk_capacity_bytes: u64::MAX,
                    disk_used_bytes: 0,
                    partition_count: 0,
                    leader_count: 0,
                    throughput_bytes_per_second: u64::MAX,
                    max_concurrent_moves: 1,
                    lifecycle: crate::reassignment::BrokerLifecycle::Active,
                }]
            });
        let reassignment = crate::reassignment::ReassignmentController::open(
            config.wal_dir.join("reassignments.json"),
            broker_capacities,
        )?;
        let cross_region = crate::cross_region::CrossRegionReplicator::open(
            config.wal_dir.join("cross-region.json"),
            crate::cross_region::ReplicationPolicy {
                max_lag_offsets: 10_000,
                max_bandwidth_bytes_per_second: 64 * 1_048_576,
                max_in_flight_chunks: 16,
            },
        )?;
        let wal = WalRuntime::new(wal);
        Ok(Self {
            inner: Arc::new(Mutex::new(DurableBrokerState {
                wal: wal.clone(),
                consumers,
                consumer_interest_index,
                messages: replay.messages,
                partition_sequences,
                ready_consumers,
                lease_deadlines,
                scheduled_deliveries,
                dead_letters,
                producer_epochs,
                producer_sequences,
                producer_in_flight: HashSet::new(),
                acked_cleanup: HashSet::new(),
                compaction_latest,
                superseded_since_compaction: 0,
            })),
            wal,
            partition_logs: Arc::new(partition_logs),
            storage_permits: Arc::new(tokio::sync::Semaphore::new(MAX_BLOCKING_STORAGE_OPS)),
            storage_gate: Arc::new(tokio::sync::RwLock::new(())),
            connections: Arc::new(Mutex::new(ConnectionState {
                clients: HashMap::new(),
            })),
            transient: Arc::new(Mutex::new(TransientState {
                subscriptions: HashMap::new(),
                interest_index: subject::SubjectTrie::default(),
                route_interest_counts: BTreeMap::new(),
            })),
            groups: Arc::new(Mutex::new(groups)),
            group_sessions: Arc::new(Mutex::new(HashMap::new())),
            next_connection_id: Arc::new(AtomicU64::new(1)),
            config,
            tls_acceptor,
            admin_tls_acceptor,
            websocket_tls_acceptor,
            quotas,
            tenant_quotas,
            policy,
            audit,
            schema_registry: Arc::new(Mutex::new(schema_registry)),
            cluster: Arc::new(Mutex::new(cluster)),
            cluster_applied_index: Arc::new(AtomicU64::new(0)),
            local_partition_applied: Arc::new(Mutex::new(HashMap::new())),
            cluster_delta_gate: Arc::new(Mutex::new(())),
            cluster_application_metrics: Arc::new(ClusterApplicationMetrics::default()),
            metrics: Arc::new(BrokerMetrics::default()),
            metrics_snapshot: Arc::new(tokio::sync::RwLock::new(None)),
            metrics_refreshing: Arc::new(AtomicBool::new(false)),
            storage_failure: Arc::new(AtomicBool::new(false)),
            audit_failure: Arc::new(AtomicBool::new(false)),
            shutting_down: Arc::new(AtomicBool::new(false)),
            redelivery_notify: Arc::new(Notify::new()),
            pull_waiters: PullWaiterRegistry::default(),
            broker_control: BrokerControlRegistry::new(),
            compaction_running: Arc::new(AtomicBool::new(false)),
            route_mesh,
            middleware: hooks.middleware.clone(),
            hooks,
            transactions: Arc::new(Mutex::new(transactions)),
            views: Arc::new(Mutex::new(views)),
            reassignment: Arc::new(Mutex::new(reassignment)),
            cross_region: Arc::new(Mutex::new(cross_region)),
        })
    }

    pub async fn transaction_begin(
        &self,
        id: impl Into<String>,
        tenant: impl Into<String>,
        producer: impl Into<String>,
        producer_epoch: u64,
    ) -> Result<()> {
        self.transactions.lock().await.begin(
            id,
            tenant,
            producer,
            producer_epoch,
            self.hooks.clock.now_ms(),
        )
    }

    pub async fn transaction_append(
        &self,
        id: &str,
        write: crate::transaction::TransactionWrite,
    ) -> Result<()> {
        self.transactions
            .lock()
            .await
            .append(id, write, self.hooks.clock.now_ms())
    }

    pub async fn transaction_prepare(&self, id: &str) -> Result<()> {
        self.transactions
            .lock()
            .await
            .prepare(id, self.hooks.clock.now_ms())
    }

    pub async fn transaction_commit(&self, id: &str) -> Result<crate::transaction::CommittedBatch> {
        self.transactions
            .lock()
            .await
            .commit(id, self.hooks.clock.now_ms())
    }

    pub async fn transaction_abort(&self, id: &str, reason: &str) -> Result<()> {
        self.transactions.lock().await.abort(id, reason)
    }

    pub async fn transaction_status(
        &self,
        id: &str,
    ) -> Option<crate::transaction::TransactionStatus> {
        self.transactions.lock().await.status(id).cloned()
    }

    pub async fn cross_region_promote(&self, expected_token: u64) -> Result<u64> {
        self.cross_region.lock().await.promote(expected_token)
    }

    pub async fn cross_region_fence(&self) -> Result<u64> {
        self.cross_region.lock().await.fence()
    }

    pub async fn cross_region_status(
        &self,
        stream: &str,
        partition: crate::stream::PartitionId,
        primary_high_watermark: u64,
    ) -> crate::cross_region::ReplicationStatus {
        self.cross_region
            .lock()
            .await
            .status(stream, partition, primary_high_watermark)
    }

    pub(super) async fn set_view_paused(&self, name: &str, paused: bool) -> bool {
        let mut views = self.views.lock().await;
        let Some(runtime) = views.get_mut(name) else {
            return false;
        };
        runtime.paused = paused;
        self.record_audit_event(crate::tenancy::AuditEvent {
            sequence: 0,
            timestamp_ms: self.hooks.clock.now_ms(),
            actor: "admin".to_string(),
            tenant: crate::tenancy::TenantId::new(runtime.view.tenant().to_string()).ok(),
            action: if paused {
                "view.pause".to_string()
            } else {
                "view.resume".to_string()
            },
            resource: format!("view/{name}"),
            outcome: "success".to_string(),
            details: std::collections::BTreeMap::new(),
        });
        true
    }

    pub(super) async fn rebuild_view(&self, name: &str) -> Result<bool> {
        let definition = {
            let views = self.views.lock().await;
            let Some(runtime) = views.get(name) else {
                return Ok(false);
            };
            runtime.definition.clone()
        };
        let mut records = self
            .inner
            .lock()
            .await
            .messages
            .values()
            .filter(|record| record.stream.as_deref() == Some(definition.source_stream.as_str()))
            .cloned()
            .collect::<Vec<_>>();
        records.sort_by_key(|record| {
            (
                record.partition.unwrap_or_default(),
                record.offset.unwrap_or_default(),
            )
        });
        let mut updates = Vec::new();
        for record in records {
            let record = self.partition_logs.load_record(&record)?;
            if let Some(update) = crate::broker::broker::view_update(&definition, &record) {
                updates.push(update);
            }
        }
        let mut views = self.views.lock().await;
        let Some(runtime) = views.get_mut(name) else {
            return Ok(false);
        };
        runtime.view.rebuild(&updates)?;
        self.record_audit_event(crate::tenancy::AuditEvent {
            sequence: 0,
            timestamp_ms: self.hooks.clock.now_ms(),
            actor: "admin".to_string(),
            tenant: crate::tenancy::TenantId::new(runtime.view.tenant().to_string()).ok(),
            action: "view.rebuild".to_string(),
            resource: format!("view/{name}"),
            outcome: "success".to_string(),
            details: [("updates".to_string(), updates.len().to_string())]
                .into_iter()
                .collect(),
        });
        Ok(true)
    }

    pub(super) async fn delete_view(&self, name: &str) -> bool {
        let mut views = self.views.lock().await;
        let Some(runtime) = views.remove(name) else {
            return false;
        };
        let _ = std::fs::remove_file(self.view_definition_path(runtime.view.tenant(), name));
        let _ = std::fs::remove_file(
            self.config
                .wal_dir
                .join("views")
                .join(runtime.view.tenant())
                .join(format!("{name}.json")),
        );
        self.record_audit_event(crate::tenancy::AuditEvent {
            sequence: 0,
            timestamp_ms: self.hooks.clock.now_ms(),
            actor: "admin".to_string(),
            tenant: crate::tenancy::TenantId::new(runtime.view.tenant().to_string()).ok(),
            action: "view.delete".to_string(),
            resource: format!("view/{name}"),
            outcome: "success".to_string(),
            details: std::collections::BTreeMap::new(),
        });
        true
    }

    pub async fn reassignment_plan(
        &self,
        move_: crate::reassignment::PlacementMove,
        source_epoch: u64,
    ) -> Result<u64> {
        self.reassignment.lock().await.begin(move_, source_epoch)
    }

    pub async fn reassignment_advance(
        &self,
        id: u64,
        progress: crate::reassignment::ReassignmentProgress,
    ) -> Result<crate::reassignment::ReassignmentPhase> {
        self.reassignment.lock().await.advance(id, progress)
    }

    pub async fn reassignment_rollback(&self, id: u64, reason: &str) -> Result<()> {
        self.reassignment.lock().await.rollback(id, reason)
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

    pub(super) async fn serve_inner(
        self,
        listener: TcpListener,
        handle_shutdown: bool,
    ) -> Result<()> {
        #[cfg(unix)]
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .context("installing SIGTERM handler")?;
        let http_listener = match self.config.http_listen {
            Some(listen) => Some(
                TcpListener::bind(listen)
                    .await
                    .with_context(|| format!("binding HTTP status listener {listen}"))?,
            ),
            None => None,
        };
        let websocket_listener = match self.config.websocket.as_ref() {
            Some(config) => Some(
                TcpListener::bind(config.listen)
                    .await
                    .with_context(|| format!("binding WebSocket listener {}", config.listen))?,
            ),
            None => None,
        };
        let route_listener = match self
            .config
            .cluster
            .as_ref()
            .and_then(|cluster| cluster.route_listen)
        {
            Some(listen) => Some(
                TcpListener::bind(listen)
                    .await
                    .with_context(|| format!("binding route listener {listen}"))?,
            ),
            None => None,
        };
        let raft_listener = match self.config.cluster.as_ref() {
            Some(cluster) => Some(
                TcpListener::bind(cluster.raft_listen)
                    .await
                    .with_context(|| format!("binding Raft listener {}", cluster.raft_listen))?,
            ),
            None => None,
        };

        self.start_cluster(raft_listener).await?;
        self.start_route_mesh(route_listener).await?;
        self.log_cluster_event("server started").await;
        self.spawn_cluster_log_monitor();
        self.spawn_http_status_listener(http_listener);
        self.spawn_websocket_listener(websocket_listener).await?;
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
                    signal = async {
                        #[cfg(unix)]
                        {
                            tokio::select! {
                                signal = tokio::signal::ctrl_c() => signal,
                                _ = sigterm.recv() => Ok(()),
                            }
                        }
                        #[cfg(not(unix))]
                        {
                            tokio::signal::ctrl_c().await
                        }
                    } => {
                        signal.context("waiting for shutdown signal")?;
                        self.shutting_down.store(true, Ordering::Release);
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
        self.shutting_down.store(true, Ordering::Release);
        self.pull_waiters.shutdown();
        let _shutdown = self.storage_gate.write().await;
        let inner = self.inner.lock().await;
        let partition_logs = self.partition_logs.clone();
        let permit = self
            .storage_permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| BrokerError::msg("storage worker pool closed"))?;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            partition_logs.flush()
        })
        .await
        .map_err(|err| BrokerError::with_source("partition flush worker failed", err))??;
        let messages = inner.messages.values().cloned().collect::<Vec<_>>();
        let consumers = inner.replayed_consumers();
        let dead_letters = inner.dead_letters.values().cloned().collect::<Vec<_>>();
        let producer_sequences = inner
            .producer_sequences
            .iter()
            .map(
                |((producer_id, epoch, sequence), entry)| ProducerSequenceRecord {
                    producer_id: producer_id.clone(),
                    epoch: *epoch,
                    sequence: *sequence,
                    fingerprint: entry.fingerprint,
                    record: entry.record.clone(),
                },
            )
            .collect::<Vec<_>>();
        drop(inner);
        let groups = self
            .groups
            .lock()
            .await
            .iter()
            .map(|(group, coordinator)| (group.clone(), coordinator.record()))
            .collect::<Vec<_>>();
        self.wal
            .checkpoint(
                messages,
                consumers,
                dead_letters,
                producer_sequences,
                groups,
            )
            .await?;
        self.wal.flush().await?;
        Ok(())
    }

    pub async fn cluster_leader(&self) -> Option<u64> {
        self.cluster_runtime().await?.current_leader().await
    }

    pub(super) async fn health_response(&self) -> HealthResponse {
        let cluster = self.cluster_response().await;
        let route_degraded = cluster
            .routes
            .as_ref()
            .is_some_and(|routes| !routes.seeds.is_empty() && routes.connected.is_empty());
        let quorum_lost = if cluster.cluster_status == "ready" {
            match self.cluster_runtime().await {
                Some(cluster) => !cluster.quorum_available().await,
                None => false,
            }
        } else {
            false
        };
        let (status, reason) = if self.shutting_down.load(Ordering::Acquire) {
            ("degraded", Some("shutting_down"))
        } else if self.storage_failure.load(Ordering::Relaxed) {
            ("degraded", Some("storage_failure"))
        } else if self.audit_failure.load(Ordering::Relaxed) {
            ("degraded", Some("audit_failure"))
        } else if route_degraded {
            ("degraded", Some("route_degraded"))
        } else if quorum_lost {
            ("degraded", Some("quorum_loss"))
        } else if cluster.cluster_status == "standalone" {
            ("ready", None)
        } else if cluster.cluster_status == "ready" {
            ("ready", None)
        } else {
            ("forming", Some("leader_election"))
        };
        HealthResponse {
            status,
            cluster_status: cluster.cluster_status,
            role: cluster.role,
            reason,
        }
    }

    pub(super) async fn metrics_response(&self) -> String {
        const SNAPSHOT_TTL: std::time::Duration = std::time::Duration::from_secs(1);
        if let Some((refreshed_at, body)) = self.metrics_snapshot.read().await.clone() {
            if refreshed_at.elapsed() < SNAPSHOT_TTL {
                return body.to_string();
            }
            if self
                .metrics_refreshing
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                let broker = self.clone();
                tokio::spawn(async move {
                    let body = broker.collect_metrics_response().await;
                    *broker.metrics_snapshot.write().await =
                        Some((std::time::Instant::now(), Arc::from(body)));
                    broker.metrics_refreshing.store(false, Ordering::Release);
                });
            }
            return body.to_string();
        }
        let body = self.collect_metrics_response().await;
        *self.metrics_snapshot.write().await =
            Some((std::time::Instant::now(), Arc::from(body.clone())));
        body
    }

    async fn collect_metrics_response(&self) -> String {
        let connections = self.connections.lock().await.clients.len();
        let transient_subscriptions = self.transient.lock().await.subscriptions.len();
        let groups = self.groups.lock().await;
        let group_count = groups.len();
        let group_members = groups
            .values()
            .map(|group| group.snapshot().members.len())
            .sum::<usize>();
        let group_moved_partitions = groups
            .values()
            .filter_map(|group| group.snapshot().rebalance)
            .map(|rebalance| rebalance.moved_partitions.len())
            .sum::<usize>();
        drop(groups);
        let inner = self.inner.lock().await;
        let wal = inner
            .wal
            .status(inner.messages.len(), inner.consumers.len());
        let consumers = inner.consumers.len();
        let pending_deliveries = inner
            .consumers
            .values()
            .map(|consumer| consumer.pending.len())
            .sum::<usize>();
        let consumer_lag_messages = inner
            .consumers
            .values()
            .map(|consumer| consumer.pending.len() + consumer.in_flight.len())
            .sum::<usize>();
        let in_flight_deliveries = inner
            .consumers
            .values()
            .map(|consumer| consumer.in_flight.len())
            .sum::<usize>();
        let scheduled_depth = inner.scheduled_deliveries.len();
        let scheduled_due_lag_ms = inner
            .scheduled_deliveries
            .peek()
            .map(|entry| {
                self.hooks
                    .clock
                    .now_ms()
                    .saturating_sub(entry.0.scheduled_at_ms)
            })
            .unwrap_or_default();
        let dead_letter_records = inner.dead_letters.len();
        let producer_sessions = inner.producer_epochs.len();
        let producer_dedup_entries = inner.producer_sequences.len();
        let compaction_candidates = inner.superseded_since_compaction;
        let compaction_keys = inner.compaction_latest.len();
        drop(inner);
        let pull_waiters = self.pull_waiters.len();

        let quotas = self.quotas.snapshot();
        let tenant_quota_usage = self.tenant_quotas.snapshot();
        let tenant_connections = tenant_quota_usage
            .values()
            .map(|usage| usage.connections)
            .sum::<usize>();
        let tenant_memory_bytes = tenant_quota_usage
            .values()
            .map(|usage| usage.memory_bytes)
            .sum::<u64>();
        let cluster = self.cluster_response().await;
        let streams = self.streams_response().await;
        let retained_messages = streams
            .streams
            .iter()
            .map(|stream| stream.retained_messages)
            .sum::<usize>();
        let retained_bytes = streams
            .streams
            .iter()
            .map(|stream| stream.retained_bytes)
            .sum::<u64>();
        let partition_count = streams
            .streams
            .iter()
            .map(|stream| stream.partition_status.len())
            .sum::<usize>();
        let audit = self.audit_status();
        let mut metrics = String::new();
        metrics.push_str("# HELP morrow_connections Current client connections.\n");
        metrics.push_str("# TYPE morrow_connections gauge\n");
        metrics.push_str(&format!("morrow_connections {connections}\n"));
        metrics.push_str("# HELP morrow_websocket_connections Current WebSocket connections.\n");
        metrics.push_str("# TYPE morrow_websocket_connections gauge\n");
        metrics.push_str(&format!(
            "morrow_websocket_connections {}\n",
            self.metrics.websocket_connections.load(Ordering::Relaxed)
        ));
        for (name, help, value) in [
            (
                "morrow_websocket_connections_total",
                "WebSocket connections accepted.",
                self.metrics
                    .websocket_connections_total
                    .load(Ordering::Relaxed),
            ),
            (
                "morrow_websocket_messages_received_total",
                "WebSocket messages received.",
                self.metrics
                    .websocket_messages_received_total
                    .load(Ordering::Relaxed),
            ),
            (
                "morrow_websocket_messages_sent_total",
                "WebSocket messages sent.",
                self.metrics
                    .websocket_messages_sent_total
                    .load(Ordering::Relaxed),
            ),
            (
                "morrow_websocket_bytes_received_total",
                "WebSocket payload bytes received.",
                self.metrics
                    .websocket_bytes_received_total
                    .load(Ordering::Relaxed),
            ),
            (
                "morrow_websocket_bytes_sent_total",
                "WebSocket payload bytes sent.",
                self.metrics
                    .websocket_bytes_sent_total
                    .load(Ordering::Relaxed),
            ),
            (
                "morrow_websocket_errors_total",
                "WebSocket transport errors.",
                self.metrics.websocket_errors_total.load(Ordering::Relaxed),
            ),
        ] {
            metrics.push_str(&format!(
                "# HELP {name} {help}\n# TYPE {name} counter\n{name} {value}\n"
            ));
        }
        metrics
            .push_str("# HELP morrow_transient_subscriptions Current transient subscriptions.\n");
        metrics.push_str("# TYPE morrow_transient_subscriptions gauge\n");
        metrics.push_str(&format!(
            "morrow_transient_subscriptions {transient_subscriptions}\n"
        ));
        metrics.push_str("# HELP morrow_consumer_groups Current consumer groups.\n");
        metrics.push_str("# TYPE morrow_consumer_groups gauge\n");
        metrics.push_str(&format!("morrow_consumer_groups {group_count}\n"));
        metrics.push_str("# HELP morrow_consumer_group_members Current active group members.\n");
        metrics.push_str("# TYPE morrow_consumer_group_members gauge\n");
        metrics.push_str(&format!("morrow_consumer_group_members {group_members}\n"));
        metrics.push_str("# HELP morrow_consumer_group_moved_partitions Partitions awaiting cooperative rebalance completion.\n");
        metrics.push_str("# TYPE morrow_consumer_group_moved_partitions gauge\n");
        metrics.push_str(&format!(
            "morrow_consumer_group_moved_partitions {group_moved_partitions}\n"
        ));
        metrics.push_str("# HELP morrow_durable_consumers Current durable consumers.\n");
        metrics.push_str("# TYPE morrow_durable_consumers gauge\n");
        metrics.push_str(&format!("morrow_durable_consumers {consumers}\n"));
        metrics.push_str("# HELP morrow_pull_waiters Current blocked pull requests.\n");
        metrics.push_str("# TYPE morrow_pull_waiters gauge\n");
        metrics.push_str(&format!("morrow_pull_waiters {pull_waiters}\n"));
        metrics.push_str("# HELP morrow_pending_deliveries Current pending deliveries.\n");
        metrics.push_str("# TYPE morrow_pending_deliveries gauge\n");
        metrics.push_str(&format!("morrow_pending_deliveries {pending_deliveries}\n"));
        metrics.push_str("# HELP morrow_in_flight_deliveries Current in-flight deliveries.\n");
        metrics.push_str("# TYPE morrow_in_flight_deliveries gauge\n");
        metrics.push_str(&format!(
            "morrow_in_flight_deliveries {in_flight_deliveries}\n"
        ));
        metrics.push_str("# HELP morrow_producer_sessions Current fenced producer identities.\n");
        metrics.push_str("# TYPE morrow_producer_sessions gauge\n");
        metrics.push_str(&format!("morrow_producer_sessions {producer_sessions}\n"));
        metrics.push_str(
            "# HELP morrow_producer_dedup_entries Current bounded producer sequence entries.\n",
        );
        metrics.push_str("# TYPE morrow_producer_dedup_entries gauge\n");
        metrics.push_str(&format!(
            "morrow_producer_dedup_entries {producer_dedup_entries}\n"
        ));
        metrics.push_str("# HELP morrow_scheduled_delivery_depth Current scheduled messages.\n");
        metrics.push_str("# TYPE morrow_scheduled_delivery_depth gauge\n");
        metrics.push_str(&format!(
            "morrow_scheduled_delivery_depth {scheduled_depth}\n"
        ));
        metrics.push_str("# HELP morrow_scheduled_delivery_due_lag_ms Age of the oldest due scheduled message.\n");
        metrics.push_str("# TYPE morrow_scheduled_delivery_due_lag_ms gauge\n");
        metrics.push_str(&format!(
            "morrow_scheduled_delivery_due_lag_ms {scheduled_due_lag_ms}\n"
        ));
        metrics.push_str("# HELP morrow_retry_exhausted_total Dead-letter terminal records recovered by the broker.\n");
        metrics.push_str("# TYPE morrow_retry_exhausted_total gauge\n");
        metrics.push_str(&format!(
            "morrow_retry_exhausted_total {dead_letter_records}\n"
        ));
        metrics.push_str("# HELP morrow_dead_letter_writes_total Durable dead-letter writes.\n");
        metrics.push_str("# TYPE morrow_dead_letter_writes_total counter\n");
        metrics.push_str(&format!(
            "morrow_dead_letter_writes_total {}\n",
            self.metrics
                .dead_letter_writes_total
                .load(Ordering::Relaxed)
        ));
        metrics.push_str(
            "# HELP morrow_dead_letter_replay_outcomes_total Dead-letter replay outcomes.\n",
        );
        metrics.push_str("# TYPE morrow_dead_letter_replay_outcomes_total counter\n");
        metrics.push_str(&format!(
            "morrow_dead_letter_replay_outcomes_total {}\n",
            self.metrics
                .dead_letter_replay_outcomes_total
                .load(Ordering::Relaxed)
        ));
        metrics.push_str("# HELP morrow_publishes_total Publish commands received.\n");
        metrics.push_str("# TYPE morrow_publishes_total counter\n");
        metrics.push_str(&format!(
            "morrow_publishes_total {}\n",
            self.metrics.publishes_total.load(Ordering::Relaxed)
        ));
        metrics.push_str("# HELP morrow_published_bytes_total Published payload bytes.\n");
        metrics.push_str("# TYPE morrow_published_bytes_total counter\n");
        metrics.push_str(&format!(
            "morrow_published_bytes_total {}\n",
            self.metrics.published_bytes_total.load(Ordering::Relaxed)
        ));
        metrics.push_str(
            "# HELP morrow_rejected_operations_total Operations rejected by broker policy.\n",
        );
        metrics.push_str("# TYPE morrow_rejected_operations_total counter\n");
        metrics.push_str(&format!(
            "morrow_rejected_operations_total {}\n",
            self.metrics
                .rejected_operations_total
                .load(Ordering::Relaxed)
        ));
        metrics.push_str("# HELP morrow_consumer_lag_messages Current consumer backlog.\n");
        metrics.push_str("# TYPE morrow_consumer_lag_messages gauge\n");
        metrics.push_str(&format!(
            "morrow_consumer_lag_messages {consumer_lag_messages}\n"
        ));
        metrics.push_str("# HELP morrow_partition_reads_total Partition-log records loaded.\n");
        metrics.push_str("# TYPE morrow_partition_reads_total counter\n");
        metrics.push_str(&format!(
            "morrow_partition_reads_total {}\n",
            self.metrics.partition_reads_total.load(Ordering::Relaxed)
        ));
        metrics.push_str("# HELP morrow_partition_writes_total Partition-log records appended.\n");
        metrics.push_str("# TYPE morrow_partition_writes_total counter\n");
        metrics.push_str(&format!(
            "morrow_partition_writes_total {}\n",
            self.metrics.partition_writes_total.load(Ordering::Relaxed)
        ));
        metrics.push_str(
            "# HELP morrow_delivery_attempts_total Delivery attempts sent to consumers.\n",
        );
        metrics.push_str("# TYPE morrow_delivery_attempts_total counter\n");
        metrics.push_str(&format!(
            "morrow_delivery_attempts_total {}\n",
            self.metrics.delivery_attempts_total.load(Ordering::Relaxed)
        ));
        metrics.push_str("# HELP morrow_acknowledgements_total Valid acknowledgements.\n");
        metrics.push_str("# TYPE morrow_acknowledgements_total counter\n");
        metrics.push_str(&format!(
            "morrow_acknowledgements_total {}\n",
            self.metrics.acknowledgements_total.load(Ordering::Relaxed)
        ));
        metrics.push_str("# HELP morrow_nacks_total Negative acknowledgements.\n");
        metrics.push_str("# TYPE morrow_nacks_total counter\n");
        metrics.push_str(&format!(
            "morrow_nacks_total {}\n",
            self.metrics.nacks_total.load(Ordering::Relaxed)
        ));
        metrics.push_str("# HELP morrow_redeliveries_total Lease-expiry redeliveries.\n");
        metrics.push_str("# TYPE morrow_redeliveries_total counter\n");
        metrics.push_str(&format!(
            "morrow_redeliveries_total {}\n",
            self.metrics.redeliveries_total.load(Ordering::Relaxed)
        ));
        append_latency_histogram(
            &mut metrics,
            "morrow_publish_latency_us",
            &self.metrics.publish_latency_us,
        );
        append_latency_histogram(
            &mut metrics,
            "morrow_delivery_latency_us",
            &self.metrics.delivery_latency_us,
        );
        metrics.push_str("# HELP morrow_wal_bytes Total WAL bytes.\n");
        metrics.push_str("# TYPE morrow_wal_bytes gauge\n");
        metrics.push_str(&format!("morrow_wal_bytes {}\n", wal.total_wal_bytes));
        metrics.push_str("# HELP morrow_wal_retained_messages Retained WAL messages.\n");
        metrics.push_str("# TYPE morrow_wal_retained_messages gauge\n");
        metrics.push_str(&format!(
            "morrow_wal_retained_messages {}\n",
            wal.retained_message_count
        ));
        metrics.push_str(
            "# HELP morrow_partition_retained_messages Current retained partition messages.\n",
        );
        metrics.push_str("# TYPE morrow_partition_retained_messages gauge\n");
        metrics.push_str(&format!(
            "morrow_partition_retained_messages {retained_messages}\n"
        ));
        metrics
            .push_str("# HELP morrow_partition_retained_bytes Current retained partition bytes.\n");
        metrics.push_str("# TYPE morrow_partition_retained_bytes gauge\n");
        metrics.push_str(&format!(
            "morrow_partition_retained_bytes {retained_bytes}\n"
        ));
        metrics.push_str("# HELP morrow_configured_partitions Configured partition count.\n");
        metrics.push_str("# TYPE morrow_configured_partitions gauge\n");
        metrics.push_str(&format!("morrow_configured_partitions {partition_count}\n"));
        metrics.push_str("# HELP morrow_recovered_partitions Recovered partition count.\n");
        metrics.push_str("# TYPE morrow_recovered_partitions gauge\n");
        metrics.push_str(&format!(
            "morrow_recovered_partitions {}\n",
            streams.recovery.completed_partitions
        ));
        metrics.push_str("# HELP morrow_wal_rotations_total WAL segment rotations.\n");
        metrics.push_str("# TYPE morrow_wal_rotations_total counter\n");
        metrics.push_str(&format!("morrow_wal_rotations_total {}\n", wal.rotations));
        metrics.push_str("# HELP morrow_wal_checkpoints_total WAL checkpoints.\n");
        metrics.push_str("# TYPE morrow_wal_checkpoints_total counter\n");
        metrics.push_str(&format!(
            "morrow_wal_checkpoints_total {}\n",
            wal.checkpoints
        ));
        metrics.push_str("# HELP morrow_wal_truncations_total WAL truncations.\n");
        metrics.push_str("# TYPE morrow_wal_truncations_total counter\n");
        metrics.push_str(&format!(
            "morrow_wal_truncations_total {}\n",
            wal.truncations
        ));
        metrics.push_str("# HELP morrow_wal_last_fsync_duration_us Last WAL fsync duration.\n");
        metrics.push_str("# TYPE morrow_wal_last_fsync_duration_us gauge\n");
        metrics.push_str(&format!(
            "morrow_wal_last_fsync_duration_us {}\n",
            wal.last_fsync_duration_ms.saturating_mul(1_000)
        ));
        metrics.push_str(
            "# HELP morrow_wal_last_checkpoint_duration_us Last WAL checkpoint duration.\n",
        );
        metrics.push_str("# TYPE morrow_wal_last_checkpoint_duration_us gauge\n");
        metrics.push_str(&format!(
            "morrow_wal_last_checkpoint_duration_us {}\n",
            wal.last_checkpoint_duration_ms.saturating_mul(1_000)
        ));
        metrics.push_str(
            "# HELP morrow_compaction_candidates Current superseded records awaiting compaction.\n",
        );
        metrics.push_str("# TYPE morrow_compaction_candidates gauge\n");
        metrics.push_str(&format!(
            "morrow_compaction_candidates {compaction_candidates}\n"
        ));
        metrics.push_str("# HELP morrow_compaction_keys Current compaction index keys.\n");
        metrics.push_str("# TYPE morrow_compaction_keys gauge\n");
        metrics.push_str(&format!("morrow_compaction_keys {compaction_keys}\n"));
        metrics.push_str("# HELP morrow_cluster_partitions Current cluster partitions.\n");
        metrics.push_str("# TYPE morrow_cluster_partitions gauge\n");
        metrics.push_str(&format!(
            "morrow_cluster_partitions {}\n",
            cluster.partitions.len()
        ));
        let reconfiguration_generation_max = cluster
            .partitions
            .iter()
            .map(|partition| partition.replica_set_generation)
            .max()
            .unwrap_or_default();
        let active_commit_members = cluster
            .partitions
            .iter()
            .map(|partition| partition.active_commit_set.len())
            .sum::<usize>();
        metrics.push_str(
            "# HELP morrow_partition_reconfiguration_generation_max Highest persisted partition replica-set generation.\n",
        );
        metrics.push_str("# TYPE morrow_partition_reconfiguration_generation_max gauge\n");
        metrics.push_str(&format!(
            "morrow_partition_reconfiguration_generation_max {reconfiguration_generation_max}\n"
        ));
        metrics.push_str(
            "# HELP morrow_partition_active_commit_members Current active commit-set members.\n",
        );
        metrics.push_str("# TYPE morrow_partition_active_commit_members gauge\n");
        metrics.push_str(&format!(
            "morrow_partition_active_commit_members {active_commit_members}\n"
        ));
        metrics.push_str("# HELP morrow_cluster_peers Current configured cluster peers.\n");
        metrics.push_str("# TYPE morrow_cluster_peers gauge\n");
        metrics.push_str(&format!("morrow_cluster_peers {}\n", cluster.peers.len()));
        metrics
            .push_str("# HELP morrow_cluster_delta_applications_total Applied cluster deltas.\n");
        metrics.push_str("# TYPE morrow_cluster_delta_applications_total counter\n");
        metrics.push_str(&format!(
            "morrow_cluster_delta_applications_total {}\n",
            cluster.state_application.delta_applications
        ));
        metrics.push_str(
            "# HELP morrow_cluster_full_reconciliations_total Full cluster reconciliations.\n",
        );
        metrics.push_str("# TYPE morrow_cluster_full_reconciliations_total counter\n");
        metrics.push_str(&format!(
            "morrow_cluster_full_reconciliations_total {}\n",
            cluster.state_application.full_reconciliations
        ));
        let connected_routes = cluster
            .routes
            .as_ref()
            .map(|routes| routes.connected.len())
            .unwrap_or_default();
        let (middleware_executions, middleware_drops, middleware_rejects, middleware_failures) =
            self.middleware.metrics_snapshot();
        let connector_count = self.connectors_response().await.count;
        metrics.push_str("# HELP morrow_connectors_connected Current connected connectors.\n");
        metrics.push_str("# TYPE morrow_connectors_connected gauge\n");
        metrics.push_str(&format!("morrow_connectors_connected {connector_count}\n"));
        metrics.push_str("# HELP morrow_route_peers_connected Current connected route peers.\n");
        metrics.push_str("# TYPE morrow_route_peers_connected gauge\n");
        metrics.push_str(&format!(
            "morrow_route_peers_connected {connected_routes}\n"
        ));
        metrics.push_str("# HELP morrow_middleware_generation Current middleware generation.\n");
        metrics.push_str("# TYPE morrow_middleware_generation gauge\n");
        metrics.push_str(&format!(
            "morrow_middleware_generation {}\n",
            self.middleware.current_generation()
        ));
        metrics.push_str("# HELP morrow_middleware_executions_total Middleware executions.\n");
        metrics.push_str("# TYPE morrow_middleware_executions_total counter\n");
        metrics.push_str(&format!(
            "morrow_middleware_executions_total {middleware_executions}\n"
        ));
        metrics.push_str("# HELP morrow_middleware_drops_total Middleware drops.\n");
        metrics.push_str("# TYPE morrow_middleware_drops_total counter\n");
        metrics.push_str(&format!(
            "morrow_middleware_drops_total {middleware_drops}\n"
        ));
        metrics.push_str("# HELP morrow_middleware_rejects_total Middleware rejects.\n");
        metrics.push_str("# TYPE morrow_middleware_rejects_total counter\n");
        metrics.push_str(&format!(
            "morrow_middleware_rejects_total {middleware_rejects}\n"
        ));
        metrics
            .push_str("# HELP morrow_middleware_failures_total Middleware execution failures.\n");
        metrics.push_str("# TYPE morrow_middleware_failures_total counter\n");
        metrics.push_str(&format!(
            "morrow_middleware_failures_total {middleware_failures}\n"
        ));
        metrics.push_str(
            "# HELP morrow_cluster_ready Whether the broker is ready to serve traffic.\n",
        );
        metrics.push_str("# TYPE morrow_cluster_ready gauge\n");
        metrics.push_str(&format!(
            "morrow_cluster_ready {}\n",
            (cluster.cluster_status == "standalone" || cluster.cluster_status == "ready") as u8
        ));
        metrics.push_str(
            "# HELP morrow_quota_rejections_total Rejected operations caused by resource quotas.\n",
        );
        metrics.push_str("# TYPE morrow_quota_rejections_total counter\n");
        metrics.push_str(&format!(
            "morrow_quota_rejections_total{{resource=\"connections\"}} {}\n",
            quotas.connections.rejections
        ));
        metrics.push_str(&format!(
            "morrow_quota_rejections_total{{resource=\"http_connections\"}} {}\n",
            quotas.http_connections.rejections
        ));
        metrics.push_str(&format!(
            "morrow_quota_rejections_total{{resource=\"raft_connections\"}} {}\n",
            quotas.raft_connections.rejections
        ));
        metrics.push_str(&format!(
            "morrow_quota_rejections_total{{resource=\"route_connections\"}} {}\n",
            quotas.route_connections.rejections
        ));
        metrics.push_str(&format!(
            "morrow_quota_rejections_total{{resource=\"state\"}} {}\n",
            quotas.state_rejections
        ));
        metrics.push_str(&format!(
            "morrow_quota_rejections_total{{resource=\"outbound\"}} {}\n",
            quotas.outbound_rejections
        ));
        metrics.push_str("# HELP morrow_tenants Current tenants with quota usage.\n");
        metrics.push_str("# TYPE morrow_tenants gauge\n");
        metrics.push_str(&format!("morrow_tenants {}\n", tenant_quota_usage.len()));
        metrics.push_str("# HELP morrow_tenant_connections Current tenant-scoped connections.\n");
        metrics.push_str("# TYPE morrow_tenant_connections gauge\n");
        metrics.push_str(&format!("morrow_tenant_connections {tenant_connections}\n"));
        metrics.push_str(
            "# HELP morrow_tenant_memory_bytes Current tenant-scoped memory reservations.\n",
        );
        metrics.push_str("# TYPE morrow_tenant_memory_bytes gauge\n");
        metrics.push_str(&format!(
            "morrow_tenant_memory_bytes {tenant_memory_bytes}\n"
        ));
        metrics
            .push_str("# HELP morrow_audit_records_written_total Durable audit records written.\n");
        metrics.push_str("# TYPE morrow_audit_records_written_total counter\n");
        metrics.push_str(&format!(
            "morrow_audit_records_written_total {}\n",
            audit.records_written
        ));
        metrics.push_str("# HELP morrow_audit_bytes_written_total Durable audit bytes written.\n");
        metrics.push_str("# TYPE morrow_audit_bytes_written_total counter\n");
        metrics.push_str(&format!(
            "morrow_audit_bytes_written_total {}\n",
            audit.bytes_written
        ));
        metrics.push_str("# HELP morrow_audit_rotations_total Audit segment rotations.\n");
        metrics.push_str("# TYPE morrow_audit_rotations_total counter\n");
        metrics.push_str(&format!(
            "morrow_audit_rotations_total {}\n",
            audit.rotations
        ));
        metrics.push_str("# HELP morrow_audit_export_position Current audit export position.\n");
        metrics.push_str("# TYPE morrow_audit_export_position gauge\n");
        metrics.push_str(&format!(
            "morrow_audit_export_position {}\n",
            audit.export_position
        ));
        metrics.push_str("# HELP morrow_audit_write_failures_total Audit write failures.\n");
        metrics.push_str("# TYPE morrow_audit_write_failures_total counter\n");
        metrics.push_str(&format!(
            "morrow_audit_write_failures_total {}\n",
            audit.write_failures
        ));
        metrics.push_str(
            "# HELP morrow_audit_verification_failures_total Audit verification failures.\n",
        );
        metrics.push_str("# TYPE morrow_audit_verification_failures_total counter\n");
        metrics.push_str(&format!(
            "morrow_audit_verification_failures_total {}\n",
            audit.verification_failures
        ));
        metrics.push_str("# HELP morrow_audit_failure Whether audit writes have failed.\n");
        metrics.push_str("# TYPE morrow_audit_failure gauge\n");
        metrics.push_str(&format!(
            "morrow_audit_failure {}\n",
            self.audit_failure.load(Ordering::Relaxed) as u8
        ));
        if let Some(oldest) = audit.oldest_retained_sequence {
            metrics.push_str(&format!("morrow_audit_oldest_retained_sequence {oldest}\n"));
        }
        if let Some(newest) = audit.newest_retained_sequence {
            metrics.push_str(&format!("morrow_audit_newest_retained_sequence {newest}\n"));
        }
        metrics
    }

    pub(super) async fn cluster_response(&self) -> ClusterResponse {
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
        let node_role = cluster_config
            .map(|cluster| match cluster.role {
                crate::config::ClusterRole::Combined => "combined",
                crate::config::ClusterRole::Controller => "controller",
                crate::config::ClusterRole::Broker => "broker",
            })
            .unwrap_or("standalone");
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
        let mut partitions = cluster
            .as_ref()
            .map(|cluster| cluster.durable_state())
            .into_iter()
            .flat_map(|state| {
                state
                    .partition_assignments
                    .into_iter()
                    .filter_map(move |(key, assignment)| {
                        let (stream, partition) = key.rsplit_once(':')?;
                        let partition = partition.parse::<u32>().ok()?;
                        let high_watermark = state
                            .partition_commits
                            .get(&key)
                            .map(|commit| commit.high_watermark);
                        let leader_client_addr = cluster_config.and_then(|cluster| {
                            cluster
                                .nodes
                                .iter()
                                .find(|node| node.node_id == assignment.leader_id)
                                .map(|node| node.client_addr.to_string())
                        });
                        Some(PartitionLeaderResponse {
                            stream: stream.to_string(),
                            partition,
                            replicas: assignment.replicas.into_iter().collect(),
                            active_commit_set: assignment.active_commit_set.into_iter().collect(),
                            replica_set_generation: assignment.replica_set_generation,
                            phase: assignment.phase,
                            leader_id: assignment.leader_id,
                            leader_client_addr,
                            leader_epoch: assignment.leader_epoch,
                            high_watermark,
                        })
                    })
            })
            .collect::<Vec<_>>();
        partitions.sort_by_key(|partition| (partition.stream.clone(), partition.partition));
        let routes = match &self.route_mesh {
            Some(route_mesh) => Some(route_mesh.topology_response().await),
            None => None,
        };
        ClusterResponse {
            cluster_size,
            cluster_status,
            node_id,
            role,
            node_role,
            controller_voter: cluster_config.is_some_and(|cluster| cluster.is_controller_voter()),
            controller_voters: cluster_config
                .map(|cluster| cluster.controller_voters.clone())
                .unwrap_or_default(),
            leader_id,
            peers,
            partitions,
            routes,
            state_application: ClusterStateApplicationResponse {
                delta_applications: self
                    .cluster_application_metrics
                    .delta_applications
                    .load(Ordering::Relaxed),
                full_reconciliations: self
                    .cluster_application_metrics
                    .full_reconciliations
                    .load(Ordering::Relaxed),
            },
        }
    }

    pub(super) async fn quotas_response(&self) -> QuotasResponse {
        let transient_subscriptions = self.transient.lock().await.subscriptions.len();
        let durable_consumers = self.inner.lock().await.consumers.len();
        QuotasResponse {
            sockets: self.quotas.snapshot(),
            transient_subscriptions: StateQuotaUsage {
                used: transient_subscriptions,
                limit: self.config.quotas.max_transient_subscriptions,
            },
            durable_consumers: StateQuotaUsage {
                used: durable_consumers,
                limit: self.config.quotas.max_durable_consumers,
            },
            outbound_bytes_per_connection_limit: self
                .config
                .quotas
                .max_outbound_bytes_per_connection,
            tenant_quotas: self.tenant_quotas.status_snapshot(),
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

    pub(super) fn spawn_accepted(&self, stream: TcpStream) {
        let Some(permit) = self.quotas.try_client() else {
            tokio::spawn(async move {
                let mut stream = stream;
                let _ = stream
                    .write_all(&protocol::err("connection quota exceeded"))
                    .await;
            });
            return;
        };
        let broker = self.clone();
        tokio::spawn(async move {
            let _permit = permit;
            if let Err(err) = broker.handle_accepted(stream).await {
                error!(error = ?err, "client error");
            }
        });
    }

    pub(super) async fn handle_accepted(&self, stream: TcpStream) -> Result<()> {
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

    pub(super) async fn route_cluster_stream<S>(&self, stream: S) -> Result<Option<S>>
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
    pub(super) async fn handle_client<S>(&self, stream: S) -> Result<()>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        self.handle_client_with_remote_addr(stream, None).await
    }

    pub(super) async fn handle_client_with_remote_addr<S>(
        &self,
        stream: S,
        remote_addr: Option<SocketAddr>,
    ) -> Result<()>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let id = self.next_connection_id.fetch_add(1, Ordering::Relaxed);
        let (reader, mut writer) = tokio::io::split(stream);
        let (sender, mut receiver) = mpsc::channel::<OutboundFrame>(256);
        let sender = OutboundQueue::new(
            sender,
            self.config.quotas.max_outbound_bytes_per_connection,
            self.quotas.clone(),
        );
        self.add_client(id, sender, remote_addr).await?;
        let nonce = {
            let connections = self.connections.lock().await;
            connections
                .clients
                .get(&id)
                .and_then(|client| client.auth_nonce.clone())
        };

        if let Err(err) = writer
            .write_all(&protocol::info_line(
                self.config.max_payload,
                nonce.as_deref(),
            ))
            .await
        {
            self.remove_client(id).await?;
            return Err(err.into());
        }
        let writer_task = tokio::spawn(async move {
            while let Some(frame) = receiver.recv().await {
                writer.write_all(frame.as_bytes()).await?;
            }
            Ok::<(), BrokerError>(())
        });

        let mut reader = BufReader::new(reader);
        let mut session_result = Ok(());
        loop {
            let read = async {
                protocol::read_command(
                    &mut reader,
                    self.config.max_payload,
                    self.config.max_control_line,
                )
                .await
            };
            let configured = self.client_is_configured(id).await;
            let timeout_ms = if configured {
                self.config.quotas.client_idle_timeout_ms
            } else {
                UNAUTHENTICATED_READ_TIMEOUT_MS
            };
            let command = match tokio::time::timeout(Duration::from_millis(timeout_ms), read).await
            {
                Ok(command) => command,
                Err(_) => {
                    session_result = Err(BrokerError::msg(if configured {
                        "client idle read timed out"
                    } else {
                        "unauthenticated read timed out"
                    }));
                    break;
                }
            };
            match command {
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
        session_result
    }

    pub(super) async fn client_is_configured(&self, id: u64) -> bool {
        self.connections
            .lock()
            .await
            .clients
            .get(&id)
            .is_some_and(|client| client.configured)
    }
}

fn append_latency_histogram(metrics: &mut String, name: &str, histogram: &LatencyHistogram) {
    const BOUNDS: [&str; 6] = ["9", "99", "999", "9999", "99999", "+Inf"];
    let (buckets, count, sum_us) = histogram.snapshot();
    metrics.push_str(&format!("# HELP {name} Latency in microseconds.\n"));
    metrics.push_str(&format!("# TYPE {name} histogram\n"));
    let mut cumulative = 0;
    for (bound, bucket) in BOUNDS.into_iter().zip(buckets) {
        cumulative += bucket;
        metrics.push_str(&format!("{name}_bucket{{le=\"{bound}\"}} {cumulative}\n"));
    }
    metrics.push_str(&format!("{name}_sum {sum_us}\n{name}_count {count}\n"));
}
