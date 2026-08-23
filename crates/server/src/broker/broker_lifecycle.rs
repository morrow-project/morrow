use super::*;

impl Morrow {
    pub fn open(config: Config) -> Result<Self> {
        Self::open_with_hooks(config, BrokerHooks::default())
    }

    pub(crate) fn open_with_hooks(config: Config, hooks: BrokerHooks) -> Result<Self> {
        config.validate()?;
        let (mut wal, mut replay) = Wal::open(
            &config.wal_dir,
            config.fsync_interval(),
            config.wal_segment_bytes,
        )?;
        if let Some(cluster) = &config.cluster {
            wal.namespace_delivery_ids(cluster.node_id);
        }
        let (partition_logs, mut envelopes) =
            PartitionLogSet::open(&config.wal_dir, &config.streams, config.wal_segment_bytes)?;
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
        let route_mesh = RouteMesh::from_config(&config, quotas.clone())?;
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
            next_connection_id: Arc::new(AtomicU64::new(1)),
            config,
            tls_acceptor,
            admin_tls_acceptor,
            quotas,
            cluster: Arc::new(Mutex::new(cluster)),
            cluster_applied_index: Arc::new(AtomicU64::new(0)),
            cluster_delta_gate: Arc::new(Mutex::new(())),
            cluster_application_metrics: Arc::new(ClusterApplicationMetrics::default()),
            metrics: Arc::new(BrokerMetrics::default()),
            redelivery_notify: Arc::new(Notify::new()),
            pull_waiters: PullWaiterRegistry::default(),
            compaction_running: Arc::new(AtomicBool::new(false)),
            route_mesh,
            middleware: hooks.middleware.clone(),
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

    pub(super) async fn serve_inner(
        self,
        listener: TcpListener,
        handle_shutdown: bool,
    ) -> Result<()> {
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
        self.wal.checkpoint(messages, consumers).await?;
        self.wal.flush().await?;
        Ok(())
    }

    pub async fn cluster_leader(&self) -> Option<u64> {
        self.cluster_runtime().await?.current_leader().await
    }

    pub(super) async fn health_response(&self) -> HealthResponse {
        let cluster = self.cluster_response().await;
        let status = if matches!(cluster.cluster_status, "standalone" | "ready") {
            "ready"
        } else {
            "forming"
        };
        HealthResponse {
            status,
            cluster_status: cluster.cluster_status,
            role: cluster.role,
        }
    }

    pub(super) async fn metrics_response(&self) -> String {
        let connections = self.connections.lock().await.clients.len();
        let transient_subscriptions = self.transient.lock().await.subscriptions.len();
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
        let in_flight_deliveries = inner
            .consumers
            .values()
            .map(|consumer| consumer.in_flight.len())
            .sum::<usize>();
        drop(inner);
        let pull_waiters = self.pull_waiters.len();

        let quotas = self.quotas.snapshot();
        let cluster = self.cluster_response().await;
        let mut metrics = String::new();
        metrics.push_str("# HELP morrow_connections Current client connections.\n");
        metrics.push_str("# TYPE morrow_connections gauge\n");
        metrics.push_str(&format!("morrow_connections {connections}\n"));
        metrics
            .push_str("# HELP morrow_transient_subscriptions Current transient subscriptions.\n");
        metrics.push_str("# TYPE morrow_transient_subscriptions gauge\n");
        metrics.push_str(&format!(
            "morrow_transient_subscriptions {transient_subscriptions}\n"
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
