use super::*;

impl Broker {
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
        let (mut partition_logs, mut envelopes) =
            PartitionLogSet::open(&config.wal_dir, &config.streams, config.wal_segment_bytes)?;
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
        for reference in replay.partition_appends.values() {
            let envelope = envelope_by_seq.get(&reference.seq).ok_or_else(|| {
                BrokerError::msg(format!(
                    "control WAL references missing stream record {}:{}:{}",
                    reference.stream, reference.partition, reference.offset
                ))
            })?;
            crate::broker_ensure!(
                envelope.stream.as_str() == reference.stream
                    && envelope.partition.0 == reference.partition
                    && envelope.offset == reference.offset,
                "control WAL partition reference does not match stream data"
            );
        }
        replay.messages.retain(|_, record| record.stream.is_none());
        replay.messages.extend(
            envelope_by_seq
                .into_iter()
                .map(|(seq, envelope)| (seq, PublishRecord::from(envelope))),
        );
        let tls_acceptor = config
            .tls
            .as_ref()
            .map(crate::tls::load_acceptor)
            .transpose()?;
        let consumers: HashMap<_, _> = replay
            .consumers
            .into_iter()
            .map(|(id, consumer)| {
                (
                    id,
                    Consumer::from_replay(consumer, &config.streams, &replay.messages),
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
                partition_logs,
                clients: HashMap::new(),
                consumers,
                transient_subscriptions: HashMap::new(),
                transient_interest_index: subject::SubjectTrie::default(),
                consumer_interest_index,
                messages: replay.messages,
                partition_sequences,
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
        let mut inner = self.inner.lock().await;
        let messages = inner.messages.values().cloned().collect::<Vec<_>>();
        let consumers = inner.replayed_consumers();
        inner.partition_logs.flush()?;
        inner.wal.checkpoint(messages, consumers)?;
        inner.wal.flush()?;
        Ok(())
    }

    pub async fn cluster_leader(&self) -> Option<u64> {
        self.cluster_runtime().await?.current_leader().await
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
        }
    }

    pub(super) async fn connections_response(&self) -> ConnectionsResponse {
        self.inner.lock().await.connections_response()
    }

    pub(super) async fn subscriptions_response(&self) -> SubscriptionsResponse {
        self.inner.lock().await.subscriptions_response()
    }

    pub(super) fn streams_response(&self) -> StreamsResponse<'_> {
        StreamsResponse {
            streams: self.config.streams.definitions(),
        }
    }

    pub(super) async fn wal_status_response(&self) -> WalStatus {
        let inner = self.inner.lock().await;
        inner
            .wal
            .status(inner.messages.len(), inner.consumers.len())
    }

    pub(super) fn spawn_http_status_listener(&self) {
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

    pub(super) async fn serve_http_status(&self, listen: SocketAddr) -> Result<()> {
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

    pub(super) async fn handle_http_status(&self, mut stream: TcpStream) -> Result<()> {
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
        let Some(admin_token) = self.config.admin_token.as_deref() else {
            return write_http_unauthorized(&mut stream).await;
        };
        if !http_authorized(&request, admin_token) {
            return write_http_unauthorized(&mut stream).await;
        }
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
            "/streams" => {
                let body = serde_json::to_vec(&self.streams_response())
                    .context("serializing HTTP streams response")?;
                write_http_response(&mut stream, "200 OK", "application/json", &body).await
            }
            "/wal" => {
                let body = serde_json::to_vec(&self.wal_status_response().await)
                    .context("serializing HTTP WAL response")?;
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

    pub(super) fn spawn_accepted(&self, stream: TcpStream) {
        let broker = self.clone();
        tokio::spawn(async move {
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
            let read = async {
                protocol::read_command(
                    &mut reader,
                    self.config.max_payload,
                    self.config.max_control_line,
                )
                .await
            };
            let configured = self.client_is_configured(id).await;
            let command = if configured {
                read.await
            } else {
                tokio::time::timeout(Duration::from_millis(UNAUTHENTICATED_READ_TIMEOUT_MS), read)
                    .await
                    .map_err(|_| BrokerError::msg("unauthenticated read timed out"))?
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
        Ok(())
    }

    pub(super) async fn client_is_configured(&self, id: u64) -> bool {
        self.inner
            .lock()
            .await
            .clients
            .get(&id)
            .is_some_and(|client| client.configured)
    }
}
