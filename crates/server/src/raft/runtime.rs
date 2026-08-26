use super::*;

#[derive(Clone)]
pub struct RaftRuntime {
    pub(super) raft: BrokerRaft,
    pub(super) state_machine: StateMachineStore,
    pub(super) nodes: HashMap<u64, ClusterNode>,
    pub(super) auth_token: String,
    pub(super) node_id: u64,
    pub(super) tls_enabled: bool,
    pub(super) partition_data: SharedReplicaData,
    pub(super) partition_write_gates: Arc<HashMap<String, tokio::sync::Mutex<()>>>,
    pub(super) configured_streams: Vec<crate::stream::StreamDefinition>,
    pub(super) heartbeat_interval_ms: u64,
    pub(super) security_references: BTreeSet<String>,
    pub(super) raft_tls: Option<RaftTlsRuntime>,
    pub(super) quotas: Arc<crate::quota::QuotaRuntime>,
    pub(super) data_clients: Arc<tokio::sync::Mutex<HashMap<u64, NetworkClient>>>,
}
#[derive(Debug, Clone)]
pub struct ClusterNode {
    pub raft_addr: String,
    pub client_addr: String,
}

#[derive(Clone)]
pub(super) struct RaftTlsRuntime {
    pub(super) acceptor: tokio_rustls::TlsAcceptor,
    pub(super) connector: tokio_rustls::TlsConnector,
    pub(super) peer_identities: Arc<HashMap<Vec<u8>, u64>>,
    pub(super) server_names: Arc<HashMap<u64, String>>,
    pub(super) handshake_timeout_ms: u64,
}
impl RaftRuntime {
    pub(crate) async fn open(
        config: &ClusterConfig,
        tls_enabled: bool,
        streams: &crate::stream::StreamCatalog,
        segment_bytes: u64,
        quotas: Arc<crate::quota::QuotaRuntime>,
    ) -> Result<Self> {
        std::fs::create_dir_all(&config.raft_dir)
            .with_context(|| format!("creating Raft directory {}", config.raft_dir.display()))?;

        let nodes = config
            .nodes
            .iter()
            .map(|node| {
                (
                    node.node_id,
                    ClusterNode {
                        raft_addr: node.raft_addr.clone(),
                        client_addr: node.client_addr.clone(),
                    },
                )
            })
            .collect::<HashMap<_, _>>();
        let raft_nodes = config
            .nodes
            .iter()
            .filter(|node| config.controller_voters.contains(&node.node_id))
            .map(|node| (node.node_id, BasicNode::new(node.raft_addr.clone())))
            .collect::<BTreeMap<_, _>>();

        let raft_dir = config.raft_dir.clone();
        let log_store = tokio::task::spawn_blocking(move || {
            LogStore::open(raft_dir.join(LOG_FILE), raft_dir.join(LEGACY_LOG_FILE))
        })
        .await
        .map_err(|err| BrokerError::with_source("opening Raft log worker", err))??;
        let raft_dir = config.raft_dir.clone();
        let state_nodes = raft_nodes.clone();
        let state_machine = tokio::task::spawn_blocking(move || {
            StateMachineStore::open(
                raft_dir.join(STATE_FILE),
                raft_dir.join(SNAPSHOT_FILE),
                raft_dir.join(LEGACY_STATE_FILE),
                raft_dir.join(LEGACY_SNAPSHOT_FILE),
                state_nodes,
            )
        })
        .await
        .map_err(|err| BrokerError::with_source("opening Raft state worker", err))??;
        let mut replica_data = ReplicaDataStore::open(
            &config.raft_dir.join("partition-data"),
            streams,
            segment_bytes,
        )?;
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .min(u128::from(u64::MAX)) as u64;
        replica_data.enforce_retention(streams.definitions(), now_ms)?;
        let partition_data = Arc::new(std::sync::Mutex::new(replica_data));
        let mut partition_write_gates = HashMap::new();
        for stream in streams.definitions() {
            for partition in 0..stream.partitions {
                partition_write_gates.insert(
                    partition_key(stream.name.as_str(), partition),
                    tokio::sync::Mutex::new(()),
                );
            }
        }
        let raft_tls = config
            .raft_tls
            .as_ref()
            .map(|tls| -> Result<RaftTlsRuntime> {
                Ok(RaftTlsRuntime {
                    acceptor: crate::tls::load_internal_acceptor(tls)?,
                    connector: crate::tls::load_internal_connector(tls)?,
                    peer_identities: Arc::new(crate::tls::load_peer_certificates(&config.nodes)?),
                    server_names: Arc::new(
                        config
                            .nodes
                            .iter()
                            .map(|node| {
                                (
                                    node.node_id,
                                    node.tls_server_name.clone().expect("validated TLS name"),
                                )
                            })
                            .collect(),
                    ),
                    handshake_timeout_ms: tls.handshake_timeout_ms,
                })
            })
            .transpose()?;
        let network = NetworkFactory {
            nodes: nodes.clone(),
            auth_token: config.auth_token.clone(),
            node_id: config.node_id,
            tls: raft_tls.clone(),
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
            auth_token: config.auth_token.clone(),
            node_id: config.node_id,
            tls_enabled,
            partition_data,
            partition_write_gates: Arc::new(partition_write_gates),
            configured_streams: streams.definitions().to_vec(),
            heartbeat_interval_ms: config.heartbeat_interval_ms,
            security_references: ["cluster-auth-token".to_string()].into_iter().collect(),
            raft_tls,
            quotas,
            data_clients: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        })
    }

    pub(super) async fn data_client(&self, target: u64) -> Result<NetworkClient> {
        let mut clients = self.data_clients.lock().await;
        if let Some(client) = clients.get(&target) {
            return Ok(client.clone());
        }
        let node = self
            .nodes
            .get(&target)
            .ok_or_else(|| BrokerError::msg("unknown data-plane peer"))?;
        let client = NetworkClient {
            addr: node.raft_addr.clone(),
            auth_token: self.auth_token.clone(),
            node_id: self.node_id,
            target,
            tls: self.raft_tls.clone(),
            connection: Arc::new(tokio::sync::Mutex::new(None)),
        };
        clients.insert(target, client.clone());
        Ok(client)
    }

    pub fn spawn_listener(&self, listener: TcpListener) {
        let raft = self.raft.clone();
        let state_machine = self.state_machine.clone();
        let auth_token = self.auth_token.clone();
        let partition_data = self.partition_data.clone();
        let tls = self.raft_tls.clone();
        let quotas = self.quotas.clone();
        tokio::spawn(async move {
            if let Err(err) = serve_raft(
                raft,
                state_machine,
                listener,
                auth_token,
                partition_data,
                tls,
                quotas,
            )
            .await
            {
                error!(error = ?err, "raft transport error");
            }
        });
    }

    pub async fn client_write(&self, command: BrokerCommand) -> Result<BrokerResponse> {
        let response = self
            .raft
            .client_write(command)
            .await
            .map_err(|err| BrokerError::with_source("proposing Raft command", err))?;
        Ok(response.data)
    }

    pub fn durable_state(&self) -> DurableState {
        let mut state = self.state_machine.durable_state();
        if let Ok(partition_data) = self.partition_data.lock() {
            for (key, commit) in partition_data.local_commits() {
                state
                    .partition_commits
                    .entry(key.clone())
                    .and_modify(|current| {
                        if commit.leader_epoch > current.leader_epoch
                            || (commit.leader_epoch == current.leader_epoch
                                && commit.high_watermark > current.high_watermark)
                        {
                            *current = *commit;
                        }
                    })
                    .or_insert(*commit);
            }
        }
        if let Ok(records) = self
            .partition_data
            .lock()
            .unwrap()
            .committed_records(&state)
        {
            for envelope in records {
                state.messages.insert(
                    envelope.legacy_seq,
                    crate::wal::PublishRecord::from(envelope),
                );
            }
        }
        state
    }

    pub(crate) fn deltas_after(&self, after: Option<u64>) -> DeltaBatch {
        self.state_machine.deltas_after(after)
    }

    pub(crate) fn partition_record(
        &self,
        stream: &str,
        partition: u32,
        offset: u64,
    ) -> Option<crate::partition_log::MessageEnvelope> {
        self.partition_data
            .lock()
            .unwrap()
            .record(stream, crate::stream::PartitionId(partition), offset)
            .ok()
            .flatten()
    }

    pub(crate) fn local_committed_records(
        &self,
    ) -> Result<Vec<crate::partition_log::MessageEnvelope>> {
        let state = self.durable_state();
        self.partition_data
            .lock()
            .map_err(|_| BrokerError::msg("partition data lock poisoned"))?
            .committed_records(&state)
    }

    pub(crate) fn is_local_partition_replica(&self, stream: &str, partition: u32) -> bool {
        self.state_machine
            .is_partition_replica(self.node_id, stream, partition)
    }

    pub fn enforce_retention(&self, now_ms: u64) -> Result<()> {
        self.partition_data
            .lock()
            .unwrap()
            .enforce_retention(&self.configured_streams, now_ms)
    }

    async fn ensure_metadata_bootstrap(&self) -> Result<()> {
        let metadata = self.state_machine.durable_state();
        if !metadata.stream_definitions.is_empty() {
            return self.validate_metadata_configuration(&metadata);
        }
        let replica_order = self.nodes.keys().copied().collect::<Vec<_>>();
        let mut assignments = HashMap::new();
        for stream in &self.configured_streams {
            for partition in 0..stream.partitions {
                let replica_count = usize::try_from(stream.storage.replicas)
                    .unwrap_or(replica_order.len())
                    .min(replica_order.len())
                    .max(1);
                let replicas = (0..replica_count)
                    .map(|offset| {
                        replica_order[(partition as usize + offset) % replica_order.len()]
                    })
                    .collect::<BTreeSet<_>>();
                let leader_id = replica_order[partition as usize % replica_order.len()];
                let active_count = usize::try_from(stream.storage.min_ack_replicas)
                    .unwrap_or(replica_count)
                    .min(replica_count)
                    .max(1);
                let active_commit_set = std::iter::once(leader_id)
                    .chain(
                        replicas
                            .iter()
                            .copied()
                            .filter(move |node| *node != leader_id),
                    )
                    .take(active_count)
                    .collect();
                assignments.insert(
                    partition_key(stream.name.as_str(), partition),
                    PartitionAssignmentMetadata {
                        replicas: replicas.clone(),
                        active_commit_set,
                        replica_set_generation: 1,
                        phase: PartitionReconfigurationPhase::Stable,
                        leader_id,
                        leader_epoch: 1,
                    },
                );
            }
        }
        let response = self
            .client_write(BrokerCommand::MetadataBootstrap {
                streams: self.configured_streams.clone(),
                assignments,
                security_references: self.security_references.clone(),
                feature_gates: [
                    "pull-consumers-v2".to_string(),
                    "controller-directed-replication-v1".to_string(),
                    "partition-local-commit-v1".to_string(),
                ]
                .into_iter()
                .collect(),
            })
            .await?;
        crate::broker_ensure!(
            matches!(
                response,
                BrokerResponse::MetadataBootstrap | BrokerResponse::Noop
            ),
            "metadata bootstrap rejected"
        );
        self.validate_metadata_configuration(&self.state_machine.durable_state())
    }

    pub async fn ensure_metadata_ready(&self) -> Result<()> {
        crate::broker_ensure!(
            self.raft.current_leader().await == Some(self.node_id),
            "not metadata leader"
        );
        self.ensure_metadata_bootstrap().await?;
        Ok(())
    }

    fn validate_metadata_configuration(&self, metadata: &DurableState) -> Result<()> {
        crate::broker_ensure!(
            metadata.stream_definitions.len() == self.configured_streams.len()
                && self.configured_streams.iter().all(|stream| {
                    metadata.stream_definitions.get(stream.name.as_str()) == Some(stream)
                }),
            "local stream configuration differs from metadata consensus"
        );
        crate::broker_ensure!(
            metadata.partition_assignments.values().all(|assignment| {
                assignment
                    .replicas
                    .iter()
                    .all(|node| self.nodes.contains_key(node))
            }),
            "partition assignment references an unknown broker"
        );
        Ok(())
    }

    pub async fn is_leader(&self) -> bool {
        self.raft.current_leader().await == Some(self.node_id)
    }

    pub async fn current_leader(&self) -> Option<u64> {
        self.raft.current_leader().await
    }

    pub async fn quorum_available(&self) -> bool {
        let metrics = self.raft.metrics();
        let metrics = metrics.borrow();
        if metrics.current_leader.is_none() {
            return false;
        }
        metrics.current_leader != Some(self.node_id)
            || metrics
                .millis_since_quorum_ack
                .is_some_and(|elapsed| elapsed <= self.heartbeat_interval_ms.saturating_mul(3))
    }

    pub async fn leader_client_addr(&self) -> Option<String> {
        let leader = self.raft.current_leader().await?;
        self.nodes.get(&leader).map(|node| node.client_addr.clone())
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

pub(super) async fn load_partition_delta(
    partition_data: SharedReplicaData,
    metadata: Arc<DurableState>,
    stream: String,
    partition: crate::stream::PartitionId,
    after: Option<u64>,
) -> Result<Vec<crate::partition_log::MessageEnvelope>> {
    tokio::task::spawn_blocking(move || {
        partition_data
            .lock()
            .map_err(|_| BrokerError::msg("partition data lock poisoned"))?
            .catch_up_records(&metadata, &stream, partition, after)
    })
    .await
    .map_err(|err| BrokerError::with_source("partition delta worker failed", err))?
}
