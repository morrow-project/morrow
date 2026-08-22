use super::*;

#[derive(Clone)]
pub struct RaftRuntime {
    pub raft: BrokerRaft,
    pub state_machine: StateMachineStore,
    pub nodes: HashMap<u64, ClusterNode>,
    auth_token: String,
    node_id: u64,
    tls_enabled: bool,
    partition_data: SharedReplicaData,
    configured_streams: Vec<crate::stream::StreamDefinition>,
    security_references: BTreeSet<String>,
    raft_tls: Option<RaftTlsRuntime>,
    quotas: Arc<crate::quota::QuotaRuntime>,
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
            configured_streams: streams.definitions().to_vec(),
            security_references: ["cluster-auth-token".to_string()].into_iter().collect(),
            raft_tls,
            quotas,
        })
    }

    pub fn spawn_listener(&self, listen: SocketAddr) {
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
                listen,
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
        for envelope in self
            .partition_data
            .lock()
            .unwrap()
            .committed_records(&state)
        {
            state.messages.insert(
                envelope.legacy_seq,
                crate::wal::PublishRecord::from(envelope),
            );
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
        self.partition_data.lock().unwrap().record(
            stream,
            crate::stream::PartitionId(partition),
            offset,
        )
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

    pub async fn replicate_partition(
        &self,
        mut envelope: crate::partition_log::MessageEnvelope,
        fsync: bool,
    ) -> Result<crate::partition_log::MessageEnvelope> {
        crate::broker_ensure!(
            self.raft.current_leader().await == Some(self.node_id),
            "not partition leader"
        );
        self.ensure_metadata_ready().await?;
        let metadata = self.state_machine.durable_state();
        crate::broker_ensure!(
            self.partition_data
                .lock()
                .unwrap()
                .has_committed_prefix(&metadata),
            "no safe replica available"
        );
        let key = partition_key(envelope.stream.as_str(), envelope.partition.0);
        let assignment = metadata
            .partition_assignments
            .get(&key)
            .cloned()
            .ok_or_else(|| BrokerError::msg("partition has no metadata assignment"))?;
        crate::broker_ensure!(
            assignment.leader_id == self.node_id,
            "partition leader assignment is not committed"
        );
        let previous = metadata.partition_commits.get(&key);
        envelope.offset = previous.map_or(0, |commit| commit.high_watermark.saturating_add(1));
        let leader_epoch = assignment.leader_epoch;
        envelope.leader_epoch = leader_epoch;
        let request = DataAppendRequest {
            leader_id: self.node_id,
            leader_epoch,
            fsync,
            committed_high_watermark: previous.map(|commit| commit.high_watermark),
            envelope: envelope.clone(),
        };
        let quorum = self.nodes.len() / 2 + 1;
        let mut replicated = 1usize;
        let mut flushed = usize::from(fsync);
        let mut joins = tokio::task::JoinSet::new();
        let committed_records = self.partition_data.lock().unwrap().catch_up_records(
            &metadata,
            envelope.stream.as_str(),
            envelope.partition,
            None,
        );
        for (node_id, node) in &self.nodes {
            if *node_id == self.node_id {
                continue;
            }
            let addr = node.raft_addr.clone();
            let auth_token = self.auth_token.clone();
            let request = request.clone();
            let committed_records = committed_records.clone();
            let local_node_id = self.node_id;
            let target_node_id = *node_id;
            let tls = self.raft_tls.clone();
            joins.spawn(async move {
                let progress = send_data_progress(
                    &addr,
                    auth_token.clone(),
                    local_node_id,
                    target_node_id,
                    tls.clone(),
                    DataProgressRequest {
                        stream: request.envelope.stream.as_str().to_string(),
                        partition: request.envelope.partition,
                    },
                )
                .await?;
                for record in committed_records
                    .into_iter()
                    .filter(|record| progress.is_none_or(|offset| record.offset > offset))
                {
                    send_data_append(
                        &addr,
                        auth_token.clone(),
                        local_node_id,
                        target_node_id,
                        tls.clone(),
                        DataAppendRequest {
                            leader_id: request.leader_id,
                            leader_epoch: request.leader_epoch,
                            fsync: request.fsync,
                            committed_high_watermark: request.committed_high_watermark,
                            envelope: record,
                        },
                    )
                    .await?;
                }
                send_data_append(
                    &addr,
                    auth_token,
                    local_node_id,
                    target_node_id,
                    tls,
                    request,
                )
                .await
            });
        }
        while let Some(response) = joins.join_next().await {
            if let Ok(Ok(response)) = response {
                if response.match_offset == envelope.offset {
                    replicated += 1;
                }
                if response.flushed_offset == Some(envelope.offset) {
                    flushed += 1;
                }
            }
        }
        crate::broker_ensure!(replicated >= quorum, "partition quorum unavailable");
        if fsync {
            crate::broker_ensure!(flushed >= quorum, "partition fsync quorum unavailable");
        }
        self.partition_data.lock().unwrap().append(&request)?;
        let response = self
            .client_write(BrokerCommand::PartitionCommit {
                stream: envelope.stream.as_str().to_string(),
                partition: envelope.partition.0,
                offset: envelope.offset,
                checksum: crate::partition_log::committed_envelope_checksum(&envelope)?,
                leader_id: self.node_id,
                leader_epoch,
            })
            .await?;
        crate::broker_ensure!(
            matches!(
                response,
                BrokerResponse::PartitionCommit {
                    high_watermark,
                    leader_epoch: committed_epoch,
                } if high_watermark == envelope.offset && committed_epoch == leader_epoch
            ),
            "partition metadata commit rejected"
        );
        Ok(envelope)
    }

    async fn ensure_metadata_bootstrap(&self) -> Result<()> {
        let metadata = self.state_machine.durable_state();
        if !metadata.stream_definitions.is_empty() {
            return self.validate_metadata_configuration(&metadata);
        }
        let replicas = self.nodes.keys().copied().collect::<BTreeSet<_>>();
        let mut assignments = HashMap::new();
        for stream in &self.configured_streams {
            for partition in 0..stream.partitions {
                assignments.insert(
                    partition_key(stream.name.as_str(), partition),
                    PartitionAssignmentMetadata {
                        replicas: replicas.clone(),
                        leader_id: self.node_id,
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
        let assignments = self
            .state_machine
            .durable_state()
            .partition_assignments
            .into_iter()
            .filter(|(_, assignment)| assignment.leader_id != self.node_id)
            .collect::<Vec<_>>();
        for (key, assignment) in assignments {
            let (stream, partition) = key
                .rsplit_once(':')
                .ok_or_else(|| BrokerError::msg("invalid partition assignment key"))?;
            let partition = partition
                .parse::<u32>()
                .map_err(|_| BrokerError::msg("invalid partition assignment number"))?;
            let leader_epoch = assignment.leader_epoch.saturating_add(1);
            let response = self
                .client_write(BrokerCommand::PartitionLeaderUpdate {
                    stream: stream.to_string(),
                    partition,
                    leader_id: self.node_id,
                    leader_epoch,
                })
                .await?;
            crate::broker_ensure!(
                matches!(
                    response,
                    BrokerResponse::PartitionLeaderUpdate {
                        leader_id,
                        leader_epoch: committed_epoch,
                    } if leader_id == self.node_id && committed_epoch == leader_epoch
                ),
                "partition leader epoch update rejected"
            );
        }
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
        let replicas = self.nodes.keys().copied().collect::<BTreeSet<_>>();
        crate::broker_ensure!(
            metadata
                .partition_assignments
                .values()
                .all(|assignment| assignment.replicas == replicas),
            "local cluster membership differs from partition assignments"
        );
        Ok(())
    }

    pub async fn is_leader(&self) -> bool {
        self.raft.current_leader().await == Some(self.node_id)
    }

    pub async fn current_leader(&self) -> Option<u64> {
        self.raft.current_leader().await
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
