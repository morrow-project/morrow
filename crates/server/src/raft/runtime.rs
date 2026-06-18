use super::*;

#[derive(Clone)]
pub struct RaftRuntime {
    pub raft: BrokerRaft,
    pub state_machine: StateMachineStore,
    pub nodes: HashMap<u64, ClusterNode>,
    auth_token: String,
    node_id: u64,
    tls_enabled: bool,
}
#[derive(Debug, Clone)]
pub struct ClusterNode {
    pub raft_addr: SocketAddr,
    pub client_addr: SocketAddr,
}
impl RaftRuntime {
    pub async fn open(config: &ClusterConfig, tls_enabled: bool) -> Result<Self> {
        std::fs::create_dir_all(&config.raft_dir)
            .with_context(|| format!("creating Raft directory {}", config.raft_dir.display()))?;

        let nodes = config
            .nodes
            .iter()
            .map(|node| {
                (
                    node.node_id,
                    ClusterNode {
                        raft_addr: node.raft_addr,
                        client_addr: node.client_addr,
                    },
                )
            })
            .collect::<HashMap<_, _>>();
        let raft_nodes = config
            .nodes
            .iter()
            .map(|node| (node.node_id, BasicNode::new(node.raft_addr)))
            .collect::<BTreeMap<_, _>>();

        let log_store = LogStore::open(config.raft_dir.join(LOG_FILE))?;
        let state_machine = StateMachineStore::open(
            config.raft_dir.join(STATE_FILE),
            config.raft_dir.join(SNAPSHOT_FILE),
            raft_nodes.clone(),
        )?;
        let network = NetworkFactory {
            nodes: nodes.clone(),
            auth_token: config.auth_token.clone(),
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
        })
    }

    pub fn spawn_listener(&self, listen: SocketAddr) {
        let raft = self.raft.clone();
        let auth_token = self.auth_token.clone();
        tokio::spawn(async move {
            if let Err(err) = serve_raft(raft, listen, auth_token).await {
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
        self.state_machine.durable_state()
    }

    pub async fn is_leader(&self) -> bool {
        self.raft.current_leader().await == Some(self.node_id)
    }

    pub async fn current_leader(&self) -> Option<u64> {
        self.raft.current_leader().await
    }

    pub async fn leader_client_addr(&self) -> Option<SocketAddr> {
        let leader = self.raft.current_leader().await?;
        self.nodes.get(&leader).map(|node| node.client_addr)
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
