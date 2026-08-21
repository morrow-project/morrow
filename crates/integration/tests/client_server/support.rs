pub(super) use client::{Client, ClientAuth, ServerFrame};
pub(super) use server::{
    Broker, Config,
    config::{
        AuthClientConfig, AuthConfig, AuthPermissions, ClusterConfig, ClusterNodeConfig, TlsConfig,
    },
};
pub(super) use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};
pub(super) use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::Duration,
};
pub(super) const TLS_SERVER_NAME: &str = "localhost";

pub(super) fn auth_config_with_permissions(
    clients: Vec<(&ClientAuth, Option<Vec<String>>, Option<Vec<String>>)>,
) -> AuthConfig {
    AuthConfig {
        enabled: true,
        clients: clients
            .into_iter()
            .map(|(client, publish, subscribe)| {
                (
                    client.client_id().to_string(),
                    AuthClientConfig {
                        public_key: client.public_key_hex().to_ascii_lowercase(),
                        permissions: Some(AuthPermissions { publish, subscribe }),
                    },
                )
            })
            .collect(),
    }
}

pub(super) struct TestDir(PathBuf);
static NEXT_TEST_DIR: AtomicU64 = AtomicU64::new(0);

impl TestDir {
    pub(super) fn new() -> Self {
        let unique = NEXT_TEST_DIR.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "client-server-harness-{}-{unique}",
            std::process::id()
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }

    pub(super) fn path(&self) -> &Path {
        &self.0
    }
}
impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
pub(super) struct Harness {
    pub(super) addr: SocketAddr,
    pub(super) max_payload: usize,
    pub(super) broker: Broker,
    pub(super) server_task: tokio::task::JoinHandle<()>,
    pub(super) _wal_dir: TestDir,
}
pub(super) struct ClusterHarness {
    pub(super) nodes: Vec<ClusterHarnessNode>,
    pub(super) brokers: Vec<Broker>,
    pub(super) server_tasks: Vec<tokio::task::JoinHandle<()>>,
    pub(super) max_payload: usize,
    pub(super) _dirs: Vec<TestDir>,
}
pub(super) struct ClusterHarnessNode {
    pub(super) node_id: u64,
    pub(super) client_addr: SocketAddr,
    pub(super) raft_addr: SocketAddr,
    pub(super) route_addr: SocketAddr,
    pub(super) http_addr: SocketAddr,
}
impl ClusterHarness {
    pub(super) async fn start_three() -> Self {
        Self::start_three_with_routes(false).await
    }

    pub(super) async fn start_three_routed() -> Self {
        Self::start_three_with_routes(true).await
    }

    pub(super) async fn start_three_with_routes(enable_routes: bool) -> Self {
        let max_payload = 1024;
        let mut nodes = Vec::new();
        for node_id in 1..=3 {
            nodes.push(ClusterHarnessNode {
                node_id,
                client_addr: free_addr().await,
                raft_addr: free_addr().await,
                route_addr: free_addr().await,
                http_addr: free_addr().await,
            });
        }

        let mut dirs = Vec::new();
        let mut brokers = Vec::new();
        let mut server_tasks = Vec::new();
        for node in &nodes {
            let dir = TestDir::new();
            let listener = TcpListener::bind(node.client_addr).await.unwrap();
            let config = Config {
                listen: node.client_addr,
                http_listen: enable_routes.then_some(node.http_addr),
                admin_token: enable_routes.then_some("test-admin-token".to_string()),
                wal_dir: dir.path().join("wal"),
                wal_segment_bytes: server::wal::DEFAULT_WAL_SEGMENT_BYTES,
                fsync_interval_ms: 1,
                max_payload,
                max_control_line: 8192,
                verbose: false,
                tls: None,
                auth: Default::default(),
                cluster: Some(ClusterConfig {
                    enabled: true,
                    node_id: node.node_id,
                    auth_token: "test-cluster-token".to_string(),
                    raft_listen: node.raft_addr,
                    route_listen: enable_routes.then_some(node.route_addr),
                    routes: if !enable_routes || node.node_id == 1 {
                        Vec::new()
                    } else {
                        vec![nodes[0].route_addr.to_string()]
                    },
                    route_reconnect_ms: 50,
                    raft_dir: dir.path().join("raft"),
                    bootstrap: node.node_id == 1,
                    nodes: nodes
                        .iter()
                        .map(|node| ClusterNodeConfig {
                            node_id: node.node_id,
                            raft_addr: node.raft_addr.to_string(),
                            client_addr: node.client_addr.to_string(),
                        })
                        .collect(),
                    election_timeout_min_ms: 150,
                    election_timeout_max_ms: 300,
                    heartbeat_interval_ms: 50,
                    snapshot_threshold: 100,
                }),
                streams: test_streams(),
            };
            let broker = Broker::open(config).unwrap();
            let server = broker.clone();
            let server_task = tokio::spawn(async move {
                if let Err(err) = server.serve_listener(listener).await {
                    panic!("cluster server failed: {err:#}");
                }
            });
            brokers.push(broker);
            server_tasks.push(server_task);
            dirs.push(dir);
        }

        Self {
            nodes,
            brokers,
            server_tasks,
            max_payload,
            _dirs: dirs,
        }
    }

    pub(super) async fn wait_for_leader(&self) -> u64 {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let mut observed = Vec::with_capacity(self.brokers.len());
            for broker in &self.brokers {
                observed.push(broker.cluster_leader().await);
            }
            if let Some(leader) = observed.first().copied().flatten()
                && observed.iter().all(|observed| *observed == Some(leader))
                && self.nodes.iter().any(|node| node.node_id == leader)
            {
                return leader;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "cluster did not elect a leader"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    pub(super) async fn wait_until_follower_knows_leader(&self, follower_id: u64, leader_id: u64) {
        let Some(follower_index) = self
            .nodes
            .iter()
            .position(|node| node.node_id == follower_id)
        else {
            panic!("unknown follower node {follower_id}");
        };
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            if self.brokers[follower_index].cluster_leader().await == Some(leader_id) {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "follower {follower_id} did not learn leader {leader_id}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    pub(super) async fn wait_for_full_route_mesh(&self) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let mut ready = true;
            for node in &self.nodes {
                let Some(value) = cluster_json(node.http_addr).await else {
                    ready = false;
                    continue;
                };
                let Some(connected) = value["routes"]["connected"].as_array() else {
                    ready = false;
                    continue;
                };
                ready &= connected.len() == self.nodes.len() - 1;
            }
            if ready {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "route mesh did not become full"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    pub(super) async fn wait_for_route_interest(&self, observer_index: usize, subject: &str) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(value) = cluster_json(self.nodes[observer_index].http_addr).await {
                if value["routes"]["connected"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .flat_map(|peer| peer["subjects"].as_array().into_iter().flatten())
                    .any(|value| value.as_str() == Some(subject))
                {
                    return;
                }
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "route interest {subject} did not propagate"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    pub(super) async fn shutdown(self) {
        for broker in &self.brokers {
            broker.shutdown().await.unwrap();
        }
        for task in self.server_tasks {
            task.abort();
        }
    }
}
pub(super) async fn free_addr() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    listener.local_addr().unwrap()
}
pub(super) async fn cluster_json(addr: SocketAddr) -> Option<serde_json::Value> {
    let mut stream = TcpStream::connect(addr).await.ok()?;
    stream
        .write_all(
            b"GET /cluster HTTP/1.1\r\nhost: localhost\r\nauthorization: Bearer test-admin-token\r\n\r\n",
        )
        .await
        .ok()?;
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.ok()?;
    let response = String::from_utf8(response).ok()?;
    let (_, body) = response.split_once("\r\n\r\n")?;
    serde_json::from_str(body).ok()
}

pub(super) async fn wait_for_partition_metadata(
    addr: SocketAddr,
    stream: &str,
    partition: u64,
    high_watermark: Option<u64>,
) -> serde_json::Value {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(value) = cluster_json(addr).await
            && let Some(found) =
                value["partitions"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .find(|candidate| {
                        candidate["stream"] == stream
                            && candidate["partition"] == partition
                            && high_watermark
                                .is_none_or(|expected| candidate["high_watermark"] == expected)
                    })
        {
            return found.clone();
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "partition metadata did not converge"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}
impl Harness {
    pub(super) async fn start() -> Self {
        Self::start_with_config(Default::default(), None).await
    }

    pub(super) async fn start_with_auth(clients: &[&ClientAuth]) -> Self {
        Self::start_with_auth_and_tls(clients, None).await
    }

    pub(super) async fn start_tls() -> Self {
        Self::start_with_config(Default::default(), Some(tls_config())).await
    }

    pub(super) async fn start_tls_with_auth(clients: &[&ClientAuth]) -> Self {
        Self::start_with_auth_and_tls(clients, Some(tls_config())).await
    }

    pub(super) async fn start_with_auth_and_tls(
        clients: &[&ClientAuth],
        tls: Option<TlsConfig>,
    ) -> Self {
        let auth = AuthConfig {
            enabled: true,
            clients: clients
                .iter()
                .map(|client| {
                    (
                        client.client_id().to_string(),
                        AuthClientConfig {
                            public_key: client.public_key_hex().to_ascii_lowercase(),
                            permissions: None,
                        },
                    )
                })
                .collect(),
        };
        Self::start_with_config(auth, tls).await
    }

    pub(super) async fn start_with_config(auth: AuthConfig, tls: Option<TlsConfig>) -> Self {
        let wal_dir = TestDir::new();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let max_payload = 1024;
        let config = Config {
            listen: addr,
            http_listen: None,
            admin_token: None,
            wal_dir: wal_dir.path().to_path_buf(),
            wal_segment_bytes: server::wal::DEFAULT_WAL_SEGMENT_BYTES,
            fsync_interval_ms: 1,
            max_payload,
            max_control_line: 8192,
            verbose: false,
            tls,
            auth,
            cluster: None,
            streams: test_streams(),
        };
        let broker = Broker::open(config).unwrap();
        let server = broker.clone();
        let server_task = tokio::spawn(async move {
            if let Err(err) = server.serve_listener(listener).await {
                panic!("server failed: {err:#}");
            }
        });
        Self {
            addr,
            max_payload,
            broker,
            server_task,
            _wal_dir: wal_dir,
        }
    }

    pub(super) async fn shutdown(self) {
        self.broker.shutdown().await.unwrap();
        self.server_task.abort();
    }
}
pub(super) fn tls_config() -> TlsConfig {
    TlsConfig {
        cert_file: tls_cert_file(),
        key_file: tls_key_file(),
        handshake_timeout_ms: 100,
    }
}

fn test_streams() -> server::stream::StreamCatalog {
    server::stream::StreamCatalog::new(
        [("orders", "orders.>"), ("service", "service.>")]
            .into_iter()
            .map(|(name, subject)| server::stream::StreamDefinition {
                name: server::stream::StreamId::new(name).unwrap(),
                subjects: vec![subject.to_string()],
                partitions: 1,
                partitioning: Default::default(),
                storage: Default::default(),
                retention: Default::default(),
            })
            .collect(),
    )
    .unwrap()
}
pub(super) fn tls_cert_file() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/server-cert.pem")
}
pub(super) fn tls_key_file() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/server-key.pem")
}
pub(super) fn tls_ca_cert_file() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/ca-cert.pem")
}
