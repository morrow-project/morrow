use super::port_allocator::free_addr;
pub(super) use client::{Client, ClientAuth, ServerFrame};
pub(super) use server::{
    Config, Morrow,
    config::{
        AuthClientConfig, AuthConfig, AuthPermissions, ClusterConfig, ClusterNodeConfig,
        InternalTlsConfig, TlsConfig,
    },
};
pub(super) use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{
        Arc, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
};
pub(super) use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::Duration,
};
pub(super) const TLS_SERVER_NAME: &str = "localhost";

trait AsyncStream: tokio::io::AsyncRead + tokio::io::AsyncWrite {}
impl<T: tokio::io::AsyncRead + tokio::io::AsyncWrite + ?Sized> AsyncStream for T {}

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
                        tenant: "default".to_string(),
                        namespace: "default".to_string(),
                        expires_at_ms: None,
                        external_subject: None,
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
    pub(super) broker: Morrow,
    pub(super) server_task: tokio::task::JoinHandle<()>,
    pub(super) _wal_dir: TestDir,
}
pub(super) struct ClusterHarness {
    pub(super) nodes: Vec<ClusterHarnessNode>,
    pub(super) brokers: Vec<Morrow>,
    pub(super) server_tasks: Vec<tokio::task::JoinHandle<()>>,
    pub(super) max_payload: usize,
    secure: bool,
    pub(super) _dirs: Vec<TestDir>,
    _test_lock: tokio::sync::OwnedMutexGuard<()>,
}
static CLUSTER_TEST_LOCK: OnceLock<Arc<tokio::sync::Mutex<()>>> = OnceLock::new();
pub(super) struct ClusterHarnessNode {
    pub(super) node_id: u64,
    pub(super) client_addr: SocketAddr,
    pub(super) raft_addr: SocketAddr,
    pub(super) route_addr: SocketAddr,
    pub(super) http_addr: SocketAddr,
}
impl ClusterHarness {
    pub(super) async fn start_three() -> Self {
        Self::start_three_with_options(false, false).await
    }

    pub(super) async fn start_three_routed() -> Self {
        Self::start_three_with_options(true, false).await
    }

    pub(super) async fn start_three_secure() -> Self {
        Self::start_three_with_options(true, true).await
    }

    pub(super) async fn start_three_with_auth(auth: AuthConfig) -> Self {
        Self::start_three_with_runtime_options(false, false, auth).await
    }

    pub(super) async fn start_three_with_options(enable_routes: bool, secure: bool) -> Self {
        Self::start_three_with_runtime_options(enable_routes, secure, AuthConfig::default()).await
    }

    async fn start_three_with_runtime_options(
        enable_routes: bool,
        secure: bool,
        auth: AuthConfig,
    ) -> Self {
        let test_lock = CLUSTER_TEST_LOCK
            .get_or_init(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
            .lock_owned()
            .await;
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
                admin_tls: secure.then(tls_config),
                quotas: Default::default(),
                wal_dir: dir.path().join("wal"),
                wal_segment_bytes: server::wal::DEFAULT_WAL_SEGMENT_BYTES,
                fsync_interval_ms: 1,
                max_payload,
                max_control_line: 8192,
                max_ack_timeout_ms: server::config::DEFAULT_MAX_ACK_TIMEOUT_MS,
                max_in_flight: server::config::DEFAULT_MAX_IN_FLIGHT,
                max_fetch_messages: server::config::DEFAULT_MAX_FETCH_MESSAGES,
                max_fetch_bytes: server::config::DEFAULT_MAX_FETCH_BYTES,
                max_encoded_batch_bytes: server::config::DEFAULT_MAX_ENCODED_BATCH_BYTES,
                verbose: false,
                tls: None,
                auth: auth.clone(),
                cluster: Some(ClusterConfig {
                    enabled: true,
                    node_id: node.node_id,
                    auth_token: "test-cluster-token".to_string(),
                    raft_listen: node.raft_addr,
                    raft_tls: secure.then(|| internal_tls_config(node.node_id)),
                    allow_insecure_internal_transports: !secure,
                    route_listen: enable_routes.then_some(node.route_addr),
                    route_advertise: enable_routes.then(|| node.route_addr.to_string()),
                    route_tls: secure.then(|| internal_tls_config(node.node_id)),
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
                            route_addr: enable_routes.then(|| node.route_addr.to_string()),
                            tls_server_name: secure.then(|| format!("node-{}", node.node_id)),
                            tls_cert_files: if secure {
                                vec![internal_cert_file(node.node_id)]
                            } else {
                                Vec::new()
                            },
                        })
                        .collect(),
                    election_timeout_min_ms: 150,
                    election_timeout_max_ms: 300,
                    heartbeat_interval_ms: 50,
                    snapshot_threshold: 100,
                }),
                streams: test_streams(),
            };
            let broker = Morrow::open(config).unwrap();
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
            secure,
            _dirs: dirs,
            _test_lock: test_lock,
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
        const ROUTE_MESH_TIMEOUT: Duration = Duration::from_secs(30);
        let deadline = tokio::time::Instant::now() + ROUTE_MESH_TIMEOUT;
        let mut observed = Vec::with_capacity(self.nodes.len());
        loop {
            let mut ready = true;
            observed.clear();
            for node in &self.nodes {
                let Some(value) = cluster_json_with_tls(node.http_addr, self.secure).await else {
                    ready = false;
                    observed.push(format!("node {}: admin endpoint unavailable", node.node_id));
                    continue;
                };
                let Some(connected) = value["routes"]["connected"].as_array() else {
                    ready = false;
                    observed.push(format!(
                        "node {}: connected route list unavailable",
                        node.node_id
                    ));
                    continue;
                };
                let connected_count = connected.len();
                ready &= connected_count == self.nodes.len() - 1;
                observed.push(format!(
                    "node {}: {connected_count}/{} connected",
                    node.node_id,
                    self.nodes.len() - 1
                ));
            }
            if ready {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "route mesh did not become full within {ROUTE_MESH_TIMEOUT:?}: {}",
                observed.join(", ")
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    pub(super) async fn wait_for_admin_tls(&self, node_index: usize) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if cluster_json_with_tls(self.nodes[node_index].http_addr, true)
                .await
                .is_some()
            {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "admin TLS listener did not become ready"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    pub(super) async fn wait_for_route_interest(&self, observer_index: usize, subject: &str) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(value) =
                cluster_json_with_tls(self.nodes[observer_index].http_addr, self.secure).await
            {
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
pub(super) async fn cluster_json(addr: SocketAddr) -> Option<serde_json::Value> {
    admin_json(addr, "/cluster").await
}

pub(super) async fn admin_json(addr: SocketAddr, path: &str) -> Option<serde_json::Value> {
    admin_json_with_tls(addr, false, path).await
}

pub(super) async fn cluster_json_with_tls(
    addr: SocketAddr,
    secure: bool,
) -> Option<serde_json::Value> {
    admin_json_with_tls(addr, secure, "/cluster").await
}

async fn admin_json_with_tls(
    addr: SocketAddr,
    secure: bool,
    path: &str,
) -> Option<serde_json::Value> {
    let stream = tokio::time::timeout(Duration::from_millis(250), TcpStream::connect(addr))
        .await
        .ok()?
        .ok()?;
    let mut stream: Box<dyn AsyncStream + Unpin> = if secure {
        let connector = internal_tls_connector(tls_ca_cert_file())?;
        Box::new(
            tokio::time::timeout(
                Duration::from_millis(250),
                connector.connect(
                    rustls::pki_types::ServerName::try_from("localhost").ok()?,
                    stream,
                ),
            )
            .await
            .ok()?
            .ok()?,
        )
    } else {
        Box::new(stream)
    };
    stream
        .write_all(
            format!(
                "GET {path} HTTP/1.1\r\nhost: localhost\r\nauthorization: Bearer test-admin-token\r\n\r\n"
            )
            .as_bytes(),
        )
        .await
        .ok()?;
    let mut response = Vec::new();
    tokio::time::timeout(
        Duration::from_millis(250),
        stream.read_to_end(&mut response),
    )
    .await
    .ok()?
    .ok()?;
    let response = String::from_utf8(response).ok()?;
    let (_, body) = response.split_once("\r\n\r\n")?;
    serde_json::from_str(body).ok()
}

fn internal_tls_connector(ca_file: PathBuf) -> Option<tokio_rustls::TlsConnector> {
    let certs = broker_pem::load_certificates(ca_file).ok()?;
    let mut roots = rustls::RootCertStore::empty();
    roots.add_parsable_certificates(certs);
    Some(tokio_rustls::TlsConnector::from(std::sync::Arc::new(
        rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    )))
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
                            tenant: "default".to_string(),
                            namespace: "default".to_string(),
                            expires_at_ms: None,
                            external_subject: None,
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
            admin_tls: None,
            quotas: Default::default(),
            wal_dir: wal_dir.path().to_path_buf(),
            wal_segment_bytes: server::wal::DEFAULT_WAL_SEGMENT_BYTES,
            fsync_interval_ms: 1,
            max_payload,
            max_control_line: 8192,
            max_ack_timeout_ms: server::config::DEFAULT_MAX_ACK_TIMEOUT_MS,
            max_in_flight: server::config::DEFAULT_MAX_IN_FLIGHT,
            max_fetch_messages: server::config::DEFAULT_MAX_FETCH_MESSAGES,
            max_fetch_bytes: server::config::DEFAULT_MAX_FETCH_BYTES,
            max_encoded_batch_bytes: server::config::DEFAULT_MAX_ENCODED_BATCH_BYTES,
            verbose: false,
            tls,
            auth,
            cluster: None,
            streams: test_streams(),
        };
        let broker = Morrow::open(config).unwrap();
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
        [("orders", "orders/**"), ("service", "service/**")]
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
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/morrow-cert.pem")
}
pub(super) fn tls_key_file() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/morrow-key.pem")
}
pub(super) fn tls_ca_cert_file() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/ca-cert.pem")
}
pub(super) fn internal_tls_config(node_id: u64) -> InternalTlsConfig {
    InternalTlsConfig {
        cert_file: internal_cert_file(node_id),
        key_file: internal_fixture(format!("node-{node_id}-key.pem")),
        ca_cert_file: internal_fixture("ca-cert.pem"),
        handshake_timeout_ms: 500,
    }
}

pub(super) fn internal_cert_file(node_id: u64) -> PathBuf {
    internal_fixture(format!("node-{node_id}-cert.pem"))
}

fn internal_fixture(name: impl AsRef<Path>) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/internal")
        .join(name)
}
