use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use client::{Client, ClientAuth, ServerFrame};
use server::{
    Broker, Config,
    config::{AuthConfig, ClusterConfig, ClusterNodeConfig, TlsConfig},
};
use tokio::{net::TcpListener, time::Duration};

const TLS_SERVER_NAME: &str = "localhost";

struct TestDir(PathBuf);

impl TestDir {
    fn new() -> Self {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("client-server-harness-{unique}"));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[tokio::test]
async fn client_can_subscribe_publish_receive_and_ack_against_server() {
    let harness = Harness::start().await;
    let mut subscriber = Client::connect(harness.addr, harness.max_payload)
        .await
        .unwrap();
    let info = subscriber.read_info().await.unwrap();
    assert!(!info.auth_required);
    assert!(info.nonce.is_none());
    subscriber
        .connect_durable("subscriber1", false, 5_000, 16)
        .await
        .unwrap();
    subscriber.subscribe("orders.*", "sid1").await.unwrap();
    subscriber.ping_roundtrip().await.unwrap();

    let mut publisher = Client::connect(harness.addr, harness.max_payload)
        .await
        .unwrap();
    publisher.read_info().await.unwrap();
    publisher
        .connect_durable("publisher1", false, 5_000, 16)
        .await
        .unwrap();
    publisher.publish("orders.created", b"hello").await.unwrap();

    let message = subscriber.next_message().await.unwrap();
    assert_eq!(message.subject, "orders.created");
    assert_eq!(message.sid, "sid1");
    assert_eq!(message.payload, b"hello");
    let ack_subject = message
        .ack_subject
        .expect("durable messages carry ack subject");
    subscriber.ack(&ack_subject).await.unwrap();
    subscriber.ping_roundtrip().await.unwrap();

    harness.shutdown().await;
}

#[tokio::test]
async fn client_request_receives_response_from_durable_responder() {
    let harness = Harness::start().await;
    let mut responder = Client::connect(harness.addr, harness.max_payload)
        .await
        .unwrap();
    responder.read_info().await.unwrap();
    responder
        .connect_durable("responder1", false, 5_000, 16)
        .await
        .unwrap();
    responder.subscribe("service.echo", "sid1").await.unwrap();
    responder.ping_roundtrip().await.unwrap();

    let responder_task = tokio::spawn(async move {
        let message = responder.next_message().await.unwrap();
        assert_eq!(message.subject, "service.echo");
        assert_eq!(message.payload, b"hello");
        assert!(
            message
                .reply_to
                .as_deref()
                .is_some_and(|reply| reply.starts_with("_INBOX."))
        );
        assert!(
            message
                .ack_subject
                .as_deref()
                .is_some_and(|ack| ack.starts_with("_BROKER.ACK."))
        );
        responder.respond(&message, b"world").await.unwrap();
        responder
            .ack(message.ack_subject.as_deref().unwrap())
            .await
            .unwrap();
    });

    let mut requester = Client::connect(harness.addr, harness.max_payload)
        .await
        .unwrap();
    requester.read_info().await.unwrap();
    requester
        .connect_durable("requester1", false, 5_000, 16)
        .await
        .unwrap();
    let response = requester
        .request("service.echo", b"hello", Duration::from_secs(3))
        .await
        .unwrap();
    assert!(response.subject.starts_with("_INBOX."));
    assert_eq!(response.payload, b"world");
    assert!(response.ack_subject.is_none());
    responder_task.await.unwrap();

    harness.shutdown().await;
}

#[tokio::test]
async fn authenticated_client_can_subscribe_publish_receive_and_ack() {
    let subscriber_auth = ClientAuth::from_seed("subscriber1", [7; 32]);
    let publisher_auth = ClientAuth::from_seed("publisher1", [8; 32]);
    let harness = Harness::start_with_auth(&[&subscriber_auth, &publisher_auth]).await;

    let mut subscriber = Client::connect(harness.addr, harness.max_payload)
        .await
        .unwrap();
    let info = subscriber.read_info().await.unwrap();
    assert!(info.auth_required);
    assert_eq!(info.nonce.as_ref().unwrap().len(), 64);
    subscriber
        .connect_authenticated(&info, &subscriber_auth, false, 5_000, 16)
        .await
        .unwrap();
    subscriber.subscribe("orders.*", "sid1").await.unwrap();
    subscriber.ping_roundtrip().await.unwrap();

    let mut publisher = Client::connect(harness.addr, harness.max_payload)
        .await
        .unwrap();
    let info = publisher.read_info().await.unwrap();
    publisher
        .connect_authenticated(&info, &publisher_auth, false, 5_000, 16)
        .await
        .unwrap();
    publisher.publish("orders.created", b"hello").await.unwrap();

    let message = subscriber.next_message().await.unwrap();
    assert_eq!(message.subject, "orders.created");
    assert_eq!(message.sid, "sid1");
    assert_eq!(message.payload, b"hello");
    let ack_subject = message
        .ack_subject
        .expect("durable messages carry ack subject");
    subscriber.ack(&ack_subject).await.unwrap();
    subscriber.ping_roundtrip().await.unwrap();

    harness.shutdown().await;
}

#[tokio::test]
async fn authenticated_connect_rejects_invalid_signature() {
    let configured_auth = ClientAuth::from_seed("client1", [7; 32]);
    let wrong_auth = ClientAuth::from_seed("client1", [9; 32]);
    let harness = Harness::start_with_auth(&[&configured_auth]).await;

    let mut client = Client::connect(harness.addr, harness.max_payload)
        .await
        .unwrap();
    let info = client.read_info().await.unwrap();
    client
        .connect_authenticated(&info, &wrong_auth, false, 5_000, 16)
        .await
        .unwrap();

    match client.next_frame().await.unwrap().unwrap() {
        ServerFrame::Err(error) => assert!(error.contains("invalid public key signature")),
        frame => panic!("expected auth error, got {frame:?}"),
    }

    harness.shutdown().await;
}

#[tokio::test]
async fn auth_nonce_is_fresh_per_connection() {
    let auth = ClientAuth::from_seed("client1", [7; 32]);
    let harness = Harness::start_with_auth(&[&auth]).await;

    let mut first = Client::connect(harness.addr, harness.max_payload)
        .await
        .unwrap();
    let first_info = first.read_info().await.unwrap();
    let mut second = Client::connect(harness.addr, harness.max_payload)
        .await
        .unwrap();
    let second_info = second.read_info().await.unwrap();

    assert!(first_info.auth_required);
    assert!(second_info.auth_required);
    assert_ne!(first_info.nonce, second_info.nonce);

    harness.shutdown().await;
}

#[tokio::test]
async fn tls_client_can_subscribe_publish_receive_and_ack() {
    let harness = Harness::start_tls().await;
    let mut subscriber = Client::connect_tls(
        harness.addr,
        TLS_SERVER_NAME,
        tls_ca_cert_file(),
        harness.max_payload,
    )
    .await
    .unwrap();
    let info = subscriber.read_info().await.unwrap();
    assert!(!info.auth_required);
    subscriber
        .connect_durable("subscriber1", false, 5_000, 16)
        .await
        .unwrap();
    subscriber.subscribe("orders.*", "sid1").await.unwrap();
    subscriber.ping_roundtrip().await.unwrap();

    let mut publisher = Client::connect_tls(
        harness.addr,
        TLS_SERVER_NAME,
        tls_ca_cert_file(),
        harness.max_payload,
    )
    .await
    .unwrap();
    publisher.read_info().await.unwrap();
    publisher
        .connect_durable("publisher1", false, 5_000, 16)
        .await
        .unwrap();
    publisher.publish("orders.created", b"hello").await.unwrap();

    let message = subscriber.next_message().await.unwrap();
    assert_eq!(message.subject, "orders.created");
    assert_eq!(message.sid, "sid1");
    assert_eq!(message.payload, b"hello");
    let ack_subject = message
        .ack_subject
        .expect("durable messages carry ack subject");
    subscriber.ack(&ack_subject).await.unwrap();
    subscriber.ping_roundtrip().await.unwrap();

    harness.shutdown().await;
}

#[tokio::test]
async fn tls_authenticated_client_can_subscribe_publish_receive_and_ack() {
    let subscriber_auth = ClientAuth::from_seed("subscriber1", [7; 32]);
    let publisher_auth = ClientAuth::from_seed("publisher1", [8; 32]);
    let harness = Harness::start_tls_with_auth(&[&subscriber_auth, &publisher_auth]).await;

    let mut subscriber = Client::connect_tls(
        harness.addr,
        TLS_SERVER_NAME,
        tls_ca_cert_file(),
        harness.max_payload,
    )
    .await
    .unwrap();
    let info = subscriber.read_info().await.unwrap();
    assert!(info.auth_required);
    subscriber
        .connect_authenticated(&info, &subscriber_auth, false, 5_000, 16)
        .await
        .unwrap();
    subscriber.subscribe("orders.*", "sid1").await.unwrap();
    subscriber.ping_roundtrip().await.unwrap();

    let mut publisher = Client::connect_tls(
        harness.addr,
        TLS_SERVER_NAME,
        tls_ca_cert_file(),
        harness.max_payload,
    )
    .await
    .unwrap();
    let info = publisher.read_info().await.unwrap();
    publisher
        .connect_authenticated(&info, &publisher_auth, false, 5_000, 16)
        .await
        .unwrap();
    publisher.publish("orders.created", b"hello").await.unwrap();

    let message = subscriber.next_message().await.unwrap();
    assert_eq!(message.subject, "orders.created");
    assert_eq!(message.sid, "sid1");
    assert_eq!(message.payload, b"hello");
    let ack_subject = message
        .ack_subject
        .expect("durable messages carry ack subject");
    subscriber.ack(&ack_subject).await.unwrap();
    subscriber.ping_roundtrip().await.unwrap();

    harness.shutdown().await;
}

#[tokio::test]
async fn plain_client_does_not_complete_info_against_tls_listener() {
    let harness = Harness::start_tls().await;
    let mut client = Client::connect(harness.addr, harness.max_payload)
        .await
        .unwrap();

    let read = tokio::time::timeout(Duration::from_secs(1), client.read_info()).await;
    assert!(
        read.is_err() || read.unwrap().is_err(),
        "plain client unexpectedly read INFO from TLS listener"
    );

    harness.shutdown().await;
}

#[tokio::test]
async fn clustered_follower_proxies_client_to_leader() {
    let harness = ClusterHarness::start_three().await;
    let leader = harness.wait_for_leader().await;
    let follower = harness
        .nodes
        .iter()
        .find(|node| node.node_id != leader)
        .expect("three node cluster has a follower");
    harness
        .wait_until_follower_knows_leader(follower.node_id, leader)
        .await;

    let mut subscriber = Client::connect(follower.client_addr, harness.max_payload)
        .await
        .unwrap();
    subscriber.read_info().await.unwrap();
    subscriber
        .connect_durable("subscriber1", false, 5_000, 16)
        .await
        .unwrap();
    subscriber.subscribe("orders.*", "sid1").await.unwrap();
    subscriber.ping_roundtrip().await.unwrap();

    let mut publisher = Client::connect(follower.client_addr, harness.max_payload)
        .await
        .unwrap();
    publisher.read_info().await.unwrap();
    publisher
        .connect_durable("publisher1", false, 5_000, 16)
        .await
        .unwrap();
    publisher.publish("orders.created", b"hello").await.unwrap();

    let message = tokio::time::timeout(Duration::from_secs(5), subscriber.next_message())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(message.subject, "orders.created");
    assert_eq!(message.payload, b"hello");
    subscriber
        .ack(message.ack_subject.as_deref().unwrap())
        .await
        .unwrap();

    harness.shutdown().await;
}

struct Harness {
    addr: SocketAddr,
    max_payload: usize,
    broker: Broker,
    server_task: tokio::task::JoinHandle<()>,
    _wal_dir: TestDir,
}

struct ClusterHarness {
    nodes: Vec<ClusterHarnessNode>,
    brokers: Vec<Broker>,
    server_tasks: Vec<tokio::task::JoinHandle<()>>,
    max_payload: usize,
    _dirs: Vec<TestDir>,
}

struct ClusterHarnessNode {
    node_id: u64,
    client_addr: SocketAddr,
    raft_addr: SocketAddr,
}

impl ClusterHarness {
    async fn start_three() -> Self {
        let max_payload = 1024;
        let mut nodes = Vec::new();
        for node_id in 1..=3 {
            nodes.push(ClusterHarnessNode {
                node_id,
                client_addr: free_addr().await,
                raft_addr: free_addr().await,
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
                http_listen: None,
                wal_dir: dir.path().join("wal"),
                fsync_interval_ms: 1,
                max_payload,
                verbose: false,
                tls: None,
                auth: Default::default(),
                cluster: Some(ClusterConfig {
                    enabled: true,
                    node_id: node.node_id,
                    raft_listen: node.raft_addr,
                    raft_dir: dir.path().join("raft"),
                    bootstrap: node.node_id == 1,
                    nodes: nodes
                        .iter()
                        .map(|node| ClusterNodeConfig {
                            node_id: node.node_id,
                            raft_addr: node.raft_addr,
                            client_addr: node.client_addr,
                        })
                        .collect(),
                    election_timeout_min_ms: 150,
                    election_timeout_max_ms: 300,
                    heartbeat_interval_ms: 50,
                    snapshot_threshold: 100,
                }),
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

    async fn wait_for_leader(&self) -> u64 {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            for broker in &self.brokers {
                if let Some(leader) = broker.cluster_leader().await {
                    return leader;
                }
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "cluster did not elect a leader"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    async fn wait_until_follower_knows_leader(&self, follower_id: u64, leader_id: u64) {
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

    async fn shutdown(self) {
        for broker in &self.brokers {
            broker.shutdown().await.unwrap();
        }
        for task in self.server_tasks {
            task.abort();
        }
    }
}

async fn free_addr() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    listener.local_addr().unwrap()
}

impl Harness {
    async fn start() -> Self {
        Self::start_with_config(Default::default(), None).await
    }

    async fn start_with_auth(clients: &[&ClientAuth]) -> Self {
        Self::start_with_auth_and_tls(clients, None).await
    }

    async fn start_tls() -> Self {
        Self::start_with_config(Default::default(), Some(tls_config())).await
    }

    async fn start_tls_with_auth(clients: &[&ClientAuth]) -> Self {
        Self::start_with_auth_and_tls(clients, Some(tls_config())).await
    }

    async fn start_with_auth_and_tls(clients: &[&ClientAuth], tls: Option<TlsConfig>) -> Self {
        let auth = AuthConfig {
            enabled: true,
            clients: clients
                .iter()
                .map(|client| {
                    (
                        client.client_id().to_string(),
                        client.public_key_hex().to_ascii_lowercase(),
                    )
                })
                .collect(),
        };
        Self::start_with_config(auth, tls).await
    }

    async fn start_with_config(auth: AuthConfig, tls: Option<TlsConfig>) -> Self {
        let wal_dir = TestDir::new();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let max_payload = 1024;
        let config = Config {
            listen: addr,
            http_listen: None,
            wal_dir: wal_dir.path().to_path_buf(),
            fsync_interval_ms: 1,
            max_payload,
            verbose: false,
            tls,
            auth,
            cluster: None,
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

    async fn shutdown(self) {
        self.broker.shutdown().await.unwrap();
        self.server_task.abort();
    }
}

fn tls_config() -> TlsConfig {
    TlsConfig {
        cert_file: tls_cert_file(),
        key_file: tls_key_file(),
        handshake_timeout_ms: 100,
    }
}

fn tls_cert_file() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/server-cert.pem")
}

fn tls_key_file() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/server-key.pem")
}

fn tls_ca_cert_file() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/ca-cert.pem")
}
