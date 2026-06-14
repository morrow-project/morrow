use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use client::{Client, ClientAuth, ServerFrame};
use server::{Broker, Config, config::AuthConfig};
use tokio::net::TcpListener;

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
        .reply_to
        .expect("durable messages carry ack subject");
    subscriber.ack(&ack_subject).await.unwrap();
    subscriber.ping_roundtrip().await.unwrap();

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
        .reply_to
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

struct Harness {
    addr: SocketAddr,
    max_payload: usize,
    broker: Broker,
    server_task: tokio::task::JoinHandle<()>,
    _wal_dir: TestDir,
}

impl Harness {
    async fn start() -> Self {
        Self::start_with_config(Default::default()).await
    }

    async fn start_with_auth(clients: &[&ClientAuth]) -> Self {
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
        Self::start_with_config(auth).await
    }

    async fn start_with_config(auth: AuthConfig) -> Self {
        let wal_dir = TestDir::new();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let max_payload = 1024;
        let config = Config {
            listen: addr,
            wal_dir: wal_dir.path().to_path_buf(),
            fsync_interval_ms: 1,
            max_payload,
            verbose: false,
            tls: None,
            auth,
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
