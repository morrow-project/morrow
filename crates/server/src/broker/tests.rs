use super::*;
use crate::config::{ClusterConfig, ClusterNodeConfig};
use std::{path::Path, sync::Arc, time::Duration};
use tempfile::TempDir;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader, DuplexStream},
    sync::mpsc,
    task::JoinHandle,
};
struct Scenario {
    _dir: TempDir,
    clock: Arc<ManualClock>,
    broker: Morrow,
    fake_cluster: Option<FakeClusterRuntime>,
}
impl Scenario {
    fn new() -> Self {
        let dir = TempDir::new().unwrap();
        let clock = Arc::new(ManualClock::new(1_000));
        let broker = deterministic_broker(test_config(dir.path()), clock.clone(), None);
        Self {
            _dir: dir,
            clock,
            broker,
            fake_cluster: None,
        }
    }

    fn new_with_quotas(quotas: crate::config::ResourceQuotaConfig) -> Self {
        let dir = TempDir::new().unwrap();
        let clock = Arc::new(ManualClock::new(1_000));
        let mut config = test_config(dir.path());
        config.quotas = quotas;
        let broker = deterministic_broker(config, clock.clone(), None);
        Self {
            _dir: dir,
            clock,
            broker,
            fake_cluster: None,
        }
    }

    fn new_with_wal_segment_bytes(wal_segment_bytes: u64) -> Self {
        let dir = TempDir::new().unwrap();
        let clock = Arc::new(ManualClock::new(1_000));
        let mut config = test_config(dir.path());
        config.wal_segment_bytes = wal_segment_bytes;
        let broker = deterministic_broker(config, clock.clone(), None);
        Self {
            _dir: dir,
            clock,
            broker,
            fake_cluster: None,
        }
    }

    fn new_fake_cluster(node_count: u64) -> Self {
        Self::new_fake_cluster_local_node(node_count, 1, Some(1))
    }

    fn new_fake_cluster_local_node(
        node_count: u64,
        local_node_id: u64,
        leader: Option<u64>,
    ) -> Self {
        Self::new_fake_cluster_local_node_with_routes(node_count, local_node_id, leader, false)
    }

    fn new_fake_route_cluster_local_node(
        node_count: u64,
        local_node_id: u64,
        leader: Option<u64>,
    ) -> Self {
        Self::new_fake_cluster_local_node_with_routes(node_count, local_node_id, leader, true)
    }

    fn new_fake_cluster_local_node_with_routes(
        node_count: u64,
        local_node_id: u64,
        leader: Option<u64>,
        route_mesh: bool,
    ) -> Self {
        let dir = TempDir::new().unwrap();
        let clock = Arc::new(ManualClock::new(1_000));
        let fake_cluster = FakeClusterRuntime::new(node_count, local_node_id, leader);
        let mut config = test_config(dir.path());
        if route_mesh {
            config.cluster = Some(fake_cluster_config(dir.path(), node_count, local_node_id));
        }
        let broker = deterministic_broker(
            config,
            clock.clone(),
            Some(ClusterRuntime::Fake(fake_cluster.clone())),
        );
        Self {
            _dir: dir,
            clock,
            broker,
            fake_cluster: Some(fake_cluster),
        }
    }

    fn broker(&self) -> &Morrow {
        &self.broker
    }

    fn fake_cluster(&self) -> &FakeClusterRuntime {
        self.fake_cluster.as_ref().unwrap()
    }

    fn set_leader(&self, leader: Option<u64>) {
        self.fake_cluster().set_leader(leader);
    }

    fn partition_available(&self, nodes: impl IntoIterator<Item = u64>) {
        self.fake_cluster().partition_available(nodes);
    }

    fn restore_all_nodes(&self) {
        self.fake_cluster().restore_all_nodes();
    }

    fn set_delay_writes(&self, delay_writes: bool) {
        self.fake_cluster().set_delay_writes(delay_writes);
    }

    fn drain_one(&self) -> Option<u64> {
        self.fake_cluster().drain_one()
    }

    #[allow(dead_code)]
    fn drain_all(&self) -> usize {
        self.fake_cluster().drain_all()
    }

    fn queued_write_count(&self) -> usize {
        self.fake_cluster().queued_write_count()
    }

    fn set_client_addr(&self, node_id: u64, addr: SocketAddr) {
        self.fake_cluster().set_client_addr(node_id, addr);
    }

    async fn connect(&self) -> TestClient {
        TestClient::connect(&self.broker).await
    }

    async fn connect_accepted(&self) -> TestClient {
        TestClient::connect_accepted(&self.broker).await
    }

    async fn connect_durable(&self, durable_id: &str, ack_timeout_ms: u64) -> TestClient {
        TestClient::connect_durable(&self.broker, durable_id, ack_timeout_ms).await
    }

    fn advance_ms(&self, millis: u64) {
        self.clock.advance_ms(millis);
    }

    async fn tick_redelivery(&self) {
        self.broker.tick_redelivery_for_test().await.unwrap();
    }

    async fn restart_broker(&mut self) {
        self.broker.shutdown().await.unwrap();
        let mut config = test_config(self._dir.path());
        config.wal_segment_bytes = self.broker.config.wal_segment_bytes;
        self.broker = deterministic_broker(
            config,
            self.clock.clone(),
            self.fake_cluster.clone().map(ClusterRuntime::Fake),
        );
    }
}
struct TestClient {
    stream: Option<BufReader<DuplexStream>>,
    task: Option<JoinHandle<()>>,
}
impl Drop for TestClient {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}
impl TestClient {
    async fn connect(broker: &Morrow) -> Self {
        Self::connect_with(broker, false).await
    }

    async fn connect_with_info(broker: &Morrow) -> (Self, String) {
        Self::connect_with_info_and_path(broker, false).await
    }

    async fn connect_accepted(broker: &Morrow) -> Self {
        Self::connect_with(broker, true).await
    }

    async fn connect_with(broker: &Morrow, accepted_path: bool) -> Self {
        Self::connect_with_info_and_path(broker, accepted_path)
            .await
            .0
    }

    async fn connect_with_info_and_path(broker: &Morrow, accepted_path: bool) -> (Self, String) {
        let (client_stream, server_stream) = tokio::io::duplex(4096);
        let server = broker.clone();
        let task = tokio::spawn(async move {
            if accepted_path {
                server
                    .handle_accepted_for_test(server_stream)
                    .await
                    .unwrap();
            } else {
                server.handle_client_for_test(server_stream).await.unwrap();
            }
        });
        let mut client = Self {
            stream: Some(BufReader::new(client_stream)),
            task: Some(task),
        };
        let info = client.read_frame().await;
        assert!(info.starts_with("INFO "));
        (client, info)
    }

    async fn connect_durable(broker: &Morrow, durable_id: &str, ack_timeout_ms: u64) -> Self {
        let mut client = Self::connect(broker).await;
        client
            .send_durable_connect(durable_id, ack_timeout_ms)
            .await;
        client
    }

    async fn connect_pull(broker: &Morrow, durable_id: &str, ack_timeout_ms: u64) -> Self {
        let mut client = Self::connect(broker).await;
        let payload = serde_json::json!({
            "durable_id": durable_id,
            "verbose": false,
            "ack_timeout_ms": ack_timeout_ms,
            "max_in_flight": 2,
            "protocol_version": 2,
        });
        client.write_line(&format!("CONN {payload}")).await;
        client
    }

    async fn send_durable_connect(&mut self, durable_id: &str, ack_timeout_ms: u64) {
        let payload = serde_json::json!({
            "durable_id": durable_id,
            "verbose": false,
            "ack_timeout_ms": ack_timeout_ms,
            "max_in_flight": 1024,
        });
        self.write_line(&format!("CONN {payload}")).await;
    }

    async fn disconnect(mut self) {
        drop(self.stream.take());
        let Some(task) = self.task.take() else {
            return;
        };
        match tokio::time::timeout(Duration::from_secs(1), task).await {
            Ok(joined) => joined.unwrap(),
            Err(_) => panic!("server task did not finish after client disconnect"),
        }
    }

    async fn write_line(&mut self, line: &str) {
        let stream = self.stream.as_mut().expect("client is disconnected");
        stream.get_mut().write_all(line.as_bytes()).await.unwrap();
        stream.get_mut().write_all(b"\r\n").await.unwrap();
    }

    async fn subscribe(&mut self, subject: &str, sid: &str) {
        self.write_line(&format!("SUB {subject} {sid}")).await;
    }

    async fn subscribe_at(&mut self, subject: &str, sid: &str, start: &str) {
        self.write_line(&format!("SUB {subject} {sid} {start}"))
            .await;
    }

    async fn subscribe_queue(&mut self, subject: &str, queue: &str, sid: &str) {
        self.write_line(&format!("SUB {subject} {queue} {sid}"))
            .await;
    }

    async fn publish(&mut self, subject: &str, payload: &[u8]) {
        self.publish_with_reply(subject, None, payload).await;
    }

    async fn ack(&mut self, ack_subject: &str) {
        let ack = protocol::parse_ack_subject(ack_subject).unwrap();
        self.write_line(&format!(
            "ACK {} {} {}",
            ack.consumer_id, ack.seq, ack.delivery_id
        ))
        .await;
        assert!(
            self.read_frame().await.starts_with("D-OK ACK "),
            "ACK did not receive D-OK"
        );
    }

    async fn publish_with_reply(&mut self, subject: &str, reply_to: Option<&str>, payload: &[u8]) {
        match reply_to {
            Some(reply_to) => {
                self.write_line(&format!("PUB {subject} {reply_to} {}", payload.len()))
                    .await;
            }
            None => {
                self.write_line(&format!("PUB {subject} {}", payload.len()))
                    .await;
            }
        }
        let stream = self.stream.as_mut().expect("client is disconnected");
        stream.get_mut().write_all(payload).await.unwrap();
        stream.get_mut().write_all(b"\r\n").await.unwrap();
    }

    async fn publish_qos(
        &mut self,
        subject: &str,
        payload: &[u8],
        level: protocol::AckLevel,
        msg_id: &str,
    ) {
        let headers = format!(
            "MORROW/1.0\r\nMorrow-QoS: {}\r\nMorrow-Msg-Id: {msg_id}\r\n\r\n",
            level as u8
        );
        self.write_line(&format!(
            "HPUB {subject} {} {}",
            headers.len(),
            headers.len() + payload.len()
        ))
        .await;
        let stream = self.stream.as_mut().expect("client is disconnected");
        stream
            .get_mut()
            .write_all(headers.as_bytes())
            .await
            .unwrap();
        stream.get_mut().write_all(payload).await.unwrap();
        stream.get_mut().write_all(b"\r\n").await.unwrap();
    }

    async fn publish_hpub(&mut self, subject: &str, headers: &[(&str, &str)], payload: &[u8]) {
        let mut block = String::from("MORROW/1.0\r\n");
        for (name, value) in headers {
            block.push_str(&format!("{name}: {value}\r\n"));
        }
        block.push_str("\r\n");
        self.write_line(&format!(
            "HPUB {subject} {} {}",
            block.len(),
            block.len() + payload.len()
        ))
        .await;
        let stream = self.stream.as_mut().expect("client is disconnected");
        stream.get_mut().write_all(block.as_bytes()).await.unwrap();
        stream.get_mut().write_all(payload).await.unwrap();
        stream.get_mut().write_all(b"\r\n").await.unwrap();
    }

    async fn expect_producer_ack(&mut self, msg_id: &str, level: u8, retained: bool, seq: &str) {
        let frame = self.read_frame().await;
        let prefix = format!("P-ACK {msg_id} {level} OK {retained} {seq}");
        assert!(
            frame == format!("{prefix}\r\n") || frame.starts_with(&format!("{prefix} ")),
            "unexpected producer ack {frame:?}"
        );
    }

    async fn ping_roundtrip(&mut self) {
        self.write_line("PING").await;
        self.expect_pong().await;
    }

    async fn expect_pong(&mut self) {
        assert_eq!(self.read_frame().await, "PONG\r\n");
    }

    async fn expect_msg(&mut self) -> String {
        let frame = self.read_frame().await;
        assert!(
            frame.starts_with("DELIVER "),
            "expected DELIVER, got {frame:?}"
        );
        frame
    }

    async fn expect_hmsg(&mut self) -> String {
        let frame = self.read_frame().await;
        assert!(
            frame.starts_with("HDELIVER "),
            "expected HDELIVER, got {frame:?}"
        );
        frame
    }

    async fn expect_err_contains(&mut self, expected: &str) -> String {
        let frame = self.read_frame().await;
        assert!(frame.starts_with("-ERR "), "expected -ERR, got {frame:?}");
        assert!(
            frame.contains(expected),
            "expected error containing {expected:?}, got {frame:?}"
        );
        frame
    }

    async fn expect_no_frame_short(&mut self) {
        match tokio::time::timeout(Duration::from_millis(25), self.read_frame_inner()).await {
            Ok(frame) => panic!("expected no frame, got {frame:?}"),
            Err(_) => {}
        }
    }

    async fn read_frame(&mut self) -> String {
        tokio::time::timeout(Duration::from_secs(1), self.read_frame_inner())
            .await
            .expect("timed out reading frame")
    }

    async fn read_frame_inner(&mut self) -> String {
        let stream = self.stream.as_mut().expect("client is disconnected");
        let mut frame = Vec::new();
        stream.read_until(b'\n', &mut frame).await.unwrap();
        assert!(!frame.is_empty(), "connection closed before frame");
        let line = std::str::from_utf8(&frame).unwrap().to_string();
        let mut parts = line.split_whitespace();
        match parts.next() {
            Some("DELIVER") | Some("DDELIVER") => {
                let tokens = line.split_whitespace().collect::<Vec<_>>();
                let size = tokens.last().unwrap().parse::<usize>().unwrap();
                let mut body = vec![0; size + 2];
                stream.read_exact(&mut body).await.unwrap();
                frame.extend_from_slice(&body);
            }
            Some("HDELIVER") => {
                let tokens = line.split_whitespace().collect::<Vec<_>>();
                let total_size = tokens.last().unwrap().parse::<usize>().unwrap();
                let mut body = vec![0; total_size + 2];
                stream.read_exact(&mut body).await.unwrap();
                frame.extend_from_slice(&body);
            }
            _ => {}
        }
        String::from_utf8(frame).unwrap()
    }
}
fn test_config(dir: &Path) -> Config {
    Config {
        listen: "127.0.0.1:0".parse().unwrap(),
        http_listen: None,
        admin_token: Some("test-admin-token".to_string()),
        admin_tls: None,
        quotas: Default::default(),
        wal_dir: dir.to_path_buf(),
        encryption_key_dir: None,
        encryption_active_key_version: 1,
        wal_segment_bytes: crate::wal::DEFAULT_WAL_SEGMENT_BYTES,
        fsync_interval_ms: 1,
        max_payload: 1024,
        max_control_line: 8192,
        max_ack_timeout_ms: crate::config::DEFAULT_MAX_ACK_TIMEOUT_MS,
        max_in_flight: crate::config::DEFAULT_MAX_IN_FLIGHT,
        max_fetch_messages: crate::config::DEFAULT_MAX_FETCH_MESSAGES,
        max_fetch_bytes: crate::config::DEFAULT_MAX_FETCH_BYTES,
        max_encoded_batch_bytes: crate::config::DEFAULT_MAX_ENCODED_BATCH_BYTES,
        verbose: false,
        tls: None,
        auth: Default::default(),
        cluster: None,
        streams: test_streams(),
    }
}
fn test_outbound_queue(
    broker: &Morrow,
    capacity: usize,
) -> (OutboundQueue, mpsc::Receiver<OutboundFrame>) {
    let (sender, receiver) = mpsc::channel(capacity);
    (
        OutboundQueue::new(
            sender,
            broker.config.quotas.max_outbound_bytes_per_connection,
            broker.quotas.clone(),
        ),
        receiver,
    )
}
fn test_streams() -> crate::stream::StreamCatalog {
    crate::stream::StreamCatalog::new(
        [
            ("orders", "orders/**"),
            ("service", "service/**"),
            ("topic", "topic"),
        ]
        .into_iter()
        .map(|(name, subject)| crate::stream::StreamDefinition {
            name: crate::stream::StreamId::new(name).unwrap(),
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
fn deterministic_broker(
    config: Config,
    clock: Arc<ManualClock>,
    initial_cluster: Option<ClusterRuntime>,
) -> Morrow {
    Morrow::open_with_hooks(
        config,
        BrokerHooks {
            clock,
            start_redelivery_loop: false,
            durable_publish_flush_mode: DurablePublishFlushMode::FlushImmediately,
            middleware: MiddlewareRuntime::default(),
            initial_cluster,
        },
    )
    .unwrap()
}
fn fake_cluster_config(dir: &Path, node_count: u64, local_node_id: u64) -> ClusterConfig {
    ClusterConfig {
        enabled: true,
        node_id: local_node_id,
        auth_token: "test-cluster-token".to_string(),
        raft_listen: SocketAddr::from(([127, 0, 0, 1], 20_000 + local_node_id as u16)),
        raft_tls: None,
        allow_insecure_internal_transports: true,
        route_listen: Some(SocketAddr::from((
            [127, 0, 0, 1],
            30_000 + local_node_id as u16,
        ))),
        route_advertise: Some(
            SocketAddr::from(([127, 0, 0, 1], 30_000 + local_node_id as u16)).to_string(),
        ),
        route_tls: None,
        routes: Vec::new(),
        route_reconnect_ms: 50,
        raft_dir: dir.join("raft"),
        bootstrap: local_node_id == 1,
        nodes: (1..=node_count)
            .map(|node_id| ClusterNodeConfig {
                node_id,
                raft_addr: SocketAddr::from(([127, 0, 0, 1], 20_000 + node_id as u16)).to_string(),
                client_addr: SocketAddr::from(([127, 0, 0, 1], 10_000 + node_id as u16))
                    .to_string(),
                route_addr: Some(
                    SocketAddr::from(([127, 0, 0, 1], 30_000 + node_id as u16)).to_string(),
                ),
                tls_server_name: None,
                tls_cert_files: Vec::new(),
            })
            .collect(),
        election_timeout_min_ms: 150,
        election_timeout_max_ms: 300,
        heartbeat_interval_ms: 50,
        snapshot_threshold: 100,
    }
}
fn ack_subject(frame: &str) -> String {
    frame.split_whitespace().nth(3).unwrap().to_string()
}
async fn http_request(broker: &Morrow, path: &str) -> String {
    http_request_with_auth(broker, path, Some("test-admin-token")).await
}
async fn http_request_with_auth(broker: &Morrow, path: &str, token: Option<&str>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let broker = broker.clone();
    let server_task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        broker.handle_http_status(stream).await.unwrap();
    });
    let mut client = TcpStream::connect(addr).await.unwrap();
    let auth = token
        .map(|token| format!("authorization: Bearer {token}\r\n"))
        .unwrap_or_default();
    client
        .write_all(format!("GET {path} HTTP/1.1\r\nhost: localhost\r\n{auth}\r\n").as_bytes())
        .await
        .unwrap();
    let mut response = Vec::new();
    client.read_to_end(&mut response).await.unwrap();
    server_task.await.unwrap();
    String::from_utf8(response).unwrap()
}

mod auth_tests;
mod cluster_admin_tests;
mod cluster_delta_tests;
mod compaction_tests;
mod cursor_tests;
mod delivery_index_tests;
mod flow_control_tests;
mod middleware_tests;
mod pull_tests;
mod qos_tests;
mod quota_tests;
mod retention_limit_tests;
mod route_interest_tests;
mod semantic_tests;
mod state_sharding_tests;
mod stream_retention_tests;
