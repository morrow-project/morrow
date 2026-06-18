use std::{
    fs,
    net::SocketAddr,
    path::{Path, PathBuf},
    process::{Command, Output, Stdio},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use client::{Client, ClientAuth};
use server::{
    Broker, Config,
    config::{AuthClientConfig, AuthConfig, TlsConfig},
};
use tokio::net::TcpListener;

const TLS_SERVER_NAME: &str = "localhost";
static CLI_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[tokio::test]
async fn cli_ping_against_server() {
    let _guard = CLI_TEST_LOCK.lock().await;
    let harness = Harness::start().await;
    let config = ClientConfigFile::new(&harness, None, None);

    let output = run_cli(["--config", config.path_str(), "ping"]).await;

    assert!(output.status.success(), "{}", stderr(&output));
    assert_eq!(stdout(&output), "PONG\n");
    harness.shutdown().await;
}

#[tokio::test]
async fn cli_pub_and_sub_against_server() {
    let _guard = CLI_TEST_LOCK.lock().await;
    let harness = Harness::start().await;
    let config = ClientConfigFile::new(&harness, None, None);
    let sub = Command::new(cli_bin())
        .args([
            "--config",
            config.path_str(),
            "sub",
            "orders.*",
            "--ack",
            "--max-messages",
            "1",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    let pub_output = run_cli([
        "--config",
        config.path_str(),
        "pub",
        "orders.created",
        "hello",
    ])
    .await;
    assert!(pub_output.status.success(), "{}", stderr(&pub_output));

    let sub_output = wait_output(sub, Duration::from_secs(3)).await;
    assert!(sub_output.status.success(), "{}", stderr(&sub_output));
    assert_eq!(stdout(&sub_output), "orders.created sid1 hello\n");
    harness.shutdown().await;
}

#[tokio::test]
async fn cli_request_against_client_responder() {
    let _guard = CLI_TEST_LOCK.lock().await;
    let harness = Harness::start().await;
    let config = ClientConfigFile::new(&harness, None, None);
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
        responder.respond(&message, b"world").await.unwrap();
        responder
            .ack(message.ack_subject.as_deref().unwrap())
            .await
            .unwrap();
    });

    let output = run_cli([
        "--config",
        config.path_str(),
        "request",
        "service.echo",
        "hello",
        "--timeout-ms",
        "3000",
    ])
    .await;

    assert!(output.status.success(), "{}", stderr(&output));
    assert_eq!(stdout(&output), "world\n");
    responder_task.await.unwrap();
    harness.shutdown().await;
}

#[tokio::test]
async fn cli_ping_with_auth() {
    let _guard = CLI_TEST_LOCK.lock().await;
    let auth = ClientAuth::from_seed("client1", [7; 32]);
    let harness = Harness::start_with_auth(&[&auth]).await;
    let config = ClientConfigFile::new(&harness, Some(&auth), None);

    let output = run_cli(["--config", config.path_str(), "ping"]).await;

    assert!(output.status.success(), "{}", stderr(&output));
    assert_eq!(stdout(&output), "PONG\n");
    harness.shutdown().await;
}

#[tokio::test]
async fn cli_ping_with_tls() {
    let _guard = CLI_TEST_LOCK.lock().await;
    let harness = Harness::start_tls().await;
    let config = ClientConfigFile::new(&harness, None, Some(tls_ca_cert_file()));

    let output = run_cli(["--config", config.path_str(), "ping"]).await;

    assert!(output.status.success(), "{}", stderr(&output));
    assert_eq!(stdout(&output), "PONG\n");
    harness.shutdown().await;
}

#[tokio::test]
async fn cli_ping_with_tls_and_auth() {
    let _guard = CLI_TEST_LOCK.lock().await;
    let auth = ClientAuth::from_seed("client1", [7; 32]);
    let harness = Harness::start_tls_with_auth(&[&auth]).await;
    let config = ClientConfigFile::new(&harness, Some(&auth), Some(tls_ca_cert_file()));

    let output = run_cli(["--config", config.path_str(), "ping"]).await;

    assert!(output.status.success(), "{}", stderr(&output));
    assert_eq!(stdout(&output), "PONG\n");
    harness.shutdown().await;
}

struct TestDir(PathBuf);

impl TestDir {
    fn new(prefix: &str) -> Self {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("{prefix}-{unique}"));
        fs::create_dir_all(&path).unwrap();
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

struct ClientConfigFile {
    _dir: TestDir,
    path: PathBuf,
}

impl ClientConfigFile {
    fn new(harness: &Harness, auth: Option<&ClientAuth>, ca_cert_file: Option<PathBuf>) -> Self {
        let dir = TestDir::new("broker-cli-config");
        let path = dir.path().join("client.json");
        let tls_json = match ca_cert_file {
            Some(path) => format!(
                r#"{{
                    "enabled": true,
                    "server_name": "{TLS_SERVER_NAME}",
                    "ca_cert_file": "{}"
                }}"#,
                path.display()
            ),
            None => r#"{"enabled": false}"#.to_string(),
        };
        let auth_json = match auth {
            Some(auth) => format!(
                r#"{{
                    "enabled": true,
                    "client_id": "{}",
                    "private_key_seed_hex": "{}"
                }}"#,
                auth.client_id(),
                seed_hex([7; 32])
            ),
            None => r#"{"enabled": false}"#.to_string(),
        };
        let durable_id = auth
            .map(|auth| auth.client_id().to_string())
            .unwrap_or_else(|| "client1".to_string());
        fs::write(
            &path,
            format!(
                r#"{{
                    "server": "{}",
                    "max_payload": {},
                    "tls": {tls_json},
                    "auth": {auth_json},
                    "connect": {{
                        "durable_id": "{durable_id}",
                        "verbose": false,
                        "ack_timeout_ms": 5000,
                        "max_in_flight": 16
                    }}
                }}"#,
                harness.addr, harness.max_payload
            ),
        )
        .unwrap();
        Self { _dir: dir, path }
    }

    fn path_str(&self) -> &str {
        self.path.to_str().unwrap()
    }
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

    async fn start_with_config(auth: AuthConfig, tls: Option<TlsConfig>) -> Self {
        let wal_dir = TestDir::new("broker-cli-wal");
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let max_payload = 1024;
        let config = Config {
            listen: addr,
            http_listen: None,
            admin_token: None,
            wal_dir: wal_dir.path().to_path_buf(),
            fsync_interval_ms: 1,
            max_payload,
            max_control_line: 8192,
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

async fn run_cli<const N: usize>(args: [&str; N]) -> Output {
    let args = args.map(str::to_string);
    tokio::task::spawn_blocking(move || Command::new(cli_bin()).args(args).output().unwrap())
        .await
        .unwrap()
}

async fn wait_output(mut child: std::process::Child, timeout: Duration) -> Output {
    tokio::task::spawn_blocking(move || {
        let start = Instant::now();
        loop {
            if child.try_wait().unwrap().is_some() {
                return child.wait_with_output().unwrap();
            }
            if start.elapsed() > timeout {
                let _ = child.kill();
                return child.wait_with_output().unwrap();
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    })
    .await
    .unwrap()
}

fn cli_bin() -> &'static str {
    env!("CARGO_BIN_EXE_broker-cli")
}

fn tls_config() -> TlsConfig {
    TlsConfig {
        cert_file: tls_cert_file(),
        key_file: tls_key_file(),
        handshake_timeout_ms: 100,
    }
}

fn tls_cert_file() -> PathBuf {
    workspace_root().join("crates/integration/tests/fixtures/server-cert.pem")
}

fn tls_key_file() -> PathBuf {
    workspace_root().join("crates/integration/tests/fixtures/server-key.pem")
}

fn tls_ca_cert_file() -> PathBuf {
    workspace_root().join("crates/integration/tests/fixtures/ca-cert.pem")
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf()
}

fn seed_hex(seed: [u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(seed.len() * 2);
    for byte in seed {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}
