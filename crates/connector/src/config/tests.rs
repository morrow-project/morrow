use super::*;
use client::ClientAuth;
use server::config::{AuthClientConfig, AuthConfig, TlsConfig};
use std::collections::HashMap;
use tempfile::TempDir;
use tokio::net::TcpListener;

#[test]
fn config_control_record_allowlist_never_serializes_secret_contents_or_unknown_fields() {
    let dir = TempDir::new().unwrap();
    let secret = "11".repeat(32);
    let config: ConnectorConfig = serde_json::from_value(serde_json::json!({
        "broker": "127.0.0.1:4222",
        "durable_id": "orders-sink",
        "consumer": "orders",
        "filter_subject": "orders/**",
        "generation": 7,
        "checkpoint_file": dir.path().join("checkpoint.json"),
        "tls": {
            "server_name": "localhost",
            "ca_cert_file": "ca.pem"
        },
        "auth": {
            "client_id": "connector-orders",
            "private_key_seed_file": dir.path().join("connector.seed")
        },
        "target": "object_store",
        "directory": dir.path().join("objects"),
        "future_secret": secret
    }))
    .unwrap();

    let descriptor = String::from_utf8(config.descriptor_json().unwrap()).unwrap();
    assert!(!descriptor.contains(&secret));
    assert!(!descriptor.contains("future_secret"));
    assert!(descriptor.contains("connector-orders"));
    assert!(descriptor.contains("objects"));
    assert!(descriptor.contains("\"version\":1"));
}

#[test]
fn secret_redactor_removes_every_configured_secret() {
    let redactor = SecretRedactor {
        secrets: vec!["top-secret".to_string(), "other-secret".to_string()],
    };
    assert_eq!(
        redactor.redact("top-secret then other-secret"),
        "[REDACTED] then [REDACTED]"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn group_readable_authentication_secret_is_rejected() {
    use std::os::unix::fs::PermissionsExt;

    let dir = TempDir::new().unwrap();
    let seed_file = dir.path().join("connector.seed");
    std::fs::write(&seed_file, "11".repeat(32)).unwrap();
    std::fs::set_permissions(&seed_file, std::fs::Permissions::from_mode(0o640)).unwrap();
    let config = connector_config(dir.path(), seed_file, "127.0.0.1:1".parse().unwrap());

    let err = match config.connect_broker().await {
        Ok(_) => panic!("group-readable secret unexpectedly accepted"),
        Err(err) => err,
    };
    assert!(err.contains("must not be accessible by group or others"));
}

#[tokio::test]
async fn authenticated_tls_connector_accepts_valid_and_rejects_invalid_credentials() {
    secure_secret_permissions_are_used_for_tls_authentication().await;
}

async fn secure_secret_permissions_are_used_for_tls_authentication() {
    let dir = TempDir::new().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let valid_seed = [7_u8; 32];
    let auth = ClientAuth::from_seed("connector-orders", valid_seed);
    let broker = server::Morrow::open(server::Config {
        production: false,
        allow_insecure_development: false,
        listen: addr,
        websocket: None,
        http_listen: None,
        admin_token: None,
        admin_tls: None,
        quotas: Default::default(),
        tenant_quotas: Default::default(),
        wal_dir: dir.path().join("wal"),
        encryption_key_dir: None,
        encryption_active_key_version: 1,
        wal_segment_bytes: server::wal::DEFAULT_WAL_SEGMENT_BYTES,
        fsync_interval_ms: 1,
        max_payload: 1024 * 1024,
        max_control_line: 8192,
        max_ack_timeout_ms: server::config::DEFAULT_MAX_ACK_TIMEOUT_MS,
        max_in_flight: server::config::DEFAULT_MAX_IN_FLIGHT,
        max_fetch_messages: server::config::DEFAULT_MAX_FETCH_MESSAGES,
        max_fetch_bytes: server::config::DEFAULT_MAX_FETCH_BYTES,
        max_encoded_batch_bytes: server::config::DEFAULT_MAX_ENCODED_BATCH_BYTES,
        audit_max_records: 10_000,
        audit_segment_bytes: 16 * 1_048_576,
        verbose: false,
        tls: Some(TlsConfig {
            cert_file: fixture("morrow-cert.pem"),
            key_file: fixture("morrow-key.pem"),
            handshake_timeout_ms: 500,
        }),
        auth: AuthConfig {
            enabled: true,
            clients: HashMap::from([(
                "connector-orders".to_string(),
                AuthClientConfig {
                    public_key: auth.public_key_hex(),
                    permissions: None,
                    tenant: "default".to_string(),
                    namespace: "default".to_string(),
                    expires_at_ms: None,
                    external_subject: None,
                },
            )]),
        },
        cluster: None,
        streams: server::stream::StreamCatalog::new(Vec::new()).unwrap(),
        views: Default::default(),
    })
    .unwrap();
    let server = broker.clone();
    let task = tokio::spawn(async move { server.serve_listener(listener).await });

    let valid_file = secret_file(&dir, "valid.seed", &hex(&valid_seed));
    let valid = connector_config(dir.path(), valid_file, addr);
    let _client = valid.connect_broker().await.unwrap();

    let invalid_file = secret_file(&dir, "invalid.seed", &hex(&[9_u8; 32]));
    let invalid = connector_config(dir.path(), invalid_file, addr);
    let err = match invalid.connect_broker().await {
        Ok(_) => panic!("invalid connector credentials unexpectedly accepted"),
        Err(err) => err,
    };
    assert!(err.contains("invalid public key signature"));
    assert!(!err.contains(&hex(&[9_u8; 32])));

    broker.shutdown().await.unwrap();
    task.abort();
}

fn connector_config(
    root: &std::path::Path,
    seed_file: PathBuf,
    broker: SocketAddr,
) -> ConnectorConfig {
    ConnectorConfig {
        broker,
        durable_id: "connector-orders".to_string(),
        consumer: "orders".to_string(),
        filter_subject: "orders/**".to_string(),
        generation: 1,
        checkpoint_file: root.join("checkpoint.json"),
        tls: ConnectorTlsConfig {
            server_name: "localhost".to_string(),
            ca_cert_file: fixture("ca-cert.pem"),
        },
        auth: ConnectorAuthConfig {
            client_id: "connector-orders".to_string(),
            private_key_seed_file: seed_file,
        },
        target: ConnectorTarget::ObjectStore {
            directory: root.join("objects"),
        },
    }
}

fn secret_file(dir: &TempDir, name: &str, value: &str) -> PathBuf {
    let path = dir.path().join(name);
    std::fs::write(&path, value).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    path
}

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../integration/tests/fixtures")
        .join(name)
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
