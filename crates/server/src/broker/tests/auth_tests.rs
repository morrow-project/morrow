use super::*;
use crate::config::{AuthClientConfig, AuthConfig, AuthPermissions};
use crate::middleware::{
    Capability, FailurePolicy, MiddlewareManifest, MiddlewareStage, ResourceBudget,
};
use ed25519_dalek::{Signer, SigningKey};

#[tokio::test]
async fn auth_rejects_unknown_missing_bad_mismatch_and_repeat_connect() {
    let scenario = auth_scenario(vec![auth_client("client1", [7; 32], None, None)]);

    let (mut unknown, unknown_info) = TestClient::connect_with_info(scenario.broker()).await;
    unknown
        .write_line(&connect_payload(&unknown_info, "client2", [8; 32], None))
        .await;
    unknown.expect_err_contains("unknown client_id").await;

    let (mut missing, _) = TestClient::connect_with_info(scenario.broker()).await;
    missing.write_line("CONN {}").await;
    missing.expect_err_contains("client_id and signature").await;

    let (mut bad, bad_info) = TestClient::connect_with_info(scenario.broker()).await;
    bad.write_line(&connect_payload(&bad_info, "client1", [9; 32], None))
        .await;
    bad.expect_err_contains("invalid public key signature")
        .await;

    let (mut mismatch, mismatch_info) = TestClient::connect_with_info(scenario.broker()).await;
    mismatch
        .write_line(&connect_payload(
            &mismatch_info,
            "client1",
            [7; 32],
            Some("other"),
        ))
        .await;
    mismatch
        .expect_err_contains("durable_id must match authenticated client_id")
        .await;

    let (mut repeat, repeat_info) = TestClient::connect_with_info(scenario.broker()).await;
    repeat
        .write_line(&connect_payload(&repeat_info, "client1", [7; 32], None))
        .await;
    repeat
        .write_line(&connect_payload(&repeat_info, "client1", [7; 32], None))
        .await;
    repeat.expect_err_contains("CONN already received").await;
}

#[tokio::test]
async fn failed_auth_leaves_no_durable_or_subscription_state() {
    let scenario = auth_scenario(vec![auth_client("client1", [7; 32], None, None)]);
    let (mut client, info) = TestClient::connect_with_info(scenario.broker()).await;

    client
        .write_line(&connect_payload(&info, "client1", [9; 32], None))
        .await;
    client
        .expect_err_contains("invalid public key signature")
        .await;
    client.subscribe("orders/*", "sid1").await;
    client.expect_err_contains("authentication required").await;
    client.subscribe("_MORROW/INBOX/client1/1", "inbox1").await;
    client.expect_err_contains("authentication required").await;

    let connections = scenario.broker().connections.lock().await;
    let client = connections.clients.get(&1).unwrap();
    assert!(!client.authenticated);
    assert!(client.durable_id.is_none());
    drop(connections);
    let inner = scenario.broker().inner.lock().await;
    assert!(inner.consumers.is_empty());
    drop(inner);
    assert!(
        scenario
            .broker()
            .transient
            .lock()
            .await
            .subscriptions
            .is_empty()
    );
}

#[tokio::test]
async fn omitted_permissions_allow_authenticated_publish_and_subscribe() {
    let scenario = auth_scenario(vec![
        auth_client("subscriber1", [7; 32], None, None),
        auth_client("publisher1", [8; 32], None, None),
    ]);
    let mut subscriber = connect_authenticated(&scenario, "subscriber1", [7; 32]).await;
    let mut publisher = connect_authenticated(&scenario, "publisher1", [8; 32]).await;

    subscriber.subscribe("orders/*", "sid1").await;
    subscriber.ping_roundtrip().await;
    publisher.publish("orders/created", b"hello").await;

    let frame = subscriber.expect_msg().await;
    assert!(frame.starts_with("DELIVER orders/created sid1 _MORROW/ACK/durable-subscriber1-sid1/"));
}

#[tokio::test]
async fn dynamic_policy_revocation_fences_an_authenticated_client_without_restart() {
    let scenario = auth_scenario(vec![auth_client("publisher1", [8; 32], None, None)]);
    let mut publisher = connect_authenticated(&scenario, "publisher1", [8; 32]).await;
    let policy = scenario.broker().policy_store();
    let scope = crate::tenancy::ResourceScope {
        tenant: crate::tenancy::TenantId::new("default").unwrap(),
        namespace: crate::tenancy::NamespaceId::new("default").unwrap(),
    };
    policy.revoke("publisher1", &scope, None);
    publisher.publish("orders/created", b"revoked").await;
    publisher
        .expect_err_contains("tenant permission denied")
        .await;
}

#[tokio::test]
async fn publish_and_subscribe_patterns_authorize_matching_subjects() {
    let scenario = auth_scenario(vec![
        auth_client("subscriber1", [7; 32], None, Some(vec!["orders/**"])),
        auth_client("publisher1", [8; 32], Some(vec!["orders/**"]), None),
    ]);
    let mut subscriber = connect_authenticated(&scenario, "subscriber1", [7; 32]).await;
    let mut publisher = connect_authenticated(&scenario, "publisher1", [8; 32]).await;

    subscriber.subscribe("events/created", "bad").await;
    subscriber
        .expect_err_contains("subscribe not authorized")
        .await;
    subscriber.subscribe("orders/**", "sid1").await;
    subscriber.ping_roundtrip().await;

    publisher.publish("events/created", b"blocked").await;
    publisher
        .expect_err_contains("publish not authorized")
        .await;
    publisher.publish("orders/us/created", b"hello").await;

    let frame = subscriber.expect_msg().await;
    assert!(frame.starts_with("DELIVER orders/us/created sid1 "));
}

#[tokio::test]
async fn ack_and_owned_inbox_subjects_remain_allowed_under_restrictive_permissions() {
    let scenario = auth_scenario(vec![
        auth_client(
            "subscriber1",
            [7; 32],
            Some(vec!["none/*"]),
            Some(vec!["orders/*"]),
        ),
        auth_client("publisher1", [8; 32], Some(vec!["orders/*"]), None),
        auth_client(
            "requester1",
            [9; 32],
            Some(vec!["service/*"]),
            Some(vec!["none/*"]),
        ),
        auth_client(
            "responder1",
            [10; 32],
            Some(vec!["none/*"]),
            Some(vec!["service/*"]),
        ),
    ]);
    let mut subscriber = connect_authenticated(&scenario, "subscriber1", [7; 32]).await;
    let mut publisher = connect_authenticated(&scenario, "publisher1", [8; 32]).await;

    subscriber.subscribe("orders/*", "sid1").await;
    subscriber.ping_roundtrip().await;
    publisher.publish("orders/created", b"hello").await;
    let delivery = subscriber.expect_msg().await;
    subscriber.ack(&ack_subject(&delivery)).await;
    subscriber.ping_roundtrip().await;

    let mut requester = connect_authenticated(&scenario, "requester1", [9; 32]).await;
    let mut responder = connect_authenticated(&scenario, "responder1", [10; 32]).await;
    requester
        .subscribe("_MORROW/INBOX/requester1/1", "reply")
        .await;
    responder.subscribe("service/echo", "svc").await;
    responder.ping_roundtrip().await;

    requester
        .publish_with_reply("service/echo", Some("_MORROW/INBOX/requester1/1"), b"hello")
        .await;
    let request = responder.expect_hmsg().await;
    assert!(request.starts_with("HDELIVER service/echo svc _MORROW/INBOX/requester1/1 "));
    responder
        .publish("_MORROW/INBOX/requester1/1", b"world")
        .await;
    responder
        .expect_err_contains("inbox publish not authorized")
        .await;
}

#[tokio::test]
async fn authenticated_clients_cannot_ack_other_consumers_delivery() {
    let scenario = auth_scenario(vec![
        auth_client("subscriber1", [7; 32], None, Some(vec!["orders/*"])),
        auth_client("publisher1", [8; 32], Some(vec!["orders/*"]), None),
    ]);
    let mut subscriber = connect_authenticated(&scenario, "subscriber1", [7; 32]).await;
    let mut publisher = connect_authenticated(&scenario, "publisher1", [8; 32]).await;

    subscriber.subscribe("orders/*", "sid1").await;
    subscriber.ping_roundtrip().await;
    publisher.publish("orders/created", b"hello").await;
    let delivery = subscriber.expect_msg().await;
    let ack = protocol::parse_ack_subject(&ack_subject(&delivery)).unwrap();
    publisher
        .write_line(&format!(
            "ACK {} {} {}",
            ack.consumer_id, ack.seq, ack.delivery_id
        ))
        .await;
    publisher.expect_err_contains("ack not authorized").await;

    let inner = scenario.broker().inner.lock().await;
    let consumer = inner.consumers.get("durable-subscriber1-sid1").unwrap();
    assert!(consumer.in_flight.contains_key(&1));
}

#[tokio::test]
async fn authenticated_clients_cannot_use_other_clients_inbox_prefix() {
    let scenario = auth_scenario(vec![
        auth_client("requester1", [9; 32], Some(vec!["service/*"]), None),
        auth_client(
            "responder1",
            [10; 32],
            Some(vec!["none/*"]),
            Some(vec!["service/*"]),
        ),
    ]);
    let mut requester = connect_authenticated(&scenario, "requester1", [9; 32]).await;
    let mut responder = connect_authenticated(&scenario, "responder1", [10; 32]).await;

    requester
        .subscribe("_MORROW/INBOX/responder1/1", "bad")
        .await;
    requester
        .expect_err_contains("inbox subscribe not authorized")
        .await;
    responder
        .publish("_MORROW/INBOX/requester1/1", b"world")
        .await;
    responder
        .expect_err_contains("inbox publish not authorized")
        .await;
}

#[tokio::test]
async fn unauthorized_cluster_publish_does_not_propose_write() {
    let scenario = auth_fake_cluster_scenario(vec![
        auth_client("subscriber1", [7; 32], None, Some(vec!["orders/*"])),
        auth_client("publisher1", [8; 32], Some(vec!["events/*"]), None),
    ]);
    let mut subscriber = connect_authenticated(&scenario, "subscriber1", [7; 32]).await;
    let mut publisher = connect_authenticated(&scenario, "publisher1", [8; 32]).await;

    subscriber.subscribe("orders/*", "sid1").await;
    subscriber.ping_roundtrip().await;
    assert_eq!(scenario.fake_cluster().write_count(), 1);

    publisher.publish("orders/created", b"blocked").await;
    publisher
        .expect_err_contains("publish not authorized")
        .await;
    assert_eq!(scenario.fake_cluster().write_count(), 1);
}

#[tokio::test]
async fn denied_original_subject_does_not_execute_ingress_middleware() {
    let scenario = auth_scenario(vec![auth_client(
        "publisher1",
        [8; 32],
        Some(vec!["orders/*"]),
        None,
    )]);
    install_middleware(
        &scenario,
        middleware_manifest("trap", "events/denied", []),
        "(module (func (export \"process\") (param i32) (result i32) unreachable))",
    );
    let mut publisher = connect_authenticated(&scenario, "publisher1", [8; 32]).await;

    publisher.publish("events/denied", b"secret").await;
    publisher
        .expect_err_contains("publish not authorized")
        .await;
    assert!(scenario.broker().inner.lock().await.messages.is_empty());
}

#[tokio::test]
async fn rewritten_and_emitted_subjects_retain_publisher_authority() {
    let scenario = auth_scenario(vec![auth_client(
        "publisher1",
        [8; 32],
        Some(vec!["orders/*"]),
        None,
    )]);
    scenario
        .broker()
        .middleware_runtime()
        .install(vec![
            (
                middleware_manifest(
                    "rewrite-denied",
                    "orders/rewrite",
                    [Capability::WriteSubject],
                ),
                wat::parse_str(rewrite_module("admin.denied")).unwrap(),
            ),
            (
                middleware_manifest(
                    "rewrite-reserved",
                    "orders/reserved",
                    [Capability::WriteSubject],
                ),
                wat::parse_str(rewrite_module("_MORROW/ADMIN")).unwrap(),
            ),
            (
                middleware_manifest(
                    "emit-denied",
                    "orders/emit",
                    [Capability::SecondaryPublish],
                ),
                wat::parse_str(
                    "(module
                        (import \"broker\" \"emit\" (func $emit (param i32 i32 i32 i32) (result i32)))
                        (memory (export \"memory\") 1)
                        (data (i32.const 0) \"admin.emittedx\")
                        (func (export \"process\") (param i32) (result i32)
                          i32.const 0 i32.const 13 i32.const 13 i32.const 1 call $emit drop
                          i32.const 0))",
                )
                .unwrap(),
            ),
        ])
        .unwrap();
    let mut publisher = connect_authenticated(&scenario, "publisher1", [8; 32]).await;

    publisher.publish("orders/rewrite", b"secret").await;
    publisher
        .expect_err_contains("publish not authorized")
        .await;
    publisher.publish("orders/reserved", b"secret").await;
    publisher
        .expect_err_contains("middleware produced reserved broker subject")
        .await;
    publisher.publish("orders/emit", b"secret").await;
    publisher
        .expect_err_contains("publish not authorized")
        .await;
    assert!(scenario.broker().inner.lock().await.messages.is_empty());
}

fn install_middleware(scenario: &Scenario, manifest: MiddlewareManifest, module: &str) {
    scenario
        .broker()
        .middleware_runtime()
        .install(vec![(manifest, wat::parse_str(module).unwrap())])
        .unwrap();
}

fn middleware_manifest(
    name: &str,
    subject: &str,
    capabilities: impl IntoIterator<Item = Capability>,
) -> MiddlewareManifest {
    MiddlewareManifest {
        name: name.to_string(),
        subject: subject.to_string(),
        stage: MiddlewareStage::Ingress,
        capabilities: capabilities.into_iter().collect(),
        failure_policy: FailurePolicy::FailClosed,
        budget: ResourceBudget::default(),
        named_kv: BTreeSet::new(),
        secrets: BTreeSet::new(),
        http_allow_lists: BTreeSet::new(),
    }
}

fn rewrite_module(subject: &str) -> String {
    format!(
        "(module
            (import \"broker\" \"set-field\" (func $set (param i32 i32 i32) (result i32)))
            (memory (export \"memory\") 1)
            (data (i32.const 0) \"{subject}\")
            (func (export \"process\") (param i32) (result i32)
              i32.const 0 i32.const 0 i32.const {} call $set drop i32.const 0))",
        subject.len()
    )
}

fn auth_scenario(clients: Vec<(String, AuthClientConfig)>) -> Scenario {
    let dir = TempDir::new().unwrap();
    let clock = Arc::new(ManualClock::new(1_000));
    let mut config = test_config(dir.path());
    config.auth = auth_config(clients);
    let broker = deterministic_broker(config, clock.clone(), None);
    Scenario {
        _dir: dir,
        clock,
        broker,
        fake_cluster: None,
    }
}

fn auth_fake_cluster_scenario(clients: Vec<(String, AuthClientConfig)>) -> Scenario {
    let dir = TempDir::new().unwrap();
    let clock = Arc::new(ManualClock::new(1_000));
    let fake_cluster = FakeClusterRuntime::new(3, 1, Some(1));
    let mut config = test_config(dir.path());
    config.auth = auth_config(clients);
    let broker = deterministic_broker(
        config,
        clock.clone(),
        Some(ClusterRuntime::Fake(fake_cluster.clone())),
    );
    Scenario {
        _dir: dir,
        clock,
        broker,
        fake_cluster: Some(fake_cluster),
    }
}

fn auth_config(clients: Vec<(String, AuthClientConfig)>) -> AuthConfig {
    AuthConfig {
        enabled: true,
        clients: clients.into_iter().collect(),
    }
}

fn auth_client(
    client_id: &str,
    seed: [u8; 32],
    publish: Option<Vec<&str>>,
    subscribe: Option<Vec<&str>>,
) -> (String, AuthClientConfig) {
    let signing_key = SigningKey::from_bytes(&seed);
    let permissions = match (publish, subscribe) {
        (None, None) => None,
        (publish, subscribe) => Some(AuthPermissions {
            publish: publish.map(patterns),
            subscribe: subscribe.map(patterns),
        }),
    };
    (
        client_id.to_string(),
        AuthClientConfig {
            public_key: hex(signing_key.verifying_key().as_bytes()),
            permissions,
        },
    )
}

async fn connect_authenticated(scenario: &Scenario, client_id: &str, seed: [u8; 32]) -> TestClient {
    let (mut client, info) = TestClient::connect_with_info(scenario.broker()).await;
    client
        .write_line(&connect_payload(&info, client_id, seed, None))
        .await;
    client
}

fn connect_payload(
    info: &str,
    client_id: &str,
    seed: [u8; 32],
    durable_id: Option<&str>,
) -> String {
    let nonce = info_nonce(info);
    let signing_key = SigningKey::from_bytes(&seed);
    let signature = hex(&signing_key.sign(nonce.as_bytes()).to_bytes());
    let mut payload = serde_json::json!({
        "client_id": client_id,
        "signature": signature,
        "verbose": false,
        "ack_timeout_ms": 25,
        "max_in_flight": 1024,
    });
    if let Some(durable_id) = durable_id {
        payload["durable_id"] = serde_json::Value::String(durable_id.to_string());
    }
    format!("CONN {payload}")
}

fn info_nonce(info: &str) -> String {
    let json = info.trim_start_matches("INFO ").trim();
    let value: serde_json::Value = serde_json::from_str(json).unwrap();
    value["nonce"].as_str().unwrap().to_string()
}

fn patterns(values: Vec<&str>) -> Vec<String> {
    values.into_iter().map(str::to_string).collect()
}

fn hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}
