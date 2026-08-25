use super::*;

#[test]
fn parses_client_config_defaults() {
    let config = CliConfig::from_json(&serde_json::json!({})).unwrap();
    assert_eq!(config.server, DEFAULT_SERVER.parse().unwrap());
    assert_eq!(config.max_payload, DEFAULT_MAX_PAYLOAD);
    assert!(config.tls.is_none());
    assert!(config.auth.is_none());
    assert!(!config.connect.verbose);
    assert_eq!(config.connect.ack_timeout_ms, DEFAULT_ACK_TIMEOUT_MS);
    assert_eq!(config.connect.max_in_flight, DEFAULT_MAX_IN_FLIGHT);
}

#[test]
fn uses_defaults_when_implicit_config_is_missing() {
    let path =
        std::env::temp_dir().join(format!("morrow-cli-missing-config-{}", std::process::id()));
    let config = CliConfig::load(&path, true).unwrap();
    assert_eq!(config.server, DEFAULT_SERVER.parse().unwrap());
}

#[test]
fn rejects_missing_explicit_config() {
    let path = std::env::temp_dir().join(format!(
        "morrow-cli-explicit-missing-config-{}",
        std::process::id()
    ));
    let err = CliConfig::load(&path, false).unwrap_err();
    assert!(err.to_string().contains("reading"));
}

#[test]
fn rejects_invalid_server_address() {
    let err = CliConfig::from_json(&serde_json::json!({"server": "bad"})).unwrap_err();
    assert!(err.to_string().contains("server"));
}

#[test]
fn rejects_auth_without_client_id() {
    let err = CliConfig::from_json(&serde_json::json!({
        "auth": {"enabled": true, "private_key_seed_hex": "00".repeat(32)}
    }))
    .unwrap_err();
    assert!(err.to_string().contains("auth.client_id"));
}

#[test]
fn rejects_auth_without_seed() {
    let err = CliConfig::from_json(&serde_json::json!({
        "auth": {"enabled": true, "client_id": "client1"}
    }))
    .unwrap_err();
    assert!(err.to_string().contains("private_key_seed_hex"));
}

#[test]
fn rejects_malformed_seed() {
    let err = CliConfig::from_json(&serde_json::json!({
        "auth": {
            "enabled": true,
            "client_id": "client1",
            "private_key_seed_hex": "bad"
        }
    }))
    .unwrap_err();
    assert!(err.to_string().contains("private_key_seed_hex"));
}

#[test]
fn parses_ping_args() {
    let args = Args::parse(["morrow-cli", "ping"].into_iter().map(str::to_string)).unwrap();
    assert_eq!(args.config_path, PathBuf::from(DEFAULT_CONFIG_PATH));
    assert!(!args.config_path_explicit);
    assert_eq!(args.command, Command::Ping);
}

#[test]
fn parses_server_override_and_bench_pubsub_args() {
    let args = Args::parse(
        [
            "morrow-cli",
            "--server",
            "127.0.0.1:4222",
            "bench",
            "pubsub",
            "orders/bench",
            "--messages",
            "20",
            "--payload-size",
            "64",
            "--publishers",
            "2",
            "--subscribers",
            "3",
            "--concurrency",
            "2",
            "--ack",
            "--json",
        ]
        .into_iter()
        .map(str::to_string),
    )
    .unwrap();
    assert_eq!(args.server, Some("127.0.0.1:4222".parse().unwrap()));
    assert_eq!(
        args.command,
        Command::BenchPubSub {
            subject: "orders/bench".into(),
            messages: Some(20),
            duration_ms: None,
            payload_size: 64,
            publishers: 2,
            subscribers: 3,
            concurrency: 2,
            ack: true,
            ack_level: Some(AckLevel::Durable),
            durable_id: None,
            json: true,
        }
    );
}

#[test]
fn parses_bench_ack_levels() {
    let args = Args::parse(
        [
            "morrow-cli",
            "bench",
            "pubsub",
            "orders",
            "--ack-level",
            "high-durability",
        ]
        .into_iter()
        .map(str::to_string),
    )
    .unwrap();
    assert!(matches!(
        args.command,
        Command::BenchPubSub {
            ack: false,
            ack_level: Some(AckLevel::HighDurability),
            ..
        }
    ));

    let legacy = Args::parse(
        ["morrow-cli", "bench", "pubsub", "orders", "--ack"]
            .into_iter()
            .map(str::to_string),
    )
    .unwrap();
    assert!(matches!(
        legacy.command,
        Command::BenchPubSub {
            ack: true,
            ack_level: Some(AckLevel::Durable),
            ..
        }
    ));
}

#[test]
fn parses_bench_duration() {
    let args = Args::parse(
        [
            "morrow-cli",
            "bench",
            "pubsub",
            "orders",
            "--duration",
            "2s",
        ]
        .into_iter()
        .map(str::to_string),
    )
    .unwrap();
    assert!(matches!(
        args.command,
        Command::BenchPubSub {
            duration_ms: Some(2_000),
            messages: None,
            ..
        }
    ));
}

#[test]
fn parses_version_args_without_a_config_path() {
    let args = Args::parse(["morrow-cli", "--version"].into_iter().map(str::to_string)).unwrap();
    assert_eq!(args.config_path, PathBuf::from(DEFAULT_CONFIG_PATH));
    assert!(!args.config_path_explicit);
    assert_eq!(args.command, Command::Version);
}

#[test]
fn parses_pub_args() {
    let args = Args::parse(
        [
            "morrow-cli",
            "--config",
            "custom.json",
            "pub",
            "orders/created",
            "hello",
        ]
        .into_iter()
        .map(str::to_string),
    )
    .unwrap();
    assert_eq!(args.config_path, PathBuf::from("custom.json"));
    assert!(args.config_path_explicit);
    assert_eq!(
        args.command,
        Command::Pub {
            subject: "orders/created".into(),
            payload: b"hello".to_vec(),
            qos: None,
            msg_id: None,
        }
    );
}

#[test]
fn parses_qos_pub_args() {
    let args = Args::parse(
        [
            "morrow-cli",
            "pub",
            "orders/created",
            "hello",
            "--qos",
            "2",
            "--msg-id",
            "msg-1",
        ]
        .into_iter()
        .map(str::to_string),
    )
    .unwrap();
    assert_eq!(
        args.command,
        Command::Pub {
            subject: "orders/created".into(),
            payload: b"hello".to_vec(),
            qos: Some(AckLevel::HighDurability),
            msg_id: Some("msg-1".into()),
        }
    );
}

#[test]
fn parses_sub_args() {
    let args = Args::parse(
        [
            "morrow-cli",
            "sub",
            "orders/*",
            "--sid",
            "sid2",
            "--queue",
            "workers",
            "--ack",
            "--max-messages",
            "2",
        ]
        .into_iter()
        .map(str::to_string),
    )
    .unwrap();
    assert_eq!(
        args.command,
        Command::Sub {
            subject: "orders/*".into(),
            sid: "sid2".into(),
            queue: Some("workers".into()),
            ack: true,
            max_messages: Some(2),
        }
    );
}

#[test]
fn parses_request_args() {
    let args = Args::parse(
        [
            "morrow-cli",
            "request",
            "orders/lookup",
            "hello",
            "--timeout-ms",
            "500",
        ]
        .into_iter()
        .map(str::to_string),
    )
    .unwrap();
    assert_eq!(
        args.command,
        Command::Request {
            subject: "orders/lookup".into(),
            payload: b"hello".to_vec(),
            timeout_ms: 500,
        }
    );
}

#[test]
fn parses_reply_args() {
    let args = Args::parse(
        ["morrow-cli", "reply", "orders/lookup", "--queue", "workers"]
            .into_iter()
            .map(str::to_string),
    )
    .unwrap();
    assert_eq!(
        args.command,
        Command::Reply {
            subject: "orders/lookup".into(),
            queue: Some("workers".into()),
        }
    );
}
