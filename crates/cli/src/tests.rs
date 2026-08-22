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
    assert_eq!(args.command, Command::Ping);
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
