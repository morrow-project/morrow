use super::*;

mod stream_tests;

#[test]
fn parses_json_config() {
    let value = serde_json::json!({
        "listen": "127.0.0.1:4223",
        "http_listen": "127.0.0.1:8223",
        "admin_token": "admin-secret",
        "wal_dir": "./target/test-wal-config",
        "wal_segment_bytes": 4096,
        "fsync_interval_ms": 10,
        "max_payload": 2048,
        "max_control_line": 4096,
        "max_ack_timeout_ms": 60000,
        "max_in_flight": 2048,
        "max_fetch_messages": 128,
        "max_fetch_bytes": 65536,
        "max_encoded_batch_bytes": 131072,
        "verbose": true,
        "tls": null,
        "auth": null
    });

    let config = Config::from_json(&value).unwrap();
    assert_eq!(config.listen, "127.0.0.1:4223".parse().unwrap());
    assert_eq!(config.http_listen, Some("127.0.0.1:8223".parse().unwrap()));
    assert_eq!(config.admin_token.as_deref(), Some("admin-secret"));
    assert_eq!(config.wal_dir, PathBuf::from("./target/test-wal-config"));
    assert_eq!(config.wal_segment_bytes, 4096);
    assert_eq!(config.fsync_interval_ms, 10);
    assert_eq!(config.max_payload, 2048);
    assert_eq!(config.max_control_line, 4096);
    assert_eq!(config.max_ack_timeout_ms, 60_000);
    assert_eq!(config.max_in_flight, 2_048);
    assert_eq!(config.max_fetch_messages, 128);
    assert_eq!(config.max_fetch_bytes, 65_536);
    assert_eq!(config.max_encoded_batch_bytes, 131_072);
    assert!(config.verbose);
    assert!(config.tls.is_none());
    assert!(!config.auth.enabled);
    assert!(config.cluster.is_none());
}

#[test]
fn parses_cluster_route_mesh_config() {
    let value = serde_json::json!({
        "listen": "127.0.0.1:4221",
        "wal_dir": "./target/test-wal-config-routes/wal",
        "cluster": {
            "enabled": true,
            "node_id": 1,
            "auth_token": "cluster-secret",
            "raft_listen": "127.0.0.1:5221",
            "route_listen": "127.0.0.1:6221",
            "routes": ["127.0.0.1:6222", "127.0.0.1:6223"],
            "raft_dir": "./target/test-wal-config-routes/raft",
            "bootstrap": true,
            "nodes": [
                {
                    "node_id": 1,
                    "raft_addr": "127.0.0.1:5221",
                    "client_addr": "127.0.0.1:4221"
                }
            ]
        }
    });

    let config = Config::from_json(&value).unwrap();
    let cluster = config.cluster.unwrap();
    assert_eq!(
        cluster.route_listen,
        Some("127.0.0.1:6221".parse().unwrap())
    );
    assert_eq!(cluster.routes, vec!["127.0.0.1:6222", "127.0.0.1:6223"]);
    assert_eq!(cluster.route_reconnect_ms, 500);
}

#[test]
fn rejects_invalid_field_types() {
    let value = serde_json::json!({
        "listen": 4222
    });

    let err = Config::from_json(&value).unwrap_err();
    assert!(err.to_string().contains("listen"));
}

#[test]
fn parses_tls_config_without_validation() {
    let value = serde_json::json!({
        "tls": {
            "cert_file": "./server-cert.pem",
            "key_file": "./server-key.pem",
            "handshake_timeout_ms": 5000
        }
    });

    let tls = get_tls_config(&value).unwrap().unwrap();
    assert_eq!(tls.cert_file, PathBuf::from("./server-cert.pem"));
    assert_eq!(tls.key_file, PathBuf::from("./server-key.pem"));
    assert_eq!(tls.handshake_timeout_ms, 5000);
}

#[test]
fn parses_cluster_config() {
    let value = serde_json::json!({
        "wal_dir": "./target/test-wal-cluster-config",
        "cluster": {
            "enabled": true,
            "node_id": 1,
            "auth_token": "cluster-secret",
            "raft_listen": "127.0.0.1:5221",
            "routes": ["localhost:6222"],
            "raft_dir": "./target/test-wal-cluster-config/raft",
            "bootstrap": true,
            "nodes": [
                {"node_id": 1, "raft_addr": "localhost:5221", "client_addr": "localhost:4221"},
                {"node_id": 2, "raft_addr": "localhost:5222", "client_addr": "localhost:4222"}
            ],
            "election_timeout_min_ms": 200,
            "election_timeout_max_ms": 400,
            "heartbeat_interval_ms": 50,
            "snapshot_threshold": 100
        }
    });

    let config = Config::from_json(&value).unwrap();
    let cluster = config.cluster.unwrap();
    assert_eq!(cluster.node_id, 1);
    assert_eq!(cluster.auth_token, "cluster-secret");
    assert_eq!(cluster.nodes.len(), 2);
    assert_eq!(cluster.routes.len(), 1);
    assert!(cluster.bootstrap);
}

#[test]
fn rejects_cluster_missing_self_node() {
    let err = Config::from_json(&serde_json::json!({
        "wal_dir": "./target/test-wal-cluster-missing-self",
        "cluster": {
            "enabled": true,
            "node_id": 3,
            "auth_token": "cluster-secret",
            "raft_listen": "127.0.0.1:5221",
            "raft_dir": "./target/test-wal-cluster-missing-self/raft",
            "nodes": [
                {"node_id": 1, "raft_addr": "127.0.0.1:5221", "client_addr": "127.0.0.1:4221"}
            ]
        }
    }))
    .unwrap_err();
    assert!(err.to_string().contains("cluster.node_id"));
}

#[test]
fn rejects_http_listener_without_admin_token() {
    let err = Config::from_json(&serde_json::json!({
        "http_listen": "127.0.0.1:8222",
        "wal_dir": "./target/test-wal-http-without-token"
    }))
    .unwrap_err();
    assert!(err.to_string().contains("admin_token"));
}

#[test]
fn rejects_cluster_without_auth_token() {
    let err = Config::from_json(&serde_json::json!({
        "wal_dir": "./target/test-wal-cluster-without-token",
        "cluster": {
            "enabled": true,
            "node_id": 1,
            "raft_listen": "127.0.0.1:5221",
            "raft_dir": "./target/test-wal-cluster-without-token/raft",
            "nodes": [
                {"node_id": 1, "raft_addr": "127.0.0.1:5221", "client_addr": "127.0.0.1:4221"}
            ]
        }
    }))
    .unwrap_err();
    assert!(err.to_string().contains("cluster.auth_token"));
}

#[test]
fn rejects_zero_max_control_line() {
    let err = Config::from_json(&serde_json::json!({
        "wal_dir": "./target/test-wal-zero-control-line",
        "max_control_line": 0
    }))
    .unwrap_err();
    assert!(err.to_string().contains("max_control_line"));
}

#[test]
fn rejects_invalid_flow_control_limits() {
    for (field, limit) in [
        ("max_ack_timeout_ms", 0_u64),
        ("max_fetch_messages", 0),
        ("max_fetch_bytes", 0),
        ("max_encoded_batch_bytes", 0),
    ] {
        let mut config = serde_json::json!({
            "wal_dir": format!("./target/test-wal-zero-{field}"),
        });
        config[field] = limit.into();
        let err = Config::from_json(&config).unwrap_err();
        assert!(err.to_string().contains(field), "unexpected error: {err}");
    }
}

#[test]
fn rejects_zero_wal_segment_bytes() {
    let err = Config::from_json(&serde_json::json!({
        "wal_dir": "./target/test-wal-zero-segment",
        "wal_segment_bytes": 0
    }))
    .unwrap_err();
    assert!(err.to_string().contains("wal_segment_bytes"));
}

#[test]
fn parses_auth_config() {
    let value = serde_json::json!({
        "auth": {
            "enabled": true,
            "clients": [
                {
                    "client_id": "client1",
                    "public_key": "ABCD",
                    "permissions": {
                        "publish": ["orders.*", "events.>"],
                        "subscribe": ["orders.created"]
                    }
                }
            ]
        }
    });

    let auth = get_auth_config(&value).unwrap();
    assert!(auth.enabled);
    let client = &auth.clients["client1"];
    assert_eq!(client.public_key, "abcd");
    let permissions = client.permissions.as_ref().unwrap();
    assert_eq!(
        permissions.publish.as_ref().unwrap(),
        &["orders.*".to_string(), "events.>".to_string()]
    );
    assert_eq!(
        permissions.subscribe.as_ref().unwrap(),
        &["orders.created".to_string()]
    );
}

#[test]
fn rejects_enabled_auth_without_clients() {
    let value = serde_json::json!({
        "auth": {"enabled": true, "clients": []}
    });

    let err = Config::from_json(&value).unwrap_err();
    assert!(err.to_string().contains("auth.clients"));
}

#[test]
fn rejects_invalid_auth_permission_pattern() {
    let value = serde_json::json!({
        "auth": {
            "enabled": true,
            "clients": [
                {
                    "client_id": "client1",
                    "public_key": "abcd",
                    "permissions": {
                        "publish": ["orders.>.created"]
                    }
                }
            ]
        }
    });

    let err = get_auth_config(&value).unwrap_err();
    assert!(err.to_string().contains("invalid subject pattern"));
}
