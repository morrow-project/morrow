use super::*;

#[test]
fn parses_json_config() {
    let value = serde_json::json!({
        "listen": "127.0.0.1:4223",
        "http_listen": "127.0.0.1:8223",
        "wal_dir": "./target/test-wal-config",
        "fsync_interval_ms": 10,
        "max_payload": 2048,
        "verbose": true,
        "tls": null,
        "auth": null
    });

    let config = Config::from_json(&value).unwrap();
    assert_eq!(config.listen, "127.0.0.1:4223".parse().unwrap());
    assert_eq!(config.http_listen, Some("127.0.0.1:8223".parse().unwrap()));
    assert_eq!(config.wal_dir, PathBuf::from("./target/test-wal-config"));
    assert_eq!(config.fsync_interval_ms, 10);
    assert_eq!(config.max_payload, 2048);
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
    assert_eq!(
        cluster.routes,
        vec![
            "127.0.0.1:6222".parse().unwrap(),
            "127.0.0.1:6223".parse().unwrap()
        ]
    );
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
            "raft_listen": "127.0.0.1:5221",
            "raft_dir": "./target/test-wal-cluster-config/raft",
            "bootstrap": true,
            "nodes": [
                {"node_id": 1, "raft_addr": "127.0.0.1:5221", "client_addr": "127.0.0.1:4221"},
                {"node_id": 2, "raft_addr": "127.0.0.1:5222", "client_addr": "127.0.0.1:4222"}
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
    assert_eq!(cluster.nodes.len(), 2);
    assert!(cluster.bootstrap);
}

#[test]
fn rejects_cluster_missing_self_node() {
    let err = Config::from_json(&serde_json::json!({
        "wal_dir": "./target/test-wal-cluster-missing-self",
        "cluster": {
            "enabled": true,
            "node_id": 3,
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
fn parses_auth_config() {
    let value = serde_json::json!({
        "auth": {
            "enabled": true,
            "clients": [
                {"client_id": "client1", "public_key": "ABCD"}
            ]
        }
    });

    let auth = get_auth_config(&value).unwrap();
    assert!(auth.enabled);
    assert_eq!(auth.clients["client1"], "abcd");
}
