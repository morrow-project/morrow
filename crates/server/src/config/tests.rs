use super::*;

mod stream_tests;

#[test]
fn recognizes_server_help_flags() {
    assert!(Config::is_help_arg(std::ffi::OsStr::new("-h")));
    assert!(Config::is_help_arg(std::ffi::OsStr::new("--help")));
    assert!(!Config::is_help_arg(std::ffi::OsStr::new("morrow.json")));
}

#[test]
fn uses_defaults_when_no_config_path_is_supplied() {
    let config = Config::load_from_args_iter([OsString::from("morrow-server")]).unwrap();

    assert_eq!(config.listen, "127.0.0.1:4222".parse().unwrap());
    assert_eq!(config.wal_dir, PathBuf::from("./morrow-wal"));
    assert_eq!(config.max_payload, 1_048_576);
    assert!(config.http_listen.is_none());
    assert!(config.websocket.is_none());
    assert!(config.tls.is_none());
    assert!(!config.auth.enabled);
    assert!(config.cluster.is_none());
}

#[test]
fn parses_json_config() {
    let value = serde_json::json!({
        "listen": "127.0.0.1:4223",
        "http_listen": "127.0.0.1:8223",
        "websocket": {
            "listen": "127.0.0.1:8080",
            "tls": null,
            "allowed_origins": ["http://localhost:3000"]
        },
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
    assert_eq!(
        config.websocket.as_ref().map(|websocket| websocket.listen),
        Some("127.0.0.1:8080".parse().unwrap())
    );
    assert_eq!(
        config.websocket.as_ref().unwrap().allowed_origins,
        vec!["http://localhost:3000"]
    );
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
fn parses_per_tenant_quota_limits() {
    let config = Config::from_json(&serde_json::json!({
        "tenant_quotas": {
            "tenant-a": {
                "max_connections": 3,
                "max_memory_bytes": 4096,
                "max_disk_bytes": 8192,
                "max_tasks": 7,
                "max_background_tasks": 9
            }
        }
    }))
    .unwrap();

    assert_eq!(
        config.tenant_quotas.get("tenant-a"),
        Some(&TenantQuotaConfig {
            max_connections: 3,
            max_memory_bytes: 4096,
            max_disk_bytes: 8192,
            max_tasks: 7,
            max_background_tasks: 9,
        })
    );
}

#[test]
fn rejects_unknown_per_tenant_quota_fields() {
    let error = Config::from_json(&serde_json::json!({
        "tenant_quotas": {"tenant-a": {"max_connections": 3, "typo": 1}}
    }))
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("unknown field tenant_quotas.tenant-a.typo")
    );
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
            "allow_insecure_internal_transports": true,
            "route_listen": "127.0.0.1:6221",
            "route_advertise": "morrow-1:6221",
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
    assert_eq!(cluster.route_advertise.as_deref(), Some("morrow-1:6221"));
    assert_eq!(cluster.routes, vec!["127.0.0.1:6222", "127.0.0.1:6223"]);
    assert_eq!(cluster.route_reconnect_ms, 500);
}

#[test]
fn derives_route_advertisement_from_self_node_metadata() {
    let config = Config::from_json(&route_advertisement_config()).unwrap();
    let cluster = config.cluster.unwrap();
    assert_eq!(cluster.route_listen, Some("0.0.0.0:6222".parse().unwrap()));
    assert_eq!(cluster.advertised_route_addr(), Some("morrow-1:6222"));
}

#[test]
fn rejects_wildcard_duplicate_and_self_route_advertisements() {
    let mut wildcard = route_advertisement_config();
    wildcard["cluster"]["route_advertise"] = serde_json::json!("0.0.0.0:6222");
    assert!(
        Config::from_json(&wildcard)
            .unwrap_err()
            .to_string()
            .contains("wildcard")
    );

    let mut invalid_hostname = route_advertisement_config();
    invalid_hostname["cluster"]["route_advertise"] = serde_json::json!("bad/host:6222");
    assert!(
        Config::from_json(&invalid_hostname)
            .unwrap_err()
            .to_string()
            .contains("routable hostname")
    );

    let mut duplicate = route_advertisement_config();
    duplicate["cluster"]["nodes"][1]["route_addr"] = serde_json::json!("morrow-1:6222");
    assert!(
        Config::from_json(&duplicate)
            .unwrap_err()
            .to_string()
            .contains("duplicate route_addr")
    );

    let mut self_seed = route_advertisement_config();
    self_seed["cluster"]["routes"] = serde_json::json!(["morrow-1:6222"]);
    assert!(
        Config::from_json(&self_seed)
            .unwrap_err()
            .to_string()
            .contains("must not contain this node")
    );

    let mut conflict = route_advertisement_config();
    conflict["cluster"]["route_advertise"] = serde_json::json!("morrow-other:6222");
    assert!(
        Config::from_json(&conflict)
            .unwrap_err()
            .to_string()
            .contains("conflicts with self")
    );
}

fn route_advertisement_config() -> serde_json::Value {
    serde_json::json!({
        "wal_dir": "./target/test-route-advertisement-config",
        "cluster": {
            "enabled": true,
            "node_id": 1,
            "auth_token": "cluster-secret",
            "raft_listen": "0.0.0.0:5222",
            "allow_insecure_internal_transports": true,
            "route_listen": "0.0.0.0:6222",
            "routes": ["morrow-2:6222"],
            "raft_dir": "./target/test-route-advertisement-config/raft",
            "bootstrap": true,
            "nodes": [
                {
                    "node_id": 1,
                    "raft_addr": "morrow-1:5222",
                    "client_addr": "morrow-1:4222",
                    "route_addr": "morrow-1:6222"
                },
                {
                    "node_id": 2,
                    "raft_addr": "morrow-2:5222",
                    "client_addr": "morrow-2:4222",
                    "route_addr": "morrow-2:6222"
                }
            ]
        }
    })
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
            "cert_file": "./morrow-cert.pem",
            "key_file": "./morrow-key.pem",
            "handshake_timeout_ms": 5000
        }
    });

    let tls = get_tls_config(&value).unwrap().unwrap();
    assert_eq!(tls.cert_file, PathBuf::from("./morrow-cert.pem"));
    assert_eq!(tls.key_file, PathBuf::from("./morrow-key.pem"));
    assert_eq!(tls.handshake_timeout_ms, 5000);
}

#[test]
fn parses_resource_quotas() {
    let config = Config::from_json(&serde_json::json!({
        "wal_dir": "./target/test-wal-quotas",
        "quotas": {
            "max_connections": 7,
            "max_connections_per_identity": 2,
            "max_outbound_bytes_per_connection": 4096,
            "client_idle_timeout_ms": 50,
            "http_header_timeout_ms": 25
        }
    }))
    .unwrap();
    assert_eq!(config.quotas.max_connections, 7);
    assert_eq!(config.quotas.max_connections_per_identity, 2);
    assert_eq!(config.quotas.max_outbound_bytes_per_connection, 4096);
    assert_eq!(config.quotas.client_idle_timeout_ms, 50);
    assert_eq!(config.quotas.http_header_timeout_ms, 25);
}

#[test]
fn rejects_zero_resource_quota() {
    let err = Config::from_json(&serde_json::json!({
        "wal_dir": "./target/test-wal-zero-quota",
        "quotas": {"max_route_connections": 0}
    }))
    .unwrap_err();
    assert!(err.to_string().contains("quotas.max_route_connections"));
}

#[test]
fn reads_admin_and_client_public_key_from_secret_files() {
    let dir = tempfile::TempDir::new().unwrap();
    let admin = dir.path().join("admin-token");
    let public_key = dir.path().join("client-public-key");
    std::fs::write(&admin, "admin-secret\n").unwrap();
    std::fs::write(&public_key, format!("{}\n", "ab".repeat(32))).unwrap();
    let config = Config::from_json(&serde_json::json!({
        "listen": "127.0.0.1:0",
        "http_listen": "127.0.0.1:0",
        "admin_token_file": admin,
        "wal_dir": dir.path().join("wal"),
        "auth": {
            "enabled": true,
            "clients": [{
                "client_id": "local-client",
                "public_key_file": public_key,
            }]
        }
    }))
    .unwrap();

    assert_eq!(config.admin_token.as_deref(), Some("admin-secret"));
    assert_eq!(
        config.auth.clients["local-client"].public_key,
        "ab".repeat(32)
    );
}

#[test]
fn rejects_missing_or_ambiguous_secret_files() {
    let dir = tempfile::TempDir::new().unwrap();
    let missing = Config::from_json(&serde_json::json!({
        "wal_dir": dir.path().join("missing-wal"),
        "http_listen": "127.0.0.1:0",
        "admin_token_file": dir.path().join("missing-token"),
    }))
    .unwrap_err();
    assert!(missing.to_string().contains("reading secret file"));

    let ambiguous = Config::from_json(&serde_json::json!({
        "wal_dir": dir.path().join("ambiguous-wal"),
        "admin_token": "inline",
        "admin_token_file": dir.path().join("token"),
    }))
    .unwrap_err();
    assert!(ambiguous.to_string().contains("mutually exclusive"));
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
            "allow_insecure_internal_transports": true,
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
            "allow_insecure_internal_transports": true,
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
fn rejects_plaintext_internal_transport_by_default() {
    let err = Config::from_json(&serde_json::json!({
        "wal_dir": "./target/test-wal-insecure-cluster",
        "cluster": {
            "enabled": true,
            "node_id": 1,
            "auth_token": "cluster-secret",
            "raft_listen": "127.0.0.1:5221",
            "raft_dir": "./target/test-wal-insecure-cluster/raft",
            "bootstrap": true,
            "nodes": [
                {"node_id": 1, "raft_addr": "127.0.0.1:5221", "client_addr": "127.0.0.1:4221"}
            ]
        }
    }))
    .unwrap_err();
    assert!(err.to_string().contains("cluster.raft_tls is required"));
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
                        "publish": ["orders/*", "events/**"],
                        "subscribe": ["orders/created"]
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
        &["orders/*".to_string(), "events/**".to_string()]
    );
    assert_eq!(
        permissions.subscribe.as_ref().unwrap(),
        &["orders/created".to_string()]
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
                        "publish": ["orders/**.created"]
                    }
                }
            ]
        }
    });

    let err = get_auth_config(&value).unwrap_err();
    assert!(err.to_string().contains("invalid subject pattern"));
}

#[test]
fn rejects_unknown_fields_at_each_configuration_level() {
    let mut value = serde_json::json!({
        "auth": {"enabled": false},
        "wal_dir": "./target/test-unknown-field"
    });
    value["auth"]["enabeld"] = serde_json::json!(true);
    let err = Config::from_json(&value).unwrap_err();
    assert!(err.to_string().contains("config.auth.enabeld"));
}

#[test]
fn production_requires_security_for_non_loopback_listeners() {
    let err = Config::from_json(&serde_json::json!({
        "production": true,
        "listen": "0.0.0.0:4222",
        "wal_dir": "./target/test-production-listener"
    }))
    .unwrap_err();
    assert!(
        err.to_string()
            .contains("TLS for non-loopback client listener")
    );
}

#[test]
fn check_config_is_side_effect_free_and_redacts_secrets() {
    let dir = tempfile::TempDir::new().unwrap();
    let config_path = dir.path().join("morrow.json");
    let wal_dir = dir.path().join("wal");
    std::fs::write(
        &config_path,
        serde_json::to_vec(&serde_json::json!({
            "production": true,
            "listen": "127.0.0.1:4222",
            "admin_token": "secret-value",
            "wal_dir": wal_dir
        }))
        .unwrap(),
    )
    .unwrap();

    let output = Config::check_file(&config_path).unwrap();
    assert!(!wal_dir.exists());
    assert!(!output.contains("secret-value"));
    assert!(output.contains("authentication_enabled"));
}
