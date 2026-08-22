#[test]
fn equal_wildcard_route_binds_use_distinct_advertised_hostnames() {
    let configs = (1..=3)
        .map(|node_id| {
            server::Config::from_json(&serde_json::json!({
                "wal_dir": format!("./target/equal-route-bind-node-{node_id}"),
                "cluster": {
                    "enabled": true,
                    "node_id": node_id,
                    "auth_token": "test-cluster-token",
                    "raft_listen": "0.0.0.0:5222",
                    "allow_insecure_internal_transports": true,
                    "route_listen": "0.0.0.0:6222",
                    "route_advertise": format!("broker-{node_id}:6222"),
                    "routes": (1..=3)
                        .filter(|peer_id| *peer_id != node_id)
                        .map(|peer_id| format!("broker-{peer_id}:6222"))
                        .collect::<Vec<_>>(),
                    "raft_dir": format!("./target/equal-route-bind-node-{node_id}/raft"),
                    "bootstrap": node_id == 1,
                    "nodes": (1..=3)
                        .map(|peer_id| serde_json::json!({
                            "node_id": peer_id,
                            "raft_addr": format!("broker-{peer_id}:5222"),
                            "client_addr": format!("broker-{peer_id}:4222"),
                            "route_addr": format!("broker-{peer_id}:6222")
                        }))
                        .collect::<Vec<_>>()
                }
            }))
            .unwrap()
        })
        .collect::<Vec<_>>();

    assert!(configs.iter().all(|config| {
        config.cluster.as_ref().unwrap().route_listen == Some("0.0.0.0:6222".parse().unwrap())
    }));
    let advertisements = configs
        .iter()
        .map(|config| {
            config
                .cluster
                .as_ref()
                .unwrap()
                .advertised_route_addr()
                .unwrap()
        })
        .collect::<std::collections::HashSet<_>>();
    assert_eq!(advertisements.len(), 3);
}
