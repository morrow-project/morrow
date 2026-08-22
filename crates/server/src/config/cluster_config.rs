use super::*;

pub(super) fn get_cluster_config(value: &serde_json::Value) -> Result<Option<ClusterConfig>> {
    let Some(cluster) = value.get("cluster") else {
        return Ok(None);
    };
    if cluster.is_null() {
        return Ok(None);
    }
    let serde_json::Value::Object(_) = cluster else {
        return Err(BrokerError::msg("config field cluster must be an object"));
    };
    let enabled = get_bool(cluster, "enabled")?.unwrap_or(false);
    if !enabled {
        return Ok(None);
    }
    let node_id = get_u64(cluster, "node_id")?
        .ok_or_else(|| BrokerError::msg("config field cluster.node_id is required"))?;
    let auth_token = get_secret(cluster, "auth_token", "auth_token_file")?.ok_or_else(|| {
        BrokerError::msg("config field cluster.auth_token or auth_token_file is required")
    })?;
    let raft_listen = get_string(cluster, "raft_listen")?
        .ok_or_else(|| BrokerError::msg("config field cluster.raft_listen is required"))?
        .parse()
        .context("config field cluster.raft_listen must be a socket address")?;
    let route_listen = get_optional_socket_addr(cluster, "route_listen")?;
    let routes = get_string_array(cluster, "routes")?;
    let route_reconnect_ms = get_u64(cluster, "route_reconnect_ms")?.unwrap_or(500);
    let raft_dir = PathBuf::from(
        get_string(cluster, "raft_dir")?
            .ok_or_else(|| BrokerError::msg("config field cluster.raft_dir is required"))?,
    );
    let bootstrap = get_bool(cluster, "bootstrap")?.unwrap_or(false);
    let election_timeout_min_ms = get_u64(cluster, "election_timeout_min_ms")?.unwrap_or(150);
    let election_timeout_max_ms = get_u64(cluster, "election_timeout_max_ms")?.unwrap_or(300);
    let heartbeat_interval_ms = get_u64(cluster, "heartbeat_interval_ms")?.unwrap_or(50);
    let snapshot_threshold = get_u64(cluster, "snapshot_threshold")?.unwrap_or(10_000);
    let nodes = get_cluster_nodes(cluster)?;
    let config = ClusterConfig {
        enabled,
        node_id,
        auth_token,
        raft_listen,
        route_listen,
        routes,
        route_reconnect_ms,
        raft_dir,
        bootstrap,
        nodes,
        election_timeout_min_ms,
        election_timeout_max_ms,
        heartbeat_interval_ms,
        snapshot_threshold,
    };
    config.validate()?;
    Ok(Some(config))
}

fn get_cluster_nodes(value: &serde_json::Value) -> Result<Vec<ClusterNodeConfig>> {
    let nodes = value
        .get("nodes")
        .ok_or_else(|| BrokerError::msg("config field cluster.nodes is required"))?;
    let serde_json::Value::Array(nodes) = nodes else {
        return Err(BrokerError::msg(
            "config field cluster.nodes must be an array",
        ));
    };
    let mut out = Vec::with_capacity(nodes.len());
    for node in nodes {
        let serde_json::Value::Object(_) = node else {
            return Err(BrokerError::msg(
                "config field cluster.nodes must contain only objects",
            ));
        };
        let node_id = get_u64(node, "node_id")?
            .ok_or_else(|| BrokerError::msg("config field cluster.nodes[].node_id is required"))?;
        let raft_addr = get_string(node, "raft_addr")?
            .ok_or_else(|| BrokerError::msg("config field cluster.nodes[].raft_addr is required"))?
            .to_string();
        let client_addr = get_string(node, "client_addr")?
            .ok_or_else(|| {
                BrokerError::msg("config field cluster.nodes[].client_addr is required")
            })?
            .to_string();
        out.push(ClusterNodeConfig {
            node_id,
            raft_addr,
            client_addr,
        });
    }
    Ok(out)
}

fn get_optional_socket_addr(value: &serde_json::Value, key: &str) -> Result<Option<SocketAddr>> {
    match value.get(key) {
        Some(serde_json::Value::String(value)) => value
            .parse()
            .with_context(|| format!("config field cluster.{key} must be a socket address"))
            .map(Some),
        Some(serde_json::Value::Null) | None => Ok(None),
        Some(_) => Err(BrokerError::msg(format!(
            "config field cluster.{key} must be a string or null"
        ))),
    }
}

fn get_string_array(value: &serde_json::Value, key: &str) -> Result<Vec<String>> {
    match value.get(key) {
        Some(serde_json::Value::Array(values)) => values
            .iter()
            .map(|value| {
                let serde_json::Value::String(value) = value else {
                    return Err(BrokerError::msg(format!(
                        "config field cluster.{key} must contain only strings"
                    )));
                };
                Ok(value.clone())
            })
            .collect(),
        Some(serde_json::Value::Null) | None => Ok(Vec::new()),
        Some(_) => Err(BrokerError::msg(format!(
            "config field cluster.{key} must be an array"
        ))),
    }
}
