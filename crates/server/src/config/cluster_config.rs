use super::*;

impl InternalTlsConfig {
    pub(super) fn validate(&self, field: &str) -> Result<()> {
        crate::broker_ensure!(
            self.handshake_timeout_ms > 0,
            "config field cluster.{field}.handshake_timeout_ms must be greater than zero"
        );
        for (name, path) in [
            ("cert_file", &self.cert_file),
            ("key_file", &self.key_file),
            ("ca_cert_file", &self.ca_cert_file),
        ] {
            crate::broker_ensure!(
                path.is_file(),
                "config field cluster.{field}.{name} must point to an existing file"
            );
        }
        Ok(())
    }
}

impl ClusterConfig {
    pub(super) fn validate(&self) -> Result<()> {
        crate::broker_ensure!(
            self.enabled,
            "cluster.enabled must be true when cluster is present"
        );
        crate::broker_ensure!(
            self.node_id > 0,
            "cluster.node_id must be greater than zero"
        );
        crate::broker_ensure!(
            self.node_id <= u64::from(u16::MAX),
            "cluster.node_id must fit in 16 bits"
        );
        crate::broker_ensure!(
            !self.auth_token.is_empty(),
            "cluster.auth_token must not be empty"
        );
        crate::broker_ensure!(
            self.heartbeat_interval_ms > 0,
            "cluster.heartbeat_interval_ms must be greater than zero"
        );
        crate::broker_ensure!(
            self.route_reconnect_ms > 0,
            "cluster.route_reconnect_ms must be greater than zero"
        );
        crate::broker_ensure!(
            self.election_timeout_min_ms > self.heartbeat_interval_ms,
            "cluster.election_timeout_min_ms must be greater than heartbeat_interval_ms"
        );
        crate::broker_ensure!(
            self.election_timeout_max_ms > self.election_timeout_min_ms,
            "cluster.election_timeout_max_ms must be greater than election_timeout_min_ms"
        );
        crate::broker_ensure!(
            self.snapshot_threshold > 0,
            "cluster.snapshot_threshold must be greater than zero"
        );
        crate::broker_ensure!(
            !self.nodes.is_empty(),
            "cluster.nodes must contain at least one node"
        );
        crate::broker_ensure!(
            !self.controller_voters.is_empty(),
            "cluster.controller_voters must contain at least one node"
        );
        crate::broker_ensure!(
            self.allow_insecure_internal_transports || self.raft_tls.is_some(),
            "cluster.raft_tls is required unless allow_insecure_internal_transports is true"
        );
        crate::broker_ensure!(
            self.allow_insecure_internal_transports
                || self.route_listen.is_none()
                || self.route_tls.is_some(),
            "cluster.route_tls is required when route_listen is set unless allow_insecure_internal_transports is true"
        );

        let mut ids = std::collections::HashSet::new();
        let mut route_addresses = std::collections::HashMap::new();
        let mut has_self = false;
        for node in &self.nodes {
            crate::broker_ensure!(
                node.node_id > 0,
                "cluster.nodes[].node_id must be greater than zero"
            );
            crate::broker_ensure!(
                ids.insert(node.node_id),
                "cluster.nodes contains duplicate node_id"
            );
            has_self |= node.node_id == self.node_id;
            if let Some(route_addr) = node.route_addr.as_deref() {
                validate_route_advertisement(route_addr, "cluster.nodes[].route_addr")?;
                crate::broker_ensure!(
                    route_addresses.insert(route_addr, node.node_id).is_none(),
                    "cluster.nodes contains duplicate route_addr {route_addr}"
                );
            }
        }
        let mut voters = std::collections::HashSet::new();
        for voter in &self.controller_voters {
            crate::broker_ensure!(
                *voter > 0 && ids.contains(voter),
                "cluster.controller_voters must reference configured node IDs"
            );
            crate::broker_ensure!(
                voters.insert(*voter),
                "cluster.controller_voters contains duplicate node_id"
            );
        }
        if self.role == ClusterRole::Controller {
            crate::broker_ensure!(
                self.controller_voters.contains(&self.node_id),
                "controller node must be a controller voter"
            );
        }
        if self.role == ClusterRole::Broker {
            crate::broker_ensure!(
                !self.controller_voters.contains(&self.node_id),
                "broker node must not be a controller voter"
            );
        }
        crate::broker_ensure!(has_self, "cluster.node_id must be present in cluster.nodes");
        crate::broker_ensure!(
            self.route_listen.is_some() || self.route_advertise.is_none(),
            "cluster.route_advertise requires cluster.route_listen"
        );
        if self.route_listen.is_some() {
            let advertised = self.advertised_route_addr().ok_or_else(|| {
                BrokerError::msg(
                    "cluster.route_advertise or self cluster.nodes[].route_addr is required when route_listen is set",
                )
            })?;
            validate_route_advertisement(advertised, "cluster.route_advertise")?;
            if let (Some(configured), Some(node_addr)) = (
                self.route_advertise.as_deref(),
                self.self_node().and_then(|node| node.route_addr.as_deref()),
            ) {
                crate::broker_ensure!(
                    configured == node_addr,
                    "cluster.route_advertise conflicts with self cluster.nodes[].route_addr"
                );
            }
            crate::broker_ensure!(
                !self.nodes.iter().any(|node| {
                    node.node_id != self.node_id && node.route_addr.as_deref() == Some(advertised)
                }),
                "cluster.route_advertise conflicts with another node"
            );
            let mut routes = std::collections::HashSet::new();
            for route in &self.routes {
                validate_route_advertisement(route, "cluster.routes[]")?;
                crate::broker_ensure!(
                    routes.insert(route),
                    "cluster.routes contains duplicate address {route}"
                );
                crate::broker_ensure!(
                    route != advertised,
                    "cluster.routes must not contain this node's advertised route address"
                );
            }
        }
        if self.raft_tls.is_some() || self.route_tls.is_some() {
            for node in &self.nodes {
                crate::broker_ensure!(
                    node.tls_server_name
                        .as_deref()
                        .is_some_and(|name| !name.is_empty()),
                    "cluster.nodes[].tls_server_name is required for internal TLS"
                );
                crate::broker_ensure!(
                    !node.tls_cert_files.is_empty()
                        && node.tls_cert_files.iter().all(|path| path.is_file()),
                    "cluster.nodes[].tls_cert_files must contain existing certificates for internal TLS"
                );
            }
        }
        if self.route_tls.is_some() {
            crate::broker_ensure!(
                self.route_listen.is_some(),
                "cluster.route_tls requires route_listen"
            );
            crate::broker_ensure!(
                self.nodes.iter().all(|node| node.route_addr.is_some()),
                "cluster.nodes[].route_addr is required for route TLS"
            );
        }
        Ok(())
    }

    pub fn self_node(&self) -> Option<&ClusterNodeConfig> {
        self.nodes.iter().find(|node| node.node_id == self.node_id)
    }

    pub fn advertised_route_addr(&self) -> Option<&str> {
        self.route_advertise
            .as_deref()
            .or_else(|| self.self_node().and_then(|node| node.route_addr.as_deref()))
    }

    pub fn is_controller_voter(&self) -> bool {
        self.controller_voters.contains(&self.node_id)
    }
}

fn validate_route_advertisement(value: &str, field: &str) -> Result<()> {
    if let Ok(address) = value.parse::<SocketAddr>() {
        crate::broker_ensure!(
            !address.ip().is_unspecified(),
            "config field {field} must not advertise a wildcard address"
        );
        crate::broker_ensure!(
            address.port() > 0,
            "config field {field} must use a non-zero port"
        );
        return Ok(());
    }
    let (host, port) = value
        .rsplit_once(':')
        .ok_or_else(|| BrokerError::msg(format!("config field {field} must be host:port")))?;
    let port = port
        .parse::<u16>()
        .map_err(|_| BrokerError::msg(format!("config field {field} has an invalid port")))?;
    crate::broker_ensure!(port > 0, "config field {field} must use a non-zero port");
    let host = host.trim_matches(['[', ']']);
    crate::broker_ensure!(
        valid_route_hostname(host),
        "config field {field} must advertise a routable hostname or IP address"
    );
    Ok(())
}

fn valid_route_hostname(host: &str) -> bool {
    !host.is_empty()
        && host.len() <= 253
        && host.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && label
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
                && label
                    .as_bytes()
                    .first()
                    .is_some_and(u8::is_ascii_alphanumeric)
                && label
                    .as_bytes()
                    .last()
                    .is_some_and(u8::is_ascii_alphanumeric)
        })
}

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
    let raft_tls = get_internal_tls_config(cluster, "raft_tls")?;
    let allow_insecure_internal_transports =
        get_bool(cluster, "allow_insecure_internal_transports")?.unwrap_or(false);
    let route_listen = get_optional_socket_addr(cluster, "route_listen")?;
    let route_advertise = get_string(cluster, "route_advertise")?.map(str::to_string);
    let route_tls = get_internal_tls_config(cluster, "route_tls")?;
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
    let role = match get_string(cluster, "role")?.as_deref() {
        None | Some("combined") => crate::config::ClusterRole::Combined,
        Some("controller") => crate::config::ClusterRole::Controller,
        Some("broker") => crate::config::ClusterRole::Broker,
        Some(_) => {
            return Err(BrokerError::msg(
                "config field cluster.role must be combined, controller, or broker",
            ));
        }
    };
    let controller_voters = get_u64_array(cluster, "controller_voters")?
        .unwrap_or_else(|| nodes.iter().map(|node| node.node_id).collect());
    let config = ClusterConfig {
        enabled,
        role,
        node_id,
        auth_token,
        raft_listen,
        raft_tls,
        allow_insecure_internal_transports,
        route_listen,
        route_advertise,
        route_tls,
        routes,
        route_reconnect_ms,
        raft_dir,
        bootstrap,
        nodes,
        controller_voters,
        election_timeout_min_ms,
        election_timeout_max_ms,
        heartbeat_interval_ms,
        snapshot_threshold,
    };
    config.validate()?;
    Ok(Some(config))
}

fn get_u64_array(value: &serde_json::Value, key: &str) -> Result<Option<Vec<u64>>> {
    match value.get(key) {
        Some(serde_json::Value::Array(values)) => values
            .iter()
            .map(|value| {
                value.as_u64().ok_or_else(|| {
                    BrokerError::msg(format!(
                        "config field cluster.{key} must contain only integers"
                    ))
                })
            })
            .collect::<Result<Vec<_>>>()
            .map(Some),
        Some(serde_json::Value::Null) | None => Ok(None),
        Some(_) => Err(BrokerError::msg(format!(
            "config field cluster.{key} must be an array"
        ))),
    }
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
        let route_addr = get_string(node, "route_addr")?.map(str::to_string);
        let tls_server_name = get_string(node, "tls_server_name")?.map(str::to_string);
        let tls_cert_files = get_string_array(node, "tls_cert_files")?
            .into_iter()
            .map(PathBuf::from)
            .collect();
        out.push(ClusterNodeConfig {
            node_id,
            raft_addr,
            client_addr,
            route_addr,
            tls_server_name,
            tls_cert_files,
        });
    }
    Ok(out)
}

fn get_internal_tls_config(
    cluster: &serde_json::Value,
    field: &str,
) -> Result<Option<InternalTlsConfig>> {
    let Some(tls) = cluster.get(field) else {
        return Ok(None);
    };
    if tls.is_null() {
        return Ok(None);
    }
    let serde_json::Value::Object(_) = tls else {
        return Err(BrokerError::msg(format!(
            "config field cluster.{field} must be an object"
        )));
    };
    let required_path = |key| {
        get_string(tls, key)?.map(PathBuf::from).ok_or_else(|| {
            BrokerError::msg(format!("config field cluster.{field}.{key} is required"))
        })
    };
    let config = InternalTlsConfig {
        cert_file: required_path("cert_file")?,
        key_file: required_path("key_file")?,
        ca_cert_file: required_path("ca_cert_file")?,
        handshake_timeout_ms: get_u64(tls, "handshake_timeout_ms")?.unwrap_or(2_000),
    };
    config.validate(field)?;
    Ok(Some(config))
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
