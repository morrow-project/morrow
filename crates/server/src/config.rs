use crate::error::{BrokerError, Result, ResultExt};
use std::{
    collections::HashMap,
    ffi::OsString,
    net::SocketAddr,
    path::{Path, PathBuf},
    time::Duration,
};
const DEFAULT_CONFIG_PATH: &str = "broker.json";
#[derive(Debug, Clone)]
pub struct Config {
    pub listen: SocketAddr,
    pub http_listen: Option<SocketAddr>,
    pub wal_dir: PathBuf,
    pub fsync_interval_ms: u64,
    pub max_payload: usize,
    pub verbose: bool,
    pub tls: Option<TlsConfig>,
    pub auth: AuthConfig,
    pub cluster: Option<ClusterConfig>,
}
#[derive(Debug, Clone)]
pub struct TlsConfig {
    pub cert_file: PathBuf,
    pub key_file: PathBuf,
    pub handshake_timeout_ms: u64,
}
#[derive(Debug, Clone, Default)]
pub struct AuthConfig {
    pub enabled: bool,
    pub clients: HashMap<String, String>,
}
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    pub enabled: bool,
    pub node_id: u64,
    pub raft_listen: SocketAddr,
    pub route_listen: Option<SocketAddr>,
    pub routes: Vec<SocketAddr>,
    pub route_reconnect_ms: u64,
    pub raft_dir: PathBuf,
    pub bootstrap: bool,
    pub nodes: Vec<ClusterNodeConfig>,
    pub election_timeout_min_ms: u64,
    pub election_timeout_max_ms: u64,
    pub heartbeat_interval_ms: u64,
    pub snapshot_threshold: u64,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterNodeConfig {
    pub node_id: u64,
    pub raft_addr: SocketAddr,
    pub client_addr: SocketAddr,
}
impl Config {
    pub fn load_from_args() -> Result<Self> {
        let mut args = std::env::args_os();
        let _program = args.next();
        let path = match (args.next(), args.next()) {
            (Some(path), None) => PathBuf::from(path),
            (None, None) => PathBuf::from(DEFAULT_CONFIG_PATH),
            _ => {
                return Err(BrokerError::msg(format!(
                    "usage: broker [config-path]\nexpected at most one config file path"
                )));
            }
        };
        Self::load(path)
    }

    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let contents = std::fs::read_to_string(path)
            .with_context(|| format!("reading config file {}", path.display()))?;
        let value: serde_json::Value = serde_json::from_str(&contents)
            .with_context(|| format!("parsing JSON config file {}", path.display()))?;
        Self::from_json(&value)
    }

    pub fn from_json(value: &serde_json::Value) -> Result<Self> {
        crate::broker_ensure!(value.is_object(), "config file must contain a JSON object");
        let listen = get_string(value, "listen")?
            .unwrap_or("127.0.0.1:4222")
            .parse()
            .context("config field listen must be a socket address")?;
        let http_listen = get_http_listen(value)?;
        let wal_dir = PathBuf::from(get_string(value, "wal_dir")?.unwrap_or("./broker-wal"));
        let fsync_interval_ms = get_u64(value, "fsync_interval_ms")?.unwrap_or(5);
        let max_payload = get_u64(value, "max_payload")?.unwrap_or(1_048_576);
        let verbose = get_bool(value, "verbose")?.unwrap_or(false);
        let tls = get_tls_config(value)?;
        let auth = get_auth_config(value)?;
        let cluster = get_cluster_config(value)?;

        let config = Self {
            listen,
            http_listen,
            wal_dir,
            fsync_interval_ms,
            max_payload: max_payload
                .try_into()
                .context("config field max_payload is too large")?,
            verbose,
            tls,
            auth,
            cluster,
        };
        config.validate()?;
        Ok(config)
    }

    pub fn fsync_interval(&self) -> Duration {
        Duration::from_millis(self.fsync_interval_ms)
    }

    pub fn validate(&self) -> Result<()> {
        crate::broker_ensure!(
            self.max_payload > 0,
            "config field max_payload must be greater than zero"
        );
        crate::broker_ensure!(
            self.fsync_interval_ms > 0,
            "config field fsync_interval_ms must be greater than zero"
        );
        if let Some(tls) = &self.tls {
            tls.validate()?;
        }
        if let Some(cluster) = &self.cluster {
            cluster.validate()?;
            std::fs::create_dir_all(&cluster.raft_dir).with_context(|| {
                format!("creating Raft directory {}", cluster.raft_dir.display())
            })?;
        }
        std::fs::create_dir_all(&self.wal_dir)
            .with_context(|| format!("creating WAL directory {}", self.wal_dir.display()))?;
        Ok(())
    }
}
impl ClusterConfig {
    fn validate(&self) -> Result<()> {
        crate::broker_ensure!(
            self.enabled,
            "cluster.enabled must be true when cluster is present"
        );
        crate::broker_ensure!(
            self.node_id > 0,
            "cluster.node_id must be greater than zero"
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

        let mut ids = std::collections::HashSet::new();
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
        }
        crate::broker_ensure!(has_self, "cluster.node_id must be present in cluster.nodes");
        Ok(())
    }

    pub fn self_node(&self) -> Option<&ClusterNodeConfig> {
        self.nodes.iter().find(|node| node.node_id == self.node_id)
    }
}
fn get_auth_config(value: &serde_json::Value) -> Result<AuthConfig> {
    let Some(auth) = value.get("auth") else {
        return Ok(AuthConfig::default());
    };
    if auth.is_null() {
        return Ok(AuthConfig::default());
    }
    let serde_json::Value::Object(_) = auth else {
        return Err(BrokerError::msg("config field auth must be an object"));
    };
    let enabled = get_bool(auth, "enabled")?.unwrap_or(false);
    let mut clients = HashMap::new();
    match auth.get("clients") {
        Some(serde_json::Value::Array(values)) => {
            for value in values {
                let serde_json::Value::Object(_) = value else {
                    return Err(BrokerError::msg(
                        "config field auth.clients must contain only objects",
                    ));
                };
                let client_id = get_string(value, "client_id")?.ok_or_else(|| {
                    BrokerError::msg("config field auth.clients[].client_id is required")
                })?;
                crate::broker_ensure!(
                    !client_id.is_empty()
                        && !client_id.contains('.')
                        && !client_id.chars().any(char::is_whitespace)
                        && !client_id.starts_with('_'),
                    "config field auth.clients[].client_id is invalid"
                );
                let public_key = get_string(value, "public_key")?.ok_or_else(|| {
                    BrokerError::msg("config field auth.clients[].public_key is required")
                })?;
                clients.insert(client_id.to_string(), public_key.to_ascii_lowercase());
            }
        }
        Some(_) => {
            return Err(BrokerError::msg(
                "config field auth.clients must be an array",
            ));
        }
        None => {}
    }
    Ok(AuthConfig { enabled, clients })
}
impl TlsConfig {
    fn validate(&self) -> Result<()> {
        crate::broker_ensure!(
            self.handshake_timeout_ms > 0,
            "config field tls.handshake_timeout_ms must be greater than zero"
        );
        crate::broker_ensure!(
            self.cert_file.is_file(),
            "config field tls.cert_file must point to an existing file"
        );
        crate::broker_ensure!(
            self.key_file.is_file(),
            "config field tls.key_file must point to an existing file"
        );
        Ok(())
    }
}
fn get_string<'a>(value: &'a serde_json::Value, key: &str) -> Result<Option<&'a str>> {
    match value.get(key) {
        Some(serde_json::Value::String(value)) => Ok(Some(value)),
        Some(_) => Err(BrokerError::msg(format!(
            "config field {key} must be a string"
        ))),
        None => Ok(None),
    }
}
fn get_u64(value: &serde_json::Value, key: &str) -> Result<Option<u64>> {
    match value.get(key) {
        Some(serde_json::Value::Number(value)) => value
            .as_u64()
            .ok_or_else(|| BrokerError::msg(format!("config field {key} must be a u64")))
            .map(Some),
        Some(_) => Err(BrokerError::msg(format!(
            "config field {key} must be an unsigned integer"
        ))),
        None => Ok(None),
    }
}
fn get_bool(value: &serde_json::Value, key: &str) -> Result<Option<bool>> {
    match value.get(key) {
        Some(serde_json::Value::Bool(value)) => Ok(Some(*value)),
        Some(_) => Err(BrokerError::msg(format!(
            "config field {key} must be a boolean"
        ))),
        None => Ok(None),
    }
}
fn get_tls_config(value: &serde_json::Value) -> Result<Option<TlsConfig>> {
    let Some(tls) = value.get("tls") else {
        return Ok(None);
    };
    if tls.is_null() {
        return Ok(None);
    }
    let serde_json::Value::Object(_) = tls else {
        return Err(BrokerError::msg("config field tls must be an object"));
    };
    let cert_file = get_string(tls, "cert_file")?
        .ok_or_else(|| BrokerError::msg("config field tls.cert_file is required"))?;
    let key_file = get_string(tls, "key_file")?
        .ok_or_else(|| BrokerError::msg("config field tls.key_file is required"))?;
    let handshake_timeout_ms = get_u64(tls, "handshake_timeout_ms")?.unwrap_or(2_000);
    Ok(Some(TlsConfig {
        cert_file: PathBuf::from(cert_file),
        key_file: PathBuf::from(key_file),
        handshake_timeout_ms,
    }))
}
fn get_http_listen(value: &serde_json::Value) -> Result<Option<SocketAddr>> {
    match value.get("http_listen") {
        Some(serde_json::Value::String(value)) => value
            .parse()
            .context("config field http_listen must be a socket address")
            .map(Some),
        Some(serde_json::Value::Null) | None => Ok(None),
        Some(_) => Err(BrokerError::msg(
            "config field http_listen must be a string or null",
        )),
    }
}
fn get_cluster_config(value: &serde_json::Value) -> Result<Option<ClusterConfig>> {
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
    let raft_listen = get_string(cluster, "raft_listen")?
        .ok_or_else(|| BrokerError::msg("config field cluster.raft_listen is required"))?
        .parse()
        .context("config field cluster.raft_listen must be a socket address")?;
    let route_listen = get_optional_socket_addr(cluster, "route_listen")?;
    let routes = get_socket_addr_array(cluster, "routes")?;
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
            .parse()
            .context("config field cluster.nodes[].raft_addr must be a socket address")?;
        let client_addr = get_string(node, "client_addr")?
            .ok_or_else(|| {
                BrokerError::msg("config field cluster.nodes[].client_addr is required")
            })?
            .parse()
            .context("config field cluster.nodes[].client_addr must be a socket address")?;
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
fn get_socket_addr_array(value: &serde_json::Value, key: &str) -> Result<Vec<SocketAddr>> {
    match value.get(key) {
        Some(serde_json::Value::Array(values)) => values
            .iter()
            .map(|value| {
                let serde_json::Value::String(value) = value else {
                    return Err(BrokerError::msg(format!(
                        "config field cluster.{key} must contain only strings"
                    )));
                };
                value.parse().with_context(|| {
                    format!("config field cluster.{key} must contain socket addresses")
                })
            })
            .collect(),
        Some(serde_json::Value::Null) | None => Ok(Vec::new()),
        Some(_) => Err(BrokerError::msg(format!(
            "config field cluster.{key} must be an array"
        ))),
    }
}
impl From<OsString> for BrokerError {
    fn from(value: OsString) -> Self {
        BrokerError::msg(format!("invalid argument {:?}", value))
    }
}

#[cfg(test)]
mod tests;
