use crate::error::{BrokerError, Result, ResultExt};
use crate::stream::StreamCatalog;
use std::{
    collections::HashMap,
    ffi::OsString,
    net::SocketAddr,
    path::{Path, PathBuf},
    time::Duration,
};
pub const DEFAULT_MAX_ACK_TIMEOUT_MS: u64 = 300_000;
pub const DEFAULT_MAX_IN_FLIGHT: usize = 4_096;
pub const DEFAULT_MAX_FETCH_MESSAGES: usize = 1_024;
pub const DEFAULT_MAX_FETCH_BYTES: usize = 16 * 1_048_576;
pub const DEFAULT_MAX_ENCODED_BATCH_BYTES: usize = 20 * 1_048_576;
#[derive(Debug, Clone)]
pub struct Config {
    pub production: bool,
    pub allow_insecure_development: bool,
    pub listen: SocketAddr,
    pub websocket: Option<WebSocketConfig>,
    pub http_listen: Option<SocketAddr>,
    pub admin_token: Option<String>,
    pub admin_tls: Option<TlsConfig>,
    pub quotas: ResourceQuotaConfig,
    pub wal_dir: PathBuf,
    pub encryption_key_dir: Option<PathBuf>,
    pub encryption_active_key_version: u32,
    pub wal_segment_bytes: u64,
    pub fsync_interval_ms: u64,
    pub max_payload: usize,
    pub max_control_line: usize,
    pub max_ack_timeout_ms: u64,
    pub max_in_flight: usize,
    pub max_fetch_messages: usize,
    pub max_fetch_bytes: usize,
    pub max_encoded_batch_bytes: usize,
    pub verbose: bool,
    pub tls: Option<TlsConfig>,
    pub auth: AuthConfig,
    pub cluster: Option<ClusterConfig>,
    pub streams: StreamCatalog,
}
#[derive(Debug, Clone)]
pub struct TlsConfig {
    pub cert_file: PathBuf,
    pub key_file: PathBuf,
    pub handshake_timeout_ms: u64,
}
#[derive(Debug, Clone)]
pub struct WebSocketConfig {
    pub listen: SocketAddr,
    pub tls: Option<TlsConfig>,
    pub allowed_origins: Vec<String>,
}
#[derive(Debug, Clone)]
pub struct InternalTlsConfig {
    pub cert_file: PathBuf,
    pub key_file: PathBuf,
    pub ca_cert_file: PathBuf,
    pub handshake_timeout_ms: u64,
}
#[derive(Debug, Clone)]
pub struct ResourceQuotaConfig {
    pub max_connections: usize,
    pub max_connections_per_identity: usize,
    pub max_transient_subscriptions: usize,
    pub max_transient_subscriptions_per_identity: usize,
    pub max_durable_consumers: usize,
    pub max_durable_consumers_per_identity: usize,
    pub max_outbound_bytes_per_connection: usize,
    pub max_http_connections: usize,
    pub max_raft_connections: usize,
    pub max_route_connections: usize,
    pub client_idle_timeout_ms: u64,
    pub http_header_timeout_ms: u64,
}
#[derive(Debug, Clone, Default)]
pub struct AuthConfig {
    pub enabled: bool,
    pub clients: HashMap<String, AuthClientConfig>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthClientConfig {
    pub public_key: String,
    pub permissions: Option<AuthPermissions>,
    pub tenant: String,
    pub namespace: String,
    pub expires_at_ms: Option<u64>,
    pub external_subject: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthPermissions {
    pub publish: Option<Vec<String>>,
    pub subscribe: Option<Vec<String>>,
}
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    pub enabled: bool,
    pub node_id: u64,
    pub auth_token: String,
    pub raft_listen: SocketAddr,
    pub raft_tls: Option<InternalTlsConfig>,
    pub allow_insecure_internal_transports: bool,
    pub route_listen: Option<SocketAddr>,
    pub route_advertise: Option<String>,
    pub route_tls: Option<InternalTlsConfig>,
    pub routes: Vec<String>,
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
    pub raft_addr: String,
    pub client_addr: String,
    pub route_addr: Option<String>,
    pub tls_server_name: Option<String>,
    pub tls_cert_files: Vec<PathBuf>,
}
impl Config {
    pub fn help_requested() -> bool {
        std::env::args_os()
            .nth(1)
            .is_some_and(|arg| Self::is_help_arg(&arg))
    }

    pub fn usage() -> &'static str {
        "Usage: morrow-server [OPTIONS] [CONFIG_PATH]\n\nOptions:\n    -h, --help             Print this help message\n    --check-config PATH   Validate and print the effective configuration without starting\n"
    }

    fn is_help_arg(arg: &std::ffi::OsStr) -> bool {
        arg == "-h" || arg == "--help"
    }

    pub fn load_from_args() -> Result<Self> {
        Self::load_from_args_iter(std::env::args_os())
    }

    pub fn check_config_from_args() -> Result<Option<String>> {
        let mut args = std::env::args_os().skip(1);
        while let Some(arg) = args.next() {
            if arg == "--check-config" {
                let path = args.next().ok_or_else(|| {
                    BrokerError::msg("--check-config requires a config file path")
                })?;
                if args.next().is_some() {
                    return Err(BrokerError::msg("--check-config accepts exactly one path"));
                }
                return Ok(Some(path.to_string_lossy().into_owned()));
            }
        }
        Ok(None)
    }

    fn load_from_args_iter<I>(args: I) -> Result<Self>
    where
        I: IntoIterator<Item = OsString>,
    {
        let mut args = args.into_iter();
        let _program = args.next();
        let path = match (args.next(), args.next()) {
            (Some(path), None) => PathBuf::from(path),
            (None, None) => return Self::from_json(&serde_json::json!({})),
            _ => {
                return Err(BrokerError::msg(format!(
                    "{}\n\nexpected at most one config file path",
                    Self::usage()
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
        Self::from_json_with_options(value, true)
    }

    pub fn check_file(path: impl AsRef<Path>) -> Result<String> {
        let path = path.as_ref();
        let contents = std::fs::read_to_string(path)
            .with_context(|| format!("reading config file {}", path.display()))?;
        let value: serde_json::Value = serde_json::from_str(&contents)
            .with_context(|| format!("parsing JSON config file {}", path.display()))?;
        let config = Self::from_json_with_options(&value, false)?;
        serde_json::to_string_pretty(&config.redacted_json())
            .map_err(|err| BrokerError::with_source("rendering effective configuration", err))
    }

    fn from_json_with_options(value: &serde_json::Value, create_dirs: bool) -> Result<Self> {
        crate::broker_ensure!(value.is_object(), "config file must contain a JSON object");
        validation::reject_unknown_fields(value)?;
        let production = get_bool(value, "production")?.unwrap_or(false);
        let allow_insecure_development =
            get_bool(value, "allow_insecure_development")?.unwrap_or(false);
        let listen = get_string(value, "listen")?
            .unwrap_or("127.0.0.1:4222")
            .parse()
            .context("config field listen must be a socket address")?;
        let websocket = get_websocket_config(value)?;
        let http_listen = get_http_listen(value)?;
        let admin_token = get_secret(value, "admin_token", "admin_token_file")?;
        let admin_tls = get_named_tls_config(value, "admin_tls")?;
        let quotas = get_resource_quotas(value)?;
        let wal_dir = PathBuf::from(get_string(value, "wal_dir")?.unwrap_or("./morrow-wal"));
        let encryption_key_dir = get_string(value, "encryption_key_dir")?.map(PathBuf::from);
        let encryption_active_key_version = get_u64(value, "encryption_active_key_version")?
            .unwrap_or(1)
            .try_into()
            .context("config field encryption_active_key_version is too large")?;
        let wal_segment_bytes =
            get_u64(value, "wal_segment_bytes")?.unwrap_or(crate::wal::DEFAULT_WAL_SEGMENT_BYTES);
        let fsync_interval_ms = get_u64(value, "fsync_interval_ms")?.unwrap_or(5);
        let max_payload = get_u64(value, "max_payload")?.unwrap_or(1_048_576);
        let max_control_line = get_u64(value, "max_control_line")?.unwrap_or(8192);
        let max_ack_timeout_ms =
            get_u64(value, "max_ack_timeout_ms")?.unwrap_or(DEFAULT_MAX_ACK_TIMEOUT_MS);
        let max_in_flight = get_bounded_usize(value, "max_in_flight", DEFAULT_MAX_IN_FLIGHT)?;
        let max_fetch_messages =
            get_bounded_usize(value, "max_fetch_messages", DEFAULT_MAX_FETCH_MESSAGES)?;
        let max_fetch_bytes = get_bounded_usize(value, "max_fetch_bytes", DEFAULT_MAX_FETCH_BYTES)?;
        let max_encoded_batch_bytes = get_bounded_usize(
            value,
            "max_encoded_batch_bytes",
            DEFAULT_MAX_ENCODED_BATCH_BYTES,
        )?;
        let verbose = get_bool(value, "verbose")?.unwrap_or(false);
        let tls = get_tls_config(value)?;
        let auth = get_auth_config(value)?;
        let cluster = get_cluster_config(value)?;
        let streams = get_streams_config(value)?;

        let config = Self {
            production,
            allow_insecure_development,
            listen,
            websocket,
            http_listen,
            admin_token,
            admin_tls,
            quotas,
            wal_dir,
            encryption_key_dir,
            encryption_active_key_version,
            wal_segment_bytes,
            fsync_interval_ms,
            max_payload: max_payload
                .try_into()
                .context("config field max_payload is too large")?,
            max_control_line: max_control_line
                .try_into()
                .context("config field max_control_line is too large")?,
            max_ack_timeout_ms,
            max_in_flight,
            max_fetch_messages,
            max_fetch_bytes,
            max_encoded_batch_bytes,
            verbose,
            tls,
            auth,
            cluster,
            streams,
        };
        config.validate_impl(create_dirs)?;
        Ok(config)
    }

    pub fn fsync_interval(&self) -> Duration {
        Duration::from_millis(self.fsync_interval_ms)
    }

    pub fn storage_encryption(&self) -> Result<Option<std::sync::Arc<crate::encryption::KeyRing>>> {
        let Some(directory) = &self.encryption_key_dir else {
            return Ok(None);
        };
        let provider = std::sync::Arc::new(crate::encryption::FileKeyProvider::new(directory));
        Ok(Some(std::sync::Arc::new(crate::encryption::KeyRing::new(
            provider,
            crate::encryption::KeyVersion::new(self.encryption_active_key_version),
        )?)))
    }

    pub fn validate(&self) -> Result<()> {
        self.validate_impl(true)
    }

    fn validate_impl(&self, create_dirs: bool) -> Result<()> {
        crate::broker_ensure!(
            self.max_payload > 0,
            "config field max_payload must be greater than zero"
        );
        crate::broker_ensure!(
            self.wal_segment_bytes > 0,
            "config field wal_segment_bytes must be greater than zero"
        );
        crate::broker_ensure!(
            self.max_control_line > 0,
            "config field max_control_line must be greater than zero"
        );
        for (name, value) in [
            ("max_in_flight", self.max_in_flight),
            ("max_fetch_messages", self.max_fetch_messages),
            ("max_fetch_bytes", self.max_fetch_bytes),
            ("max_encoded_batch_bytes", self.max_encoded_batch_bytes),
        ] {
            crate::broker_ensure!(value > 0, "config field {name} must be greater than zero");
            crate::broker_ensure!(
                u32::try_from(value).is_ok(),
                "config field {name} must fit in 32 bits"
            );
        }
        crate::broker_ensure!(
            self.max_ack_timeout_ms > 0,
            "config field max_ack_timeout_ms must be greater than zero"
        );
        crate::broker_ensure!(
            self.max_in_flight >= crate::broker::DEFAULT_MAX_IN_FLIGHT,
            "config field max_in_flight must allow the default client value"
        );
        crate::broker_ensure!(
            self.max_encoded_batch_bytes >= self.max_payload,
            "config field max_encoded_batch_bytes must be at least max_payload"
        );
        crate::broker_ensure!(
            self.fsync_interval_ms > 0,
            "config field fsync_interval_ms must be greater than zero"
        );
        if self.production && !self.allow_insecure_development {
            crate::broker_ensure!(
                self.tls.is_some() || self.listen.ip().is_loopback(),
                "production config requires TLS for non-loopback client listener"
            );
            crate::broker_ensure!(
                self.auth.enabled || self.listen.ip().is_loopback(),
                "production config requires authentication for non-loopback client listener"
            );
            if let Some(websocket) = &self.websocket {
                crate::broker_ensure!(
                    websocket.tls.is_some() || websocket.listen.ip().is_loopback(),
                    "production config requires TLS for non-loopback WebSocket listener"
                );
            }
            if let Some(http_listen) = self.http_listen {
                crate::broker_ensure!(
                    self.admin_tls.is_some() || http_listen.ip().is_loopback(),
                    "production config requires admin TLS for non-loopback admin listener"
                );
            }
        }
        if self.http_listen.is_some() {
            crate::broker_ensure!(
                self.admin_token
                    .as_deref()
                    .is_some_and(|token| !token.is_empty()),
                "config field admin_token is required when http_listen is set"
            );
        }
        if let Some(websocket) = &self.websocket {
            crate::broker_ensure!(
                websocket.listen != self.listen,
                "config field websocket.listen must differ from listen"
            );
            crate::broker_ensure!(
                websocket
                    .allowed_origins
                    .iter()
                    .all(|origin| !origin.trim().is_empty()),
                "config field websocket.allowed_origins must not contain empty origins"
            );
            if let Some(tls) = &websocket.tls {
                tls.validate_named("websocket.tls")?;
            }
        }
        if let Some(tls) = &self.admin_tls {
            crate::broker_ensure!(
                self.http_listen.is_some(),
                "config field admin_tls requires http_listen"
            );
            tls.validate_named("admin_tls")?;
        }
        if let Some(tls) = &self.tls {
            tls.validate()?;
        }
        if let Some(cluster) = &self.cluster {
            cluster.validate()?;
            if create_dirs {
                std::fs::create_dir_all(&cluster.raft_dir).with_context(|| {
                    format!("creating Raft directory {}", cluster.raft_dir.display())
                })?;
            }
        }
        if self.auth.enabled {
            crate::broker_ensure!(
                !self.auth.clients.is_empty(),
                "config field auth.clients must contain at least one client when auth is enabled"
            );
        }
        if create_dirs {
            std::fs::create_dir_all(&self.wal_dir)
                .with_context(|| format!("creating WAL directory {}", self.wal_dir.display()))?;
        }
        Ok(())
    }

    fn redacted_json(&self) -> serde_json::Value {
        serde_json::json!({
            "production": self.production,
            "allow_insecure_development": self.allow_insecure_development,
            "listen": self.listen.to_string(),
            "websocket": self.websocket.as_ref().map(|value| value.listen.to_string()),
            "http_listen": self.http_listen.map(|value| value.to_string()),
            "tls": self.tls.is_some(),
            "admin_tls": self.admin_tls.is_some(),
            "authentication_enabled": self.auth.enabled,
            "wal_dir": self.wal_dir,
            "cluster_enabled": self.cluster.is_some(),
            "quotas": {
                "max_connections": self.quotas.max_connections,
                "max_outbound_bytes_per_connection": self.quotas.max_outbound_bytes_per_connection
            }
        })
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
                let public_key =
                    get_secret(value, "public_key", "public_key_file")?.ok_or_else(|| {
                        BrokerError::msg(
                            "config field auth.clients[].public_key or public_key_file is required",
                        )
                    })?;
                let permissions = get_auth_permissions(value)?;
                let tenant = get_string(value, "tenant")?
                    .unwrap_or("default")
                    .to_string();
                let namespace = get_string(value, "namespace")?
                    .unwrap_or("default")
                    .to_string();
                let expires_at_ms = value
                    .get("expires_at_ms")
                    .and_then(serde_json::Value::as_u64);
                if value.get("expires_at_ms").is_some() && expires_at_ms.is_none() {
                    return Err(BrokerError::msg(
                        "config field auth.clients[].expires_at_ms must be an unsigned integer",
                    ));
                }
                let external_subject = get_string(value, "external_subject")?.map(str::to_string);
                if let Some(subject) = &external_subject {
                    crate::broker_ensure!(
                        !subject.is_empty() && !subject.chars().any(char::is_whitespace),
                        "config field auth.clients[].external_subject must be non-empty"
                    );
                }
                crate::tenancy::TenantId::new(tenant.clone())?;
                crate::tenancy::NamespaceId::new(namespace.clone())?;
                clients.insert(
                    client_id.to_string(),
                    AuthClientConfig {
                        public_key: public_key.to_ascii_lowercase(),
                        permissions,
                        tenant,
                        namespace,
                        expires_at_ms,
                        external_subject,
                    },
                );
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
fn get_auth_permissions(value: &serde_json::Value) -> Result<Option<AuthPermissions>> {
    let Some(permissions) = value.get("permissions") else {
        return Ok(None);
    };
    let serde_json::Value::Object(_) = permissions else {
        return Err(BrokerError::msg(
            "config field auth.clients[].permissions must be an object",
        ));
    };
    let publish = get_permission_patterns(permissions, "publish")?;
    let subscribe = get_permission_patterns(permissions, "subscribe")?;
    crate::broker_ensure!(
        publish.is_some() || subscribe.is_some(),
        "config field auth.clients[].permissions must contain publish or subscribe"
    );
    Ok(Some(AuthPermissions { publish, subscribe }))
}
fn get_permission_patterns(
    permissions: &serde_json::Value,
    key: &str,
) -> Result<Option<Vec<String>>> {
    let Some(value) = permissions.get(key) else {
        return Ok(None);
    };
    let serde_json::Value::Array(values) = value else {
        return Err(BrokerError::msg(format!(
            "config field auth.clients[].permissions.{key} must be an array"
        )));
    };
    crate::broker_ensure!(
        !values.is_empty(),
        "config field auth.clients[].permissions.{key} must not be empty"
    );
    let mut patterns = Vec::with_capacity(values.len());
    for value in values {
        let serde_json::Value::String(pattern) = value else {
            return Err(BrokerError::msg(format!(
                "config field auth.clients[].permissions.{key} must contain only strings"
            )));
        };
        crate::broker_ensure!(
            protocol::subject::validate_subscription(pattern),
            "config field auth.clients[].permissions.{key} contains invalid subject pattern"
        );
        patterns.push(pattern.to_string());
    }
    Ok(Some(patterns))
}
impl TlsConfig {
    fn validate(&self) -> Result<()> {
        self.validate_named("tls")
    }

    fn validate_named(&self, field: &str) -> Result<()> {
        crate::broker_ensure!(
            self.handshake_timeout_ms > 0,
            "config field {field}.handshake_timeout_ms must be greater than zero"
        );
        crate::broker_ensure!(
            self.cert_file.is_file(),
            "config field {field}.cert_file must point to an existing file"
        );
        crate::broker_ensure!(
            self.key_file.is_file(),
            "config field {field}.key_file must point to an existing file"
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

fn get_bounded_usize(value: &serde_json::Value, key: &str, default: usize) -> Result<usize> {
    get_u64(value, key)?
        .map(|value| {
            usize::try_from(value).with_context(|| format!("config field {key} is too large"))
        })
        .transpose()
        .map(|value| value.unwrap_or(default))
}

fn get_secret(
    value: &serde_json::Value,
    inline_key: &str,
    file_key: &str,
) -> Result<Option<String>> {
    let inline = get_string(value, inline_key)?;
    let file = get_string(value, file_key)?;
    crate::broker_ensure!(
        inline.is_none() || file.is_none(),
        "config fields {inline_key} and {file_key} are mutually exclusive"
    );
    let secret = match (inline, file) {
        (Some(secret), None) => Some(secret.to_string()),
        (None, Some(path)) => Some(
            std::fs::read_to_string(path)
                .with_context(|| format!("reading secret file for config field {file_key}"))?
                .trim()
                .to_string(),
        ),
        (None, None) => None,
        (Some(_), Some(_)) => unreachable!(),
    };
    if let Some(secret) = &secret {
        crate::broker_ensure!(
            !secret.is_empty(),
            "config field {inline_key} must not be empty"
        );
    }
    Ok(secret)
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
    get_named_tls_config(value, "tls")
}

fn get_named_tls_config(value: &serde_json::Value, field: &str) -> Result<Option<TlsConfig>> {
    let Some(tls) = value.get(field) else {
        return Ok(None);
    };
    if tls.is_null() {
        return Ok(None);
    }
    let serde_json::Value::Object(_) = tls else {
        return Err(BrokerError::msg(format!(
            "config field {field} must be an object"
        )));
    };
    let cert_file = get_string(tls, "cert_file")?
        .ok_or_else(|| BrokerError::msg(format!("config field {field}.cert_file is required")))?;
    let key_file = get_string(tls, "key_file")?
        .ok_or_else(|| BrokerError::msg(format!("config field {field}.key_file is required")))?;
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

fn get_websocket_config(value: &serde_json::Value) -> Result<Option<WebSocketConfig>> {
    let Some(websocket) = value.get("websocket") else {
        return Ok(None);
    };
    if websocket.is_null() {
        return Ok(None);
    }
    let serde_json::Value::Object(_) = websocket else {
        return Err(BrokerError::msg(
            "config field websocket must be an object or null",
        ));
    };
    let listen = get_string(websocket, "listen")?
        .ok_or_else(|| BrokerError::msg("config field websocket.listen is required"))?
        .parse()
        .context("config field websocket.listen must be a socket address")?;
    let allowed_origins = match websocket.get("allowed_origins") {
        None | Some(serde_json::Value::Null) => Vec::new(),
        Some(serde_json::Value::Array(values)) => values
            .iter()
            .map(|value| match value {
                serde_json::Value::String(origin) if !origin.trim().is_empty() => {
                    Ok(origin.clone())
                }
                _ => Err(BrokerError::msg(
                    "config field websocket.allowed_origins must contain only non-empty strings",
                )),
            })
            .collect::<Result<Vec<_>>>()?,
        Some(_) => {
            return Err(BrokerError::msg(
                "config field websocket.allowed_origins must be an array or null",
            ));
        }
    };
    Ok(Some(WebSocketConfig {
        listen,
        tls: get_named_tls_config(websocket, "tls")?,
        allowed_origins,
    }))
}
#[path = "config/cluster_config.rs"]
mod cluster_config;
use cluster_config::get_cluster_config;

#[path = "config/quota_config.rs"]
mod quota_config;
use quota_config::get_resource_quotas;

impl From<OsString> for BrokerError {
    fn from(value: OsString) -> Self {
        BrokerError::msg(format!("invalid argument {:?}", value))
    }
}

#[path = "config/stream_config.rs"]
mod stream_config;
use stream_config::get_streams_config;

#[path = "config/validation.rs"]
mod validation;

#[cfg(test)]
mod tests;
