use std::{
    collections::HashMap,
    ffi::OsString,
    net::SocketAddr,
    path::{Path, PathBuf},
    time::Duration,
};

use crate::error::{BrokerError, Result, ResultExt};

const DEFAULT_CONFIG_PATH: &str = "broker.json";

#[derive(Debug, Clone)]
pub struct Config {
    pub listen: SocketAddr,
    pub wal_dir: PathBuf,
    pub fsync_interval_ms: u64,
    pub max_payload: usize,
    pub verbose: bool,
    pub tls: Option<TlsConfig>,
    pub auth: AuthConfig,
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
        let wal_dir = PathBuf::from(get_string(value, "wal_dir")?.unwrap_or("./broker-wal"));
        let fsync_interval_ms = get_u64(value, "fsync_interval_ms")?.unwrap_or(5);
        let max_payload = get_u64(value, "max_payload")?.unwrap_or(1_048_576);
        let verbose = get_bool(value, "verbose")?.unwrap_or(false);
        let tls = get_tls_config(value)?;
        let auth = get_auth_config(value)?;

        let config = Self {
            listen,
            wal_dir,
            fsync_interval_ms,
            max_payload: max_payload
                .try_into()
                .context("config field max_payload is too large")?,
            verbose,
            tls,
            auth,
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
        std::fs::create_dir_all(&self.wal_dir)
            .with_context(|| format!("creating WAL directory {}", self.wal_dir.display()))?;
        Ok(())
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

impl From<OsString> for BrokerError {
    fn from(value: OsString) -> Self {
        BrokerError::msg(format!("invalid argument {:?}", value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_json_config() {
        let value = serde_json::json!({
            "listen": "127.0.0.1:4223",
            "wal_dir": "./target/test-wal-config",
            "fsync_interval_ms": 10,
            "max_payload": 2048,
            "verbose": true,
            "tls": null,
            "auth": null
        });

        let config = Config::from_json(&value).unwrap();
        assert_eq!(config.listen, "127.0.0.1:4223".parse().unwrap());
        assert_eq!(config.wal_dir, PathBuf::from("./target/test-wal-config"));
        assert_eq!(config.fsync_interval_ms, 10);
        assert_eq!(config.max_payload, 2048);
        assert!(config.verbose);
        assert!(config.tls.is_none());
        assert!(!config.auth.enabled);
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
}
