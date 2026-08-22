use client::{Client, ClientAuth, ClientOptions, ClientTlsOptions};
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, path::PathBuf};

pub const CONNECTOR_DESCRIPTOR_VERSION: u32 = 1;

#[derive(Debug, Clone, Deserialize)]
pub struct ConnectorConfig {
    pub broker: SocketAddr,
    pub durable_id: String,
    pub consumer: String,
    pub filter_subject: String,
    pub generation: u64,
    pub checkpoint_file: PathBuf,
    pub tls: ConnectorTlsConfig,
    pub auth: ConnectorAuthConfig,
    #[serde(flatten)]
    pub target: ConnectorTarget,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectorTlsConfig {
    pub server_name: String,
    pub ca_cert_file: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectorAuthConfig {
    pub client_id: String,
    pub private_key_seed_file: PathBuf,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "target", rename_all = "snake_case")]
pub enum ConnectorTarget {
    ObjectStore { directory: PathBuf },
    AppendDatabase { file: PathBuf },
}

#[derive(Debug, Serialize)]
struct ConnectorDescriptor<'a> {
    version: u32,
    durable_id: &'a str,
    consumer: &'a str,
    filter_subject: &'a str,
    generation: u64,
    checkpoint_file: &'a PathBuf,
    broker: SocketAddr,
    transport: TransportDescriptor<'a>,
    target: &'a ConnectorTarget,
}

#[derive(Debug, Serialize)]
struct TransportDescriptor<'a> {
    tls: bool,
    server_name: &'a str,
    ca_cert_file: &'a PathBuf,
    client_id: &'a str,
    private_key_seed_ref: &'a PathBuf,
}

#[derive(Debug, Clone)]
pub struct SecretRedactor {
    secrets: Vec<String>,
}

impl ConnectorConfig {
    pub fn descriptor_json(&self) -> Result<Vec<u8>, String> {
        serde_json::to_vec(&ConnectorDescriptor {
            version: CONNECTOR_DESCRIPTOR_VERSION,
            durable_id: &self.durable_id,
            consumer: &self.consumer,
            filter_subject: &self.filter_subject,
            generation: self.generation,
            checkpoint_file: &self.checkpoint_file,
            broker: self.broker,
            transport: TransportDescriptor {
                tls: true,
                server_name: &self.tls.server_name,
                ca_cert_file: &self.tls.ca_cert_file,
                client_id: &self.auth.client_id,
                private_key_seed_ref: &self.auth.private_key_seed_file,
            },
            target: &self.target,
        })
        .map_err(|_| "serializing redacted connector descriptor".to_string())
    }

    pub async fn connect_broker(&self) -> Result<(Client, SecretRedactor), String> {
        validate_secret_file(&self.auth.private_key_seed_file)?;
        let seed = std::fs::read_to_string(&self.auth.private_key_seed_file)
            .map_err(|_| "reading connector authentication secret reference".to_string())?;
        let seed = seed.trim().to_string();
        let auth = ClientAuth::from_seed_hex(&self.auth.client_id, &seed)
            .map_err(|_| "connector authentication secret is invalid".to_string())?;
        let redactor = SecretRedactor {
            secrets: vec![seed],
        };
        let options = ClientOptions {
            addr: self.broker,
            max_payload: 1024 * 1024,
            tls: Some(ClientTlsOptions {
                server_name: self.tls.server_name.clone(),
                ca_cert_file: self.tls.ca_cert_file.clone(),
            }),
            auth: Some(auth),
            durable_id: None,
            verbose: false,
            ack_timeout_ms: 30_000,
            max_in_flight: 256,
        };
        let mut client = Client::connect_with_options(&options)
            .await
            .map_err(|err| redactor.redact(&err.to_string()))?;
        client
            .ping_roundtrip()
            .await
            .map_err(|err| redactor.redact(&err.to_string()))?;
        Ok((client, redactor))
    }
}

impl SecretRedactor {
    pub fn redact(&self, message: &str) -> String {
        self.secrets
            .iter()
            .filter(|secret| !secret.is_empty())
            .fold(message.to_string(), |message, secret| {
                message.replace(secret, "[REDACTED]")
            })
    }
}

fn validate_secret_file(path: &std::path::Path) -> Result<(), String> {
    let metadata = std::fs::metadata(path)
        .map_err(|_| "reading connector authentication secret metadata".to_string())?;
    if !metadata.is_file() {
        return Err("connector authentication secret reference is not a file".to_string());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(
                "connector authentication secret must not be accessible by group or others"
                    .to_string(),
            );
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "config/tests.rs"]
mod tests;
