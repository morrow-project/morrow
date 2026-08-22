use client::{
    Client, ClientAuth, ClientOptions, ClientTlsOptions, ServerFrame, protocol::AckLevel,
};
use std::{
    error::Error,
    fmt, fs,
    io::BufRead,
    net::SocketAddr,
    path::{Path, PathBuf},
    time::Duration,
};
const DEFAULT_CONFIG_PATH: &str = "client.json";
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
const DEFAULT_SERVER: &str = "127.0.0.1:4222";
const DEFAULT_MAX_PAYLOAD: usize = 1_048_576;
const DEFAULT_ACK_TIMEOUT_MS: u64 = 30_000;
const DEFAULT_MAX_IN_FLIGHT: usize = 1024;
const DEFAULT_SID: &str = "sid1";
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    Version,
    Ping,
    Pub {
        subject: String,
        payload: Vec<u8>,
        qos: Option<AckLevel>,
        msg_id: Option<String>,
    },
    Request {
        subject: String,
        payload: Vec<u8>,
        timeout_ms: u64,
    },
    Reply {
        subject: String,
        queue: Option<String>,
    },
    Sub {
        subject: String,
        sid: String,
        queue: Option<String>,
        ack: bool,
        max_messages: Option<usize>,
    },
}
#[derive(Debug, Clone)]
pub struct Args {
    pub config_path: PathBuf,
    pub command: Command,
}
#[derive(Debug, Clone)]
pub struct CliConfig {
    server: SocketAddr,
    max_payload: usize,
    tls: Option<CliTlsConfig>,
    auth: Option<CliAuthConfig>,
    connect: CliConnectConfig,
}
#[derive(Debug, Clone)]
struct CliTlsConfig {
    server_name: String,
    ca_cert_file: PathBuf,
}
#[derive(Debug, Clone)]
struct CliAuthConfig {
    client_id: String,
    private_key_seed_hex: String,
}
#[derive(Debug, Clone)]
struct CliConnectConfig {
    durable_id: Option<String>,
    verbose: bool,
    ack_timeout_ms: u64,
    max_in_flight: usize,
}

#[path = "cli/args.rs"]
mod args;
#[path = "cli/config.rs"]
mod config;
#[path = "cli/error.rs"]
mod error;
#[path = "cli/parse_commands.rs"]
mod parse_commands;
#[path = "cli/run.rs"]
mod run;
use self::{parse_commands::*, run::Result};
pub use run::{CliError, run};

#[cfg(test)]
mod tests;
