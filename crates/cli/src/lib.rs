use std::{
    error::Error,
    fmt, fs,
    io::BufRead,
    net::SocketAddr,
    path::{Path, PathBuf},
    time::Duration,
};

use client::{Client, ClientAuth, ClientOptions, ClientTlsOptions, ServerFrame};

const DEFAULT_CONFIG_PATH: &str = "client.json";
const DEFAULT_SERVER: &str = "127.0.0.1:4222";
const DEFAULT_MAX_PAYLOAD: usize = 1_048_576;
const DEFAULT_ACK_TIMEOUT_MS: u64 = 30_000;
const DEFAULT_MAX_IN_FLIGHT: usize = 1024;
const DEFAULT_SID: &str = "sid1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    Ping,
    Pub {
        subject: String,
        payload: Vec<u8>,
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

#[derive(Debug)]
pub struct CliError {
    message: String,
    source: Option<Box<dyn Error + Send + Sync + 'static>>,
}

pub type Result<T> = std::result::Result<T, CliError>;

pub async fn run(args: impl IntoIterator<Item = String>) -> Result<()> {
    let args = Args::parse(args)?;
    let config = CliConfig::load(&args.config_path)?;
    run_command(&config, args.command).await
}

async fn run_command(config: &CliConfig, command: Command) -> Result<()> {
    match command {
        Command::Ping => {
            let mut client = config.connect_client().await?;
            client.ping_roundtrip().await?;
            println!("PONG");
            Ok(())
        }
        Command::Pub { subject, payload } => {
            let mut client = config.connect_client().await?;
            client.publish(&subject, &payload).await?;
            if config.connect.verbose {
                expect_ok(&mut client).await?;
            }
            Ok(())
        }
        Command::Request {
            subject,
            payload,
            timeout_ms,
        } => {
            let mut client = config.connect_client().await?;
            let response = client
                .request(&subject, &payload, Duration::from_millis(timeout_ms))
                .await?;
            println!("{}", String::from_utf8_lossy(&response.payload));
            Ok(())
        }
        Command::Reply { subject, queue } => {
            let mut client = config.connect_client().await?;
            match queue {
                Some(queue) => {
                    client
                        .subscribe_queue(&subject, &queue, DEFAULT_SID)
                        .await?
                }
                None => client.subscribe(&subject, DEFAULT_SID).await?,
            }
            client.ping_roundtrip().await?;
            loop {
                let message = client.next_message().await?;
                println!(
                    "{} {} {}",
                    message.subject,
                    message.sid,
                    String::from_utf8_lossy(&message.payload)
                );
                let response = read_stdin_line().await?;
                client.respond(&message, response.as_bytes()).await?;
                if let Some(ack_subject) = &message.ack_subject {
                    client.ack(ack_subject).await?;
                }
            }
        }
        Command::Sub {
            subject,
            sid,
            queue,
            ack,
            max_messages,
        } => {
            let mut client = config.connect_client().await?;
            match queue {
                Some(queue) => client.subscribe_queue(&subject, &queue, &sid).await?,
                None => client.subscribe(&subject, &sid).await?,
            }
            client.ping_roundtrip().await?;
            let mut received = 0_usize;
            loop {
                let message = client.next_message().await?;
                println!(
                    "{} {} {}",
                    message.subject,
                    message.sid,
                    String::from_utf8_lossy(&message.payload)
                );
                if ack {
                    if let Some(ack_subject) = &message.ack_subject {
                        client.ack(ack_subject).await?;
                    }
                }
                received += 1;
                if max_messages.is_some_and(|limit| received >= limit) {
                    return Ok(());
                }
            }
        }
    }
}

async fn expect_ok(client: &mut Client) -> Result<()> {
    loop {
        match client.next_frame().await? {
            Some(ServerFrame::Ok) => return Ok(()),
            Some(ServerFrame::Err(err)) => return Err(CliError::msg(err)),
            Some(ServerFrame::Pong) => {}
            Some(frame) => {
                return Err(CliError::msg(format!(
                    "expected +OK after publish, got {frame:?}"
                )));
            }
            None => return Err(CliError::msg("connection closed before +OK")),
        }
    }
}

impl Args {
    pub fn parse(args: impl IntoIterator<Item = String>) -> Result<Self> {
        let mut args = args.into_iter();
        let _program = args.next();
        let mut config_path = PathBuf::from(DEFAULT_CONFIG_PATH);
        let mut rest = Vec::new();
        while let Some(arg) = args.next() {
            if arg == "--config" {
                let value = args
                    .next()
                    .ok_or_else(|| CliError::msg("--config requires a path"))?;
                config_path = PathBuf::from(value);
            } else {
                rest.push(arg);
                rest.extend(args);
                break;
            }
        }
        let command = parse_command(rest)?;
        Ok(Self {
            config_path,
            command,
        })
    }
}

fn parse_command(args: Vec<String>) -> Result<Command> {
    let mut args = args.into_iter();
    let command = args.next().ok_or_else(usage)?;
    match command.as_str() {
        "ping" => {
            ensure_no_more(args, "ping")?;
            Ok(Command::Ping)
        }
        "pub" => {
            let subject = args
                .next()
                .ok_or_else(|| CliError::msg("pub requires a subject"))?;
            let payload = args
                .next()
                .ok_or_else(|| CliError::msg("pub requires a payload"))?
                .into_bytes();
            ensure_no_more(args, "pub")?;
            Ok(Command::Pub { subject, payload })
        }
        "sub" => parse_sub(args),
        "request" => parse_request(args),
        "reply" => parse_reply(args),
        _ => Err(usage()),
    }
}

fn parse_request(mut args: impl Iterator<Item = String>) -> Result<Command> {
    let subject = args
        .next()
        .ok_or_else(|| CliError::msg("request requires a subject"))?;
    let payload = args
        .next()
        .ok_or_else(|| CliError::msg("request requires a payload"))?
        .into_bytes();
    let mut timeout_ms = 30_000;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--timeout-ms" => {
                let value = args
                    .next()
                    .ok_or_else(|| CliError::msg("--timeout-ms requires a value"))?;
                timeout_ms = value
                    .parse()
                    .map_err(|_| CliError::msg("--timeout-ms must be an integer"))?;
            }
            _ => return Err(CliError::msg(format!("unknown request option {arg}"))),
        }
    }
    Ok(Command::Request {
        subject,
        payload,
        timeout_ms,
    })
}

fn parse_reply(mut args: impl Iterator<Item = String>) -> Result<Command> {
    let subject = args
        .next()
        .ok_or_else(|| CliError::msg("reply requires a subject"))?;
    let mut queue = None;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--queue" => {
                queue = Some(
                    args.next()
                        .ok_or_else(|| CliError::msg("--queue requires a value"))?,
                );
            }
            _ => return Err(CliError::msg(format!("unknown reply option {arg}"))),
        }
    }
    Ok(Command::Reply { subject, queue })
}

async fn read_stdin_line() -> Result<String> {
    tokio::task::spawn_blocking(|| {
        let mut line = String::new();
        let read = std::io::stdin()
            .lock()
            .read_line(&mut line)
            .map_err(|err| CliError::with_source("reading response from stdin", err))?;
        if read == 0 {
            return Err(CliError::msg("stdin closed before response was provided"));
        }
        while line.ends_with('\n') || line.ends_with('\r') {
            line.pop();
        }
        Ok(line)
    })
    .await
    .map_err(|err| CliError::with_source("joining stdin reader task", err))?
}

fn parse_sub(mut args: impl Iterator<Item = String>) -> Result<Command> {
    let subject = args
        .next()
        .ok_or_else(|| CliError::msg("sub requires a subject"))?;
    let mut sid = DEFAULT_SID.to_string();
    let mut queue = None;
    let mut ack = false;
    let mut max_messages = None;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--sid" => {
                sid = args
                    .next()
                    .ok_or_else(|| CliError::msg("--sid requires a value"))?;
            }
            "--queue" => {
                queue = Some(
                    args.next()
                        .ok_or_else(|| CliError::msg("--queue requires a value"))?,
                );
            }
            "--ack" => ack = true,
            "--max-messages" => {
                let value = args
                    .next()
                    .ok_or_else(|| CliError::msg("--max-messages requires a value"))?;
                max_messages = Some(
                    value
                        .parse()
                        .map_err(|_| CliError::msg("--max-messages must be an integer"))?,
                );
            }
            _ => return Err(CliError::msg(format!("unknown sub option {arg}"))),
        }
    }
    Ok(Command::Sub {
        subject,
        sid,
        queue,
        ack,
        max_messages,
    })
}

fn ensure_no_more(mut args: impl Iterator<Item = String>, command: &str) -> Result<()> {
    if let Some(arg) = args.next() {
        return Err(CliError::msg(format!(
            "{command} received unexpected argument {arg}"
        )));
    }
    Ok(())
}

fn usage() -> CliError {
    CliError::msg(
        "usage: broker-cli [--config client.json] <ping|pub|sub|request|reply>\n\
         pub <subject> <payload>\n\
         sub <subject> [--sid sid] [--queue group] [--ack] [--max-messages n]\n\
         request <subject> <payload> [--timeout-ms n]\n\
         reply <subject> [--queue group]",
    )
}

impl CliConfig {
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let contents = fs::read_to_string(path)
            .map_err(|err| CliError::with_source(format!("reading {}", path.display()), err))?;
        let value: serde_json::Value = serde_json::from_str(&contents)
            .map_err(|err| CliError::with_source(format!("parsing {}", path.display()), err))?;
        Self::from_json(&value)
    }

    pub fn from_json(value: &serde_json::Value) -> Result<Self> {
        if !value.is_object() {
            return Err(CliError::msg("client config must be a JSON object"));
        }
        let server = get_string(value, "server")?
            .unwrap_or(DEFAULT_SERVER)
            .parse()
            .map_err(|err| {
                CliError::with_source("config field server must be a socket address", err)
            })?;
        let max_payload = get_u64(value, "max_payload")?
            .unwrap_or(DEFAULT_MAX_PAYLOAD as u64)
            .try_into()
            .map_err(|err| CliError::with_source("config field max_payload is too large", err))?;
        let tls = parse_tls(value)?;
        let auth = parse_auth(value)?;
        let connect = parse_connect(value)?;
        Ok(Self {
            server,
            max_payload,
            tls,
            auth,
            connect,
        })
    }

    async fn connect_client(&self) -> Result<Client> {
        let options = self.client_options()?;
        Ok(Client::connect_with_options(&options).await?)
    }

    fn client_options(&self) -> Result<ClientOptions> {
        let auth = self
            .auth
            .as_ref()
            .map(|auth| ClientAuth::from_seed_hex(&auth.client_id, &auth.private_key_seed_hex))
            .transpose()?;
        let tls = self.tls.as_ref().map(|tls| ClientTlsOptions {
            server_name: tls.server_name.clone(),
            ca_cert_file: tls.ca_cert_file.clone(),
        });
        Ok(ClientOptions {
            addr: self.server,
            max_payload: self.max_payload,
            tls,
            auth,
            durable_id: self.connect.durable_id.clone(),
            verbose: self.connect.verbose,
            ack_timeout_ms: self.connect.ack_timeout_ms,
            max_in_flight: self.connect.max_in_flight,
        })
    }
}

fn parse_tls(value: &serde_json::Value) -> Result<Option<CliTlsConfig>> {
    let Some(tls) = value.get("tls") else {
        return Ok(None);
    };
    if tls.is_null() {
        return Ok(None);
    }
    let enabled = get_bool(tls, "enabled")?.unwrap_or(false);
    if !enabled {
        return Ok(None);
    }
    let server_name = get_string(tls, "server_name")?
        .unwrap_or("localhost")
        .to_string();
    let ca_cert_file = get_string(tls, "ca_cert_file")?.ok_or_else(|| {
        CliError::msg("config field tls.ca_cert_file is required when TLS is enabled")
    })?;
    Ok(Some(CliTlsConfig {
        server_name,
        ca_cert_file: PathBuf::from(ca_cert_file),
    }))
}

fn parse_auth(value: &serde_json::Value) -> Result<Option<CliAuthConfig>> {
    let Some(auth) = value.get("auth") else {
        return Ok(None);
    };
    if auth.is_null() {
        return Ok(None);
    }
    let enabled = get_bool(auth, "enabled")?.unwrap_or(false);
    if !enabled {
        return Ok(None);
    }
    let client_id = get_string(auth, "client_id")?
        .ok_or_else(|| {
            CliError::msg("config field auth.client_id is required when auth is enabled")
        })?
        .to_string();
    let private_key_seed_hex = get_string(auth, "private_key_seed_hex")?
        .ok_or_else(|| {
            CliError::msg("config field auth.private_key_seed_hex is required when auth is enabled")
        })?
        .to_string();
    ClientAuth::from_seed_hex(&client_id, &private_key_seed_hex)?;
    Ok(Some(CliAuthConfig {
        client_id,
        private_key_seed_hex,
    }))
}

fn parse_connect(value: &serde_json::Value) -> Result<CliConnectConfig> {
    let connect = value.get("connect").unwrap_or(&serde_json::Value::Null);
    if !connect.is_null() && !connect.is_object() {
        return Err(CliError::msg("config field connect must be an object"));
    }
    Ok(CliConnectConfig {
        durable_id: get_string(connect, "durable_id")?.map(str::to_string),
        verbose: get_bool(connect, "verbose")?.unwrap_or(false),
        ack_timeout_ms: get_u64(connect, "ack_timeout_ms")?.unwrap_or(DEFAULT_ACK_TIMEOUT_MS),
        max_in_flight: get_u64(connect, "max_in_flight")?
            .unwrap_or(DEFAULT_MAX_IN_FLIGHT as u64)
            .try_into()
            .map_err(|err| {
                CliError::with_source("config field connect.max_in_flight is too large", err)
            })?,
    })
}

fn get_string<'a>(value: &'a serde_json::Value, key: &str) -> Result<Option<&'a str>> {
    match value.get(key) {
        Some(serde_json::Value::String(value)) => Ok(Some(value)),
        Some(_) => Err(CliError::msg(format!(
            "config field {key} must be a string"
        ))),
        None => Ok(None),
    }
}

fn get_u64(value: &serde_json::Value, key: &str) -> Result<Option<u64>> {
    match value.get(key) {
        Some(serde_json::Value::Number(value)) => value
            .as_u64()
            .ok_or_else(|| CliError::msg(format!("config field {key} must be an unsigned integer")))
            .map(Some),
        Some(_) => Err(CliError::msg(format!(
            "config field {key} must be an unsigned integer"
        ))),
        None => Ok(None),
    }
}

fn get_bool(value: &serde_json::Value, key: &str) -> Result<Option<bool>> {
    match value.get(key) {
        Some(serde_json::Value::Bool(value)) => Ok(Some(*value)),
        Some(_) => Err(CliError::msg(format!(
            "config field {key} must be a boolean"
        ))),
        None => Ok(None),
    }
}

impl CliError {
    fn msg(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            source: None,
        }
    }

    fn with_source(message: impl Into<String>, source: impl Error + Send + Sync + 'static) -> Self {
        Self {
            message: message.into(),
            source: Some(Box::new(source)),
        }
    }
}

impl fmt::Display for CliError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl Error for CliError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source
            .as_deref()
            .map(|source| source as &(dyn Error + 'static))
    }
}

impl From<client::ClientError> for CliError {
    fn from(source: client::ClientError) -> Self {
        Self::with_source(source.to_string(), source)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_client_config_defaults() {
        let config = CliConfig::from_json(&serde_json::json!({})).unwrap();
        assert_eq!(config.server, DEFAULT_SERVER.parse().unwrap());
        assert_eq!(config.max_payload, DEFAULT_MAX_PAYLOAD);
        assert!(config.tls.is_none());
        assert!(config.auth.is_none());
        assert!(!config.connect.verbose);
        assert_eq!(config.connect.ack_timeout_ms, DEFAULT_ACK_TIMEOUT_MS);
        assert_eq!(config.connect.max_in_flight, DEFAULT_MAX_IN_FLIGHT);
    }

    #[test]
    fn rejects_invalid_server_address() {
        let err = CliConfig::from_json(&serde_json::json!({"server": "bad"})).unwrap_err();
        assert!(err.to_string().contains("server"));
    }

    #[test]
    fn rejects_auth_without_client_id() {
        let err = CliConfig::from_json(&serde_json::json!({
            "auth": {"enabled": true, "private_key_seed_hex": "00".repeat(32)}
        }))
        .unwrap_err();
        assert!(err.to_string().contains("auth.client_id"));
    }

    #[test]
    fn rejects_auth_without_seed() {
        let err = CliConfig::from_json(&serde_json::json!({
            "auth": {"enabled": true, "client_id": "client1"}
        }))
        .unwrap_err();
        assert!(err.to_string().contains("private_key_seed_hex"));
    }

    #[test]
    fn rejects_malformed_seed() {
        let err = CliConfig::from_json(&serde_json::json!({
            "auth": {
                "enabled": true,
                "client_id": "client1",
                "private_key_seed_hex": "bad"
            }
        }))
        .unwrap_err();
        assert!(err.to_string().contains("private_key_seed_hex"));
    }

    #[test]
    fn parses_ping_args() {
        let args = Args::parse(["broker-cli", "ping"].into_iter().map(str::to_string)).unwrap();
        assert_eq!(args.config_path, PathBuf::from(DEFAULT_CONFIG_PATH));
        assert_eq!(args.command, Command::Ping);
    }

    #[test]
    fn parses_pub_args() {
        let args = Args::parse(
            [
                "broker-cli",
                "--config",
                "custom.json",
                "pub",
                "orders.created",
                "hello",
            ]
            .into_iter()
            .map(str::to_string),
        )
        .unwrap();
        assert_eq!(args.config_path, PathBuf::from("custom.json"));
        assert_eq!(
            args.command,
            Command::Pub {
                subject: "orders.created".into(),
                payload: b"hello".to_vec(),
            }
        );
    }

    #[test]
    fn parses_sub_args() {
        let args = Args::parse(
            [
                "broker-cli",
                "sub",
                "orders.*",
                "--sid",
                "sid2",
                "--queue",
                "workers",
                "--ack",
                "--max-messages",
                "2",
            ]
            .into_iter()
            .map(str::to_string),
        )
        .unwrap();
        assert_eq!(
            args.command,
            Command::Sub {
                subject: "orders.*".into(),
                sid: "sid2".into(),
                queue: Some("workers".into()),
                ack: true,
                max_messages: Some(2),
            }
        );
    }

    #[test]
    fn parses_request_args() {
        let args = Args::parse(
            [
                "broker-cli",
                "request",
                "orders.lookup",
                "hello",
                "--timeout-ms",
                "500",
            ]
            .into_iter()
            .map(str::to_string),
        )
        .unwrap();
        assert_eq!(
            args.command,
            Command::Request {
                subject: "orders.lookup".into(),
                payload: b"hello".to_vec(),
                timeout_ms: 500,
            }
        );
    }

    #[test]
    fn parses_reply_args() {
        let args = Args::parse(
            ["broker-cli", "reply", "orders.lookup", "--queue", "workers"]
                .into_iter()
                .map(str::to_string),
        )
        .unwrap();
        assert_eq!(
            args.command,
            Command::Reply {
                subject: "orders.lookup".into(),
                queue: Some("workers".into()),
            }
        );
    }
}
