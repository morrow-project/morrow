use std::{
    error::Error, fmt, fs::File, io::BufReader as StdBufReader, net::SocketAddr, path::Path,
    sync::Arc,
};

use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    net::TcpStream,
};
use tokio_rustls::TlsConnector;

use ed25519_dalek::{Signer, SigningKey};
use rustls::{
    ClientConfig, RootCertStore,
    pki_types::{CertificateDer, ServerName},
};

pub use protocol;

pub struct Client {
    stream: BufReader<Box<dyn ClientStream>>,
    max_payload: usize,
}

trait ClientStream: AsyncRead + AsyncWrite + Unpin + Send {}

impl<T> ClientStream for T where T: AsyncRead + AsyncWrite + Unpin + Send {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Info {
    pub raw: String,
    pub auth_required: bool,
    pub nonce: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    pub subject: String,
    pub sid: String,
    pub reply_to: Option<String>,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct ClientAuth {
    client_id: String,
    signing_key: SigningKey,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerFrame {
    Info(Info),
    Message(Message),
    Pong,
    Ok,
    Err(String),
}

#[derive(Debug)]
pub struct ClientError {
    message: String,
    source: Option<Box<dyn Error + Send + Sync + 'static>>,
}

pub type Result<T> = std::result::Result<T, ClientError>;

impl Client {
    pub async fn connect(addr: SocketAddr, max_payload: usize) -> Result<Self> {
        let stream = TcpStream::connect(addr)
            .await
            .map_err(|err| ClientError::with_source(format!("connecting to {addr}"), err))?;
        Ok(Self {
            stream: BufReader::new(Box::new(stream)),
            max_payload,
        })
    }

    pub async fn connect_tls(
        addr: SocketAddr,
        server_name: &str,
        root_cert_file: impl AsRef<Path>,
        max_payload: usize,
    ) -> Result<Self> {
        let stream = TcpStream::connect(addr)
            .await
            .map_err(|err| ClientError::with_source(format!("connecting to {addr}"), err))?;
        let server_name = ServerName::try_from(server_name.to_string())
            .map_err(|err| ClientError::with_source("invalid TLS server name", err))?;
        let connector = TlsConnector::from(Arc::new(tls_config(root_cert_file)?));
        let stream = connector
            .connect(server_name, stream)
            .await
            .map_err(|err| ClientError::with_source("performing TLS handshake", err))?;
        Ok(Self {
            stream: BufReader::new(Box::new(stream)),
            max_payload,
        })
    }

    pub async fn read_info(&mut self) -> Result<Info> {
        match self.next_frame().await? {
            Some(ServerFrame::Info(info)) => Ok(info),
            Some(frame) => Err(ClientError::msg(format!("expected INFO, got {frame:?}"))),
            None => Err(ClientError::msg("connection closed before INFO")),
        }
    }

    pub async fn connect_durable(
        &mut self,
        durable_id: &str,
        verbose: bool,
        ack_timeout_ms: u64,
        max_in_flight: usize,
    ) -> Result<()> {
        let payload = serde_json::json!({
            "durable_id": durable_id,
            "verbose": verbose,
            "ack_timeout_ms": ack_timeout_ms,
            "max_in_flight": max_in_flight,
        });
        self.write_line(&format!("CONNECT {payload}")).await
    }

    pub async fn connect_authenticated(
        &mut self,
        info: &Info,
        auth: &ClientAuth,
        verbose: bool,
        ack_timeout_ms: u64,
        max_in_flight: usize,
    ) -> Result<()> {
        let nonce = info
            .nonce
            .as_deref()
            .ok_or_else(|| ClientError::msg("INFO frame does not contain an auth nonce"))?;
        let payload = serde_json::json!({
            "client_id": auth.client_id,
            "signature": auth.sign_nonce(nonce),
            "verbose": verbose,
            "ack_timeout_ms": ack_timeout_ms,
            "max_in_flight": max_in_flight,
        });
        self.write_line(&format!("CONNECT {payload}")).await
    }

    pub async fn subscribe(&mut self, subject: &str, sid: &str) -> Result<()> {
        self.write_line(&format!("SUB {subject} {sid}")).await
    }

    pub async fn subscribe_queue(&mut self, subject: &str, queue: &str, sid: &str) -> Result<()> {
        self.write_line(&format!("SUB {subject} {queue} {sid}"))
            .await
    }

    pub async fn publish(&mut self, subject: &str, payload: &[u8]) -> Result<()> {
        self.publish_with_reply(subject, None, payload).await
    }

    pub async fn publish_with_reply(
        &mut self,
        subject: &str,
        reply_to: Option<&str>,
        payload: &[u8],
    ) -> Result<()> {
        if payload.len() > self.max_payload {
            return Err(ClientError::msg(format!(
                "payload size {} exceeds max payload {}",
                payload.len(),
                self.max_payload
            )));
        }
        match reply_to {
            Some(reply_to) => {
                self.write_line(&format!("PUB {subject} {reply_to} {}", payload.len()))
                    .await?;
            }
            None => {
                self.write_line(&format!("PUB {subject} {}", payload.len()))
                    .await?;
            }
        }
        self.stream
            .get_mut()
            .write_all(payload)
            .await
            .map_err(|err| ClientError::with_source("writing PUB payload", err))?;
        self.stream
            .get_mut()
            .write_all(b"\r\n")
            .await
            .map_err(|err| ClientError::with_source("writing PUB payload terminator", err))
    }

    pub async fn ack(&mut self, ack_subject: &str) -> Result<()> {
        self.publish(ack_subject, b"").await
    }

    pub async fn ping(&mut self) -> Result<()> {
        self.write_line("PING").await
    }

    pub async fn ping_roundtrip(&mut self) -> Result<()> {
        self.ping().await?;
        loop {
            match self.next_frame().await? {
                Some(ServerFrame::Pong) => return Ok(()),
                Some(ServerFrame::Ok) => {}
                Some(frame) => {
                    return Err(ClientError::msg(format!(
                        "expected PONG during ping roundtrip, got {frame:?}"
                    )));
                }
                None => return Err(ClientError::msg("connection closed before PONG")),
            }
        }
    }

    pub async fn next_message(&mut self) -> Result<Message> {
        loop {
            match self.next_frame().await? {
                Some(ServerFrame::Message(message)) => return Ok(message),
                Some(ServerFrame::Ok) => {}
                Some(frame) => {
                    return Err(ClientError::msg(format!("expected MSG, got {frame:?}")));
                }
                None => return Err(ClientError::msg("connection closed before MSG")),
            }
        }
    }

    pub async fn next_frame(&mut self) -> Result<Option<ServerFrame>> {
        let mut line = Vec::new();
        let read = self
            .stream
            .read_until(b'\n', &mut line)
            .await
            .map_err(|err| ClientError::with_source("reading server frame", err))?;
        if read == 0 {
            return Ok(None);
        }
        trim_crlf(&mut line)?;
        let line = String::from_utf8(line)
            .map_err(|err| ClientError::with_source("server frame is not UTF-8", err))?;
        parse_frame(&mut self.stream, &line, self.max_payload).await
    }

    async fn write_line(&mut self, line: &str) -> Result<()> {
        self.stream
            .get_mut()
            .write_all(line.as_bytes())
            .await
            .map_err(|err| ClientError::with_source("writing protocol line", err))?;
        self.stream
            .get_mut()
            .write_all(b"\r\n")
            .await
            .map_err(|err| ClientError::with_source("writing protocol line terminator", err))
    }
}

impl ClientAuth {
    pub fn new(client_id: impl Into<String>, signing_key: SigningKey) -> Self {
        Self {
            client_id: client_id.into(),
            signing_key,
        }
    }

    pub fn from_seed(client_id: impl Into<String>, seed: [u8; 32]) -> Self {
        Self::new(client_id, SigningKey::from_bytes(&seed))
    }

    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    pub fn public_key_hex(&self) -> String {
        hex(self.signing_key.verifying_key().as_bytes())
    }

    fn sign_nonce(&self, nonce: &str) -> String {
        hex(&self.signing_key.sign(nonce.as_bytes()).to_bytes())
    }
}

async fn parse_frame(
    stream: &mut BufReader<Box<dyn ClientStream>>,
    line: &str,
    max_payload: usize,
) -> Result<Option<ServerFrame>> {
    let mut parts = line.split_whitespace();
    let Some(op) = parts.next() else {
        return Err(ClientError::msg("empty server frame"));
    };
    match op {
        "INFO" => Ok(Some(ServerFrame::Info(parse_info(line)?))),
        "PONG" => Ok(Some(ServerFrame::Pong)),
        "+OK" => Ok(Some(ServerFrame::Ok)),
        "-ERR" => Ok(Some(ServerFrame::Err(line.to_string()))),
        "MSG" => parse_msg(stream, parts, max_payload).await.map(Some),
        _ => Err(ClientError::msg(format!("unsupported server frame {op}"))),
    }
}

fn parse_info(line: &str) -> Result<Info> {
    let payload = line
        .strip_prefix("INFO")
        .map(str::trim)
        .ok_or_else(|| ClientError::msg("invalid INFO frame"))?;
    let value: serde_json::Value = serde_json::from_str(payload)
        .map_err(|err| ClientError::with_source("parsing INFO JSON", err))?;
    Ok(Info {
        raw: payload.to_string(),
        auth_required: value
            .get("auth_required")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false),
        nonce: value
            .get("nonce")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string),
    })
}

async fn parse_msg<'a>(
    stream: &mut BufReader<Box<dyn ClientStream>>,
    mut parts: impl Iterator<Item = &'a str>,
    max_payload: usize,
) -> Result<ServerFrame> {
    let subject = parts
        .next()
        .ok_or_else(|| ClientError::msg("MSG missing subject"))?
        .to_string();
    let sid = parts
        .next()
        .ok_or_else(|| ClientError::msg("MSG missing sid"))?
        .to_string();
    let third = parts
        .next()
        .ok_or_else(|| ClientError::msg("MSG missing payload size"))?;
    let fourth = parts.next();
    if parts.next().is_some() {
        return Err(ClientError::msg("MSG has too many arguments"));
    }
    let (reply_to, size_token) = match fourth {
        Some(size) => (Some(third.to_string()), size),
        None => (None, third),
    };
    let size = size_token
        .parse::<usize>()
        .map_err(|_| ClientError::msg("MSG payload size must be an integer"))?;
    if size > max_payload {
        return Err(ClientError::msg(format!(
            "MSG payload size {size} exceeds max payload {max_payload}"
        )));
    }
    let mut payload = vec![0; size + 2];
    stream
        .read_exact(&mut payload)
        .await
        .map_err(|err| ClientError::with_source("reading MSG payload", err))?;
    if &payload[size..] != b"\r\n" {
        return Err(ClientError::msg("MSG payload must be followed by CRLF"));
    }
    payload.truncate(size);
    Ok(ServerFrame::Message(Message {
        subject,
        sid,
        reply_to,
        payload,
    }))
}

fn trim_crlf(line: &mut Vec<u8>) -> Result<()> {
    if line.ends_with(b"\r\n") {
        line.truncate(line.len() - 2);
        Ok(())
    } else if line.ends_with(b"\n") {
        line.truncate(line.len() - 1);
        Ok(())
    } else {
        Err(ClientError::msg("server frame missing newline"))
    }
}

fn tls_config(root_cert_file: impl AsRef<Path>) -> Result<ClientConfig> {
    let mut roots = RootCertStore::empty();
    for cert in load_certs(root_cert_file)? {
        roots
            .add(cert)
            .map_err(|err| ClientError::with_source("adding root certificate", err))?;
    }
    Ok(ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth())
}

fn load_certs(path: impl AsRef<Path>) -> Result<Vec<CertificateDer<'static>>> {
    let path = path.as_ref();
    let file = File::open(path)
        .map_err(|err| ClientError::with_source(format!("opening {}", path.display()), err))?;
    let mut reader = StdBufReader::new(file);
    let certs: Vec<_> = rustls_pemfile::certs(&mut reader)
        .collect::<std::result::Result<_, _>>()
        .map_err(|err| ClientError::with_source("reading root certificate PEM", err))?;
    if certs.is_empty() {
        return Err(ClientError::msg(
            "root certificate file contains no certificates",
        ));
    }
    Ok(certs)
}

impl ClientError {
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

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl Error for ClientError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source
            .as_deref()
            .map(|source| source as &(dyn Error + 'static))
    }
}

fn hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}
