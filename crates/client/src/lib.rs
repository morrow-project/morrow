use std::{
    error::Error, fmt, fs::File, io::BufReader as StdBufReader, net::SocketAddr, path::Path,
    path::PathBuf, sync::Arc, time::Duration,
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
    inbox_prefix: String,
    inbox_counter: u64,
}

#[derive(Debug, Clone)]
pub struct ClientOptions {
    pub addr: SocketAddr,
    pub max_payload: usize,
    pub tls: Option<ClientTlsOptions>,
    pub auth: Option<ClientAuth>,
    pub durable_id: Option<String>,
    pub verbose: bool,
    pub ack_timeout_ms: u64,
    pub max_in_flight: usize,
}

#[derive(Debug, Clone)]
pub struct ClientTlsOptions {
    pub server_name: String,
    pub ca_cert_file: PathBuf,
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
    pub ack_subject: Option<String>,
    pub headers: Vec<(String, String)>,
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
            inbox_prefix: default_inbox_prefix(),
            inbox_counter: 0,
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
            inbox_prefix: default_inbox_prefix(),
            inbox_counter: 0,
        })
    }

    pub async fn read_info(&mut self) -> Result<Info> {
        match self.next_frame().await? {
            Some(ServerFrame::Info(info)) => Ok(info),
            Some(frame) => Err(ClientError::msg(format!("expected INFO, got {frame:?}"))),
            None => Err(ClientError::msg("connection closed before INFO")),
        }
    }

    pub async fn connect_with_options(options: &ClientOptions) -> Result<Self> {
        let mut client = match &options.tls {
            Some(tls) => {
                Self::connect_tls(
                    options.addr,
                    &tls.server_name,
                    &tls.ca_cert_file,
                    options.max_payload,
                )
                .await?
            }
            None => Self::connect(options.addr, options.max_payload).await?,
        };
        let info = client.read_info().await?;
        if let Some(auth) = &options.auth {
            client
                .connect_authenticated(
                    &info,
                    auth,
                    options.verbose,
                    options.ack_timeout_ms,
                    options.max_in_flight,
                )
                .await?;
        } else {
            let durable_id = options
                .durable_id
                .as_deref()
                .ok_or_else(|| ClientError::msg("durable_id is required when auth is disabled"))?;
            client
                .connect_durable(
                    durable_id,
                    options.verbose,
                    options.ack_timeout_ms,
                    options.max_in_flight,
                )
                .await?;
        }
        Ok(client)
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
        self.write_line(&format!("CONNECT {payload}")).await?;
        self.inbox_prefix = inbox_prefix(durable_id);
        Ok(())
    }

    pub async fn connect_transient(&mut self, verbose: bool) -> Result<()> {
        let payload = serde_json::json!({
            "verbose": verbose,
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
        self.write_line(&format!("CONNECT {payload}")).await?;
        self.inbox_prefix = inbox_prefix(&auth.client_id);
        Ok(())
    }

    pub async fn subscribe(&mut self, subject: &str, sid: &str) -> Result<()> {
        self.write_line(&format!("SUB {subject} {sid}")).await
    }

    pub async fn subscribe_queue(&mut self, subject: &str, queue: &str, sid: &str) -> Result<()> {
        self.write_line(&format!("SUB {subject} {queue} {sid}"))
            .await
    }

    pub async fn unsubscribe(&mut self, sid: &str) -> Result<()> {
        self.write_line(&format!("UNSUB {sid}")).await
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

    pub async fn request(
        &mut self,
        subject: &str,
        payload: &[u8],
        timeout: Duration,
    ) -> Result<Message> {
        self.inbox_counter = self.inbox_counter.saturating_add(1);
        let inbox = format!("{}.{}", self.inbox_prefix, self.inbox_counter);
        let sid = format!("inbox{}", self.inbox_counter);
        self.subscribe(&inbox, &sid).await?;
        self.ping_roundtrip().await?;
        self.publish_with_reply(subject, Some(&inbox), payload)
            .await?;

        let response = match tokio::time::timeout(timeout, async {
            loop {
                let message = self.next_message().await?;
                if message.subject == inbox {
                    return Ok(message);
                }
            }
        })
        .await
        {
            Ok(response) => response?,
            Err(_) => {
                let _ = self.unsubscribe(&sid).await;
                return Err(ClientError::msg("request timed out"));
            }
        };

        if let Some(ack_subject) = &response.ack_subject {
            self.ack(ack_subject).await?;
        }
        self.unsubscribe(&sid).await?;
        Ok(response)
    }

    pub async fn respond(&mut self, message: &Message, payload: &[u8]) -> Result<()> {
        let reply_to = message
            .reply_to
            .as_deref()
            .ok_or_else(|| ClientError::msg("message does not contain a reply subject"))?;
        self.publish(reply_to, payload).await
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

    pub fn from_seed_hex(client_id: impl Into<String>, seed_hex: &str) -> Result<Self> {
        Ok(Self::from_seed(
            client_id,
            decode_fixed::<32>(seed_hex, "private_key_seed_hex")?,
        ))
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
        "HMSG" => parse_hmsg(stream, parts, max_payload).await.map(Some),
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
    let (reply_to, ack_subject) = match reply_to {
        Some(reply_to) if protocol::parse_ack_subject(&reply_to).is_some() => {
            (None, Some(reply_to))
        }
        reply_to => (reply_to, None),
    };
    Ok(ServerFrame::Message(Message {
        subject,
        sid,
        reply_to,
        ack_subject,
        headers: Vec::new(),
        payload,
    }))
}

async fn parse_hmsg<'a>(
    stream: &mut BufReader<Box<dyn ClientStream>>,
    mut parts: impl Iterator<Item = &'a str>,
    max_payload: usize,
) -> Result<ServerFrame> {
    let subject = parts
        .next()
        .ok_or_else(|| ClientError::msg("HMSG missing subject"))?
        .to_string();
    let sid = parts
        .next()
        .ok_or_else(|| ClientError::msg("HMSG missing sid"))?
        .to_string();
    let third = parts
        .next()
        .ok_or_else(|| ClientError::msg("HMSG missing headers length"))?;
    let fourth = parts
        .next()
        .ok_or_else(|| ClientError::msg("HMSG missing total length"))?;
    let fifth = parts.next();
    if parts.next().is_some() {
        return Err(ClientError::msg("HMSG has too many arguments"));
    }
    let (reply_to, headers_len_token, total_len_token) = match fifth {
        Some(total_len) => (Some(third.to_string()), fourth, total_len),
        None => (None, third, fourth),
    };
    let headers_len = parse_frame_len(headers_len_token, "HMSG headers length")?;
    let total_len = parse_frame_len(total_len_token, "HMSG total length")?;
    if headers_len > total_len {
        return Err(ClientError::msg(
            "HMSG headers length exceeds total frame length",
        ));
    }
    if total_len > max_payload {
        return Err(ClientError::msg(format!(
            "HMSG total length {total_len} exceeds max payload {max_payload}"
        )));
    }

    let mut frame = vec![0; total_len + 2];
    stream
        .read_exact(&mut frame)
        .await
        .map_err(|err| ClientError::with_source("reading HMSG payload", err))?;
    if &frame[total_len..] != b"\r\n" {
        return Err(ClientError::msg("HMSG payload must be followed by CRLF"));
    }
    frame.truncate(total_len);
    let payload = frame.split_off(headers_len);
    let headers = parse_headers(&frame)?;
    let ack_subject = header_value(&headers, "Broker-Ack").map(str::to_string);
    Ok(ServerFrame::Message(Message {
        subject,
        sid,
        reply_to,
        ack_subject,
        headers,
        payload,
    }))
}

fn parse_frame_len(value: &str, field: &str) -> Result<usize> {
    value
        .parse::<usize>()
        .map_err(|_| ClientError::msg(format!("{field} must be an integer")))
}

fn parse_headers(bytes: &[u8]) -> Result<Vec<(String, String)>> {
    let text = std::str::from_utf8(bytes)
        .map_err(|err| ClientError::with_source("HMSG headers are not UTF-8", err))?;
    let mut lines = text.split("\r\n");
    match lines.next() {
        Some("NATS/1.0") => {}
        _ => return Err(ClientError::msg("HMSG headers missing NATS/1.0 line")),
    }
    let mut headers = Vec::new();
    for line in lines {
        if line.is_empty() {
            break;
        }
        let (name, value) = line
            .split_once(':')
            .ok_or_else(|| ClientError::msg("malformed HMSG header line"))?;
        headers.push((name.trim().to_string(), value.trim().to_string()));
    }
    Ok(headers)
}

fn header_value<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(header_name, _)| header_name.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_str())
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

fn default_inbox_prefix() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("_INBOX.client.{:x}.{:x}", std::process::id(), nanos)
}

fn inbox_prefix(client_id: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("_INBOX.{client_id}.{:x}", nanos)
}

fn decode_fixed<const N: usize>(value: &str, field: &str) -> Result<[u8; N]> {
    let value = value.trim();
    if value.len() != N * 2 {
        return Err(ClientError::msg(format!(
            "{field} must be {} hex characters",
            N * 2
        )));
    }
    let mut out = [0_u8; N];
    for (idx, chunk) in value.as_bytes().chunks_exact(2).enumerate() {
        out[idx] = (hex_value(chunk[0], field)? << 4) | hex_value(chunk[1], field)?;
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use tokio::io::AsyncWriteExt;

    use super::*;

    #[tokio::test]
    async fn parses_hmsg_with_reply_and_broker_ack_header() {
        let (mut writer, reader) = tokio::io::duplex(128);
        writer
            .write_all(b"NATS/1.0\r\nBroker-Ack: _BROKER.ACK.consumer.1.2\r\n\r\nhello\r\n")
            .await
            .unwrap();
        let mut reader = BufReader::new(Box::new(reader) as Box<dyn ClientStream>);

        let frame = parse_frame(
            &mut reader,
            "HMSG service.echo sid1 _INBOX.client.1 50 55",
            1024,
        )
        .await
        .unwrap()
        .unwrap();

        let ServerFrame::Message(message) = frame else {
            panic!("expected HMSG to parse as Message");
        };
        assert_eq!(message.subject, "service.echo");
        assert_eq!(message.sid, "sid1");
        assert_eq!(message.reply_to.as_deref(), Some("_INBOX.client.1"));
        assert_eq!(
            message.ack_subject.as_deref(),
            Some("_BROKER.ACK.consumer.1.2")
        );
        assert_eq!(message.payload, b"hello");
    }

    #[tokio::test]
    async fn parses_msg_ack_reply_as_ack_subject() {
        let (mut writer, reader) = tokio::io::duplex(64);
        writer.write_all(b"hello\r\n").await.unwrap();
        let mut reader = BufReader::new(Box::new(reader) as Box<dyn ClientStream>);

        let frame = parse_frame(
            &mut reader,
            "MSG orders.created sid1 _BROKER.ACK.consumer.1.2 5",
            1024,
        )
        .await
        .unwrap()
        .unwrap();

        let ServerFrame::Message(message) = frame else {
            panic!("expected MSG to parse as Message");
        };
        assert!(message.reply_to.is_none());
        assert_eq!(
            message.ack_subject.as_deref(),
            Some("_BROKER.ACK.consumer.1.2")
        );
    }

    #[tokio::test]
    async fn rejects_malformed_hmsg_lengths() {
        let (_writer, reader) = tokio::io::duplex(64);
        let mut reader = BufReader::new(Box::new(reader) as Box<dyn ClientStream>);

        let err = parse_frame(&mut reader, "HMSG service.echo sid1 nope 5", 1024)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("headers length"));

        let err = parse_frame(&mut reader, "HMSG service.echo sid1 6 5", 1024)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("exceeds total"));

        let err = parse_frame(&mut reader, "HMSG service.echo sid1 1 2048", 1024)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("exceeds max payload"));
    }
}

fn hex_value(byte: u8, field: &str) -> Result<u8> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(ClientError::msg(format!("{field} must be hex encoded"))),
    }
}
