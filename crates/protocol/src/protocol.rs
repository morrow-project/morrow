use std::str;

use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    Connect {
        verbose: bool,
        durable_id: Option<String>,
        ack_timeout_ms: Option<u64>,
        max_in_flight: Option<usize>,
        auth: Option<ConnectAuth>,
    },
    Ping,
    Pong,
    Pub {
        subject: String,
        reply_to: Option<String>,
        payload: Vec<u8>,
        ack: Option<ProducerAckRequest>,
    },
    Sub {
        subject: String,
        queue: Option<String>,
        sid: String,
    },
    Unsub {
        sid: String,
        max_messages: Option<usize>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckLevel {
    Accepted = 0,
    Durable = 1,
    HighDurability = 2,
    ClusterDurable = 3,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProducerAckRequest {
    pub level: AckLevel,
    pub msg_id: String,
}

impl AckLevel {
    fn parse(value: &str) -> Result<Self, ProtocolError> {
        match value {
            "0" => Ok(Self::Accepted),
            "1" => Ok(Self::Durable),
            "2" => Ok(Self::HighDurability),
            "3" => Ok(Self::ClusterDurable),
            _ => Err(ProtocolError("Broker-QoS must be 0, 1, 2, or 3".into())),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectAuth {
    pub client_id: String,
    pub signature: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AckSubject {
    pub consumer_id: String,
    pub seq: u64,
    pub delivery_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtocolError(pub String);

impl std::fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ProtocolError {}

pub async fn read_command<R: AsyncBufRead + Unpin>(
    reader: &mut R,
    max_payload: usize,
    max_control_line: usize,
) -> Result<Option<Command>, ProtocolError> {
    let Some(mut line) = read_control_line(reader, max_control_line).await? else {
        return Ok(None);
    };

    trim_crlf(&mut line)?;
    let line =
        str::from_utf8(&line).map_err(|_| ProtocolError("protocol line is not UTF-8".into()))?;
    let mut parts = line.split_whitespace();
    let Some(op) = parts.next() else {
        return Err(ProtocolError("empty protocol line".into()));
    };

    match op.to_ascii_uppercase().as_str() {
        "CONNECT" => {
            let payload = line
                .strip_prefix(op)
                .map(str::trim)
                .filter(|payload| !payload.is_empty())
                .unwrap_or("{}");
            let connect = parse_connect(payload)?;
            Ok(Some(connect))
        }
        "PING" => Ok(Some(Command::Ping)),
        "PONG" => Ok(Some(Command::Pong)),
        "SUB" => parse_sub(parts).map(Some),
        "UNSUB" => parse_unsub(parts).map(Some),
        "PUB" => read_pub(reader, parts, max_payload).await.map(Some),
        "HPUB" => read_hpub(reader, parts, max_payload).await.map(Some),
        _ => Err(ProtocolError(format!("unsupported command {op}"))),
    }
}

async fn read_control_line<R: AsyncBufRead + Unpin>(
    reader: &mut R,
    max_control_line: usize,
) -> Result<Option<Vec<u8>>, ProtocolError> {
    let mut line = Vec::new();
    loop {
        let available = reader
            .fill_buf()
            .await
            .map_err(|err| ProtocolError(format!("read failed: {err}")))?;
        if available.is_empty() {
            return if line.is_empty() {
                Ok(None)
            } else {
                Err(ProtocolError("protocol line missing newline".into()))
            };
        }
        let take = available
            .iter()
            .position(|byte| *byte == b'\n')
            .map(|idx| idx + 1)
            .unwrap_or(available.len());
        if line.len() + take > max_control_line {
            return Err(ProtocolError(format!(
                "protocol line exceeds max_control_line {max_control_line}"
            )));
        }
        line.extend_from_slice(&available[..take]);
        reader.consume(take);
        if line.ends_with(b"\n") {
            return Ok(Some(line));
        }
    }
}

fn parse_connect(payload: &str) -> Result<Command, ProtocolError> {
    let value: serde_json::Value = serde_json::from_str(payload)
        .map_err(|err| ProtocolError(format!("invalid CONNECT payload: {err}")))?;
    let verbose = get_bool(&value, "verbose")?.unwrap_or(false);
    let durable_id = get_string(&value, "durable_id")?.map(str::to_string);
    if let Some(durable_id) = &durable_id {
        validate_identifier("durable_id", durable_id)?;
    }
    let ack_timeout_ms = get_u64(&value, "ack_timeout_ms")?;
    let max_in_flight = get_u64(&value, "max_in_flight")?
        .map(|value| {
            value
                .try_into()
                .map_err(|_| ProtocolError("max_in_flight is too large".into()))
        })
        .transpose()?;
    let auth = parse_connect_auth(&value)?;
    Ok(Command::Connect {
        verbose,
        durable_id,
        ack_timeout_ms,
        max_in_flight,
        auth,
    })
}

fn parse_connect_auth(value: &serde_json::Value) -> Result<Option<ConnectAuth>, ProtocolError> {
    let client_id = get_string(value, "client_id")?.map(str::to_string);
    let signature = get_string(value, "signature")?.map(str::to_string);
    if let Some(client_id) = &client_id {
        validate_identifier("client_id", client_id)?;
    }
    match (client_id, signature) {
        (Some(client_id), Some(signature)) => Ok(Some(ConnectAuth {
            client_id,
            signature,
        })),
        (None, None) => Ok(None),
        _ => Err(ProtocolError(
            "CONNECT client_id and signature must be provided together".into(),
        )),
    }
}

fn get_bool(value: &serde_json::Value, key: &str) -> Result<Option<bool>, ProtocolError> {
    match value.get(key) {
        Some(serde_json::Value::Bool(value)) => Ok(Some(*value)),
        Some(_) => Err(ProtocolError(format!(
            "CONNECT field {key} must be a boolean"
        ))),
        None => Ok(None),
    }
}

fn get_string<'a>(
    value: &'a serde_json::Value,
    key: &str,
) -> Result<Option<&'a str>, ProtocolError> {
    match value.get(key) {
        Some(serde_json::Value::String(value)) => Ok(Some(value)),
        Some(_) => Err(ProtocolError(format!(
            "CONNECT field {key} must be a string"
        ))),
        None => Ok(None),
    }
}

fn get_u64(value: &serde_json::Value, key: &str) -> Result<Option<u64>, ProtocolError> {
    match value.get(key) {
        Some(serde_json::Value::Number(value)) => value
            .as_u64()
            .ok_or_else(|| {
                ProtocolError(format!("CONNECT field {key} must be an unsigned integer"))
            })
            .map(Some),
        Some(_) => Err(ProtocolError(format!(
            "CONNECT field {key} must be an unsigned integer"
        ))),
        None => Ok(None),
    }
}

pub fn validate_identifier(name: &str, value: &str) -> Result<(), ProtocolError> {
    if value.is_empty()
        || value.contains('.')
        || value.chars().any(char::is_whitespace)
        || value.starts_with('_')
    {
        return Err(ProtocolError(format!(
            "{name} must be non-empty and must not contain '.', whitespace, or start with '_'"
        )));
    }
    Ok(())
}

fn parse_sub<'a>(mut parts: impl Iterator<Item = &'a str>) -> Result<Command, ProtocolError> {
    let subject = parts
        .next()
        .ok_or_else(|| ProtocolError("SUB requires a subject".into()))?
        .to_string();
    let second = parts
        .next()
        .ok_or_else(|| ProtocolError("SUB requires a sid".into()))?;
    let third = parts.next();
    if parts.next().is_some() {
        return Err(ProtocolError("SUB has too many arguments".into()));
    }

    let (queue, sid) = match third {
        Some(sid) => (Some(second.to_string()), sid.to_string()),
        None => (None, second.to_string()),
    };

    Ok(Command::Sub {
        subject,
        queue,
        sid,
    })
}

fn parse_unsub<'a>(mut parts: impl Iterator<Item = &'a str>) -> Result<Command, ProtocolError> {
    let sid = parts
        .next()
        .ok_or_else(|| ProtocolError("UNSUB requires a sid".into()))?
        .to_string();
    let max_messages = match parts.next() {
        Some(value) => Some(
            value
                .parse()
                .map_err(|_| ProtocolError("UNSUB max messages must be an integer".into()))?,
        ),
        None => None,
    };
    if parts.next().is_some() {
        return Err(ProtocolError("UNSUB has too many arguments".into()));
    }
    Ok(Command::Unsub { sid, max_messages })
}

async fn read_pub<'a, R: AsyncBufRead + Unpin>(
    reader: &mut R,
    mut parts: impl Iterator<Item = &'a str>,
    max_payload: usize,
) -> Result<Command, ProtocolError> {
    let subject = parts
        .next()
        .ok_or_else(|| ProtocolError("PUB requires a subject".into()))?
        .to_string();
    let second = parts
        .next()
        .ok_or_else(|| ProtocolError("PUB requires a payload size".into()))?;
    let third = parts.next();
    if parts.next().is_some() {
        return Err(ProtocolError("PUB has too many arguments".into()));
    }

    let (reply_to, size_token) = match third {
        Some(size) => (Some(second.to_string()), size),
        None => (None, second),
    };
    let size = size_token
        .parse::<usize>()
        .map_err(|_| ProtocolError("PUB payload size must be an integer".into()))?;
    if size > max_payload {
        return Err(ProtocolError(format!(
            "payload size {size} exceeds max payload {max_payload}"
        )));
    }

    let mut payload = vec![0; size + 2];
    reader
        .read_exact(&mut payload)
        .await
        .map_err(|err| ProtocolError(format!("failed to read PUB payload: {err}")))?;
    if &payload[size..] != b"\r\n" {
        return Err(ProtocolError("PUB payload must be followed by CRLF".into()));
    }
    payload.truncate(size);

    Ok(Command::Pub {
        subject,
        reply_to,
        payload,
        ack: None,
    })
}

async fn read_hpub<'a, R: AsyncBufRead + Unpin>(
    reader: &mut R,
    mut parts: impl Iterator<Item = &'a str>,
    max_payload: usize,
) -> Result<Command, ProtocolError> {
    let subject = parts
        .next()
        .ok_or_else(|| ProtocolError("HPUB requires a subject".into()))?
        .to_string();
    let second = parts
        .next()
        .ok_or_else(|| ProtocolError("HPUB requires a headers length".into()))?;
    let third = parts
        .next()
        .ok_or_else(|| ProtocolError("HPUB requires a total length".into()))?;
    let fourth = parts.next();
    if parts.next().is_some() {
        return Err(ProtocolError("HPUB has too many arguments".into()));
    }

    let (reply_to, headers_len_token, total_len_token) = match fourth {
        Some(total_len) => (Some(second.to_string()), third, total_len),
        None => (None, second, third),
    };
    let headers_len = parse_len(headers_len_token, "HPUB headers length")?;
    let total_len = parse_len(total_len_token, "HPUB total length")?;
    if headers_len > total_len {
        return Err(ProtocolError(
            "HPUB headers length exceeds total frame length".into(),
        ));
    }
    if total_len > max_payload {
        return Err(ProtocolError(format!(
            "HPUB total length {total_len} exceeds max payload {max_payload}"
        )));
    }

    let mut frame = vec![0; total_len + 2];
    reader
        .read_exact(&mut frame)
        .await
        .map_err(|err| ProtocolError(format!("failed to read HPUB payload: {err}")))?;
    if &frame[total_len..] != b"\r\n" {
        return Err(ProtocolError(
            "HPUB payload must be followed by CRLF".into(),
        ));
    }
    frame.truncate(total_len);
    let payload = frame.split_off(headers_len);
    let headers = parse_headers(&frame)?;
    let ack = parse_producer_ack_request(&headers)?;

    Ok(Command::Pub {
        subject,
        reply_to,
        payload,
        ack,
    })
}

fn parse_len(value: &str, field: &str) -> Result<usize, ProtocolError> {
    value
        .parse::<usize>()
        .map_err(|_| ProtocolError(format!("{field} must be an integer")))
}

fn parse_headers(bytes: &[u8]) -> Result<Vec<(String, String)>, ProtocolError> {
    let text = std::str::from_utf8(bytes)
        .map_err(|err| ProtocolError(format!("HPUB headers are not UTF-8: {err}")))?;
    let mut lines = text.split("\r\n");
    match lines.next() {
        Some("NATS/1.0") => {}
        _ => return Err(ProtocolError("HPUB headers missing NATS/1.0 line".into())),
    }
    let mut headers = Vec::new();
    for line in lines {
        if line.is_empty() {
            break;
        }
        let (name, value) = line
            .split_once(':')
            .ok_or_else(|| ProtocolError("malformed HPUB header line".into()))?;
        headers.push((name.trim().to_string(), value.trim().to_string()));
    }
    Ok(headers)
}

fn parse_producer_ack_request(
    headers: &[(String, String)],
) -> Result<Option<ProducerAckRequest>, ProtocolError> {
    let Some(level) = header_value(headers, "Broker-QoS") else {
        return Ok(None);
    };
    let level = AckLevel::parse(level)?;
    let msg_id = header_value(headers, "Broker-Msg-Id")
        .ok_or_else(|| ProtocolError("Broker-Msg-Id is required when Broker-QoS is set".into()))?;
    validate_msg_id(msg_id)?;
    Ok(Some(ProducerAckRequest {
        level,
        msg_id: msg_id.to_string(),
    }))
}

fn header_value<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(header_name, _)| header_name.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_str())
}

fn validate_msg_id(value: &str) -> Result<(), ProtocolError> {
    if value.is_empty()
        || value.len() > 128
        || value.chars().any(|ch| ch == '\r' || ch == '\n')
        || value.chars().any(char::is_whitespace)
    {
        return Err(ProtocolError(
            "Broker-Msg-Id must be non-empty, at most 128 bytes, and contain no whitespace".into(),
        ));
    }
    Ok(())
}

fn trim_crlf(line: &mut Vec<u8>) -> Result<(), ProtocolError> {
    if line.ends_with(b"\r\n") {
        line.truncate(line.len() - 2);
        Ok(())
    } else if line.ends_with(b"\n") {
        line.truncate(line.len() - 1);
        Ok(())
    } else {
        Err(ProtocolError("protocol line missing newline".into()))
    }
}

pub fn info_line(max_payload: usize, nonce: Option<&str>) -> Vec<u8> {
    let nonce = nonce
        .map(|nonce| format!(",\"nonce\":\"{nonce}\",\"auth_required\":true"))
        .unwrap_or_else(|| ",\"auth_required\":false".to_string());
    format!(
        "INFO {{\"server_id\":\"broker\",\"server_name\":\"broker\",\"version\":\"{}\",\"proto\":1,\"max_payload\":{max_payload}{nonce},\"tls_required\":false}}\r\n",
        env!("CARGO_PKG_VERSION"),
    )
    .into_bytes()
}

pub fn pong() -> &'static [u8] {
    b"PONG\r\n"
}

pub fn ok() -> &'static [u8] {
    b"+OK\r\n"
}

pub fn producer_ack(msg_id: &str, level: AckLevel, retained: bool, seq: Option<u64>) -> Vec<u8> {
    let seq = seq
        .map(|seq| seq.to_string())
        .unwrap_or_else(|| "-".to_string());
    format!("P-ACK {msg_id} {} OK {retained} {seq}\r\n", level as u8).into_bytes()
}

pub fn err(message: &str) -> Vec<u8> {
    format!("-ERR '{}'\r\n", message.replace('\'', "")).into_bytes()
}

pub fn msg(subject: &str, sid: &str, reply_to: Option<&str>, payload: &[u8]) -> Vec<u8> {
    let header = match reply_to {
        Some(reply_to) => format!("MSG {subject} {sid} {reply_to} {}\r\n", payload.len()),
        None => format!("MSG {subject} {sid} {}\r\n", payload.len()),
    };
    let mut frame = Vec::with_capacity(header.len() + payload.len() + 2);
    frame.extend_from_slice(header.as_bytes());
    frame.extend_from_slice(payload);
    frame.extend_from_slice(b"\r\n");
    frame
}

pub fn hmsg(
    subject: &str,
    sid: &str,
    reply_to: Option<&str>,
    headers: &[(&str, &str)],
    payload: &[u8],
) -> Vec<u8> {
    let mut header_block = String::from("NATS/1.0\r\n");
    for (name, value) in headers {
        header_block.push_str(name);
        header_block.push_str(": ");
        header_block.push_str(value);
        header_block.push_str("\r\n");
    }
    header_block.push_str("\r\n");

    let headers_len = header_block.len();
    let total_len = headers_len + payload.len();
    let protocol_header = match reply_to {
        Some(reply_to) => format!("HMSG {subject} {sid} {reply_to} {headers_len} {total_len}\r\n"),
        None => format!("HMSG {subject} {sid} {headers_len} {total_len}\r\n"),
    };
    let mut frame = Vec::with_capacity(protocol_header.len() + total_len + 2);
    frame.extend_from_slice(protocol_header.as_bytes());
    frame.extend_from_slice(header_block.as_bytes());
    frame.extend_from_slice(payload);
    frame.extend_from_slice(b"\r\n");
    frame
}

pub fn ack_subject(consumer_id: &str, seq: u64, delivery_id: u64) -> String {
    format!("_BROKER.ACK.{consumer_id}.{seq}.{delivery_id}")
}

pub fn parse_ack_subject(subject: &str) -> Option<AckSubject> {
    let mut parts = subject.split('.');
    if parts.next()? != "_BROKER" || parts.next()? != "ACK" {
        return None;
    }
    let consumer_id = parts.next()?.to_string();
    let seq = parts.next()?.parse().ok()?;
    let delivery_id = parts.next()?.parse().ok()?;
    if parts.next().is_some() {
        return None;
    }
    Some(AckSubject {
        consumer_id,
        seq,
        delivery_id,
    })
}

#[cfg(test)]
mod tests;
