use std::str;

use crate::consumer_commands::*;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    Connect {
        verbose: bool,
        durable_id: Option<String>,
        ack_timeout_ms: Option<u64>,
        max_in_flight: Option<usize>,
        protocol_version: Option<u32>,
        auth: Option<ConnectAuth>,
    },
    Ping,
    Pong,
    Pub {
        subject: String,
        reply_to: Option<String>,
        headers: Vec<(String, String)>,
        key: Option<Vec<u8>>,
        payload: Vec<u8>,
        ack: Option<ProducerAckRequest>,
    },
    Sub {
        subject: String,
        queue: Option<String>,
        sid: String,
        start: StartPosition,
    },
    Unsub {
        sid: String,
        max_messages: Option<usize>,
    },
    ConsumerCreate {
        name: String,
        filter_subject: String,
        start: StartPosition,
        retry_policy: RetryPolicy,
    },
    ConsumerDelete {
        name: String,
    },
    Fetch {
        name: String,
        max_messages: usize,
        max_bytes: usize,
        max_wait_ms: u64,
    },
    Ack {
        name: String,
        seq: u64,
        delivery_id: u64,
    },
    Nack {
        name: String,
        seq: u64,
        delivery_id: u64,
        delay_ms: u64,
    },
    Extend {
        name: String,
        seq: u64,
        delivery_id: u64,
        extension_ms: u64,
    },
    Credit {
        sid: String,
        messages: usize,
        bytes: usize,
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

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StartPosition {
    Earliest,
    #[default]
    Latest,
    Committed,
    Offset(u64),
    Timestamp(u64),
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub backoff: RetryBackoff,
    pub initial_delay_ms: u64,
    pub max_delay_ms: u64,
    pub jitter_percent: u8,
    pub terminal_action: RetryTerminalAction,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RetryBackoff {
    Fixed,
    Exponential,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RetryTerminalAction {
    DeadLetter,
    Discard,
    Pause,
    Retain,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: u32::MAX,
            backoff: RetryBackoff::Fixed,
            initial_delay_ms: 0,
            max_delay_ms: 300_000,
            jitter_percent: 0,
            terminal_action: RetryTerminalAction::Retain,
        }
    }
}

impl RetryPolicy {
    pub fn delay_ms(&self, attempt: u32) -> u64 {
        let exponent = attempt.saturating_sub(1).min(63);
        let multiplier = match self.backoff {
            RetryBackoff::Fixed => 1,
            RetryBackoff::Exponential => 1u64 << exponent,
        };
        self.initial_delay_ms
            .saturating_mul(multiplier)
            .min(self.max_delay_ms)
    }
}

impl AckLevel {
    fn parse(value: &str) -> Result<Self, ProtocolError> {
        match value {
            "0" => Ok(Self::Accepted),
            "1" => Ok(Self::Durable),
            "2" => Ok(Self::HighDurability),
            "3" => Ok(Self::ClusterDurable),
            _ => Err(ProtocolError("Morrow-QoS must be 0, 1, 2, or 3".into())),
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
        "CONN" => {
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
        "CONSUMER" => parse_consumer(parts).map(Some),
        "FETCH" => parse_fetch(parts).map(Some),
        "ACK" => parse_delivery_control(parts, "ACK").map(Some),
        "NACK" => parse_delivery_control(parts, "NACK").map(Some),
        "EXTEND" => parse_delivery_control(parts, "EXTEND").map(Some),
        "CREDIT" => parse_credit(parts).map(Some),
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
        .map_err(|err| ProtocolError(format!("invalid CONN payload: {err}")))?;
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
    let protocol_version = get_u64(&value, "protocol_version")?
        .map(|value| {
            value
                .try_into()
                .map_err(|_| ProtocolError("protocol_version is too large".into()))
        })
        .transpose()?;
    let auth = parse_connect_auth(&value)?;
    Ok(Command::Connect {
        verbose,
        durable_id,
        ack_timeout_ms,
        max_in_flight,
        protocol_version,
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
            "CONN client_id and signature must be provided together".into(),
        )),
    }
}

fn get_bool(value: &serde_json::Value, key: &str) -> Result<Option<bool>, ProtocolError> {
    match value.get(key) {
        Some(serde_json::Value::Bool(value)) => Ok(Some(*value)),
        Some(_) => Err(ProtocolError(format!("CONN field {key} must be a boolean"))),
        None => Ok(None),
    }
}

fn get_string<'a>(
    value: &'a serde_json::Value,
    key: &str,
) -> Result<Option<&'a str>, ProtocolError> {
    match value.get(key) {
        Some(serde_json::Value::String(value)) => Ok(Some(value)),
        Some(_) => Err(ProtocolError(format!("CONN field {key} must be a string"))),
        None => Ok(None),
    }
}

fn get_u64(value: &serde_json::Value, key: &str) -> Result<Option<u64>, ProtocolError> {
    match value.get(key) {
        Some(serde_json::Value::Number(value)) => value
            .as_u64()
            .ok_or_else(|| ProtocolError(format!("CONN field {key} must be an unsigned integer")))
            .map(Some),
        Some(_) => Err(ProtocolError(format!(
            "CONN field {key} must be an unsigned integer"
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
    let fourth = parts.next();
    if parts.next().is_some() {
        return Err(ProtocolError("SUB has too many arguments".into()));
    }

    let (queue, sid, start) = match (third, fourth) {
        (None, None) => (None, second.to_string(), StartPosition::Latest),
        (Some(start), None) if start.starts_with('@') => {
            (None, second.to_string(), parse_start_position(start)?)
        }
        (Some(sid), None) => (
            Some(second.to_string()),
            sid.to_string(),
            StartPosition::Latest,
        ),
        (Some(sid), Some(start)) => (
            Some(second.to_string()),
            sid.to_string(),
            parse_start_position(start)?,
        ),
        (None, Some(_)) => unreachable!(),
    };

    Ok(Command::Sub {
        subject,
        queue,
        sid,
        start,
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
        headers: Vec::new(),
        key: None,
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
    let key = header_value(&headers, "Morrow-Key").map(|value| value.as_bytes().to_vec());
    let headers = headers
        .into_iter()
        .filter(|(name, _)| {
            !name.eq_ignore_ascii_case("Morrow-QoS")
                && !name.eq_ignore_ascii_case("Morrow-Msg-Id")
                && !name.eq_ignore_ascii_case("Morrow-Key")
        })
        .collect();

    Ok(Command::Pub {
        subject,
        reply_to,
        headers,
        key,
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
        Some("MORROW/1.0") => {}
        _ => return Err(ProtocolError("HPUB headers missing MORROW/1.0 line".into())),
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
    let Some(level) = header_value(headers, "Morrow-QoS") else {
        return Ok(None);
    };
    let level = AckLevel::parse(level)?;
    let msg_id = header_value(headers, "Morrow-Msg-Id")
        .ok_or_else(|| ProtocolError("Morrow-Msg-Id is required when Morrow-QoS is set".into()))?;
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
            "Morrow-Msg-Id must be non-empty, at most 128 bytes, and contain no whitespace".into(),
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

#[cfg(test)]
mod tests;
