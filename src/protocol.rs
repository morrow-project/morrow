use std::str;

use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    Connect {
        verbose: bool,
        durable_id: Option<String>,
        ack_timeout_ms: Option<u64>,
        max_in_flight: Option<usize>,
    },
    Ping,
    Pong,
    Pub {
        subject: String,
        reply_to: Option<String>,
        payload: Vec<u8>,
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
) -> Result<Option<Command>, ProtocolError> {
    let mut line = Vec::new();
    let read = reader
        .read_until(b'\n', &mut line)
        .await
        .map_err(|err| ProtocolError(format!("read failed: {err}")))?;
    if read == 0 {
        return Ok(None);
    }

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
        _ => Err(ProtocolError(format!("unsupported command {op}"))),
    }
}

fn parse_connect(payload: &str) -> Result<Command, ProtocolError> {
    let value: serde_json::Value = serde_json::from_str(payload)
        .map_err(|err| ProtocolError(format!("invalid CONNECT payload: {err}")))?;
    let verbose = value
        .get("verbose")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let durable_id = value
        .get("durable_id")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string);
    if let Some(durable_id) = &durable_id {
        validate_identifier("durable_id", durable_id)?;
    }
    let ack_timeout_ms = value
        .get("ack_timeout_ms")
        .and_then(serde_json::Value::as_u64);
    let max_in_flight = value
        .get("max_in_flight")
        .and_then(serde_json::Value::as_u64)
        .map(|value| {
            value
                .try_into()
                .map_err(|_| ProtocolError("max_in_flight is too large".into()))
        })
        .transpose()?;
    Ok(Command::Connect {
        verbose,
        durable_id,
        ack_timeout_ms,
        max_in_flight,
    })
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
    })
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

pub fn info_line(max_payload: usize) -> Vec<u8> {
    format!(
        "INFO {{\"server_id\":\"broker\",\"server_name\":\"broker\",\"version\":\"{}\",\"proto\":1,\"max_payload\":{max_payload},\"auth_required\":false,\"tls_required\":false}}\r\n",
        env!("CARGO_PKG_VERSION")
    )
    .into_bytes()
}

pub fn pong() -> &'static [u8] {
    b"PONG\r\n"
}

pub fn ok() -> &'static [u8] {
    b"+OK\r\n"
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
mod tests {
    use tokio::io::BufReader;

    use super::*;

    #[tokio::test]
    async fn parses_pub_with_payload() {
        let mut reader = BufReader::new(&b"PUB orders.created 5\r\nhello\r\n"[..]);
        let command = read_command(&mut reader, 1024).await.unwrap().unwrap();
        assert_eq!(
            command,
            Command::Pub {
                subject: "orders.created".into(),
                reply_to: None,
                payload: b"hello".to_vec()
            }
        );
    }

    #[tokio::test]
    async fn parses_connect_durable_metadata() {
        let mut reader = BufReader::new(
            &b"CONNECT {\"verbose\":true,\"durable_id\":\"client1\",\"ack_timeout_ms\":25,\"max_in_flight\":7}\r\n"[..],
        );
        let command = read_command(&mut reader, 1024).await.unwrap().unwrap();
        assert_eq!(
            command,
            Command::Connect {
                verbose: true,
                durable_id: Some("client1".into()),
                ack_timeout_ms: Some(25),
                max_in_flight: Some(7),
            }
        );
    }

    #[tokio::test]
    async fn parses_sub_variants() {
        let mut reader = BufReader::new(&b"SUB orders.* workers 7\r\n"[..]);
        let command = read_command(&mut reader, 1024).await.unwrap().unwrap();
        assert_eq!(
            command,
            Command::Sub {
                subject: "orders.*".into(),
                queue: Some("workers".into()),
                sid: "7".into()
            }
        );
    }

    #[tokio::test]
    async fn rejects_oversized_payload() {
        let mut reader = BufReader::new(&b"PUB orders.created 5\r\nhello\r\n"[..]);
        let err = read_command(&mut reader, 4).await.unwrap_err();
        assert!(err.0.contains("exceeds max payload"));
    }

    #[test]
    fn encodes_msg_frames() {
        assert_eq!(
            msg("orders.created", "1", None, b"ok"),
            b"MSG orders.created 1 2\r\nok\r\n"
        );
    }

    #[test]
    fn parses_ack_subjects() {
        assert_eq!(
            parse_ack_subject("_BROKER.ACK.consumer1.42.9"),
            Some(AckSubject {
                consumer_id: "consumer1".into(),
                seq: 42,
                delivery_id: 9,
            })
        );
        assert!(parse_ack_subject("_BROKER.ACK.consumer1.nope.9").is_none());
    }
}
