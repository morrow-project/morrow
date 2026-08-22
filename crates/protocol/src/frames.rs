use crate::protocol::{AckLevel, AckSubject};

pub fn info_line(max_payload: usize, nonce: Option<&str>) -> Vec<u8> {
    let nonce = nonce
        .map(|nonce| format!(",\"nonce\":\"{nonce}\",\"auth_required\":true"))
        .unwrap_or_else(|| ",\"auth_required\":false".to_string());
    format!(
        "INFO {{\"server_id\":\"morrow\",\"server_name\":\"Morrow\",\"version\":\"{}\",\"proto\":2,\"protocol_versions\":[1,2],\"max_payload\":{max_payload}{nonce},\"tls_required\":false}}\r\n",
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

pub fn producer_ack_with_position(
    msg_id: &str,
    level: AckLevel,
    retained: bool,
    seq: Option<u64>,
    position: Option<(&str, u32, u64, u64, u64)>,
) -> Vec<u8> {
    let mut frame = String::from_utf8(producer_ack(msg_id, level, retained, seq))
        .expect("producer ack is UTF-8");
    if let Some((stream, partition, offset, partitioning_epoch, leader_epoch)) = position {
        frame.truncate(frame.len() - 2);
        frame.push_str(&format!(
            " {stream} {partition} {offset} {partitioning_epoch} {leader_epoch}\r\n"
        ));
    }
    frame.into_bytes()
}

pub fn err(message: &str) -> Vec<u8> {
    format!("-ERR '{}'\r\n", message.replace('\'', "")).into_bytes()
}

pub fn msg(subject: &str, sid: &str, reply_to: Option<&str>, payload: &[u8]) -> Vec<u8> {
    let header = match reply_to {
        Some(reply_to) => format!("DELIVER {subject} {sid} {reply_to} {}\r\n", payload.len()),
        None => format!("DELIVER {subject} {sid} {}\r\n", payload.len()),
    };
    payload_frame(header, payload)
}

pub fn hmsg(
    subject: &str,
    sid: &str,
    reply_to: Option<&str>,
    headers: &[(&str, &str)],
    payload: &[u8],
) -> Vec<u8> {
    let mut header_block = String::from("MORROW/1.0\r\n");
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
        Some(reply_to) => {
            format!("HDELIVER {subject} {sid} {reply_to} {headers_len} {total_len}\r\n")
        }
        None => format!("HDELIVER {subject} {sid} {headers_len} {total_len}\r\n"),
    };
    let mut frame = Vec::with_capacity(protocol_header.len() + total_len + 2);
    frame.extend_from_slice(protocol_header.as_bytes());
    frame.extend_from_slice(header_block.as_bytes());
    frame.extend_from_slice(payload);
    frame.extend_from_slice(b"\r\n");
    frame
}

pub fn ack_subject(consumer_id: &str, seq: u64, delivery_id: u64) -> String {
    format!("_MORROW/ACK/{consumer_id}/{seq}/{delivery_id}")
}

pub fn parse_ack_subject(subject: &str) -> Option<AckSubject> {
    let mut parts = subject.split('/');
    if parts.next()? != "_MORROW" || parts.next()? != "ACK" {
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

pub fn consumer_ok(operation: &str, name: &str) -> Vec<u8> {
    format!("C-OK {operation} {name}\r\n").into_bytes()
}

pub fn control_ok(operation: &str, name: &str, seq: u64, delivery_id: u64) -> Vec<u8> {
    format!("D-OK {operation} {name} {seq} {delivery_id}\r\n").into_bytes()
}

pub fn batch(name: &str, messages: usize, bytes: usize) -> Vec<u8> {
    format!("BATCH {name} {messages} {bytes}\r\n").into_bytes()
}

#[allow(clippy::too_many_arguments)]
pub fn durable_message(
    name: &str,
    subject: &str,
    reply_to: Option<&str>,
    headers: &[(&str, &str)],
    stream: &str,
    partition: u32,
    offset: u64,
    key: Option<&[u8]>,
    timestamp_ms: u64,
    attempt: u32,
    lease_deadline_ms: u64,
    seq: u64,
    delivery_id: u64,
    payload: &[u8],
) -> Vec<u8> {
    let mut header_block = String::from("MORROW/1.0\r\n");
    for (header, value) in headers {
        header_block.push_str(header);
        header_block.push_str(": ");
        header_block.push_str(value);
        header_block.push_str("\r\n");
    }
    header_block.push_str("\r\n");
    let headers_len = header_block.len();
    let total_len = headers_len + payload.len();
    let reply_to = reply_to.unwrap_or("-");
    let key = key.map(hex).unwrap_or_else(|| "-".to_string());
    let protocol_header = format!(
        "DDELIVER {name} {subject} {reply_to} {stream} {partition} {offset} {key} {timestamp_ms} {attempt} {lease_deadline_ms} {seq} {delivery_id} {headers_len} {total_len}\r\n"
    );
    let mut frame = Vec::with_capacity(protocol_header.len() + total_len + 2);
    frame.extend_from_slice(protocol_header.as_bytes());
    frame.extend_from_slice(header_block.as_bytes());
    frame.extend_from_slice(payload);
    frame.extend_from_slice(b"\r\n");
    frame
}

#[allow(clippy::too_many_arguments)]
pub fn durable_message_encoded_len<'a, I>(
    name: &str,
    subject: &str,
    reply_to: Option<&str>,
    headers: I,
    stream: &str,
    partition: u32,
    offset: u64,
    key: Option<&[u8]>,
    timestamp_ms: u64,
    attempt: u32,
    lease_deadline_ms: u64,
    seq: u64,
    delivery_id: u64,
    payload: &[u8],
) -> usize
where
    I: IntoIterator<Item = (&'a str, &'a str)>,
{
    let headers_len = "MORROW/1.0\r\n".len()
        + headers
            .into_iter()
            .map(|(header, value)| header.len() + 2 + value.len() + 2)
            .sum::<usize>()
        + 2;
    let total_len = headers_len + payload.len();
    let reply_to = reply_to.unwrap_or("-");
    let key_len = key.map_or(1, |key| key.len().saturating_mul(2));
    "DDELIVER ".len()
        + name.len()
        + 1
        + subject.len()
        + 1
        + reply_to.len()
        + 1
        + stream.len()
        + 1
        + digits(partition as u64)
        + 1
        + digits(offset)
        + 1
        + key_len
        + 1
        + digits(timestamp_ms)
        + 1
        + digits(attempt as u64)
        + 1
        + digits(lease_deadline_ms)
        + 1
        + digits(seq)
        + 1
        + digits(delivery_id)
        + 1
        + digits(headers_len as u64)
        + 1
        + digits(total_len as u64)
        + 2
        + total_len
        + 2
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut value = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        value.push(DIGITS[(byte >> 4) as usize] as char);
        value.push(DIGITS[(byte & 0xf) as usize] as char);
    }
    value
}

fn digits(value: u64) -> usize {
    let mut value = value;
    let mut count = 1;
    while value >= 10 {
        value /= 10;
        count += 1;
    }
    count
}

fn payload_frame(header: String, payload: &[u8]) -> Vec<u8> {
    let mut frame = Vec::with_capacity(header.len() + payload.len() + 2);
    frame.extend_from_slice(header.as_bytes());
    frame.extend_from_slice(payload);
    frame.extend_from_slice(b"\r\n");
    frame
}
