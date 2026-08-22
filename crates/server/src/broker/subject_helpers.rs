use super::*;

pub(super) fn consumer_id(
    durable_id: &str,
    queue: Option<&str>,
    subject: &str,
    sid: &str,
) -> String {
    match queue {
        Some(queue) => format!("queue-{queue}-{}", hex(subject.as_bytes())),
        None => format!("durable-{durable_id}-{sid}"),
    }
}

pub(super) fn is_inbox_subscription(subject: &str) -> bool {
    subject == "_MORROW/INBOX/**" || subject.starts_with("_MORROW/INBOX/")
}

pub(super) fn is_inbox_publish(subject: &str) -> bool {
    subject.starts_with("_MORROW/INBOX/")
}

pub(super) fn inbox_belongs_to(subject: &str, client_id: &str) -> bool {
    subject
        .strip_prefix("_MORROW/INBOX/")
        .is_some_and(|tail| tail == client_id || tail.starts_with(&format!("{client_id}/")))
}

pub(super) fn hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

pub(super) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

pub(super) fn format_leader_id(leader_id: Option<u64>) -> String {
    leader_id
        .map(|leader_id| leader_id.to_string())
        .unwrap_or_else(|| "none".to_string())
}
