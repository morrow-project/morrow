use super::*;

pub(super) fn hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

pub(super) fn default_inbox_prefix() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("_MORROW/INBOX/client/{:x}/{:x}", std::process::id(), nanos)
}

pub(super) fn inbox_prefix(client_id: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("_MORROW/INBOX/{client_id}/{:x}", nanos)
}

pub(super) fn decode_fixed<const N: usize>(value: &str, field: &str) -> Result<[u8; N]> {
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
pub(super) fn hex_value(byte: u8, field: &str) -> Result<u8> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(ClientError::msg(format!("{field} must be hex encoded"))),
    }
}
