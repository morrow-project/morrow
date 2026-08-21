use crate::protocol::{Command, ProtocolError, StartPosition, validate_identifier};

pub(super) fn parse_consumer<'a>(
    mut parts: impl Iterator<Item = &'a str>,
) -> Result<Command, ProtocolError> {
    let operation = required(&mut parts, "CONSUMER requires CREATE or DELETE")?;
    let name = required(&mut parts, "CONSUMER requires a name")?.to_string();
    validate_identifier("consumer name", &name)?;
    match operation.to_ascii_uppercase().as_str() {
        "CREATE" => {
            let filter_subject = required(&mut parts, "CONSUMER CREATE requires a filter")?;
            let start = parts
                .next()
                .map(parse_start_position)
                .transpose()?
                .unwrap_or_default();
            no_more(parts, "CONSUMER CREATE")?;
            Ok(Command::ConsumerCreate {
                name,
                filter_subject: filter_subject.to_string(),
                start,
            })
        }
        "DELETE" => {
            no_more(parts, "CONSUMER DELETE")?;
            Ok(Command::ConsumerDelete { name })
        }
        _ => Err(ProtocolError(
            "CONSUMER operation must be CREATE or DELETE".into(),
        )),
    }
}

pub(super) fn parse_fetch<'a>(
    mut parts: impl Iterator<Item = &'a str>,
) -> Result<Command, ProtocolError> {
    let name = required(&mut parts, "FETCH requires a consumer name")?.to_string();
    let max_messages = parse_usize(
        required(&mut parts, "FETCH requires max messages")?,
        "FETCH max messages",
    )?;
    let max_bytes = parse_usize(
        required(&mut parts, "FETCH requires max bytes")?,
        "FETCH max bytes",
    )?;
    let max_wait_ms = parse_u64(
        required(&mut parts, "FETCH requires max wait")?,
        "FETCH max wait",
    )?;
    no_more(parts, "FETCH")?;
    if max_messages == 0 || max_bytes == 0 {
        return Err(ProtocolError(
            "FETCH max messages and max bytes must be greater than zero".into(),
        ));
    }
    Ok(Command::Fetch {
        name,
        max_messages,
        max_bytes,
        max_wait_ms,
    })
}

pub(super) fn parse_delivery_control<'a>(
    mut parts: impl Iterator<Item = &'a str>,
    operation: &str,
) -> Result<Command, ProtocolError> {
    let name = required(&mut parts, &format!("{operation} requires a consumer name"))?.to_string();
    let seq = parse_u64(
        required(&mut parts, &format!("{operation} requires a sequence"))?,
        "delivery sequence",
    )?;
    let delivery_id = parse_u64(
        required(&mut parts, &format!("{operation} requires a delivery id"))?,
        "delivery id",
    )?;
    match operation {
        "ACK" => {
            no_more(parts, operation)?;
            Ok(Command::Ack {
                name,
                seq,
                delivery_id,
            })
        }
        "NACK" => {
            let delay_ms = parse_u64(required(&mut parts, "NACK requires a delay")?, "NACK delay")?;
            no_more(parts, operation)?;
            Ok(Command::Nack {
                name,
                seq,
                delivery_id,
                delay_ms,
            })
        }
        "EXTEND" => {
            let extension_ms = parse_u64(
                required(&mut parts, "EXTEND requires an extension")?,
                "EXTEND duration",
            )?;
            no_more(parts, operation)?;
            Ok(Command::Extend {
                name,
                seq,
                delivery_id,
                extension_ms,
            })
        }
        _ => unreachable!(),
    }
}

pub(super) fn parse_credit<'a>(
    mut parts: impl Iterator<Item = &'a str>,
) -> Result<Command, ProtocolError> {
    let sid = required(&mut parts, "CREDIT requires a sid")?.to_string();
    let messages = parse_usize(
        required(&mut parts, "CREDIT requires messages")?,
        "CREDIT messages",
    )?;
    let bytes = parse_usize(
        required(&mut parts, "CREDIT requires bytes")?,
        "CREDIT bytes",
    )?;
    no_more(parts, "CREDIT")?;
    if messages == 0 || bytes == 0 {
        return Err(ProtocolError(
            "CREDIT messages and bytes must be greater than zero".into(),
        ));
    }
    Ok(Command::Credit {
        sid,
        messages,
        bytes,
    })
}

pub(super) fn parse_start_position(value: &str) -> Result<StartPosition, ProtocolError> {
    match value {
        "@earliest" => Ok(StartPosition::Earliest),
        "@latest" => Ok(StartPosition::Latest),
        "@committed" => Ok(StartPosition::Committed),
        _ if value.starts_with("@offset:") => value[8..]
            .parse()
            .map(StartPosition::Offset)
            .map_err(|_| ProtocolError("SUB offset must be an integer".into())),
        _ if value.starts_with("@time:") => value[6..]
            .parse()
            .map(StartPosition::Timestamp)
            .map_err(|_| ProtocolError("SUB timestamp must be an integer".into())),
        _ => Err(ProtocolError("invalid start position".into())),
    }
}

fn required<'a>(
    parts: &mut impl Iterator<Item = &'a str>,
    message: &str,
) -> Result<&'a str, ProtocolError> {
    parts.next().ok_or_else(|| ProtocolError(message.into()))
}

fn no_more<'a>(
    mut parts: impl Iterator<Item = &'a str>,
    operation: &str,
) -> Result<(), ProtocolError> {
    if parts.next().is_some() {
        return Err(ProtocolError(format!("{operation} has too many arguments")));
    }
    Ok(())
}

fn parse_u64(value: &str, field: &str) -> Result<u64, ProtocolError> {
    value
        .parse()
        .map_err(|_| ProtocolError(format!("{field} must be an integer")))
}

fn parse_usize(value: &str, field: &str) -> Result<usize, ProtocolError> {
    value
        .parse()
        .map_err(|_| ProtocolError(format!("{field} must be an integer")))
}
