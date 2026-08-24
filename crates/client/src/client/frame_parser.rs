use super::*;

pub(super) async fn parse_frame(
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
        "P-ACK" => parse_producer_ack(parts).map(Some),
        "C-OK" => parse_consumer_ok(parts).map(Some),
        "D-OK" => parse_delivery_control_ok(parts).map(Some),
        "BATCH" => parse_batch(parts).map(Some),
        "DDELIVER" => parse_durable_message(stream, parts, max_payload)
            .await
            .map(Some),
        "G-OK" => parse_group_ok(parts).map(Some),
        "DELIVER" => parse_msg(stream, parts, max_payload).await.map(Some),
        "HDELIVER" => parse_hmsg(stream, parts, max_payload).await.map(Some),
        _ => Err(ClientError::msg(format!("unsupported server frame {op}"))),
    }
}

fn parse_group_ok<'a>(mut parts: impl Iterator<Item = &'a str>) -> Result<ServerFrame> {
    let operation = next_part(&mut parts, "G-OK missing operation")?.to_string();
    if operation == "JOIN" || operation == "HEARTBEAT" {
        let group = next_part(&mut parts, "G-OK group response missing group")?.to_string();
        let generation = next_part(&mut parts, "G-OK group response missing generation")?
            .parse()
            .map_err(|_| ClientError::msg("G-OK group generation is invalid"))?;
        let assignment = parts.next().unwrap_or_default();
        let partitions = if assignment.is_empty() {
            Vec::new()
        } else {
            assignment
                .split(',')
                .map(|partition| {
                    partition
                        .parse()
                        .map_err(|_| ClientError::msg("G-OK group partition is invalid"))
                })
                .collect::<Result<Vec<_>>>()?
        };
        reject_extra(&mut parts, "G-OK group response")?;
        Ok(ServerFrame::GroupOk {
            operation,
            group: Some(group),
            generation: Some(generation),
            partitions,
        })
    } else {
        reject_extra(&mut parts, "G-OK")?;
        Ok(ServerFrame::GroupOk {
            operation,
            group: None,
            generation: None,
            partitions: Vec::new(),
        })
    }
}

pub(super) fn parse_info(line: &str) -> Result<Info> {
    let payload = line
        .strip_prefix("INFO")
        .map(str::trim)
        .ok_or_else(|| ClientError::msg("invalid INFO frame"))?;
    let value: serde_json::Value = serde_json::from_str(payload)
        .map_err(|err| ClientError::with_source("parsing INFO JSON", err))?;
    Ok(Info {
        raw: payload.to_string(),
        proto: value
            .get("proto")
            .and_then(serde_json::Value::as_u64)
            .and_then(|value| value.try_into().ok())
            .unwrap_or(1),
        protocol_versions: value
            .get("protocol_versions")
            .and_then(serde_json::Value::as_array)
            .map(|versions| {
                versions
                    .iter()
                    .filter_map(serde_json::Value::as_u64)
                    .filter_map(|value| value.try_into().ok())
                    .collect()
            })
            .unwrap_or_else(|| vec![1]),
        encodings: value
            .get("encodings")
            .and_then(serde_json::Value::as_array)
            .map(|encodings| {
                encodings
                    .iter()
                    .filter_map(serde_json::Value::as_str)
                    .filter_map(|encoding| match encoding {
                        "text" => Some(protocol::WireEncoding::Text),
                        "cbor" => Some(protocol::WireEncoding::Cbor),
                        _ => None,
                    })
                    .collect()
            })
            .unwrap_or_else(|| vec![protocol::WireEncoding::Text]),
        features: value
            .get("features")
            .and_then(serde_json::Value::as_array)
            .map(|features| {
                features
                    .iter()
                    .filter_map(serde_json::Value::as_str)
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default(),
        max_frame_size: value
            .get("max_frame_size")
            .and_then(serde_json::Value::as_u64)
            .and_then(|value| value.try_into().ok())
            .unwrap_or(16 * 1024 * 1024),
        max_metadata_size: value
            .get("max_metadata_size")
            .and_then(serde_json::Value::as_u64)
            .and_then(|value| value.try_into().ok())
            .unwrap_or(16 * 1024 * 1024),
        max_payload_size: value
            .get("max_payload_size")
            .and_then(serde_json::Value::as_u64)
            .and_then(|value| value.try_into().ok())
            .unwrap_or(16 * 1024 * 1024),
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

pub(super) fn parse_consumer_ok<'a>(
    mut parts: impl Iterator<Item = &'a str>,
) -> Result<ServerFrame> {
    let operation = next_part(&mut parts, "C-OK missing operation")?.to_string();
    let name = next_part(&mut parts, "C-OK missing consumer name")?.to_string();
    reject_extra(&mut parts, "C-OK")?;
    Ok(ServerFrame::ConsumerOk { operation, name })
}

pub(super) fn parse_delivery_control_ok<'a>(
    mut parts: impl Iterator<Item = &'a str>,
) -> Result<ServerFrame> {
    let operation = next_part(&mut parts, "D-OK missing operation")?.to_string();
    let name = next_part(&mut parts, "D-OK missing consumer name")?.to_string();
    let seq = parse_number(
        next_part(&mut parts, "D-OK missing sequence")?,
        "D-OK sequence",
    )?;
    let delivery_id = parse_number(
        next_part(&mut parts, "D-OK missing delivery id")?,
        "D-OK delivery id",
    )?;
    reject_extra(&mut parts, "D-OK")?;
    Ok(ServerFrame::DeliveryControlOk {
        operation,
        name,
        seq,
        delivery_id,
    })
}

pub(super) fn parse_batch<'a>(mut parts: impl Iterator<Item = &'a str>) -> Result<ServerFrame> {
    let name = next_part(&mut parts, "BATCH missing consumer name")?.to_string();
    let messages = parse_number(
        next_part(&mut parts, "BATCH missing message count")?,
        "BATCH message count",
    )?;
    let bytes = parse_number(
        next_part(&mut parts, "BATCH missing byte count")?,
        "BATCH byte count",
    )?;
    reject_extra(&mut parts, "BATCH")?;
    Ok(ServerFrame::Batch {
        name,
        messages,
        bytes,
    })
}

pub(super) async fn parse_durable_message<'a>(
    stream: &mut BufReader<Box<dyn ClientStream>>,
    mut parts: impl Iterator<Item = &'a str>,
    max_payload: usize,
) -> Result<ServerFrame> {
    let consumer = next_part(&mut parts, "DDELIVER missing consumer name")?.to_string();
    let subject = next_part(&mut parts, "DDELIVER missing subject")?.to_string();
    let reply_to = match next_part(&mut parts, "DDELIVER missing reply subject")? {
        "-" => None,
        reply_to => Some(reply_to.to_string()),
    };
    let durable_stream = next_part(&mut parts, "DDELIVER missing stream")?.to_string();
    let partition = parse_number(
        next_part(&mut parts, "DDELIVER missing partition")?,
        "DDELIVER partition",
    )?;
    let offset = parse_number(
        next_part(&mut parts, "DDELIVER missing offset")?,
        "DDELIVER offset",
    )?;
    let key = match next_part(&mut parts, "DDELIVER missing key")? {
        "-" => None,
        key => Some(decode_hex(key, "DDELIVER key")?),
    };
    let timestamp_ms = parse_number(
        next_part(&mut parts, "DDELIVER missing timestamp")?,
        "DDELIVER timestamp",
    )?;
    let attempt = parse_number(
        next_part(&mut parts, "DDELIVER missing attempt")?,
        "DDELIVER attempt",
    )?;
    let lease_deadline_ms = parse_number(
        next_part(&mut parts, "DDELIVER missing lease deadline")?,
        "DDELIVER lease deadline",
    )?;
    let seq = parse_number(
        next_part(&mut parts, "DDELIVER missing sequence")?,
        "DDELIVER sequence",
    )?;
    let delivery_id = parse_number(
        next_part(&mut parts, "DDELIVER missing delivery id")?,
        "DDELIVER delivery id",
    )?;
    let headers_len = parse_frame_len(
        next_part(&mut parts, "DDELIVER missing headers length")?,
        "DDELIVER headers length",
    )?;
    let total_len = parse_frame_len(
        next_part(&mut parts, "DDELIVER missing total length")?,
        "DDELIVER total length",
    )?;
    reject_extra(&mut parts, "DDELIVER")?;
    if headers_len > total_len {
        return Err(ClientError::msg(
            "DDELIVER headers length exceeds total frame length",
        ));
    }
    if total_len > max_payload {
        return Err(ClientError::msg(format!(
            "DDELIVER total length {total_len} exceeds max payload {max_payload}"
        )));
    }
    let mut body = vec![0; total_len + 2];
    stream
        .read_exact(&mut body)
        .await
        .map_err(|err| ClientError::with_source("reading DDELIVER payload", err))?;
    if &body[total_len..] != b"\r\n" {
        return Err(ClientError::msg(
            "DDELIVER payload must be followed by CRLF",
        ));
    }
    body.truncate(total_len);
    let payload = body.split_off(headers_len);
    let headers = parse_headers(&body)?;
    Ok(ServerFrame::DurableMessage(DurableMessage {
        consumer,
        subject,
        reply_to,
        headers,
        stream: durable_stream,
        partition,
        offset,
        key,
        timestamp_ms,
        attempt,
        lease_deadline_ms,
        seq,
        delivery_id,
        payload,
    }))
}

fn next_part<'a>(parts: &mut impl Iterator<Item = &'a str>, message: &str) -> Result<&'a str> {
    parts.next().ok_or_else(|| ClientError::msg(message))
}

fn reject_extra<'a>(parts: &mut impl Iterator<Item = &'a str>, frame: &str) -> Result<()> {
    if parts.next().is_some() {
        return Err(ClientError::msg(format!("{frame} has too many arguments")));
    }
    Ok(())
}

fn parse_number<T: std::str::FromStr>(value: &str, field: &str) -> Result<T> {
    value
        .parse()
        .map_err(|_| ClientError::msg(format!("{field} must be an integer")))
}

pub(super) fn parse_producer_ack<'a>(
    mut parts: impl Iterator<Item = &'a str>,
) -> Result<ServerFrame> {
    let msg_id = parts
        .next()
        .ok_or_else(|| ClientError::msg("P-ACK missing message id"))?
        .to_string();
    let level = match parts
        .next()
        .ok_or_else(|| ClientError::msg("P-ACK missing level"))?
    {
        "0" => protocol::AckLevel::Accepted,
        "1" => protocol::AckLevel::Durable,
        "2" => protocol::AckLevel::HighDurability,
        "3" => protocol::AckLevel::ClusterDurable,
        _ => return Err(ClientError::msg("P-ACK level must be 0, 1, 2, or 3")),
    };
    match parts
        .next()
        .ok_or_else(|| ClientError::msg("P-ACK missing status"))?
    {
        "OK" => {}
        status => {
            return Err(ClientError::msg(format!(
                "unsupported P-ACK status {status}"
            )));
        }
    }
    let retained = match parts
        .next()
        .ok_or_else(|| ClientError::msg("P-ACK missing retained flag"))?
    {
        "true" => true,
        "false" => false,
        _ => {
            return Err(ClientError::msg(
                "P-ACK retained flag must be true or false",
            ));
        }
    };
    let seq = match parts
        .next()
        .ok_or_else(|| ClientError::msg("P-ACK missing sequence"))?
    {
        "-" => None,
        value => Some(
            value
                .parse()
                .map_err(|_| ClientError::msg("P-ACK sequence must be an integer"))?,
        ),
    };
    let stream = parts.next().map(str::to_string);
    let partition = parse_optional_position(&mut parts, "partition")?;
    let offset = parse_optional_position(&mut parts, "offset")?;
    let partitioning_epoch = parse_optional_position(&mut parts, "partitioning epoch")?;
    let leader_epoch = parse_optional_position(&mut parts, "leader epoch")?;
    if stream.is_some() && leader_epoch.is_none() {
        return Err(ClientError::msg(
            "P-ACK has an incomplete partition position",
        ));
    }
    if parts.next().is_some() {
        return Err(ClientError::msg("P-ACK has too many arguments"));
    }
    let partition = partition
        .map(|value| {
            value
                .try_into()
                .map_err(|_| ClientError::msg("P-ACK partition is too large"))
        })
        .transpose()?;
    Ok(ServerFrame::ProducerAck(ProducerAck {
        msg_id,
        level,
        retained,
        seq,
        stream,
        partition,
        offset,
        partitioning_epoch,
        leader_epoch,
    }))
}

fn parse_optional_position<'a>(
    parts: &mut impl Iterator<Item = &'a str>,
    field: &str,
) -> Result<Option<u64>> {
    parts
        .next()
        .map(|value| {
            value
                .parse()
                .map_err(|_| ClientError::msg(format!("P-ACK {field} must be an integer")))
        })
        .transpose()
}

pub(super) async fn parse_msg<'a>(
    stream: &mut BufReader<Box<dyn ClientStream>>,
    mut parts: impl Iterator<Item = &'a str>,
    max_payload: usize,
) -> Result<ServerFrame> {
    let subject = parts
        .next()
        .ok_or_else(|| ClientError::msg("DELIVER missing subject"))?
        .to_string();
    let sid = parts
        .next()
        .ok_or_else(|| ClientError::msg("DELIVER missing sid"))?
        .to_string();
    let third = parts
        .next()
        .ok_or_else(|| ClientError::msg("DELIVER missing payload size"))?;
    let fourth = parts.next();
    if parts.next().is_some() {
        return Err(ClientError::msg("DELIVER has too many arguments"));
    }
    let (reply_to, size_token) = match fourth {
        Some(size) => (Some(third.to_string()), size),
        None => (None, third),
    };
    let size = size_token
        .parse::<usize>()
        .map_err(|_| ClientError::msg("DELIVER payload size must be an integer"))?;
    if size > max_payload {
        return Err(ClientError::msg(format!(
            "DELIVER payload size {size} exceeds max payload {max_payload}"
        )));
    }
    let mut payload = vec![0; size + 2];
    stream
        .read_exact(&mut payload)
        .await
        .map_err(|err| ClientError::with_source("reading DELIVER payload", err))?;
    if &payload[size..] != b"\r\n" {
        return Err(ClientError::msg("DELIVER payload must be followed by CRLF"));
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
        key: None,
        timestamp_ms: None,
        headers: Vec::new(),
        payload,
    }))
}

pub(super) async fn parse_hmsg<'a>(
    stream: &mut BufReader<Box<dyn ClientStream>>,
    mut parts: impl Iterator<Item = &'a str>,
    max_payload: usize,
) -> Result<ServerFrame> {
    let subject = parts
        .next()
        .ok_or_else(|| ClientError::msg("HDELIVER missing subject"))?
        .to_string();
    let sid = parts
        .next()
        .ok_or_else(|| ClientError::msg("HDELIVER missing sid"))?
        .to_string();
    let third = parts
        .next()
        .ok_or_else(|| ClientError::msg("HDELIVER missing headers length"))?;
    let fourth = parts
        .next()
        .ok_or_else(|| ClientError::msg("HDELIVER missing total length"))?;
    let fifth = parts.next();
    if parts.next().is_some() {
        return Err(ClientError::msg("HDELIVER has too many arguments"));
    }
    let (reply_to, headers_len_token, total_len_token) = match fifth {
        Some(total_len) => (Some(third.to_string()), fourth, total_len),
        None => (None, third, fourth),
    };
    let headers_len = parse_frame_len(headers_len_token, "HDELIVER headers length")?;
    let total_len = parse_frame_len(total_len_token, "HDELIVER total length")?;
    if headers_len > total_len {
        return Err(ClientError::msg(
            "HDELIVER headers length exceeds total frame length",
        ));
    }
    if total_len > max_payload {
        return Err(ClientError::msg(format!(
            "HDELIVER total length {total_len} exceeds max payload {max_payload}"
        )));
    }

    let mut frame = vec![0; total_len + 2];
    stream
        .read_exact(&mut frame)
        .await
        .map_err(|err| ClientError::with_source("reading HDELIVER payload", err))?;
    if &frame[total_len..] != b"\r\n" {
        return Err(ClientError::msg(
            "HDELIVER payload must be followed by CRLF",
        ));
    }
    frame.truncate(total_len);
    let payload = frame.split_off(headers_len);
    let headers = parse_headers(&frame)?;
    let ack_subject = header_value(&headers, "Morrow-Ack").map(str::to_string);
    let key = header_value(&headers, "Morrow-Key-Hex")
        .map(|key| decode_hex(key, "Morrow-Key-Hex"))
        .transpose()?;
    let timestamp_ms = header_value(&headers, "Morrow-Timestamp")
        .map(|value| parse_number(value, "Morrow-Timestamp"))
        .transpose()?;
    Ok(ServerFrame::Message(Message {
        subject,
        sid,
        reply_to,
        ack_subject,
        key,
        timestamp_ms,
        headers,
        payload,
    }))
}

fn decode_hex(value: &str, field: &str) -> Result<Vec<u8>> {
    if !value.len().is_multiple_of(2) {
        return Err(ClientError::msg(format!("{field} must be even-length hex")));
    }
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let text = std::str::from_utf8(pair).expect("hex pair is ASCII-sized");
            u8::from_str_radix(text, 16)
                .map_err(|_| ClientError::msg(format!("{field} must be hex")))
        })
        .collect()
}

pub(super) fn parse_frame_len(value: &str, field: &str) -> Result<usize> {
    value
        .parse::<usize>()
        .map_err(|_| ClientError::msg(format!("{field} must be an integer")))
}

pub(super) fn parse_headers(bytes: &[u8]) -> Result<Vec<(String, String)>> {
    let text = std::str::from_utf8(bytes)
        .map_err(|err| ClientError::with_source("HDELIVER headers are not UTF-8", err))?;
    let mut lines = text.split("\r\n");
    match lines.next() {
        Some("MORROW/1.0") => {}
        _ => return Err(ClientError::msg("HDELIVER headers missing MORROW/1.0 line")),
    }
    let mut headers = Vec::new();
    for line in lines {
        if line.is_empty() {
            break;
        }
        let (name, value) = line
            .split_once(':')
            .ok_or_else(|| ClientError::msg("malformed HDELIVER header line"))?;
        headers.push((name.trim().to_string(), value.trim().to_string()));
    }
    Ok(headers)
}

pub(super) fn header_value<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(header_name, _)| header_name.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_str())
}

pub(super) fn trim_crlf(line: &mut Vec<u8>) -> Result<()> {
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

pub(super) fn tls_config(root_cert_file: impl AsRef<Path>) -> Result<ClientConfig> {
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

pub(super) fn load_certs(path: impl AsRef<Path>) -> Result<Vec<CertificateDer<'static>>> {
    let path = path.as_ref();
    let certs = broker_pem::load_certificates(path)
        .map_err(|err| ClientError::with_source("reading root certificate PEM", err))?;
    if certs.is_empty() {
        return Err(ClientError::msg(
            "root certificate file contains no certificates",
        ));
    }
    Ok(certs)
}
