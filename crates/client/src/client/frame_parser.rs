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
        "MSG" => parse_msg(stream, parts, max_payload).await.map(Some),
        "HMSG" => parse_hmsg(stream, parts, max_payload).await.map(Some),
        _ => Err(ClientError::msg(format!("unsupported server frame {op}"))),
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

pub(super) async fn parse_msg<'a>(
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

pub(super) async fn parse_hmsg<'a>(
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

pub(super) fn parse_frame_len(value: &str, field: &str) -> Result<usize> {
    value
        .parse::<usize>()
        .map_err(|_| ClientError::msg(format!("{field} must be an integer")))
}

pub(super) fn parse_headers(bytes: &[u8]) -> Result<Vec<(String, String)>> {
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
