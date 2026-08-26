use super::*;

impl Client {
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
        self.write_payload(payload, "PUB").await
    }

    pub async fn publish_with_headers(
        &mut self,
        subject: &str,
        reply_to: Option<&str>,
        payload: &[u8],
        headers: &[(String, String)],
    ) -> Result<()> {
        let headers = build_headers(headers, &[])?;
        self.write_hpub(subject, reply_to, payload, &headers).await
    }

    pub async fn publish_with_key_and_headers(
        &mut self,
        subject: &str,
        reply_to: Option<&str>,
        payload: &[u8],
        key: &str,
        headers: &[(String, String)],
    ) -> Result<()> {
        validate_key(Some(key))?;
        let reserved = [("Morrow-Key", key.to_string())];
        let headers = build_headers(headers, &reserved)?;
        self.write_hpub(subject, reply_to, payload, &headers).await
    }

    pub async fn publish_with_qos(
        &mut self,
        subject: &str,
        reply_to: Option<&str>,
        payload: &[u8],
        level: protocol::AckLevel,
        msg_id: &str,
    ) -> Result<ProducerAck> {
        self.publish_with_qos_key_and_headers(subject, reply_to, payload, level, msg_id, None, &[])
            .await
    }

    pub async fn publish_with_qos_and_key(
        &mut self,
        subject: &str,
        reply_to: Option<&str>,
        payload: &[u8],
        level: protocol::AckLevel,
        msg_id: &str,
        key: Option<&str>,
    ) -> Result<ProducerAck> {
        self.publish_with_qos_key_and_headers(subject, reply_to, payload, level, msg_id, key, &[])
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn publish_with_qos_key_and_headers(
        &mut self,
        subject: &str,
        reply_to: Option<&str>,
        payload: &[u8],
        level: protocol::AckLevel,
        msg_id: &str,
        key: Option<&str>,
        headers: &[(String, String)],
    ) -> Result<ProducerAck> {
        self.publish_with_qos_key_headers_and_producer(
            subject, reply_to, payload, level, msg_id, key, headers, None,
        )
        .await
    }

    pub async fn publish_batch_with_qos_and_key(
        &mut self,
        requests: &[BatchPublishRequest<'_>],
    ) -> Result<Vec<ProducerAck>> {
        let requests = requests
            .iter()
            .map(|request| BatchPublishRequestWithHeaders {
                subject: request.subject,
                payload: request.payload,
                level: request.level,
                msg_id: request.msg_id,
                key: request.key,
                headers: &[],
            })
            .collect::<Vec<_>>();
        self.publish_batch_with_qos_key_and_headers(&requests).await
    }

    pub async fn publish_batch_with_qos_key_and_headers(
        &mut self,
        requests: &[BatchPublishRequestWithHeaders<'_>],
    ) -> Result<Vec<ProducerAck>> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }
        let mut message_ids = HashSet::with_capacity(requests.len());
        for request in requests {
            validate_producer_msg_id(request.msg_id)?;
            if !message_ids.insert(request.msg_id) {
                return Err(ClientError::msg("batch publish message IDs must be unique"));
            }
            validate_key(request.key)?;
            let mut reserved = vec![
                ("Morrow-QoS", (request.level as u8).to_string()),
                ("Morrow-Msg-Id", request.msg_id.to_string()),
            ];
            if let Some(key) = request.key {
                reserved.push(("Morrow-Key", key.to_string()));
            }
            let headers = build_headers(request.headers, &reserved)?;
            self.write_hpub(request.subject, None, request.payload, &headers)
                .await?;
        }
        let mut acknowledgements = HashMap::with_capacity(requests.len());
        while acknowledgements.len() < requests.len() {
            match self.next_frame().await? {
                Some(ServerFrame::ProducerAck(ack)) => {
                    acknowledgements.insert(ack.msg_id.clone(), ack);
                }
                Some(ServerFrame::Err(error)) => return Err(ClientError::msg(error)),
                Some(ServerFrame::Message(_)) => {
                    return Err(ClientError::msg("unexpected message during batch publish"));
                }
                Some(frame) => {
                    return Err(ClientError::msg(format!(
                        "unexpected frame in batch publish: {frame:?}"
                    )));
                }
                None => return Err(ClientError::msg("connection closed before batch P-ACKs")),
            }
        }
        requests
            .iter()
            .map(|request| {
                acknowledgements
                    .remove(request.msg_id)
                    .ok_or_else(|| ClientError::msg("missing batch P-ACK"))
            })
            .collect()
    }

    pub async fn publish_with_producer_sequence(
        &mut self,
        subject: &str,
        reply_to: Option<&str>,
        payload: &[u8],
        level: protocol::AckLevel,
        msg_id: &str,
        key: Option<&str>,
        producer: &protocol::ProducerSequence,
    ) -> Result<ProducerAck> {
        self.publish_with_qos_key_headers_and_producer(
            subject,
            reply_to,
            payload,
            level,
            msg_id,
            key,
            &[],
            Some(producer),
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn publish_with_qos_key_headers_and_producer(
        &mut self,
        subject: &str,
        reply_to: Option<&str>,
        payload: &[u8],
        level: protocol::AckLevel,
        msg_id: &str,
        key: Option<&str>,
        application_headers: &[(String, String)],
        producer: Option<&protocol::ProducerSequence>,
    ) -> Result<ProducerAck> {
        validate_producer_msg_id(msg_id)?;
        validate_key(key)?;
        let mut reserved = vec![
            ("Morrow-QoS", (level as u8).to_string()),
            ("Morrow-Msg-Id", msg_id.to_string()),
        ];
        if let Some(producer) = producer {
            reserved.extend([
                ("Morrow-Producer-Id", producer.producer_id.clone()),
                ("Morrow-Producer-Epoch", producer.epoch.to_string()),
                ("Morrow-Producer-Sequence", producer.sequence.to_string()),
            ]);
        }
        if let Some(key) = key {
            reserved.push(("Morrow-Key", key.to_string()));
        }
        let headers = build_headers(application_headers, &reserved)?;
        self.write_hpub(subject, reply_to, payload, &headers)
            .await?;
        loop {
            match self.next_frame().await? {
                Some(ServerFrame::ProducerAck(ack)) if ack.msg_id == msg_id => return Ok(ack),
                Some(ServerFrame::ProducerAck(_)) => {}
                Some(ServerFrame::Err(err)) => return Err(ClientError::msg(err)),
                Some(frame) => {
                    return Err(ClientError::msg(format!(
                        "expected P-ACK after HPUB, got {frame:?}"
                    )));
                }
                None => return Err(ClientError::msg("connection closed before P-ACK")),
            }
        }
    }

    async fn write_hpub(
        &mut self,
        subject: &str,
        reply_to: Option<&str>,
        payload: &[u8],
        headers: &[u8],
    ) -> Result<()> {
        let total_len = headers.len().saturating_add(payload.len());
        if total_len > self.max_payload {
            return Err(ClientError::msg(format!(
                "HPUB total length {total_len} exceeds max payload {}",
                self.max_payload
            )));
        }
        match reply_to {
            Some(reply_to) => {
                self.write_line(&format!(
                    "HPUB {subject} {reply_to} {} {total_len}",
                    headers.len()
                ))
                .await?;
            }
            None => {
                self.write_line(&format!("HPUB {subject} {} {total_len}", headers.len()))
                    .await?;
            }
        }
        self.stream
            .get_mut()
            .write_all(headers)
            .await
            .map_err(|err| ClientError::with_source("writing HPUB headers", err))?;
        self.write_payload(payload, "HPUB").await
    }

    async fn write_payload(&mut self, payload: &[u8], operation: &str) -> Result<()> {
        self.stream
            .get_mut()
            .write_all(payload)
            .await
            .map_err(|err| ClientError::with_source(format!("writing {operation} payload"), err))?;
        self.stream
            .get_mut()
            .write_all(b"\r\n")
            .await
            .map_err(|err| {
                ClientError::with_source(format!("writing {operation} payload terminator"), err)
            })
    }
}

fn build_headers(application: &[(String, String)], reserved: &[(&str, String)]) -> Result<Vec<u8>> {
    let mut block = String::from("MORROW/1.0\r\n");
    for (name, value) in application {
        validate_application_header(name, value)?;
        block.push_str(name);
        block.push_str(": ");
        block.push_str(value);
        block.push_str("\r\n");
    }
    for (name, value) in reserved {
        block.push_str(name);
        block.push_str(": ");
        block.push_str(value);
        block.push_str("\r\n");
    }
    block.push_str("\r\n");
    Ok(block.into_bytes())
}

fn validate_application_header(name: &str, value: &str) -> Result<()> {
    if name.is_empty()
        || name.to_ascii_lowercase().starts_with("morrow-")
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        || value.contains(['\r', '\n'])
    {
        return Err(ClientError::msg(
            "application headers must be single-line and cannot use Morrow-* names",
        ));
    }
    Ok(())
}

fn validate_key(key: Option<&str>) -> Result<()> {
    if key.is_some_and(|key| key.is_empty() || key.contains(['\r', '\n'])) {
        return Err(ClientError::msg(
            "publish key must be non-empty and single-line",
        ));
    }
    Ok(())
}

fn validate_producer_msg_id(msg_id: &str) -> Result<()> {
    if msg_id.is_empty()
        || msg_id.len() > 128
        || msg_id.chars().any(|ch| ch == '\r' || ch == '\n')
        || msg_id.chars().any(char::is_whitespace)
    {
        return Err(ClientError::msg(
            "msg_id must be non-empty, at most 128 bytes, and contain no whitespace",
        ));
    }
    Ok(())
}
