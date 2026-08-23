use super::*;

impl Client {
    pub async fn connect(addr: SocketAddr, max_payload: usize) -> Result<Self> {
        let stream = TcpStream::connect(addr)
            .await
            .map_err(|err| ClientError::with_source(format!("connecting to {addr}"), err))?;
        Ok(Self {
            stream: BufReader::new(Box::new(stream)),
            max_payload,
            inbox_prefix: default_inbox_prefix(),
            inbox_counter: 0,
            durable: false,
            push_credit_messages: 0,
            pending_messages: VecDeque::new(),
        })
    }

    pub async fn connect_tls(
        addr: SocketAddr,
        server_name: &str,
        root_cert_file: impl AsRef<Path>,
        max_payload: usize,
    ) -> Result<Self> {
        let stream = TcpStream::connect(addr)
            .await
            .map_err(|err| ClientError::with_source(format!("connecting to {addr}"), err))?;
        let server_name = ServerName::try_from(server_name.to_string())
            .map_err(|err| ClientError::with_source("invalid TLS server name", err))?;
        let connector = TlsConnector::from(Arc::new(tls_config(root_cert_file)?));
        let stream = connector
            .connect(server_name, stream)
            .await
            .map_err(|err| ClientError::with_source("performing TLS handshake", err))?;
        Ok(Self {
            stream: BufReader::new(Box::new(stream)),
            max_payload,
            inbox_prefix: default_inbox_prefix(),
            inbox_counter: 0,
            durable: false,
            push_credit_messages: 0,
            pending_messages: VecDeque::new(),
        })
    }

    pub async fn read_info(&mut self) -> Result<Info> {
        match self.next_frame().await? {
            Some(ServerFrame::Info(info)) => Ok(info),
            Some(frame) => Err(ClientError::msg(format!("expected INFO, got {frame:?}"))),
            None => Err(ClientError::msg("connection closed before INFO")),
        }
    }

    pub async fn connect_with_options(options: &ClientOptions) -> Result<Self> {
        let mut client = match &options.tls {
            Some(tls) => {
                Self::connect_tls(
                    options.addr,
                    &tls.server_name,
                    &tls.ca_cert_file,
                    options.max_payload,
                )
                .await?
            }
            None => Self::connect(options.addr, options.max_payload).await?,
        };
        let info = client.read_info().await?;
        if let Some(auth) = &options.auth {
            client
                .connect_authenticated(
                    &info,
                    auth,
                    options.verbose,
                    options.ack_timeout_ms,
                    options.max_in_flight,
                )
                .await?;
        } else {
            let durable_id = options
                .durable_id
                .as_deref()
                .ok_or_else(|| ClientError::msg("durable_id is required when auth is disabled"))?;
            client
                .connect_durable(
                    durable_id,
                    options.verbose,
                    options.ack_timeout_ms,
                    options.max_in_flight,
                )
                .await?;
        }
        Ok(client)
    }

    pub async fn connect_durable(
        &mut self,
        durable_id: &str,
        verbose: bool,
        ack_timeout_ms: u64,
        max_in_flight: usize,
    ) -> Result<()> {
        let payload = serde_json::json!({
            "durable_id": durable_id,
            "verbose": verbose,
            "ack_timeout_ms": ack_timeout_ms,
            "max_in_flight": max_in_flight,
            "protocol_version": 2,
        });
        self.write_line(&format!("CONN {payload}")).await?;
        self.inbox_prefix = inbox_prefix(durable_id);
        self.durable = true;
        self.push_credit_messages = max_in_flight;
        Ok(())
    }

    pub async fn connect_transient(&mut self, verbose: bool) -> Result<()> {
        let payload = serde_json::json!({
            "verbose": verbose,
            "protocol_version": 2,
        });
        self.write_line(&format!("CONN {payload}")).await
    }

    pub async fn connect_authenticated(
        &mut self,
        info: &Info,
        auth: &ClientAuth,
        verbose: bool,
        ack_timeout_ms: u64,
        max_in_flight: usize,
    ) -> Result<()> {
        let nonce = info
            .nonce
            .as_deref()
            .ok_or_else(|| ClientError::msg("INFO frame does not contain an auth nonce"))?;
        let payload = serde_json::json!({
            "client_id": auth.client_id,
            "signature": auth.sign_nonce(nonce),
            "verbose": verbose,
            "ack_timeout_ms": ack_timeout_ms,
            "max_in_flight": max_in_flight,
            "protocol_version": 2,
        });
        self.write_line(&format!("CONN {payload}")).await?;
        self.inbox_prefix = inbox_prefix(&auth.client_id);
        self.durable = true;
        self.push_credit_messages = max_in_flight;
        Ok(())
    }

    pub async fn subscribe(&mut self, subject: &str, sid: &str) -> Result<()> {
        self.write_line(&format!("SUB {subject} {sid}")).await?;
        if self.durable && !subject.starts_with("_MORROW/INBOX/") {
            self.grant_push_credit(
                sid,
                self.push_credit_messages,
                self.max_payload.saturating_mul(self.push_credit_messages),
            )
            .await?;
        }
        Ok(())
    }

    pub async fn subscribe_queue(&mut self, subject: &str, queue: &str, sid: &str) -> Result<()> {
        self.write_line(&format!("SUB {subject} {queue} {sid}"))
            .await?;
        self.grant_push_credit(
            sid,
            self.push_credit_messages,
            self.max_payload.saturating_mul(self.push_credit_messages),
        )
        .await
    }

    pub async fn unsubscribe(&mut self, sid: &str) -> Result<()> {
        self.write_line(&format!("UNSUB {sid}")).await
    }

    pub async fn group_join(
        &mut self,
        group: &str,
        member: &str,
        partitions: u32,
        strategy: protocol::GroupAssignmentStrategy,
        instance_id: Option<&str>,
    ) -> Result<GroupAssignment> {
        let strategy = match strategy {
            protocol::GroupAssignmentStrategy::Range => "range",
            protocol::GroupAssignmentStrategy::RoundRobin => "round_robin",
            protocol::GroupAssignmentStrategy::Sticky => "sticky",
        };
        let suffix = instance_id.map(|id| format!(" {id}")).unwrap_or_default();
        self.write_line(&format!(
            "GROUP JOIN {group} {member} {partitions} {strategy}{suffix}"
        ))
        .await?;
        loop {
            match self.next_frame().await? {
                Some(ServerFrame::GroupOk {
                    operation,
                    group: Some(response_group),
                    generation: Some(generation),
                    partitions,
                }) if operation == "JOIN" && response_group == group => {
                    return Ok(GroupAssignment {
                        group: response_group,
                        generation,
                        partitions,
                    });
                }
                Some(ServerFrame::Err(err)) => return Err(ClientError::msg(err)),
                Some(frame) => {
                    return Err(ClientError::msg(format!(
                        "expected group join response, got {frame:?}"
                    )));
                }
                None => {
                    return Err(ClientError::msg(
                        "connection closed before group join response",
                    ));
                }
            }
        }
    }

    pub async fn group_heartbeat(
        &mut self,
        group: &str,
        member: &str,
        generation: u64,
    ) -> Result<GroupAssignment> {
        self.write_line(&format!("GROUP HEARTBEAT {group} {member} {generation}"))
            .await?;
        loop {
            match self.next_frame().await? {
                Some(ServerFrame::GroupOk {
                    operation,
                    group: Some(response_group),
                    generation: Some(generation),
                    partitions,
                }) if operation == "HEARTBEAT" && response_group == group => {
                    return Ok(GroupAssignment {
                        group: response_group,
                        generation,
                        partitions,
                    });
                }
                Some(ServerFrame::Err(err)) => return Err(ClientError::msg(err)),
                Some(frame) => {
                    return Err(ClientError::msg(format!(
                        "expected group heartbeat response, got {frame:?}"
                    )));
                }
                None => {
                    return Err(ClientError::msg(
                        "connection closed before group heartbeat response",
                    ));
                }
            }
        }
    }

    pub async fn group_leave(&mut self, group: &str, member: &str, generation: u64) -> Result<()> {
        self.write_line(&format!("GROUP LEAVE {group} {member} {generation}"))
            .await?;
        self.expect_group_ok("LEAVE").await
    }

    pub async fn group_commit(
        &mut self,
        group: &str,
        member: &str,
        generation: u64,
        partition: u32,
        offset: u64,
    ) -> Result<()> {
        self.write_line(&format!(
            "GROUP COMMIT {group} {member} {generation} {partition} {offset}"
        ))
        .await?;
        self.expect_group_ok("COMMIT").await
    }

    async fn expect_group_ok(&mut self, operation: &str) -> Result<()> {
        loop {
            match self.next_frame().await? {
                Some(ServerFrame::GroupOk {
                    operation: actual, ..
                }) if actual == operation => return Ok(()),
                Some(ServerFrame::Err(err)) => return Err(ClientError::msg(err)),
                Some(frame) => {
                    return Err(ClientError::msg(format!(
                        "expected group {operation} response, got {frame:?}"
                    )));
                }
                None => return Err(ClientError::msg("connection closed before group response")),
            }
        }
    }

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
        self.stream
            .get_mut()
            .write_all(payload)
            .await
            .map_err(|err| ClientError::with_source("writing PUB payload", err))?;
        self.stream
            .get_mut()
            .write_all(b"\r\n")
            .await
            .map_err(|err| ClientError::with_source("writing PUB payload terminator", err))
    }

    pub async fn publish_with_qos(
        &mut self,
        subject: &str,
        reply_to: Option<&str>,
        payload: &[u8],
        level: protocol::AckLevel,
        msg_id: &str,
    ) -> Result<ProducerAck> {
        self.publish_with_qos_and_key(subject, reply_to, payload, level, msg_id, None)
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
        self.publish_with_qos_and_key_and_producer(
            subject, reply_to, payload, level, msg_id, key, None,
        )
        .await
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
        self.publish_with_qos_and_key_and_producer(
            subject,
            reply_to,
            payload,
            level,
            msg_id,
            key,
            Some(producer),
        )
        .await
    }

    async fn publish_with_qos_and_key_and_producer(
        &mut self,
        subject: &str,
        reply_to: Option<&str>,
        payload: &[u8],
        level: protocol::AckLevel,
        msg_id: &str,
        key: Option<&str>,
        producer: Option<&protocol::ProducerSequence>,
    ) -> Result<ProducerAck> {
        validate_producer_msg_id(msg_id)?;
        if key.is_some_and(|key| key.is_empty() || key.contains(['\r', '\n'])) {
            return Err(ClientError::msg(
                "publish key must be non-empty and single-line",
            ));
        }
        if payload.len() > self.max_payload {
            return Err(ClientError::msg(format!(
                "payload size {} exceeds max payload {}",
                payload.len(),
                self.max_payload
            )));
        }
        let mut headers = format!(
            "MORROW/1.0\r\nMorrow-QoS: {}\r\nMorrow-Msg-Id: {msg_id}\r\n\r\n",
            level as u8
        );
        if let Some(producer) = producer {
            headers.truncate(headers.len() - 2);
            headers.push_str(&format!(
                "Morrow-Producer-Id: {}\r\nMorrow-Producer-Epoch: {}\r\nMorrow-Producer-Sequence: {}\r\n\r\n",
                producer.producer_id, producer.epoch, producer.sequence
            ));
        }
        if let Some(key) = key {
            headers.truncate(headers.len() - 2);
            headers.push_str(&format!("Morrow-Key: {key}\r\n\r\n"));
        }
        let total_len = headers.len() + payload.len();
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
            .write_all(headers.as_bytes())
            .await
            .map_err(|err| ClientError::with_source("writing HPUB headers", err))?;
        self.stream
            .get_mut()
            .write_all(payload)
            .await
            .map_err(|err| ClientError::with_source("writing HPUB payload", err))?;
        self.stream
            .get_mut()
            .write_all(b"\r\n")
            .await
            .map_err(|err| ClientError::with_source("writing HPUB payload terminator", err))?;

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

    pub async fn ack(&mut self, ack_subject: &str) -> Result<()> {
        let ack = protocol::parse_ack_subject(ack_subject)
            .ok_or_else(|| ClientError::msg("invalid Morrow ACK subject"))?;
        self.delivery_control_identity("ACK", &ack.consumer_id, ack.seq, ack.delivery_id, None)
            .await
    }

    pub async fn request(
        &mut self,
        subject: &str,
        payload: &[u8],
        timeout: Duration,
    ) -> Result<Message> {
        self.inbox_counter = self.inbox_counter.saturating_add(1);
        let inbox = format!("{}.{}", self.inbox_prefix, self.inbox_counter);
        let sid = format!("inbox{}", self.inbox_counter);
        self.subscribe(&inbox, &sid).await?;
        self.ping_roundtrip().await?;
        self.publish_with_reply(subject, Some(&inbox), payload)
            .await?;

        let response = match tokio::time::timeout(timeout, async {
            loop {
                let message = self.next_message().await?;
                if message.subject == inbox {
                    return Ok(message);
                }
            }
        })
        .await
        {
            Ok(response) => response?,
            Err(_) => {
                let _ = self.unsubscribe(&sid).await;
                return Err(ClientError::msg("request timed out"));
            }
        };

        if let Some(ack_subject) = &response.ack_subject {
            self.ack(ack_subject).await?;
        }
        self.unsubscribe(&sid).await?;
        Ok(response)
    }

    pub async fn respond(&mut self, message: &Message, payload: &[u8]) -> Result<()> {
        let reply_to = message
            .reply_to
            .as_deref()
            .ok_or_else(|| ClientError::msg("message does not contain a reply subject"))?;
        self.publish(reply_to, payload).await
    }

    pub async fn ping(&mut self) -> Result<()> {
        self.write_line("PING").await
    }

    pub async fn ping_roundtrip(&mut self) -> Result<()> {
        self.ping().await?;
        loop {
            match self.read_frame().await? {
                Some(ServerFrame::Pong) => return Ok(()),
                Some(ServerFrame::Ok) => {}
                Some(ServerFrame::Message(message)) => self.pending_messages.push_back(message),
                Some(ServerFrame::Err(err)) => return Err(ClientError::msg(err)),
                Some(frame) => {
                    return Err(ClientError::msg(format!(
                        "expected PONG during ping roundtrip, got {frame:?}"
                    )));
                }
                None => return Err(ClientError::msg("connection closed before PONG")),
            }
        }
    }

    pub async fn next_message(&mut self) -> Result<Message> {
        if let Some(message) = self.pending_messages.pop_front() {
            return Ok(message);
        }
        loop {
            match self.next_frame().await? {
                Some(ServerFrame::Message(message)) => return Ok(message),
                Some(ServerFrame::Ok) => {}
                Some(frame) => {
                    return Err(ClientError::msg(format!("expected DELIVER, got {frame:?}")));
                }
                None => return Err(ClientError::msg("connection closed before DELIVER")),
            }
        }
    }

    pub async fn next_frame(&mut self) -> Result<Option<ServerFrame>> {
        if let Some(message) = self.pending_messages.pop_front() {
            return Ok(Some(ServerFrame::Message(message)));
        }
        self.read_frame().await
    }

    async fn read_frame(&mut self) -> Result<Option<ServerFrame>> {
        let mut line = Vec::new();
        let read = self
            .stream
            .read_until(b'\n', &mut line)
            .await
            .map_err(|err| ClientError::with_source("reading server frame", err))?;
        if read == 0 {
            return Ok(None);
        }
        trim_crlf(&mut line)?;
        let line = String::from_utf8(line)
            .map_err(|err| ClientError::with_source("server frame is not UTF-8", err))?;
        parse_frame(&mut self.stream, &line, self.max_payload).await
    }

    pub(super) async fn write_line(&mut self, line: &str) -> Result<()> {
        self.stream
            .get_mut()
            .write_all(line.as_bytes())
            .await
            .map_err(|err| ClientError::with_source("writing protocol line", err))?;
        self.stream
            .get_mut()
            .write_all(b"\r\n")
            .await
            .map_err(|err| ClientError::with_source("writing protocol line terminator", err))
    }
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
