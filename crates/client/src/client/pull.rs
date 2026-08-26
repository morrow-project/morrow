use super::*;

impl Client {
    pub async fn grant_push_credit(
        &mut self,
        sid: &str,
        messages: usize,
        bytes: usize,
    ) -> Result<()> {
        if messages == 0 || bytes == 0 {
            return Err(ClientError::msg(
                "push credit messages and bytes must be greater than zero",
            ));
        }
        self.write_line(&format!("CREDIT {sid} {messages} {bytes}"))
            .await
    }

    pub async fn create_consumer(
        &mut self,
        name: &str,
        filter_subject: &str,
        start: protocol::StartPosition,
    ) -> Result<()> {
        self.write_line(&format!(
            "CONSUMER CREATE {name} {filter_subject} {}",
            format_start(start)
        ))
        .await?;
        self.expect_consumer_ok("CREATE", name).await
    }

    pub async fn delete_consumer(&mut self, name: &str) -> Result<()> {
        self.write_line(&format!("CONSUMER DELETE {name}")).await?;
        self.expect_consumer_ok("DELETE", name).await
    }

    pub async fn fetch(
        &mut self,
        name: &str,
        max_messages: usize,
        max_bytes: usize,
        max_wait: Duration,
    ) -> Result<Vec<DurableMessage>> {
        if max_messages == 0 || max_bytes == 0 {
            return Err(ClientError::msg(
                "fetch message and byte limits must be greater than zero",
            ));
        }
        let max_wait_ms = duration_millis(max_wait, "fetch wait")?;
        self.write_line(&format!(
            "FETCH {name} {max_messages} {max_bytes} {max_wait_ms}"
        ))
        .await?;
        let (messages, declared_bytes) = self.read_batch_header(name).await?;
        if messages > max_messages || declared_bytes > max_bytes {
            return Err(ClientError::msg("server exceeded FETCH limits"));
        }
        let mut deliveries = Vec::with_capacity(messages);
        let mut bytes = 0usize;
        while deliveries.len() < messages {
            match self.next_frame().await? {
                Some(ServerFrame::DurableMessage(message)) if message.consumer == name => {
                    bytes = bytes.saturating_add(message.payload.len());
                    if bytes > max_bytes {
                        return Err(ClientError::msg("server exceeded FETCH byte limit"));
                    }
                    deliveries.push(message);
                }
                Some(ServerFrame::Err(err)) => return Err(ClientError::msg(err)),
                Some(frame) => {
                    return Err(ClientError::msg(format!(
                        "expected DDELIVER in FETCH batch, got {frame:?}"
                    )));
                }
                None => return Err(ClientError::msg("connection closed during FETCH batch")),
            }
        }
        if bytes != declared_bytes {
            return Err(ClientError::msg(
                "BATCH byte count does not match deliveries",
            ));
        }
        Ok(deliveries)
    }

    pub async fn ack_delivery(&mut self, message: &DurableMessage) -> Result<()> {
        self.delivery_control("ACK", message, None).await
    }

    pub async fn nack_delivery(&mut self, message: &DurableMessage, delay: Duration) -> Result<()> {
        let delay_ms = duration_millis(delay, "NACK delay")?;
        self.delivery_control("NACK", message, Some(delay_ms)).await
    }

    pub async fn extend_lease(
        &mut self,
        message: &DurableMessage,
        extension: Duration,
    ) -> Result<()> {
        let extension_ms = duration_millis(extension, "lease extension")?;
        if extension_ms == 0 {
            return Err(ClientError::msg(
                "lease extension must be greater than zero",
            ));
        }
        self.delivery_control("EXTEND", message, Some(extension_ms))
            .await
    }

    async fn read_batch_header(&mut self, name: &str) -> Result<(usize, usize)> {
        loop {
            match self.next_frame().await? {
                Some(ServerFrame::Batch {
                    name: batch_name,
                    messages,
                    bytes,
                }) if batch_name == name => return Ok((messages, bytes)),
                Some(ServerFrame::Ok) => {}
                Some(ServerFrame::Err(err)) => return Err(ClientError::msg(err)),
                Some(frame) => {
                    return Err(ClientError::msg(format!(
                        "expected BATCH after FETCH, got {frame:?}"
                    )));
                }
                None => return Err(ClientError::msg("connection closed before BATCH")),
            }
        }
    }

    async fn delivery_control(
        &mut self,
        operation: &str,
        message: &DurableMessage,
        duration_ms: Option<u64>,
    ) -> Result<()> {
        self.delivery_control_identity(
            operation,
            &message.consumer,
            message.seq,
            message.delivery_id,
            duration_ms,
        )
        .await
    }

    pub(crate) async fn delivery_control_identity(
        &mut self,
        operation: &str,
        consumer: &str,
        sequence: u64,
        delivery: u64,
        duration_ms: Option<u64>,
    ) -> Result<()> {
        let suffix = duration_ms
            .map(|duration| format!(" {duration}"))
            .unwrap_or_default();
        self.write_line(&format!(
            "{operation} {consumer} {sequence} {delivery}{suffix}"
        ))
        .await?;
        loop {
            match self.read_frame().await? {
                Some(ServerFrame::DeliveryControlOk {
                    operation: actual,
                    name,
                    seq,
                    delivery_id,
                }) if actual == operation
                    && name == consumer
                    && seq == sequence
                    && delivery_id == delivery =>
                {
                    return Ok(());
                }
                Some(ServerFrame::Ok) => {}
                Some(ServerFrame::Message(message)) => {
                    self.pending_messages.push_back(message);
                }
                Some(ServerFrame::Err(err)) => return Err(ClientError::msg(err)),
                Some(frame) => {
                    return Err(ClientError::msg(format!(
                        "expected D-OK after {operation}, got {frame:?}"
                    )));
                }
                None => return Err(ClientError::msg("connection closed before D-OK")),
            }
        }
    }

    async fn expect_consumer_ok(&mut self, operation: &str, name: &str) -> Result<()> {
        loop {
            match self.next_frame().await? {
                Some(ServerFrame::ConsumerOk {
                    operation: actual,
                    name: actual_name,
                }) if actual == operation && actual_name == name => return Ok(()),
                Some(ServerFrame::Ok) => {}
                Some(ServerFrame::Err(err)) => return Err(ClientError::msg(err)),
                Some(frame) => {
                    return Err(ClientError::msg(format!(
                        "expected C-OK after CONSUMER {operation}, got {frame:?}"
                    )));
                }
                None => return Err(ClientError::msg("connection closed before C-OK")),
            }
        }
    }
}

fn format_start(start: protocol::StartPosition) -> String {
    match start {
        protocol::StartPosition::Earliest => "@earliest".into(),
        protocol::StartPosition::Latest => "@latest".into(),
        protocol::StartPosition::Committed => "@committed".into(),
        protocol::StartPosition::Offset(offset) => format!("@offset:{offset}"),
        protocol::StartPosition::Timestamp(timestamp) => format!("@time:{timestamp}"),
    }
}

fn duration_millis(duration: Duration, field: &str) -> Result<u64> {
    duration
        .as_millis()
        .try_into()
        .map_err(|_| ClientError::msg(format!("{field} is too large")))
}
