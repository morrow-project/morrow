use super::*;

const MAX_FETCH_WAIT_MS: u64 = 300_000;

#[derive(Debug, Clone, Copy)]
pub(super) enum PullControl {
    Ack,
    Nack(u64),
    Extend(u64),
}

pub(super) struct PullBatch {
    pub(super) deliveries: Vec<PullDelivery>,
    pub(super) bytes: usize,
}

pub(super) struct PullDelivery {
    pub(super) message: PublishRecord,
    pub(super) lease: DeliveryAttemptRecord,
}

impl Broker {
    pub(super) async fn create_pull_consumer(
        &self,
        connection_id: u64,
        name: String,
        filter_subject: String,
        start: protocol::StartPosition,
    ) -> Result<()> {
        protocol::validate_identifier("consumer name", &name)?;
        crate::broker_ensure!(
            subject::validate_subscription(&filter_subject),
            "invalid consumer filter subject"
        );
        self.authorize_subscribe(connection_id, &filter_subject)
            .await?;
        let (consumer_id, ack_timeout_ms, max_in_flight) =
            self.pull_consumer_context(connection_id, &name).await?;
        {
            let identity = self
                .connections
                .lock()
                .await
                .clients
                .get(&connection_id)
                .and_then(|client| client.durable_id.clone())
                .ok_or_else(|| BrokerError::msg("durable identity required"))?;
            let inner = self.inner.lock().await;
            let prefix = format!("pull-{}-", hex(identity.as_bytes()));
            let identity_consumers = inner
                .consumers
                .keys()
                .filter(|consumer_id| consumer_id.starts_with(&prefix))
                .count();
            if inner.consumers.len() >= self.config.quotas.max_durable_consumers
                || identity_consumers >= self.config.quotas.max_durable_consumers_per_identity
            {
                self.quotas.reject_state();
                crate::broker_bail!("durable consumer quota exceeded");
            }
        }
        let record = ConsumerRecord {
            consumer_id: consumer_id.clone(),
            filter_subject,
            queue_group: None,
            ack_timeout_ms,
            max_in_flight,
            start_position: start,
        };
        let cursors = {
            let inner = self.inner.lock().await;
            crate::consumer_cursor::ConsumerCursorSet::new(
                &record.filter_subject,
                start,
                max_in_flight,
                &self.config.streams,
                &inner.messages,
            )
        };
        crate::broker_ensure!(
            !cursors.partitions.is_empty(),
            "consumer filter has no durable stream binding"
        );
        crate::broker_ensure!(
            !self.inner.lock().await.consumers.contains_key(&consumer_id),
            "consumer already exists"
        );

        if let Some(cluster) = self.cluster_runtime().await {
            self.cluster_write(
                &cluster,
                BrokerCommand::CursorConsumerUpsert { record, cursors },
            )
            .await?;
        } else {
            let mut inner = self.inner.lock().await;
            crate::broker_ensure!(
                !inner.consumers.contains_key(&consumer_id),
                "consumer already exists"
            );
            inner.wal.append_consumer_upsert(&record)?;
            inner.wal.append_consumer_cursor(&ConsumerCursorRecord {
                consumer_id: consumer_id.clone(),
                cursors: cursors.clone(),
            })?;
            inner
                .consumer_interest_index
                .insert(&record.filter_subject, consumer_id.clone());
            inner.consumers.insert(
                consumer_id.clone(),
                Consumer {
                    record,
                    cursors,
                    members: HashMap::new(),
                    pending: BTreeSet::new(),
                    pending_attempts: HashMap::new(),
                    in_flight: HashMap::new(),
                    acked: HashSet::new(),
                    delivered: 0,
                },
            );
            inner.mark_consumer_ready(&consumer_id);
            drop(inner);
            self.wal.flush_due().await?;
        }
        self.send_to(connection_id, protocol::consumer_ok("CREATE", &name))
            .await
    }

    pub(super) async fn delete_pull_consumer(
        &self,
        connection_id: u64,
        name: String,
    ) -> Result<()> {
        let (consumer_id, _, _) = self.pull_consumer_context(connection_id, &name).await?;
        crate::broker_ensure!(
            self.inner.lock().await.consumers.contains_key(&consumer_id),
            "unknown consumer"
        );
        if let Some(cluster) = self.cluster_runtime().await {
            self.cluster_write(
                &cluster,
                BrokerCommand::ConsumerDelete {
                    consumer_id: consumer_id.clone(),
                },
            )
            .await?;
        } else {
            let mut inner = self.inner.lock().await;
            inner.wal.append_consumer_delete(&consumer_id)?;
            if let Some(consumer) = inner.consumers.remove(&consumer_id) {
                inner
                    .consumer_interest_index
                    .remove(&consumer.record.filter_subject, &consumer_id);
            }
            inner.ready_consumers.remove(&consumer_id);
            drop(inner);
            self.wal.flush_due().await?;
        }
        self.pull_waiters.cancel_consumer(&consumer_id);
        self.send_to(connection_id, protocol::consumer_ok("DELETE", &name))
            .await
    }

    pub(super) async fn fetch_pull(
        &self,
        connection_id: u64,
        name: String,
        max_messages: usize,
        max_bytes: usize,
        max_wait_ms: u64,
    ) -> Result<()> {
        crate::broker_ensure!(max_messages > 0 && max_bytes > 0, "invalid FETCH limits");
        crate::broker_ensure!(
            max_messages <= self.config.max_fetch_messages,
            "FETCH max messages exceeds server limit {}",
            self.config.max_fetch_messages
        );
        crate::broker_ensure!(
            max_bytes <= self.config.max_fetch_bytes,
            "FETCH max bytes exceeds server limit {}",
            self.config.max_fetch_bytes
        );
        crate::broker_ensure!(
            max_wait_ms <= MAX_FETCH_WAIT_MS,
            "FETCH max wait exceeds {MAX_FETCH_WAIT_MS} ms"
        );
        let (consumer_id, _, max_in_flight) =
            self.pull_consumer_context(connection_id, &name).await?;
        let filter_subject = self
            .inner
            .lock()
            .await
            .consumers
            .get(&consumer_id)
            .map(|consumer| consumer.record.filter_subject.clone())
            .ok_or_else(|| BrokerError::msg("unknown consumer"))?;
        crate::broker_ensure!(
            max_messages <= max_in_flight,
            "FETCH max messages exceeds consumer max_in_flight"
        );
        let header_bytes = protocol::batch(&name, max_messages, max_bytes).len();
        let encoded_delivery_bytes = self
            .config
            .max_encoded_batch_bytes
            .checked_sub(header_bytes)
            .ok_or_else(|| BrokerError::msg("FETCH encoded batch exceeds server limit"))?;

        let deadline = tokio::time::Instant::now() + Duration::from_millis(max_wait_ms);
        let waiter = (max_wait_ms > 0)
            .then(|| {
                self.pull_waiters
                    .register(connection_id, &consumer_id, &filter_subject)
            })
            .transpose()?;
        let batch = loop {
            let availability = waiter
                .as_ref()
                .map(|waiter| waiter.availability().notified());
            tokio::pin!(availability);
            if let Some(notification) = availability.as_mut().as_pin_mut() {
                notification.enable();
            }
            let cancellation = waiter
                .as_ref()
                .map(|waiter| waiter.cancellation().notified());
            tokio::pin!(cancellation);
            if let Some(notification) = cancellation.as_mut().as_pin_mut() {
                notification.enable();
            }

            #[cfg(test)]
            self.pull_waiters.record_fetch_check();
            let batch = self
                .fetch_pull_once(
                    &consumer_id,
                    max_messages,
                    max_bytes,
                    encoded_delivery_bytes,
                )
                .await?;
            if !batch.deliveries.is_empty() || waiter.is_none() {
                break batch;
            }
            if waiter.as_ref().unwrap().is_cancelled() {
                crate::broker_bail!("FETCH cancelled");
            }
            tokio::select! {
                _ = tokio::time::sleep_until(deadline) => break batch,
                _ = availability.as_mut().as_pin_mut().unwrap() => {}
                _ = cancellation.as_mut().as_pin_mut().unwrap() => {}
            }
        };
        let mut frame = protocol::batch(&name, batch.deliveries.len(), batch.bytes);
        for delivery in batch.deliveries {
            let message = delivery.message;
            let headers = message
                .headers
                .iter()
                .map(|header| (header.name.as_str(), header.value.as_str()))
                .collect::<Vec<_>>();
            frame.extend_from_slice(&protocol::durable_message(
                &name,
                &message.subject,
                message.reply_to.as_deref(),
                &headers,
                message.stream.as_deref().unwrap_or_default(),
                message.partition.unwrap_or_default(),
                message.offset.unwrap_or_default(),
                message.key.as_deref(),
                message.timestamp_ms,
                delivery.lease.attempt,
                delivery.lease.deadline_ms,
                delivery.lease.seq,
                delivery.lease.delivery_id,
                &message.payload,
            ));
        }
        crate::broker_ensure!(
            frame.len() <= self.config.max_encoded_batch_bytes,
            "FETCH encoded batch exceeds server limit {}",
            self.config.max_encoded_batch_bytes
        );
        self.send_to(connection_id, frame).await
    }

    async fn fetch_pull_once(
        &self,
        consumer_id: &str,
        max_messages: usize,
        max_bytes: usize,
        max_encoded_bytes: usize,
    ) -> Result<PullBatch> {
        let batch = {
            let mut inner = self.inner.lock().await;
            crate::broker_ensure!(
                inner.consumers.contains_key(consumer_id),
                "unknown consumer"
            );
            inner.prepare_pull_batch(
                consumer_id,
                max_messages,
                max_bytes,
                max_encoded_bytes,
                &self.partition_logs,
                self.hooks.clock.now_ms(),
            )?
        };
        self.wal.flush_due().await?;
        Ok(batch)
    }

    pub(super) async fn control_pull_delivery(
        &self,
        connection_id: u64,
        name: String,
        seq: u64,
        delivery_id: u64,
        control: PullControl,
    ) -> Result<()> {
        let (consumer_id, _, _) = self.pull_consumer_context(connection_id, &name).await?;
        let operation = match control {
            PullControl::Ack => {
                crate::broker_ensure!(
                    self.ack(AckSubject {
                        consumer_id,
                        seq,
                        delivery_id,
                    })
                    .await?,
                    "stale or unknown delivery identity"
                );
                "ACK"
            }
            PullControl::Nack(delay_ms) => {
                crate::broker_ensure!(
                    delay_ms <= self.config.max_ack_timeout_ms,
                    "NACK delay exceeds server limit {}",
                    self.config.max_ack_timeout_ms
                );
                let deadline = self
                    .hooks
                    .clock
                    .now_ms()
                    .checked_add(delay_ms)
                    .ok_or_else(|| BrokerError::msg("NACK deadline overflow"))?;
                self.update_pull_lease(&consumer_id, seq, delivery_id, deadline)
                    .await?;
                "NACK"
            }
            PullControl::Extend(extension_ms) => {
                crate::broker_ensure!(
                    extension_ms > 0,
                    "lease extension must be greater than zero"
                );
                crate::broker_ensure!(
                    extension_ms <= self.config.max_ack_timeout_ms,
                    "EXTEND duration exceeds server limit {}",
                    self.config.max_ack_timeout_ms
                );
                let current = self
                    .pull_lease_deadline(&consumer_id, seq, delivery_id)
                    .await?;
                let deadline = current
                    .max(self.hooks.clock.now_ms())
                    .checked_add(extension_ms)
                    .ok_or_else(|| BrokerError::msg("EXTEND deadline overflow"))?;
                self.update_pull_lease(&consumer_id, seq, delivery_id, deadline)
                    .await?;
                "EXTEND"
            }
        };
        self.send_to(
            connection_id,
            protocol::control_ok(operation, &name, seq, delivery_id),
        )
        .await
    }

    async fn update_pull_lease(
        &self,
        consumer_id: &str,
        seq: u64,
        delivery_id: u64,
        deadline_ms: u64,
    ) -> Result<()> {
        let mut inner = self.inner.lock().await;
        let lease = inner
            .consumers
            .get(consumer_id)
            .and_then(|consumer| consumer.in_flight.get(&seq))
            .filter(|lease| lease.delivery_id == delivery_id)
            .cloned()
            .ok_or_else(|| BrokerError::msg("stale or unknown delivery identity"))?;
        let record = DeliveryAttemptRecord {
            seq,
            consumer_id: consumer_id.to_string(),
            delivery_id,
            deadline_ms,
            attempt: lease.attempt,
        };
        inner.wal.append_delivery_lease(&record)?;
        inner
            .consumers
            .get_mut(consumer_id)
            .unwrap()
            .in_flight
            .get_mut(&seq)
            .unwrap()
            .deadline_ms = deadline_ms;
        inner.schedule_lease(consumer_id, seq, &record);
        drop(inner);
        self.redelivery_notify.notify_one();
        self.wal.flush_due().await?;
        Ok(())
    }

    async fn pull_lease_deadline(
        &self,
        consumer_id: &str,
        seq: u64,
        delivery_id: u64,
    ) -> Result<u64> {
        self.inner
            .lock()
            .await
            .consumers
            .get(consumer_id)
            .and_then(|consumer| consumer.in_flight.get(&seq))
            .filter(|lease| lease.delivery_id == delivery_id)
            .map(|lease| lease.deadline_ms)
            .ok_or_else(|| BrokerError::msg("stale or unknown delivery identity"))
    }

    async fn pull_consumer_context(
        &self,
        connection_id: u64,
        name: &str,
    ) -> Result<(String, u64, usize)> {
        protocol::validate_identifier("consumer name", name)?;
        let connections = self.connections.lock().await;
        let client = connections
            .clients
            .get(&connection_id)
            .ok_or_else(|| BrokerError::msg("unknown connection"))?;
        crate::broker_ensure!(client.configured, "CONNECT required");
        crate::broker_ensure!(
            client.protocol_version >= 2,
            "pull consumers require protocol version 2"
        );
        let durable_id = client
            .durable_id
            .as_deref()
            .ok_or_else(|| BrokerError::msg("durable identity required"))?;
        Ok((
            pull_consumer_id(durable_id, name),
            client.ack_timeout_ms,
            client.max_in_flight,
        ))
    }

    pub(super) async fn add_push_credit(
        &self,
        connection_id: u64,
        sid: &str,
        messages: usize,
        bytes: usize,
    ) -> Result<()> {
        let protocol_version = self
            .connections
            .lock()
            .await
            .clients
            .get(&connection_id)
            .map(|client| client.protocol_version)
            .ok_or_else(|| BrokerError::msg("unknown connection"))?;
        crate::broker_ensure!(protocol_version >= 2, "CREDIT requires protocol version 2");
        let mut inner = self.inner.lock().await;
        let (consumer_id, member, max_messages) = inner
            .consumers
            .iter_mut()
            .find_map(|(consumer_id, consumer)| {
                let max_messages = consumer.record.max_in_flight;
                consumer
                    .members
                    .get_mut(&connection_id)
                    .filter(|member| member.sid == sid)
                    .map(|member| (consumer_id.clone(), member, max_messages))
            })
            .ok_or_else(|| BrokerError::msg("unknown durable subscription sid"))?;
        let max_bytes = self.config.max_payload.saturating_mul(max_messages);
        member.credit_messages = member
            .credit_messages
            .saturating_add(messages)
            .min(max_messages);
        member.credit_bytes = member.credit_bytes.saturating_add(bytes).min(max_bytes);
        inner.mark_consumer_ready(&consumer_id);
        drop(inner);
        self.pull_waiters.notify_consumer(&consumer_id);
        self.deliver_pending().await?;
        self.send_verbose_ok(connection_id).await
    }
}

fn pull_consumer_id(durable_id: &str, name: &str) -> String {
    format!(
        "pull-{}-{}",
        hex(durable_id.as_bytes()),
        hex(name.as_bytes())
    )
}
