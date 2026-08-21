use super::*;

const MAX_FETCH_WAIT_MS: u64 = 300_000;

#[derive(Debug, Clone, Copy)]
pub(super) enum PullControl {
    Ack,
    Nack(u64),
    Extend(u64),
}

struct PullBatch {
    deliveries: Vec<PullDelivery>,
    bytes: usize,
}

struct PullDelivery {
    message: PublishRecord,
    lease: DeliveryAttemptRecord,
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
            self.sync_from_cluster(&cluster).await?;
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
            inner.wal.flush_due()?;
            inner.consumers.insert(
                consumer_id,
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
            self.sync_from_cluster(&cluster).await?;
        } else {
            let mut inner = self.inner.lock().await;
            inner.wal.append_consumer_delete(&consumer_id)?;
            inner.wal.flush_due()?;
            inner.consumers.remove(&consumer_id);
        }
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
            max_wait_ms <= MAX_FETCH_WAIT_MS,
            "FETCH max wait exceeds {MAX_FETCH_WAIT_MS} ms"
        );
        let (consumer_id, _, max_in_flight) =
            self.pull_consumer_context(connection_id, &name).await?;
        crate::broker_ensure!(
            self.inner.lock().await.consumers.contains_key(&consumer_id),
            "unknown consumer"
        );
        crate::broker_ensure!(
            max_messages <= max_in_flight,
            "FETCH max messages exceeds consumer max_in_flight"
        );
        let max_batch_bytes = self.config.max_payload.saturating_mul(max_messages);
        crate::broker_ensure!(
            max_bytes <= max_batch_bytes,
            "FETCH max bytes exceeds bounded batch capacity"
        );

        let mut batch = self
            .fetch_pull_once(&consumer_id, max_messages, max_bytes)
            .await?;
        if batch.deliveries.is_empty() && max_wait_ms > 0 {
            let deadline = tokio::time::Instant::now() + Duration::from_millis(max_wait_ms);
            while batch.deliveries.is_empty() {
                let now = tokio::time::Instant::now();
                if now >= deadline {
                    break;
                }
                tokio::time::sleep((deadline - now).min(Duration::from_millis(10))).await;
                batch = self
                    .fetch_pull_once(&consumer_id, max_messages, max_bytes)
                    .await?;
            }
        }
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
                delivery.lease.attempt,
                delivery.lease.deadline_ms,
                delivery.lease.seq,
                delivery.lease.delivery_id,
                &message.payload,
            ));
        }
        self.send_to(connection_id, frame).await
    }

    async fn fetch_pull_once(
        &self,
        consumer_id: &str,
        max_messages: usize,
        max_bytes: usize,
    ) -> Result<PullBatch> {
        let mut inner = self.inner.lock().await;
        inner.prepare_pull_batch(
            consumer_id,
            max_messages,
            max_bytes,
            self.hooks.clock.now_ms(),
        )
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
                let deadline = self.hooks.clock.now_ms().saturating_add(delay_ms);
                self.update_pull_lease(&consumer_id, seq, delivery_id, deadline)
                    .await?;
                "NACK"
            }
            PullControl::Extend(extension_ms) => {
                crate::broker_ensure!(
                    extension_ms > 0,
                    "lease extension must be greater than zero"
                );
                let current = self
                    .pull_lease_deadline(&consumer_id, seq, delivery_id)
                    .await?;
                let deadline = current
                    .max(self.hooks.clock.now_ms())
                    .saturating_add(extension_ms);
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
        inner.wal.flush_due()?;
        inner
            .consumers
            .get_mut(consumer_id)
            .unwrap()
            .in_flight
            .get_mut(&seq)
            .unwrap()
            .deadline_ms = deadline_ms;
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
        let inner = self.inner.lock().await;
        let client = inner
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
        let mut inner = self.inner.lock().await;
        let client = inner
            .clients
            .get(&connection_id)
            .ok_or_else(|| BrokerError::msg("unknown connection"))?;
        crate::broker_ensure!(
            client.protocol_version >= 2,
            "CREDIT requires protocol version 2"
        );
        let (member, max_messages) = inner
            .consumers
            .values_mut()
            .find_map(|consumer| {
                let max_messages = consumer.record.max_in_flight;
                consumer
                    .members
                    .get_mut(&connection_id)
                    .filter(|member| member.sid == sid)
                    .map(|member| (member, max_messages))
            })
            .ok_or_else(|| BrokerError::msg("unknown durable subscription sid"))?;
        let max_bytes = self.config.max_payload.saturating_mul(max_messages);
        member.credit_messages = member
            .credit_messages
            .saturating_add(messages)
            .min(max_messages);
        member.credit_bytes = member.credit_bytes.saturating_add(bytes).min(max_bytes);
        drop(inner);
        self.deliver_pending().await?;
        self.send_verbose_ok(connection_id).await
    }
}

impl Inner {
    fn prepare_pull_batch(
        &mut self,
        consumer_id: &str,
        max_messages: usize,
        max_bytes: usize,
        now: u64,
    ) -> Result<PullBatch> {
        let mut deliveries = Vec::new();
        let mut bytes = 0usize;
        while deliveries.len() < max_messages {
            let Some((seq, attempt, deadline_ms)) = self.next_pull_candidate(consumer_id, now)
            else {
                break;
            };
            let Some(message) = self.messages.get(&seq).cloned() else {
                break;
            };
            if bytes.saturating_add(message.payload.len()) > max_bytes {
                break;
            }
            let lease = self
                .wal
                .append_delivery_attempt(seq, consumer_id, deadline_ms, attempt)?;
            let cursors = {
                let consumer = self.consumers.get_mut(consumer_id).unwrap();
                consumer.cursors.mark_delivered(&message);
                consumer.in_flight.insert(
                    seq,
                    InFlight {
                        delivery_id: lease.delivery_id,
                        deadline_ms,
                        attempt,
                    },
                );
                consumer.pending_attempts.remove(&seq);
                consumer.delivered += 1;
                consumer.cursors.clone()
            };
            self.wal.append_consumer_cursor(&ConsumerCursorRecord {
                consumer_id: consumer_id.to_string(),
                cursors,
            })?;
            bytes += message.payload.len();
            deliveries.push(PullDelivery { message, lease });
        }
        self.wal.flush_due()?;
        Ok(PullBatch { deliveries, bytes })
    }

    fn next_pull_candidate(&mut self, consumer_id: &str, now: u64) -> Option<(u64, u32, u64)> {
        let consumer = self.consumers.get_mut(consumer_id)?;
        let expired = consumer
            .in_flight
            .iter()
            .filter(|(_, lease)| lease.deadline_ms <= now)
            .min_by_key(|(seq, _)| **seq)
            .map(|(seq, lease)| (*seq, lease.attempt.saturating_add(1)));
        let (seq, attempt) = if let Some(expired) = expired {
            expired
        } else {
            if consumer.in_flight.len() >= consumer.record.max_in_flight {
                return None;
            }
            let leased = consumer.in_flight.keys().copied().collect::<HashSet<_>>();
            let seq = consumer.cursors.next_candidate(
                &consumer.record.filter_subject,
                &self.messages,
                &leased,
            )?;
            let attempt = consumer.pending_attempts.get(&seq).copied().unwrap_or(1);
            (seq, attempt)
        };
        Some((
            seq,
            attempt,
            now.saturating_add(consumer.record.ack_timeout_ms),
        ))
    }
}

fn pull_consumer_id(durable_id: &str, name: &str) -> String {
    format!(
        "pull-{}-{}",
        hex(durable_id.as_bytes()),
        hex(name.as_bytes())
    )
}
