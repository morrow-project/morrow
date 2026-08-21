use super::*;

impl Inner {
    pub(super) fn prepare_durable_deliveries(&mut self, now: u64) -> Result<Vec<Delivery>> {
        let mut deliveries = Vec::new();
        let consumer_ids: Vec<_> = self.consumers.keys().cloned().collect();
        for consumer_id in consumer_ids {
            loop {
                let Some((seq, connection_id, sid, attempt, deadline_ms)) =
                    self.next_delivery_for(&consumer_id, now)
                else {
                    break;
                };
                let Some(message) = self.messages.get(&seq).cloned() else {
                    if let Some(consumer) = self.consumers.get_mut(&consumer_id) {
                        consumer.pending.remove(&seq);
                    }
                    continue;
                };
                let delivery =
                    self.wal
                        .append_delivery_attempt(seq, &consumer_id, deadline_ms, attempt)?;
                let ack_subject = protocol::ack_subject(&consumer_id, seq, delivery.delivery_id);
                let cursor_snapshot = if let Some(consumer) = self.consumers.get_mut(&consumer_id) {
                    if message.offset.is_some() {
                        consumer.cursors.mark_delivered(&message);
                    }
                    consumer.pending.remove(&seq);
                    consumer.pending_attempts.remove(&seq);
                    consumer.in_flight.insert(
                        seq,
                        InFlight {
                            delivery_id: delivery.delivery_id,
                            deadline_ms: delivery.deadline_ms,
                            attempt: delivery.attempt,
                        },
                    );
                    consumer.delivered += 1;
                    Some(consumer.cursors.clone())
                } else {
                    None
                };
                if let Some(cursors) = cursor_snapshot {
                    self.wal.append_consumer_cursor(&ConsumerCursorRecord {
                        consumer_id: consumer_id.clone(),
                        cursors,
                    })?;
                }
                if let Some(client) = self.clients.get(&connection_id) {
                    let frame = durable_message_frame(
                        &message,
                        &sid,
                        &ack_subject,
                        delivery.attempt,
                        delivery.deadline_ms,
                        client.protocol_version,
                    );
                    deliveries.push(Delivery {
                        sender: client.sender.clone(),
                        frame,
                    });
                    self.consume_durable_member(&consumer_id, connection_id, message.payload.len());
                }
            }
        }
        self.wal.flush_due()?;
        Ok(deliveries)
    }

    pub(super) fn next_delivery_for(
        &mut self,
        consumer_id: &str,
        now: u64,
    ) -> Option<(u64, u64, String, u32, u64)> {
        let consumer = self.consumers.get_mut(consumer_id)?;
        if consumer.in_flight.len() >= consumer.record.max_in_flight || consumer.members.is_empty()
        {
            return None;
        }
        let leased = consumer.in_flight.keys().copied().collect::<HashSet<_>>();
        let seq = consumer
            .cursors
            .next_candidate(&consumer.record.filter_subject, &self.messages, &leased)
            .or_else(|| {
                consumer
                    .pending
                    .iter()
                    .find(|seq| !leased.contains(seq))
                    .copied()
            })?;
        let payload_len = self.messages.get(&seq)?.payload.len();
        let (connection_id, member) = consumer
            .members
            .iter()
            .filter(|(connection_id, member)| {
                self.clients.contains_key(connection_id)
                    && member.credit_messages > 0
                    && member.credit_bytes >= payload_len
            })
            .min_by_key(|(connection_id, _)| **connection_id)?;
        let attempt = consumer.pending_attempts.get(&seq).copied().unwrap_or(1);
        let deadline_ms = now.saturating_add(consumer.record.ack_timeout_ms);
        Some((
            seq,
            *connection_id,
            member.sid.clone(),
            attempt,
            deadline_ms,
        ))
    }

    pub(super) fn sync_durable_state(&mut self, state: DurableState) -> Result<()> {
        let mut partition_records = state
            .messages
            .values()
            .filter(|record| {
                record.stream.is_some() && record.partition.is_some() && record.offset.is_some()
            })
            .collect::<Vec<_>>();
        partition_records.sort_by_key(|record| {
            (
                record.stream.as_deref().unwrap_or_default(),
                record.partition.unwrap_or_default(),
                record.offset.unwrap_or_default(),
            )
        });
        for record in partition_records {
            let (Some(stream), Some(partition), Some(offset)) =
                (record.stream.as_deref(), record.partition, record.offset)
            else {
                continue;
            };
            let is_new = !self.messages.contains_key(&record.seq);
            let envelope = crate::partition_log::MessageEnvelope {
                namespace: if record.namespace.is_empty() {
                    DEFAULT_NAMESPACE.to_string()
                } else {
                    record.namespace.clone()
                },
                stream: crate::stream::StreamId::new(stream)?,
                partition: crate::stream::PartitionId(partition),
                offset,
                subject: record.subject.clone(),
                key: record.key.clone(),
                headers: record.headers.clone(),
                timestamp_ms: record.timestamp_ms,
                reply_to: record.reply_to.clone(),
                payload: record.payload.clone(),
                partitioning_epoch: record.partitioning_epoch,
                leader_epoch: record.leader_epoch,
                legacy_seq: record.seq,
            };
            self.partition_logs.append_committed(envelope)?;
            if is_new {
                self.wal.append_partition_append(&PartitionAppendRecord {
                    seq: record.seq,
                    stream: stream.to_string(),
                    partition,
                    offset,
                    subject: record.subject.clone(),
                })?;
            }
        }
        self.messages = state.messages;
        let mut next = HashMap::new();
        for (consumer_id, durable) in state.consumers {
            let existing = self.consumers.remove(&consumer_id);
            let (members, delivered) = existing
                .as_ref()
                .map(|consumer| (consumer.members.clone(), consumer.delivered))
                .unwrap_or_default();
            let cursors = existing
                .as_ref()
                .map(|consumer| consumer.cursors.clone())
                .unwrap_or(durable.cursors);
            let pending = existing
                .as_ref()
                .map(|consumer| consumer.pending.clone())
                .unwrap_or_default();
            let pending_attempts = existing
                .as_ref()
                .map(|consumer| consumer.pending_attempts.clone())
                .unwrap_or_default();
            let in_flight = existing
                .as_ref()
                .map(|consumer| consumer.in_flight.clone())
                .unwrap_or_default();
            let acked = existing.map(|consumer| consumer.acked).unwrap_or_default();
            next.insert(
                consumer_id,
                Consumer {
                    record: durable.record,
                    cursors,
                    members,
                    pending,
                    pending_attempts,
                    in_flight,
                    acked,
                    delivered,
                },
            );
        }
        self.consumers = next;
        Ok(())
    }

    pub(super) fn cleanup_acked_messages(&mut self) {
        let removable: Vec<_> = self
            .messages
            .iter()
            .filter(|(seq, _)| {
                if self
                    .messages
                    .get(seq)
                    .is_some_and(|message| message.stream.is_some())
                {
                    return false;
                }
                let mut interested = false;
                for consumer in self.consumers.values() {
                    if consumer.pending.contains(seq)
                        || consumer.in_flight.contains_key(seq)
                        || consumer.acked.contains(seq)
                    {
                        interested = true;
                        if !consumer.acked.contains(seq) {
                            return false;
                        }
                    }
                }
                interested
            })
            .map(|(seq, _)| *seq)
            .collect();
        for seq in removable {
            self.messages.remove(&seq);
        }
    }

    pub(super) fn decrement_transient_subscription(&mut self, connection_id: u64, sid: &str) {
        let key = (connection_id, sid.to_string());
        let should_remove = self
            .transient_subscriptions
            .get_mut(&key)
            .and_then(|subscription| decrement_remaining(&mut subscription.remaining_deliveries))
            .unwrap_or(false);
        if should_remove {
            self.transient_subscriptions.remove(&key);
        }
    }

    pub(super) fn consume_durable_member(
        &mut self,
        consumer_id: &str,
        connection_id: u64,
        payload_bytes: usize,
    ) {
        let should_remove = self
            .consumers
            .get_mut(consumer_id)
            .and_then(|consumer| consumer.members.get_mut(&connection_id))
            .map(|member| {
                member.credit_messages = member.credit_messages.saturating_sub(1);
                member.credit_bytes = member.credit_bytes.saturating_sub(payload_bytes);
                decrement_remaining(&mut member.remaining_deliveries).unwrap_or(false)
            })
            .unwrap_or(false);
        if should_remove {
            if let Some(consumer) = self.consumers.get_mut(consumer_id) {
                consumer.members.remove(&connection_id);
            }
        }
    }
}

fn durable_message_frame(
    message: &PublishRecord,
    sid: &str,
    ack_subject: &str,
    attempt: u32,
    deadline_ms: u64,
    protocol_version: u32,
) -> Vec<u8> {
    if protocol_version >= 2 {
        let mut headers = message
            .headers
            .iter()
            .map(|header| (header.name.clone(), header.value.clone()))
            .collect::<Vec<_>>();
        headers.push(("Broker-Ack".into(), ack_subject.into()));
        if let (Some(stream), Some(partition), Some(offset)) =
            (&message.stream, message.partition, message.offset)
        {
            headers.push(("Broker-Stream".into(), stream.clone()));
            headers.push(("Broker-Partition".into(), partition.to_string()));
            headers.push(("Broker-Offset".into(), offset.to_string()));
        }
        headers.push(("Broker-Attempt".into(), attempt.to_string()));
        headers.push(("Broker-Lease-Deadline".into(), deadline_ms.to_string()));
        let borrowed = headers
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
            .collect::<Vec<_>>();
        return protocol::hmsg(
            &message.subject,
            sid,
            message.reply_to.as_deref(),
            &borrowed,
            &message.payload,
        );
    }
    if message.headers.is_empty() && message.reply_to.is_none() {
        return protocol::msg(&message.subject, sid, Some(ack_subject), &message.payload);
    }
    let mut headers = message
        .headers
        .iter()
        .map(|header| (header.name.clone(), header.value.clone()))
        .collect::<Vec<_>>();
    headers.push(("Broker-Ack".into(), ack_subject.into()));
    let header_refs = headers
        .iter()
        .map(|(name, value)| (name.as_str(), value.as_str()))
        .collect::<Vec<_>>();
    protocol::hmsg(
        &message.subject,
        sid,
        message.reply_to.as_deref(),
        &header_refs,
        &message.payload,
    )
}
