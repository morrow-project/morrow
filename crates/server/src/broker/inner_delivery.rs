use super::delivery_index::scheduled_at_ms;
use super::*;

const MAX_DURABLE_DELIVERIES_PER_TURN: usize = 64;
const MAX_DURABLE_DELIVERY_BYTES_PER_TURN: usize = 1024 * 1024;

impl DurableBrokerState {
    pub(super) fn prepare_durable_deliveries(
        &mut self,
        connections: &ConnectionState,
        partition_logs: &PartitionLogSet,
        middleware: &MiddlewareRuntime,
        now: u64,
    ) -> Result<Vec<Delivery>> {
        let mut deliveries = Vec::new();
        let consumer_ids = std::mem::take(&mut self.ready_consumers);
        let mut processed = 0;
        let mut processed_bytes = 0usize;
        for consumer_id in consumer_ids {
            if processed >= MAX_DURABLE_DELIVERIES_PER_TURN
                || processed_bytes >= MAX_DURABLE_DELIVERY_BYTES_PER_TURN
            {
                self.ready_consumers.insert(consumer_id);
                continue;
            }
            loop {
                if processed >= MAX_DURABLE_DELIVERIES_PER_TURN
                    || processed_bytes >= MAX_DURABLE_DELIVERY_BYTES_PER_TURN
                {
                    self.ready_consumers.insert(consumer_id.clone());
                    break;
                }
                let Some((seq, connection_id, sid, attempt, deadline_ms, message)) =
                    self.next_delivery_for(&consumer_id, connections, partition_logs, now)?
                else {
                    break;
                };
                let PublishRecord {
                    seq: message_seq,
                    namespace,
                    stream,
                    partition,
                    offset,
                    subject,
                    key,
                    headers,
                    timestamp_ms,
                    reply_to,
                    payload,
                    partitioning_epoch,
                    leader_epoch,
                } = message;
                let cursor_record = PublishRecord {
                    seq: message_seq,
                    namespace: namespace.clone(),
                    stream: stream.clone(),
                    partition,
                    offset,
                    subject: String::new(),
                    key: None,
                    headers: Vec::new(),
                    timestamp_ms,
                    reply_to: None,
                    payload: Vec::new(),
                    partitioning_epoch,
                    leader_epoch,
                };
                let outcome = middleware
                    .process(
                        MiddlewareStage::BeforeDeliver,
                        MiddlewareMessage {
                            subject,
                            key,
                            headers: headers
                                .into_iter()
                                .map(|header| (header.name, header.value))
                                .collect(),
                            payload,
                            reply_to,
                        },
                        0,
                    )
                    .map_err(|err| {
                        BrokerError::with_source("before-deliver middleware failed", err)
                    })?;
                crate::broker_ensure!(
                    outcome.emitted.is_empty(),
                    "before-deliver middleware cannot emit publications"
                );
                if outcome.decision == MiddlewareDecision::Reject {
                    crate::broker_bail!("before-deliver middleware rejected delivery");
                }
                if outcome.decision == MiddlewareDecision::Drop {
                    self.acknowledge_filtered_delivery(&consumer_id, &cursor_record)?;
                    continue;
                }
                let delivery_message = PublishRecord {
                    seq: message_seq,
                    namespace,
                    stream,
                    partition,
                    offset,
                    subject: outcome.message.subject,
                    key: outcome.message.key,
                    headers: outcome
                        .message
                        .headers
                        .into_iter()
                        .map(|(name, value)| MessageHeader { name, value })
                        .collect(),
                    timestamp_ms,
                    reply_to: outcome.message.reply_to,
                    payload: outcome.message.payload,
                    partitioning_epoch,
                    leader_epoch,
                };
                let delivery =
                    self.wal
                        .append_delivery_attempt(seq, &consumer_id, deadline_ms, attempt)?;
                let ack_subject = protocol::ack_subject(&consumer_id, seq, delivery.delivery_id);
                let cursor_snapshot = if let Some(consumer) = self.consumers.get_mut(&consumer_id) {
                    if cursor_record.offset.is_some() {
                        consumer.cursors.mark_delivered(&cursor_record);
                    }
                    consumer.pending.remove(&seq);
                    consumer.pending_attempts.remove(&seq);
                    consumer.in_flight.insert(
                        seq,
                        InFlight {
                            delivery_id: delivery.delivery_id,
                            deadline_ms: delivery.deadline_ms,
                            attempt: delivery.attempt,
                            retry_waiting: false,
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
                self.schedule_lease(&consumer_id, seq, &delivery);
                if let Some(client) = connections.clients.get(&connection_id) {
                    let frame = durable_message_frame(
                        &delivery_message,
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
                    self.consume_durable_member(
                        &consumer_id,
                        connection_id,
                        delivery_message.payload.len(),
                    );
                }
                processed += 1;
                processed_bytes = processed_bytes.saturating_add(delivery_message.payload.len());
            }
        }
        Ok(deliveries)
    }

    fn acknowledge_filtered_delivery(
        &mut self,
        consumer_id: &str,
        message: &PublishRecord,
    ) -> Result<()> {
        let consumer = self
            .consumers
            .get(consumer_id)
            .ok_or_else(|| BrokerError::msg("middleware consumer disappeared"))?;
        let mut cursors = consumer.cursors.clone();
        cursors.acknowledge(message, &consumer.record.filter_subject, &self.messages)?;
        self.consumers.get_mut(consumer_id).unwrap().cursors = cursors.clone();
        self.wal.append_consumer_cursor(&ConsumerCursorRecord {
            consumer_id: consumer_id.to_string(),
            cursors,
        })?;
        Ok(())
    }

    pub(super) fn next_delivery_for(
        &mut self,
        consumer_id: &str,
        connections: &ConnectionState,
        partition_logs: &PartitionLogSet,
        now: u64,
    ) -> Result<Option<(u64, u64, String, u32, u64, PublishRecord)>> {
        let Some(consumer) = self.consumers.get_mut(consumer_id) else {
            return Ok(None);
        };
        if consumer.in_flight.len() >= consumer.record.max_in_flight || consumer.members.is_empty()
        {
            return Ok(None);
        }
        let in_flight = &consumer.in_flight;
        let seq = consumer
            .cursors
            .next_indexed_candidate(
                &consumer.record.filter_subject,
                &self.messages,
                &self.partition_sequences,
                partition_logs,
                |seq| in_flight.contains_key(&seq),
            )
            .or_else(|| {
                consumer
                    .pending
                    .iter()
                    .find(|seq| !in_flight.contains_key(seq))
                    .copied()
            });
        let Some(seq) = seq else {
            return Ok(None);
        };
        let Some(metadata) = self.messages.get(&seq) else {
            return Ok(None);
        };
        if scheduled_at_ms(metadata).is_some_and(|scheduled_at_ms| scheduled_at_ms > now) {
            return Ok(None);
        }
        let message = partition_logs.load_record(metadata)?;
        let payload_len = message.payload.len();
        let member = consumer
            .members
            .iter()
            .filter(|(connection_id, member)| {
                connections.clients.contains_key(connection_id)
                    && member.credit_messages > 0
                    && member.credit_bytes >= payload_len
            })
            .min_by_key(|(connection_id, _)| **connection_id);
        let Some((connection_id, member)) = member else {
            return Ok(None);
        };
        let attempt = consumer.pending_attempts.get(&seq).copied().unwrap_or(1);
        let deadline_ms = now.saturating_add(consumer.record.ack_timeout_ms);
        Ok(Some((
            seq,
            *connection_id,
            member.sid.clone(),
            attempt,
            deadline_ms,
            message,
        )))
    }

    pub(super) fn sync_durable_state(
        &mut self,
        partition_logs: &PartitionLogSet,
        mut state: DurableState,
        catalog: &crate::stream::StreamCatalog,
    ) -> Result<()> {
        state.messages.retain(|_, record| {
            let (Some(stream), Some(partition), Some(offset)) =
                (record.stream.as_deref(), record.partition, record.offset)
            else {
                return true;
            };
            !partition_logs.is_before_retention_floor(
                stream,
                crate::stream::PartitionId(partition),
                offset,
            )
        });
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
                schema_id: None,
                payload: record.payload.clone(),
                partitioning_epoch: record.partitioning_epoch,
                leader_epoch: record.leader_epoch,
                legacy_seq: record.seq,
            };
            partition_logs.append_committed(envelope)?;
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
        for record in state.messages.values_mut() {
            if record.stream.is_some() {
                record.payload.clear();
                record.payload.shrink_to_fit();
            }
        }
        self.messages = state.messages;
        self.rebuild_compaction_index(catalog);
        self.partition_sequences = self
            .messages
            .values()
            .filter_map(|record| {
                Some((
                    (record.stream.clone()?, record.partition?, record.offset?),
                    record.seq,
                ))
            })
            .collect();
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
            let preparing = existing
                .as_ref()
                .map(|consumer| consumer.preparing.clone())
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
                    preparing,
                    in_flight,
                    acked,
                    delivered,
                },
            );
        }
        self.consumers = next;
        self.consumer_interest_index = subject::SubjectTrie::default();
        for (consumer_id, consumer) in &self.consumers {
            self.consumer_interest_index
                .insert(&consumer.record.filter_subject, consumer_id.clone());
        }
        self.ready_consumers = self.consumers.keys().cloned().collect();
        self.lease_deadlines = self
            .consumers
            .iter()
            .flat_map(|(consumer_id, consumer)| {
                consumer.in_flight.iter().map(|(seq, lease)| {
                    Reverse(LeaseDeadline {
                        deadline_ms: lease.deadline_ms,
                        consumer_id: consumer_id.clone(),
                        seq: *seq,
                        delivery_id: lease.delivery_id,
                    })
                })
            })
            .collect();
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
            if let Some(record) = self.messages.remove(&seq)
                && let (Some(stream), Some(partition), Some(offset)) =
                    (record.stream, record.partition, record.offset)
            {
                self.partition_sequences
                    .remove(&(stream, partition, offset));
            }
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
        headers.push(("Morrow-Ack".into(), ack_subject.into()));
        if let Some(key) = &message.key {
            headers.push(("Morrow-Key-Hex".into(), hex(key)));
        }
        headers.push(("Morrow-Timestamp".into(), message.timestamp_ms.to_string()));
        if let (Some(stream), Some(partition), Some(offset)) =
            (&message.stream, message.partition, message.offset)
        {
            headers.push(("Morrow-Stream".into(), stream.clone()));
            headers.push(("Morrow-Partition".into(), partition.to_string()));
            headers.push(("Morrow-Offset".into(), offset.to_string()));
        }
        headers.push(("Morrow-Attempt".into(), attempt.to_string()));
        headers.push(("Morrow-Lease-Deadline".into(), deadline_ms.to_string()));
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
    headers.push(("Morrow-Ack".into(), ack_subject.into()));
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

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut value = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        value.push(DIGITS[(byte >> 4) as usize] as char);
        value.push(DIGITS[(byte & 0xf) as usize] as char);
    }
    value
}
