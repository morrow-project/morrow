use super::*;

impl DurableBrokerState {
    pub(super) fn prepare_pull_batch(
        &mut self,
        consumer_id: &str,
        max_messages: usize,
        max_bytes: usize,
        max_encoded_bytes: usize,
        partition_logs: &PartitionLogSet,
        now: u64,
    ) -> Result<PullBatch> {
        let mut deliveries = Vec::new();
        let mut bytes = 0usize;
        let mut encoded_bytes = 0usize;
        while deliveries.len() < max_messages {
            let Some((seq, attempt, deadline_ms)) =
                self.next_pull_candidate(consumer_id, partition_logs, now)
            else {
                break;
            };
            let Some(metadata) = self.messages.get(&seq) else {
                break;
            };
            let message = partition_logs.load_record(metadata)?;
            let Some(next_payload_bytes) = bytes.checked_add(message.payload.len()) else {
                crate::broker_bail!("FETCH payload byte count overflow")
            };
            if next_payload_bytes > max_bytes {
                break;
            }
            let encoded_upper_bound = encoded_delivery_upper_bound(consumer_id, &message);
            let Some(next_encoded_bytes) = encoded_bytes.checked_add(encoded_upper_bound) else {
                crate::broker_bail!("FETCH encoded byte count overflow")
            };
            if next_encoded_bytes > max_encoded_bytes {
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
            self.schedule_lease(consumer_id, seq, &lease);
            bytes = next_payload_bytes;
            encoded_bytes = next_encoded_bytes;
            deliveries.push(PullDelivery { message, lease });
        }
        Ok(PullBatch { deliveries, bytes })
    }

    fn next_pull_candidate(
        &mut self,
        consumer_id: &str,
        partition_logs: &PartitionLogSet,
        now: u64,
    ) -> Option<(u64, u32, u64)> {
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
            let seq = consumer.cursors.next_indexed_candidate(
                &consumer.record.filter_subject,
                &self.messages,
                &self.partition_sequences,
                partition_logs,
                &leased,
            )?;
            let attempt = consumer.pending_attempts.get(&seq).copied().unwrap_or(1);
            (seq, attempt)
        };
        Some((
            seq,
            attempt,
            now.checked_add(consumer.record.ack_timeout_ms)?,
        ))
    }
}

fn encoded_delivery_upper_bound(consumer_id: &str, message: &PublishRecord) -> usize {
    let headers = message
        .headers
        .iter()
        .map(|header| (header.name.as_str(), header.value.as_str()))
        .collect::<Vec<_>>();
    protocol::durable_message(
        consumer_id,
        &message.subject,
        message.reply_to.as_deref(),
        &headers,
        message.stream.as_deref().unwrap_or_default(),
        message.partition.unwrap_or_default(),
        message.offset.unwrap_or_default(),
        message.key.as_deref(),
        message.timestamp_ms,
        u32::MAX,
        u64::MAX,
        u64::MAX,
        u64::MAX,
        &message.payload,
    )
    .len()
}
