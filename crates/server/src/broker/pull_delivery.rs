use super::*;

impl DurableBrokerState {
    pub(super) fn reserve_pull_candidate(
        &mut self,
        consumer_id: &str,
        partition_logs: &PartitionLogSet,
        now: u64,
    ) -> Option<(u64, u32, u64, PublishRecord)> {
        let (seq, attempt, deadline_ms) =
            self.next_pull_candidate(consumer_id, partition_logs, now)?;
        let metadata = self.messages.get(&seq)?.clone();
        self.consumers.get_mut(consumer_id)?.preparing.insert(seq);
        Some((seq, attempt, deadline_ms, metadata))
    }

    pub(super) fn release_pull_candidate(&mut self, consumer_id: &str, seq: u64) {
        if let Some(consumer) = self.consumers.get_mut(consumer_id) {
            consumer.preparing.remove(&seq);
        }
    }

    pub(super) fn commit_pull_delivery(
        &mut self,
        consumer_id: &str,
        seq: u64,
        attempt: u32,
        deadline_ms: u64,
        message: &PublishRecord,
    ) -> Result<DeliveryAttemptRecord> {
        let reserved = self
            .consumers
            .get_mut(consumer_id)
            .ok_or_else(|| BrokerError::msg("unknown consumer"))?
            .preparing
            .remove(&seq);
        crate::broker_ensure!(reserved, "pull candidate is not reserved");
        let lease = self
            .wal
            .append_delivery_attempt(seq, consumer_id, deadline_ms, attempt)?;
        let cursors = {
            let consumer = self.consumers.get_mut(consumer_id).unwrap();
            consumer.cursors.mark_delivered(message);
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
        Ok(lease)
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
            let in_flight = &consumer.in_flight;
            let preparing = &consumer.preparing;
            let seq = consumer.cursors.next_indexed_candidate(
                &consumer.record.filter_subject,
                &self.messages,
                &self.partition_sequences,
                partition_logs,
                |seq| in_flight.contains_key(&seq) || preparing.contains(&seq),
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
