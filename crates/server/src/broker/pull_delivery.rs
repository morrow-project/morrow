use super::*;
use super::delivery_index::scheduled_at_ms;

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
        if scheduled_at_ms(&metadata).is_some_and(|scheduled_at_ms| scheduled_at_ms > now) {
            return None;
        }
        self.consumers.get_mut(consumer_id)?.preparing.insert(seq);
        Some((seq, attempt, deadline_ms, metadata))
    }

    pub(super) fn release_pull_candidate(&mut self, consumer_id: &str, seq: u64) {
        if let Some(consumer) = self.consumers.get_mut(consumer_id) {
            consumer.preparing.remove(&seq);
        }
    }

    pub(super) fn commit_pull_deliveries(
        &mut self,
        consumer_id: &str,
        prepared: Vec<(u64, u32, u64, PublishRecord)>,
    ) -> Result<Vec<DeliveryAttemptRecord>> {
        let (old_cursors, old_delivered, wal_entries) = {
            let consumer = self
                .consumers
                .get_mut(consumer_id)
                .ok_or_else(|| BrokerError::msg("unknown consumer"))?;
            let old_cursors = consumer.cursors.clone();
            let old_delivered = consumer.delivered;
            let mut entries = Vec::with_capacity(prepared.len());
            for (seq, attempt, deadline_ms, message) in &prepared {
                crate::broker_ensure!(
                    consumer.preparing.remove(seq),
                    "pull candidate is not reserved"
                );
                consumer.cursors.mark_delivered(message);
                entries.push(DeliveryBatchEntry {
                    seq: *seq,
                    consumer_id: consumer_id.to_string(),
                    deadline_ms: *deadline_ms,
                    attempt: *attempt,
                    cursors: ConsumerCursorRecord {
                        consumer_id: consumer_id.to_string(),
                        cursors: consumer.cursors.clone(),
                    },
                });
            }
            (old_cursors, old_delivered, entries)
        };
        let leases = match self.wal.append_delivery_batch(wal_entries) {
            Ok(leases) => leases,
            Err(err) => {
                let consumer = self.consumers.get_mut(consumer_id).unwrap();
                consumer.cursors = old_cursors;
                consumer.delivered = old_delivered;
                for (seq, _, _, _) in prepared {
                    consumer.preparing.insert(seq);
                }
                return Err(err);
            }
        };
        crate::broker_ensure!(
            leases.len() == prepared.len(),
            "WAL delivery batch length mismatch"
        );
        let consumer = self.consumers.get_mut(consumer_id).unwrap();
        for ((seq, _, _, _), lease) in prepared.iter().zip(&leases) {
            consumer.pending_attempts.remove(seq);
            consumer.in_flight.insert(
                *seq,
                InFlight {
                    delivery_id: lease.delivery_id,
                    deadline_ms: lease.deadline_ms,
                    attempt: lease.attempt,
                },
            );
            consumer.delivered += 1;
        }
        for ((seq, _, _, _), lease) in prepared.iter().zip(&leases) {
            self.schedule_lease(consumer_id, *seq, lease);
        }
        Ok(leases)
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
            if self
                .messages
                .get(&seq)
                .and_then(scheduled_at_ms)
                .is_some_and(|scheduled_at_ms| scheduled_at_ms > now)
            {
                return None;
            }
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
