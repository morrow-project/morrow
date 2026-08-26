use super::*;
use crate::partition_log::RetentionChange;

impl DurableBrokerState {
    pub(super) fn apply_retention_changes(
        &mut self,
        changes: &[RetentionChange],
        tenants: &std::collections::BTreeSet<String>,
    ) -> Result<HashMap<String, u64>> {
        let mut released = HashMap::new();
        for change in changes {
            for (tenant, bytes) in self.apply_retention_change_metadata(change, tenants)? {
                *released.entry(tenant).or_default() += bytes;
            }
        }
        Ok(released)
    }

    fn apply_retention_change_metadata(
        &mut self,
        change: &RetentionChange,
        tenants: &std::collections::BTreeSet<String>,
    ) -> Result<HashMap<String, u64>> {
        let mut released = HashMap::new();
        let removed = self
            .messages
            .iter()
            .filter(|(_, record)| retained_partition_record(record, change))
            .filter(|(_, record)| {
                record
                    .offset
                    .is_some_and(|offset| offset < change.earliest_offset)
            })
            .map(|(seq, _)| *seq)
            .collect::<HashSet<_>>();
        for record in self
            .messages
            .values()
            .filter(|record| removed.contains(&record.seq))
        {
            let tenant = tenants
                .iter()
                .find(|tenant| record.subject.starts_with(&format!("{tenant}.")))
                .cloned()
                .unwrap_or_else(|| crate::quota::DEFAULT_TENANT.to_string());
            *released.entry(tenant).or_default() +=
                crate::quota::persistent_publish_record_bytes(record);
        }
        self.messages.retain(|seq, _| !removed.contains(seq));
        self.remove_compaction_sequences(&removed);
        self.partition_sequences
            .retain(|_, seq| !removed.contains(seq));
        for consumer in self.consumers.values_mut() {
            consumer.pending.retain(|seq| !removed.contains(seq));
            consumer
                .pending_attempts
                .retain(|seq, _| !removed.contains(seq));
            consumer.in_flight.retain(|seq, _| !removed.contains(seq));
            consumer.acked.retain(|seq| !removed.contains(seq));
            consumer.cursors.apply_retention_floor(
                &change.stream,
                change.partition.0,
                change.earliest_offset,
            );
        }
        self.ready_consumers.extend(self.consumers.keys().cloned());
        Ok(released)
    }
}

fn retained_partition_record(record: &PublishRecord, change: &RetentionChange) -> bool {
    record.stream.as_deref() == Some(change.stream.as_str())
        && record.partition == Some(change.partition.0)
}

fn message_envelope(record: &PublishRecord) -> Result<MessageEnvelope> {
    Ok(MessageEnvelope {
        namespace: if record.namespace.is_empty() {
            DEFAULT_NAMESPACE.to_string()
        } else {
            record.namespace.clone()
        },
        stream: crate::stream::StreamId::new(
            record
                .stream
                .clone()
                .ok_or_else(|| BrokerError::msg("retained record has no stream"))?,
        )?,
        partition: crate::stream::PartitionId(
            record
                .partition
                .ok_or_else(|| BrokerError::msg("retained record has no partition"))?,
        ),
        offset: record
            .offset
            .ok_or_else(|| BrokerError::msg("retained record has no offset"))?,
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
    })
}
