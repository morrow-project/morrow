use super::*;
use crate::partition_log::RetentionChange;

impl DurableBrokerState {
    pub(super) fn enforce_stream_retention(
        &mut self,
        partition_logs: &PartitionLogSet,
        catalog: &crate::stream::StreamCatalog,
        now_ms: u64,
    ) -> Result<()> {
        let changes = partition_logs.retention_changes(catalog.definitions(), now_ms);
        for change in &changes {
            self.apply_retention_change(partition_logs, change)?;
        }
        Ok(())
    }

    fn apply_retention_change(
        &mut self,
        partition_logs: &PartitionLogSet,
        change: &RetentionChange,
    ) -> Result<()> {
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
        let retained = self
            .messages
            .values()
            .filter(|record| retained_partition_record(record, change))
            .filter(|record| {
                record
                    .offset
                    .is_some_and(|offset| offset >= change.earliest_offset)
            })
            .map(|record| partition_logs.load_record(record))
            .map(|record| record.and_then(|record| message_envelope(&record)))
            .collect::<Result<Vec<_>>>()?;
        partition_logs.retain_partition(change, &retained)?;
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
        Ok(())
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
