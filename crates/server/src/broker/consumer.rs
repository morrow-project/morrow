use super::*;

impl Consumer {
    pub(super) fn from_replay(
        replay: ReplayedConsumer,
        catalog: &crate::stream::StreamCatalog,
        messages: &HashMap<u64, PublishRecord>,
        logs: &PartitionLogSet,
    ) -> Self {
        let mut cursors = replay.cursors.unwrap_or_else(|| {
            crate::consumer_cursor::ConsumerCursorSet::new(
                &replay.record.filter_subject,
                replay.record.start_position,
                replay.record.max_in_flight,
                catalog,
                messages,
            )
        });
        for stream in catalog.definitions() {
            for partition in 0..stream.partitions {
                let status = logs
                    .retention_status(stream.name.as_str(), crate::stream::PartitionId(partition))
                    .expect("consumer catalog references configured partition logs");
                cursors.apply_retention_floor(
                    stream.name.as_str(),
                    partition,
                    status.earliest_offset,
                );
            }
        }
        let mut pending = replay.pending;
        pending.retain(|seq| messages.contains_key(seq));
        let mut pending_attempts = replay.pending_attempts;
        pending_attempts.retain(|seq, _| messages.contains_key(seq));
        let mut in_flight = replay.in_flight;
        in_flight.retain(|seq, _| messages.contains_key(seq));
        let mut acked = replay.acked;
        acked.retain(|seq| messages.contains_key(seq));
        Self {
            record: replay.record,
            cursors,
            members: HashMap::new(),
            pending,
            pending_attempts,
            preparing: HashSet::new(),
            in_flight: in_flight
                .into_iter()
                .map(|(seq, attempt)| {
                    (
                        seq,
                        InFlight {
                            delivery_id: attempt.delivery_id,
                            deadline_ms: attempt.deadline_ms,
                            attempt: attempt.attempt,
                        },
                    )
                })
                .collect(),
            acked,
            delivered: 0,
        }
    }
}
