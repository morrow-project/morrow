use super::*;

impl Consumer {
    pub(super) fn from_replay(
        replay: ReplayedConsumer,
        catalog: &crate::stream::StreamCatalog,
        messages: &HashMap<u64, PublishRecord>,
    ) -> Self {
        let cursors = replay.cursors.unwrap_or_else(|| {
            crate::consumer_cursor::ConsumerCursorSet::new(
                &replay.record.filter_subject,
                replay.record.start_position,
                replay.record.max_in_flight,
                catalog,
                messages,
            )
        });
        Self {
            record: replay.record,
            cursors,
            members: HashMap::new(),
            pending: replay.pending,
            pending_attempts: replay.pending_attempts,
            in_flight: replay
                .in_flight
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
            acked: replay.acked,
            delivered: 0,
        }
    }
}
