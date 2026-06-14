use super::*;

impl Consumer {
    pub(super) fn from_replay(replay: ReplayedConsumer) -> Self {
        Self {
            record: replay.record,
            members: HashMap::new(),
            pending: replay.pending,
            pending_attempts: HashMap::new(),
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
