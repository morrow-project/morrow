use super::*;

impl DurableBrokerState {
    pub(super) fn mark_subject_ready(&mut self, subject: &str) {
        self.ready_consumers
            .extend(self.consumer_interest_index.matching(subject));
    }

    pub(super) fn mark_consumer_ready(&mut self, consumer_id: &str) {
        if self.consumers.contains_key(consumer_id) {
            self.ready_consumers.insert(consumer_id.to_string());
        }
    }

    pub(super) fn schedule_lease(
        &mut self,
        consumer_id: &str,
        seq: u64,
        lease: &DeliveryAttemptRecord,
    ) {
        self.lease_deadlines.push(Reverse(LeaseDeadline {
            deadline_ms: lease.deadline_ms,
            consumer_id: consumer_id.to_string(),
            seq,
            delivery_id: lease.delivery_id,
        }));
        self.compact_stale_deadlines();
    }

    pub(super) fn next_lease_deadline(&mut self) -> Option<u64> {
        self.discard_stale_deadlines();
        self.lease_deadlines
            .peek()
            .map(|deadline| deadline.0.deadline_ms)
    }

    pub(super) fn expire_due_leases(&mut self, now: u64, limit: usize) -> usize {
        let mut expired = 0;
        while expired < limit {
            self.discard_stale_deadlines();
            let Some(deadline) = self
                .lease_deadlines
                .peek()
                .map(|deadline| deadline.0.clone())
            else {
                break;
            };
            if deadline.deadline_ms > now {
                break;
            }
            self.lease_deadlines.pop();
            let Some(lease) = self
                .consumers
                .get_mut(&deadline.consumer_id)
                .and_then(|consumer| consumer.in_flight.remove(&deadline.seq))
            else {
                continue;
            };
            if self
                .messages
                .get(&deadline.seq)
                .is_some_and(|message| message.offset.is_none())
            {
                self.consumers
                    .get_mut(&deadline.consumer_id)
                    .unwrap()
                    .pending
                    .insert(deadline.seq);
            }
            let consumer = self.consumers.get_mut(&deadline.consumer_id).unwrap();
            consumer
                .pending_attempts
                .insert(deadline.seq, lease.attempt.saturating_add(1));
            self.ready_consumers.insert(deadline.consumer_id);
            expired += 1;
        }
        expired
    }

    fn discard_stale_deadlines(&mut self) {
        while self.lease_deadlines.peek().is_some_and(|deadline| {
            let deadline = &deadline.0;
            !self
                .consumers
                .get(&deadline.consumer_id)
                .and_then(|consumer| consumer.in_flight.get(&deadline.seq))
                .is_some_and(|lease| {
                    lease.delivery_id == deadline.delivery_id
                        && lease.deadline_ms == deadline.deadline_ms
                })
        }) {
            self.lease_deadlines.pop();
        }
    }

    fn compact_stale_deadlines(&mut self) {
        if self.lease_deadlines.len() < 1_024 || self.lease_deadlines.len() % 1_024 != 0 {
            return;
        }
        let live = self
            .consumers
            .values()
            .map(|consumer| consumer.in_flight.len())
            .sum::<usize>();
        if self.lease_deadlines.len() <= live.saturating_mul(2).saturating_add(1_024) {
            return;
        }
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
    }
}
