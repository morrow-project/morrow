use super::*;

impl DurableBrokerState {
    pub(super) fn mark_subject_ready(&mut self, subject: &str) {
        self.ready_consumers
            .extend(self.consumer_interest_index.matching(subject));
    }

    pub(super) fn observe_published_record(&mut self, record: &PublishRecord) {
        if let Some(scheduled_at_ms) = scheduled_at_ms(record) {
            self.scheduled_deliveries.push(Reverse(ScheduledDelivery {
                scheduled_at_ms,
                seq: record.seq,
            }));
        }
        for consumer in self.consumers.values_mut() {
            consumer
                .cursors
                .observe_published_record(&consumer.record.filter_subject, record);
        }
    }

    pub(super) fn activate_due_scheduled(&mut self, now: u64, limit: usize) -> usize {
        let mut activated = 0;
        while activated < limit {
            let Some(entry) = self.scheduled_deliveries.peek().map(|entry| entry.0.clone())
            else {
                break;
            };
            if entry.scheduled_at_ms > now {
                break;
            }
            self.scheduled_deliveries.pop();
            if self
                .messages
                .get(&entry.seq)
                .and_then(scheduled_at_ms)
                .is_some_and(|scheduled_at_ms| scheduled_at_ms == entry.scheduled_at_ms)
            {
                if let Some(subject) = self.messages.get(&entry.seq).map(|message| message.subject.clone()) {
                    self.mark_subject_ready(&subject);
                    activated += 1;
                }
            }
        }
        activated
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
        let lease_deadline = self.lease_deadlines
            .peek()
            .map(|deadline| deadline.0.deadline_ms);
        let scheduled_deadline = self
            .scheduled_deliveries
            .peek()
            .map(|deadline| deadline.0.scheduled_at_ms);
        match (lease_deadline, scheduled_deadline) {
            (Some(lease), Some(scheduled)) => Some(lease.min(scheduled)),
            (Some(lease), None) => Some(lease),
            (None, Some(scheduled)) => Some(scheduled),
            (None, None) => None,
        }
    }

    pub(super) fn expire_due_leases(&mut self, now: u64, limit: usize) -> Result<usize> {
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
            let terminal = self
                .consumers
                .get(&deadline.consumer_id)
                .is_some_and(|consumer| lease.attempt >= consumer.record.retry_policy.max_attempts);
            if terminal {
                let message = self.messages.get(&deadline.seq).cloned();
                let action = self
                    .consumers
                    .get(&deadline.consumer_id)
                    .map(|consumer| consumer.record.retry_policy.terminal_action)
                    .unwrap_or(protocol::RetryTerminalAction::Retain);
                match action {
                    protocol::RetryTerminalAction::DeadLetter => {
                        if let Some(ref message) = message {
                            let record = DeadLetterRecord {
                                id: deadline.seq,
                                source_seq: deadline.seq,
                                consumer_id: deadline.consumer_id.clone(),
                                source_stream: message.stream.clone(),
                                source_partition: message.partition,
                                source_offset: message.offset,
                                reason: "delivery_attempts_exhausted".into(),
                                attempt_count: lease.attempt,
                                first_delivery_ms: deadline.deadline_ms
                                    .saturating_sub(self.consumers[&deadline.consumer_id].record.ack_timeout_ms),
                                last_delivery_ms: deadline.deadline_ms,
                                payload: message.payload.clone(),
                            };
                            self.wal.append_dead_letter(&record)?;
                            self.dead_letters.insert(record.id, record);
                        }
                    }
                    protocol::RetryTerminalAction::Discard
                    | protocol::RetryTerminalAction::Retain
                    | protocol::RetryTerminalAction::Pause => {}
                }
                self.wal.append_ack(
                    deadline.seq,
                    &deadline.consumer_id,
                    lease.delivery_id,
                )?;
                let acknowledged_cursors = message.as_ref().and_then(|message| {
                    if message.offset.is_some() {
                        let consumer = self.consumers.get(&deadline.consumer_id)?;
                        let mut cursors = consumer.cursors.clone();
                        cursors
                            .acknowledge(
                                message,
                                &consumer.record.filter_subject,
                                &self.messages,
                            )
                            .ok()?;
                        Some(cursors)
                    } else {
                        None
                    }
                });
                if let Some(consumer) = self.consumers.get_mut(&deadline.consumer_id) {
                    if let Some(cursors) = acknowledged_cursors.clone() {
                        consumer.cursors = cursors;
                    } else {
                        consumer.acked.insert(deadline.seq);
                    }
                    consumer.pending.remove(&deadline.seq);
                }
                if let Some(cursors) = acknowledged_cursors {
                    self.wal.append_consumer_cursor(&ConsumerCursorRecord {
                        consumer_id: deadline.consumer_id.clone(),
                        cursors,
                    })?;
                }
                expired += 1;
                continue;
            }
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
        Ok(expired)
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

pub(super) fn scheduled_at_ms(record: &PublishRecord) -> Option<u64> {
    record
        .headers
        .iter()
        .find(|header| header.name.eq_ignore_ascii_case("Morrow-Scheduled-At"))
        .and_then(|header| header.value.parse().ok())
}
