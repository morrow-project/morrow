use super::*;

impl Inner {
    pub(super) fn prepare_durable_deliveries(&mut self, now: u64) -> Result<Vec<Delivery>> {
        let mut deliveries = Vec::new();
        let consumer_ids: Vec<_> = self.consumers.keys().cloned().collect();
        for consumer_id in consumer_ids {
            loop {
                let Some((seq, connection_id, sid, attempt, deadline_ms)) =
                    self.next_delivery_for(&consumer_id, now)
                else {
                    break;
                };
                let Some(message) = self.messages.get(&seq).cloned() else {
                    if let Some(consumer) = self.consumers.get_mut(&consumer_id) {
                        consumer.pending.remove(&seq);
                    }
                    continue;
                };
                let delivery =
                    self.wal
                        .append_delivery_attempt(seq, &consumer_id, deadline_ms, attempt)?;
                let ack_subject = protocol::ack_subject(&consumer_id, seq, delivery.delivery_id);
                if let Some(consumer) = self.consumers.get_mut(&consumer_id) {
                    consumer.pending.remove(&seq);
                    consumer.pending_attempts.remove(&seq);
                    consumer.in_flight.insert(
                        seq,
                        InFlight {
                            delivery_id: delivery.delivery_id,
                            deadline_ms: delivery.deadline_ms,
                            attempt: delivery.attempt,
                        },
                    );
                    consumer.delivered += 1;
                }
                if let Some(client) = self.clients.get(&connection_id) {
                    let frame = match message.reply_to.as_deref() {
                        Some(reply_to) => protocol::hmsg(
                            &message.subject,
                            &sid,
                            Some(reply_to),
                            &[("Broker-Ack", &ack_subject)],
                            &message.payload,
                        ),
                        None => protocol::msg(
                            &message.subject,
                            &sid,
                            Some(&ack_subject),
                            &message.payload,
                        ),
                    };
                    deliveries.push(Delivery {
                        sender: client.sender.clone(),
                        frame,
                    });
                    self.decrement_durable_member(&consumer_id, connection_id);
                }
            }
        }
        self.wal.flush_due()?;
        Ok(deliveries)
    }

    pub(super) fn next_delivery_for(
        &self,
        consumer_id: &str,
        now: u64,
    ) -> Option<(u64, u64, String, u32, u64)> {
        let consumer = self.consumers.get(consumer_id)?;
        if consumer.in_flight.len() >= consumer.record.max_in_flight || consumer.members.is_empty()
        {
            return None;
        }
        let seq = *consumer.pending.iter().next()?;
        let (connection_id, member) = consumer
            .members
            .iter()
            .filter(|(connection_id, _)| self.clients.contains_key(connection_id))
            .min_by_key(|(connection_id, _)| **connection_id)?;
        let attempt = consumer.pending_attempts.get(&seq).copied().unwrap_or(1);
        let deadline_ms = now.saturating_add(consumer.record.ack_timeout_ms);
        Some((
            seq,
            *connection_id,
            member.sid.clone(),
            attempt,
            deadline_ms,
        ))
    }

    pub(super) fn next_cluster_delivery(&self, now: u64) -> Option<ClusterDeliveryCandidate> {
        for consumer_id in self.consumers.keys() {
            let consumer = self.consumers.get(consumer_id)?;
            if consumer.in_flight.len() >= consumer.record.max_in_flight
                || consumer.members.is_empty()
            {
                continue;
            }
            let seq = consumer.pending.iter().next().copied().or_else(|| {
                consumer
                    .in_flight
                    .iter()
                    .filter(|(_, in_flight)| in_flight.deadline_ms <= now)
                    .map(|(seq, _)| *seq)
                    .min()
            })?;
            let (connection_id, member) = consumer
                .members
                .iter()
                .filter(|(connection_id, _)| self.clients.contains_key(connection_id))
                .min_by_key(|(connection_id, _)| **connection_id)?;
            let attempt = consumer
                .in_flight
                .get(&seq)
                .map(|in_flight| in_flight.attempt.saturating_add(1))
                .or_else(|| consumer.pending_attempts.get(&seq).copied())
                .unwrap_or(1);
            let deadline_ms = now.saturating_add(consumer.record.ack_timeout_ms);
            return Some(ClusterDeliveryCandidate {
                consumer_id: consumer_id.clone(),
                seq,
                connection_id: *connection_id,
                sid: member.sid.clone(),
                attempt,
                deadline_ms,
            });
        }
        None
    }

    pub(super) fn delivery_for_record(
        &mut self,
        record: &crate::wal::DeliveryAttemptRecord,
        connection_id: u64,
        sid: &str,
    ) -> Option<Delivery> {
        let message = self.messages.get(&record.seq)?.clone();
        let client = self.clients.get(&connection_id)?;
        if let Some(consumer) = self.consumers.get_mut(&record.consumer_id) {
            consumer.delivered += 1;
        }
        let ack_subject =
            protocol::ack_subject(&record.consumer_id, record.seq, record.delivery_id);
        let frame = match message.reply_to.as_deref() {
            Some(reply_to) => protocol::hmsg(
                &message.subject,
                sid,
                Some(reply_to),
                &[("Broker-Ack", &ack_subject)],
                &message.payload,
            ),
            None => protocol::msg(&message.subject, sid, Some(&ack_subject), &message.payload),
        };
        let delivery = Delivery {
            sender: client.sender.clone(),
            frame,
        };
        self.decrement_durable_member(&record.consumer_id, connection_id);
        Some(delivery)
    }

    pub(super) fn sync_durable_state(&mut self, state: DurableState) {
        self.messages = state.messages;
        let mut next = HashMap::new();
        for (consumer_id, durable) in state.consumers {
            let (members, delivered) = self
                .consumers
                .remove(&consumer_id)
                .map(|consumer| (consumer.members, consumer.delivered))
                .unwrap_or_default();
            next.insert(
                consumer_id,
                Consumer {
                    record: durable.record,
                    members,
                    pending: durable.pending,
                    pending_attempts: durable.pending_attempts,
                    in_flight: durable
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
                    acked: durable.acked,
                    delivered,
                },
            );
        }
        self.consumers = next;
    }

    pub(super) fn cleanup_acked_messages(&mut self) {
        let removable: Vec<_> = self
            .messages
            .iter()
            .filter(|(seq, _)| {
                let mut interested = false;
                for consumer in self.consumers.values() {
                    if consumer.pending.contains(seq)
                        || consumer.in_flight.contains_key(seq)
                        || consumer.acked.contains(seq)
                    {
                        interested = true;
                        if !consumer.acked.contains(seq) {
                            return false;
                        }
                    }
                }
                interested
            })
            .map(|(seq, _)| *seq)
            .collect();
        for seq in removable {
            self.messages.remove(&seq);
        }
    }

    pub(super) fn decrement_transient_subscription(&mut self, connection_id: u64, sid: &str) {
        let key = (connection_id, sid.to_string());
        let should_remove = self
            .transient_subscriptions
            .get_mut(&key)
            .and_then(|subscription| decrement_remaining(&mut subscription.remaining_deliveries))
            .unwrap_or(false);
        if should_remove {
            self.transient_subscriptions.remove(&key);
        }
    }

    pub(super) fn decrement_durable_member(&mut self, consumer_id: &str, connection_id: u64) {
        let should_remove = self
            .consumers
            .get_mut(consumer_id)
            .and_then(|consumer| consumer.members.get_mut(&connection_id))
            .and_then(|member| decrement_remaining(&mut member.remaining_deliveries))
            .unwrap_or(false);
        if should_remove {
            if let Some(consumer) = self.consumers.get_mut(consumer_id) {
                consumer.members.remove(&connection_id);
            }
        }
    }
}
