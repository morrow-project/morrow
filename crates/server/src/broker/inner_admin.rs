use super::*;

impl Inner {
    pub(super) fn connections_response(&self) -> ConnectionsResponse {
        let mut connections = self
            .clients
            .iter()
            .map(|(id, client)| {
                let subscriptions = self
                    .consumers
                    .values()
                    .filter(|consumer| consumer.members.contains_key(id))
                    .count();
                let transient_subscriptions = self
                    .transient_subscriptions
                    .keys()
                    .filter(|(connection_id, _)| connection_id == id)
                    .count();
                ConnectionResponse {
                    id: *id,
                    remote_addr: client.remote_addr.map(|addr| addr.to_string()),
                    durable_id: client.durable_id.clone(),
                    authenticated: client.authenticated,
                    verbose: client.verbose,
                    connected_at_ms: client.connected_at_ms,
                    ack_timeout_ms: client.ack_timeout_ms,
                    max_in_flight: client.max_in_flight,
                    subscriptions,
                    transient_subscriptions,
                }
            })
            .collect::<Vec<_>>();
        connections.sort_by_key(|connection| connection.id);
        ConnectionsResponse {
            count: connections.len(),
            connections,
        }
    }

    pub(super) fn subscriptions_response(&self) -> SubscriptionsResponse {
        let mut durable_consumers = self
            .consumers
            .iter()
            .map(|(consumer_id, consumer)| {
                let mut members = consumer
                    .members
                    .iter()
                    .map(|(connection_id, member)| ConsumerMemberResponse {
                        connection_id: *connection_id,
                        sid: member.sid.clone(),
                        remaining_deliveries: member.remaining_deliveries,
                    })
                    .collect::<Vec<_>>();
                members.sort_by_key(|member| (member.connection_id, member.sid.clone()));
                DurableConsumerResponse {
                    consumer_id: consumer_id.clone(),
                    filter_subject: consumer.record.filter_subject.clone(),
                    queue_group: consumer.record.queue_group.clone(),
                    members,
                    pending: consumer.pending.len(),
                    in_flight: consumer.in_flight.len(),
                    acked: consumer.acked.len(),
                    delivered: consumer.delivered,
                    ack_timeout_ms: consumer.record.ack_timeout_ms,
                    max_in_flight: consumer.record.max_in_flight,
                }
            })
            .collect::<Vec<_>>();
        durable_consumers.sort_by(|left, right| left.consumer_id.cmp(&right.consumer_id));
        let mut transient_subscriptions = self
            .transient_subscriptions
            .iter()
            .map(
                |((connection_id, _), subscription)| TransientSubscriptionResponse {
                    connection_id: *connection_id,
                    sid: subscription.sid.clone(),
                    subject: subscription.subject.clone(),
                    remaining_deliveries: subscription.remaining_deliveries,
                },
            )
            .collect::<Vec<_>>();
        transient_subscriptions
            .sort_by_key(|subscription| (subscription.connection_id, subscription.sid.clone()));
        SubscriptionsResponse {
            durable_consumers,
            transient_subscriptions,
        }
    }

    pub(super) fn route_interests(&self) -> Vec<String> {
        let mut interests = self
            .transient_subscriptions
            .values()
            .map(|subscription| subscription.subject.clone())
            .collect::<Vec<_>>();
        interests.sort();
        interests.dedup();
        interests
    }

    pub(super) fn replayed_consumers(&self) -> Vec<ReplayedConsumer> {
        self.consumers
            .values()
            .map(|consumer| ReplayedConsumer {
                record: consumer.record.clone(),
                pending: consumer.pending.clone(),
                in_flight: consumer
                    .in_flight
                    .iter()
                    .map(|(seq, in_flight)| {
                        (
                            *seq,
                            DeliveryAttemptRecord {
                                seq: *seq,
                                consumer_id: consumer.record.consumer_id.clone(),
                                delivery_id: in_flight.delivery_id,
                                deadline_ms: in_flight.deadline_ms,
                                attempt: in_flight.attempt,
                            },
                        )
                    })
                    .collect(),
                acked: consumer.acked.clone(),
            })
            .collect()
    }

    pub(super) fn has_matching_durable_consumer(&self, subject_name: &str) -> bool {
        self.consumers
            .values()
            .any(|consumer| subject::matches(&consumer.record.filter_subject, subject_name))
    }

    pub(super) fn matching_durable_consumers(&self, subject_name: &str) -> Vec<String> {
        self.consumers
            .iter()
            .filter(|(_, consumer)| subject::matches(&consumer.record.filter_subject, subject_name))
            .map(|(consumer_id, _)| consumer_id.clone())
            .collect()
    }

    pub(super) fn upsert_consumer(&mut self, record: ConsumerRecord) -> &mut Consumer {
        let consumer_id = record.consumer_id.clone();
        let consumer = self
            .consumers
            .entry(consumer_id)
            .or_insert_with(|| Consumer {
                record: record.clone(),
                members: HashMap::new(),
                pending: BTreeSet::new(),
                pending_attempts: HashMap::new(),
                in_flight: HashMap::new(),
                acked: HashSet::new(),
                delivered: 0,
            });
        consumer.record = record;
        consumer
    }

    pub(super) fn prepare_transient_deliveries(
        &mut self,
        subject_name: &str,
        reply_to: Option<&str>,
        payload: &[u8],
    ) -> Vec<Delivery> {
        let matched = self
            .transient_subscriptions
            .iter()
            .filter(|(_, subscription)| subject::matches(&subscription.subject, subject_name))
            .filter_map(|((connection_id, _), subscription)| {
                let client = self.clients.get(connection_id)?;
                Some((
                    *connection_id,
                    subscription.sid.clone(),
                    Delivery {
                        sender: client.sender.clone(),
                        frame: protocol::msg(subject_name, &subscription.sid, reply_to, payload),
                    },
                ))
            })
            .collect::<Vec<_>>();
        for (connection_id, sid, _) in &matched {
            self.decrement_transient_subscription(*connection_id, sid);
        }
        matched
            .into_iter()
            .map(|(_, _, delivery)| delivery)
            .collect()
    }
}
