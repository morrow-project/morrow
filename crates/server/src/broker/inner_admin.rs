use super::*;

impl ConnectionState {
    pub(super) fn response(
        &self,
        durable_counts: &HashMap<u64, usize>,
        transient_counts: &HashMap<u64, usize>,
    ) -> ConnectionsResponse {
        self.response_page(durable_counts, transient_counts, 0, usize::MAX)
    }

    pub(super) fn response_page(
        &self,
        durable_counts: &HashMap<u64, usize>,
        transient_counts: &HashMap<u64, usize>,
        offset: usize,
        limit: usize,
    ) -> ConnectionsResponse {
        let mut connections = self
            .clients
            .iter()
            .map(|(id, client)| {
                let subscriptions = durable_counts.get(id).copied().unwrap_or_default();
                let transient_subscriptions = transient_counts.get(id).copied().unwrap_or_default();
                ConnectionResponse {
                    id: *id,
                    remote_addr: client.remote_addr.map(|addr| addr.to_string()),
                    durable_id: client.durable_id.clone(),
                    authenticated: client.authenticated,
                    verbose: client.verbose,
                    connected_at_ms: client.connected_at_ms,
                    ack_timeout_ms: client.ack_timeout_ms,
                    max_in_flight: client.max_in_flight,
                    protocol_version: client.protocol_version,
                    subscriptions,
                    transient_subscriptions,
                }
            })
            .collect::<Vec<_>>();
        connections.sort_by_key(|connection| connection.id);
        let total_count = connections.len();
        let page = connections
            .into_iter()
            .skip(offset)
            .take(limit)
            .collect::<Vec<_>>();
        let next_offset = (offset + page.len() < total_count).then_some(offset + page.len());
        ConnectionsResponse {
            count: page.len(),
            total_count,
            next_offset,
            connections: page,
        }
    }
}

impl DurableBrokerState {
    pub(super) fn producers_response_page(&self, offset: usize, limit: usize) -> ProducersResponse {
        let mut producers = self
            .producer_epochs
            .iter()
            .map(|(producer_id, epoch)| ProducerResponse {
                producer_id: producer_id.clone(),
                epoch: *epoch,
                dedup_entries: self
                    .producer_sequences
                    .keys()
                    .filter(|(id, producer_epoch, _)| id == producer_id && producer_epoch == epoch)
                    .count(),
            })
            .collect::<Vec<_>>();
        producers.sort_by(|left, right| left.producer_id.cmp(&right.producer_id));
        let total_count = producers.len();
        let page = producers
            .into_iter()
            .skip(offset)
            .take(limit)
            .collect::<Vec<_>>();
        ProducersResponse {
            count: page.len(),
            total_count,
            next_offset: (offset + page.len() < total_count).then_some(offset + page.len()),
            producers: page,
        }
    }

    pub(super) fn dead_letters_response_page(
        &self,
        offset: usize,
        limit: usize,
    ) -> DeadLettersResponse {
        let total_count = self.dead_letters.len();
        let records = self
            .dead_letters
            .values()
            .skip(offset)
            .take(limit)
            .map(|record| DeadLetterResponse {
                id: record.id,
                source_seq: record.source_seq,
                consumer_id: record.consumer_id.clone(),
                source_stream: record.source_stream.clone(),
                source_partition: record.source_partition,
                source_offset: record.source_offset,
                reason: record.reason.clone(),
                attempt_count: record.attempt_count,
                first_delivery_ms: record.first_delivery_ms,
                last_delivery_ms: record.last_delivery_ms,
                payload_bytes: record.payload.len(),
            })
            .collect::<Vec<_>>();
        DeadLettersResponse {
            count: records.len(),
            total_count,
            next_offset: (offset + records.len() < total_count).then_some(offset + records.len()),
            records,
        }
    }

    pub(super) fn purge_dead_letter(&mut self, id: u64) -> Result<bool> {
        if !self.dead_letters.contains_key(&id) {
            return Ok(false);
        }
        self.wal.purge_dead_letter(id)?;
        self.dead_letters.remove(&id);
        Ok(true)
    }

    pub(super) fn replay_dead_letter(&mut self, id: u64) -> Result<bool> {
        let Some(record) = self.dead_letters.get(&id).cloned() else {
            return Ok(false);
        };
        if !self.messages.contains_key(&record.source_seq) {
            return Ok(false);
        }
        let Some(consumer) = self.consumers.get_mut(&record.consumer_id) else {
            return Ok(false);
        };
        self.wal.purge_dead_letter(id)?;
        self.dead_letters.remove(&id);
        consumer.acked.remove(&record.source_seq);
        consumer.pending.insert(record.source_seq);
        consumer.pending_attempts.insert(record.source_seq, 1);
        self.mark_consumer_ready(&record.consumer_id);
        Ok(true)
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
                        credit_messages: member.credit_messages,
                        credit_bytes: member.credit_bytes,
                    })
                    .collect::<Vec<_>>();
                members.sort_by_key(|member| (member.connection_id, member.sid.clone()));
                let cursors = consumer
                    .cursors
                    .partitions
                    .values()
                    .map(|cursor| PartitionCursorResponse {
                        stream: cursor.stream.clone(),
                        partition: cursor.partition,
                        committed_offset: cursor.committed_offset,
                        delivered_offset: cursor.delivered_offset,
                        acknowledged_out_of_order: cursor.acknowledged_offsets.len(),
                        retention_gaps: cursor.retention_gaps,
                    })
                    .collect();
                DurableConsumerResponse {
                    consumer_id: consumer_id.clone(),
                    filter_subject: consumer.record.filter_subject.clone(),
                    queue_group: consumer.record.queue_group.clone(),
                    members,
                    pending: consumer.pending.len(),
                    in_flight: consumer.in_flight.len(),
                    acked: consumer.acked.len(),
                    cursors,
                    delivered: consumer.delivered,
                    ack_timeout_ms: consumer.record.ack_timeout_ms,
                    max_in_flight: consumer.record.max_in_flight,
                }
            })
            .collect::<Vec<_>>();
        durable_consumers.sort_by(|left, right| left.consumer_id.cmp(&right.consumer_id));
        let transient_subscriptions = Vec::new();
        SubscriptionsResponse {
            durable_consumers,
            transient_subscriptions,
        }
    }

    pub(super) fn replayed_consumers(&self) -> Vec<ReplayedConsumer> {
        self.consumers
            .values()
            .map(|consumer| ReplayedConsumer {
                record: consumer.record.clone(),
                cursors: Some(consumer.cursors.clone()),
                pending: consumer.pending.clone(),
                pending_attempts: consumer.pending_attempts.clone(),
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
                                retry_waiting: in_flight.retry_waiting,
                            },
                        )
                    })
                    .collect(),
                acked: consumer.acked.clone(),
            })
            .collect()
    }

    pub(super) fn upsert_consumer(
        &mut self,
        record: ConsumerRecord,
        catalog: &crate::stream::StreamCatalog,
    ) -> &mut Consumer {
        let consumer_id = record.consumer_id.clone();
        let filter_changed = self
            .consumers
            .get(&consumer_id)
            .is_some_and(|existing| existing.record.filter_subject != record.filter_subject);
        if let Some(existing) = self.consumers.get(&consumer_id) {
            self.consumer_interest_index
                .remove(&existing.record.filter_subject, &consumer_id);
        }
        self.consumer_interest_index
            .insert(&record.filter_subject, consumer_id.clone());
        self.ready_consumers.insert(consumer_id.clone());
        let initial_cursors = crate::consumer_cursor::ConsumerCursorSet::new(
            &record.filter_subject,
            record.start_position,
            record.max_in_flight,
            catalog,
            &self.messages,
        );
        let consumer = self
            .consumers
            .entry(consumer_id)
            .or_insert_with(|| Consumer {
                record: record.clone(),
                cursors: initial_cursors,
                members: HashMap::new(),
                pending: BTreeSet::new(),
                pending_attempts: HashMap::new(),
                preparing: HashSet::new(),
                in_flight: HashMap::new(),
                acked: HashSet::new(),
                delivered: 0,
            });
        consumer.record = record;
        if filter_changed {
            consumer.cursors.frontiers.clear();
        }
        consumer
    }
}

impl TransientState {
    pub(super) fn subscriptions_response(&self) -> Vec<TransientSubscriptionResponse> {
        let mut subscriptions = self
            .subscriptions
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
        subscriptions
            .sort_by_key(|subscription| (subscription.connection_id, subscription.sid.clone()));
        subscriptions
    }

    pub(super) fn prepare_transient_deliveries(
        &mut self,
        connections: &ConnectionState,
        subject_name: &str,
        reply_to: Option<&str>,
        headers: &[(String, String)],
        payload: &[u8],
    ) -> (Vec<Delivery>, RouteInterestChanges) {
        let matched = self
            .interest_index
            .matching(subject_name)
            .into_iter()
            .filter_map(|key| {
                let subscription = self.subscriptions.get(&key)?;
                let (connection_id, _) = &key;
                let client = connections.clients.get(connection_id)?;
                let header_refs = headers
                    .iter()
                    .map(|(name, value)| (name.as_str(), value.as_str()))
                    .collect::<Vec<_>>();
                let frame = if header_refs.is_empty() {
                    protocol::msg(subject_name, &subscription.sid, reply_to, payload)
                } else {
                    protocol::hmsg(
                        subject_name,
                        &subscription.sid,
                        reply_to,
                        &header_refs,
                        payload,
                    )
                };
                Some((
                    *connection_id,
                    subscription.sid.clone(),
                    Delivery {
                        sender: client.sender.clone(),
                        frame,
                    },
                ))
            })
            .collect::<Vec<_>>();
        let mut route_changes = RouteInterestChanges::default();
        for (connection_id, sid, _) in &matched {
            route_changes.merge(self.decrement_subscription(*connection_id, sid));
        }
        (
            matched
                .into_iter()
                .map(|(_, _, delivery)| delivery)
                .collect(),
            route_changes,
        )
    }
}

impl Morrow {
    pub(super) async fn producers_response_page(
        &self,
        offset: usize,
        limit: usize,
    ) -> ProducersResponse {
        self.inner
            .lock()
            .await
            .producers_response_page(offset, limit)
    }
    pub(super) async fn dead_letter_response(&self, id: u64) -> Option<DeadLetterResponse> {
        self.inner
            .lock()
            .await
            .dead_letters
            .get(&id)
            .map(|record| DeadLetterResponse {
                id: record.id,
                source_seq: record.source_seq,
                consumer_id: record.consumer_id.clone(),
                source_stream: record.source_stream.clone(),
                source_partition: record.source_partition,
                source_offset: record.source_offset,
                reason: record.reason.clone(),
                attempt_count: record.attempt_count,
                first_delivery_ms: record.first_delivery_ms,
                last_delivery_ms: record.last_delivery_ms,
                payload_bytes: record.payload.len(),
            })
    }

    pub(super) async fn purge_dead_letter(&self, id: u64) -> Result<bool> {
        let mut inner = self.inner.lock().await;
        let purged = inner.purge_dead_letter(id)?;
        drop(inner);
        if purged {
            self.wal.flush_due().await?;
        }
        Ok(purged)
    }

    pub(super) async fn replay_dead_letter(&self, id: u64) -> Result<bool> {
        let mut inner = self.inner.lock().await;
        let replayed = inner.replay_dead_letter(id)?;
        drop(inner);
        if replayed {
            self.metrics
                .dead_letter_replay_outcomes_total
                .fetch_add(1, Ordering::Relaxed);
            self.wal.flush_due().await?;
            self.pull_waiters.notify_all();
            self.deliver_pending().await?;
        }
        Ok(replayed)
    }
    pub(super) async fn dead_letters_response_page(
        &self,
        offset: usize,
        limit: usize,
    ) -> DeadLettersResponse {
        self.inner
            .lock()
            .await
            .dead_letters_response_page(offset, limit)
    }

    pub(super) async fn connections_response(&self) -> ConnectionsResponse {
        self.connections_response_page(None).await
    }

    pub(super) async fn connections_response_page(
        &self,
        page: Option<(usize, usize)>,
    ) -> ConnectionsResponse {
        let durable_counts = {
            let inner = self.inner.lock().await;
            let mut counts = HashMap::new();
            for consumer in inner.consumers.values() {
                for connection_id in consumer.members.keys() {
                    *counts.entry(*connection_id).or_default() += 1;
                }
            }
            counts
        };
        let transient_counts = {
            let transient = self.transient.lock().await;
            let mut counts = HashMap::new();
            for connection_id in transient
                .subscriptions
                .keys()
                .map(|(connection_id, _)| connection_id)
            {
                *counts.entry(*connection_id).or_default() += 1;
            }
            counts
        };
        let connections = self.connections.lock().await;
        match page {
            Some((offset, limit)) => {
                connections.response_page(&durable_counts, &transient_counts, offset, limit)
            }
            None => connections.response(&durable_counts, &transient_counts),
        }
    }

    pub(super) async fn subscriptions_response(&self) -> SubscriptionsResponse {
        let transient_subscriptions = self.transient.lock().await.subscriptions_response();
        let mut response = self.inner.lock().await.subscriptions_response();
        response.transient_subscriptions = transient_subscriptions;
        response
    }

    pub(super) async fn subscriptions_response_page(
        &self,
        offset: usize,
        limit: usize,
    ) -> SubscriptionsPageResponse {
        let response = self.subscriptions_response().await;
        let durable_total_count = response.durable_consumers.len();
        let transient_total_count = response.transient_subscriptions.len();
        let durable_consumers = response
            .durable_consumers
            .into_iter()
            .skip(offset)
            .take(limit)
            .collect::<Vec<_>>();
        let transient_subscriptions = response
            .transient_subscriptions
            .into_iter()
            .skip(offset)
            .take(limit)
            .collect::<Vec<_>>();
        SubscriptionsPageResponse {
            durable_next_offset: (offset + durable_consumers.len() < durable_total_count)
                .then_some(offset + durable_consumers.len()),
            transient_next_offset: (offset + transient_subscriptions.len() < transient_total_count)
                .then_some(offset + transient_subscriptions.len()),
            durable_consumers,
            transient_subscriptions,
            durable_total_count,
            transient_total_count,
        }
    }

    pub(super) async fn connectors_response(&self) -> ConnectorsResponse {
        let connections = self.connections.lock().await;
        let mut connectors = connections
            .clients
            .iter()
            .filter_map(|(connection_id, client)| {
                let durable_id = client.durable_id.as_deref()?;
                durable_id
                    .starts_with("connector-")
                    .then(|| ConnectorResponse {
                        connection_id: *connection_id,
                        durable_id: durable_id.to_string(),
                        status: "connected",
                        authenticated: client.authenticated,
                        connected_at_ms: client.connected_at_ms,
                        protocol_version: client.protocol_version,
                    })
            })
            .collect::<Vec<_>>();
        connectors.sort_by_key(|connector| connector.connection_id);
        ConnectorsResponse {
            count: connectors.len(),
            connectors,
        }
    }

    pub(super) async fn routes_response(&self) -> Option<RouteTopologyResponse> {
        let route_mesh = self.route_mesh.as_ref()?;
        Some(route_mesh.topology_response().await)
    }

    pub(super) async fn wal_status_response(&self) -> WalStatus {
        let inner = self.inner.lock().await;
        inner
            .wal
            .status(inner.messages.len(), inner.consumers.len())
    }

    pub(super) async fn streams_response(&self) -> StreamsResponse {
        let partition_logs = &self.partition_logs;
        let streams = self
            .config
            .streams
            .definitions()
            .iter()
            .map(|definition| {
                let partitions = (0..definition.partitions)
                    .map(|partition| {
                        partition_logs.retention_status(
                            definition.name.as_str(),
                            crate::stream::PartitionId(partition),
                        )
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(StreamResponse {
                    retained_messages: partitions
                        .iter()
                        .map(|partition| partition.retained_messages)
                        .sum(),
                    retained_bytes: partitions
                        .iter()
                        .map(|partition| partition.retained_bytes)
                        .sum(),
                    definition: definition.clone(),
                    partition_status: partitions,
                })
            })
            .collect::<Result<Vec<_>>>()
            .expect("stream status references configured partition logs");
        StreamsResponse {
            streams,
            recovery: partition_logs.recovery_status(),
        }
    }
}
