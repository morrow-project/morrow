use super::*;

impl Broker {
    pub(super) async fn publish(
        &self,
        publisher_id: u64,
        subject_name: String,
        reply_to: Option<String>,
        headers: Vec<(String, String)>,
        key: Option<Vec<u8>>,
        payload: Vec<u8>,
        producer_ack: Option<protocol::ProducerAckRequest>,
    ) -> Result<()> {
        self.publish_with_depth(
            publisher_id,
            subject_name,
            reply_to,
            headers,
            key,
            payload,
            producer_ack,
            0,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn publish_with_depth(
        &self,
        publisher_id: u64,
        subject_name: String,
        reply_to: Option<String>,
        headers: Vec<(String, String)>,
        key: Option<Vec<u8>>,
        payload: Vec<u8>,
        producer_ack: Option<protocol::ProducerAckRequest>,
        recursion_depth: usize,
    ) -> Result<()> {
        if let Some(consumer_ack) = protocol::parse_ack_subject(&subject_name) {
            self.authorize_ack_publish(publisher_id, &consumer_ack)
                .await?;
            let _ = self.ack(consumer_ack).await?;
            if let Some(producer_ack) = &producer_ack {
                self.send_producer_ack(publisher_id, producer_ack, false, None)
                    .await?;
            } else {
                self.send_verbose_ok(publisher_id).await?;
            }
            return Ok(());
        }
        crate::broker_ensure!(
            !subject_name.starts_with("_BROKER."),
            "reserved broker subject"
        );
        crate::broker_ensure!(
            subject::validate_subject(&subject_name),
            "invalid publish subject"
        );
        crate::broker_ensure!(
            payload.len() <= self.config.max_payload,
            "payload exceeds max payload"
        );
        let mut middleware_message = MiddlewareMessage {
            subject: subject_name,
            key,
            headers,
            payload,
            reply_to,
        };
        let mut emitted_messages = Vec::new();
        for stage in [
            MiddlewareStage::Ingress,
            MiddlewareStage::Route,
            MiddlewareStage::BeforeAppend,
        ] {
            let outcome = self
                .middleware
                .process(stage, middleware_message, recursion_depth)
                .map_err(|err| BrokerError::with_source("middleware rejected publish", err))?;
            middleware_message = outcome.message;
            emitted_messages.extend(outcome.emitted);
            match outcome.decision {
                MiddlewareDecision::Continue => {}
                MiddlewareDecision::Drop => {
                    if let Some(ack) = &producer_ack {
                        self.send_producer_ack(publisher_id, ack, false, None)
                            .await?;
                    } else {
                        self.send_verbose_ok(publisher_id).await?;
                    }
                    return Ok(());
                }
                MiddlewareDecision::Reject => crate::broker_bail!("middleware rejected publish"),
            }
        }
        let MiddlewareMessage {
            subject: subject_name,
            key,
            headers,
            payload,
            reply_to,
        } = middleware_message;
        crate::broker_ensure!(
            subject::validate_subject(&subject_name),
            "middleware produced invalid publish subject"
        );
        crate::broker_ensure!(
            payload.len() <= self.config.max_payload,
            "middleware payload exceeds max payload"
        );
        self.authorize_publish(publisher_id, &subject_name).await?;
        for emitted in emitted_messages {
            Box::pin(self.publish_with_depth(
                publisher_id,
                emitted.subject,
                None,
                Vec::new(),
                None,
                emitted.payload,
                None,
                recursion_depth.saturating_add(1),
            ))
            .await?;
        }
        let ack = producer_ack.as_ref();
        if ack.is_some_and(|ack| ack.level == protocol::AckLevel::ClusterDurable)
            && self.cluster_runtime().await.is_none()
        {
            crate::broker_bail!("CLUSTER_DURABLE requires clustered mode");
        }

        let (transient_deliveries, verbose, namespace) = {
            let mut inner = self.inner.lock().await;
            let client = inner.clients.get(&publisher_id);
            let verbose = client
                .map(|client| client.verbose)
                .unwrap_or(self.config.verbose);
            let namespace = client
                .and_then(|client| client.durable_id.clone())
                .unwrap_or_else(|| DEFAULT_NAMESPACE.to_string());
            (
                inner.prepare_transient_deliveries(
                    &subject_name,
                    reply_to.as_deref(),
                    &headers,
                    &payload,
                ),
                verbose,
                namespace,
            )
        };
        for delivery in transient_deliveries {
            let _ = delivery.sender.send(delivery.frame).await;
        }
        self.sync_route_interests().await;
        if let Some(route_mesh) = &self.route_mesh {
            route_mesh
                .forward_publish(&subject_name, reply_to.as_deref(), &payload)
                .await;
        }

        if is_inbox_publish(&subject_name) {
            if let Some(ack) = ack {
                self.send_producer_ack(publisher_id, ack, false, None)
                    .await?;
            } else if verbose {
                self.send_to(publisher_id, protocol::ok().to_vec()).await?;
            }
            return Ok(());
        }

        let stream = self.config.streams.resolve_primary(&subject_name).cloned();
        let accepted_ack = ack.is_some_and(|ack| ack.level == protocol::AckLevel::Accepted);
        if accepted_ack {
            self.send_producer_ack(publisher_id, ack.unwrap(), stream.is_some(), None)
                .await?;
        }
        if stream.is_none() {
            if ack.is_some() && !accepted_ack {
                crate::broker_bail!("NO_DURABLE_BINDING for subject {subject_name}");
            }
            if ack.is_none() && verbose {
                self.send_to(publisher_id, protocol::ok().to_vec()).await?;
            }
            return Ok(());
        }
        let stream = stream.unwrap();

        if let Some(cluster) = self.cluster_runtime().await {
            let partition = select_partition(&stream, &subject_name, key.as_deref(), publisher_id);
            let stored_headers = headers
                .iter()
                .map(|(name, value)| MessageHeader {
                    name: name.clone(),
                    value: value.clone(),
                })
                .collect::<Vec<_>>();
            let seq = self.inner.lock().await.wal.reserve_publish_seq();
            let envelope = MessageEnvelope {
                namespace,
                stream: stream.name.clone(),
                partition,
                offset: 0,
                subject: subject_name,
                key,
                headers: stored_headers,
                timestamp_ms: self.hooks.clock.now_ms(),
                reply_to,
                payload,
                partitioning_epoch: stream.partitioning.epoch,
                leader_epoch: 0,
                legacy_seq: seq,
            };
            let fsync = ack.is_none_or(|ack| ack.level == protocol::AckLevel::HighDurability);
            let envelope = cluster.replicate_partition(envelope, fsync).await?;
            cluster.enforce_retention(self.hooks.clock.now_ms())?;
            self.sync_from_cluster(&cluster).await?;
            self.inner
                .lock()
                .await
                .enforce_stream_retention(&self.config.streams, self.hooks.clock.now_ms())?;
            let committed_record = PublishRecord::from(envelope.clone());
            self.run_after_commit_middleware(publisher_id, &committed_record)
                .await?;
            if let Some(ack) = ack.filter(|_| !accepted_ack) {
                self.send_positioned_producer_ack(publisher_id, ack, &committed_record)
                    .await?;
            }
            self.deliver_pending().await?;
            if ack.is_none() && verbose {
                self.send_to(publisher_id, protocol::ok().to_vec()).await?;
            }
            return Ok(());
        }

        let record = {
            let mut inner = self.inner.lock().await;
            let seq = inner.wal.reserve_publish_seq();
            let stored_headers = headers
                .iter()
                .map(|(name, value)| MessageHeader {
                    name: name.clone(),
                    value: value.clone(),
                })
                .collect::<Vec<_>>();
            let envelope = inner.partition_logs.append(AppendRequest {
                namespace: &namespace,
                stream: &stream,
                subject: &subject_name,
                key: key.as_deref(),
                partition_hint: Some(select_partition(
                    &stream,
                    &subject_name,
                    key.as_deref(),
                    publisher_id,
                )),
                headers: &stored_headers,
                timestamp_ms: self.hooks.clock.now_ms(),
                reply_to: reply_to.as_deref(),
                payload: &payload,
                leader_epoch: 0,
                legacy_seq: Some(seq),
            })?;
            let reference = PartitionAppendRecord::from(&envelope);
            inner.wal.append_partition_append(&reference)?;
            let record = PublishRecord::from(envelope);
            if let (Some(stream), Some(partition), Some(offset)) =
                (record.stream.clone(), record.partition, record.offset)
            {
                inner
                    .partition_sequences
                    .insert((stream, partition, offset), record.seq);
            }
            inner.messages.insert(record.seq, record.clone());
            inner.enforce_stream_retention(&self.config.streams, self.hooks.clock.now_ms())?;
            inner.apply_stream_compaction(&self.config.streams);
            record
        };

        self.run_after_commit_middleware(publisher_id, &record)
            .await?;

        if ack.is_none_or(|ack| ack.level == protocol::AckLevel::HighDurability) {
            match self.hooks.durable_publish_flush_mode {
                DurablePublishFlushMode::SleepThenFlush => {
                    tokio::time::sleep(self.config.fsync_interval()).await;
                    let mut inner = self.inner.lock().await;
                    inner.partition_logs.flush()?;
                    inner.wal.flush()?;
                }
                #[cfg(test)]
                DurablePublishFlushMode::FlushImmediately => {
                    let mut inner = self.inner.lock().await;
                    inner.partition_logs.flush()?;
                    inner.wal.flush()?;
                }
            }
        }

        if let Some(ack) = ack.filter(|_| !accepted_ack) {
            self.send_positioned_producer_ack(publisher_id, ack, &record)
                .await?;
        }

        self.deliver_pending().await?;

        if ack.is_none() && verbose {
            self.send_to(publisher_id, protocol::ok().to_vec()).await?;
        }

        Ok(())
    }

    pub(super) async fn authorize_publish(
        &self,
        connection_id: u64,
        subject_name: &str,
    ) -> Result<()> {
        if !self.config.auth.enabled {
            return Ok(());
        }
        let inner = self.inner.lock().await;
        let client = inner
            .clients
            .get(&connection_id)
            .ok_or_else(|| BrokerError::msg("unknown connection"))?;
        crate::broker_ensure!(client.authenticated, "authentication required");
        let client_id = client
            .durable_id
            .as_deref()
            .ok_or_else(|| BrokerError::msg("authenticated client is missing durable identity"))?;
        if is_inbox_publish(subject_name) {
            crate::broker_ensure!(
                inbox_belongs_to(subject_name, client_id),
                "inbox publish not authorized"
            );
            return Ok(());
        }
        let auth_client = self
            .config
            .auth
            .clients
            .get(client_id)
            .ok_or_else(|| BrokerError::msg("unknown authenticated client"))?;
        let Some(permissions) = &auth_client.permissions else {
            return Ok(());
        };
        let Some(patterns) = &permissions.publish else {
            return Ok(());
        };
        crate::broker_ensure!(
            patterns
                .iter()
                .any(|pattern| subject::matches(pattern, subject_name)),
            "publish not authorized"
        );
        Ok(())
    }

    pub(super) async fn authorize_ack_publish(
        &self,
        connection_id: u64,
        ack: &AckSubject,
    ) -> Result<()> {
        if !self.config.auth.enabled {
            return Ok(());
        }
        let inner = self.inner.lock().await;
        let client = inner
            .clients
            .get(&connection_id)
            .ok_or_else(|| BrokerError::msg("unknown connection"))?;
        crate::broker_ensure!(client.authenticated, "authentication required");
        let consumer = inner
            .consumers
            .get(&ack.consumer_id)
            .ok_or_else(|| BrokerError::msg("ack consumer not found"))?;
        crate::broker_ensure!(
            consumer.members.contains_key(&connection_id),
            "ack not authorized"
        );
        Ok(())
    }

    pub(super) async fn deliver_route_publish(
        &self,
        subject_name: &str,
        reply_to: Option<&str>,
        payload: &[u8],
    ) -> Result<()> {
        crate::broker_ensure!(
            subject::validate_subject(subject_name),
            "invalid route publish subject"
        );
        crate::broker_ensure!(
            payload.len() <= self.config.max_payload,
            "route payload exceeds max payload"
        );
        let deliveries = {
            let mut inner = self.inner.lock().await;
            inner.prepare_transient_deliveries(subject_name, reply_to, &[], payload)
        };
        for delivery in deliveries {
            let _ = delivery.sender.send(delivery.frame).await;
        }
        self.sync_route_interests().await;
        Ok(())
    }

    pub(super) async fn ack(&self, ack: AckSubject) -> Result<bool> {
        let mut inner = self.inner.lock().await;
        let mut should_cleanup = false;
        let mut acknowledged_record = None;
        let valid = inner
            .consumers
            .get(&ack.consumer_id)
            .and_then(|consumer| consumer.in_flight.get(&ack.seq))
            .is_some_and(|in_flight| in_flight.delivery_id == ack.delivery_id);
        if valid {
            let message = inner.messages.get(&ack.seq).cloned();
            acknowledged_record = message.clone();
            let acknowledged_cursors = match message.as_ref() {
                Some(message) if message.offset.is_some() => {
                    let consumer = &inner.consumers[&ack.consumer_id];
                    let mut cursors = consumer.cursors.clone();
                    cursors.acknowledge(
                        message,
                        &consumer.record.filter_subject,
                        &inner.messages,
                    )?;
                    Some(cursors)
                }
                _ => None,
            };
            inner
                .wal
                .append_ack(ack.seq, &ack.consumer_id, ack.delivery_id)?;
            let cursor_snapshot = {
                let consumer = inner.consumers.get_mut(&ack.consumer_id).unwrap();
                consumer.in_flight.remove(&ack.seq);
                consumer.pending.remove(&ack.seq);
                consumer.pending_attempts.remove(&ack.seq);
                if let Some(cursors) = acknowledged_cursors {
                    consumer.cursors = cursors;
                } else if message.is_some() {
                    consumer.acked.insert(ack.seq);
                }
                consumer.cursors.clone()
            };
            inner.wal.append_consumer_cursor(&ConsumerCursorRecord {
                consumer_id: ack.consumer_id.clone(),
                cursors: cursor_snapshot,
            })?;
            should_cleanup = true;
        }
        inner.wal.flush_due()?;
        if should_cleanup {
            inner.cleanup_acked_messages();
        }
        drop(inner);
        if let Some(record) = acknowledged_record {
            let outcome = self
                .middleware
                .process(
                    MiddlewareStage::AfterAck,
                    MiddlewareMessage {
                        subject: record.subject,
                        key: record.key,
                        headers: record
                            .headers
                            .into_iter()
                            .map(|header| (header.name, header.value))
                            .collect(),
                        payload: record.payload,
                        reply_to: record.reply_to,
                    },
                    0,
                )
                .map_err(|err| BrokerError::with_source("after-ack middleware failed", err))?;
            crate::broker_ensure!(
                outcome.decision != MiddlewareDecision::Reject,
                "after-ack middleware rejected acknowledgement"
            );
        }
        Ok(valid)
    }

    pub(super) async fn send_verbose_ok(&self, publisher_id: u64) -> Result<()> {
        let verbose = {
            let inner = self.inner.lock().await;
            inner
                .clients
                .get(&publisher_id)
                .map(|client| client.verbose)
                .unwrap_or(self.config.verbose)
        };
        if verbose {
            self.send_to(publisher_id, protocol::ok().to_vec()).await?;
        }
        Ok(())
    }

    pub(super) async fn deliver_pending(&self) -> Result<()> {
        let deliveries = {
            let mut inner = self.inner.lock().await;
            inner.prepare_durable_deliveries(self.hooks.clock.now_ms())?
        };

        for delivery in deliveries {
            let _ = delivery.sender.send(delivery.frame).await;
        }
        Ok(())
    }

    pub(super) async fn redelivery_loop(self) {
        let mut interval =
            tokio::time::interval(Duration::from_millis(REDELIVERY_SCAN_INTERVAL_MS));
        loop {
            interval.tick().await;
            if let Err(err) = self.expire_and_redeliver().await {
                error!(error = ?err, "redelivery error");
            }
        }
    }

    pub(super) async fn expire_and_redeliver(&self) -> Result<()> {
        let now = self.hooks.clock.now_ms();
        if let Some(cluster) = self.cluster_runtime().await {
            cluster.enforce_retention(now)?;
            self.sync_from_cluster(&cluster).await?;
        }
        {
            let mut inner = self.inner.lock().await;
            inner.enforce_stream_retention(&self.config.streams, now)?;
            let partitioned = inner
                .messages
                .iter()
                .filter(|(_, message)| message.offset.is_some())
                .map(|(seq, _)| *seq)
                .collect::<HashSet<_>>();
            for consumer in inner.consumers.values_mut() {
                let expired: Vec<_> = consumer
                    .in_flight
                    .iter()
                    .filter(|(_, in_flight)| in_flight.deadline_ms <= now)
                    .map(|(seq, _)| *seq)
                    .collect();
                for seq in expired {
                    if let Some(in_flight) = consumer.in_flight.remove(&seq) {
                        if !partitioned.contains(&seq) {
                            consumer.pending.insert(seq);
                        }
                        consumer
                            .pending_attempts
                            .insert(seq, in_flight.attempt.saturating_add(1));
                    }
                }
            }
        }
        self.deliver_pending().await
    }

    pub(super) async fn send_to(&self, connection_id: u64, frame: Vec<u8>) -> Result<()> {
        let sender = {
            let inner = self.inner.lock().await;
            inner
                .clients
                .get(&connection_id)
                .map(|client| client.sender.clone())
                .ok_or_else(|| BrokerError::msg("unknown connection"))?
        };
        sender
            .send(frame)
            .await
            .map_err(|_| BrokerError::msg("connection closed"))
    }

    pub(super) async fn remove_client(&self, connection_id: u64) -> Result<()> {
        let mut inner = self.inner.lock().await;
        inner.clients.remove(&connection_id);
        let removed = inner
            .transient_subscriptions
            .iter()
            .filter(|((client_id, _), _)| *client_id == connection_id)
            .map(|(key, subscription)| (key.clone(), subscription.subject.clone()))
            .collect::<Vec<_>>();
        for (key, subject) in removed {
            inner.transient_subscriptions.remove(&key);
            inner.transient_interest_index.remove(&subject, &key);
        }
        for consumer in inner.consumers.values_mut() {
            consumer.members.remove(&connection_id);
        }
        drop(inner);
        self.sync_route_interests().await;
        Ok(())
    }
}
