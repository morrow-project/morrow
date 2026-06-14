use super::*;

impl Broker {
    pub(super) async fn publish(
        &self,
        publisher_id: u64,
        subject_name: String,
        reply_to: Option<String>,
        payload: Vec<u8>,
    ) -> Result<()> {
        if let Some(ack) = protocol::parse_ack_subject(&subject_name) {
            self.ack(ack).await?;
            self.send_verbose_ok(publisher_id).await?;
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

        let (transient_deliveries, verbose) = {
            let mut inner = self.inner.lock().await;
            let verbose = inner
                .clients
                .get(&publisher_id)
                .map(|client| client.verbose)
                .unwrap_or(self.config.verbose);
            (
                inner.prepare_transient_deliveries(&subject_name, reply_to.as_deref(), &payload),
                verbose,
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
            if verbose {
                self.send_to(publisher_id, protocol::ok().to_vec()).await?;
            }
            return Ok(());
        }

        if let Some(cluster) = self.cluster_runtime().await {
            let has_durable = {
                let inner = self.inner.lock().await;
                inner.has_matching_durable_consumer(&subject_name)
            };
            if !has_durable {
                if verbose {
                    self.send_to(publisher_id, protocol::ok().to_vec()).await?;
                }
                return Ok(());
            }
            self.cluster_write(
                &cluster,
                BrokerCommand::Publish {
                    subject: subject_name,
                    reply_to,
                    payload,
                },
            )
            .await?;
            self.sync_from_cluster(&cluster).await;
            self.deliver_pending().await?;
            if verbose {
                self.send_to(publisher_id, protocol::ok().to_vec()).await?;
            }
            return Ok(());
        }

        let has_durable = {
            let mut inner = self.inner.lock().await;
            let matching_consumers = inner.matching_durable_consumers(&subject_name);
            let has_durable = !matching_consumers.is_empty();
            if has_durable {
                let record =
                    inner
                        .wal
                        .append_publish(&subject_name, reply_to.as_deref(), &payload)?;
                for consumer_id in matching_consumers {
                    if let Some(consumer) = inner.consumers.get_mut(&consumer_id) {
                        consumer.pending.insert(record.seq);
                    }
                }
                inner.messages.insert(record.seq, record);
            } else {
                inner.wal.flush_due()?;
            }

            has_durable
        };

        if has_durable {
            match self.hooks.durable_publish_flush_mode {
                DurablePublishFlushMode::SleepThenFlush => {
                    tokio::time::sleep(self.config.fsync_interval()).await;
                    let mut inner = self.inner.lock().await;
                    inner.wal.flush()?;
                }
                #[cfg(test)]
                DurablePublishFlushMode::FlushImmediately => {
                    let mut inner = self.inner.lock().await;
                    inner.wal.flush()?;
                }
            }
        }

        self.deliver_pending().await?;

        if verbose {
            self.send_to(publisher_id, protocol::ok().to_vec()).await?;
        }

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
            inner.prepare_transient_deliveries(subject_name, reply_to, payload)
        };
        for delivery in deliveries {
            let _ = delivery.sender.send(delivery.frame).await;
        }
        self.sync_route_interests().await;
        Ok(())
    }

    pub(super) async fn ack(&self, ack: AckSubject) -> Result<()> {
        if let Some(cluster) = self.cluster_runtime().await {
            self.cluster_write(
                &cluster,
                BrokerCommand::Ack {
                    seq: ack.seq,
                    consumer_id: ack.consumer_id,
                    delivery_id: ack.delivery_id,
                },
            )
            .await?;
            self.sync_from_cluster(&cluster).await;
            return Ok(());
        }
        let mut inner = self.inner.lock().await;
        let mut should_cleanup = false;
        let valid = inner
            .consumers
            .get(&ack.consumer_id)
            .and_then(|consumer| consumer.in_flight.get(&ack.seq))
            .is_some_and(|in_flight| in_flight.delivery_id == ack.delivery_id);
        if valid {
            inner
                .wal
                .append_ack(ack.seq, &ack.consumer_id, ack.delivery_id)?;
            let consumer = inner.consumers.get_mut(&ack.consumer_id).unwrap();
            consumer.in_flight.remove(&ack.seq);
            consumer.pending.remove(&ack.seq);
            consumer.pending_attempts.remove(&ack.seq);
            consumer.acked.insert(ack.seq);
            should_cleanup = true;
        }
        inner.wal.flush_due()?;
        if should_cleanup {
            inner.cleanup_acked_messages();
        }
        Ok(())
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
        if let Some(cluster) = self.cluster_runtime().await {
            return self.deliver_pending_clustered(cluster).await;
        }
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
        if self.cluster_runtime().await.is_some() {
            return self.deliver_pending().await;
        }
        {
            let mut inner = self.inner.lock().await;
            let now = self.hooks.clock.now_ms();
            for consumer in inner.consumers.values_mut() {
                let expired: Vec<_> = consumer
                    .in_flight
                    .iter()
                    .filter(|(_, in_flight)| in_flight.deadline_ms <= now)
                    .map(|(seq, _)| *seq)
                    .collect();
                for seq in expired {
                    if let Some(in_flight) = consumer.in_flight.remove(&seq) {
                        consumer.pending.insert(seq);
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
        inner
            .transient_subscriptions
            .retain(|(client_id, _), _| *client_id != connection_id);
        for consumer in inner.consumers.values_mut() {
            consumer.members.remove(&connection_id);
        }
        drop(inner);
        self.sync_route_interests().await;
        Ok(())
    }

    pub(super) async fn start_cluster(&self) -> Result<()> {
        let Some(cluster_config) = &self.config.cluster else {
            return Ok(());
        };
        let runtime = RaftRuntime::open(cluster_config, self.tls_acceptor.is_some()).await?;
        runtime.spawn_listener(cluster_config.raft_listen);
        let runtime = ClusterRuntime::real(runtime);
        self.sync_from_cluster(&runtime).await;
        *self.cluster.lock().await = Some(runtime);
        Ok(())
    }

    pub(super) async fn start_route_mesh(&self) -> Result<()> {
        let Some(route_mesh) = &self.route_mesh else {
            return Ok(());
        };
        route_mesh.start(self.clone()).await?;
        self.sync_route_interests().await;
        Ok(())
    }

    pub(super) fn spawn_cluster_log_monitor(&self) {
        if self.config.cluster.is_none() {
            return;
        }
        let broker = self.clone();
        tokio::spawn(async move {
            broker.cluster_log_monitor().await;
        });
    }

    pub(super) async fn cluster_log_monitor(self) {
        let mut previous_leader = self.current_leader_for_log().await;
        let mut full_mesh_formed = false;
        let mut interval =
            tokio::time::interval(Duration::from_millis(CLUSTER_LOG_SCAN_INTERVAL_MS));
        loop {
            interval.tick().await;
            let leader = self.current_leader_for_log().await;
            if leader != previous_leader {
                self.log_cluster_event("cluster leader changed").await;
                previous_leader = leader;
            }

            let Some(route_mesh) = &self.route_mesh else {
                continue;
            };
            let cluster_size = self.cluster_size_for_log().await;
            let formed = cluster_size > 1
                && route_mesh.connected_peer_count().await >= cluster_size.saturating_sub(1);
            if formed && !full_mesh_formed {
                self.log_cluster_event("full member cluster formed").await;
            }
            full_mesh_formed = formed;
        }
    }

    pub(super) async fn log_cluster_event(&self, event: &str) {
        let cluster_size = self.cluster_size_for_log().await;
        let leader_id = format_leader_id(self.current_leader_for_log().await);
        info!(event, cluster_size, leader_id, "cluster lifecycle");
    }

    pub(super) async fn cluster_size_for_log(&self) -> usize {
        self.cluster_runtime()
            .await
            .map(|cluster| cluster.cluster_size())
            .or_else(|| {
                self.config
                    .cluster
                    .as_ref()
                    .map(|cluster| cluster.nodes.len())
            })
            .unwrap_or(1)
    }

    pub(super) async fn current_leader_for_log(&self) -> Option<u64> {
        self.cluster_runtime().await?.current_leader().await
    }

    pub(super) async fn cluster_runtime(&self) -> Option<ClusterRuntime> {
        self.cluster.lock().await.clone()
    }

    pub(super) async fn sync_from_cluster(&self, cluster: &ClusterRuntime) {
        let state = cluster.durable_state();
        let mut inner = self.inner.lock().await;
        inner.sync_durable_state(state);
    }

    pub(super) async fn sync_route_interests(&self) {
        let Some(route_mesh) = &self.route_mesh else {
            return;
        };
        let interests = {
            let inner = self.inner.lock().await;
            inner.route_interests()
        };
        route_mesh.set_local_interests(interests).await;
    }

    pub(super) async fn cluster_write(
        &self,
        cluster: &ClusterRuntime,
        command: BrokerCommand,
    ) -> Result<BrokerResponse> {
        if self.route_mesh.is_some() {
            cluster.client_write_forwarded(command).await
        } else {
            cluster.client_write(command).await
        }
    }

    pub(super) async fn deliver_pending_clustered(&self, cluster: ClusterRuntime) -> Result<()> {
        loop {
            let candidate = {
                let inner = self.inner.lock().await;
                inner.next_cluster_delivery(self.hooks.clock.now_ms())
            };
            let Some(candidate) = candidate else {
                break;
            };
            let response = self
                .cluster_write(
                    &cluster,
                    BrokerCommand::DeliveryAttempt {
                        seq: candidate.seq,
                        consumer_id: candidate.consumer_id.clone(),
                        deadline_ms: candidate.deadline_ms,
                        attempt: candidate.attempt,
                    },
                )
                .await?;
            self.sync_from_cluster(&cluster).await;
            let crate::raft::BrokerResponse::DeliveryAttempt {
                record: Some(record),
            } = response
            else {
                continue;
            };
            let delivery = {
                let mut inner = self.inner.lock().await;
                inner.delivery_for_record(&record, candidate.connection_id, &candidate.sid)
            };
            if let Some(delivery) = delivery {
                let _ = delivery.sender.send(delivery.frame).await;
            }
        }
        Ok(())
    }
}
