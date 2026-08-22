use super::*;

impl Broker {
    pub(super) async fn handle_command(&self, connection_id: u64, command: Command) -> Result<()> {
        match command {
            Command::Connect {
                verbose,
                durable_id,
                ack_timeout_ms,
                max_in_flight,
                protocol_version,
                auth,
            } => {
                self.configure_client(
                    connection_id,
                    verbose,
                    durable_id,
                    ack_timeout_ms,
                    max_in_flight,
                    protocol_version,
                    auth,
                )
                .await
            }
            Command::Ping => self.send_to(connection_id, protocol::pong().to_vec()).await,
            Command::Pong => Ok(()),
            Command::Sub {
                subject,
                queue,
                sid,
                start,
            } => {
                self.subscribe(connection_id, subject, queue, sid, start)
                    .await
            }
            Command::Unsub { sid, max_messages } => {
                self.unsubscribe(connection_id, &sid, max_messages).await
            }
            Command::ConsumerCreate {
                name,
                filter_subject,
                start,
            } => {
                self.create_pull_consumer(connection_id, name, filter_subject, start)
                    .await
            }
            Command::ConsumerDelete { name } => {
                self.delete_pull_consumer(connection_id, name).await
            }
            Command::Fetch {
                name,
                max_messages,
                max_bytes,
                max_wait_ms,
            } => {
                self.fetch_pull(connection_id, name, max_messages, max_bytes, max_wait_ms)
                    .await
            }
            Command::Ack {
                name,
                seq,
                delivery_id,
            } => {
                self.control_pull_delivery(connection_id, name, seq, delivery_id, PullControl::Ack)
                    .await
            }
            Command::Nack {
                name,
                seq,
                delivery_id,
                delay_ms,
            } => {
                self.control_pull_delivery(
                    connection_id,
                    name,
                    seq,
                    delivery_id,
                    PullControl::Nack(delay_ms),
                )
                .await
            }
            Command::Extend {
                name,
                seq,
                delivery_id,
                extension_ms,
            } => {
                self.control_pull_delivery(
                    connection_id,
                    name,
                    seq,
                    delivery_id,
                    PullControl::Extend(extension_ms),
                )
                .await
            }
            Command::Credit {
                sid,
                messages,
                bytes,
            } => {
                self.add_push_credit(connection_id, &sid, messages, bytes)
                    .await
            }
            Command::Pub {
                subject,
                reply_to,
                headers,
                key,
                payload,
                ack,
            } => {
                self.publish(connection_id, subject, reply_to, headers, key, payload, ack)
                    .await
            }
        }
    }

    pub(super) async fn add_client(
        &self,
        id: u64,
        sender: mpsc::Sender<Vec<u8>>,
        remote_addr: Option<SocketAddr>,
    ) -> Result<()> {
        let mut inner = self.inner.lock().await;
        inner.clients.insert(
            id,
            Client {
                sender,
                remote_addr,
                connected_at_ms: self.hooks.clock.now_ms(),
                configured: false,
                verbose: self.config.verbose,
                durable_id: None,
                authenticated: false,
                auth_nonce: if self.config.auth.enabled {
                    Some(auth::nonce()?)
                } else {
                    None
                },
                ack_timeout_ms: DEFAULT_ACK_TIMEOUT_MS,
                max_in_flight: DEFAULT_MAX_IN_FLIGHT,
                protocol_version: 1,
            },
        );
        Ok(())
    }

    pub(super) async fn configure_client(
        &self,
        id: u64,
        verbose: bool,
        durable_id: Option<String>,
        ack_timeout_ms: Option<u64>,
        max_in_flight: Option<usize>,
        protocol_version: Option<u32>,
        auth: Option<ConnectAuth>,
    ) -> Result<()> {
        let ack_timeout_ms = ack_timeout_ms.unwrap_or(DEFAULT_ACK_TIMEOUT_MS);
        let max_in_flight = max_in_flight.unwrap_or(DEFAULT_MAX_IN_FLIGHT);
        crate::broker_ensure!(
            ack_timeout_ms > 0,
            "CONNECT ack_timeout_ms must be greater than zero"
        );
        crate::broker_ensure!(
            ack_timeout_ms <= self.config.max_ack_timeout_ms,
            "CONNECT ack_timeout_ms exceeds server limit {}",
            self.config.max_ack_timeout_ms
        );
        crate::broker_ensure!(
            max_in_flight > 0,
            "CONNECT max_in_flight must be greater than zero"
        );
        crate::broker_ensure!(
            max_in_flight <= self.config.max_in_flight,
            "CONNECT max_in_flight exceeds server limit {}",
            self.config.max_in_flight
        );
        let mut inner = self.inner.lock().await;
        let client = inner
            .clients
            .get_mut(&id)
            .ok_or_else(|| BrokerError::msg("unknown connection"))?;
        crate::broker_ensure!(!client.configured, "CONNECT already received");
        let durable_id = if self.config.auth.enabled {
            let nonce = client
                .auth_nonce
                .as_deref()
                .ok_or_else(|| BrokerError::msg("missing auth nonce"))?;
            let auth = auth
                .as_ref()
                .ok_or_else(|| BrokerError::msg("CONNECT client_id and signature are required"))?;
            let public_key = self
                .config
                .auth
                .clients
                .get(&auth.client_id)
                .ok_or_else(|| BrokerError::msg("unknown client_id"))?;
            let client_id = auth::verify(auth, nonce, &public_key.public_key)?;
            if let Some(durable_id) = durable_id {
                crate::broker_ensure!(
                    durable_id == client_id,
                    "CONNECT durable_id must match authenticated client_id"
                );
            }
            client.authenticated = true;
            Some(client_id)
        } else {
            durable_id
        };
        client.verbose = verbose || self.config.verbose;
        client.durable_id = durable_id;
        client.ack_timeout_ms = ack_timeout_ms;
        client.max_in_flight = max_in_flight;
        client.protocol_version = protocol_version.unwrap_or(1);
        crate::broker_ensure!(
            matches!(client.protocol_version, 1 | 2),
            "unsupported protocol version {}; supported versions are 1 and 2",
            client.protocol_version
        );
        client.configured = true;
        Ok(())
    }

    pub(super) async fn subscribe(
        &self,
        connection_id: u64,
        sub_subject: String,
        queue: Option<String>,
        sid: String,
        start: protocol::StartPosition,
    ) -> Result<()> {
        crate::broker_ensure!(
            subject::validate_subscription(&sub_subject),
            "invalid subscription subject"
        );
        self.authorize_subscribe(connection_id, &sub_subject)
            .await?;
        protocol::validate_identifier("sid", &sid)?;
        if let Some(queue) = &queue {
            protocol::validate_identifier("queue group", queue)?;
        }

        let durable_record = {
            let mut inner = self.inner.lock().await;
            let client = inner
                .clients
                .get(&connection_id)
                .ok_or_else(|| BrokerError::msg("unknown connection"))?;
            if is_inbox_subscription(&sub_subject) || client.durable_id.is_none() {
                crate::broker_ensure!(
                    start == protocol::StartPosition::Latest,
                    "transient subscriptions only support @latest"
                );
                crate::broker_ensure!(
                    queue.is_none(),
                    "transient subscriptions do not support queue groups"
                );
                let key = (connection_id, sid.clone());
                if let Some(existing) = inner.transient_subscriptions.get(&key) {
                    let existing_subject = existing.subject.clone();
                    inner
                        .transient_interest_index
                        .remove(&existing_subject, &key);
                }
                inner
                    .transient_interest_index
                    .insert(&sub_subject, key.clone());
                inner.transient_subscriptions.insert(
                    key,
                    TransientSubscription {
                        subject: sub_subject,
                        sid,
                        remaining_deliveries: None,
                    },
                );
                None
            } else {
                if let Some(durable_id) = &client.durable_id {
                    let consumer_id = consumer_id(durable_id, queue.as_deref(), &sub_subject, &sid);
                    let record = ConsumerRecord {
                        consumer_id: consumer_id.clone(),
                        filter_subject: sub_subject,
                        queue_group: queue,
                        ack_timeout_ms: client.ack_timeout_ms,
                        max_in_flight: client.max_in_flight,
                        start_position: start,
                    };
                    Some((consumer_id, record, sid, client.protocol_version))
                } else {
                    crate::broker_bail!("CONNECT durable_id is required before SUB")
                }
            }
        };

        if let Some((_consumer_id, record, sid, protocol_version)) = durable_record {
            if let Some(cluster) = self.cluster_runtime().await {
                let cursors = {
                    let inner = self.inner.lock().await;
                    crate::consumer_cursor::ConsumerCursorSet::new(
                        &record.filter_subject,
                        record.start_position,
                        record.max_in_flight,
                        &self.config.streams,
                        &inner.messages,
                    )
                };
                self.cluster_write(
                    &cluster,
                    BrokerCommand::CursorConsumerUpsert {
                        record: record.clone(),
                        cursors,
                    },
                )
                .await?;
                self.sync_from_cluster(&cluster).await?;
            } else {
                let mut inner = self.inner.lock().await;
                inner.wal.append_consumer_upsert(&record)?;
                let cursors = inner
                    .upsert_consumer(record.clone(), &self.config.streams)
                    .cursors
                    .clone();
                inner.wal.append_consumer_cursor(&ConsumerCursorRecord {
                    consumer_id: record.consumer_id.clone(),
                    cursors,
                })?;
                inner.wal.flush_due()?;
            }
            let mut inner = self.inner.lock().await;
            let consumer = inner.upsert_consumer(record, &self.config.streams);
            consumer.members.insert(
                connection_id,
                SubscriptionMember {
                    sid,
                    remaining_deliveries: None,
                    credit_messages: if protocol_version >= 2 { 0 } else { usize::MAX },
                    credit_bytes: if protocol_version >= 2 { 0 } else { usize::MAX },
                },
            );
            drop(inner);
            self.deliver_pending().await?;
        }
        self.sync_route_interests().await;
        Ok(())
    }

    pub(super) async fn authorize_subscribe(
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
        if is_inbox_subscription(subject_name) {
            crate::broker_ensure!(
                inbox_belongs_to(subject_name, client_id),
                "inbox subscribe not authorized"
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
        let Some(patterns) = &permissions.subscribe else {
            return Ok(());
        };
        crate::broker_ensure!(
            patterns
                .iter()
                .any(|pattern| subject::matches(pattern, subject_name)),
            "subscribe not authorized"
        );
        Ok(())
    }

    pub(super) async fn unsubscribe(
        &self,
        connection_id: u64,
        sid: &str,
        max_messages: Option<usize>,
    ) -> Result<()> {
        if let Some(max_messages) = max_messages {
            crate::broker_ensure!(
                max_messages > 0,
                "UNSUB max_messages must be greater than zero"
            );
        }
        let mut inner = self.inner.lock().await;
        let mut found = false;
        if let Some(subscription) = inner
            .transient_subscriptions
            .get_mut(&(connection_id, sid.to_string()))
        {
            found = true;
            if let Some(max_messages) = max_messages {
                subscription.remaining_deliveries = Some(max_messages);
            } else {
                let key = (connection_id, sid.to_string());
                let subject = subscription.subject.clone();
                inner.transient_subscriptions.remove(&key);
                inner.transient_interest_index.remove(&subject, &key);
            }
        }
        for consumer in inner.consumers.values_mut() {
            if consumer
                .members
                .get(&connection_id)
                .is_some_and(|member| member.sid == sid)
            {
                if let Some(max_messages) = max_messages {
                    if let Some(member) = consumer.members.get_mut(&connection_id) {
                        member.remaining_deliveries = Some(max_messages);
                    }
                } else {
                    consumer.members.remove(&connection_id);
                }
                found = true;
            }
        }
        crate::broker_ensure!(found, "unknown sid");
        drop(inner);
        self.sync_route_interests().await;
        Ok(())
    }
}
