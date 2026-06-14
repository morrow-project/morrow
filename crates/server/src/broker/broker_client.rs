use super::*;

impl Broker {
    pub(super) async fn handle_command(&self, connection_id: u64, command: Command) -> Result<()> {
        match command {
            Command::Connect {
                verbose,
                durable_id,
                ack_timeout_ms,
                max_in_flight,
                auth,
            } => {
                self.configure_client(
                    connection_id,
                    verbose,
                    durable_id,
                    ack_timeout_ms,
                    max_in_flight,
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
            } => self.subscribe(connection_id, subject, queue, sid).await,
            Command::Unsub { sid, max_messages } => {
                self.unsubscribe(connection_id, &sid, max_messages).await
            }
            Command::Pub {
                subject,
                reply_to,
                payload,
            } => {
                self.publish(connection_id, subject, reply_to, payload)
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
        auth: Option<ConnectAuth>,
    ) -> Result<()> {
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
        client.ack_timeout_ms = ack_timeout_ms.unwrap_or(DEFAULT_ACK_TIMEOUT_MS);
        client.max_in_flight = max_in_flight.unwrap_or(DEFAULT_MAX_IN_FLIGHT);
        crate::broker_ensure!(
            client.ack_timeout_ms > 0,
            "ack_timeout_ms must be greater than zero"
        );
        crate::broker_ensure!(
            client.max_in_flight > 0,
            "max_in_flight must be greater than zero"
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
                    queue.is_none(),
                    "transient subscriptions do not support queue groups"
                );
                inner.transient_subscriptions.insert(
                    (connection_id, sid.clone()),
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
                    };
                    Some((consumer_id, record, sid))
                } else {
                    crate::broker_bail!("CONNECT durable_id is required before SUB")
                }
            }
        };

        if let Some((_consumer_id, record, sid)) = durable_record {
            if let Some(cluster) = self.cluster_runtime().await {
                self.cluster_write(
                    &cluster,
                    BrokerCommand::ConsumerUpsert {
                        record: record.clone(),
                    },
                )
                .await?;
                self.sync_from_cluster(&cluster).await;
            } else {
                let mut inner = self.inner.lock().await;
                inner.wal.append_consumer_upsert(&record)?;
                inner.wal.flush_due()?;
                inner.upsert_consumer(record.clone());
            }
            let mut inner = self.inner.lock().await;
            let consumer = inner.upsert_consumer(record);
            consumer.members.insert(
                connection_id,
                SubscriptionMember {
                    sid,
                    remaining_deliveries: None,
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
        if is_inbox_subscription(subject_name) {
            return Ok(());
        }
        let client_id = client
            .durable_id
            .as_deref()
            .ok_or_else(|| BrokerError::msg("authenticated client is missing durable identity"))?;
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
                inner
                    .transient_subscriptions
                    .remove(&(connection_id, sid.to_string()));
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
