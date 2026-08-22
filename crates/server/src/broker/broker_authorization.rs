use super::*;

impl Broker {
    pub(super) async fn authorize_publish(
        &self,
        connection_id: u64,
        subject_name: &str,
    ) -> Result<()> {
        let result = self
            .check_publish_authorization(connection_id, subject_name)
            .await;
        if let Err(err) = &result {
            warn!(
                connection_id,
                subject = %subject_name,
                reason = %err,
                "publish authorization denied"
            );
        }
        result
    }

    async fn check_publish_authorization(
        &self,
        connection_id: u64,
        subject_name: &str,
    ) -> Result<()> {
        if !self.config.auth.enabled {
            return Ok(());
        }
        let connections = self.connections.lock().await;
        let client = connections
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
        let connections = self.connections.lock().await;
        let client = connections
            .clients
            .get(&connection_id)
            .ok_or_else(|| BrokerError::msg("unknown connection"))?;
        crate::broker_ensure!(client.authenticated, "authentication required");
        let inner = self.inner.lock().await;
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
}
