use super::*;

impl Morrow {
    pub(super) fn record_quota_rejection(
        &self,
        connection_id: u64,
        tenant: &str,
        dimension: &str,
        reason: &str,
    ) {
        let mut details = std::collections::BTreeMap::new();
        details.insert("dimension".to_string(), dimension.to_string());
        details.insert("reason".to_string(), reason.to_string());
        self.record_audit_event(crate::tenancy::AuditEvent {
            sequence: 0,
            timestamp_ms: self.hooks.clock.now_ms(),
            actor: format!("connection:{connection_id}"),
            tenant: crate::tenancy::TenantId::new(tenant.to_string()).ok(),
            action: "quota.reject".to_string(),
            resource: format!("tenant/{tenant}/quota"),
            outcome: "denied".to_string(),
            details,
        });
    }

    pub fn policy_store(&self) -> Arc<crate::tenancy::PolicyStore> {
        self.policy.clone()
    }

    pub(super) async fn authorize_policy(
        &self,
        connection_id: u64,
        permission: crate::tenancy::Permission,
        resource: Option<&str>,
    ) -> Result<()> {
        if !self.config.auth.enabled {
            return Ok(());
        }
        let subject = self
            .connections
            .lock()
            .await
            .clients
            .get(&connection_id)
            .and_then(|client| client.durable_id.clone())
            .ok_or_else(|| BrokerError::msg("authenticated client is missing durable identity"))?;
        let client_config = self
            .config
            .auth
            .clients
            .get(&subject)
            .ok_or_else(|| BrokerError::msg("unknown authenticated client"))?;
        let scope = crate::tenancy::ResourceScope {
            tenant: crate::tenancy::TenantId::new(client_config.tenant.clone())?,
            namespace: crate::tenancy::NamespaceId::new(client_config.namespace.clone())?,
        };
        let policy_subject = client_config
            .external_subject
            .as_deref()
            .unwrap_or(&subject);
        let scoped_mode = self
            .config
            .auth
            .clients
            .values()
            .any(|client| client.tenant != "default" || client.namespace != "default");
        if scoped_mode {
            if let Some(resource) = resource {
                crate::broker_ensure!(
                    scope.contains_subject(resource),
                    "resource is outside authenticated tenant namespace"
                );
            }
        }
        self.policy.authorize(
            policy_subject,
            &scope,
            permission,
            self.hooks.clock.now_ms(),
        )
    }

    pub(super) fn record_audit_event(&self, event: crate::tenancy::AuditEvent) -> Result<()> {
        self.audit
            .lock()
            .expect("audit log lock poisoned")
            .append(event)
            .map(|_| ())
    }

    pub(super) fn record_authorization_denial(
        &self,
        connection_id: u64,
        action: &str,
        resource: &str,
        reason: &str,
    ) {
        let mut details = std::collections::BTreeMap::new();
        details.insert("connection_id".to_string(), connection_id.to_string());
        details.insert("reason".to_string(), reason.to_string());
        let event = crate::tenancy::AuditEvent {
            sequence: 0,
            timestamp_ms: self.hooks.clock.now_ms(),
            actor: format!("connection:{connection_id}"),
            tenant: crate::tenancy::TenantId::new("default").ok(),
            action: action.to_string(),
            resource: resource.to_string(),
            outcome: "denied".to_string(),
            details,
        };
        if let Err(error) = self.record_audit_event(event) {
            error!(%error, "audit append failed while recording authorization denial");
        }
    }

    pub(super) async fn authorize_publish(
        &self,
        connection_id: u64,
        subject_name: &str,
    ) -> Result<()> {
        let result = self
            .check_publish_authorization(connection_id, subject_name)
            .await;
        if let Err(err) = &result {
            self.record_authorization_denial(
                connection_id,
                "publish",
                subject_name,
                &err.to_string(),
            );
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
            .ok_or_else(|| BrokerError::msg("authenticated client is missing durable identity"))?
            .to_string();
        if is_inbox_publish(subject_name) {
            crate::broker_ensure!(
                inbox_belongs_to(subject_name, &client_id),
                "inbox publish not authorized"
            );
            return Ok(());
        }
        drop(connections);
        self.authorize_policy(
            connection_id,
            crate::tenancy::Permission::Publish,
            Some(subject_name),
        )
        .await?;
        let auth_client = self
            .config
            .auth
            .clients
            .get(&client_id)
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
