use super::*;

impl Default for ResourceQuotaConfig {
    fn default() -> Self {
        Self {
            max_connections: 10_000,
            max_connections_per_identity: 100,
            max_transient_subscriptions: 100_000,
            max_transient_subscriptions_per_identity: 1_000,
            max_durable_consumers: 100_000,
            max_durable_consumers_per_identity: 1_000,
            max_outbound_bytes_per_connection: 16 * 1024 * 1024,
            max_http_connections: 128,
            max_raft_connections: 1_024,
            max_route_connections: 1_024,
            client_idle_timeout_ms: 5 * 60 * 1_000,
            http_header_timeout_ms: 5_000,
        }
    }
}

pub(super) fn get_resource_quotas(value: &serde_json::Value) -> Result<ResourceQuotaConfig> {
    let defaults = ResourceQuotaConfig::default();
    let Some(quotas) = value.get("quotas") else {
        return Ok(defaults);
    };
    let serde_json::Value::Object(_) = quotas else {
        return Err(BrokerError::msg("config field quotas must be an object"));
    };
    let config = ResourceQuotaConfig {
        max_connections: quota_usize(quotas, "max_connections")?
            .unwrap_or(defaults.max_connections),
        max_connections_per_identity: quota_usize(quotas, "max_connections_per_identity")?
            .unwrap_or(defaults.max_connections_per_identity),
        max_transient_subscriptions: quota_usize(quotas, "max_transient_subscriptions")?
            .unwrap_or(defaults.max_transient_subscriptions),
        max_transient_subscriptions_per_identity: quota_usize(
            quotas,
            "max_transient_subscriptions_per_identity",
        )?
        .unwrap_or(defaults.max_transient_subscriptions_per_identity),
        max_durable_consumers: quota_usize(quotas, "max_durable_consumers")?
            .unwrap_or(defaults.max_durable_consumers),
        max_durable_consumers_per_identity: quota_usize(
            quotas,
            "max_durable_consumers_per_identity",
        )?
        .unwrap_or(defaults.max_durable_consumers_per_identity),
        max_outbound_bytes_per_connection: quota_usize(
            quotas,
            "max_outbound_bytes_per_connection",
        )?
        .unwrap_or(defaults.max_outbound_bytes_per_connection),
        max_http_connections: quota_usize(quotas, "max_http_connections")?
            .unwrap_or(defaults.max_http_connections),
        max_raft_connections: quota_usize(quotas, "max_raft_connections")?
            .unwrap_or(defaults.max_raft_connections),
        max_route_connections: quota_usize(quotas, "max_route_connections")?
            .unwrap_or(defaults.max_route_connections),
        client_idle_timeout_ms: get_u64(quotas, "client_idle_timeout_ms")?
            .unwrap_or(defaults.client_idle_timeout_ms),
        http_header_timeout_ms: get_u64(quotas, "http_header_timeout_ms")?
            .unwrap_or(defaults.http_header_timeout_ms),
    };
    for (name, value) in [
        ("max_connections", config.max_connections),
        (
            "max_connections_per_identity",
            config.max_connections_per_identity,
        ),
        (
            "max_transient_subscriptions",
            config.max_transient_subscriptions,
        ),
        (
            "max_transient_subscriptions_per_identity",
            config.max_transient_subscriptions_per_identity,
        ),
        ("max_durable_consumers", config.max_durable_consumers),
        (
            "max_durable_consumers_per_identity",
            config.max_durable_consumers_per_identity,
        ),
        (
            "max_outbound_bytes_per_connection",
            config.max_outbound_bytes_per_connection,
        ),
        ("max_http_connections", config.max_http_connections),
        ("max_raft_connections", config.max_raft_connections),
        ("max_route_connections", config.max_route_connections),
    ] {
        crate::broker_ensure!(
            value > 0,
            "config field quotas.{name} must be greater than zero"
        );
    }
    crate::broker_ensure!(
        config.client_idle_timeout_ms > 0,
        "config field quotas.client_idle_timeout_ms must be greater than zero"
    );
    crate::broker_ensure!(
        config.http_header_timeout_ms > 0,
        "config field quotas.http_header_timeout_ms must be greater than zero"
    );
    Ok(config)
}

fn quota_usize(value: &serde_json::Value, key: &str) -> Result<Option<usize>> {
    get_u64(value, key)?
        .map(|value| {
            value
                .try_into()
                .map_err(|_| BrokerError::msg(format!("config field quotas.{key} is too large")))
        })
        .transpose()
}
