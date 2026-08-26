use super::{BrokerError, Result};
use serde_json::Value;
use std::collections::HashSet;

pub(super) fn reject_unknown_fields(value: &Value) -> Result<()> {
    object(
        value,
        "config",
        &[
            "production",
            "allow_insecure_development",
            "listen",
            "websocket",
            "http_listen",
            "admin_token",
            "admin_token_file",
            "admin_tls",
            "quotas",
            "tenant_quotas",
            "views",
            "wal_dir",
            "encryption_key_dir",
            "encryption_active_key_version",
            "wal_segment_bytes",
            "fsync_interval_ms",
            "max_payload",
            "max_control_line",
            "max_ack_timeout_ms",
            "max_in_flight",
            "max_fetch_messages",
            "max_fetch_bytes",
            "max_encoded_batch_bytes",
            "audit_max_records",
            "audit_segment_bytes",
            "verbose",
            "tls",
            "auth",
            "cluster",
            "streams",
            "connector_control_plane",
        ],
    )?;
    for field in ["tls", "admin_tls"] {
        if let Some(value) = value.get(field).filter(|value| !value.is_null()) {
            object(
                value,
                &format!("config.{field}"),
                &["cert_file", "key_file", "handshake_timeout_ms"],
            )?;
        }
    }
    if let Some(websocket) = value.get("websocket").filter(|value| !value.is_null()) {
        object(
            websocket,
            "config.websocket",
            &["listen", "tls", "allowed_origins"],
        )?;
        if let Some(tls) = websocket.get("tls").filter(|value| !value.is_null()) {
            object(
                tls,
                "config.websocket.tls",
                &["cert_file", "key_file", "handshake_timeout_ms"],
            )?;
        }
    }
    if let Some(quotas) = value.get("quotas") {
        object(
            quotas,
            "config.quotas",
            &[
                "max_connections",
                "max_connections_per_identity",
                "max_transient_subscriptions",
                "max_transient_subscriptions_per_identity",
                "max_durable_consumers",
                "max_durable_consumers_per_identity",
                "max_outbound_bytes_per_connection",
                "max_http_connections",
                "max_raft_connections",
                "max_route_connections",
                "client_idle_timeout_ms",
                "http_header_timeout_ms",
            ],
        )?;
    }
    validate_auth(value.get("auth"))?;
    validate_cluster(value.get("cluster"))?;
    validate_streams(value.get("streams"))?;
    if let Some(control) = value
        .get("connector_control_plane")
        .filter(|value| value.is_object())
    {
        object(control, "config.connector_control_plane", &["storage"])?;
        if let Some(storage) = control.get("storage") {
            object(
                storage,
                "config.connector_control_plane.storage",
                &["mode", "replicas", "min_ack_replicas"],
            )?;
        }
    }
    Ok(())
}

fn validate_auth(auth: Option<&Value>) -> Result<()> {
    let Some(auth) = auth.filter(|value| !value.is_null()) else {
        return Ok(());
    };
    object(auth, "config.auth", &["enabled", "clients"])?;
    if let Some(clients) = auth.get("clients") {
        let Value::Array(clients) = clients else {
            return Ok(());
        };
        for client in clients {
            object(
                client,
                "config.auth.clients[]",
                &[
                    "client_id",
                    "public_key",
                    "public_key_file",
                    "permissions",
                    "tenant",
                    "namespace",
                    "expires_at_ms",
                    "external_subject",
                ],
            )?;
            if let Some(permissions) = client.get("permissions") {
                object(
                    permissions,
                    "config.auth.clients[].permissions",
                    &["publish", "subscribe"],
                )?;
            }
        }
    }
    Ok(())
}

fn validate_cluster(cluster: Option<&Value>) -> Result<()> {
    let Some(cluster) = cluster.filter(|value| !value.is_null()) else {
        return Ok(());
    };
    object(
        cluster,
        "config.cluster",
        &[
            "enabled",
            "role",
            "node_id",
            "auth_token",
            "auth_token_file",
            "raft_listen",
            "raft_tls",
            "allow_insecure_internal_transports",
            "route_listen",
            "route_advertise",
            "route_tls",
            "routes",
            "route_reconnect_ms",
            "raft_dir",
            "bootstrap",
            "nodes",
            "controller_voters",
            "election_timeout_min_ms",
            "election_timeout_max_ms",
            "heartbeat_interval_ms",
            "snapshot_threshold",
        ],
    )?;
    for field in ["raft_tls", "route_tls"] {
        if let Some(tls) = cluster.get(field).filter(|value| !value.is_null()) {
            object(
                tls,
                &format!("config.cluster.{field}"),
                &[
                    "cert_file",
                    "key_file",
                    "ca_cert_file",
                    "handshake_timeout_ms",
                ],
            )?;
        }
    }
    if let Some(nodes) = cluster.get("nodes") {
        let Value::Array(nodes) = nodes else {
            return Ok(());
        };
        for node in nodes {
            object(
                node,
                "config.cluster.nodes[]",
                &[
                    "node_id",
                    "raft_addr",
                    "client_addr",
                    "route_addr",
                    "tls_server_name",
                    "tls_cert_files",
                ],
            )?;
        }
    }
    Ok(())
}

fn validate_streams(streams: Option<&Value>) -> Result<()> {
    let Some(Value::Array(streams)) = streams else {
        return Ok(());
    };
    for stream in streams {
        object(
            stream,
            "config.streams[]",
            &[
                "name",
                "subjects",
                "partitions",
                "partitioning",
                "storage",
                "retention",
            ],
        )?;
        if let Some(partitioning) = stream.get("partitioning") {
            object(
                partitioning,
                "config.streams[].partitioning",
                &["strategy", "token", "fallback", "epoch"],
            )?;
        }
        if let Some(storage) = stream.get("storage") {
            object(
                storage,
                "config.streams[].storage",
                &["mode", "replicas", "min_ack_replicas"],
            )?;
        }
        if let Some(retention) = stream.get("retention") {
            object(
                retention,
                "config.streams[].retention",
                &["max_age_ms", "max_bytes", "compaction"],
            )?;
        }
    }
    Ok(())
}

fn object(value: &Value, path: &str, allowed: &[&str]) -> Result<()> {
    let Value::Object(fields) = value else {
        return Err(BrokerError::msg(format!("{path} must be an object")));
    };
    let allowed = allowed.iter().copied().collect::<HashSet<_>>();
    if let Some(key) = fields.keys().find(|key| !allowed.contains(key.as_str())) {
        return Err(BrokerError::msg(format!(
            "unknown config field {path}.{key}"
        )));
    }
    Ok(())
}
