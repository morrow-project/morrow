use super::*;
use crate::stream::{
    CompactionPolicy, PartitionFallback, PartitioningPolicy, PartitioningStrategy, RetentionPolicy,
    StorageMode, StoragePolicy, StreamCatalog, StreamDefinition, StreamId,
    connector_control_streams,
};

pub(super) fn get_streams_config(value: &serde_json::Value) -> Result<StreamCatalog> {
    let mut definitions = match value.get("streams") {
        None | Some(serde_json::Value::Null) => Vec::new(),
        Some(serde_json::Value::Array(streams)) => streams
            .iter()
            .map(parse_stream)
            .collect::<Result<Vec<_>>>()?,
        Some(_) => return Err(BrokerError::msg("config field streams must be an array")),
    };
    if let Some(storage) = connector_control_storage(value)? {
        definitions.extend(connector_control_streams(storage));
    }
    StreamCatalog::new(definitions)
}

fn connector_control_storage(value: &serde_json::Value) -> Result<Option<StoragePolicy>> {
    match value.get("connector_control_plane") {
        None | Some(serde_json::Value::Null) | Some(serde_json::Value::Bool(false)) => Ok(None),
        Some(serde_json::Value::Bool(true)) => Ok(Some(StoragePolicy::default())),
        Some(control @ serde_json::Value::Object(_)) => parse_storage(control).map(Some),
        Some(_) => Err(BrokerError::msg(
            "config field connector_control_plane must be a boolean or object",
        )),
    }
}

fn parse_stream(value: &serde_json::Value) -> Result<StreamDefinition> {
    let serde_json::Value::Object(_) = value else {
        return Err(BrokerError::msg("config field streams[] must be an object"));
    };
    let name = get_string(value, "name")?
        .ok_or_else(|| BrokerError::msg("config field streams[].name is required"))?;
    let subjects = parse_subjects(value)?;
    let partitions = get_u64(value, "partitions")?.unwrap_or(1);
    let partitions = partitions
        .try_into()
        .context("config field streams[].partitions is too large")?;
    Ok(StreamDefinition {
        name: StreamId::new(name)?,
        subjects,
        partitions,
        partitioning: parse_partitioning(value)?,
        storage: parse_storage(value)?,
        retention: parse_retention(value)?,
    })
}

fn parse_subjects(value: &serde_json::Value) -> Result<Vec<String>> {
    let subjects = value
        .get("subjects")
        .ok_or_else(|| BrokerError::msg("config field streams[].subjects is required"))?;
    let serde_json::Value::Array(subjects) = subjects else {
        return Err(BrokerError::msg(
            "config field streams[].subjects must be an array",
        ));
    };
    subjects
        .iter()
        .map(|subject| match subject {
            serde_json::Value::String(subject) => Ok(subject.clone()),
            _ => Err(BrokerError::msg(
                "config field streams[].subjects must contain only strings",
            )),
        })
        .collect()
}

fn parse_partitioning(value: &serde_json::Value) -> Result<PartitioningPolicy> {
    let Some(partitioning) = value.get("partitioning") else {
        return Ok(PartitioningPolicy::default());
    };
    let serde_json::Value::Object(_) = partitioning else {
        return Err(BrokerError::msg(
            "config field streams[].partitioning must be an object",
        ));
    };
    let strategy = match get_string(partitioning, "strategy")?.unwrap_or("key") {
        "key" => PartitioningStrategy::Key,
        "subject_token" => {
            let token = get_u64(partitioning, "token")?.ok_or_else(|| {
                BrokerError::msg(
                    "config field streams[].partitioning.token is required for subject_token",
                )
            })?;
            PartitioningStrategy::SubjectToken {
                token: token
                    .try_into()
                    .context("config field streams[].partitioning.token is too large")?,
            }
        }
        other => {
            return Err(BrokerError::msg(format!(
                "config field streams[].partitioning.strategy has unsupported value {other}"
            )));
        }
    };
    let fallback = match get_string(partitioning, "fallback")?.unwrap_or("sticky") {
        "sticky" => PartitionFallback::Sticky,
        "subject_hash" => PartitionFallback::SubjectHash,
        other => {
            return Err(BrokerError::msg(format!(
                "config field streams[].partitioning.fallback has unsupported value {other}"
            )));
        }
    };
    Ok(PartitioningPolicy {
        strategy,
        fallback,
        epoch: get_u64(partitioning, "epoch")?.unwrap_or(1),
    })
}

fn parse_storage(value: &serde_json::Value) -> Result<StoragePolicy> {
    let Some(storage) = value.get("storage") else {
        return Ok(StoragePolicy::default());
    };
    let serde_json::Value::Object(_) = storage else {
        return Err(BrokerError::msg(
            "config field streams[].storage must be an object",
        ));
    };
    let mode = match get_string(storage, "mode")?.unwrap_or("local") {
        "local" => StorageMode::Local,
        "quorum" => StorageMode::Quorum,
        "quorum_fsync" => StorageMode::QuorumFsync,
        other => {
            return Err(BrokerError::msg(format!(
                "config field streams[].storage.mode has unsupported value {other}"
            )));
        }
    };
    let (default_replicas, default_min_ack) = match mode {
        StorageMode::Local => (1, 1),
        StorageMode::Quorum | StorageMode::QuorumFsync => (3, 2),
    };
    Ok(StoragePolicy {
        mode,
        replicas: get_u64(storage, "replicas")?
            .unwrap_or(default_replicas)
            .try_into()
            .context("config field streams[].storage.replicas is too large")?,
        min_ack_replicas: get_u64(storage, "min_ack_replicas")?
            .unwrap_or(default_min_ack)
            .try_into()
            .context("config field streams[].storage.min_ack_replicas is too large")?,
    })
}

fn parse_retention(value: &serde_json::Value) -> Result<RetentionPolicy> {
    let Some(retention) = value.get("retention") else {
        return Ok(RetentionPolicy::default());
    };
    let serde_json::Value::Object(_) = retention else {
        return Err(BrokerError::msg(
            "config field streams[].retention must be an object",
        ));
    };
    Ok(RetentionPolicy {
        max_age_ms: get_u64(retention, "max_age_ms")?,
        max_bytes: get_u64(retention, "max_bytes")?,
        compaction: match get_string(retention, "compaction")?.unwrap_or("none") {
            "none" => CompactionPolicy::None,
            "key" => CompactionPolicy::Key,
            other => {
                return Err(BrokerError::msg(format!(
                    "config field streams[].retention.compaction has unsupported value {other}"
                )));
            }
        },
    })
}
