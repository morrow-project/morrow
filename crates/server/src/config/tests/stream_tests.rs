use super::*;
use crate::stream::{CompactionPolicy, PartitionFallback, PartitioningStrategy, StorageMode};

#[test]
fn parses_stream_configuration() {
    let config = Config::from_json(&serde_json::json!({
        "wal_dir": "./target/test-wal-stream-config",
        "streams": [{
            "name": "orders",
            "subjects": ["orders.*", "payments.>"],
            "partitions": 32,
            "partitioning": {
                "strategy": "subject_token",
                "token": 1,
                "fallback": "subject_hash",
                "epoch": 2
            },
            "storage": {
                "mode": "quorum",
                "replicas": 5,
                "min_ack_replicas": 3
            },
            "retention": {
                "max_age_ms": 60000,
                "max_bytes": 1048576
            }
        }]
    }))
    .unwrap();

    let stream = &config.streams.definitions()[0];
    assert_eq!(stream.name.as_str(), "orders");
    assert_eq!(stream.partitions, 32);
    assert_eq!(stream.partitioning.epoch, 2);
    assert_eq!(stream.partitioning.fallback, PartitionFallback::SubjectHash);
    assert_eq!(
        stream.partitioning.strategy,
        PartitioningStrategy::SubjectToken { token: 1 }
    );
    assert_eq!(stream.storage.mode, StorageMode::Quorum);
    assert_eq!(stream.storage.replicas, 5);
    assert_eq!(stream.storage.min_ack_replicas, 3);
    assert_eq!(stream.retention.max_age_ms, Some(60_000));
    assert_eq!(stream.retention.max_bytes, Some(1_048_576));
    assert_eq!(stream.retention.compaction, CompactionPolicy::None);
}

#[test]
fn enables_four_key_compacted_connector_control_streams() {
    let config = Config::from_json(&serde_json::json!({
        "wal_dir": "./target/test-connector-control-stream-config",
        "connector_control_plane": {
            "storage": {
                "mode": "quorum_fsync",
                "replicas": 3,
                "min_ack_replicas": 2
            }
        }
    }))
    .unwrap();

    assert_eq!(config.streams.definitions().len(), 4);
    for stream in config.streams.definitions() {
        assert_eq!(stream.retention.compaction, CompactionPolicy::Key);
        assert_eq!(stream.storage.mode, StorageMode::QuorumFsync);
        assert_eq!(stream.partitions, 1);
        assert_eq!(stream.subjects.len(), 1);
    }
    for (_, subject) in protocol::connector_control::CONTROL_SUBJECTS {
        assert_eq!(
            config.streams.resolve_primary(subject).unwrap().subjects,
            vec![subject.to_string()]
        );
    }
}

#[test]
fn rejects_ambiguous_stream_configuration() {
    let error = Config::from_json(&serde_json::json!({
        "wal_dir": "./target/test-wal-ambiguous-stream-config",
        "streams": [
            {"name": "orders", "subjects": ["orders.>"]},
            {"name": "created", "subjects": ["orders.*.created"]}
        ]
    }))
    .unwrap_err();

    assert!(error.to_string().contains("ambiguous stream bindings"));
}

#[test]
fn rejects_stream_with_invalid_partition_count() {
    let error = Config::from_json(&serde_json::json!({
        "wal_dir": "./target/test-wal-zero-stream-partitions",
        "streams": [{"name": "orders", "subjects": ["orders.>"], "partitions": 0}]
    }))
    .unwrap_err();

    assert!(error.to_string().contains("partitions"));
}

#[test]
fn rejects_stream_that_captures_inbox_subjects() {
    let error = Config::from_json(&serde_json::json!({
        "wal_dir": "./target/test-wal-inbox-stream",
        "streams": [{"name": "everything", "subjects": [">"]}]
    }))
    .unwrap_err();

    assert!(error.to_string().contains("reserved inbox"));
}
