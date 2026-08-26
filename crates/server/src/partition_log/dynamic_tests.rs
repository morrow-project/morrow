use super::*;
use crate::stream::{
    PartitionFallback, PartitioningPolicy, PartitioningStrategy, RetentionPolicy, StoragePolicy,
};
use std::collections::BTreeSet;
use tempfile::TempDir;

#[test]
fn activated_partition_can_append_and_reload_without_startup_recovery() {
    let dir = TempDir::new().unwrap();
    let stream = StreamDefinition {
        name: StreamId::new("orders").unwrap(),
        subjects: vec!["orders/**".into()],
        partitions: 1,
        partitioning: PartitioningPolicy {
            strategy: PartitioningStrategy::Key,
            fallback: PartitionFallback::SubjectHash,
            epoch: 2,
        },
        storage: StoragePolicy::default(),
        retention: RetentionPolicy::default(),
    };
    let catalog = StreamCatalog::new(vec![stream]).unwrap();
    let (logs, replay) = PartitionLogSet::open(dir.path(), &catalog, 64 * 1024).unwrap();
    assert!(replay.is_empty());
    logs.activate_partition("orders", PartitionId(1)).unwrap();

    let appended = logs
        .append_envelope(MessageEnvelope {
            namespace: "default".into(),
            stream: StreamId::new("orders").unwrap(),
            partition: PartitionId(1),
            offset: 0,
            subject: "orders/created".into(),
            key: None,
            headers: Vec::new(),
            timestamp_ms: 1,
            reply_to: None,
            schema_id: None,
            payload: b"hello".to_vec(),
            partitioning_epoch: 3,
            leader_epoch: 1,
            legacy_seq: 1,
        })
        .unwrap();
    assert_eq!(
        logs.load_envelope("orders", PartitionId(1), appended.offset)
            .unwrap()
            .unwrap()
            .payload,
        b"hello"
    );
    assert_eq!(
        logs.retention_status("orders", PartitionId(1))
            .unwrap()
            .next_offset,
        1
    );
}

#[test]
fn recovery_status_distinguishes_configured_and_assigned_partitions() {
    let dir = TempDir::new().unwrap();
    let stream = StreamDefinition {
        name: StreamId::new("orders").unwrap(),
        subjects: vec!["orders/**".into()],
        partitions: 3,
        partitioning: PartitioningPolicy::default(),
        storage: StoragePolicy::default(),
        retention: RetentionPolicy::default(),
    };
    let catalog = StreamCatalog::new(vec![stream]).unwrap();
    let assigned = BTreeSet::from([("orders".to_string(), 1)]);
    let (logs, _) = PartitionLogSet::open_with_encryption_for_partitions(
        dir.path(),
        &catalog,
        64 * 1024,
        None,
        Some(&assigned),
    )
    .unwrap();
    let status = logs.recovery_status();
    assert_eq!(status.configured_partitions, 3);
    assert_eq!(status.assigned_partitions, 1);
    assert_eq!(status.completed_partitions, 1);
    assert_eq!(status.active_partitions, 1);
}
