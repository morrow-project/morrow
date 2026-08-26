use super::*;
use crate::stream::{
    PartitionFallback, PartitioningPolicy, PartitioningStrategy, RetentionPolicy, StoragePolicy,
};
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
}
