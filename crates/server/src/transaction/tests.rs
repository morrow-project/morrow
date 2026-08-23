use super::*;
use tempfile::tempdir;

fn limits() -> TransactionLimits {
    TransactionLimits {
        max_messages: 4,
        max_bytes: 32,
        max_partitions: 2,
        max_duration_ms: 100,
        max_concurrent: 2,
    }
}

fn write(offset: u64, partition: u32, bytes: &[u8]) -> TransactionWrite {
    TransactionWrite {
        stream: "orders".into(),
        partition: PartitionId(partition),
        offset,
        bytes: bytes.to_vec(),
    }
}

#[test]
fn commit_exposes_writes_offsets_and_view_mutations_atomically() {
    let mut coordinator = TransactionCoordinator::new(limits()).unwrap();
    coordinator
        .begin("tx-1", "tenant-a", "producer", 1, 0)
        .unwrap();
    coordinator
        .append("tx-1", write(0, 0, b"output"), 1)
        .unwrap();
    coordinator
        .commit_offset(
            "tx-1",
            OffsetCommit {
                consumer: "worker".into(),
                stream: "orders".into(),
                partition: PartitionId(0),
                offset: 0,
            },
            1,
        )
        .unwrap();
    coordinator
        .mutate_view(
            "tx-1",
            ViewMutation {
                view: "orders-by-id".into(),
                key: "id-1".into(),
                value: Some(b"output".to_vec()),
            },
            1,
        )
        .unwrap();
    assert!(coordinator.visible_batch("tx-1").is_none());
    coordinator.prepare("tx-1", 2).unwrap();
    let committed = coordinator.commit("tx-1", 3).unwrap();
    assert_eq!(committed.writes.len(), 1);
    assert_eq!(committed.offsets.len(), 1);
    assert_eq!(committed.views.len(), 1);
    assert_eq!(coordinator.visible_batch("tx-1"), Some(committed));
}

#[test]
fn abort_timeout_recovery_and_fencing_prevent_partial_visibility() {
    let mut coordinator = TransactionCoordinator::new(limits()).unwrap();
    coordinator
        .begin("old", "tenant-a", "producer", 1, 0)
        .unwrap();
    coordinator
        .append("old", write(0, 0, b"partial"), 1)
        .unwrap();
    coordinator.prepare("old", 2).unwrap();
    coordinator
        .begin("new", "tenant-a", "producer", 2, 3)
        .unwrap();
    assert!(matches!(
        coordinator.status("old"),
        Some(TransactionStatus::Aborted { .. })
    ));
    assert!(coordinator.visible_batch("old").is_none());
    assert!(
        coordinator
            .begin("stale", "tenant-a", "producer", 1, 4)
            .is_err()
    );
    coordinator
        .begin("timeout", "tenant-a", "other", 1, 0)
        .unwrap();
    assert_eq!(coordinator.recover(101).unwrap(), 1);
    assert!(coordinator.commit("timeout", 101).is_err());
}

#[test]
fn transaction_limits_and_restart_are_enforced() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("transactions.json");
    let mut coordinator = TransactionCoordinator::open(&path, limits()).unwrap();
    coordinator
        .begin("tx", "tenant-a", "producer", 1, 0)
        .unwrap();
    coordinator.append("tx", write(0, 0, &[0; 32]), 1).unwrap();
    assert!(coordinator.append("tx", write(1, 1, b"x"), 1).is_err());
    drop(coordinator);
    let mut reopened = TransactionCoordinator::open(path, limits()).unwrap();
    assert!(matches!(
        reopened.status("tx"),
        Some(TransactionStatus::Open)
    ));
    reopened.abort("tx", "test").unwrap();
    assert!(reopened.visible_batch("tx").is_none());
}
