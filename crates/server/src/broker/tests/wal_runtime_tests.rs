use crate::wal::{PartitionAppendRecord, Wal};
use std::time::Duration;

fn append(seq: u64, partition: u32) -> PartitionAppendRecord {
    PartitionAppendRecord {
        seq,
        stream: "orders".to_string(),
        partition,
        offset: seq,
        subject: "orders/created".to_string(),
    }
}

#[test]
fn standalone_partition_append_batch_reports_bounded_batch_metrics() {
    let directory = tempfile::tempdir().unwrap();
    let (wal, _) = Wal::open(directory.path(), Duration::from_millis(1), 96).unwrap();
    let runtime = super::super::wal_runtime::WalRuntime::new(wal);

    runtime
        .append_partition_append_batch(vec![append(1, 0), append(2, 0), append(3, 0)])
        .unwrap();

    let status = runtime.status(0, 0);
    assert_eq!(status.partition_append_batches, 1);
    assert_eq!(status.partition_append_records, 3);
    assert_eq!(status.partition_append_batch_max_records, 3);
    assert!(status.partition_append_batch_max_bytes > 0);
}

#[test]
fn standalone_partition_append_batch_records_mixed_partition_inputs() {
    let directory = tempfile::tempdir().unwrap();
    let (wal, _) = Wal::open(directory.path(), Duration::from_millis(1), 96).unwrap();
    let runtime = super::super::wal_runtime::WalRuntime::new(wal);

    runtime
        .append_partition_append_batch(vec![append(1, 0), append(2, 1)])
        .unwrap();

    let status = runtime.status(0, 0);
    assert_eq!(status.partition_append_records, 2);
}
