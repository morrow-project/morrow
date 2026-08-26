use super::partition_batch::*;
use std::time::Duration;

#[test]
fn batches_by_record_and_byte_limits() {
    let mut batcher = PartitionBatcher::new(BatchLimits {
        max_records: 3,
        max_bytes: 10,
        max_delay: Duration::from_secs(1),
    })
    .unwrap();
    assert!(batcher.push(1, 2).is_none());
    let batch = batcher.push(2, 8).unwrap();
    assert_eq!(batch.items, vec![1, 2]);
    assert_eq!(batch.bytes, 10);
    assert_eq!(batcher.len(), 0);
}

#[test]
fn sparse_batches_can_be_flushed_explicitly() {
    let mut batcher = PartitionBatcher::new(BatchLimits {
        max_records: 10,
        max_bytes: 100,
        max_delay: Duration::from_secs(1),
    })
    .unwrap();
    batcher.push("message", 7);
    assert_eq!(batcher.flush().unwrap().items, vec!["message"]);
}
