use super::*;
use crate::{
    partition_log::{DEFAULT_NAMESPACE, MessageHeader},
    stream::{StreamDefinition, StreamId},
};

fn catalog() -> StreamCatalog {
    StreamCatalog::new(vec![StreamDefinition {
        name: StreamId::new("orders").unwrap(),
        subjects: vec!["orders/**".into()],
        partitions: 1,
        partitioning: Default::default(),
        storage: Default::default(),
        retention: Default::default(),
    }])
    .unwrap()
}

fn message(seq: u64, offset: u64) -> PublishRecord {
    PublishRecord {
        seq,
        namespace: DEFAULT_NAMESPACE.into(),
        stream: Some("orders".into()),
        partition: Some(0),
        offset: Some(offset),
        subject: "orders/created".into(),
        key: None,
        headers: Vec::<MessageHeader>::new(),
        timestamp_ms: offset * 10,
        reply_to: None,
        payload: vec![],
        partitioning_epoch: 1,
        leader_epoch: 0,
    }
}

#[test]
fn earliest_and_latest_choose_different_initial_offsets() {
    let messages = [(1, message(1, 0)), (2, message(2, 1))]
        .into_iter()
        .collect();
    let earliest = ConsumerCursorSet::new(
        "orders/*",
        StartPosition::Earliest,
        8,
        &catalog(),
        &messages,
    );
    let latest =
        ConsumerCursorSet::new("orders/*", StartPosition::Latest, 8, &catalog(), &messages);
    assert_eq!(earliest.committed_offset("orders", 0), Some(0));
    assert_eq!(latest.committed_offset("orders", 0), Some(2));

    let exact = ConsumerCursorSet::new(
        "orders/*",
        StartPosition::Offset(1),
        8,
        &catalog(),
        &messages,
    );
    let timestamp = ConsumerCursorSet::new(
        "orders/*",
        StartPosition::Timestamp(5),
        8,
        &catalog(),
        &messages,
    );
    assert_eq!(exact.committed_offset("orders", 0), Some(1));
    assert_eq!(timestamp.committed_offset("orders", 0), Some(1));
}

#[test]
fn out_of_order_acknowledgements_close_the_gap() {
    let messages = [(1, message(1, 0)), (2, message(2, 1)), (3, message(3, 2))]
        .into_iter()
        .collect();
    let mut cursors = ConsumerCursorSet::new(
        "orders/*",
        StartPosition::Earliest,
        3,
        &catalog(),
        &messages,
    );
    cursors
        .acknowledge(&messages[&2], "orders/*", &messages)
        .unwrap();
    assert_eq!(cursors.committed_offset("orders", 0), Some(0));
    cursors
        .acknowledge(&messages[&1], "orders/*", &messages)
        .unwrap();
    assert_eq!(cursors.committed_offset("orders", 0), Some(2));
}

#[test]
fn acknowledgement_window_is_bounded() {
    let messages = [(1, message(1, 0)), (2, message(2, 1)), (3, message(3, 2))]
        .into_iter()
        .collect();
    let mut cursors = ConsumerCursorSet::new(
        "orders/*",
        StartPosition::Earliest,
        1,
        &catalog(),
        &messages,
    );
    cursors
        .acknowledge(&messages[&2], "orders/*", &messages)
        .unwrap();
    let error = cursors
        .acknowledge(&messages[&3], "orders/*", &messages)
        .unwrap_err();
    assert!(error.to_string().contains("window exceeded"));
    cursors
        .acknowledge(&messages[&1], "orders/*", &messages)
        .unwrap();
    assert_eq!(cursors.committed_offset("orders", 0), Some(2));
    cursors
        .acknowledge(&messages[&3], "orders/*", &messages)
        .unwrap();
    assert_eq!(cursors.committed_offset("orders", 0), Some(3));
}

#[test]
fn retention_gap_advances_to_earliest_observable_offset() {
    let messages = [(3, message(3, 2))].into_iter().collect();
    let mut cursors = ConsumerCursorSet::new(
        "orders/*",
        StartPosition::Earliest,
        8,
        &catalog(),
        &messages,
    );
    assert_eq!(
        cursors.next_candidate("orders/*", &messages, &HashSet::new()),
        Some(3)
    );
    let cursor = &cursors.partitions["orders:0"];
    assert_eq!(cursor.committed_offset, 2);
    assert_eq!(cursor.retention_gaps, 1);
}
