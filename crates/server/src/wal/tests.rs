use std::{
    fs::OpenOptions,
    io::Write,
    path::Path,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use super::*;

static TEST_DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

struct TestDir(std::path::PathBuf);

impl TestDir {
    fn new() -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let counter = TEST_DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "broker-wal-test-{}-{nanos}-{counter}",
            std::process::id()
        ));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[test]
fn replays_consumer_publish_delivery_and_ack_state() {
    let dir = TestDir::new();
    let (mut wal, _) = Wal::open(dir.path(), Duration::from_millis(1)).unwrap();
    let consumer = ConsumerRecord {
        consumer_id: "durable-client-1".into(),
        filter_subject: "orders.*".into(),
        queue_group: None,
        ack_timeout_ms: 30_000,
        max_in_flight: 1024,
    };
    wal.append_consumer_upsert(&consumer).unwrap();
    let first = wal.append_publish("orders.created", None, b"one").unwrap();
    let attempt = wal
        .append_delivery_attempt(first.seq, &consumer.consumer_id, 1_000, 1)
        .unwrap();
    wal.append_ack(first.seq, &consumer.consumer_id, attempt.delivery_id)
        .unwrap();
    let second = wal.append_publish("orders.created", None, b"two").unwrap();
    wal.append_delivery_attempt(second.seq, &consumer.consumer_id, 2_000, 1)
        .unwrap();
    wal.flush().unwrap();
    drop(wal);

    let (_, replay) = Wal::open(dir.path(), Duration::from_millis(1)).unwrap();
    let replayed = replay.consumers.get(&consumer.consumer_id).unwrap();
    assert!(replayed.acked.contains(&first.seq));
    assert!(replayed.pending.contains(&second.seq));
    assert!(replayed.in_flight.is_empty());
    assert!(!replay.messages.contains_key(&first.seq));
    assert!(replay.messages.contains_key(&second.seq));
}

#[test]
fn replay_retains_message_until_all_matching_consumers_ack() {
    let dir = TestDir::new();
    let (mut wal, _) = Wal::open(dir.path(), Duration::from_millis(1)).unwrap();
    let first_consumer = ConsumerRecord {
        consumer_id: "durable-client-1".into(),
        filter_subject: "orders.*".into(),
        queue_group: None,
        ack_timeout_ms: 30_000,
        max_in_flight: 1024,
    };
    let second_consumer = ConsumerRecord {
        consumer_id: "durable-client-2".into(),
        filter_subject: "orders.*".into(),
        queue_group: None,
        ack_timeout_ms: 30_000,
        max_in_flight: 1024,
    };
    wal.append_consumer_upsert(&first_consumer).unwrap();
    wal.append_consumer_upsert(&second_consumer).unwrap();
    let message = wal.append_publish("orders.created", None, b"one").unwrap();
    let attempt = wal
        .append_delivery_attempt(message.seq, &first_consumer.consumer_id, 1_000, 1)
        .unwrap();
    wal.append_ack(
        message.seq,
        &first_consumer.consumer_id,
        attempt.delivery_id,
    )
    .unwrap();
    wal.flush().unwrap();
    drop(wal);

    let (_, replay) = Wal::open(dir.path(), Duration::from_millis(1)).unwrap();
    assert!(replay.messages.contains_key(&message.seq));
    assert!(
        replay.consumers[&first_consumer.consumer_id]
            .acked
            .contains(&message.seq)
    );
    assert!(
        replay.consumers[&second_consumer.consumer_id]
            .pending
            .contains(&message.seq)
    );
}

#[test]
fn later_consumers_do_not_receive_older_publishes_on_replay() {
    let dir = TestDir::new();
    let (mut wal, _) = Wal::open(dir.path(), Duration::from_millis(1)).unwrap();
    wal.append_publish("orders.created", None, b"old").unwrap();
    let consumer = ConsumerRecord {
        consumer_id: "durable-client-1".into(),
        filter_subject: "orders.*".into(),
        queue_group: None,
        ack_timeout_ms: 30_000,
        max_in_flight: 1024,
    };
    wal.append_consumer_upsert(&consumer).unwrap();
    wal.flush().unwrap();
    drop(wal);

    let (_, replay) = Wal::open(dir.path(), Duration::from_millis(1)).unwrap();
    assert!(replay.consumers[&consumer.consumer_id].pending.is_empty());
}

#[test]
fn truncates_partial_record_on_replay() {
    let dir = TestDir::new();
    let path = dir.path().join(WAL_FILE);
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&path)
        .unwrap();
    file.write_all(&100_u32.to_le_bytes()).unwrap();
    file.write_all(b"partial").unwrap();
    drop(file);

    let (_, replay) = Wal::open(dir.path(), Duration::from_millis(1)).unwrap();
    assert!(replay.messages.is_empty());
    assert_eq!(std::fs::metadata(path).unwrap().len(), 0);
}
