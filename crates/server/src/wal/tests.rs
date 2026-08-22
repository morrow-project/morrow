use std::{
    collections::{HashMap, HashSet},
    fs::OpenOptions,
    io::Write,
    path::Path,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use super::wal::{segment_path, segmented_paths, tmp_segment_path};
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
            "morrow-wal-test-{}-{nanos}-{counter}",
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
fn opens_empty_directory_with_v1_segment() {
    let dir = TestDir::new();
    let (wal, replay) = open_wal(dir.path());

    assert_eq!(wal.active_segment_id, 1);
    assert_eq!(wal.active_bytes, SEGMENT_HEADER_LEN);
    assert!(segment_path(dir.path(), 1).is_file());
    assert_eq!(replay.next_seq, 1);
}

#[test]
fn replays_consumer_publish_delivery_and_ack_state() {
    let dir = TestDir::new();
    let (mut wal, _) = open_wal(dir.path());
    let consumer = consumer("durable-client-1");
    wal.append_consumer_upsert(&consumer).unwrap();
    let first = wal.append_publish("orders/created", None, b"one").unwrap();
    let attempt = wal
        .append_delivery_attempt(first.seq, &consumer.consumer_id, 1_000, 1)
        .unwrap();
    wal.append_ack(first.seq, &consumer.consumer_id, attempt.delivery_id)
        .unwrap();
    let second = wal.append_publish("orders/created", None, b"two").unwrap();
    wal.append_delivery_attempt(second.seq, &consumer.consumer_id, 2_000, 1)
        .unwrap();
    wal.flush().unwrap();
    drop(wal);

    let (_, replay) = open_wal(dir.path());
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
    let (mut wal, _) = open_wal(dir.path());
    let first_consumer = consumer("durable-client-1");
    let second_consumer = consumer("durable-client-2");
    wal.append_consumer_upsert(&first_consumer).unwrap();
    wal.append_consumer_upsert(&second_consumer).unwrap();
    let message = wal.append_publish("orders/created", None, b"one").unwrap();
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

    let (_, replay) = open_wal(dir.path());
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
    let (mut wal, _) = open_wal(dir.path());
    wal.append_publish("orders/created", None, b"old").unwrap();
    let consumer = consumer("durable-client-1");
    wal.append_consumer_upsert(&consumer).unwrap();
    wal.flush().unwrap();
    drop(wal);

    let (_, replay) = open_wal(dir.path());
    assert!(replay.consumers[&consumer.consumer_id].pending.is_empty());
}

#[test]
fn rotates_and_replays_multiple_segments() {
    let dir = TestDir::new();
    let (mut wal, _) = Wal::open(dir.path(), Duration::from_millis(1), 96).unwrap();
    let consumer = consumer("durable-client-1");
    wal.append_consumer_upsert(&consumer).unwrap();
    wal.append_publish("orders/created", None, b"one").unwrap();
    wal.append_publish("orders/updated", None, b"two").unwrap();
    wal.flush().unwrap();
    assert!(wal.active_segment_id > 1);
    drop(wal);

    let (wal, replay) = Wal::open(dir.path(), Duration::from_millis(1), 96).unwrap();
    assert!(!wal.sealed_segments.is_empty());
    assert_eq!(replay.messages.len(), 2);
    assert_eq!(replay.consumers.len(), 1);
}

#[test]
fn migrates_legacy_wal_to_segmented_layout() {
    let dir = TestDir::new();
    let path = dir.path().join(WAL_FILE);
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&path)
        .unwrap();
    let consumer = consumer("durable-client-1");
    write_record_to(
        &mut file,
        KIND_CONSUMER_UPSERT,
        &consumer_upsert_body(&consumer).unwrap(),
    )
    .unwrap();
    let record = PublishRecord {
        seq: 1,
        namespace: crate::partition_log::DEFAULT_NAMESPACE.to_string(),
        stream: None,
        partition: None,
        offset: None,
        subject: "orders/created".into(),
        key: None,
        headers: Vec::new(),
        timestamp_ms: 0,
        reply_to: None,
        payload: b"one".to_vec(),
        partitioning_epoch: 0,
        leader_epoch: 0,
    };
    write_record_to(&mut file, KIND_PUBLISH, &publish_body(&record).unwrap()).unwrap();
    file.flush().unwrap();
    drop(file);

    let (_, replay) = open_wal(dir.path());

    assert!(segment_path(dir.path(), 1).is_file());
    assert!(dir.path().join(LEGACY_WAL_FILE).is_file());
    assert!(replay.messages.contains_key(&1));
    assert!(replay.consumers.contains_key("durable-client-1"));
}

#[test]
fn checkpoint_removes_covered_segments() {
    let dir = TestDir::new();
    let (mut wal, _) = Wal::open(dir.path(), Duration::from_millis(1), 96).unwrap();
    let consumer = consumer("durable-client-1");
    wal.append_consumer_upsert(&consumer).unwrap();
    let first = wal.append_publish("orders/created", None, b"one").unwrap();
    let second = wal.append_publish("orders/updated", None, b"two").unwrap();
    assert!(wal.active_segment_id > 1);
    let replayed = ReplayedConsumer {
        record: consumer,
        cursors: None,
        pending: [first.seq, second.seq].into_iter().collect(),
        pending_attempts: HashMap::new(),
        in_flight: HashMap::new(),
        acked: HashSet::new(),
    };

    wal.checkpoint(vec![first, second], vec![replayed]).unwrap();

    assert!(wal.sealed_segments.is_empty());
    assert_eq!(segmented_paths(dir.path()).unwrap().len(), 1);
    assert!(wal.metrics.checkpoints >= 1);
    assert!(wal.metrics.deleted_segments >= 1);
    drop(wal);

    let (_, replay) = open_wal(dir.path());
    let consumer = replay.consumers.get("durable-client-1").unwrap();
    assert_eq!(consumer.pending, [1, 2].into_iter().collect());
    assert_eq!(replay.messages.len(), 2);
}

#[test]
fn truncates_torn_final_active_record_on_replay() {
    let dir = TestDir::new();
    let path = segment_path(dir.path(), 1);
    let mut file = super::wal::create_segment(&path).unwrap();
    file.write_all(&100_u32.to_le_bytes()).unwrap();
    file.write_all(b"partial").unwrap();
    drop(file);

    let (wal, replay) = open_wal(dir.path());

    assert!(replay.messages.is_empty());
    assert_eq!(std::fs::metadata(path).unwrap().len(), SEGMENT_HEADER_LEN);
    assert_eq!(wal.metrics.truncations, 1);
}

#[test]
fn corrupt_sealed_segment_fails_replay() {
    let dir = TestDir::new();
    let first = segment_path(dir.path(), 1);
    let mut file = super::wal::create_segment(&first).unwrap();
    file.write_all(&100_u32.to_le_bytes()).unwrap();
    file.write_all(b"partial").unwrap();
    drop(file);
    let second = segment_path(dir.path(), 2);
    super::wal::create_segment(&second).unwrap();

    let err = Wal::open(
        dir.path(),
        Duration::from_millis(1),
        DEFAULT_WAL_SEGMENT_BYTES,
    )
    .unwrap_err();
    assert!(err.to_string().contains("truncated sealed WAL segment"));
}

#[test]
fn interrupted_checkpoint_tmp_is_ignored() {
    let dir = TestDir::new();
    let mut tmp = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(tmp_segment_path(dir.path(), 1))
        .unwrap();
    tmp.write_all(b"junk").unwrap();
    drop(tmp);

    let (wal, replay) = open_wal(dir.path());

    assert_eq!(wal.active_segment_id, 1);
    assert!(replay.messages.is_empty());
    assert!(!tmp_segment_path(dir.path(), 1).exists());
}

#[test]
fn clustered_delivery_ids_are_namespaced_by_node() {
    let dir = TestDir::new();
    let (mut wal, _) = open_wal(dir.path());
    wal.namespace_delivery_ids(7);
    let attempt = wal.append_delivery_attempt(1, "consumer", 10, 1).unwrap();
    assert_eq!(attempt.delivery_id >> 48, 7);
}

fn open_wal(dir: &Path) -> (Wal, Replay) {
    Wal::open(dir, Duration::from_millis(1), DEFAULT_WAL_SEGMENT_BYTES).unwrap()
}

fn consumer(consumer_id: &str) -> ConsumerRecord {
    ConsumerRecord {
        consumer_id: consumer_id.into(),
        filter_subject: "orders/*".into(),
        queue_group: None,
        ack_timeout_ms: 30_000,
        max_in_flight: 1024,
        start_position: protocol::StartPosition::Latest,
    }
}
