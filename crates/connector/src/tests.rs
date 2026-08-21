use super::*;
use std::collections::BTreeMap;

fn record(offset: u64) -> ConnectorRecord {
    ConnectorRecord {
        stream: "orders".to_string(),
        partition: 0,
        offset,
        subject: "orders.created".to_string(),
        key: None,
        payload: format!("record-{offset}").into_bytes(),
        schema_id: Some("orders-v1".to_string()),
    }
}

#[test]
fn object_sink_is_idempotent_and_checkpoint_recovers_after_restart() {
    let dir = tempfile::TempDir::new().unwrap();
    let checkpoint = dir.path().join("checkpoint.json");
    let sink = ObjectStoreSink::new("objects", 3, dir.path().join("objects"));
    let store = CheckpointStore::open(&checkpoint, 3).unwrap();
    let mut worker = ConnectorWorker::new(sink, store, 2, 2048, 2, 1024).unwrap();
    worker.enqueue(record(0)).unwrap();
    worker.enqueue(record(1)).unwrap();
    assert!(worker.enqueue(record(2)).is_err());
    assert_eq!(worker.drain_once().unwrap(), 2);
    assert_eq!(worker.checkpoint("orders", 0), Some(1));
    drop(worker);

    let sink = ObjectStoreSink::new("objects", 3, dir.path().join("objects"));
    let store = CheckpointStore::open(&checkpoint, 3).unwrap();
    let mut restarted = ConnectorWorker::new(sink, store, 2, 2048, 2, 1024).unwrap();
    restarted.enqueue(record(1)).unwrap();
    assert_eq!(restarted.drain_once().unwrap(), 1);
    assert_eq!(restarted.checkpoint("orders", 0), Some(1));
}

#[test]
fn append_database_deduplicates_replayed_offsets_after_process_restart() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("database.jsonl");
    let batch = ConnectorBatch {
        generation: 4,
        records: vec![record(7)],
    };
    AppendDatabaseSink::open("database", 4, &path)
        .unwrap()
        .write_batch(&batch)
        .unwrap();
    AppendDatabaseSink::open("database", 4, &path)
        .unwrap()
        .write_batch(&batch)
        .unwrap();
    assert_eq!(std::fs::read_to_string(path).unwrap().lines().count(), 1);
}

struct OutageSink {
    available: bool,
}

impl Connector for OutageSink {
    fn name(&self) -> &str {
        "outage"
    }

    fn generation(&self) -> u64 {
        1
    }
}

impl SinkTask for OutageSink {
    fn write_batch(&mut self, batch: &ConnectorBatch) -> Result<SinkCompletion, String> {
        if !self.available {
            return Err("target unavailable".to_string());
        }
        Ok(SinkCompletion {
            offsets: batch
                .records
                .iter()
                .map(|record| ((record.stream.clone(), record.partition), record.offset))
                .collect::<BTreeMap<_, _>>(),
        })
    }

    fn completion_boundary(&self) -> &'static str {
        "test completion"
    }
}

#[test]
fn target_outage_keeps_a_bounded_unacknowledged_queue() {
    let dir = tempfile::TempDir::new().unwrap();
    let store = CheckpointStore::open(dir.path().join("checkpoint.json"), 1).unwrap();
    let mut worker =
        ConnectorWorker::new(OutageSink { available: false }, store, 2, 2048, 2, 1024).unwrap();
    worker.enqueue(record(0)).unwrap();
    worker.enqueue(record(1)).unwrap();
    assert!(worker.drain_once().is_err());
    assert_eq!(worker.queued(), 2);
    assert!(worker.queued_bytes() <= 2048);
    assert!(worker.enqueue(record(2)).is_err());
}

#[test]
fn oversized_records_are_rejected_before_queueing() {
    let dir = tempfile::TempDir::new().unwrap();
    let store = CheckpointStore::open(dir.path().join("checkpoint.json"), 1).unwrap();
    let mut worker =
        ConnectorWorker::new(OutageSink { available: true }, store, 4, 64, 2, 32).unwrap();
    let mut oversized = record(0);
    oversized.payload = vec![0; 64];
    assert!(worker.enqueue(oversized).is_err());
    assert_eq!(worker.queued(), 0);
    assert_eq!(worker.queued_bytes(), 0);
}

#[test]
fn stale_connector_generation_is_fenced() {
    let dir = tempfile::TempDir::new().unwrap();
    let mut sink = ObjectStoreSink::new("objects", 8, dir.path());
    assert!(
        sink.write_batch(&ConnectorBatch {
            generation: 7,
            records: vec![record(0)],
        })
        .is_err()
    );
}
