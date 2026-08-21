use crate::{CheckpointStore, ConnectorBatch, ConnectorRecord, SinkTask};
use std::collections::VecDeque;

pub struct ConnectorWorker<T: SinkTask> {
    sink: T,
    checkpoints: CheckpointStore,
    queue: VecDeque<ConnectorRecord>,
    max_queue_records: usize,
    max_batch_records: usize,
    max_batch_bytes: usize,
}

impl<T: SinkTask> ConnectorWorker<T> {
    pub fn new(
        sink: T,
        checkpoints: CheckpointStore,
        max_queue_records: usize,
        max_batch_records: usize,
        max_batch_bytes: usize,
    ) -> Result<Self, String> {
        if max_queue_records == 0 || max_batch_records == 0 || max_batch_bytes == 0 {
            return Err("connector queue and batch limits must be positive".to_string());
        }
        Ok(Self {
            sink,
            checkpoints,
            queue: VecDeque::new(),
            max_queue_records,
            max_batch_records,
            max_batch_bytes,
        })
    }

    pub fn enqueue(&mut self, record: ConnectorRecord) -> Result<(), String> {
        if self.queue.len() >= self.max_queue_records {
            return Err("connector queue is full".to_string());
        }
        self.queue.push_back(record);
        Ok(())
    }

    pub fn drain_once(&mut self) -> Result<usize, String> {
        let mut records = Vec::new();
        let mut bytes = 0usize;
        for record in &self.queue {
            let record_bytes = record.payload.len() + record.key.as_ref().map_or(0, Vec::len);
            if records.len() >= self.max_batch_records
                || (!records.is_empty()
                    && bytes.saturating_add(record_bytes) > self.max_batch_bytes)
            {
                break;
            }
            bytes = bytes.saturating_add(record_bytes);
            records.push(record.clone());
        }
        if records.is_empty() {
            return Ok(0);
        }
        let generation = self.sink.generation();
        let completion = self.sink.write_batch(&ConnectorBatch {
            generation,
            records: records.clone(),
        })?;
        self.checkpoints.commit(generation, &completion.offsets)?;
        for _ in 0..records.len() {
            self.queue.pop_front();
        }
        Ok(records.len())
    }

    pub fn queued(&self) -> usize {
        self.queue.len()
    }

    pub fn checkpoint(&self, stream: &str, partition: u32) -> Option<u64> {
        self.checkpoints.offset(stream, partition)
    }
}
