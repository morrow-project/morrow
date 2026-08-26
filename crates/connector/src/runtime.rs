use crate::{CheckpointStore, ConnectorBatch, ConnectorRecord, SinkTask};
use std::collections::VecDeque;

pub struct ConnectorWorker<T: SinkTask> {
    sink: T,
    checkpoints: CheckpointStore,
    queue: VecDeque<ConnectorRecord>,
    max_queue_records: usize,
    max_queue_bytes: usize,
    queued_bytes: usize,
    max_batch_records: usize,
    max_batch_bytes: usize,
}

impl<T: SinkTask> ConnectorWorker<T> {
    pub fn new(
        sink: T,
        checkpoints: CheckpointStore,
        max_queue_records: usize,
        max_queue_bytes: usize,
        max_batch_records: usize,
        max_batch_bytes: usize,
    ) -> Result<Self, String> {
        if max_queue_records == 0
            || max_queue_bytes == 0
            || max_batch_records == 0
            || max_batch_bytes == 0
        {
            return Err("connector queue and batch limits must be positive".to_string());
        }
        Ok(Self {
            sink,
            checkpoints,
            queue: VecDeque::new(),
            max_queue_records,
            max_queue_bytes,
            queued_bytes: 0,
            max_batch_records,
            max_batch_bytes,
        })
    }

    pub fn enqueue(&mut self, record: ConnectorRecord) -> Result<(), String> {
        let bytes = record_size(&record);
        if bytes > self.max_batch_bytes {
            return Err("connector record exceeds batch byte limit".to_string());
        }
        if self.queue.len() >= self.max_queue_records
            || self.queued_bytes.saturating_add(bytes) > self.max_queue_bytes
        {
            return Err("connector queue is full".to_string());
        }
        self.queued_bytes += bytes;
        self.queue.push_back(record);
        Ok(())
    }

    pub fn drain_once(&mut self) -> Result<usize, String> {
        let mut record_count = 0usize;
        let mut bytes = 0usize;
        for record in &self.queue {
            let record_bytes = record.payload.len() + record.key.as_ref().map_or(0, Vec::len);
            if record_count >= self.max_batch_records
                || (record_count > 0 && bytes.saturating_add(record_bytes) > self.max_batch_bytes)
            {
                break;
            }
            bytes = bytes.saturating_add(record_bytes);
            record_count += 1;
        }
        if record_count == 0 {
            return Ok(0);
        }
        let mut records = Vec::with_capacity(record_count);
        for _ in 0..record_count {
            if let Some(record) = self.queue.pop_front() {
                self.queued_bytes = self.queued_bytes.saturating_sub(record_size(&record));
                records.push(record);
            }
        }
        let generation = self.sink.generation();
        let batch = ConnectorBatch {
            generation,
            records,
        };
        let completion = match self.sink.write_batch(&batch) {
            Ok(completion) => completion,
            Err(error) => {
                self.restore_front(batch.records);
                return Err(error);
            }
        };
        if let Err(error) = self.checkpoints.commit(generation, &completion.offsets) {
            self.restore_front(batch.records);
            return Err(error);
        }
        Ok(batch.records.len())
    }

    pub fn queued(&self) -> usize {
        self.queue.len()
    }

    pub fn queued_bytes(&self) -> usize {
        self.queued_bytes
    }

    pub fn checkpoint(&self, stream: &str, partition: u32) -> Option<u64> {
        self.checkpoints.offset(stream, partition)
    }

    fn restore_front(&mut self, records: Vec<ConnectorRecord>) {
        for record in records.into_iter().rev() {
            self.queued_bytes = self.queued_bytes.saturating_add(record_size(&record));
            self.queue.push_front(record);
        }
    }
}

fn record_size(record: &ConnectorRecord) -> usize {
    record.stream.len()
        + record.subject.len()
        + record.key.as_ref().map_or(0, Vec::len)
        + record.payload.len()
        + record.schema_id.as_ref().map_or(0, String::len)
}
