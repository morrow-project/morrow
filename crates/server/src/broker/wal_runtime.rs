use super::*;
use crate::wal::GroupStateRecord;
use std::collections::HashMap;
use std::sync::mpsc::{self, SyncSender};

const WAL_QUEUE_CAPACITY: usize = 128;
const MAX_PARTITION_APPEND_BATCH_RECORDS: usize = 256;
const MAX_PARTITION_APPEND_BATCH_BYTES: usize = 8 * 1024 * 1024;

#[derive(Clone)]
pub(super) struct WalRuntime {
    sender: SyncSender<WalCommand>,
    next_publish_seq: Arc<AtomicU64>,
    flush_coordinator: Arc<FlushCoordinator>,
    partition_flush_coordinators: Arc<tokio::sync::Mutex<HashMap<String, Arc<FlushCoordinator>>>>,
}

struct FlushCoordinator {
    state: tokio::sync::Mutex<FlushState>,
}

struct FlushState {
    running: bool,
    waiters: Vec<tokio::sync::oneshot::Sender<Result<()>>>,
}

enum WalCommand {
    PartitionAppend(PartitionAppendRecord, mpsc::Sender<Result<()>>),
    PartitionAppendBatch(Vec<PartitionAppendRecord>, mpsc::Sender<Result<()>>),
    ConsumerUpsert(ConsumerRecord, mpsc::Sender<Result<()>>),
    ConsumerCursor(ConsumerCursorRecord, mpsc::Sender<Result<()>>),
    ConsumerCursorDelta(ConsumerCursorDeltaRecord, mpsc::Sender<Result<()>>),
    ConsumerDelete(String, mpsc::Sender<Result<()>>),
    DeliveryAttempt {
        seq: u64,
        consumer_id: String,
        deadline_ms: u64,
        attempt: u32,
        response: mpsc::Sender<Result<DeliveryAttemptRecord>>,
    },
    DeliveryLease(DeliveryAttemptRecord, mpsc::Sender<Result<()>>),
    DeliveryBatch {
        entries: Vec<DeliveryBatchEntry>,
        response: mpsc::Sender<Result<Vec<DeliveryAttemptRecord>>>,
    },
    Ack {
        seq: u64,
        consumer_id: String,
        delivery_id: u64,
        response: mpsc::Sender<Result<()>>,
    },
    DeadLetter(DeadLetterRecord, mpsc::Sender<Result<()>>),
    DeadLetterPurge(u64, mpsc::Sender<Result<()>>),
    ProducerSequence(ProducerSequenceRecord, mpsc::Sender<Result<()>>),
    GroupState(
        String,
        crate::consumer_group::GroupRecord,
        mpsc::Sender<Result<()>>,
    ),
    FlushDue(mpsc::Sender<Result<()>>),
    Flush(mpsc::Sender<Result<()>>),
    Checkpoint {
        messages: Vec<PublishRecord>,
        consumers: Vec<ReplayedConsumer>,
        dead_letters: Vec<DeadLetterRecord>,
        producer_sequences: Vec<ProducerSequenceRecord>,
        groups: Vec<GroupStateRecord>,
        response: mpsc::Sender<Result<()>>,
    },
    Status {
        retained_message_count: usize,
        consumer_count: usize,
        response: mpsc::Sender<Result<WalStatus>>,
    },
}

pub(super) struct DeliveryBatchEntry {
    pub(super) seq: u64,
    pub(super) consumer_id: String,
    pub(super) deadline_ms: u64,
    pub(super) attempt: u32,
    pub(super) cursors: ConsumerCursorRecord,
}

impl WalRuntime {
    pub(super) fn new(wal: Wal) -> Self {
        let next_publish_seq = wal.next_publish_seq();
        let (sender, receiver) = mpsc::sync_channel(WAL_QUEUE_CAPACITY);
        std::thread::Builder::new()
            .name("morrow-wal".to_string())
            .spawn(move || wal_worker(wal, receiver))
            .expect("spawning WAL worker");
        Self {
            sender,
            next_publish_seq: Arc::new(AtomicU64::new(next_publish_seq)),
            flush_coordinator: Arc::new(FlushCoordinator {
                state: tokio::sync::Mutex::new(FlushState {
                    running: false,
                    waiters: Vec::new(),
                }),
            }),
            partition_flush_coordinators: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        }
    }

    pub(super) fn reserve_publish_seq(&self) -> u64 {
        self.next_publish_seq.fetch_add(1, Ordering::Relaxed)
    }

    pub(super) fn append_partition_append(&self, record: &PartitionAppendRecord) -> Result<()> {
        self.request(|response| WalCommand::PartitionAppend(record.clone(), response))
    }

    pub(super) fn append_partition_append_batch(
        &self,
        records: Vec<PartitionAppendRecord>,
    ) -> Result<()> {
        crate::broker_ensure!(
            !records.is_empty() && records.len() <= MAX_PARTITION_APPEND_BATCH_RECORDS,
            "partition append batch is outside the supported bound"
        );
        let encoded_bytes = records
            .iter()
            .map(|record| {
                record.stream.len() + record.subject.len() + std::mem::size_of::<u64>() * 2
            })
            .sum::<usize>();
        crate::broker_ensure!(
            encoded_bytes <= MAX_PARTITION_APPEND_BATCH_BYTES,
            "partition append batch bytes exceed the supported bound"
        );
        self.request(|response| WalCommand::PartitionAppendBatch(records, response))
    }

    /// Async callers use these adapters so bounded queue backpressure and WAL
    /// response waits never block a Tokio worker. The synchronous methods are
    /// retained for startup/recovery code that is not running on an async
    /// executor.
    pub(super) async fn append_partition_append_async(
        &self,
        record: PartitionAppendRecord,
    ) -> Result<()> {
        self.request_async(move |response| WalCommand::PartitionAppend(record, response))
            .await
    }

    pub(super) fn append_consumer_upsert(&self, record: &ConsumerRecord) -> Result<()> {
        self.request(|response| WalCommand::ConsumerUpsert(record.clone(), response))
    }

    pub(super) async fn append_consumer_upsert_async(&self, record: ConsumerRecord) -> Result<()> {
        self.request_async(move |response| WalCommand::ConsumerUpsert(record, response))
            .await
    }

    pub(super) fn append_consumer_cursor(&self, record: &ConsumerCursorRecord) -> Result<()> {
        self.request(|response| WalCommand::ConsumerCursor(record.clone(), response))
    }

    pub(super) fn append_consumer_cursor_delta(
        &self,
        record: &ConsumerCursorDeltaRecord,
    ) -> Result<()> {
        self.request(|response| WalCommand::ConsumerCursorDelta(record.clone(), response))
    }

    pub(super) async fn append_consumer_cursor_async(
        &self,
        record: ConsumerCursorRecord,
    ) -> Result<()> {
        self.request_async(move |response| WalCommand::ConsumerCursor(record, response))
            .await
    }

    pub(super) fn append_consumer_delete(&self, consumer_id: &str) -> Result<()> {
        self.request(|response| WalCommand::ConsumerDelete(consumer_id.to_string(), response))
    }

    pub(super) async fn append_consumer_delete_async(&self, consumer_id: String) -> Result<()> {
        self.request_async(move |response| WalCommand::ConsumerDelete(consumer_id, response))
            .await
    }

    pub(super) fn append_delivery_attempt(
        &self,
        seq: u64,
        consumer_id: &str,
        deadline_ms: u64,
        attempt: u32,
    ) -> Result<DeliveryAttemptRecord> {
        self.request(|response| WalCommand::DeliveryAttempt {
            seq,
            consumer_id: consumer_id.to_string(),
            deadline_ms,
            attempt,
            response,
        })
    }

    pub(super) async fn append_delivery_attempt_async(
        &self,
        seq: u64,
        consumer_id: String,
        deadline_ms: u64,
        attempt: u32,
    ) -> Result<DeliveryAttemptRecord> {
        self.request_async(move |response| WalCommand::DeliveryAttempt {
            seq,
            consumer_id,
            deadline_ms,
            attempt,
            response,
        })
        .await
    }

    pub(super) fn append_delivery_lease(&self, record: &DeliveryAttemptRecord) -> Result<()> {
        self.request(|response| WalCommand::DeliveryLease(record.clone(), response))
    }

    pub(super) async fn append_delivery_lease_async(
        &self,
        record: DeliveryAttemptRecord,
    ) -> Result<()> {
        self.request_async(move |response| WalCommand::DeliveryLease(record, response))
            .await
    }

    pub(super) fn append_delivery_batch(
        &self,
        entries: Vec<DeliveryBatchEntry>,
    ) -> Result<Vec<DeliveryAttemptRecord>> {
        self.request(|response| WalCommand::DeliveryBatch { entries, response })
    }

    pub(super) async fn append_delivery_batch_async(
        &self,
        entries: Vec<DeliveryBatchEntry>,
    ) -> Result<Vec<DeliveryAttemptRecord>> {
        self.request_async(move |response| WalCommand::DeliveryBatch { entries, response })
            .await
    }

    pub(super) fn append_ack(&self, seq: u64, consumer_id: &str, delivery_id: u64) -> Result<()> {
        self.request(|response| WalCommand::Ack {
            seq,
            consumer_id: consumer_id.to_string(),
            delivery_id,
            response,
        })
    }

    pub(super) async fn append_ack_async(
        &self,
        seq: u64,
        consumer_id: String,
        delivery_id: u64,
    ) -> Result<()> {
        self.request_async(move |response| WalCommand::Ack {
            seq,
            consumer_id,
            delivery_id,
            response,
        })
        .await
    }

    pub(super) fn append_dead_letter(&self, record: &DeadLetterRecord) -> Result<()> {
        self.request(|response| WalCommand::DeadLetter(record.clone(), response))
    }

    pub(super) async fn append_dead_letter_async(&self, record: DeadLetterRecord) -> Result<()> {
        self.request_async(move |response| WalCommand::DeadLetter(record, response))
            .await
    }

    pub(super) fn purge_dead_letter(&self, id: u64) -> Result<()> {
        self.request(|response| WalCommand::DeadLetterPurge(id, response))
    }

    pub(super) async fn purge_dead_letter_async(&self, id: u64) -> Result<()> {
        self.request_async(move |response| WalCommand::DeadLetterPurge(id, response))
            .await
    }

    pub(super) fn append_producer_sequence(&self, record: &ProducerSequenceRecord) -> Result<()> {
        self.request(|response| WalCommand::ProducerSequence(record.clone(), response))
    }

    pub(super) async fn append_producer_sequence_async(
        &self,
        record: ProducerSequenceRecord,
    ) -> Result<()> {
        self.request_async(move |response| WalCommand::ProducerSequence(record, response))
            .await
    }

    pub(super) fn append_group_state(
        &self,
        group: &str,
        record: &crate::consumer_group::GroupRecord,
    ) -> Result<()> {
        self.request(|response| WalCommand::GroupState(group.to_string(), record.clone(), response))
    }

    pub(super) async fn append_group_state_async(
        &self,
        group: String,
        record: crate::consumer_group::GroupRecord,
    ) -> Result<()> {
        self.request_async(move |response| WalCommand::GroupState(group, record, response))
            .await
    }

    pub(super) async fn flush_due(&self) -> Result<()> {
        let runtime = self.clone();
        tokio::task::spawn_blocking(move || runtime.request(WalCommand::FlushDue))
            .await
            .map_err(|err| BrokerError::with_source("WAL worker join failed", err))?
    }

    pub(super) async fn flush(&self) -> Result<()> {
        let runtime = self.clone();
        tokio::task::spawn_blocking(move || runtime.request(WalCommand::Flush))
            .await
            .map_err(|err| BrokerError::with_source("WAL worker join failed", err))?
    }

    /// Join concurrent durability requests into one WAL barrier. The first
    /// caller opens a commit epoch; arrivals during the configured interval
    /// share its flush and all receive the same result.
    pub(super) async fn flush_grouped(&self, interval: Duration) -> Result<()> {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let start_worker = {
            let mut state = self.flush_coordinator.state.lock().await;
            state.waiters.push(sender);
            if state.running {
                false
            } else {
                state.running = true;
                true
            }
        };
        if start_worker {
            let runtime = self.clone();
            let coordinator = self.flush_coordinator.clone();
            tokio::spawn(async move {
                tokio::time::sleep(interval).await;
                let result = runtime.flush().await;
                let mut state = coordinator.state.lock().await;
                state.running = false;
                for waiter in state.waiters.drain(..) {
                    let outcome = match &result {
                        Ok(()) => Ok(()),
                        Err(error) => Err(BrokerError::msg(error.to_string())),
                    };
                    let _ = waiter.send(outcome);
                }
            });
        }
        receiver
            .await
            .map_err(|_| BrokerError::msg("WAL group-commit coordinator stopped"))?
    }

    /// Join concurrent high-durability partition flushes into one physical
    /// barrier. The whole partition log set is flushed once for the group;
    /// callers still await the same durability boundary individually.
    pub(super) async fn flush_partitions_grouped(
        &self,
        partition_logs: Arc<crate::partition_log::PartitionLogSet>,
        stream: String,
        partition: crate::stream::PartitionId,
        interval: Duration,
    ) -> Result<()> {
        let key = format!("{stream}:{}", partition.0);
        let coordinator = {
            let mut coordinators = self.partition_flush_coordinators.lock().await;
            coordinators
                .entry(key)
                .or_insert_with(|| {
                    Arc::new(FlushCoordinator {
                        state: tokio::sync::Mutex::new(FlushState {
                            running: false,
                            waiters: Vec::new(),
                        }),
                    })
                })
                .clone()
        };
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let start_worker = {
            let mut state = coordinator.state.lock().await;
            state.waiters.push(sender);
            if state.running {
                false
            } else {
                state.running = true;
                true
            }
        };
        if start_worker {
            let coordinator = coordinator.clone();
            tokio::spawn(async move {
                tokio::time::sleep(interval).await;
                let result = tokio::task::spawn_blocking(move || {
                    partition_logs.flush_partition(&stream, partition)
                })
                .await
                .map_err(|err| BrokerError::with_source("partition flush worker failed", err))
                .and_then(|result| result);
                let mut state = coordinator.state.lock().await;
                state.running = false;
                for waiter in state.waiters.drain(..) {
                    let outcome = match &result {
                        Ok(()) => Ok(()),
                        Err(error) => Err(BrokerError::msg(error.to_string())),
                    };
                    let _ = waiter.send(outcome);
                }
            });
        }
        receiver
            .await
            .map_err(|_| BrokerError::msg("partition group-commit coordinator stopped"))?
    }

    pub(super) async fn checkpoint(
        &self,
        messages: Vec<PublishRecord>,
        consumers: Vec<ReplayedConsumer>,
        dead_letters: Vec<DeadLetterRecord>,
        producer_sequences: Vec<ProducerSequenceRecord>,
        groups: Vec<GroupStateRecord>,
    ) -> Result<()> {
        let runtime = self.clone();
        tokio::task::spawn_blocking(move || {
            runtime.request(|response| WalCommand::Checkpoint {
                messages,
                consumers,
                dead_letters,
                producer_sequences,
                groups,
                response,
            })
        })
        .await
        .map_err(|err| BrokerError::with_source("WAL worker join failed", err))?
    }

    pub(super) fn status(&self, retained_message_count: usize, consumer_count: usize) -> WalStatus {
        self.request(|response| WalCommand::Status {
            retained_message_count,
            consumer_count,
            response,
        })
        .expect("WAL worker stopped while broker is running")
    }

    fn request<T>(&self, command: impl FnOnce(mpsc::Sender<Result<T>>) -> WalCommand) -> Result<T> {
        let (response, result) = mpsc::channel();
        self.sender
            .send(command(response))
            .map_err(|_| BrokerError::msg("WAL worker stopped"))?;
        result
            .recv()
            .map_err(|_| BrokerError::msg("WAL worker dropped response"))?
    }

    async fn request_async<T, F>(&self, command: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(mpsc::Sender<Result<T>>) -> WalCommand + Send + 'static,
    {
        let runtime = self.clone();
        tokio::task::spawn_blocking(move || runtime.request(command))
            .await
            .map_err(|err| BrokerError::with_source("WAL request worker failed", err))?
    }
}

fn wal_worker(mut wal: Wal, receiver: mpsc::Receiver<WalCommand>) {
    let mut pending = None;
    loop {
        let command = match pending.take() {
            Some(command) => command,
            None => match receiver.recv() {
                Ok(command) => command,
                Err(_) => break,
            },
        };
        match command {
            WalCommand::PartitionAppend(record, response) => {
                let mut records = vec![record];
                let mut responses = vec![response];
                let mut estimated_bytes = records
                    .iter()
                    .map(partition_append_estimated_bytes)
                    .sum::<usize>();
                while records.len() < MAX_PARTITION_APPEND_BATCH_RECORDS {
                    match receiver.try_recv() {
                        Ok(WalCommand::PartitionAppend(record, response)) => {
                            let record_bytes = partition_append_estimated_bytes(&record);
                            if estimated_bytes.saturating_add(record_bytes)
                                > MAX_PARTITION_APPEND_BATCH_BYTES
                            {
                                pending = Some(WalCommand::PartitionAppend(record, response));
                                break;
                            }
                            estimated_bytes = estimated_bytes.saturating_add(record_bytes);
                            records.push(record);
                            responses.push(response);
                        }
                        Ok(command) => {
                            pending = Some(command);
                            break;
                        }
                        Err(mpsc::TryRecvError::Empty | mpsc::TryRecvError::Disconnected) => break,
                    }
                }
                let mut outcomes = Vec::with_capacity(records.len());
                let mut failed: Option<String> = None;
                for record in &records {
                    let outcome = match (&failed, wal.append_partition_append(record)) {
                        (Some(error), _) => Err(BrokerError::msg(error.clone())),
                        (None, Ok(())) => Ok(()),
                        (None, Err(error)) => {
                            let message = error.to_string();
                            failed = Some(message.clone());
                            Err(BrokerError::msg(message))
                        }
                    };
                    outcomes.push(outcome);
                }
                for (response, outcome) in responses.into_iter().zip(outcomes) {
                    let _ = response.send(outcome);
                }
            }
            WalCommand::PartitionAppendBatch(records, response) => {
                let result = records
                    .iter()
                    .try_for_each(|record| wal.append_partition_append(record));
                let _ = response.send(result);
            }
            WalCommand::ConsumerUpsert(record, response) => {
                let _ = response.send(wal.append_consumer_upsert(&record));
            }
            WalCommand::ConsumerCursor(record, response) => {
                let _ = response.send(wal.append_consumer_cursor(&record));
            }
            WalCommand::ConsumerCursorDelta(record, response) => {
                let _ = response.send(wal.append_consumer_cursor_delta(&record));
            }
            WalCommand::ConsumerDelete(consumer_id, response) => {
                let _ = response.send(wal.append_consumer_delete(&consumer_id));
            }
            WalCommand::DeliveryAttempt {
                seq,
                consumer_id,
                deadline_ms,
                attempt,
                response,
            } => {
                let _ = response.send(wal.append_delivery_attempt(
                    seq,
                    &consumer_id,
                    deadline_ms,
                    attempt,
                ));
            }
            WalCommand::DeliveryLease(record, response) => {
                let _ = response.send(wal.append_delivery_lease(&record));
            }
            WalCommand::DeliveryBatch { entries, response } => {
                let result = entries
                    .into_iter()
                    .map(|entry| {
                        let lease = wal.append_delivery_attempt(
                            entry.seq,
                            &entry.consumer_id,
                            entry.deadline_ms,
                            entry.attempt,
                        )?;
                        wal.append_consumer_cursor(&entry.cursors)?;
                        Ok(lease)
                    })
                    .collect();
                let _ = response.send(result);
            }
            WalCommand::Ack {
                seq,
                consumer_id,
                delivery_id,
                response,
            } => {
                let result = wal.append_ack(seq, &consumer_id, delivery_id).map(|_| ());
                let _ = response.send(result);
            }
            WalCommand::DeadLetter(record, response) => {
                let _ = response.send(wal.append_dead_letter(&record));
            }
            WalCommand::DeadLetterPurge(id, response) => {
                let _ = response.send(wal.purge_dead_letter(id));
            }
            WalCommand::ProducerSequence(record, response) => {
                let _ = response.send(wal.append_producer_sequence(&record));
            }
            WalCommand::GroupState(group, record, response) => {
                let _ = response.send(wal.append_group_state(&group, &record));
            }
            WalCommand::FlushDue(response) => {
                let _ = response.send(wal.flush_due());
            }
            WalCommand::Flush(response) => {
                let _ = response.send(wal.flush());
            }
            WalCommand::Checkpoint {
                messages,
                consumers,
                dead_letters,
                producer_sequences,
                groups,
                response,
            } => {
                let _ = response.send(wal.checkpoint(
                    messages,
                    consumers,
                    dead_letters,
                    producer_sequences,
                    groups,
                ));
            }
            WalCommand::Status {
                retained_message_count,
                consumer_count,
                response,
            } => {
                let _ = response.send(Ok(wal.status(retained_message_count, consumer_count)));
            }
        }
    }
}

fn partition_append_estimated_bytes(record: &PartitionAppendRecord) -> usize {
    record.stream.len() + record.subject.len() + std::mem::size_of::<u64>() * 2
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn concurrent_flushes_join_one_group_commit_epoch() {
        let directory = tempfile::tempdir().unwrap();
        let (wal, _) = Wal::open(directory.path(), Duration::from_millis(1), 96).unwrap();
        let runtime = WalRuntime::new(wal);
        let first = runtime.clone();
        let second = runtime.clone();
        let (left, right) = tokio::join!(
            first.flush_grouped(Duration::from_millis(5)),
            second.flush_grouped(Duration::from_millis(5)),
        );
        left.unwrap();
        right.unwrap();
        let state = runtime.flush_coordinator.state.lock().await;
        assert!(!state.running);
        assert!(state.waiters.is_empty());
    }
}
