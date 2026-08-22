use super::*;
use std::sync::mpsc::{self, SyncSender};

const WAL_QUEUE_CAPACITY: usize = 128;

#[derive(Clone)]
pub(super) struct WalRuntime {
    sender: SyncSender<WalCommand>,
    next_publish_seq: Arc<AtomicU64>,
}

enum WalCommand {
    PartitionAppend(PartitionAppendRecord, mpsc::Sender<Result<()>>),
    ConsumerUpsert(ConsumerRecord, mpsc::Sender<Result<()>>),
    ConsumerCursor(ConsumerCursorRecord, mpsc::Sender<Result<()>>),
    ConsumerDelete(String, mpsc::Sender<Result<()>>),
    DeliveryAttempt {
        seq: u64,
        consumer_id: String,
        deadline_ms: u64,
        attempt: u32,
        response: mpsc::Sender<Result<DeliveryAttemptRecord>>,
    },
    DeliveryLease(DeliveryAttemptRecord, mpsc::Sender<Result<()>>),
    Ack {
        seq: u64,
        consumer_id: String,
        delivery_id: u64,
        response: mpsc::Sender<Result<()>>,
    },
    FlushDue(mpsc::Sender<Result<()>>),
    Flush(mpsc::Sender<Result<()>>),
    Checkpoint {
        messages: Vec<PublishRecord>,
        consumers: Vec<ReplayedConsumer>,
        response: mpsc::Sender<Result<()>>,
    },
    Status {
        retained_message_count: usize,
        consumer_count: usize,
        response: mpsc::Sender<Result<WalStatus>>,
    },
}

impl WalRuntime {
    pub(super) fn new(wal: Wal) -> Self {
        let next_publish_seq = wal.next_publish_seq();
        let (sender, receiver) = mpsc::sync_channel(WAL_QUEUE_CAPACITY);
        std::thread::Builder::new()
            .name("broker-wal".to_string())
            .spawn(move || wal_worker(wal, receiver))
            .expect("spawning WAL worker");
        Self {
            sender,
            next_publish_seq: Arc::new(AtomicU64::new(next_publish_seq)),
        }
    }

    pub(super) fn reserve_publish_seq(&self) -> u64 {
        self.next_publish_seq.fetch_add(1, Ordering::Relaxed)
    }

    pub(super) fn append_partition_append(&self, record: &PartitionAppendRecord) -> Result<()> {
        self.request(|response| WalCommand::PartitionAppend(record.clone(), response))
    }

    pub(super) fn append_consumer_upsert(&self, record: &ConsumerRecord) -> Result<()> {
        self.request(|response| WalCommand::ConsumerUpsert(record.clone(), response))
    }

    pub(super) fn append_consumer_cursor(&self, record: &ConsumerCursorRecord) -> Result<()> {
        self.request(|response| WalCommand::ConsumerCursor(record.clone(), response))
    }

    pub(super) fn append_consumer_delete(&self, consumer_id: &str) -> Result<()> {
        self.request(|response| WalCommand::ConsumerDelete(consumer_id.to_string(), response))
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

    pub(super) fn append_delivery_lease(&self, record: &DeliveryAttemptRecord) -> Result<()> {
        self.request(|response| WalCommand::DeliveryLease(record.clone(), response))
    }

    pub(super) fn append_ack(&self, seq: u64, consumer_id: &str, delivery_id: u64) -> Result<()> {
        self.request(|response| WalCommand::Ack {
            seq,
            consumer_id: consumer_id.to_string(),
            delivery_id,
            response,
        })
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

    pub(super) async fn checkpoint(
        &self,
        messages: Vec<PublishRecord>,
        consumers: Vec<ReplayedConsumer>,
    ) -> Result<()> {
        let runtime = self.clone();
        tokio::task::spawn_blocking(move || {
            runtime.request(|response| WalCommand::Checkpoint {
                messages,
                consumers,
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
}

fn wal_worker(mut wal: Wal, receiver: mpsc::Receiver<WalCommand>) {
    while let Ok(command) = receiver.recv() {
        match command {
            WalCommand::PartitionAppend(record, response) => {
                let _ = response.send(wal.append_partition_append(&record));
            }
            WalCommand::ConsumerUpsert(record, response) => {
                let _ = response.send(wal.append_consumer_upsert(&record));
            }
            WalCommand::ConsumerCursor(record, response) => {
                let _ = response.send(wal.append_consumer_cursor(&record));
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
            WalCommand::Ack {
                seq,
                consumer_id,
                delivery_id,
                response,
            } => {
                let result = wal.append_ack(seq, &consumer_id, delivery_id).map(|_| ());
                let _ = response.send(result);
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
                response,
            } => {
                let _ = response.send(wal.checkpoint(messages, consumers));
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
