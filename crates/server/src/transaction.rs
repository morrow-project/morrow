//! Bounded transaction coordination for atomic publish/offset/view batches.
//!
//! The coordinator stores intent and terminal state, while visibility is
//! derived only from `Committed` transactions. A restart therefore cannot
//! expose a partial prepared batch.

use crate::error::{BrokerError, Result};
use crate::stream::PartitionId;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct TransactionLimits {
    pub max_messages: usize,
    pub max_bytes: usize,
    pub max_partitions: usize,
    pub max_duration_ms: u64,
    pub max_concurrent: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TransactionStatus {
    Open,
    Prepared,
    Committed,
    Aborted { reason: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TransactionWrite {
    pub stream: String,
    pub partition: PartitionId,
    pub offset: u64,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OffsetCommit {
    pub consumer: String,
    pub stream: String,
    pub partition: PartitionId,
    pub offset: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ViewMutation {
    pub view: String,
    pub key: String,
    pub value: Option<Vec<u8>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TransactionRecord {
    pub id: String,
    pub tenant: String,
    pub producer: String,
    pub producer_epoch: u64,
    pub started_at_ms: u64,
    pub deadline_ms: u64,
    pub status: TransactionStatus,
    pub writes: Vec<TransactionWrite>,
    pub offsets: Vec<OffsetCommit>,
    pub views: Vec<ViewMutation>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CommittedBatch {
    pub id: String,
    pub writes: Vec<TransactionWrite>,
    pub offsets: Vec<OffsetCommit>,
    pub views: Vec<ViewMutation>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct DurableState {
    transactions: BTreeMap<String, TransactionRecord>,
    producer_epochs: BTreeMap<String, u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum TransactionDelta {
    Begin(TransactionRecord),
    ProducerEpoch {
        producer: String,
        epoch: u64,
    },
    Write {
        id: String,
        write: TransactionWrite,
    },
    Offset {
        id: String,
        offset: OffsetCommit,
    },
    View {
        id: String,
        mutation: ViewMutation,
    },
    Status {
        id: String,
        status: TransactionStatus,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct JournalEntry {
    delta: TransactionDelta,
    checksum: u32,
}

#[derive(Debug)]
pub struct TransactionCoordinator {
    path: Option<PathBuf>,
    journal_path: Option<PathBuf>,
    limits: TransactionLimits,
    state: DurableState,
    journal_entries: usize,
}

impl TransactionCoordinator {
    pub fn new(limits: TransactionLimits) -> Result<Self> {
        validate_limits(limits)?;
        Ok(Self {
            path: None,
            journal_path: None,
            limits,
            state: DurableState::default(),
            journal_entries: 0,
        })
    }

    pub fn open(path: impl Into<PathBuf>, limits: TransactionLimits) -> Result<Self> {
        validate_limits(limits)?;
        let path = path.into();
        let state = if path.exists() {
            serde_json::from_slice(&fs::read(&path)?)
                .map_err(|error| BrokerError::with_source("decoding transaction state", error))?
        } else {
            DurableState::default()
        };
        let journal_path = path.with_extension("log");
        let mut coordinator = Self {
            path: Some(path),
            journal_path: Some(journal_path.clone()),
            limits,
            state,
            journal_entries: 0,
        };
        if journal_path.exists() {
            let file = File::open(&journal_path)?;
            for line in BufReader::new(file).lines() {
                let line = line?;
                if line.trim().is_empty() {
                    continue;
                }
                let entry: JournalEntry = serde_json::from_str(&line).map_err(|error| {
                    BrokerError::with_source("decoding transaction journal", error)
                })?;
                let delta_body = serde_json::to_vec(&entry.delta).map_err(|error| {
                    BrokerError::with_source("encoding transaction journal", error)
                })?;
                crate::broker_ensure!(
                    crc32fast::hash(&delta_body) == entry.checksum,
                    "transaction journal checksum mismatch"
                );
                coordinator.apply_delta(entry.delta)?;
                coordinator.journal_entries += 1;
            }
        }
        Ok(coordinator)
    }

    pub fn begin(
        &mut self,
        id: impl Into<String>,
        tenant: impl Into<String>,
        producer: impl Into<String>,
        producer_epoch: u64,
        now_ms: u64,
    ) -> Result<()> {
        let id = id.into();
        let tenant = tenant.into();
        let producer = producer.into();
        crate::broker_ensure!(
            !id.is_empty() && !tenant.is_empty() && !producer.is_empty(),
            "transaction identity is empty"
        );
        if let Some(existing) = self.state.transactions.get(&id) {
            crate::broker_ensure!(
                existing.producer == producer && existing.producer_epoch == producer_epoch,
                "transaction ID was reused with a different producer epoch"
            );
            return Ok(());
        }
        let current_epoch = self
            .state
            .producer_epochs
            .get(&producer)
            .copied()
            .unwrap_or(0);
        crate::broker_ensure!(producer_epoch >= current_epoch, "producer epoch is fenced");
        if producer_epoch > current_epoch {
            let mut deltas = Vec::new();
            for record in self.state.transactions.values_mut().filter(|record| {
                record.producer == producer
                    && matches!(
                        record.status,
                        TransactionStatus::Open | TransactionStatus::Prepared
                    )
            }) {
                record.status = TransactionStatus::Aborted {
                    reason: "producer epoch fenced".into(),
                };
                deltas.push(TransactionDelta::Status {
                    id: record.id.clone(),
                    status: record.status.clone(),
                });
            }
            deltas.push(TransactionDelta::ProducerEpoch {
                producer: producer.clone(),
                epoch: producer_epoch,
            });
            self.state
                .producer_epochs
                .insert(producer.clone(), producer_epoch);
            self.append_deltas(&deltas)?;
        }
        let active = self
            .state
            .transactions
            .values()
            .filter(|record| {
                matches!(
                    record.status,
                    TransactionStatus::Open | TransactionStatus::Prepared
                )
            })
            .count();
        crate::broker_ensure!(
            active < self.limits.max_concurrent,
            "concurrent transaction quota exceeded"
        );
        let record = TransactionRecord {
            id: id.clone(),
            tenant,
            producer,
            producer_epoch,
            started_at_ms: now_ms,
            deadline_ms: now_ms.saturating_add(self.limits.max_duration_ms),
            status: TransactionStatus::Open,
            writes: Vec::new(),
            offsets: Vec::new(),
            views: Vec::new(),
        };
        self.state.transactions.insert(id.clone(), record.clone());
        self.append_deltas(&[TransactionDelta::Begin(record)])
    }

    pub fn append(&mut self, id: &str, write: TransactionWrite, now_ms: u64) -> Result<()> {
        self.with_open(
            id,
            now_ms,
            TransactionDelta::Write {
                id: id.to_string(),
                write: write.clone(),
            },
            |record, limits| {
                let messages = record.writes.len() + record.offsets.len() + record.views.len();
                crate::broker_ensure!(
                    messages < limits.max_messages,
                    "transaction message quota exceeded"
                );
                crate::broker_ensure!(
                    total_bytes(record).saturating_add(write.bytes.len()) <= limits.max_bytes,
                    "transaction byte quota exceeded"
                );
                crate::broker_ensure!(
                    partition_count(record, Some((&write.stream, write.partition)))
                        <= limits.max_partitions,
                    "transaction partition quota exceeded"
                );
                record.writes.push(write);
                Ok(())
            },
        )
    }

    pub fn commit_offset(&mut self, id: &str, offset: OffsetCommit, now_ms: u64) -> Result<()> {
        self.with_open(
            id,
            now_ms,
            TransactionDelta::Offset {
                id: id.to_string(),
                offset: offset.clone(),
            },
            |record, limits| {
                let messages = record.writes.len() + record.offsets.len() + record.views.len();
                crate::broker_ensure!(
                    messages < limits.max_messages,
                    "transaction message quota exceeded"
                );
                crate::broker_ensure!(
                    partition_count(record, Some((&offset.stream, offset.partition)))
                        <= limits.max_partitions,
                    "transaction partition quota exceeded"
                );
                record.offsets.push(offset);
                Ok(())
            },
        )
    }

    pub fn mutate_view(&mut self, id: &str, mutation: ViewMutation, now_ms: u64) -> Result<()> {
        self.with_open(
            id,
            now_ms,
            TransactionDelta::View {
                id: id.to_string(),
                mutation: mutation.clone(),
            },
            |record, limits| {
                let messages = record.writes.len() + record.offsets.len() + record.views.len();
                crate::broker_ensure!(
                    messages < limits.max_messages,
                    "transaction message quota exceeded"
                );
                let value_bytes = mutation.value.as_ref().map_or(0, Vec::len);
                crate::broker_ensure!(
                    total_bytes(record).saturating_add(value_bytes) <= limits.max_bytes,
                    "transaction byte quota exceeded"
                );
                record.views.push(mutation);
                Ok(())
            },
        )
    }

    pub fn prepare(&mut self, id: &str, now_ms: u64) -> Result<()> {
        let status = {
            let record = self
                .state
                .transactions
                .get_mut(id)
                .ok_or_else(|| BrokerError::msg("unknown transaction"))?;
            ensure_live(record, now_ms)?;
            crate::broker_ensure!(
                matches!(record.status, TransactionStatus::Open),
                "transaction is not open"
            );
            record.status = TransactionStatus::Prepared;
            record.status.clone()
        };
        self.append_deltas(&[TransactionDelta::Status {
            id: id.to_string(),
            status,
        }])
    }

    pub fn commit(&mut self, id: &str, now_ms: u64) -> Result<CommittedBatch> {
        let (status, batch) = {
            let record = self
                .state
                .transactions
                .get_mut(id)
                .ok_or_else(|| BrokerError::msg("unknown transaction"))?;
            ensure_live(record, now_ms)?;
            crate::broker_ensure!(
                matches!(record.status, TransactionStatus::Prepared),
                "transaction is not prepared"
            );
            record.status = TransactionStatus::Committed;
            (record.status.clone(), committed_batch(record))
        };
        self.append_deltas(&[TransactionDelta::Status {
            id: id.to_string(),
            status,
        }])?;
        Ok(batch)
    }

    pub fn abort(&mut self, id: &str, reason: impl Into<String>) -> Result<()> {
        let status = {
            let record = self
                .state
                .transactions
                .get_mut(id)
                .ok_or_else(|| BrokerError::msg("unknown transaction"))?;
            crate::broker_ensure!(
                matches!(
                    record.status,
                    TransactionStatus::Open | TransactionStatus::Prepared
                ),
                "transaction is already terminal"
            );
            record.status = TransactionStatus::Aborted {
                reason: reason.into(),
            };
            record.status.clone()
        };
        self.append_deltas(&[TransactionDelta::Status {
            id: id.to_string(),
            status,
        }])
    }

    pub fn recover(&mut self, now_ms: u64) -> Result<usize> {
        let mut recovered = 0;
        for record in self.state.transactions.values_mut().filter(|record| {
            matches!(
                record.status,
                TransactionStatus::Open | TransactionStatus::Prepared
            ) && record.deadline_ms <= now_ms
        }) {
            record.status = TransactionStatus::Aborted {
                reason: "transaction timeout during recovery".into(),
            };
            recovered += 1;
        }
        let deltas = self
            .state
            .transactions
            .values()
            .filter(|record| matches!(record.status, TransactionStatus::Aborted { .. }))
            .map(|record| TransactionDelta::Status {
                id: record.id.clone(),
                status: record.status.clone(),
            })
            .collect::<Vec<_>>();
        self.append_deltas(&deltas)?;
        Ok(recovered)
    }

    pub fn status(&self, id: &str) -> Option<&TransactionStatus> {
        self.state.transactions.get(id).map(|record| &record.status)
    }

    pub fn visible_batch(&self, id: &str) -> Option<CommittedBatch> {
        let record = self.state.transactions.get(id)?;
        matches!(record.status, TransactionStatus::Committed).then(|| committed_batch(record))
    }

    fn with_open(
        &mut self,
        id: &str,
        now_ms: u64,
        delta: TransactionDelta,
        operation: impl FnOnce(&mut TransactionRecord, TransactionLimits) -> Result<()>,
    ) -> Result<()> {
        let record = self
            .state
            .transactions
            .get_mut(id)
            .ok_or_else(|| BrokerError::msg("unknown transaction"))?;
        ensure_live(record, now_ms)?;
        crate::broker_ensure!(
            matches!(record.status, TransactionStatus::Open),
            "transaction is not open"
        );
        operation(record, self.limits)?;
        self.append_deltas(&[delta])
    }

    fn append_deltas(&mut self, deltas: &[TransactionDelta]) -> Result<()> {
        let Some(path) = &self.journal_path else {
            return Ok(());
        };
        if deltas.is_empty() {
            return Ok(());
        }
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut file = OpenOptions::new().create(true).append(true).open(path)?;
        for delta in deltas {
            let delta_body = serde_json::to_vec(delta)
                .map_err(|error| BrokerError::with_source("encoding transaction journal", error))?;
            let body = serde_json::to_vec(&JournalEntry {
                delta: delta.clone(),
                checksum: crc32fast::hash(&delta_body),
            })
            .map_err(|error| BrokerError::with_source("encoding transaction journal", error))?;
            file.write_all(&body)?;
            file.write_all(b"\n")?;
            self.journal_entries += 1;
        }
        file.sync_data()?;
        if self.journal_entries >= 64 {
            self.checkpoint()?;
        }
        Ok(())
    }

    fn apply_delta(&mut self, delta: TransactionDelta) -> Result<()> {
        match delta {
            TransactionDelta::Begin(record) => {
                self.state.transactions.insert(record.id.clone(), record);
            }
            TransactionDelta::ProducerEpoch { producer, epoch } => {
                self.state.producer_epochs.insert(producer, epoch);
            }
            TransactionDelta::Write { id, write } => self
                .state
                .transactions
                .get_mut(&id)
                .ok_or_else(|| {
                    BrokerError::msg("transaction journal references unknown transaction")
                })?
                .writes
                .push(write),
            TransactionDelta::Offset { id, offset } => self
                .state
                .transactions
                .get_mut(&id)
                .ok_or_else(|| {
                    BrokerError::msg("transaction journal references unknown transaction")
                })?
                .offsets
                .push(offset),
            TransactionDelta::View { id, mutation } => self
                .state
                .transactions
                .get_mut(&id)
                .ok_or_else(|| {
                    BrokerError::msg("transaction journal references unknown transaction")
                })?
                .views
                .push(mutation),
            TransactionDelta::Status { id, status } => {
                self.state
                    .transactions
                    .get_mut(&id)
                    .ok_or_else(|| {
                        BrokerError::msg("transaction journal references unknown transaction")
                    })?
                    .status = status;
            }
        }
        Ok(())
    }

    fn checkpoint(&mut self) -> Result<()> {
        let Some(path) = &self.path else {
            return Ok(());
        };
        let terminal = self
            .state
            .transactions
            .iter()
            .filter(|(_, record)| {
                matches!(
                    record.status,
                    TransactionStatus::Committed | TransactionStatus::Aborted { .. }
                )
            })
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>();
        let keep_terminal = self.limits.max_concurrent.saturating_mul(16).max(64);
        if terminal.len() > keep_terminal {
            let remove_count = terminal.len() - keep_terminal;
            for id in terminal.into_iter().take(remove_count) {
                self.state.transactions.remove(&id);
            }
        }
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let body = serde_json::to_vec(&self.state)
            .map_err(|error| BrokerError::with_source("encoding transaction state", error))?;
        let temporary = path.with_extension("tmp");
        fs::write(&temporary, body)?;
        fs::rename(temporary, path)?;
        if let Some(journal_path) = &self.journal_path {
            let file = File::create(journal_path)?;
            file.sync_data()?;
        }
        self.journal_entries = 0;
        Ok(())
    }
}

fn validate_limits(limits: TransactionLimits) -> Result<()> {
    crate::broker_ensure!(
        limits.max_messages > 0
            && limits.max_bytes > 0
            && limits.max_partitions > 0
            && limits.max_duration_ms > 0
            && limits.max_concurrent > 0,
        "transaction limits must be positive"
    );
    Ok(())
}

fn ensure_live(record: &TransactionRecord, now_ms: u64) -> Result<()> {
    crate::broker_ensure!(now_ms <= record.deadline_ms, "transaction timed out");
    Ok(())
}

fn total_bytes(record: &TransactionRecord) -> usize {
    record
        .writes
        .iter()
        .map(|write| write.bytes.len())
        .sum::<usize>()
        + record
            .views
            .iter()
            .map(|view| view.value.as_ref().map_or(0, Vec::len))
            .sum::<usize>()
}

fn partition_count(record: &TransactionRecord, write: Option<(&str, PartitionId)>) -> usize {
    let mut partitions = BTreeSet::new();
    for item in &record.writes {
        partitions.insert((item.stream.clone(), item.partition.0));
    }
    for item in &record.offsets {
        partitions.insert((item.stream.clone(), item.partition.0));
    }
    if let Some((stream, partition)) = write {
        partitions.insert((stream.to_string(), partition.0));
    }
    partitions.len()
}

fn committed_batch(record: &TransactionRecord) -> CommittedBatch {
    CommittedBatch {
        id: record.id.clone(),
        writes: record.writes.clone(),
        offsets: record.offsets.clone(),
        views: record.views.clone(),
    }
}

#[cfg(test)]
mod tests;
