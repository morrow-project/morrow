//! Bounded transaction coordination for atomic publish/offset/view batches.
//!
//! The coordinator stores intent and terminal state, while visibility is
//! derived only from `Committed` transactions. A restart therefore cannot
//! expose a partial prepared batch.

use crate::error::{BrokerError, Result};
use crate::stream::PartitionId;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
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

#[derive(Debug)]
pub struct TransactionCoordinator {
    path: Option<PathBuf>,
    limits: TransactionLimits,
    state: DurableState,
}

impl TransactionCoordinator {
    pub fn new(limits: TransactionLimits) -> Result<Self> {
        validate_limits(limits)?;
        Ok(Self {
            path: None,
            limits,
            state: DurableState::default(),
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
        Ok(Self {
            path: Some(path),
            limits,
            state,
        })
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
            }
            self.state
                .producer_epochs
                .insert(producer.clone(), producer_epoch);
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
        self.state.transactions.insert(
            id.clone(),
            TransactionRecord {
                id,
                tenant,
                producer,
                producer_epoch,
                started_at_ms: now_ms,
                deadline_ms: now_ms.saturating_add(self.limits.max_duration_ms),
                status: TransactionStatus::Open,
                writes: Vec::new(),
                offsets: Vec::new(),
                views: Vec::new(),
            },
        );
        self.persist()
    }

    pub fn append(&mut self, id: &str, write: TransactionWrite, now_ms: u64) -> Result<()> {
        self.with_open(id, now_ms, |record, limits| {
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
        })
    }

    pub fn commit_offset(&mut self, id: &str, offset: OffsetCommit, now_ms: u64) -> Result<()> {
        self.with_open(id, now_ms, |record, limits| {
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
        })
    }

    pub fn mutate_view(&mut self, id: &str, mutation: ViewMutation, now_ms: u64) -> Result<()> {
        self.with_open(id, now_ms, |record, limits| {
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
        })
    }

    pub fn prepare(&mut self, id: &str, now_ms: u64) -> Result<()> {
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
        self.persist()
    }

    pub fn commit(&mut self, id: &str, now_ms: u64) -> Result<CommittedBatch> {
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
        let batch = committed_batch(record);
        self.persist()?;
        Ok(batch)
    }

    pub fn abort(&mut self, id: &str, reason: impl Into<String>) -> Result<()> {
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
        self.persist()
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
        self.persist()?;
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
        self.persist()
    }

    fn persist(&self) -> Result<()> {
        let Some(path) = &self.path else {
            return Ok(());
        };
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let body = serde_json::to_vec(&self.state)
            .map_err(|error| BrokerError::with_source("encoding transaction state", error))?;
        let temporary = path.with_extension("tmp");
        fs::write(&temporary, body)?;
        fs::rename(temporary, path)?;
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
