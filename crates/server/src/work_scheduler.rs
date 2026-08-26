//! Hierarchical, bounded budgets for foreground and background broker work.

use std::{collections::BTreeMap, sync::Arc};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum WorkClass {
    Control,
    Foreground,
    Observer,
    CatchUp,
    Snapshot,
    Reassignment,
    Retention,
    Compaction,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkBudget {
    pub max_records: u64,
    pub max_bytes: u64,
    pub max_concurrency: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct WorkUsage {
    pub records: u64,
    pub bytes: u64,
    pub concurrency: u32,
    pub rejected: u64,
}

#[derive(Debug, Clone)]
pub struct WorkScheduler {
    budgets: BTreeMap<WorkClass, WorkBudget>,
    usage: BTreeMap<WorkClass, WorkUsage>,
}

/// A scheduler reservation that releases its usage even when the owning future
/// is cancelled. Normal completion may drop it explicitly; `Drop` handles the
/// cancellation and shutdown paths without requiring an async finally block.
pub struct WorkReservation {
    scheduler: Arc<tokio::sync::Mutex<WorkScheduler>>,
    class: WorkClass,
    records: u64,
    bytes: u64,
}

impl WorkReservation {
    pub async fn try_acquire(
        scheduler: Arc<tokio::sync::Mutex<WorkScheduler>>,
        class: WorkClass,
        records: u64,
        bytes: u64,
    ) -> Option<Self> {
        if !scheduler.lock().await.try_reserve(class, records, bytes) {
            return None;
        }
        Some(Self {
            scheduler,
            class,
            records,
            bytes,
        })
    }
}

impl Drop for WorkReservation {
    fn drop(&mut self) {
        let scheduler = self.scheduler.clone();
        let class = self.class;
        let records = self.records;
        let bytes = self.bytes;
        tokio::spawn(async move {
            scheduler.lock().await.release(class, records, bytes);
        });
    }
}

impl WorkScheduler {
    pub fn new(budgets: impl IntoIterator<Item = (WorkClass, WorkBudget)>) -> Self {
        let budgets = budgets.into_iter().collect::<BTreeMap<_, _>>();
        let usage = budgets
            .keys()
            .map(|class| (*class, WorkUsage::default()))
            .collect();
        Self { budgets, usage }
    }

    pub fn try_reserve(&mut self, class: WorkClass, records: u64, bytes: u64) -> bool {
        let Some(budget) = self.budgets.get(&class).copied() else {
            return false;
        };
        let usage = self.usage.entry(class).or_default();
        if usage.records.saturating_add(records) > budget.max_records
            || usage.bytes.saturating_add(bytes) > budget.max_bytes
            || usage.concurrency.saturating_add(1) > budget.max_concurrency
        {
            usage.rejected = usage.rejected.saturating_add(1);
            return false;
        }
        usage.records += records;
        usage.bytes += bytes;
        usage.concurrency += 1;
        true
    }

    pub fn release(&mut self, class: WorkClass, records: u64, bytes: u64) {
        let usage = self.usage.entry(class).or_default();
        usage.records = usage.records.saturating_sub(records);
        usage.bytes = usage.bytes.saturating_sub(bytes);
        usage.concurrency = usage.concurrency.saturating_sub(1);
    }

    pub fn usage(&self, class: WorkClass) -> WorkUsage {
        self.usage.get(&class).copied().unwrap_or_default()
    }
}
