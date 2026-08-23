//! Bounded materialized key/value views over retained compacted-stream history.

use crate::error::{BrokerError, Result};
use crate::stream::PartitionId;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, VecDeque};
use std::fs;
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ViewPosition {
    pub stream: String,
    pub partition: PartitionId,
    pub offset: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ViewUpdate {
    pub key: String,
    pub value: Option<Vec<u8>>,
    pub position: ViewPosition,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ViewEvent {
    pub sequence: u64,
    pub update: ViewUpdate,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ViewSnapshot {
    pub tenant: String,
    pub name: String,
    pub entries: BTreeMap<String, Vec<u8>>,
    pub positions: BTreeMap<String, u64>,
    pub last_sequence: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct ViewLimits {
    pub max_entries: usize,
    pub max_value_bytes: usize,
    pub watch_capacity: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DurableView {
    snapshot: ViewSnapshot,
    watch: VecDeque<ViewEvent>,
}

#[derive(Debug)]
pub struct MaterializedView {
    path: Option<PathBuf>,
    tenant: String,
    name: String,
    limits: ViewLimits,
    entries: BTreeMap<String, Vec<u8>>,
    positions: BTreeMap<String, u64>,
    last_sequence: u64,
    watch: VecDeque<ViewEvent>,
}

impl MaterializedView {
    pub fn new(
        tenant: impl Into<String>,
        name: impl Into<String>,
        limits: ViewLimits,
    ) -> Result<Self> {
        let tenant = tenant.into();
        let name = name.into();
        validate_identity(&tenant, "tenant")?;
        validate_identity(&name, "view")?;
        validate_limits(limits)?;
        Ok(Self {
            path: None,
            tenant,
            name,
            limits,
            entries: BTreeMap::new(),
            positions: BTreeMap::new(),
            last_sequence: 0,
            watch: VecDeque::new(),
        })
    }

    pub fn open(
        path: impl Into<PathBuf>,
        tenant: impl Into<String>,
        name: impl Into<String>,
        limits: ViewLimits,
    ) -> Result<Self> {
        let path = path.into();
        let mut view = Self::new(tenant, name, limits)?;
        view.path = Some(path.clone());
        if path.exists() {
            let durable: DurableView = serde_json::from_slice(&fs::read(&path)?)
                .map_err(|error| BrokerError::with_source("decoding materialized view", error))?;
            view.restore_snapshot(durable.snapshot)?;
            view.watch = durable.watch;
            view.watch.truncate(view.limits.watch_capacity);
        }
        Ok(view)
    }

    pub fn tenant(&self) -> &str {
        &self.tenant
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn point_read(&self, key: &str) -> Option<&[u8]> {
        self.entries.get(key).map(Vec::as_slice)
    }
    pub fn entry_count(&self) -> usize {
        self.entries.len()
    }

    pub fn consistency_positions(&self) -> BTreeMap<String, u64> {
        self.positions.clone()
    }

    pub fn apply(&mut self, update: ViewUpdate) -> Result<bool> {
        validate_update(&update, self.limits.max_value_bytes)?;
        let position_key = position_key(&update.position);
        if let Some(current) = self.positions.get(&position_key) {
            if update.position.offset < *current {
                return Ok(false);
            }
            if update.position.offset == *current {
                return Ok(self
                    .entries
                    .get(&update.key)
                    .map(|value| update.value.as_deref() == Some(value.as_slice()))
                    .unwrap_or(update.value.is_none()));
            }
        }
        if update.value.is_some()
            && !self.entries.contains_key(&update.key)
            && self.entries.len() >= self.limits.max_entries
        {
            return Err(BrokerError::msg("materialized view entry quota exceeded"));
        }
        match &update.value {
            Some(value) => {
                self.entries.insert(update.key.clone(), value.clone());
            }
            None => {
                self.entries.remove(&update.key);
            }
        }
        self.positions.insert(position_key, update.position.offset);
        self.last_sequence = self.last_sequence.saturating_add(1);
        self.watch.push_back(ViewEvent {
            sequence: self.last_sequence,
            update,
        });
        while self.watch.len() > self.limits.watch_capacity {
            self.watch.pop_front();
        }
        self.persist()?;
        Ok(true)
    }

    pub fn rebuild(&mut self, history: &[ViewUpdate]) -> Result<()> {
        let mut ordered = history.to_vec();
        ordered.sort_by_key(|update| {
            (
                update.position.stream.clone(),
                update.position.partition.0,
                update.position.offset,
            )
        });
        self.entries.clear();
        self.positions.clear();
        self.last_sequence = 0;
        self.watch.clear();
        for update in ordered {
            self.apply(update)?;
        }
        Ok(())
    }

    pub fn snapshot(&self) -> ViewSnapshot {
        ViewSnapshot {
            tenant: self.tenant.clone(),
            name: self.name.clone(),
            entries: self.entries.clone(),
            positions: self.positions.clone(),
            last_sequence: self.last_sequence,
        }
    }

    pub fn restore_snapshot(&mut self, snapshot: ViewSnapshot) -> Result<()> {
        crate::broker_ensure!(
            snapshot.tenant == self.tenant,
            "view snapshot tenant mismatch"
        );
        crate::broker_ensure!(snapshot.name == self.name, "view snapshot name mismatch");
        crate::broker_ensure!(
            snapshot.entries.len() <= self.limits.max_entries,
            "view snapshot exceeds entry quota"
        );
        crate::broker_ensure!(
            snapshot
                .entries
                .values()
                .all(|value| value.len() <= self.limits.max_value_bytes),
            "view snapshot exceeds value quota"
        );
        self.entries = snapshot.entries;
        self.positions = snapshot.positions;
        self.last_sequence = snapshot.last_sequence;
        self.persist()
    }

    pub fn watch_from(&self, sequence: u64) -> Result<Vec<ViewEvent>> {
        if let Some(first) = self.watch.front().map(|event| event.sequence) {
            crate::broker_ensure!(
                sequence.saturating_add(1) >= first,
                "view watch cursor expired"
            );
        }
        Ok(self
            .watch
            .iter()
            .filter(|event| event.sequence > sequence)
            .cloned()
            .collect())
    }

    fn persist(&self) -> Result<()> {
        let Some(path) = &self.path else {
            return Ok(());
        };
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let durable = DurableView {
            snapshot: self.snapshot(),
            watch: self.watch.clone(),
        };
        let body = serde_json::to_vec(&durable)
            .map_err(|error| BrokerError::with_source("encoding materialized view", error))?;
        let temporary = path.with_extension("tmp");
        fs::write(&temporary, body)?;
        fs::rename(temporary, path)?;
        Ok(())
    }
}

fn validate_identity(value: &str, field: &str) -> Result<()> {
    crate::broker_ensure!(
        !value.is_empty()
            && value.len() <= 128
            && value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.')),
        "invalid view {field}"
    );
    Ok(())
}

fn validate_limits(limits: ViewLimits) -> Result<()> {
    crate::broker_ensure!(
        limits.max_entries > 0 && limits.max_value_bytes > 0 && limits.watch_capacity > 0,
        "materialized view limits must be positive"
    );
    Ok(())
}

fn validate_update(update: &ViewUpdate, max_value_bytes: usize) -> Result<()> {
    crate::broker_ensure!(
        !update.key.is_empty() && update.key.len() <= 512,
        "invalid materialized view key"
    );
    crate::broker_ensure!(
        update
            .value
            .as_ref()
            .is_none_or(|value| value.len() <= max_value_bytes),
        "materialized view value exceeds quota"
    );
    Ok(())
}

fn position_key(position: &ViewPosition) -> String {
    format!("{}:{:010}", position.stream, position.partition.0)
}

#[cfg(test)]
mod tests;
