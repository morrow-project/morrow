//! Asynchronous, checkpointed cross-region segment replication.
//!
//! Shipping is intentionally decoupled from local append acknowledgement. A
//! checkpoint is advanced only after the receiver verifies the immutable
//! segment chunk, and a fencing token prevents an old primary from accepting
//! writes after promotion elsewhere.

use crate::backup::sha256;
use crate::error::{BrokerError, Result};
use crate::stream::PartitionId;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum RegionRole {
    Primary,
    Standby,
    Fenced,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ReadLocality {
    PrimaryOnly,
    PreferLocal,
    LocalOnly,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicationTopology {
    pub primary_region: String,
    pub standby_regions: Vec<String>,
    pub read_locality: ReadLocality,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicationPolicy {
    pub max_lag_offsets: u64,
    pub max_bandwidth_bytes_per_second: u64,
    pub max_in_flight_chunks: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SegmentChunk {
    pub stream: String,
    pub partition: PartitionId,
    pub segment_id: u64,
    pub first_offset: u64,
    pub last_offset: u64,
    pub bytes: Vec<u8>,
    pub checksum: String,
}

impl SegmentChunk {
    pub fn new(
        stream: impl Into<String>,
        partition: PartitionId,
        segment_id: u64,
        first_offset: u64,
        last_offset: u64,
        bytes: Vec<u8>,
    ) -> Self {
        let checksum = sha256(&bytes);
        Self {
            stream: stream.into(),
            partition,
            segment_id,
            first_offset,
            last_offset,
            bytes,
            checksum,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct ReplicationCheckpoint {
    pub segment_id: Option<u64>,
    pub last_offset: Option<u64>,
    pub checksum: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct DurableState {
    role: RegionRole,
    fencing_token: u64,
    checkpoints: BTreeMap<String, ReplicationCheckpoint>,
}

#[derive(Debug)]
pub struct CrossRegionReplicator {
    path: Option<PathBuf>,
    policy: ReplicationPolicy,
    state: DurableState,
    bytes_in_window: u64,
    in_flight: u32,
}

impl CrossRegionReplicator {
    pub fn new(policy: ReplicationPolicy, role: RegionRole) -> Self {
        Self {
            path: None,
            policy,
            state: DurableState {
                role,
                fencing_token: 1,
                checkpoints: BTreeMap::new(),
            },
            bytes_in_window: 0,
            in_flight: 0,
        }
    }

    pub fn open(path: impl Into<PathBuf>, policy: ReplicationPolicy) -> Result<Self> {
        let path = path.into();
        let state = if path.exists() {
            serde_json::from_slice(&fs::read(&path)?).map_err(|error| {
                BrokerError::with_source("decoding replication checkpoint", error)
            })?
        } else {
            DurableState {
                role: RegionRole::Standby,
                fencing_token: 1,
                checkpoints: BTreeMap::new(),
            }
        };
        Ok(Self {
            path: Some(path),
            policy,
            state,
            bytes_in_window: 0,
            in_flight: 0,
        })
    }

    pub fn role(&self) -> RegionRole {
        self.state.role
    }

    pub fn fencing_token(&self) -> u64 {
        self.state.fencing_token
    }

    pub fn checkpoint(&self, stream: &str, partition: PartitionId) -> ReplicationCheckpoint {
        self.state
            .checkpoints
            .get(&checkpoint_key(stream, partition))
            .cloned()
            .unwrap_or_default()
    }

    pub fn lag(&self, stream: &str, partition: PartitionId, primary_high_watermark: u64) -> u64 {
        primary_high_watermark.saturating_sub(
            self.checkpoint(stream, partition)
                .last_offset
                .map_or(0, |offset| offset.saturating_add(1)),
        )
    }

    pub fn ship(&mut self, chunk: &SegmentChunk, source_fencing_token: u64) -> Result<bool> {
        crate::broker_ensure!(self.state.role != RegionRole::Fenced, "region is fenced");
        crate::broker_ensure!(
            source_fencing_token == self.state.fencing_token,
            "stale replication fencing token"
        );
        crate::broker_ensure!(
            sha256(&chunk.bytes) == chunk.checksum,
            "replication chunk checksum mismatch"
        );
        crate::broker_ensure!(
            chunk.first_offset <= chunk.last_offset,
            "invalid replication offset range"
        );
        crate::broker_ensure!(
            self.in_flight < self.policy.max_in_flight_chunks,
            "replication in-flight limit reached"
        );
        crate::broker_ensure!(
            self.bytes_in_window
                .saturating_add(chunk.bytes.len() as u64)
                <= self.policy.max_bandwidth_bytes_per_second,
            "replication bandwidth limit reached"
        );
        let key = checkpoint_key(&chunk.stream, chunk.partition);
        let current = self.state.checkpoints.entry(key).or_default();
        if current
            .last_offset
            .is_some_and(|offset| chunk.last_offset < offset)
        {
            return Ok(false);
        }
        if current.last_offset == Some(chunk.last_offset) {
            crate::broker_ensure!(
                current.checksum.as_deref() == Some(chunk.checksum.as_str()),
                "conflicting replicated position"
            );
            return Ok(false);
        }
        crate::broker_ensure!(
            current.last_offset.map_or(true, |offset| chunk.first_offset
                == offset.saturating_add(1)),
            "replication chunk creates an offset gap"
        );
        self.in_flight += 1;
        self.bytes_in_window += chunk.bytes.len() as u64;
        current.segment_id = Some(chunk.segment_id);
        current.last_offset = Some(chunk.last_offset);
        current.checksum = Some(chunk.checksum.clone());
        self.in_flight = self.in_flight.saturating_sub(1);
        self.persist()?;
        Ok(true)
    }

    pub fn reset_bandwidth_window(&mut self) {
        self.bytes_in_window = 0;
    }

    pub fn promote(&mut self, expected_token: u64) -> Result<u64> {
        crate::broker_ensure!(
            expected_token == self.state.fencing_token,
            "stale promotion token"
        );
        self.state.fencing_token = self.state.fencing_token.saturating_add(1);
        self.state.role = RegionRole::Primary;
        self.persist()?;
        Ok(self.state.fencing_token)
    }

    pub fn fence(&mut self) -> Result<u64> {
        self.state.fencing_token = self.state.fencing_token.saturating_add(1);
        self.state.role = RegionRole::Fenced;
        self.persist()?;
        Ok(self.state.fencing_token)
    }

    pub fn demote(&mut self) -> Result<()> {
        self.state.role = RegionRole::Standby;
        self.state.fencing_token = self.state.fencing_token.saturating_add(1);
        self.persist()
    }

    fn persist(&self) -> Result<()> {
        let Some(path) = &self.path else {
            return Ok(());
        };
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let temporary = path.with_extension("tmp");
        let body = serde_json::to_vec(&self.state)
            .map_err(|error| BrokerError::with_source("encoding replication checkpoint", error))?;
        fs::write(&temporary, body)?;
        fs::rename(temporary, path)?;
        Ok(())
    }
}

fn checkpoint_key(stream: &str, partition: PartitionId) -> String {
    format!("{stream}:{:010}", partition.0)
}

#[cfg(test)]
mod tests;
