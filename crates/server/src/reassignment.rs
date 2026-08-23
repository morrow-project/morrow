//! Restartable partition movement and capacity-aware placement planning.
//!
//! The controller is deliberately separate from the data-plane replication
//! engine.  It records the safety boundary reached by a move and only permits
//! leadership transfer after the destination has caught up to the committed
//! high watermark.

use crate::error::{BrokerError, Result};
use crate::partition_replication::ReplicaProgress;
use crate::stream::PartitionId;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs;
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum BrokerLifecycle {
    Active,
    Draining,
    Decommissioned,
    Replacement,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BrokerCapacity {
    pub node_id: u64,
    pub region: String,
    pub zone: String,
    pub disk_capacity_bytes: u64,
    pub disk_used_bytes: u64,
    pub partition_count: u32,
    pub leader_count: u32,
    pub throughput_bytes_per_second: u64,
    pub max_concurrent_moves: u32,
    pub lifecycle: BrokerLifecycle,
}

impl BrokerCapacity {
    fn score(&self) -> (u128, u32, u32, u64, u64) {
        let disk = if self.disk_capacity_bytes == 0 {
            u128::MAX
        } else {
            u128::from(self.disk_used_bytes) * 1_000_000 / u128::from(self.disk_capacity_bytes)
        };
        (
            disk,
            self.partition_count,
            self.leader_count,
            self.throughput_bytes_per_second,
            self.node_id,
        )
    }

    fn eligible(&self) -> bool {
        matches!(
            self.lifecycle,
            BrokerLifecycle::Active | BrokerLifecycle::Replacement
        ) && self.max_concurrent_moves > 0
            && self.disk_used_bytes < self.disk_capacity_bytes
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct PlacementConstraints {
    pub min_distinct_regions: usize,
    pub min_distinct_zones: usize,
    pub allowed_regions: BTreeSet<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PartitionPlacement {
    pub stream: String,
    pub partition: PartitionId,
    pub replicas: BTreeSet<u64>,
    pub leader: u64,
    pub constraints: PlacementConstraints,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PlacementMove {
    pub stream: String,
    pub partition: PartitionId,
    pub from: u64,
    pub to: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ReassignmentPhase {
    AddReplica,
    CatchingUp,
    TransferLeadership,
    RemoveReplica,
    Complete,
    RolledBack { reason: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Reassignment {
    pub id: u64,
    pub stream: String,
    pub partition: PartitionId,
    pub source: u64,
    pub destination: u64,
    pub source_epoch: u64,
    pub phase: ReassignmentPhase,
    pub last_high_watermark: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReassignmentProgress {
    pub high_watermark: Option<u64>,
    pub destination: ReplicaProgress,
    pub quorum_available: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MoveThrottle {
    max_concurrent_moves: u32,
    max_bytes_per_window: u64,
    active_moves: u32,
    bytes_in_window: u64,
}

impl MoveThrottle {
    pub fn new(max_concurrent_moves: u32, max_bytes_per_window: u64) -> Self {
        Self {
            max_concurrent_moves,
            max_bytes_per_window,
            active_moves: 0,
            bytes_in_window: 0,
        }
    }

    pub fn try_start(&mut self, estimated_bytes: u64) -> bool {
        if self.active_moves >= self.max_concurrent_moves
            || self.bytes_in_window.saturating_add(estimated_bytes) > self.max_bytes_per_window
        {
            return false;
        }
        self.active_moves += 1;
        self.bytes_in_window += estimated_bytes;
        true
    }

    pub fn finish(&mut self) {
        self.active_moves = self.active_moves.saturating_sub(1);
    }

    pub fn reset_window(&mut self) {
        self.bytes_in_window = 0;
    }

    pub fn active_moves(&self) -> u32 {
        self.active_moves
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct PersistedState {
    next_id: u64,
    reassignments: BTreeMap<u64, Reassignment>,
}

/// Durable controller state.  The atomic JSON snapshot is intentionally small:
/// partition data stays in the replication engine, while this file records
/// only the phase and fencing metadata needed to resume safely.
#[derive(Debug)]
pub struct ReassignmentController {
    path: Option<PathBuf>,
    state: PersistedState,
    brokers: HashMap<u64, BrokerCapacity>,
    active_moves: u32,
}

impl ReassignmentController {
    pub fn new(brokers: impl IntoIterator<Item = BrokerCapacity>) -> Self {
        Self {
            path: None,
            state: PersistedState {
                next_id: 1,
                ..Default::default()
            },
            brokers: brokers
                .into_iter()
                .map(|broker| (broker.node_id, broker))
                .collect(),
            active_moves: 0,
        }
    }

    pub fn open(
        path: impl Into<PathBuf>,
        brokers: impl IntoIterator<Item = BrokerCapacity>,
    ) -> Result<Self> {
        let path = path.into();
        let state = if path.exists() {
            serde_json::from_slice(&fs::read(&path)?)
                .map_err(|error| BrokerError::with_source("decoding reassignment state", error))?
        } else {
            PersistedState {
                next_id: 1,
                ..Default::default()
            }
        };
        let active_moves = state
            .reassignments
            .values()
            .filter(|move_| {
                !matches!(
                    move_.phase,
                    ReassignmentPhase::Complete | ReassignmentPhase::RolledBack { .. }
                )
            })
            .count() as u32;
        Ok(Self {
            path: Some(path),
            state,
            brokers: brokers
                .into_iter()
                .map(|broker| (broker.node_id, broker))
                .collect(),
            active_moves,
        })
    }

    pub fn plans(&self) -> impl Iterator<Item = &Reassignment> {
        self.state.reassignments.values()
    }

    pub fn plan(&self, id: u64) -> Option<&Reassignment> {
        self.state.reassignments.get(&id)
    }

    pub fn begin(&mut self, move_: PlacementMove, source_epoch: u64) -> Result<u64> {
        let source = self
            .brokers
            .get(&move_.from)
            .ok_or_else(|| BrokerError::msg("reassignment source broker is unknown"))?;
        let destination = self
            .brokers
            .get(&move_.to)
            .ok_or_else(|| BrokerError::msg("reassignment destination broker is unknown"))?;
        crate::broker_ensure!(
            move_.from != move_.to,
            "reassignment source equals destination"
        );
        crate::broker_ensure!(
            source.lifecycle != BrokerLifecycle::Decommissioned,
            "source broker is decommissioned"
        );
        crate::broker_ensure!(destination.eligible(), "destination broker is not eligible");
        crate::broker_ensure!(
            self.active_moves < destination.max_concurrent_moves,
            "move concurrency limit reached"
        );
        let id = self.state.next_id;
        self.state.next_id = self.state.next_id.saturating_add(1);
        self.state.reassignments.insert(
            id,
            Reassignment {
                id,
                stream: move_.stream,
                partition: move_.partition,
                source: move_.from,
                destination: move_.to,
                source_epoch,
                phase: ReassignmentPhase::AddReplica,
                last_high_watermark: None,
            },
        );
        self.active_moves += 1;
        self.persist()?;
        Ok(id)
    }

    pub fn advance(
        &mut self,
        id: u64,
        progress: ReassignmentProgress,
    ) -> Result<ReassignmentPhase> {
        let plan = self
            .state
            .reassignments
            .get_mut(&id)
            .ok_or_else(|| BrokerError::msg("unknown reassignment"))?;
        plan.last_high_watermark = progress.high_watermark;
        match plan.phase {
            ReassignmentPhase::AddReplica => plan.phase = ReassignmentPhase::CatchingUp,
            ReassignmentPhase::CatchingUp => {
                crate::broker_ensure!(progress.quorum_available, "cannot advance without quorum");
                crate::broker_ensure!(
                    progress.destination.match_offset >= progress.high_watermark,
                    "destination is not caught up to the committed high watermark"
                );
                plan.phase = ReassignmentPhase::TransferLeadership;
            }
            ReassignmentPhase::TransferLeadership => {
                crate::broker_ensure!(
                    progress.quorum_available,
                    "cannot transfer leadership without quorum"
                );
                crate::broker_ensure!(
                    progress.destination.match_offset >= progress.high_watermark,
                    "leadership candidate is stale"
                );
                plan.phase = ReassignmentPhase::RemoveReplica;
            }
            ReassignmentPhase::RemoveReplica => {
                plan.phase = ReassignmentPhase::Complete;
                self.active_moves = self.active_moves.saturating_sub(1);
            }
            ReassignmentPhase::Complete | ReassignmentPhase::RolledBack { .. } => {}
        }
        let phase = plan.phase.clone();
        self.persist()?;
        Ok(phase)
    }

    pub fn rollback(&mut self, id: u64, reason: impl Into<String>) -> Result<()> {
        let plan = self
            .state
            .reassignments
            .get_mut(&id)
            .ok_or_else(|| BrokerError::msg("unknown reassignment"))?;
        crate::broker_ensure!(
            matches!(
                plan.phase,
                ReassignmentPhase::AddReplica | ReassignmentPhase::CatchingUp
            ),
            "cannot roll back after leadership transfer"
        );
        plan.phase = ReassignmentPhase::RolledBack {
            reason: reason.into(),
        };
        self.active_moves = self.active_moves.saturating_sub(1);
        self.persist()
    }

    pub fn set_broker_lifecycle(&mut self, node_id: u64, lifecycle: BrokerLifecycle) -> Result<()> {
        self.brokers
            .get_mut(&node_id)
            .ok_or_else(|| BrokerError::msg("unknown broker"))?
            .lifecycle = lifecycle;
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
            .map_err(|error| BrokerError::with_source("encoding reassignment state", error))?;
        fs::write(&temporary, body)?;
        fs::rename(temporary, path)?;
        Ok(())
    }
}

pub fn plan_moves(
    placements: &[PartitionPlacement],
    brokers: &[BrokerCapacity],
) -> Vec<PlacementMove> {
    let by_node = brokers
        .iter()
        .map(|broker| (broker.node_id, broker))
        .collect::<BTreeMap<_, _>>();
    let mut load = brokers
        .iter()
        .map(|broker| {
            (
                broker.node_id,
                [
                    broker.disk_used_bytes,
                    u64::from(broker.partition_count),
                    u64::from(broker.leader_count),
                    broker.throughput_bytes_per_second,
                ],
            )
        })
        .collect::<HashMap<_, _>>();
    let mut moves = Vec::new();
    let mut ordered = placements.to_vec();
    ordered.sort_by_key(|placement| (placement.stream.clone(), placement.partition.0));
    for placement in ordered {
        let Some(source) = placement
            .replicas
            .iter()
            .filter_map(|node| by_node.get(node))
            .max_by_key(|broker| {
                load.get(&broker.node_id)
                    .map_or_else(|| broker.score(), |values| load_score(broker, values))
            })
        else {
            continue;
        };
        let Some(destination) = brokers
            .iter()
            .filter(|broker| broker.eligible() && !placement.replicas.contains(&broker.node_id))
            .filter(|broker| {
                placement.constraints.allowed_regions.is_empty()
                    || placement
                        .constraints
                        .allowed_regions
                        .contains(&broker.region)
            })
            .filter(|broker| {
                broker.region != source.region
                    || placement.constraints.min_distinct_regions
                        <= distinct_regions(&placement.replicas, &by_node)
            })
            .min_by_key(|broker| {
                load.get(&broker.node_id)
                    .map_or_else(|| broker.score(), |values| load_score(broker, values))
            })
        else {
            continue;
        };
        let source_score = load
            .get(&source.node_id)
            .map_or_else(|| source.score(), |values| load_score(source, values));
        let destination_score = load.get(&destination.node_id).map_or_else(
            || destination.score(),
            |values| load_score(destination, values),
        );
        if destination_score < source_score {
            moves.push(PlacementMove {
                stream: placement.stream,
                partition: placement.partition,
                from: source.node_id,
                to: destination.node_id,
            });
            if let Some(values) = load.get_mut(&source.node_id) {
                values[1] = values[1].saturating_sub(1);
                values[2] = values[2].saturating_sub(if placement.leader == source.node_id {
                    1
                } else {
                    0
                });
            }
            if let Some(values) = load.get_mut(&destination.node_id) {
                values[1] = values[1].saturating_add(1);
                values[2] = values[2].saturating_add(if placement.leader == destination.node_id {
                    1
                } else {
                    0
                });
            }
        }
    }
    moves
}

fn load_score(broker: &BrokerCapacity, values: &[u64; 4]) -> (u128, u32, u32, u64, u64) {
    let disk = if broker.disk_capacity_bytes == 0 {
        u128::MAX
    } else {
        u128::from(values[0]) * 1_000_000 / u128::from(broker.disk_capacity_bytes)
    };
    (
        disk,
        values[1].min(u64::from(u32::MAX)) as u32,
        values[2].min(u64::from(u32::MAX)) as u32,
        values[3],
        broker.node_id,
    )
}

fn distinct_regions(replicas: &BTreeSet<u64>, brokers: &BTreeMap<u64, &BrokerCapacity>) -> usize {
    replicas
        .iter()
        .filter_map(|node| brokers.get(node).map(|broker| broker.region.as_str()))
        .collect::<BTreeSet<_>>()
        .len()
}

#[cfg(test)]
mod tests;
