//! Broker-managed consumer-group coordination.
//!
//! The coordinator deliberately has no networking or storage dependencies.  It
//! is the single place where membership, generations, leases, assignments and
//! committed offsets are reconciled, which keeps broker integration and future
//! cluster replication deterministic.

use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AssignmentStrategy {
    Range,
    RoundRobin,
    Sticky,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct GroupConfig {
    pub heartbeat_timeout_ms: u64,
    pub rebalance_timeout_ms: u64,
    pub strategy: AssignmentStrategy,
    pub max_members: usize,
    pub max_partitions: u32,
}

impl Default for GroupConfig {
    fn default() -> Self {
        Self {
            heartbeat_timeout_ms: 30_000,
            rebalance_timeout_ms: 60_000,
            strategy: AssignmentStrategy::Sticky,
            max_members: 10_000,
            max_partitions: 10_000,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Member {
    pub id: String,
    pub instance_id: Option<String>,
    pub last_heartbeat_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Assignment {
    pub generation: u64,
    pub member_id: String,
    pub partitions: Vec<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Rebalance {
    pub generation: u64,
    pub moved_partitions: Vec<u32>,
    pub deadline_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct GroupSnapshot {
    pub generation: u64,
    pub members: Vec<Member>,
    pub assignments: Vec<Assignment>,
    pub committed_offsets: BTreeMap<u32, u64>,
    pub rebalance: Option<Rebalance>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct GroupRecord {
    pub partitions: u32,
    pub config: GroupConfig,
    pub snapshot: GroupSnapshot,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GroupError {
    UnknownMember,
    StaleGeneration { expected: u64, actual: u64 },
    MemberAlreadyExists,
    InvalidTimeout,
    PartitionCountCannotDecrease,
    QuotaExceeded,
}

impl std::fmt::Display for GroupError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownMember => f.write_str("consumer-group member is not active"),
            Self::StaleGeneration { expected, actual } => {
                write!(
                    f,
                    "stale consumer-group generation {actual}; current generation is {expected}"
                )
            }
            Self::MemberAlreadyExists => f.write_str("consumer-group member already exists"),
            Self::InvalidTimeout => {
                f.write_str("consumer-group timeouts must be greater than zero")
            }
            Self::PartitionCountCannotDecrease => {
                f.write_str("consumer-group partition count cannot decrease")
            }
            Self::QuotaExceeded => f.write_str("consumer-group quota exceeded"),
        }
    }
}

impl std::error::Error for GroupError {}

#[derive(Debug, Clone)]
pub struct GroupCoordinator {
    config: GroupConfig,
    partitions: u32,
    generation: u64,
    members: BTreeMap<String, Member>,
    assignments: BTreeMap<String, BTreeSet<u32>>,
    previous_assignments: BTreeMap<String, BTreeSet<u32>>,
    committed_offsets: BTreeMap<u32, u64>,
    rebalance: Option<Rebalance>,
}

impl GroupCoordinator {
    pub fn new(partitions: u32, config: GroupConfig) -> Result<Self, GroupError> {
        if config.heartbeat_timeout_ms == 0 || config.rebalance_timeout_ms == 0 {
            return Err(GroupError::InvalidTimeout);
        }
        if partitions == 0 || partitions > config.max_partitions || config.max_members == 0 {
            return Err(GroupError::QuotaExceeded);
        }
        Ok(Self {
            config,
            partitions,
            generation: 0,
            members: BTreeMap::new(),
            assignments: BTreeMap::new(),
            previous_assignments: BTreeMap::new(),
            committed_offsets: BTreeMap::new(),
            rebalance: None,
        })
    }

    pub fn join(
        &mut self,
        member_id: impl Into<String>,
        instance_id: Option<String>,
        now_ms: u64,
    ) -> Result<u64, GroupError> {
        let member_id = member_id.into();
        if !self.members.contains_key(&member_id) && self.members.len() >= self.config.max_members {
            return Err(GroupError::QuotaExceeded);
        }
        // Rejoining the same member id fences the old session and starts a
        // fresh generation. This is the static-membership rolling-restart
        // path as well as the recovery path after a lost connection.
        self.members.remove(&member_id);
        self.members.insert(
            member_id.clone(),
            Member {
                id: member_id.clone(),
                instance_id,
                last_heartbeat_ms: now_ms,
            },
        );
        self.rebalance(now_ms);
        Ok(self.generation)
    }

    pub fn partition_count(&self) -> u32 {
        self.partitions
    }

    pub fn expand_partitions(&mut self, partitions: u32, now_ms: u64) -> Result<(), GroupError> {
        if partitions < self.partitions {
            return Err(GroupError::PartitionCountCannotDecrease);
        }
        if partitions > self.config.max_partitions {
            return Err(GroupError::QuotaExceeded);
        }
        if partitions > self.partitions {
            self.partitions = partitions;
            self.rebalance(now_ms);
        }
        Ok(())
    }

    pub fn heartbeat(
        &mut self,
        member_id: &str,
        generation: u64,
        now_ms: u64,
    ) -> Result<(), GroupError> {
        self.check_generation(generation)?;
        let member = self
            .members
            .get_mut(member_id)
            .ok_or(GroupError::UnknownMember)?;
        member.last_heartbeat_ms = now_ms;
        if self
            .rebalance
            .as_ref()
            .is_some_and(|rebalance| rebalance.generation == generation)
        {
            self.rebalance = None;
        }
        Ok(())
    }

    pub fn refresh_member(&mut self, member_id: &str, now_ms: u64) -> Result<u64, GroupError> {
        let member = self
            .members
            .get_mut(member_id)
            .ok_or(GroupError::UnknownMember)?;
        member.last_heartbeat_ms = now_ms;
        Ok(self.generation)
    }

    pub fn leave(
        &mut self,
        member_id: &str,
        generation: u64,
        now_ms: u64,
    ) -> Result<(), GroupError> {
        self.check_generation(generation)?;
        if self.members.remove(member_id).is_none() {
            return Err(GroupError::UnknownMember);
        }
        self.rebalance(now_ms);
        Ok(())
    }

    /// Expires members without scanning retained data. Returns the members removed.
    pub fn expire(&mut self, now_ms: u64) -> Vec<String> {
        let expired = self
            .members
            .values()
            .filter(|member| {
                now_ms.saturating_sub(member.last_heartbeat_ms) >= self.config.heartbeat_timeout_ms
            })
            .map(|member| member.id.clone())
            .collect::<Vec<_>>();
        if expired.is_empty() {
            return expired;
        }
        for member_id in &expired {
            self.members.remove(member_id);
        }
        self.rebalance(now_ms);
        expired
    }

    /// Commits are monotonic and fenced by the generation that owned them.
    pub fn commit(
        &mut self,
        member_id: &str,
        generation: u64,
        partition: u32,
        offset: u64,
    ) -> Result<bool, GroupError> {
        self.check_generation(generation)?;
        if !self.members.contains_key(member_id) {
            return Err(GroupError::UnknownMember);
        }
        if !self
            .assignments
            .get(member_id)
            .is_some_and(|partitions| partitions.contains(&partition))
        {
            return Err(GroupError::UnknownMember);
        }
        let current = self.committed_offsets.entry(partition).or_default();
        if offset <= *current {
            return Ok(false);
        }
        *current = offset;
        Ok(true)
    }

    pub fn assigned_partitions(
        &self,
        member_id: &str,
        generation: u64,
    ) -> Result<BTreeSet<u32>, GroupError> {
        self.check_generation(generation)?;
        self.assignments
            .get(member_id)
            .cloned()
            .ok_or(GroupError::UnknownMember)
    }

    pub fn snapshot(&self) -> GroupSnapshot {
        let mut assignments = self
            .assignments
            .iter()
            .map(|(member_id, partitions)| Assignment {
                generation: self.generation,
                member_id: member_id.clone(),
                partitions: partitions.iter().copied().collect(),
            })
            .collect::<Vec<_>>();
        assignments.sort_by(|left, right| left.member_id.cmp(&right.member_id));
        GroupSnapshot {
            generation: self.generation,
            members: self.members.values().cloned().collect(),
            assignments,
            committed_offsets: self.committed_offsets.clone(),
            rebalance: self.rebalance.clone(),
        }
    }

    pub fn record(&self) -> GroupRecord {
        GroupRecord {
            partitions: self.partitions,
            config: self.config.clone(),
            snapshot: self.snapshot(),
        }
    }

    pub fn from_record(record: GroupRecord) -> Result<Self, GroupError> {
        Self::from_record_internal(record, true)
    }

    pub fn from_replicated_record(record: GroupRecord) -> Result<Self, GroupError> {
        Self::from_record_internal(record, false)
    }

    fn from_record_internal(record: GroupRecord, recovery_fence: bool) -> Result<Self, GroupError> {
        let mut assignments = BTreeMap::new();
        for assignment in &record.snapshot.assignments {
            assignments.insert(
                assignment.member_id.clone(),
                assignment.partitions.iter().copied().collect(),
            );
        }
        for member in &record.snapshot.members {
            assignments.entry(member.id.clone()).or_default();
        }
        if record.config.heartbeat_timeout_ms == 0
            || record.config.rebalance_timeout_ms == 0
            || record.config.max_members == 0
            || record.partitions == 0
            || record.partitions > record.config.max_partitions
        {
            return Err(GroupError::InvalidTimeout);
        }
        let members = record
            .snapshot
            .members
            .iter()
            .cloned()
            .map(|member| (member.id.clone(), member))
            .collect::<BTreeMap<_, _>>();
        Ok(Self {
            config: record.config,
            partitions: record.partitions,
            // Membership is ephemeral. Bump the generation on process
            // recovery, but preserve it when applying a live Raft delta.
            generation: if recovery_fence {
                record.snapshot.generation.saturating_add(1)
            } else {
                record.snapshot.generation
            },
            members: if recovery_fence {
                BTreeMap::new()
            } else {
                members
            },
            previous_assignments: assignments.clone(),
            assignments: if recovery_fence {
                BTreeMap::new()
            } else {
                assignments
            },
            committed_offsets: record.snapshot.committed_offsets,
            rebalance: if recovery_fence {
                None
            } else {
                record.snapshot.rebalance
            },
        })
    }

    fn check_generation(&self, generation: u64) -> Result<(), GroupError> {
        if generation != self.generation {
            return Err(GroupError::StaleGeneration {
                expected: self.generation,
                actual: generation,
            });
        }
        Ok(())
    }

    fn rebalance(&mut self, now_ms: u64) {
        self.generation = self.generation.saturating_add(1);
        self.previous_assignments = std::mem::take(&mut self.assignments);
        self.assignments = BTreeMap::new();
        for member_id in self.members.keys() {
            self.assignments.insert(member_id.clone(), BTreeSet::new());
        }
        let member_ids = self.members.keys().cloned().collect::<Vec<_>>();
        if !member_ids.is_empty() {
            match self.config.strategy {
                AssignmentStrategy::Range => {
                    for partition in 0..self.partitions {
                        let index = (partition as usize * member_ids.len())
                            / self.partitions.max(1) as usize;
                        self.assignments
                            .get_mut(&member_ids[index.min(member_ids.len() - 1)])
                            .unwrap()
                            .insert(partition);
                    }
                }
                AssignmentStrategy::RoundRobin => {
                    for partition in 0..self.partitions {
                        self.assignments
                            .get_mut(&member_ids[partition as usize % member_ids.len()])
                            .unwrap()
                            .insert(partition);
                    }
                }
                AssignmentStrategy::Sticky => self.assign_sticky(&member_ids),
            }
        }
        let moved_partitions = self
            .previous_assignments
            .iter()
            .flat_map(|(member_id, partitions)| {
                partitions.iter().filter_map(|partition| {
                    let still_owned = self
                        .assignments
                        .get(member_id)
                        .is_some_and(|current| current.contains(partition));
                    (!still_owned).then_some(*partition)
                })
            })
            .collect::<BTreeSet<_>>();
        self.rebalance = (!moved_partitions.is_empty()).then_some(Rebalance {
            generation: self.generation,
            moved_partitions: moved_partitions.into_iter().collect(),
            deadline_ms: now_ms.saturating_add(self.config.rebalance_timeout_ms),
        });
    }

    fn assign_sticky(&mut self, member_ids: &[String]) {
        let max_per_member = self.partitions.div_ceil(member_ids.len() as u32);
        for partition in 0..self.partitions {
            let previous_owner =
                self.previous_assignments
                    .iter()
                    .find_map(|(member_id, partitions)| {
                        partitions.contains(&partition).then_some(member_id)
                    });
            let owner = previous_owner
                .filter(|member_id| {
                    self.assignments
                        .get(*member_id)
                        .is_some_and(|partitions| partitions.len() < max_per_member as usize)
                })
                .cloned()
                .unwrap_or_else(|| {
                    member_ids
                        .iter()
                        .min_by_key(|member_id| {
                            (
                                self.assignments.get(*member_id).unwrap().len(),
                                member_id.as_str(),
                            )
                        })
                        .unwrap()
                        .clone()
                });
            self.assignments.get_mut(&owner).unwrap().insert(partition);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn assignments_are_unique_and_complete() {
        let mut coordinator = GroupCoordinator::new(
            11,
            GroupConfig {
                strategy: AssignmentStrategy::RoundRobin,
                ..Default::default()
            },
        )
        .unwrap();
        coordinator.join("a", None, 0).unwrap();
        let generation = coordinator.join("b", None, 1).unwrap();
        let snapshot = coordinator.snapshot();
        let partitions = snapshot
            .assignments
            .iter()
            .flat_map(|assignment| assignment.partitions.iter())
            .copied()
            .collect::<BTreeSet<_>>();
        assert_eq!(partitions, (0..11).collect());
        assert!(coordinator.heartbeat("a", generation - 1, 2).is_err());
    }

    #[test]
    fn commits_are_fenced_and_monotonic() {
        let mut coordinator = GroupCoordinator::new(1, Default::default()).unwrap();
        let generation = coordinator.join("a", None, 0).unwrap();
        assert!(coordinator.commit("a", generation, 0, 9).unwrap());
        assert!(!coordinator.commit("a", generation, 0, 4).unwrap());
        assert!(matches!(
            coordinator.commit("a", generation - 1, 0, 10),
            Err(GroupError::StaleGeneration { .. })
        ));
    }

    #[test]
    fn heartbeat_expiry_rebalances_without_history_scan() {
        let mut coordinator = GroupCoordinator::new(
            4,
            GroupConfig {
                heartbeat_timeout_ms: 10,
                ..Default::default()
            },
        )
        .unwrap();
        coordinator.join("a", None, 0).unwrap();
        let generation = coordinator.join("b", None, 5).unwrap();
        assert_eq!(coordinator.expire(10), vec!["a"]);
        assert_eq!(coordinator.snapshot().members[0].id, "b");
        assert_ne!(coordinator.snapshot().generation, generation);
    }

    #[test]
    fn deterministic_strategies_are_complete_and_unique() {
        for strategy in [
            AssignmentStrategy::Range,
            AssignmentStrategy::RoundRobin,
            AssignmentStrategy::Sticky,
        ] {
            let mut first = GroupCoordinator::new(
                17,
                GroupConfig {
                    strategy,
                    ..Default::default()
                },
            )
            .unwrap();
            for (index, member) in ["a", "b", "c", "d"].into_iter().enumerate() {
                first.join(member, None, index as u64).unwrap();
            }
            let snapshot = first.snapshot();
            let assigned = snapshot
                .assignments
                .iter()
                .flat_map(|assignment| assignment.partitions.iter())
                .copied()
                .collect::<BTreeSet<_>>();
            assert_eq!(assigned, (0..17).collect());

            let mut second = GroupCoordinator::new(
                17,
                GroupConfig {
                    strategy,
                    ..Default::default()
                },
            )
            .unwrap();
            for member in ["a", "b", "c", "d"] {
                second.join(member, None, 0).unwrap();
            }
            assert_eq!(snapshot.assignments, second.snapshot().assignments);
        }
    }

    #[test]
    fn partition_expansion_preserves_offsets_and_fences_old_generation() {
        let mut coordinator = GroupCoordinator::new(2, Default::default()).unwrap();
        let generation = coordinator.join("a", None, 0).unwrap();
        coordinator.commit("a", generation, 0, 7).unwrap();
        coordinator.expand_partitions(4, 1).unwrap();
        assert_eq!(coordinator.partition_count(), 4);
        assert_eq!(coordinator.snapshot().committed_offsets.get(&0), Some(&7));
        assert!(matches!(
            coordinator.commit("a", generation, 0, 8),
            Err(GroupError::StaleGeneration { .. })
        ));
    }

    #[test]
    #[ignore = "manual high-cardinality coordinator benchmark"]
    fn benchmark_thousand_groups_and_ten_thousand_members() {
        let started = std::time::Instant::now();
        let mut groups = Vec::with_capacity(1_000);
        for group_index in 0..1_000 {
            let mut group = GroupCoordinator::new(10, Default::default()).unwrap();
            for member_index in 0..10 {
                group
                    .join(format!("member-{group_index}-{member_index}"), None, 0)
                    .unwrap();
            }
            groups.push(group);
        }
        assert_eq!(
            groups
                .iter()
                .map(|group| group.snapshot().members.len())
                .sum::<usize>(),
            10_000
        );
        assert!(started.elapsed() < std::time::Duration::from_secs(10));
    }
}
