use crate::{
    error::{BrokerError, Result},
    partition_log::MessageEnvelope,
    stream::PartitionId,
};
use std::collections::{BTreeMap, BTreeSet, HashMap};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Durability {
    Quorum,
    QuorumFsync,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionAssignment {
    pub stream: String,
    pub partition: PartitionId,
    pub replicas: BTreeSet<u64>,
    pub leader: u64,
    pub leader_epoch: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ReplicaProgress {
    pub match_offset: Option<u64>,
    pub flushed_offset: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommitResult {
    pub offset: u64,
    pub high_watermark: u64,
    pub leader_epoch: u64,
    pub replicated: usize,
    pub flushed: usize,
}

#[derive(Debug, Clone)]
struct Replica {
    records: Vec<MessageEnvelope>,
    flushed_offset: Option<u64>,
}

#[derive(Debug, Clone)]
struct PartitionState {
    assignment: PartitionAssignment,
    replicas: BTreeMap<u64, Replica>,
    high_watermark: Option<u64>,
}

/// Controller-directed leader/follower replication for immutable partition logs.
///
/// The controller owns assignments and epochs. Message envelopes live only in
/// replica logs and are deliberately absent from metadata snapshots.
#[derive(Debug, Clone, Default)]
pub struct PartitionReplication {
    partitions: HashMap<(String, PartitionId), PartitionState>,
    available: BTreeSet<u64>,
}

impl PartitionReplication {
    pub fn new(nodes: impl IntoIterator<Item = u64>) -> Self {
        Self {
            partitions: HashMap::new(),
            available: nodes.into_iter().collect(),
        }
    }

    pub fn assign(&mut self, assignment: PartitionAssignment) -> Result<()> {
        crate::broker_ensure!(!assignment.replicas.is_empty(), "empty replica set");
        crate::broker_ensure!(
            assignment.replicas.contains(&assignment.leader),
            "partition leader is not a replica"
        );
        crate::broker_ensure!(assignment.leader_epoch > 0, "leader epoch must be positive");
        let key = (assignment.stream.clone(), assignment.partition);
        match self.partitions.get_mut(&key) {
            Some(state) => {
                crate::broker_ensure!(
                    assignment.leader_epoch >= state.assignment.leader_epoch,
                    "stale partition assignment epoch"
                );
                for node in &assignment.replicas {
                    state.replicas.entry(*node).or_insert_with(|| Replica {
                        records: Vec::new(),
                        flushed_offset: None,
                    });
                }
                state
                    .replicas
                    .retain(|node, _| assignment.replicas.contains(node));
                state.assignment = assignment;
            }
            None => {
                let replicas = assignment
                    .replicas
                    .iter()
                    .map(|node| {
                        (
                            *node,
                            Replica {
                                records: Vec::new(),
                                flushed_offset: None,
                            },
                        )
                    })
                    .collect();
                self.partitions.insert(
                    key,
                    PartitionState {
                        assignment,
                        replicas,
                        high_watermark: None,
                    },
                );
            }
        }
        Ok(())
    }

    pub fn append(
        &mut self,
        node: u64,
        leader_epoch: u64,
        envelope: MessageEnvelope,
        durability: Durability,
    ) -> Result<CommitResult> {
        let key = (envelope.stream.as_str().to_string(), envelope.partition);
        let state = self
            .partitions
            .get_mut(&key)
            .ok_or_else(|| BrokerError::msg("unknown partition assignment"))?;
        crate::broker_ensure!(state.assignment.leader == node, "not partition leader");
        crate::broker_ensure!(
            state.assignment.leader_epoch == leader_epoch,
            "fenced partition leader epoch"
        );
        crate::broker_ensure!(
            self.available.contains(&node),
            "partition leader unavailable"
        );
        crate::broker_ensure!(
            envelope.leader_epoch == leader_epoch,
            "envelope leader epoch mismatch"
        );

        let quorum = state.assignment.replicas.len() / 2 + 1;
        let leader = state.replicas.get_mut(&node).unwrap();
        append_or_validate(leader, &envelope, state.high_watermark)?;
        if durability == Durability::QuorumFsync {
            leader.flushed_offset = Some(envelope.offset);
        }

        let followers = state
            .assignment
            .replicas
            .iter()
            .copied()
            .filter(|replica| *replica != node && self.available.contains(replica))
            .collect::<Vec<_>>();
        for replica_id in followers {
            let replica = state.replicas.get_mut(&replica_id).unwrap();
            reconcile_and_append(replica, &envelope, state.high_watermark)?;
            if durability == Durability::QuorumFsync {
                replica.flushed_offset = Some(envelope.offset);
            }
        }

        let replicated = matching_replicas(state, envelope.offset, &self.available);
        crate::broker_ensure!(replicated >= quorum, "partition quorum unavailable");
        let flushed = flushed_replicas(state, envelope.offset, &self.available);
        if durability == Durability::QuorumFsync {
            crate::broker_ensure!(flushed >= quorum, "partition fsync quorum unavailable");
        }
        state.high_watermark = Some(envelope.offset);
        Ok(CommitResult {
            offset: envelope.offset,
            high_watermark: envelope.offset,
            leader_epoch,
            replicated,
            flushed,
        })
    }

    pub fn catch_up(&mut self, stream: &str, partition: PartitionId, node: u64) -> Result<()> {
        crate::broker_ensure!(self.available.contains(&node), "replica unavailable");
        let state = self.state_mut(stream, partition)?;
        crate::broker_ensure!(state.assignment.replicas.contains(&node), "not a replica");
        let leader_records = state.replicas[&state.assignment.leader].records.clone();
        let replica = state.replicas.get_mut(&node).unwrap();
        reconcile_committed(replica, &leader_records, state.high_watermark)?;
        Ok(())
    }

    pub fn transfer_leadership(
        &mut self,
        stream: &str,
        partition: PartitionId,
        candidate: u64,
    ) -> Result<u64> {
        crate::broker_ensure!(self.available.contains(&candidate), "candidate unavailable");
        let state = self.state_mut(stream, partition)?;
        crate::broker_ensure!(
            state.assignment.replicas.contains(&candidate),
            "candidate is not a replica"
        );
        crate::broker_ensure!(
            is_safe(state, candidate),
            "candidate is missing committed partition data"
        );
        state.assignment.leader = candidate;
        state.assignment.leader_epoch = state.assignment.leader_epoch.saturating_add(1);
        Ok(state.assignment.leader_epoch)
    }

    pub fn elect_safe_leader(
        &mut self,
        stream: &str,
        partition: PartitionId,
    ) -> Result<(u64, u64)> {
        let available = self.available.clone();
        let state = self.state_mut(stream, partition)?;
        let candidate = state
            .assignment
            .replicas
            .iter()
            .copied()
            .find(|node| available.contains(node) && is_safe(state, *node))
            .ok_or_else(|| BrokerError::msg("no safe replica available"))?;
        state.assignment.leader = candidate;
        state.assignment.leader_epoch = state.assignment.leader_epoch.saturating_add(1);
        Ok((candidate, state.assignment.leader_epoch))
    }

    pub fn set_available(&mut self, nodes: impl IntoIterator<Item = u64>) {
        self.available = nodes.into_iter().collect();
    }

    pub fn progress(
        &self,
        stream: &str,
        partition: PartitionId,
    ) -> Result<BTreeMap<u64, ReplicaProgress>> {
        let state = self.state(stream, partition)?;
        Ok(state
            .replicas
            .iter()
            .map(|(node, replica)| {
                (
                    *node,
                    ReplicaProgress {
                        match_offset: replica.records.last().map(|record| record.offset),
                        flushed_offset: replica.flushed_offset,
                    },
                )
            })
            .collect())
    }

    pub fn high_watermark(&self, stream: &str, partition: PartitionId) -> Result<Option<u64>> {
        Ok(self.state(stream, partition)?.high_watermark)
    }

    pub fn committed_records(
        &self,
        stream: &str,
        partition: PartitionId,
        node: u64,
    ) -> Result<Vec<MessageEnvelope>> {
        let state = self.state(stream, partition)?;
        let replica = state
            .replicas
            .get(&node)
            .ok_or_else(|| BrokerError::msg("not a replica"))?;
        let count = state.high_watermark.map_or(0, |offset| offset as usize + 1);
        Ok(replica.records.iter().take(count).cloned().collect())
    }

    #[cfg(test)]
    pub(crate) fn inject_uncommitted(
        &mut self,
        stream: &str,
        partition: PartitionId,
        node: u64,
        envelope: MessageEnvelope,
    ) -> Result<()> {
        self.state_mut(stream, partition)?
            .replicas
            .get_mut(&node)
            .ok_or_else(|| BrokerError::msg("not a replica"))?
            .records
            .push(envelope);
        Ok(())
    }

    fn state(&self, stream: &str, partition: PartitionId) -> Result<&PartitionState> {
        self.partitions
            .get(&(stream.to_string(), partition))
            .ok_or_else(|| BrokerError::msg("unknown partition assignment"))
    }

    fn state_mut(&mut self, stream: &str, partition: PartitionId) -> Result<&mut PartitionState> {
        self.partitions
            .get_mut(&(stream.to_string(), partition))
            .ok_or_else(|| BrokerError::msg("unknown partition assignment"))
    }
}

fn append_or_validate(
    replica: &mut Replica,
    envelope: &MessageEnvelope,
    high_watermark: Option<u64>,
) -> Result<()> {
    match replica.records.get(envelope.offset as usize) {
        Some(existing) => crate::broker_ensure!(existing == envelope, "immutable record conflict"),
        None => {
            crate::broker_ensure!(
                envelope.offset == replica.records.len() as u64,
                "partition append creates an offset gap"
            );
            replica.records.push(envelope.clone());
        }
    }
    if high_watermark.is_some_and(|committed| envelope.offset <= committed) {
        crate::broker_ensure!(
            replica.records[envelope.offset as usize] == *envelope,
            "committed record conflict"
        );
    }
    Ok(())
}

fn reconcile_and_append(
    replica: &mut Replica,
    envelope: &MessageEnvelope,
    high_watermark: Option<u64>,
) -> Result<()> {
    if let Some(existing) = replica.records.get(envelope.offset as usize) {
        if existing != envelope {
            crate::broker_ensure!(
                high_watermark.is_none_or(|committed| envelope.offset > committed),
                "replica diverges within committed history"
            );
            replica.records.truncate(envelope.offset as usize);
        }
    }
    append_or_validate(replica, envelope, high_watermark)
}

fn reconcile_committed(
    replica: &mut Replica,
    leader: &[MessageEnvelope],
    high_watermark: Option<u64>,
) -> Result<()> {
    let count = high_watermark.map_or(0, |offset| offset as usize + 1);
    for (index, expected) in leader.iter().take(count).enumerate() {
        if let Some(actual) = replica.records.get(index) {
            crate::broker_ensure!(
                actual == expected,
                "replica diverges within committed history"
            );
        } else {
            replica.records.push(expected.clone());
        }
    }
    replica.records.truncate(count);
    Ok(())
}

fn matching_replicas(state: &PartitionState, offset: u64, available: &BTreeSet<u64>) -> usize {
    let leader_record = &state.replicas[&state.assignment.leader].records[offset as usize];
    state
        .replicas
        .iter()
        .filter(|(node, replica)| {
            state.assignment.replicas.contains(node)
                && available.contains(node)
                && replica.records.get(offset as usize) == Some(leader_record)
        })
        .count()
}

fn flushed_replicas(state: &PartitionState, offset: u64, available: &BTreeSet<u64>) -> usize {
    state
        .replicas
        .iter()
        .filter(|(node, replica)| {
            available.contains(node)
                && replica
                    .flushed_offset
                    .is_some_and(|flushed| flushed >= offset)
        })
        .count()
}

fn is_safe(state: &PartitionState, node: u64) -> bool {
    match state.high_watermark {
        None => true,
        Some(high_watermark) => {
            state.replicas[&node].records.get(high_watermark as usize)
                == state.replicas[&state.assignment.leader]
                    .records
                    .get(high_watermark as usize)
        }
    }
}

#[cfg(test)]
#[path = "partition_replication/tests.rs"]
mod tests;
