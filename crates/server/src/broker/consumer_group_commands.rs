use super::*;
use crate::consumer_group::{AssignmentStrategy, GroupConfig, GroupCoordinator};

impl Morrow {
    pub(super) async fn handle_group_join(
        &self,
        connection_id: u64,
        group: String,
        member: String,
        partitions: u32,
        strategy: protocol::GroupAssignmentStrategy,
        instance_id: Option<String>,
    ) -> Result<()> {
        let strategy = match strategy {
            protocol::GroupAssignmentStrategy::Range => AssignmentStrategy::Range,
            protocol::GroupAssignmentStrategy::RoundRobin => AssignmentStrategy::RoundRobin,
            protocol::GroupAssignmentStrategy::Sticky => AssignmentStrategy::Sticky,
        };
        let mut groups = self.groups.lock().await;
        let coordinator = groups.entry(group.clone()).or_insert_with(|| {
            GroupCoordinator::new(
                partitions,
                GroupConfig {
                    strategy,
                    ..Default::default()
                },
            )
            .expect("default group timeouts are valid")
        });
        crate::broker_ensure!(
            coordinator.snapshot().assignments.is_empty()
                || coordinator.snapshot().members.is_empty()
                || coordinator
                    .snapshot()
                    .assignments
                    .iter()
                    .flat_map(|assignment| assignment.partitions.iter())
                    .count()
                    == partitions as usize,
            "group partition count cannot change while members are active"
        );
        let generation = coordinator
            .join(member.clone(), instance_id, self.hooks.clock.now_ms())
            .with_context(|| format!("could not join consumer group {group}"))?;
        let assignment = coordinator
            .snapshot()
            .assignments
            .into_iter()
            .find(|assignment| assignment.member_id == member)
            .map(|assignment| {
                assignment
                    .partitions
                    .iter()
                    .map(u32::to_string)
                    .collect::<Vec<_>>()
                    .join(",")
            })
            .unwrap_or_default();
        drop(groups);
        self.send_to(
            connection_id,
            format!("G-OK JOIN {group} {generation} {assignment}\r\n").into_bytes(),
        )
        .await
    }

    pub(super) async fn handle_group_heartbeat(
        &self,
        connection_id: u64,
        group: String,
        member: String,
        generation: u64,
    ) -> Result<()> {
        let mut groups = self.groups.lock().await;
        let coordinator = groups
            .get_mut(&group)
            .ok_or_else(|| BrokerError::msg("consumer group does not exist"))?;
        coordinator
            .heartbeat(&member, generation, self.hooks.clock.now_ms())
            .with_context(|| format!("consumer-group heartbeat failed for {group}"))?;
        drop(groups);
        self.send_to(connection_id, b"G-OK HEARTBEAT\r\n".to_vec())
            .await
    }

    pub(super) async fn handle_group_leave(
        &self,
        connection_id: u64,
        group: String,
        member: String,
        generation: u64,
    ) -> Result<()> {
        let mut groups = self.groups.lock().await;
        let coordinator = groups
            .get_mut(&group)
            .ok_or_else(|| BrokerError::msg("consumer group does not exist"))?;
        coordinator
            .leave(&member, generation, self.hooks.clock.now_ms())
            .with_context(|| format!("consumer-group leave failed for {group}"))?;
        drop(groups);
        self.send_to(connection_id, b"G-OK LEAVE\r\n".to_vec())
            .await
    }

    pub(super) async fn handle_group_commit(
        &self,
        connection_id: u64,
        group: String,
        member: String,
        generation: u64,
        partition: u32,
        offset: u64,
    ) -> Result<()> {
        let mut groups = self.groups.lock().await;
        let coordinator = groups
            .get_mut(&group)
            .ok_or_else(|| BrokerError::msg("consumer group does not exist"))?;
        coordinator
            .commit(&member, generation, partition, offset)
            .with_context(|| format!("consumer-group commit failed for {group}"))?;
        drop(groups);
        self.send_to(connection_id, b"G-OK COMMIT\r\n".to_vec())
            .await
    }
}
