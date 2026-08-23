use super::*;
use crate::consumer_group::{AssignmentStrategy, GroupConfig, GroupCoordinator};

impl Morrow {
    pub(super) async fn group_partitions_for_fetch(
        &self,
        connection_id: u64,
        consumer_name: &str,
    ) -> Result<Option<BTreeSet<u32>>> {
        let groups = self.groups.lock().await;
        let Some(coordinator) = groups.get(consumer_name) else {
            return Ok(None);
        };
        let Some(session) = self
            .group_sessions
            .lock()
            .await
            .get(&connection_id)
            .cloned()
        else {
            return Err(BrokerError::msg(
                "consumer-group membership is required for FETCH",
            ));
        };
        if session.group != consumer_name {
            return Err(BrokerError::msg("consumer is joined to a different group"));
        }
        coordinator
            .assigned_partitions(&session.member, session.generation)
            .map(Some)
            .with_context(|| {
                format!("consumer-group assignment is no longer valid for {consumer_name}")
            })
    }

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
        coordinator
            .expand_partitions(partitions, self.hooks.clock.now_ms())
            .with_context(|| format!("could not resize consumer group {group}"))?;
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
        let record = coordinator.record();
        drop(groups);
        self.persist_group_state(&group, record).await?;
        self.group_sessions.lock().await.insert(
            connection_id,
            GroupMemberSession {
                group: group.clone(),
                member: member.clone(),
                generation,
            },
        );
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
        let now = self.hooks.clock.now_ms();
        match coordinator.heartbeat(&member, generation, now) {
            Ok(()) => {}
            Err(crate::consumer_group::GroupError::StaleGeneration { .. }) => {
                coordinator
                    .refresh_member(&member, now)
                    .with_context(|| format!("consumer-group heartbeat failed for {group}"))?;
            }
            Err(error) => {
                return Err(BrokerError::with_source(
                    format!("consumer-group heartbeat failed for {group}"),
                    error,
                ));
            }
        }
        let current = coordinator.snapshot();
        let assignment = current
            .assignments
            .iter()
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
        let current_generation = current.generation;
        drop(groups);
        self.group_sessions
            .lock()
            .await
            .entry(connection_id)
            .and_modify(|session| {
                session.generation = current_generation;
            });
        self.send_to(
            connection_id,
            format!("G-OK HEARTBEAT {group} {current_generation} {assignment}\r\n").into_bytes(),
        )
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
        let record = coordinator.record();
        drop(groups);
        self.persist_group_state(&group, record).await?;
        self.group_sessions.lock().await.remove(&connection_id);
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
        let record = coordinator.record();
        drop(groups);
        self.persist_group_state(&group, record).await?;
        self.send_to(connection_id, b"G-OK COMMIT\r\n".to_vec())
            .await
    }

    async fn persist_group_state(
        &self,
        group: &str,
        record: crate::consumer_group::GroupRecord,
    ) -> Result<()> {
        if let Some(cluster) = self.cluster_runtime().await {
            self.cluster_write(
                &cluster,
                BrokerCommand::GroupUpsert {
                    group: group.to_string(),
                    record,
                },
            )
            .await?;
        } else {
            self.wal.append_group_state(group, &record)?;
        }
        self.wal.flush_due().await
    }
}
