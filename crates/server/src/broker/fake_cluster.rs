#[cfg(test)]
use super::*;

#[cfg(test)]
impl FakeClusterRuntime {
    pub(super) fn new(node_count: u64, local_node_id: u64, leader: Option<u64>) -> Self {
        assert!(node_count > 0);
        assert!(local_node_id > 0 && local_node_id <= node_count);
        if let Some(leader) = leader {
            assert!(leader > 0 && leader <= node_count);
        }
        let mut nodes = HashMap::new();
        let mut raft_nodes = BTreeMap::new();
        for node_id in 1..=node_count {
            let addr = SocketAddr::from(([127, 0, 0, 1], 10_000 + node_id as u16));
            nodes.insert(node_id, addr);
            raft_nodes.insert(node_id, BasicNode::new(addr));
        }
        Self {
            inner: Arc::new(std::sync::Mutex::new(FakeClusterState {
                local_node_id,
                leader,
                tls_enabled: false,
                available_nodes: nodes.keys().copied().collect(),
                nodes,
                state: DurableState::new(raft_nodes),
                partition_replication: PartitionReplication::new(1..=node_count),
                data_messages: HashMap::new(),
                data_writes: 0,
                writes: 0,
                delay_writes: false,
                queued_writes: VecDeque::new(),
                next_write_id: 1,
            })),
        }
    }

    pub(super) async fn client_write(&self, command: BrokerCommand) -> Result<BrokerResponse> {
        let pending = {
            let mut inner = self.inner.lock().unwrap();
            inner.ensure_writable()?;
            if !inner.delay_writes {
                return Ok(inner.apply_command(command));
            }
            let (tx, rx) = oneshot::channel();
            let id = inner.next_write_id;
            inner.next_write_id += 1;
            inner.queued_writes.push_back(QueuedWrite {
                id,
                command,
                response: tx,
            });
            rx
        };
        pending
            .await
            .map_err(|_| BrokerError::msg("queued write canceled"))
    }

    pub(super) async fn client_write_forwarded(
        &self,
        command: BrokerCommand,
    ) -> Result<BrokerResponse> {
        self.client_write(command).await
    }

    pub(super) fn durable_state(&self) -> DurableState {
        let inner = self.inner.lock().unwrap();
        let mut state = inner.state.clone();
        state.messages.extend(inner.data_messages.clone());
        state
    }

    pub(super) fn partition_record(
        &self,
        stream: &str,
        partition: u32,
        offset: u64,
    ) -> Option<MessageEnvelope> {
        self.inner
            .lock()
            .unwrap()
            .data_messages
            .values()
            .find(|record| {
                record.stream.as_deref() == Some(stream)
                    && record.partition == Some(partition)
                    && record.offset == Some(offset)
            })
            .cloned()
            .and_then(|record| {
                let stream = crate::stream::StreamId::new(record.stream.as_deref()?).ok()?;
                Some(MessageEnvelope {
                    namespace: record.namespace,
                    stream,
                    partition: crate::stream::PartitionId(record.partition?),
                    offset: record.offset?,
                    subject: record.subject,
                    key: record.key,
                    headers: record.headers,
                    timestamp_ms: record.timestamp_ms,
                    reply_to: record.reply_to,
                    payload: record.payload,
                    partitioning_epoch: record.partitioning_epoch,
                    leader_epoch: record.leader_epoch,
                    legacy_seq: record.seq,
                })
            })
    }

    pub(super) fn is_local_partition_replica(&self, stream: &str, partition: u32) -> bool {
        let inner = self.inner.lock().unwrap();
        inner
            .state
            .partition_assignments
            .get(&crate::raft::partition_key(stream, partition))
            .is_some_and(|assignment| assignment.replicas.contains(&inner.local_node_id))
    }

    pub(super) async fn replicate_partition(
        &self,
        mut envelope: MessageEnvelope,
        fsync: bool,
    ) -> Result<MessageEnvelope> {
        let (command, leader_epoch) = {
            let mut inner = self.inner.lock().unwrap();
            inner.ensure_writable()?;
            let leader = inner.leader.unwrap();
            let replicas = inner.nodes.keys().copied().collect::<BTreeSet<_>>();
            let previous = inner
                .partition_replication
                .high_watermark(envelope.stream.as_str(), envelope.partition)
                .ok()
                .flatten();
            let key = crate::raft::partition_key(envelope.stream.as_str(), envelope.partition.0);
            let leader_epoch = inner.state.partition_commits.get(&key).map_or(1, |commit| {
                if commit.leader_id == leader {
                    commit.leader_epoch
                } else {
                    commit.leader_epoch.saturating_add(1)
                }
            });
            inner.state.partition_assignments.insert(
                key,
                crate::raft::PartitionAssignmentMetadata {
                    replicas: replicas.clone(),
                    leader_id: leader,
                    leader_epoch,
                },
            );
            inner.partition_replication.assign(PartitionAssignment {
                stream: envelope.stream.as_str().to_string(),
                partition: envelope.partition,
                replicas,
                leader,
                leader_epoch,
            })?;
            let available = inner.available_nodes.iter().copied().collect::<Vec<_>>();
            inner.partition_replication.set_available(available);
            envelope.offset = previous.map_or(0, |offset| offset.saturating_add(1));
            envelope.leader_epoch = leader_epoch;
            inner.partition_replication.append(
                leader,
                leader_epoch,
                envelope.clone(),
                if fsync {
                    PartitionDurability::QuorumFsync
                } else {
                    PartitionDurability::Quorum
                },
            )?;
            inner
                .data_messages
                .insert(envelope.legacy_seq, PublishRecord::from(envelope.clone()));
            inner.data_writes += 1;
            (
                BrokerCommand::PartitionCommit {
                    stream: envelope.stream.as_str().to_string(),
                    partition: envelope.partition.0,
                    offset: envelope.offset,
                    checksum: crate::partition_log::committed_envelope_checksum(&envelope)?,
                    leader_id: leader,
                    leader_epoch,
                },
                leader_epoch,
            )
        };
        let response = self.client_write(command).await?;
        crate::broker_ensure!(
            matches!(
                response,
                BrokerResponse::PartitionCommit {
                    high_watermark,
                    leader_epoch: committed_epoch,
                } if high_watermark == envelope.offset && committed_epoch == leader_epoch
            ),
            "partition metadata commit rejected"
        );
        Ok(envelope)
    }

    pub(super) async fn is_leader(&self) -> bool {
        let inner = self.inner.lock().unwrap();
        inner.leader == Some(inner.local_node_id)
    }

    pub(super) async fn current_leader(&self) -> Option<u64> {
        self.inner.lock().unwrap().leader
    }

    pub(super) async fn leader_client_addr(&self) -> Option<SocketAddr> {
        let inner = self.inner.lock().unwrap();
        let leader = inner.leader?;
        inner.nodes.get(&leader).copied()
    }

    pub(super) fn tls_enabled(&self) -> bool {
        self.inner.lock().unwrap().tls_enabled
    }

    pub(super) fn write_count(&self) -> usize {
        self.inner.lock().unwrap().writes
    }

    pub(super) fn data_write_count(&self) -> usize {
        self.inner.lock().unwrap().data_writes
    }

    pub(super) fn node_count(&self) -> usize {
        self.inner.lock().unwrap().nodes.len()
    }

    pub(super) fn local_node_id(&self) -> u64 {
        self.inner.lock().unwrap().local_node_id
    }

    pub(super) fn queued_write_count(&self) -> usize {
        self.inner.lock().unwrap().queued_writes.len()
    }

    pub(super) fn set_client_addr(&self, node_id: u64, addr: SocketAddr) {
        let mut inner = self.inner.lock().unwrap();
        assert!(inner.nodes.contains_key(&node_id));
        inner.nodes.insert(node_id, addr);
    }

    pub(super) fn set_leader(&self, leader: Option<u64>) {
        let mut inner = self.inner.lock().unwrap();
        if let Some(leader) = leader {
            assert!(inner.nodes.contains_key(&leader));
        }
        inner.leader = leader;
    }

    pub(super) fn partition_available(&self, nodes: impl IntoIterator<Item = u64>) {
        let mut inner = self.inner.lock().unwrap();
        let available = nodes.into_iter().collect::<BTreeSet<_>>();
        for node_id in &available {
            assert!(inner.nodes.contains_key(node_id));
        }
        inner.available_nodes = available;
    }

    pub(super) fn restore_all_nodes(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.available_nodes = inner.nodes.keys().copied().collect();
    }

    pub(super) fn quorum_available(&self) -> bool {
        let inner = self.inner.lock().unwrap();
        inner.available_nodes.contains(&inner.local_node_id)
            && inner.available_nodes.len() >= inner.quorum_size()
    }

    pub(super) fn set_delay_writes(&self, delay_writes: bool) {
        self.inner.lock().unwrap().delay_writes = delay_writes;
    }

    pub(super) fn drain_one(&self) -> Option<u64> {
        let queued = self.inner.lock().unwrap().queued_writes.pop_front()?;
        let response = {
            let mut inner = self.inner.lock().unwrap();
            inner.apply_command(queued.command)
        };
        let _ = queued.response.send(response);
        Some(queued.id)
    }

    #[allow(dead_code)]
    pub(super) fn drain_all(&self) -> usize {
        let mut drained = 0;
        while self.drain_one().is_some() {
            drained += 1;
        }
        drained
    }
}

#[cfg(test)]
impl FakeClusterState {
    pub(super) fn quorum_size(&self) -> usize {
        (self.nodes.len() / 2) + 1
    }

    pub(super) fn ensure_writable(&self) -> Result<()> {
        if self.leader != Some(self.local_node_id) {
            crate::broker_bail!("not leader");
        }
        self.ensure_quorum()
    }

    pub(super) fn ensure_quorum(&self) -> Result<()> {
        if !self.available_nodes.contains(&self.local_node_id)
            || self.available_nodes.len() < self.quorum_size()
        {
            crate::broker_bail!("quorum unavailable");
        }
        Ok(())
    }

    pub(super) fn apply_command(&mut self, command: BrokerCommand) -> BrokerResponse {
        self.writes += 1;
        self.state.messages.extend(self.data_messages.clone());
        let response = self.state.apply_command(command);
        let partitioned = self
            .state
            .messages
            .extract_if(|_, record| record.stream.is_some())
            .collect::<HashMap<_, _>>();
        self.data_messages = partitioned;
        response
    }
}
