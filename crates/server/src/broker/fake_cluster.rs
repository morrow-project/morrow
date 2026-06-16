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
        self.inner.lock().unwrap().state.clone()
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
        self.state.apply_command(command)
    }
}
