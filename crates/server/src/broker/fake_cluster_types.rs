#[cfg(test)]
use super::*;

#[cfg(test)]
#[derive(Clone)]
pub(super) struct FakeClusterRuntime {
    pub(super) inner: Arc<std::sync::Mutex<FakeClusterState>>,
}

#[cfg(test)]
pub(super) struct FakeClusterState {
    pub(super) local_node_id: u64,
    pub(super) leader: Option<u64>,
    pub(super) tls_enabled: bool,
    pub(super) nodes: HashMap<u64, SocketAddr>,
    pub(super) available_nodes: BTreeSet<u64>,
    pub(super) state: DurableState,
    pub(super) partition_replication: PartitionReplication,
    pub(super) data_messages: HashMap<u64, PublishRecord>,
    pub(super) data_writes: usize,
    pub(super) writes: usize,
    pub(super) delay_writes: bool,
    pub(super) queued_writes: VecDeque<QueuedWrite>,
    pub(super) next_write_id: u64,
}

#[cfg(test)]
pub(super) struct QueuedWrite {
    pub(super) id: u64,
    pub(super) command: BrokerCommand,
    pub(super) response: oneshot::Sender<BrokerResponse>,
}
