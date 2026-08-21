use super::*;

#[derive(Clone)]
pub(super) enum ClusterRuntime {
    Real(RaftRuntime),
    #[cfg(test)]
    Fake(FakeClusterRuntime),
}

impl ClusterRuntime {
    pub(super) fn real(runtime: RaftRuntime) -> Self {
        Self::Real(runtime)
    }

    pub(super) async fn client_write(&self, command: BrokerCommand) -> Result<BrokerResponse> {
        match self {
            Self::Real(runtime) => runtime.client_write(command).await,
            #[cfg(test)]
            Self::Fake(runtime) => runtime.client_write(command).await,
        }
    }

    pub(super) async fn client_write_forwarded(
        &self,
        command: BrokerCommand,
    ) -> Result<BrokerResponse> {
        match self {
            Self::Real(runtime) => runtime.client_write(command).await,
            #[cfg(test)]
            Self::Fake(runtime) => runtime.client_write_forwarded(command).await,
        }
    }

    pub(super) fn durable_state(&self) -> DurableState {
        match self {
            Self::Real(runtime) => runtime.durable_state(),
            #[cfg(test)]
            Self::Fake(runtime) => runtime.durable_state(),
        }
    }

    pub(super) async fn replicate_partition(
        &self,
        envelope: MessageEnvelope,
        fsync: bool,
    ) -> Result<MessageEnvelope> {
        match self {
            Self::Real(runtime) => runtime.replicate_partition(envelope, fsync).await,
            #[cfg(test)]
            Self::Fake(runtime) => runtime.replicate_partition(envelope, fsync).await,
        }
    }

    pub(super) async fn is_leader(&self) -> bool {
        match self {
            Self::Real(runtime) => runtime.is_leader().await,
            #[cfg(test)]
            Self::Fake(runtime) => runtime.is_leader().await,
        }
    }

    pub(super) async fn current_leader(&self) -> Option<u64> {
        match self {
            Self::Real(runtime) => runtime.current_leader().await,
            #[cfg(test)]
            Self::Fake(runtime) => runtime.current_leader().await,
        }
    }

    pub(super) async fn ensure_metadata_ready(&self) -> Result<()> {
        match self {
            Self::Real(runtime) => runtime.ensure_metadata_ready().await,
            #[cfg(test)]
            Self::Fake(_) => Ok(()),
        }
    }

    pub(super) async fn leader_client_addr(&self) -> Option<SocketAddr> {
        match self {
            Self::Real(runtime) => runtime.leader_client_addr().await,
            #[cfg(test)]
            Self::Fake(runtime) => runtime.leader_client_addr().await,
        }
    }

    pub(super) fn tls_enabled(&self) -> bool {
        match self {
            Self::Real(runtime) => runtime.tls_enabled(),
            #[cfg(test)]
            Self::Fake(runtime) => runtime.tls_enabled(),
        }
    }

    pub(super) fn cluster_size(&self) -> usize {
        match self {
            Self::Real(runtime) => runtime.cluster_size(),
            #[cfg(test)]
            Self::Fake(runtime) => runtime.node_count(),
        }
    }

    pub(super) fn local_node_id(&self) -> u64 {
        match self {
            Self::Real(runtime) => runtime.node_id(),
            #[cfg(test)]
            Self::Fake(runtime) => runtime.local_node_id(),
        }
    }
}
