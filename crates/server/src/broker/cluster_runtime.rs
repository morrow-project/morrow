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

    pub(super) fn deltas_after(&self, after: Option<u64>) -> Option<DeltaBatch> {
        match self {
            Self::Real(runtime) => Some(runtime.deltas_after(after)),
            #[cfg(test)]
            Self::Fake(_) => None,
        }
    }

    pub(super) fn partition_record(
        &self,
        stream: &str,
        partition: u32,
        offset: u64,
    ) -> Option<MessageEnvelope> {
        match self {
            Self::Real(runtime) => runtime.partition_record(stream, partition, offset),
            #[cfg(test)]
            Self::Fake(runtime) => runtime.partition_record(stream, partition, offset),
        }
    }

    pub(super) fn local_committed_records(
        &self,
    ) -> Result<Vec<crate::partition_log::MessageEnvelope>> {
        match self {
            Self::Real(runtime) => runtime.local_committed_records(),
            #[cfg(test)]
            Self::Fake(_) => Ok(Vec::new()),
        }
    }

    pub(super) fn is_local_partition_replica(&self, stream: &str, partition: u32) -> bool {
        match self {
            Self::Real(runtime) => runtime.is_local_partition_replica(stream, partition),
            #[cfg(test)]
            Self::Fake(runtime) => runtime.is_local_partition_replica(stream, partition),
        }
    }

    pub(super) fn has_delta_stream(&self) -> bool {
        matches!(self, Self::Real(_))
    }

    pub(super) fn enforce_retention(&self, now_ms: u64) -> Result<()> {
        match self {
            Self::Real(runtime) => runtime.enforce_retention(now_ms),
            #[cfg(test)]
            Self::Fake(_) => Ok(()),
        }
    }

    pub(super) async fn replicate_partition(
        &self,
        envelope: MessageEnvelope,
        fsync: bool,
        cluster_durable: bool,
    ) -> Result<MessageEnvelope> {
        match self {
            Self::Real(runtime) => {
                runtime
                    .replicate_partition(envelope, fsync, cluster_durable)
                    .await
            }
            #[cfg(test)]
            Self::Fake(runtime) => {
                runtime
                    .replicate_partition(envelope, fsync, cluster_durable)
                    .await
            }
        }
    }

    pub(super) async fn replicate_partition_batch(
        &self,
        envelopes: Vec<MessageEnvelope>,
        fsync: bool,
        cluster_durable: bool,
    ) -> Result<Vec<MessageEnvelope>> {
        match self {
            Self::Real(runtime) => {
                runtime
                    .replicate_partition_batch(envelopes, fsync, cluster_durable)
                    .await
            }
            #[cfg(test)]
            Self::Fake(runtime) => {
                let mut replicated = Vec::with_capacity(envelopes.len());
                for envelope in envelopes {
                    replicated.push(
                        runtime
                            .replicate_partition(envelope, fsync, cluster_durable)
                            .await?,
                    );
                }
                Ok(replicated)
            }
        }
    }

    pub(super) fn partition_ingress_metrics(
        &self,
    ) -> Option<crate::raft::partition_runtime::PartitionIngressMetricsSnapshot> {
        match self {
            Self::Real(runtime) => Some(runtime.partition_ingress_metrics()),
            #[cfg(test)]
            Self::Fake(_) => None,
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

    pub(super) fn partition_assignment_count(&self) -> usize {
        match self {
            Self::Real(runtime) => runtime.partition_assignment_count(),
            #[cfg(test)]
            Self::Fake(runtime) => runtime.durable_state().partition_assignments.len(),
        }
    }

    pub(super) async fn quorum_available(&self) -> bool {
        match self {
            Self::Real(runtime) => runtime.quorum_available().await,
            #[cfg(test)]
            Self::Fake(runtime) => runtime.quorum_available(),
        }
    }

    pub(super) async fn ensure_metadata_ready(&self) -> Result<()> {
        match self {
            Self::Real(runtime) => runtime.ensure_metadata_ready().await,
            #[cfg(test)]
            Self::Fake(_) => Ok(()),
        }
    }

    pub(super) async fn leader_client_addr(&self) -> Option<String> {
        match self {
            Self::Real(runtime) => runtime.leader_client_addr().await,
            #[cfg(test)]
            Self::Fake(runtime) => runtime
                .leader_client_addr()
                .await
                .map(|address| address.to_string()),
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
