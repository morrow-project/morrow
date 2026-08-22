use super::*;

pub(super) struct DurableBrokerState {
    pub(super) wal: WalRuntime,
    pub(super) consumers: HashMap<String, Consumer>,
    pub(super) consumer_interest_index: subject::SubjectTrie<String>,
    pub(super) messages: HashMap<u64, PublishRecord>,
    pub(super) partition_sequences: HashMap<(String, u32, u64), u64>,
}

pub(super) struct ConnectionState {
    pub(super) clients: HashMap<u64, Client>,
}

pub(super) struct TransientState {
    pub(super) subscriptions: HashMap<(u64, String), TransientSubscription>,
    pub(super) interest_index: subject::SubjectTrie<(u64, String)>,
}

#[derive(Clone)]
pub(super) struct Client {
    pub(super) sender: OutboundQueue,
    pub(super) remote_addr: Option<SocketAddr>,
    pub(super) connected_at_ms: u64,
    pub(super) configured: bool,
    pub(super) verbose: bool,
    pub(super) durable_id: Option<String>,
    pub(super) authenticated: bool,
    pub(super) auth_nonce: Option<String>,
    pub(super) ack_timeout_ms: u64,
    pub(super) max_in_flight: usize,
    pub(super) protocol_version: u32,
}

#[derive(Debug, Clone)]
pub(super) struct Consumer {
    pub(super) record: ConsumerRecord,
    pub(super) cursors: crate::consumer_cursor::ConsumerCursorSet,
    pub(super) members: HashMap<u64, SubscriptionMember>,
    pub(super) pending: BTreeSet<u64>,
    pub(super) pending_attempts: HashMap<u64, u32>,
    pub(super) in_flight: HashMap<u64, InFlight>,
    pub(super) acked: HashSet<u64>,
    pub(super) delivered: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SubscriptionMember {
    pub(super) sid: String,
    pub(super) remaining_deliveries: Option<usize>,
    pub(super) credit_messages: usize,
    pub(super) credit_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct InFlight {
    pub(super) delivery_id: u64,
    pub(super) deadline_ms: u64,
    pub(super) attempt: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct TransientSubscription {
    pub(super) subject: String,
    pub(super) sid: String,
    pub(super) remaining_deliveries: Option<usize>,
}

pub(super) struct Delivery {
    pub(super) sender: OutboundQueue,
    pub(super) frame: Vec<u8>,
}

#[derive(Clone)]
pub(super) struct OutboundQueue {
    sender: mpsc::Sender<OutboundFrame>,
    queued_bytes: Arc<AtomicUsize>,
    limit: usize,
    quotas: Arc<crate::quota::QuotaRuntime>,
}

pub(super) struct OutboundFrame {
    bytes: Vec<u8>,
    queued_bytes: Arc<AtomicUsize>,
}

impl OutboundQueue {
    pub(super) fn new(
        sender: mpsc::Sender<OutboundFrame>,
        limit: usize,
        quotas: Arc<crate::quota::QuotaRuntime>,
    ) -> Self {
        Self {
            sender,
            queued_bytes: Arc::new(AtomicUsize::new(0)),
            limit,
            quotas,
        }
    }

    pub(super) async fn send(&self, bytes: Vec<u8>) -> std::result::Result<(), ()> {
        let len = bytes.len();
        let reserved =
            self.queued_bytes
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |queued| {
                    queued.checked_add(len).filter(|next| *next <= self.limit)
                });
        if reserved.is_err() {
            self.quotas.reject_outbound();
            return Err(());
        }
        let frame = OutboundFrame {
            bytes,
            queued_bytes: self.queued_bytes.clone(),
        };
        if self.sender.try_send(frame).is_err() {
            self.quotas.reject_outbound();
            return Err(());
        }
        Ok(())
    }
}

impl OutboundFrame {
    pub(super) fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }
}

impl Drop for OutboundFrame {
    fn drop(&mut self) {
        self.queued_bytes
            .fetch_sub(self.bytes.len(), Ordering::Relaxed);
    }
}

#[derive(Debug, serde::Serialize)]
pub(super) struct ClusterResponse {
    pub(super) cluster_size: usize,
    pub(super) cluster_status: &'static str,
    pub(super) node_id: Option<u64>,
    pub(super) role: &'static str,
    pub(super) leader_id: Option<u64>,
    pub(super) peers: Vec<ClusterPeerResponse>,
    pub(super) partitions: Vec<PartitionLeaderResponse>,
    pub(super) routes: Option<RouteTopologyResponse>,
}

#[derive(Debug, serde::Serialize)]
pub(super) struct PartitionLeaderResponse {
    pub(super) stream: String,
    pub(super) partition: u32,
    pub(super) replicas: Vec<u64>,
    pub(super) leader_id: u64,
    pub(super) leader_client_addr: Option<String>,
    pub(super) leader_epoch: u64,
    pub(super) high_watermark: Option<u64>,
}

#[derive(Debug, serde::Serialize)]
pub(super) struct ClusterPeerResponse {
    pub(super) node_id: u64,
    pub(super) client_addr: String,
    pub(super) raft_addr: String,
    pub(super) is_self: bool,
    pub(super) is_leader: bool,
}

#[derive(Debug, serde::Serialize)]
pub(super) struct ConnectionsResponse {
    pub(super) count: usize,
    pub(super) connections: Vec<ConnectionResponse>,
}

#[derive(Debug, serde::Serialize)]
pub(super) struct QuotasResponse {
    pub(super) sockets: crate::quota::QuotaSnapshot,
    pub(super) transient_subscriptions: StateQuotaUsage,
    pub(super) durable_consumers: StateQuotaUsage,
    pub(super) outbound_bytes_per_connection_limit: usize,
}

#[derive(Debug, serde::Serialize)]
pub(super) struct StateQuotaUsage {
    pub(super) used: usize,
    pub(super) limit: usize,
}

#[derive(Debug, serde::Serialize)]
pub(super) struct ConnectionResponse {
    pub(super) id: u64,
    pub(super) remote_addr: Option<String>,
    pub(super) durable_id: Option<String>,
    pub(super) authenticated: bool,
    pub(super) verbose: bool,
    pub(super) connected_at_ms: u64,
    pub(super) ack_timeout_ms: u64,
    pub(super) max_in_flight: usize,
    pub(super) protocol_version: u32,
    pub(super) subscriptions: usize,
    pub(super) transient_subscriptions: usize,
}

#[derive(Debug, serde::Serialize)]
pub(super) struct SubscriptionsResponse {
    pub(super) durable_consumers: Vec<DurableConsumerResponse>,
    pub(super) transient_subscriptions: Vec<TransientSubscriptionResponse>,
}

#[derive(Debug, serde::Serialize)]
pub(super) struct StreamsResponse {
    pub(super) streams: Vec<StreamResponse>,
}

#[derive(Debug, serde::Serialize)]
pub(super) struct StreamResponse {
    #[serde(flatten)]
    pub(super) definition: crate::stream::StreamDefinition,
    pub(super) retained_messages: usize,
    pub(super) retained_bytes: u64,
    pub(super) partition_status: Vec<crate::partition_log::PartitionRetentionStatus>,
}

#[derive(Debug, serde::Serialize)]
pub(super) struct DurableConsumerResponse {
    pub(super) consumer_id: String,
    pub(super) filter_subject: String,
    pub(super) queue_group: Option<String>,
    pub(super) members: Vec<ConsumerMemberResponse>,
    pub(super) pending: usize,
    pub(super) in_flight: usize,
    pub(super) acked: usize,
    pub(super) cursors: Vec<PartitionCursorResponse>,
    pub(super) delivered: usize,
    pub(super) ack_timeout_ms: u64,
    pub(super) max_in_flight: usize,
}

#[derive(Debug, serde::Serialize)]
pub(super) struct PartitionCursorResponse {
    pub(super) stream: String,
    pub(super) partition: u32,
    pub(super) committed_offset: u64,
    pub(super) delivered_offset: Option<u64>,
    pub(super) acknowledged_out_of_order: usize,
    pub(super) retention_gaps: u64,
}

#[derive(Debug, serde::Serialize)]
pub(super) struct ConsumerMemberResponse {
    pub(super) connection_id: u64,
    pub(super) sid: String,
    pub(super) remaining_deliveries: Option<usize>,
    pub(super) credit_messages: usize,
    pub(super) credit_bytes: usize,
}

#[derive(Debug, serde::Serialize)]
pub(super) struct TransientSubscriptionResponse {
    pub(super) connection_id: u64,
    pub(super) sid: String,
    pub(super) subject: String,
    pub(super) remaining_deliveries: Option<usize>,
}
