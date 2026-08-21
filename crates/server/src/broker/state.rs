use super::*;

pub(super) struct Inner {
    pub(super) wal: Wal,
    pub(super) partition_logs: crate::partition_log::PartitionLogSet,
    pub(super) clients: HashMap<u64, Client>,
    pub(super) consumers: HashMap<String, Consumer>,
    pub(super) transient_subscriptions: HashMap<(u64, String), TransientSubscription>,
    pub(super) messages: HashMap<u64, PublishRecord>,
}

pub(super) struct Client {
    pub(super) sender: mpsc::Sender<Vec<u8>>,
    pub(super) remote_addr: Option<SocketAddr>,
    pub(super) connected_at_ms: u64,
    pub(super) configured: bool,
    pub(super) verbose: bool,
    pub(super) durable_id: Option<String>,
    pub(super) authenticated: bool,
    pub(super) auth_nonce: Option<String>,
    pub(super) ack_timeout_ms: u64,
    pub(super) max_in_flight: usize,
}

#[derive(Debug, Clone)]
pub(super) struct Consumer {
    pub(super) record: ConsumerRecord,
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
    pub(super) sender: mpsc::Sender<Vec<u8>>,
    pub(super) frame: Vec<u8>,
}

#[derive(Debug, serde::Serialize)]
pub(super) struct ClusterResponse {
    pub(super) cluster_size: usize,
    pub(super) cluster_status: &'static str,
    pub(super) node_id: Option<u64>,
    pub(super) role: &'static str,
    pub(super) leader_id: Option<u64>,
    pub(super) peers: Vec<ClusterPeerResponse>,
    pub(super) routes: Option<RouteTopologyResponse>,
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
pub(super) struct ConnectionResponse {
    pub(super) id: u64,
    pub(super) remote_addr: Option<String>,
    pub(super) durable_id: Option<String>,
    pub(super) authenticated: bool,
    pub(super) verbose: bool,
    pub(super) connected_at_ms: u64,
    pub(super) ack_timeout_ms: u64,
    pub(super) max_in_flight: usize,
    pub(super) subscriptions: usize,
    pub(super) transient_subscriptions: usize,
}

#[derive(Debug, serde::Serialize)]
pub(super) struct SubscriptionsResponse {
    pub(super) durable_consumers: Vec<DurableConsumerResponse>,
    pub(super) transient_subscriptions: Vec<TransientSubscriptionResponse>,
}

#[derive(Debug, serde::Serialize)]
pub(super) struct StreamsResponse<'a> {
    pub(super) streams: &'a [crate::stream::StreamDefinition],
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
    pub(super) delivered: usize,
    pub(super) ack_timeout_ms: u64,
    pub(super) max_in_flight: usize,
}

#[derive(Debug, serde::Serialize)]
pub(super) struct ConsumerMemberResponse {
    pub(super) connection_id: u64,
    pub(super) sid: String,
    pub(super) remaining_deliveries: Option<usize>,
}

#[derive(Debug, serde::Serialize)]
pub(super) struct TransientSubscriptionResponse {
    pub(super) connection_id: u64,
    pub(super) sid: String,
    pub(super) subject: String,
    pub(super) remaining_deliveries: Option<usize>,
}
