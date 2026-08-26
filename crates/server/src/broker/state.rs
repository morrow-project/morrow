use super::*;
use std::hash::{Hash, Hasher};

const MAX_PRODUCER_SEQUENCES_PER_PRODUCER: usize = 4_096;

pub(super) struct DurableBrokerState {
    pub(super) wal: WalRuntime,
    pub(super) consumers: HashMap<String, Consumer>,
    pub(super) consumer_interest_index: subject::SubjectTrie<String>,
    pub(super) messages: HashMap<u64, PublishRecord>,
    pub(super) partition_sequences: BTreeMap<(String, u32, u64), u64>,
    pub(super) ready_consumers: BTreeSet<String>,
    pub(super) lease_deadlines: BinaryHeap<Reverse<LeaseDeadline>>,
    pub(super) scheduled_deliveries: BinaryHeap<Reverse<ScheduledDelivery>>,
    pub(super) dead_letters: BTreeMap<u64, DeadLetterRecord>,
    pub(super) compaction_latest: HashMap<CompactionKey, (u64, u64)>,
    pub(super) superseded_since_compaction: usize,
    pub(super) producer_epochs: HashMap<String, u64>,
    pub(super) producer_sequences: HashMap<(String, u64, u64), ProducerDedupEntry>,
    pub(super) producer_in_flight: HashSet<(String, u64, u64)>,
}

#[derive(Debug, Clone)]
pub(super) struct ProducerDedupEntry {
    pub(super) fingerprint: u64,
    pub(super) record: PublishRecord,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ProducerSequenceDecision {
    New,
    Duplicate,
}

pub(super) fn producer_fingerprint(
    subject: &str,
    key: Option<&[u8]>,
    headers: &[(String, String)],
    payload: &[u8],
) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    subject.hash(&mut hasher);
    key.hash(&mut hasher);
    headers.hash(&mut hasher);
    payload.hash(&mut hasher);
    hasher.finish()
}

impl DurableBrokerState {
    pub(super) fn ready_connection_ids(&self) -> HashSet<u64> {
        self.ready_consumers
            .iter()
            .flat_map(|consumer_id| {
                self.consumers
                    .get(consumer_id)
                    .into_iter()
                    .flat_map(|consumer| consumer.members.keys().copied())
            })
            .collect()
    }
    pub(super) fn begin_producer_sequence(
        &mut self,
        producer: &protocol::ProducerSequence,
        fingerprint: u64,
    ) -> Result<(ProducerSequenceDecision, Option<PublishRecord>)> {
        if let Some(entry) = self.producer_sequences.get(&(
            producer.producer_id.clone(),
            producer.epoch,
            producer.sequence,
        )) {
            crate::broker_ensure!(
                entry.fingerprint == fingerprint,
                "producer sequence was reused with different content"
            );
            return Ok((
                ProducerSequenceDecision::Duplicate,
                Some(entry.record.clone()),
            ));
        }
        let current_epoch = self
            .producer_epochs
            .get(&producer.producer_id)
            .copied()
            .unwrap_or(producer.epoch);
        crate::broker_ensure!(
            producer.epoch >= current_epoch,
            "producer epoch is stale and has been fenced"
        );
        if producer.epoch > current_epoch {
            self.producer_epochs
                .insert(producer.producer_id.clone(), producer.epoch);
            self.producer_sequences
                .retain(|(producer_id, _, _), _| producer_id != &producer.producer_id);
        }
        crate::broker_ensure!(
            !self
                .producer_in_flight
                .iter()
                .any(|(producer_id, epoch, sequence)| {
                    producer_id == &producer.producer_id
                        && *epoch == producer.epoch
                        && *sequence != producer.sequence
                }),
            "producer has another sequence in progress"
        );
        let last_sequence = self
            .producer_sequences
            .keys()
            .filter(|(producer_id, epoch, _)| {
                producer_id == &producer.producer_id && *epoch == producer.epoch
            })
            .map(|(_, _, sequence)| *sequence)
            .max();
        match last_sequence {
            None => crate::broker_ensure!(producer.sequence <= 1, "producer sequence gap"),
            Some(last) => {
                if producer.sequence <= last {
                    crate::broker_ensure!(
                        producer.sequence == last.saturating_add(1),
                        "producer sequence is outside the deduplication frontier"
                    );
                } else {
                    crate::broker_ensure!(
                        producer.sequence == last.saturating_add(1),
                        "producer sequence gap"
                    );
                }
            }
        }
        let identity = (
            producer.producer_id.clone(),
            producer.epoch,
            producer.sequence,
        );
        crate::broker_ensure!(
            self.producer_in_flight.insert(identity),
            "producer sequence is already in progress"
        );
        Ok((ProducerSequenceDecision::New, None))
    }

    pub(super) fn complete_producer_sequence(
        &mut self,
        producer: &protocol::ProducerSequence,
        fingerprint: u64,
        record: PublishRecord,
    ) {
        let identity = (
            producer.producer_id.clone(),
            producer.epoch,
            producer.sequence,
        );
        self.producer_in_flight.remove(&identity);
        self.producer_epochs
            .insert(producer.producer_id.clone(), producer.epoch);
        let producer_count = self
            .producer_sequences
            .keys()
            .filter(|(producer_id, epoch, _)| {
                producer_id == &producer.producer_id && *epoch == producer.epoch
            })
            .count();
        if producer_count >= MAX_PRODUCER_SEQUENCES_PER_PRODUCER {
            if let Some(oldest) = self
                .producer_sequences
                .keys()
                .filter(|(producer_id, epoch, _)| {
                    producer_id == &producer.producer_id && *epoch == producer.epoch
                })
                .min_by_key(|(_, _, sequence)| *sequence)
                .cloned()
            {
                self.producer_sequences.remove(&oldest);
            }
        }
        self.producer_sequences.insert(
            identity,
            ProducerDedupEntry {
                fingerprint,
                record,
            },
        );
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct LeaseDeadline {
    pub(super) deadline_ms: u64,
    pub(super) consumer_id: String,
    pub(super) seq: u64,
    pub(super) delivery_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct ScheduledDelivery {
    pub(super) scheduled_at_ms: u64,
    pub(super) seq: u64,
}

pub(super) struct ConnectionState {
    pub(super) clients: HashMap<u64, Client>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct GroupMemberSession {
    pub(super) group: String,
    pub(super) member: String,
    pub(super) generation: u64,
}

pub(super) struct TransientState {
    pub(super) subscriptions: HashMap<(u64, String), TransientSubscription>,
    pub(super) interest_index: subject::SubjectTrie<(u64, String)>,
    pub(super) route_interest_counts: BTreeMap<String, usize>,
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
    pub(super) ack_contract_version: Option<u16>,
    pub(super) quota_tenant: String,
    pub(super) quota_usage: crate::quota::TenantQuotaUsage,
}

#[derive(Debug, Clone)]
pub(super) struct Consumer {
    pub(super) record: ConsumerRecord,
    pub(super) cursors: crate::consumer_cursor::ConsumerCursorSet,
    pub(super) members: HashMap<u64, SubscriptionMember>,
    pub(super) pending: BTreeSet<u64>,
    pub(super) pending_attempts: HashMap<u64, u32>,
    pub(super) preparing: HashSet<u64>,
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
    pub(super) retry_waiting: bool,
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
    pub(super) state_application: ClusterStateApplicationResponse,
}

#[derive(Debug, serde::Serialize)]
pub(super) struct HealthResponse {
    pub(super) status: &'static str,
    pub(super) cluster_status: &'static str,
    pub(super) role: &'static str,
    pub(super) reason: Option<&'static str>,
}

#[derive(Debug, serde::Serialize)]
pub(super) struct MiddlewareResponse {
    pub(super) current_generation: u64,
}

#[derive(Debug, serde::Serialize)]
pub(super) struct ConnectorsResponse {
    pub(super) count: usize,
    pub(super) connectors: Vec<ConnectorResponse>,
}

#[derive(Debug, serde::Serialize)]
pub(super) struct ConnectorResponse {
    pub(super) connection_id: u64,
    pub(super) durable_id: String,
    pub(super) status: &'static str,
    pub(super) authenticated: bool,
    pub(super) connected_at_ms: u64,
    pub(super) protocol_version: u32,
}

#[derive(Debug, serde::Serialize)]
pub(super) struct DeadLettersResponse {
    pub(super) count: usize,
    pub(super) total_count: usize,
    pub(super) next_offset: Option<usize>,
    pub(super) records: Vec<DeadLetterResponse>,
}

#[derive(Debug, serde::Serialize)]
pub(super) struct DeadLetterResponse {
    pub(super) id: u64,
    pub(super) source_seq: u64,
    pub(super) consumer_id: String,
    pub(super) source_stream: Option<String>,
    pub(super) source_partition: Option<u32>,
    pub(super) source_offset: Option<u64>,
    pub(super) reason: String,
    pub(super) attempt_count: u32,
    pub(super) first_delivery_ms: u64,
    pub(super) last_delivery_ms: u64,
    pub(super) payload_bytes: usize,
}

#[derive(Debug, serde::Serialize)]
pub(super) struct ProducersResponse {
    pub(super) count: usize,
    pub(super) total_count: usize,
    pub(super) next_offset: Option<usize>,
    pub(super) producers: Vec<ProducerResponse>,
}

#[derive(Debug, serde::Serialize)]
pub(super) struct ProducerResponse {
    pub(super) producer_id: String,
    pub(super) epoch: u64,
    pub(super) dedup_entries: usize,
}

#[derive(Debug, Default)]
pub(super) struct ClusterApplicationMetrics {
    pub(super) delta_applications: AtomicU64,
    pub(super) full_reconciliations: AtomicU64,
}

#[derive(Debug, Default)]
pub(super) struct BrokerMetrics {
    pub(super) websocket_connections: AtomicU64,
    pub(super) websocket_connections_total: AtomicU64,
    pub(super) websocket_messages_received_total: AtomicU64,
    pub(super) websocket_messages_sent_total: AtomicU64,
    pub(super) websocket_bytes_received_total: AtomicU64,
    pub(super) websocket_bytes_sent_total: AtomicU64,
    pub(super) websocket_errors_total: AtomicU64,
    pub(super) publishes_total: AtomicU64,
    pub(super) published_bytes_total: AtomicU64,
    pub(super) rejected_operations_total: AtomicU64,
    pub(super) partition_reads_total: AtomicU64,
    pub(super) partition_writes_total: AtomicU64,
    pub(super) delivery_attempts_total: AtomicU64,
    pub(super) acknowledgements_total: AtomicU64,
    pub(super) nacks_total: AtomicU64,
    pub(super) redeliveries_total: AtomicU64,
    pub(super) dead_letter_writes_total: AtomicU64,
    pub(super) dead_letter_replay_outcomes_total: AtomicU64,
    pub(super) publish_latency_us: LatencyHistogram,
    pub(super) delivery_latency_us: LatencyHistogram,
}

#[derive(Debug)]
pub(super) struct LatencyHistogram {
    buckets: [AtomicU64; 6],
    count: AtomicU64,
    sum_us: AtomicU64,
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self {
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            count: AtomicU64::new(0),
            sum_us: AtomicU64::new(0),
        }
    }
}

impl LatencyHistogram {
    pub(super) fn observe(&self, duration: Duration) {
        let micros = duration.as_micros().min(u64::MAX as u128) as u64;
        let bucket = match micros {
            0..=9 => 0,
            10..=99 => 1,
            100..=999 => 2,
            1_000..=9_999 => 3,
            10_000..=99_999 => 4,
            _ => 5,
        };
        self.buckets[bucket].fetch_add(1, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum_us.fetch_add(micros, Ordering::Relaxed);
    }

    pub(super) fn snapshot(&self) -> ([u64; 6], u64, u64) {
        (
            std::array::from_fn(|index| self.buckets[index].load(Ordering::Relaxed)),
            self.count.load(Ordering::Relaxed),
            self.sum_us.load(Ordering::Relaxed),
        )
    }
}

#[derive(Debug, serde::Serialize)]
pub(super) struct ClusterStateApplicationResponse {
    pub(super) delta_applications: u64,
    pub(super) full_reconciliations: u64,
}

#[derive(Debug, serde::Serialize)]
pub(super) struct PartitionLeaderResponse {
    pub(super) stream: String,
    pub(super) partition: u32,
    pub(super) replicas: Vec<u64>,
    pub(super) active_commit_set: Vec<u64>,
    pub(super) replica_set_generation: u64,
    pub(super) phase: crate::raft::PartitionReconfigurationPhase,
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
    pub(super) total_count: usize,
    pub(super) next_offset: Option<usize>,
    pub(super) connections: Vec<ConnectionResponse>,
}

#[derive(Debug, serde::Serialize)]
pub(super) struct QuotasResponse {
    pub(super) sockets: crate::quota::QuotaSnapshot,
    pub(super) transient_subscriptions: StateQuotaUsage,
    pub(super) durable_consumers: StateQuotaUsage,
    pub(super) outbound_bytes_per_connection_limit: usize,
    pub(super) tenant_quotas: HashMap<String, crate::quota::TenantQuotaStatus>,
}

#[derive(Debug, serde::Serialize)]
pub(super) struct ViewStatusResponse {
    pub(super) name: String,
    pub(super) tenant: String,
    pub(super) source_stream: String,
    pub(super) paused: bool,
    pub(super) entries: usize,
    pub(super) positions: std::collections::BTreeMap<String, u64>,
}

#[derive(Debug, serde::Serialize)]
pub(super) struct ViewQueryResponse {
    pub(super) name: String,
    pub(super) tenant: String,
    pub(super) key: String,
    pub(super) value: Option<Vec<u8>>,
    pub(super) positions: std::collections::BTreeMap<String, u64>,
}

#[derive(Debug, serde::Deserialize)]
pub(super) struct ViewCreateRequest {
    pub(super) tenant: String,
    pub(super) source_stream: String,
    pub(super) source_subject: Option<String>,
    pub(super) key_header: Option<String>,
    pub(super) max_entries: usize,
    pub(super) max_value_bytes: usize,
    pub(super) watch_capacity: usize,
}

#[derive(Debug, serde::Serialize)]
pub(super) struct ViewWatchResponse {
    pub(super) name: String,
    pub(super) tenant: String,
    pub(super) since: u64,
    pub(super) events: Vec<crate::materialized_view::ViewEvent>,
    pub(super) positions: std::collections::BTreeMap<String, u64>,
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
pub(super) struct SubscriptionsPageResponse {
    pub(super) durable_consumers: Vec<DurableConsumerResponse>,
    pub(super) transient_subscriptions: Vec<TransientSubscriptionResponse>,
    pub(super) durable_total_count: usize,
    pub(super) durable_next_offset: Option<usize>,
    pub(super) transient_total_count: usize,
    pub(super) transient_next_offset: Option<usize>,
}

#[derive(Debug, serde::Serialize)]
pub(super) struct StreamsResponse {
    pub(super) streams: Vec<StreamResponse>,
    pub(super) recovery: crate::partition_log::PartitionRecoveryStatus,
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
