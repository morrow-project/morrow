pub(super) struct ClusterDeliveryCandidate {
    pub(super) consumer_id: String,
    pub(super) seq: u64,
    pub(super) connection_id: u64,
    pub(super) sid: String,
    pub(super) attempt: u32,
    pub(super) deadline_ms: u64,
}
