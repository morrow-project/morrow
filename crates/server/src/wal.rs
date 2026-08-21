use crate::error::{BrokerError, Result, ResultExt};
use protocol::subject;
use std::{
    collections::{BTreeSet, HashMap, HashSet},
    fs::{File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};
const WAL_FILE: &str = "broker.wal";
const LEGACY_WAL_FILE: &str = "broker.wal.legacy";
const SEGMENT_EXTENSION: &str = "wal";
const SEGMENT_TMP_EXTENSION: &str = "wal.tmp";
const SEGMENT_HEADER: &[u8] = b"BROKERWAL\x01\n";
const SEGMENT_HEADER_LEN: u64 = SEGMENT_HEADER.len() as u64;
const KIND_PUBLISH: u8 = 1;
const KIND_CONSUMER_UPSERT: u8 = 2;
const KIND_DELIVERY_ATTEMPT: u8 = 3;
const KIND_ACK: u8 = 4;
pub const DEFAULT_WAL_SEGMENT_BYTES: u64 = 64 * 1024 * 1024;
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct PublishRecord {
    pub seq: u64,
    pub stream: Option<String>,
    pub subject: String,
    pub reply_to: Option<String>,
    pub payload: Vec<u8>,
}
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct ConsumerRecord {
    pub consumer_id: String,
    pub filter_subject: String,
    pub queue_group: Option<String>,
    pub ack_timeout_ms: u64,
    pub max_in_flight: usize,
}
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct DeliveryAttemptRecord {
    pub seq: u64,
    pub consumer_id: String,
    pub delivery_id: u64,
    pub deadline_ms: u64,
    pub attempt: u32,
}
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct AckRecord {
    pub seq: u64,
    pub consumer_id: String,
    pub delivery_id: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct ReplayedConsumer {
    pub record: ConsumerRecord,
    pub pending: BTreeSet<u64>,
    pub in_flight: HashMap<u64, DeliveryAttemptRecord>,
    pub acked: HashSet<u64>,
}
#[derive(Debug)]
pub struct Replay {
    pub messages: HashMap<u64, PublishRecord>,
    pub consumers: HashMap<String, ReplayedConsumer>,
    pub next_seq: u64,
    pub next_delivery_id: u64,
    pub duration_ms: u64,
    pub truncations: u64,
}
#[derive(Debug)]
pub struct Wal {
    pub(super) file: File,
    pub(super) dir: PathBuf,
    pub(super) active_segment_id: u64,
    pub(super) active_path: PathBuf,
    pub(super) active_bytes: u64,
    pub(super) sealed_segments: Vec<SegmentInfo>,
    pub(super) segment_bytes: u64,
    pub(super) next_seq: u64,
    pub(super) next_delivery_id: u64,
    pub(super) fsync_interval: Duration,
    pub(super) last_sync: Instant,
    pub(super) metrics: WalMetrics,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentInfo {
    pub id: u64,
    pub path: PathBuf,
    pub bytes: u64,
}

#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct WalMetrics {
    pub last_replay_duration_ms: u64,
    pub last_checkpoint_duration_ms: u64,
    pub last_fsync_duration_ms: u64,
    pub rotations: u64,
    pub checkpoints: u64,
    pub truncations: u64,
    pub deleted_segments: u64,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct WalStatus {
    pub active_segment_id: u64,
    pub active_segment_path: String,
    pub active_segment_bytes: u64,
    pub sealed_segment_count: usize,
    pub total_wal_bytes: u64,
    pub retained_message_count: usize,
    pub consumer_count: usize,
    pub next_seq: u64,
    pub next_delivery_id: u64,
    pub last_replay_duration_ms: u64,
    pub last_checkpoint_duration_ms: u64,
    pub last_fsync_duration_ms: u64,
    pub rotations: u64,
    pub checkpoints: u64,
    pub truncations: u64,
    pub deleted_segments: u64,
}

mod cursor;
mod record_io;
mod replay;
mod wal;

use self::{cursor::*, record_io::*, replay::*};

#[cfg(test)]
mod tests;
