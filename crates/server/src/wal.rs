use crate::error::{BrokerError, Result, ResultExt};
use protocol::subject;
use std::{
    collections::{BTreeSet, HashMap, HashSet},
    fs::{File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};
const WAL_FILE: &str = "morrow.wal";
const LEGACY_WAL_FILE: &str = "morrow.wal.legacy";
const SEGMENT_EXTENSION: &str = "wal";
const SEGMENT_TMP_EXTENSION: &str = "wal.tmp";
const SEGMENT_HEADER: &[u8] = b"BROKERWAL\x01\n";
const SEGMENT_HEADER_LEN: u64 = SEGMENT_HEADER.len() as u64;
const KIND_PUBLISH: u8 = 1;
const KIND_CONSUMER_UPSERT: u8 = 2;
const KIND_DELIVERY_ATTEMPT: u8 = 3;
const KIND_ACK: u8 = 4;
const KIND_PARTITION_APPEND: u8 = 5;
const KIND_CONSUMER_CURSOR: u8 = 6;
const KIND_CONSUMER_DELETE: u8 = 7;
const KIND_DEAD_LETTER: u8 = 8;
const KIND_DEAD_LETTER_PURGE: u8 = 9;
const KIND_PRODUCER_SEQUENCE: u8 = 10;
const KIND_GROUP_STATE: u8 = 11;
const KIND_CONSUMER_CURSOR_DELTA: u8 = 12;
pub(super) const ENCRYPTED_BODY_MAGIC: &[u8] = b"MORROW-WAL-ENC1\n";
pub const DEFAULT_WAL_SEGMENT_BYTES: u64 = 64 * 1024 * 1024;
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct PublishRecord {
    pub seq: u64,
    #[serde(default)]
    pub namespace: String,
    pub stream: Option<String>,
    #[serde(default)]
    pub partition: Option<u32>,
    #[serde(default)]
    pub offset: Option<u64>,
    pub subject: String,
    #[serde(default)]
    pub key: Option<Vec<u8>>,
    #[serde(default)]
    pub headers: Vec<crate::partition_log::MessageHeader>,
    #[serde(default)]
    pub timestamp_ms: u64,
    pub reply_to: Option<String>,
    pub payload: Vec<u8>,
    #[serde(default)]
    pub partitioning_epoch: u64,
    #[serde(default)]
    pub leader_epoch: u64,
}

impl PublishRecord {
    pub(crate) fn into_resident_metadata(mut self) -> Self {
        if self.stream.is_some() {
            self.payload.clear();
            self.payload.shrink_to_fit();
        }
        self
    }
}

impl From<crate::partition_log::MessageEnvelope> for PublishRecord {
    fn from(envelope: crate::partition_log::MessageEnvelope) -> Self {
        Self {
            seq: envelope.legacy_seq,
            namespace: envelope.namespace,
            stream: Some(envelope.stream.as_str().to_string()),
            partition: Some(envelope.partition.0),
            offset: Some(envelope.offset),
            subject: envelope.subject,
            key: envelope.key,
            headers: envelope.headers,
            timestamp_ms: envelope.timestamp_ms,
            reply_to: envelope.reply_to,
            payload: envelope.payload,
            partitioning_epoch: envelope.partitioning_epoch,
            leader_epoch: envelope.leader_epoch,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct ConsumerRecord {
    pub consumer_id: String,
    pub filter_subject: String,
    pub queue_group: Option<String>,
    pub ack_timeout_ms: u64,
    pub max_in_flight: usize,
    #[serde(default)]
    pub start_position: protocol::StartPosition,
    #[serde(default)]
    pub retry_policy: protocol::RetryPolicy,
}
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct DeliveryAttemptRecord {
    pub seq: u64,
    pub consumer_id: String,
    pub delivery_id: u64,
    pub deadline_ms: u64,
    pub attempt: u32,
    #[serde(default)]
    pub retry_waiting: bool,
}
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct AckRecord {
    pub seq: u64,
    pub consumer_id: String,
    pub delivery_id: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct DeadLetterRecord {
    pub id: u64,
    pub source_seq: u64,
    pub consumer_id: String,
    pub source_stream: Option<String>,
    pub source_partition: Option<u32>,
    pub source_offset: Option<u64>,
    pub reason: String,
    pub attempt_count: u32,
    pub first_delivery_ms: u64,
    pub last_delivery_ms: u64,
    pub payload: Vec<u8>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeadLetterPurgeRecord {
    pub id: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct ProducerSequenceRecord {
    pub producer_id: String,
    pub epoch: u64,
    pub sequence: u64,
    pub fingerprint: u64,
    pub record: PublishRecord,
}
pub type GroupStateRecord = (String, crate::consumer_group::GroupRecord);
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionAppendRecord {
    pub seq: u64,
    pub stream: String,
    pub partition: u32,
    pub offset: u64,
    pub subject: String,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerCursorRecord {
    pub consumer_id: String,
    pub cursors: crate::consumer_cursor::ConsumerCursorSet,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerCursorDeltaRecord {
    pub consumer_id: String,
    pub cursor: crate::consumer_cursor::PartitionCursor,
    pub ack_window: usize,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerDeleteRecord {
    pub consumer_id: String,
}
impl From<&crate::partition_log::MessageEnvelope> for PartitionAppendRecord {
    fn from(envelope: &crate::partition_log::MessageEnvelope) -> Self {
        Self {
            seq: envelope.legacy_seq,
            stream: envelope.stream.as_str().to_string(),
            partition: envelope.partition.0,
            offset: envelope.offset,
            subject: envelope.subject.clone(),
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct ReplayedConsumer {
    pub record: ConsumerRecord,
    pub cursors: Option<crate::consumer_cursor::ConsumerCursorSet>,
    pub pending: BTreeSet<u64>,
    pub pending_attempts: HashMap<u64, u32>,
    pub in_flight: HashMap<u64, DeliveryAttemptRecord>,
    pub acked: HashSet<u64>,
}
#[derive(Debug)]
pub struct Replay {
    pub messages: HashMap<u64, PublishRecord>,
    pub partition_appends: HashMap<u64, PartitionAppendRecord>,
    pub consumers: HashMap<String, ReplayedConsumer>,
    pub dead_letters: HashMap<u64, DeadLetterRecord>,
    pub producer_sequences: HashMap<(String, u64, u64), ProducerSequenceRecord>,
    pub groups: HashMap<String, crate::consumer_group::GroupRecord>,
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
    pub(super) encryption: Option<Arc<crate::encryption::KeyRing>>,
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
    pub partition_append_batches: u64,
    pub partition_append_records: u64,
    pub partition_append_bytes: u64,
    pub flushes: u64,
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
    pub partition_append_batches: u64,
    pub partition_append_records: u64,
    pub partition_append_bytes: u64,
    pub flushes: u64,
}

mod cursor;
mod record_io;
mod replay;
mod wal;

use self::{cursor::*, record_io::*, replay::*};

#[cfg(test)]
mod tests;
