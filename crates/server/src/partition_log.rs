use crate::{
    error::Result,
    stream::{PartitionId, RetentionPolicy, StreamCatalog, StreamDefinition, StreamId},
};
use std::io::{Cursor, Read};
use std::{collections::HashMap, path::Path};

mod codec;
mod dynamic;
mod log;
mod set;
mod subject_index;

pub use self::set::PartitionLogSet;

pub const DEFAULT_NAMESPACE: &str = "default";

pub(crate) fn committed_envelope_checksum(envelope: &MessageEnvelope) -> Result<u32> {
    codec::envelope_checksum(envelope)
}

pub(crate) fn read_segment_offset(bytes: &[u8], target: u64) -> Result<Option<MessageEnvelope>> {
    let mut cursor = Cursor::new(bytes);
    let mut header = vec![0; codec::SEGMENT_HEADER.len()];
    cursor.read_exact(&mut header)?;
    crate::broker_ensure!(
        header == codec::SEGMENT_HEADER,
        "remote partition segment header is invalid"
    );
    while let Some((envelope, _)) = codec::read_batch(&mut cursor)? {
        if envelope.offset == target {
            return Ok(Some(envelope));
        }
        if envelope.offset > target {
            break;
        }
    }
    Ok(None)
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct MessageHeader {
    pub name: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct MessageEnvelope {
    pub namespace: String,
    pub stream: StreamId,
    pub partition: PartitionId,
    pub offset: u64,
    pub subject: String,
    pub key: Option<Vec<u8>>,
    pub headers: Vec<MessageHeader>,
    pub timestamp_ms: u64,
    pub reply_to: Option<String>,
    /// Immutable registry identifier; routing never deserializes the payload.
    #[serde(default)]
    pub schema_id: Option<u64>,
    pub payload: Vec<u8>,
    pub partitioning_epoch: u64,
    pub leader_epoch: u64,
    pub legacy_seq: u64,
}

impl MessageEnvelope {
    pub(crate) fn into_resident_metadata(mut self) -> Self {
        self.payload.clear();
        self.payload.shrink_to_fit();
        self
    }
}

pub struct AppendRequest<'a> {
    pub namespace: &'a str,
    pub stream: &'a StreamDefinition,
    pub subject: &'a str,
    pub key: Option<&'a [u8]>,
    pub partition_hint: Option<PartitionId>,
    pub headers: &'a [MessageHeader],
    pub timestamp_ms: u64,
    pub reply_to: Option<&'a str>,
    pub payload: &'a [u8],
    pub leader_epoch: u64,
    pub legacy_seq: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartitionPosition {
    pub partition: PartitionId,
    pub offset: u64,
    pub partitioning_epoch: u64,
    pub leader_epoch: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SubjectIndexQuery {
    pub(crate) offsets: Vec<u64>,
    pub(crate) used_index: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RetentionChange {
    pub(crate) stream: String,
    pub(crate) partition: PartitionId,
    pub(crate) earliest_offset: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub(crate) struct PartitionRetentionStatus {
    pub(crate) partition: u32,
    pub(crate) earliest_offset: u64,
    pub(crate) next_offset: u64,
    pub(crate) retained_messages: usize,
    pub(crate) retained_bytes: u64,
    pub(crate) deleted_messages: u64,
    pub(crate) deleted_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub(crate) struct PartitionRecoveryStatus {
    pub(crate) total_partitions: usize,
    pub(crate) completed_partitions: usize,
    pub(crate) records_scanned: usize,
    pub(crate) resident_metadata_bytes: usize,
    pub(crate) elapsed_ms: u64,
    pub(crate) workers: usize,
}

pub fn select_partition(
    stream: &StreamDefinition,
    subject: &str,
    key: Option<&[u8]>,
    sticky_value: u64,
) -> PartitionId {
    select_partition_with_count(stream, subject, key, sticky_value, stream.partitions)
}

pub fn select_partition_with_count(
    stream: &StreamDefinition,
    subject: &str,
    key: Option<&[u8]>,
    sticky_value: u64,
    partition_count: u32,
) -> PartitionId {
    assert!(
        partition_count > 0,
        "partition count must be greater than zero"
    );
    let value = key
        .map(stable_hash)
        .unwrap_or_else(|| match &stream.partitioning.strategy {
            crate::stream::PartitioningStrategy::SubjectToken { token } => subject
                .split('/')
                .nth(*token as usize)
                .map(|value| stable_hash(value.as_bytes()))
                .unwrap_or_else(|| fallback_hash(stream, subject, sticky_value)),
            crate::stream::PartitioningStrategy::Key => {
                fallback_hash(stream, subject, sticky_value)
            }
        });
    PartitionId((value % u64::from(partition_count)) as u32)
}

fn fallback_hash(stream: &StreamDefinition, subject: &str, sticky_value: u64) -> u64 {
    match stream.partitioning.fallback {
        crate::stream::PartitionFallback::Sticky => sticky_value,
        crate::stream::PartitionFallback::SubjectHash => stable_hash(subject.as_bytes()),
    }
}

fn stable_hash(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf29ce484222325, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3)
    })
}

#[cfg(test)]
#[path = "partition_log/dynamic_tests.rs"]
mod dynamic_tests;
#[cfg(test)]
#[path = "partition_log/tests.rs"]
mod tests;
