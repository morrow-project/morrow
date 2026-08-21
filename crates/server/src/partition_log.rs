use crate::{
    error::Result,
    stream::{PartitionId, StreamCatalog, StreamDefinition, StreamId},
};
use std::{collections::HashMap, path::Path};

mod codec;
mod log;
mod set;

pub use self::set::PartitionLogSet;

pub const DEFAULT_NAMESPACE: &str = "default";

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
    pub payload: Vec<u8>,
    pub partitioning_epoch: u64,
    pub leader_epoch: u64,
    pub legacy_seq: u64,
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

pub fn select_partition(
    stream: &StreamDefinition,
    subject: &str,
    key: Option<&[u8]>,
    sticky_value: u64,
) -> PartitionId {
    let value = key
        .map(stable_hash)
        .unwrap_or_else(|| match &stream.partitioning.strategy {
            crate::stream::PartitioningStrategy::SubjectToken { token } => subject
                .split('.')
                .nth(*token as usize)
                .map(|value| stable_hash(value.as_bytes()))
                .unwrap_or_else(|| fallback_hash(stream, subject, sticky_value)),
            crate::stream::PartitioningStrategy::Key => {
                fallback_hash(stream, subject, sticky_value)
            }
        });
    PartitionId((value % u64::from(stream.partitions)) as u32)
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
#[path = "partition_log/tests.rs"]
mod tests;
