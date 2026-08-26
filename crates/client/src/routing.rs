//! Client-side partition leader metadata and deterministic routing.

use std::collections::{HashMap, VecDeque};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Partitioning {
    Key,
    SubjectToken { token: usize },
    Sticky,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionLeader {
    pub partition: u32,
    pub leader_epoch: u64,
    pub address: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamMetadata {
    pub name: String,
    pub partitions: u32,
    pub partitioning_epoch: u64,
    pub partitioning: Partitioning,
    pub leaders: Vec<PartitionLeader>,
}

#[derive(Debug)]
pub struct PartitionLeaderCache {
    max_streams: usize,
    streams: HashMap<String, StreamMetadata>,
    order: VecDeque<String>,
}

impl PartitionLeaderCache {
    pub fn new(max_streams: usize) -> Option<Self> {
        (max_streams > 0).then_some(Self {
            max_streams,
            streams: HashMap::new(),
            order: VecDeque::new(),
        })
    }

    pub fn insert(&mut self, metadata: StreamMetadata) -> bool {
        if metadata.partitions == 0
            || metadata.leaders.len() != metadata.partitions as usize
            || metadata
                .leaders
                .iter()
                .enumerate()
                .any(|(index, leader)| leader.partition != index as u32)
        {
            return false;
        }
        if self
            .streams
            .get(&metadata.name)
            .is_some_and(|current| current.partitioning_epoch > metadata.partitioning_epoch)
        {
            return false;
        }
        let name = metadata.name.clone();
        self.streams.insert(name.clone(), metadata);
        self.order.retain(|entry| entry != &name);
        self.order.push_back(name);
        while self.order.len() > self.max_streams {
            if let Some(evicted) = self.order.pop_front() {
                self.streams.remove(&evicted);
            }
        }
        true
    }

    pub fn route(
        &self,
        stream: &str,
        subject: &str,
        key: Option<&[u8]>,
        sticky: u64,
    ) -> Option<&PartitionLeader> {
        let metadata = self.streams.get(stream)?;
        let value = key
            .map(stable_hash)
            .or_else(|| match metadata.partitioning {
                Partitioning::SubjectToken { token } => subject
                    .split('/')
                    .nth(token)
                    .map(|part| stable_hash(part.as_bytes())),
                Partitioning::Key => None,
                Partitioning::Sticky => Some(sticky),
            })
            .unwrap_or_else(|| stable_hash(subject.as_bytes()));
        metadata
            .leaders
            .get((value % u64::from(metadata.partitions)) as usize)
    }

    /// Return the partition selected by the same key/sticky rules as `route`.
    pub fn partition_for(
        &self,
        stream: &str,
        subject: &str,
        key: Option<&[u8]>,
        sticky: u64,
    ) -> Option<u32> {
        let metadata = self.streams.get(stream)?;
        let value = key
            .map(stable_hash)
            .or_else(|| match metadata.partitioning {
                Partitioning::SubjectToken { token } => subject
                    .split('/')
                    .nth(token)
                    .map(|part| stable_hash(part.as_bytes())),
                Partitioning::Key => None,
                Partitioning::Sticky => Some(sticky),
            })
            .unwrap_or_else(|| stable_hash(subject.as_bytes()));
        Some((value % u64::from(metadata.partitions)) as u32)
    }

    pub fn invalidate(&mut self, stream: &str, partitioning_epoch: u64) {
        if self
            .streams
            .get(stream)
            .is_some_and(|metadata| metadata.partitioning_epoch <= partitioning_epoch)
        {
            self.streams.remove(stream);
            self.order.retain(|entry| entry != stream);
        }
    }
}

fn stable_hash(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf29ce484222325, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3)
    })
}
