//! Client-side partition leader metadata and deterministic routing.

use serde::Deserialize;
use std::{
    collections::{HashMap, VecDeque},
    future::Future,
    net::SocketAddr,
};

use super::{Client, ClientOptions, ProducerAck};

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

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct PartitionMetadataResponse {
    version: u32,
    partitions: Vec<PartitionMetadataEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct PartitionMetadataEntry {
    stream: String,
    partition: u32,
    leader_epoch: u64,
    partitioning_epoch: u64,
    partitioning: WirePartitioning,
    leader_client_addr: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct WirePartitioning {
    strategy: String,
    #[serde(default)]
    token: Option<u32>,
}

#[derive(Debug)]
pub struct PartitionLeaderCache {
    max_streams: usize,
    streams: HashMap<String, StreamMetadata>,
    order: VecDeque<String>,
}

/// A bounded pool of direct connections selected from partition metadata.
/// The regular [`Client`] remains unchanged for proxy-compatible callers.
pub struct RoutedClient {
    options: ClientOptions,
    cache: PartitionLeaderCache,
    clients: HashMap<SocketAddr, Client>,
    max_connections: usize,
    sticky: u64,
}

impl RoutedClient {
    pub fn new(options: ClientOptions, max_streams: usize, max_connections: usize) -> Option<Self> {
        Some(Self {
            options,
            cache: PartitionLeaderCache::new(max_streams)?,
            clients: HashMap::new(),
            max_connections: max_connections.max(1),
            sticky: 0,
        })
    }

    pub fn apply_metadata_json(&mut self, payload: &[u8]) -> Result<usize, String> {
        self.cache.apply_metadata_json(payload)
    }

    pub fn invalidate(&mut self, stream: &str, epoch: u64) {
        self.cache.invalidate(stream, epoch);
    }

    pub fn cached_connections(&self) -> usize {
        self.clients.len()
    }

    pub async fn publish_to_stream(
        &mut self,
        stream: &str,
        subject: &str,
        payload: &[u8],
        key: Option<&str>,
    ) -> super::error::Result<()> {
        self.publish_to_stream_with_headers(stream, subject, payload, key, &[])
            .await
    }

    /// Publish directly to the selected partition leader while preserving
    /// application headers. The metadata cache and bounded connection pool are
    /// shared with the headerless convenience method above.
    pub async fn publish_to_stream_with_headers(
        &mut self,
        stream: &str,
        subject: &str,
        payload: &[u8],
        key: Option<&str>,
        headers: &[(String, String)],
    ) -> super::error::Result<()> {
        let address = self.route_address(stream, subject, key);
        let mut client = self.take_client(address).await?;
        let result = match key {
            Some(key) => {
                client
                    .publish_with_key_and_headers(subject, None, payload, key, headers)
                    .await
            }
            None if headers.is_empty() => client.publish(subject, payload).await,
            None => {
                client
                    .publish_with_headers(subject, None, payload, headers)
                    .await
            }
        };
        self.return_client(address, client);
        result
    }

    pub async fn publish_to_stream_with_qos(
        &mut self,
        stream: &str,
        subject: &str,
        payload: &[u8],
        level: protocol::AckLevel,
        msg_id: &str,
        key: Option<&str>,
    ) -> super::error::Result<ProducerAck> {
        self.publish_to_stream_with_qos_and_headers(
            stream,
            subject,
            payload,
            level,
            msg_id,
            key,
            &[],
        )
        .await
    }

    /// Acknowledged direct publish with application headers. A stable message
    /// ID still makes the bounded stale-route retry idempotent.
    pub async fn publish_to_stream_with_qos_and_headers(
        &mut self,
        stream: &str,
        subject: &str,
        payload: &[u8],
        level: protocol::AckLevel,
        msg_id: &str,
        key: Option<&str>,
        headers: &[(String, String)],
    ) -> super::error::Result<ProducerAck> {
        let address = self.route_address(stream, subject, key);
        let result = self
            .publish_qos_once(address, subject, payload, level, msg_id, key, headers)
            .await;
        match result {
            Ok(ack) => Ok(ack),
            Err(first_error) => {
                // A direct leader may have moved after metadata was cached.
                // Retry once through the bootstrap/proxy path. The stable
                // producer message ID makes an uncertain first attempt safe
                // to deduplicate; fire-and-forget publishes intentionally do
                // not have this retry behavior.
                self.clients.remove(&address);
                self.cache.invalidate(stream, u64::MAX);
                let retry_address = self.route_address(stream, subject, key);
                self.publish_qos_once(
                    retry_address,
                    subject,
                    payload,
                    level,
                    msg_id,
                    key,
                    headers,
                )
                    .await
                    .map_err(|retry_error| {
                        super::error::ClientError::msg(format!(
                            "publish failed after one metadata refresh retry: {first_error}; retry: {retry_error}"
                        ))
                    })
            }
        }
    }

    /// Refresh metadata once after a bounded publish failure, then retry with
    /// the stable producer identity. The caller owns the metadata transport so
    /// this remains usable with HTTP, an embedded control client, or a cached
    /// file; the routing layer only enforces one refresh attempt.
    pub async fn publish_to_stream_with_qos_and_headers_refresh<F, Fut>(
        &mut self,
        stream: &str,
        subject: &str,
        payload: &[u8],
        level: protocol::AckLevel,
        msg_id: &str,
        key: Option<&str>,
        headers: &[(String, String)],
        refresh: F,
    ) -> super::error::Result<ProducerAck>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Vec<u8>, String>>,
    {
        let address = self.route_address(stream, subject, key);
        match self
            .publish_qos_once(address, subject, payload, level, msg_id, key, headers)
            .await
        {
            Ok(ack) => Ok(ack),
            Err(first_error) => {
                self.cache.invalidate(stream, u64::MAX);
                let metadata = refresh().await.map_err(|refresh_error| {
                    super::error::ClientError::msg(format!(
                        "publish failed and metadata refresh failed: {first_error}; refresh: {refresh_error}"
                    ))
                })?;
                self.apply_metadata_json(&metadata).map_err(|error| {
                    super::error::ClientError::msg(format!(
                        "publish failed and refreshed metadata was invalid: {first_error}; metadata: {error}"
                    ))
                })?;
                let retry_address = self.route_address(stream, subject, key);
                self.publish_qos_once(
                    retry_address,
                    subject,
                    payload,
                    level,
                    msg_id,
                    key,
                    headers,
                )
                .await
                .map_err(|retry_error| {
                    super::error::ClientError::msg(format!(
                        "publish failed after metadata refresh: {first_error}; retry: {retry_error}"
                    ))
                })
            }
        }
    }

    async fn publish_qos_once(
        &mut self,
        address: SocketAddr,
        subject: &str,
        payload: &[u8],
        level: protocol::AckLevel,
        msg_id: &str,
        key: Option<&str>,
        headers: &[(String, String)],
    ) -> super::error::Result<ProducerAck> {
        let mut client = self.take_client(address).await?;
        let result = client
            .publish_with_qos_key_and_headers(subject, None, payload, level, msg_id, key, headers)
            .await;
        if result.is_ok() {
            self.return_client(address, client);
        }
        result
    }

    fn route_address(&mut self, stream: &str, subject: &str, key: Option<&str>) -> SocketAddr {
        let sticky = self.sticky;
        self.sticky = self.sticky.wrapping_add(1);
        self.cache
            .route(stream, subject, key.map(str::as_bytes), sticky)
            .and_then(|leader| leader.address.parse().ok())
            .unwrap_or(self.options.addr)
    }

    async fn take_client(&mut self, address: SocketAddr) -> super::error::Result<Client> {
        if let Some(client) = self.clients.remove(&address) {
            return Ok(client);
        }
        let mut options = self.options.clone();
        options.addr = address;
        Client::connect_with_options(&options).await
    }

    fn return_client(&mut self, address: SocketAddr, client: Client) {
        if self.clients.len() < self.max_connections {
            self.clients.insert(address, client);
        }
    }
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
        if self.streams.get(&metadata.name).is_some_and(|current| {
            current.partitioning_epoch == metadata.partitioning_epoch
                && current
                    .leaders
                    .iter()
                    .zip(&metadata.leaders)
                    .any(|(existing, next)| next.leader_epoch < existing.leader_epoch)
        }) {
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

    /// Apply the server's versioned partition metadata response. Entries
    /// without a routable leader address are ignored so callers retain their
    /// existing proxy fallback.
    pub fn apply_metadata_json(&mut self, payload: &[u8]) -> Result<usize, String> {
        let response: PartitionMetadataResponse =
            serde_json::from_slice(payload).map_err(|error| error.to_string())?;
        if response.version != 1 {
            return Err(format!(
                "unsupported partition metadata version {}",
                response.version
            ));
        }
        let mut grouped = HashMap::<String, Vec<PartitionMetadataEntry>>::new();
        for entry in response.partitions {
            if entry.leader_client_addr.is_some() {
                grouped.entry(entry.stream.clone()).or_default().push(entry);
            }
        }
        let mut applied = 0;
        for (name, mut entries) in grouped {
            entries.sort_by_key(|entry| entry.partition);
            let Some(first) = entries.first() else {
                continue;
            };
            if entries.iter().enumerate().any(|(index, entry)| {
                entry.partition != index as u32
                    || entry.partitioning_epoch != first.partitioning_epoch
                    || entry.partitioning != first.partitioning
            }) {
                continue;
            }
            let Some(leaders) = entries
                .iter()
                .map(|entry| {
                    Some(PartitionLeader {
                        partition: entry.partition,
                        leader_epoch: entry.leader_epoch,
                        address: entry.leader_client_addr.clone()?,
                    })
                })
                .collect::<Option<Vec<_>>>()
            else {
                continue;
            };
            if self.insert(StreamMetadata {
                name,
                partitions: leaders.len() as u32,
                partitioning_epoch: first.partitioning_epoch,
                partitioning: match first.partitioning.strategy.as_str() {
                    "key" => Partitioning::Key,
                    "subject_token" => Partitioning::SubjectToken {
                        token: first.partitioning.token.unwrap_or_default() as usize,
                    },
                    _ => continue,
                },
                leaders,
            }) {
                applied += 1;
            }
        }
        Ok(applied)
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
