use super::{log::PartitionLog, *};
use crate::error::ResultExt;
use crate::partition_cache::PartitionResourceCache;
use crate::wal::PublishRecord;
use std::collections::BTreeSet;
use std::sync::{
    Mutex,
    atomic::{AtomicU64, Ordering},
};
use std::time::Instant;

const MAX_RECOVERY_WORKERS: usize = 8;
const MIN_RECOVERY_WORKERS: usize = 1;
const DEFAULT_METADATA_CACHE_CAPACITY: usize = 4096;
const MAX_METADATA_CACHE_CAPACITY: usize = 1_000_000;

#[derive(Debug)]
pub struct PartitionLogSet {
    logs: HashMap<(String, PartitionId), Mutex<PartitionLog>>,
    sticky: Mutex<HashMap<String, u64>>,
    next_legacy_seq: AtomicU64,
    pub truncations: u64,
    recovery: PartitionRecoveryStatus,
    metadata_cache: Mutex<PartitionResourceCache<(String, PartitionId, u64), MessageEnvelope>>,
}

impl PartitionLogSet {
    pub fn open(
        wal_dir: &Path,
        catalog: &StreamCatalog,
        segment_bytes: u64,
    ) -> Result<(Self, Vec<MessageEnvelope>)> {
        Self::open_with_encryption(wal_dir, catalog, segment_bytes, None)
    }

    pub fn open_with_encryption(
        wal_dir: &Path,
        catalog: &StreamCatalog,
        segment_bytes: u64,
        encryption: Option<std::sync::Arc<crate::encryption::KeyRing>>,
    ) -> Result<(Self, Vec<MessageEnvelope>)> {
        Self::open_with_encryption_for_partitions(wal_dir, catalog, segment_bytes, encryption, None)
    }

    /// Open only the partitions assigned to this broker. An empty assignment
    /// is valid and defers all partition-log recovery until placement is known.
    pub(crate) fn open_with_encryption_for_partitions(
        wal_dir: &Path,
        catalog: &StreamCatalog,
        segment_bytes: u64,
        encryption: Option<std::sync::Arc<crate::encryption::KeyRing>>,
        assigned: Option<&BTreeSet<(String, u32)>>,
    ) -> Result<(Self, Vec<MessageEnvelope>)> {
        let root = wal_dir.join("streams");
        std::fs::create_dir_all(&root)
            .with_context(|| format!("creating stream data directory {}", root.display()))?;
        let started = Instant::now();
        let work = catalog
            .definitions()
            .iter()
            .flat_map(|stream| {
                (0..stream.partitions)
                    .filter(|partition| {
                        assigned.is_none_or(|assigned| {
                            assigned.contains(&(stream.name.as_str().to_string(), *partition))
                        })
                    })
                    .map(|partition| (stream.name.clone(), PartitionId(partition)))
            })
            .collect::<Vec<_>>();
        let configured_workers = std::env::var("MORROW_PARTITION_RECOVERY_WORKERS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(MAX_RECOVERY_WORKERS)
            .clamp(MIN_RECOVERY_WORKERS, MAX_RECOVERY_WORKERS);
        let workers = std::thread::available_parallelism()
            .map_or(1, usize::from)
            .min(configured_workers)
            .min(work.len().max(1));
        let chunk_size = work.len().max(1).div_ceil(workers);
        let recovered = std::thread::scope(|scope| -> Result<Vec<_>> {
            let handles = work
                .chunks(chunk_size)
                .map(|chunk| {
                    let root = &root;
                    let encryption = encryption.clone();
                    scope.spawn(move || -> Result<Vec<_>> {
                        chunk
                            .iter()
                            .map(|(stream, partition)| {
                                let (log, replay, repaired) = PartitionLog::open(
                                    root,
                                    stream,
                                    *partition,
                                    segment_bytes,
                                    encryption.clone(),
                                )?;
                                Ok((stream.clone(), *partition, log, replay, repaired))
                            })
                            .collect()
                    })
                })
                .collect::<Vec<_>>();
            let mut recovered = Vec::with_capacity(work.len());
            for handle in handles {
                recovered.extend(handle.join().map_err(|_| {
                    crate::error::BrokerError::msg("partition recovery worker panicked")
                })??);
            }
            Ok(recovered)
        })?;
        let mut logs = HashMap::new();
        let mut envelopes = Vec::new();
        let mut truncations = 0;
        for (stream, partition, log, replay, repaired) in recovered {
            logs.insert((stream.as_str().to_string(), partition), Mutex::new(log));
            envelopes.extend(replay);
            truncations += repaired;
        }
        envelopes.sort_by_key(|envelope| envelope.legacy_seq);
        let resident_metadata_bytes = envelopes.iter().map(resident_envelope_bytes).sum();
        let metadata_cache_capacity = std::env::var("MORROW_PARTITION_METADATA_CACHE_CAPACITY")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(DEFAULT_METADATA_CACHE_CAPACITY)
            .clamp(1, MAX_METADATA_CACHE_CAPACITY);
        let next_legacy_seq = envelopes
            .iter()
            .map(|envelope| envelope.legacy_seq)
            .max()
            .unwrap_or(0)
            .saturating_add(1);
        Ok((
            Self {
                logs,
                sticky: Mutex::new(HashMap::new()),
                next_legacy_seq: AtomicU64::new(next_legacy_seq),
                truncations,
                recovery: PartitionRecoveryStatus {
                    total_partitions: work.len(),
                    completed_partitions: work.len(),
                    records_scanned: envelopes.len(),
                    resident_metadata_bytes,
                    elapsed_ms: started.elapsed().as_millis() as u64,
                    workers,
                },
                metadata_cache: Mutex::new(
                    PartitionResourceCache::new(metadata_cache_capacity)
                        .expect("metadata cache capacity is clamped above zero"),
                ),
            },
            envelopes,
        ))
    }

    pub fn append(&self, request: AppendRequest<'_>) -> Result<MessageEnvelope> {
        let mut sticky_values = self.sticky.lock().expect("partition sticky lock poisoned");
        let sticky = sticky_values
            .entry(request.stream.name.as_str().to_string())
            .or_default();
        let partition = request.partition_hint.unwrap_or_else(|| {
            select_partition(request.stream, request.subject, request.key, *sticky)
        });
        *sticky = sticky.saturating_add(1);
        drop(sticky_values);
        let legacy_seq = request
            .legacy_seq
            .unwrap_or_else(|| self.next_legacy_seq.fetch_add(1, Ordering::Relaxed));
        self.next_legacy_seq
            .fetch_max(legacy_seq.saturating_add(1), Ordering::Relaxed);
        let envelope = MessageEnvelope {
            namespace: request.namespace.to_string(),
            stream: request.stream.name.clone(),
            partition,
            offset: 0,
            subject: request.subject.to_string(),
            key: request.key.map(<[u8]>::to_vec),
            headers: request.headers.to_vec(),
            timestamp_ms: request.timestamp_ms,
            reply_to: request.reply_to.map(str::to_string),
            schema_id: None,
            payload: request.payload.to_vec(),
            partitioning_epoch: request.stream.partitioning.epoch,
            leader_epoch: request.leader_epoch,
            legacy_seq,
        };
        self.append_envelope(envelope)
    }

    pub(crate) fn append_envelope(&self, envelope: MessageEnvelope) -> Result<MessageEnvelope> {
        self.next_legacy_seq
            .fetch_max(envelope.legacy_seq.saturating_add(1), Ordering::Relaxed);
        let appended = self
            .logs
            .get(&(envelope.stream.as_str().to_string(), envelope.partition))
            .expect("catalog partitions are opened together")
            .lock()
            .expect("partition log lock poisoned")
            .append(envelope)?;
        self.cache_envelope(&appended);
        Ok(appended)
    }

    pub fn flush(&self) -> Result<()> {
        for log in self.logs.values() {
            log.lock()
                .expect("partition log lock poisoned")
                .release_resources()?;
        }
        Ok(())
    }

    pub fn flush_partition(&self, stream: &str, partition: PartitionId) -> Result<()> {
        self.logs
            .get(&(stream.to_string(), partition))
            .ok_or_else(|| crate::error::BrokerError::msg("unknown stream partition"))?
            .lock()
            .expect("partition log lock poisoned")
            .release_resources()
    }

    pub fn append_committed(&self, envelope: MessageEnvelope) -> Result<MessageEnvelope> {
        self.next_legacy_seq
            .fetch_max(envelope.legacy_seq.saturating_add(1), Ordering::Relaxed);
        let appended = self
            .logs
            .get(&(envelope.stream.as_str().to_string(), envelope.partition))
            .ok_or_else(|| crate::error::BrokerError::msg("unknown stream partition"))?
            .lock()
            .expect("partition log lock poisoned")
            .append_committed(envelope)?;
        self.cache_envelope(&appended);
        Ok(appended)
    }

    pub(crate) fn rewrite_partition(
        &self,
        stream: &str,
        partition: PartitionId,
        records: &[MessageEnvelope],
    ) -> Result<()> {
        let result = self
            .logs
            .get(&(stream.to_string(), partition))
            .ok_or_else(|| crate::error::BrokerError::msg("unknown stream partition"))?
            .lock()
            .expect("partition log lock poisoned")
            .rewrite(records, None);
        self.clear_metadata_cache();
        result
    }

    pub(crate) fn enforce_retention(
        &self,
        envelopes: &mut Vec<MessageEnvelope>,
        catalog: &StreamCatalog,
        now_ms: u64,
    ) -> Result<Vec<RetentionChange>> {
        let changes = self.retention_changes(catalog.definitions(), now_ms);
        for change in &changes {
            envelopes.retain(|envelope| {
                envelope.stream.as_str() != change.stream
                    || envelope.partition != change.partition
                    || envelope.offset >= change.earliest_offset
            });
            let retained_metadata = envelopes
                .iter()
                .filter(|envelope| {
                    envelope.stream.as_str() == change.stream
                        && envelope.partition == change.partition
                })
                .cloned()
                .collect::<Vec<_>>();
            let mut log = self
                .logs
                .get(&(change.stream.clone(), change.partition))
                .expect("retention changes reference configured logs")
                .lock()
                .expect("partition log lock poisoned");
            let retained = retained_metadata
                .iter()
                .map(|metadata| {
                    log.read_offset(metadata.offset)?.ok_or_else(|| {
                        crate::error::BrokerError::msg("retained partition record is missing")
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            log.rewrite(&retained, Some(change.earliest_offset))?;
        }
        self.clear_metadata_cache();
        Ok(changes)
    }

    pub(crate) fn retention_changes(
        &self,
        streams: &[StreamDefinition],
        now_ms: u64,
    ) -> Vec<RetentionChange> {
        let mut changes = Vec::new();
        for stream in streams {
            for partition in 0..stream.partitions {
                let partition = PartitionId(partition);
                let Some(log) = self
                    .logs
                    .get(&(stream.name.as_str().to_string(), partition))
                else {
                    continue;
                };
                if let Some(earliest_offset) = log
                    .lock()
                    .expect("partition log lock poisoned")
                    .enforce_retention(&stream.retention, now_ms)
                {
                    changes.push(RetentionChange {
                        stream: stream.name.as_str().to_string(),
                        partition,
                        earliest_offset,
                    });
                }
            }
        }
        changes
    }

    pub(crate) fn retain_partition(
        &self,
        change: &RetentionChange,
        records: &[MessageEnvelope],
    ) -> Result<()> {
        let result = self
            .logs
            .get(&(change.stream.clone(), change.partition))
            .ok_or_else(|| crate::error::BrokerError::msg("unknown stream partition"))?
            .lock()
            .expect("partition log lock poisoned")
            .rewrite(records, Some(change.earliest_offset));
        self.clear_metadata_cache();
        result
    }

    pub(crate) fn advance_retention(&self, change: &RetentionChange) -> Result<()> {
        let result = self
            .logs
            .get(&(change.stream.clone(), change.partition))
            .ok_or_else(|| crate::error::BrokerError::msg("unknown stream partition"))?
            .lock()
            .expect("partition log lock poisoned")
            .advance_retention_floor(change.earliest_offset);
        self.clear_metadata_cache();
        result
    }

    pub(crate) fn compact_visible_offsets(
        &self,
        visible_offsets: &HashMap<(String, PartitionId), BTreeSet<u64>>,
        catalog: &StreamCatalog,
    ) -> Result<()> {
        for stream in catalog
            .definitions()
            .iter()
            .filter(|stream| stream.retention.compaction == crate::stream::CompactionPolicy::Key)
        {
            for partition in 0..stream.partitions {
                let partition = PartitionId(partition);
                let key = (stream.name.as_str().to_string(), partition);
                let Some(offsets) = visible_offsets.get(&key) else {
                    continue;
                };
                let mut log = self
                    .logs
                    .get(&key)
                    .ok_or_else(|| crate::error::BrokerError::msg("unknown stream partition"))?
                    .lock()
                    .expect("partition log lock poisoned");
                while log.compact_visible_offsets(offsets)? {}
            }
        }
        Ok(())
    }

    pub(crate) fn retention_status(
        &self,
        stream: &str,
        partition: PartitionId,
    ) -> Result<PartitionRetentionStatus> {
        Ok(self
            .logs
            .get(&(stream.to_string(), partition))
            .ok_or_else(|| crate::error::BrokerError::msg("unknown stream partition"))?
            .lock()
            .expect("partition log lock poisoned")
            .retention_status(partition))
    }

    pub(crate) fn recovery_status(&self) -> PartitionRecoveryStatus {
        self.recovery.clone()
    }

    pub(crate) fn is_before_retention_floor(
        &self,
        stream: &str,
        partition: PartitionId,
        offset: u64,
    ) -> bool {
        self.logs
            .get(&(stream.to_string(), partition))
            .is_some_and(|log| {
                offset
                    < log
                        .lock()
                        .expect("partition log lock poisoned")
                        .retention_status(partition)
                        .earliest_offset
            })
    }

    pub(crate) fn matching_offsets(
        &self,
        stream: &str,
        partition: PartitionId,
        filter: &str,
    ) -> Result<SubjectIndexQuery> {
        self.logs
            .get(&(stream.to_string(), partition))
            .ok_or_else(|| crate::error::BrokerError::msg("unknown stream partition"))?
            .lock()
            .expect("partition log lock poisoned")
            .matching_offsets(filter)
    }

    pub(crate) fn load_record(&self, metadata: &PublishRecord) -> Result<PublishRecord> {
        let (Some(stream), Some(partition), Some(offset)) = (
            metadata.stream.as_deref(),
            metadata.partition,
            metadata.offset,
        ) else {
            return Ok(metadata.clone());
        };
        let envelope = self
            .load_envelope(stream, PartitionId(partition), offset)?
            .ok_or_else(|| crate::error::BrokerError::msg("partition record is unavailable"))?;
        Ok(PublishRecord::from(envelope))
    }

    pub(crate) fn load_envelope(
        &self,
        stream: &str,
        partition: PartitionId,
        offset: u64,
    ) -> Result<Option<MessageEnvelope>> {
        let key = (stream.to_string(), partition, offset);
        if let Some(envelope) = self
            .metadata_cache
            .lock()
            .expect("metadata cache lock poisoned")
            .get(&key)
            .cloned()
        {
            return Ok(Some(envelope));
        }
        let envelope = self
            .logs
            .get(&(stream.to_string(), partition))
            .ok_or_else(|| crate::error::BrokerError::msg("unknown stream partition"))?
            .lock()
            .expect("partition log lock poisoned")
            .read_offset(offset)?;
        if let Some(envelope) = &envelope {
            self.metadata_cache
                .lock()
                .expect("metadata cache lock poisoned")
                .insert(key, envelope.clone());
        }
        Ok(envelope)
    }

    pub(crate) fn metadata_cache_stats(&self) -> (usize, u64) {
        let cache = self
            .metadata_cache
            .lock()
            .expect("metadata cache lock poisoned");
        (cache.len(), cache.evictions())
    }

    fn cache_envelope(&self, envelope: &MessageEnvelope) {
        self.metadata_cache
            .lock()
            .expect("metadata cache lock poisoned")
            .insert(
                (
                    envelope.stream.as_str().to_string(),
                    envelope.partition,
                    envelope.offset,
                ),
                envelope.clone(),
            );
    }

    fn clear_metadata_cache(&self) {
        self.metadata_cache
            .lock()
            .expect("metadata cache lock poisoned")
            .clear();
    }

    #[cfg(test)]
    pub(crate) fn with_partition_lock_for_test<T>(
        &self,
        stream: &str,
        partition: PartitionId,
        operation: impl FnOnce() -> T,
    ) -> T {
        let _guard = self
            .logs
            .get(&(stream.to_string(), partition))
            .expect("test partition exists")
            .lock()
            .expect("partition log lock poisoned");
        operation()
    }

    #[cfg(test)]
    pub(crate) fn stage_partition_rewrite_for_test(
        &self,
        stream: &str,
        partition: PartitionId,
        records: &[MessageEnvelope],
        next_offset: u64,
    ) -> Result<()> {
        self.logs
            .get(&(stream.to_string(), partition))
            .expect("test partition exists")
            .lock()
            .expect("partition log lock poisoned")
            .stage_rewrite_for_test(records, next_offset)
    }
}

fn resident_envelope_bytes(envelope: &MessageEnvelope) -> usize {
    std::mem::size_of::<MessageEnvelope>()
        + envelope.namespace.capacity()
        + envelope.stream.as_str().len()
        + envelope.subject.capacity()
        + envelope.key.as_ref().map_or(0, Vec::capacity)
        + envelope
            .headers
            .iter()
            .map(|header| header.name.capacity() + header.value.capacity())
            .sum::<usize>()
        + envelope.headers.capacity() * std::mem::size_of::<MessageHeader>()
        + envelope.reply_to.as_ref().map_or(0, String::capacity)
        + envelope.payload.capacity()
}
