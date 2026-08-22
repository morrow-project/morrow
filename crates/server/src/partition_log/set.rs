use super::{log::PartitionLog, *};
use crate::error::ResultExt;
use crate::wal::PublishRecord;
use std::sync::{
    Mutex,
    atomic::{AtomicU64, Ordering},
};
use std::time::Instant;

const MAX_RECOVERY_WORKERS: usize = 8;

#[derive(Debug)]
pub struct PartitionLogSet {
    logs: HashMap<(String, PartitionId), Mutex<PartitionLog>>,
    sticky: Mutex<HashMap<String, u64>>,
    next_legacy_seq: AtomicU64,
    pub truncations: u64,
    recovery: PartitionRecoveryStatus,
}

impl PartitionLogSet {
    pub fn open(
        wal_dir: &Path,
        catalog: &StreamCatalog,
        segment_bytes: u64,
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
                    .map(|partition| (stream.name.clone(), PartitionId(partition)))
            })
            .collect::<Vec<_>>();
        let workers = std::thread::available_parallelism()
            .map_or(1, usize::from)
            .min(MAX_RECOVERY_WORKERS)
            .min(work.len().max(1));
        let chunk_size = work.len().max(1).div_ceil(workers);
        let recovered = std::thread::scope(|scope| -> Result<Vec<_>> {
            let handles = work
                .chunks(chunk_size)
                .map(|chunk| {
                    let root = &root;
                    scope.spawn(move || -> Result<Vec<_>> {
                        chunk
                            .iter()
                            .map(|(stream, partition)| {
                                let (log, replay, repaired) =
                                    PartitionLog::open(root, stream, *partition, segment_bytes)?;
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
        self.logs
            .get(&(envelope.stream.as_str().to_string(), envelope.partition))
            .expect("catalog partitions are opened together")
            .lock()
            .expect("partition log lock poisoned")
            .append(envelope)
    }

    pub fn flush(&self) -> Result<()> {
        for log in self.logs.values() {
            log.lock().expect("partition log lock poisoned").flush()?;
        }
        Ok(())
    }

    pub fn flush_partition(&self, stream: &str, partition: PartitionId) -> Result<()> {
        self.logs
            .get(&(stream.to_string(), partition))
            .ok_or_else(|| crate::error::BrokerError::msg("unknown stream partition"))?
            .lock()
            .expect("partition log lock poisoned")
            .flush()
    }

    pub fn append_committed(&self, envelope: MessageEnvelope) -> Result<MessageEnvelope> {
        self.next_legacy_seq
            .fetch_max(envelope.legacy_seq.saturating_add(1), Ordering::Relaxed);
        self.logs
            .get(&(envelope.stream.as_str().to_string(), envelope.partition))
            .ok_or_else(|| crate::error::BrokerError::msg("unknown stream partition"))?
            .lock()
            .expect("partition log lock poisoned")
            .append_committed(envelope)
    }

    pub(crate) fn rewrite_partition(
        &self,
        stream: &str,
        partition: PartitionId,
        records: &[MessageEnvelope],
    ) -> Result<()> {
        self.logs
            .get(&(stream.to_string(), partition))
            .ok_or_else(|| crate::error::BrokerError::msg("unknown stream partition"))?
            .lock()
            .expect("partition log lock poisoned")
            .rewrite(records, None)
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
                let log = self
                    .logs
                    .get(&(stream.name.as_str().to_string(), partition))
                    .expect("catalog partitions are opened together");
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
        self.logs
            .get(&(change.stream.clone(), change.partition))
            .ok_or_else(|| crate::error::BrokerError::msg("unknown stream partition"))?
            .lock()
            .expect("partition log lock poisoned")
            .rewrite(records, Some(change.earliest_offset))
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
        self.logs
            .get(&(stream.to_string(), partition))
            .ok_or_else(|| crate::error::BrokerError::msg("unknown stream partition"))?
            .lock()
            .expect("partition log lock poisoned")
            .read_offset(offset)
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
