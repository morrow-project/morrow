use super::{log::PartitionLog, *};
use crate::error::Result;
use std::sync::{Mutex, atomic::Ordering};

const DEFAULT_MAX_DYNAMIC_PARTITIONS: usize = 4_096;
const MAX_DYNAMIC_PARTITIONS: usize = 65_536;

pub(crate) fn max_dynamic_partitions() -> usize {
    std::env::var("MORROW_MAX_ACTIVE_DYNAMIC_PARTITIONS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(DEFAULT_MAX_DYNAMIC_PARTITIONS)
        .clamp(1, MAX_DYNAMIC_PARTITIONS)
}

impl PartitionLogSet {
    pub(crate) fn metadata_cache_stats(&self) -> (usize, u64) {
        let cache = self
            .metadata_cache
            .lock()
            .expect("metadata cache lock poisoned");
        (cache.len(), cache.evictions())
    }

    pub(crate) fn active_resource_count(&self) -> usize {
        self.logs
            .values()
            .filter(|log| {
                log.lock()
                    .expect("partition log lock poisoned")
                    .has_active_resource()
            })
            .count()
            .saturating_add(self.dynamic_active_resource_count())
    }

    pub(super) fn cache_envelope(&self, envelope: &MessageEnvelope) {
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

    pub(super) fn mark_dirty(&self, stream: &str, partition: PartitionId) {
        self.dirty
            .lock()
            .expect("dirty partition lock poisoned")
            .insert((stream.to_string(), partition));
    }

    pub(super) fn clear_metadata_cache(&self) {
        self.metadata_cache
            .lock()
            .expect("metadata cache lock poisoned")
            .clear();
    }

    pub(crate) fn activate_partition(&self, stream: &str, partition: PartitionId) -> Result<()> {
        let key = (stream.to_string(), partition);
        if self.logs.contains_key(&key)
            || self
                .dynamic_logs
                .lock()
                .expect("dynamic partition lock poisoned")
                .contains_key(&key)
        {
            return Ok(());
        }
        let limit = max_dynamic_partitions() as u64;
        let reserved = self
            .dynamic_partition_count
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                (count < limit).then_some(count.saturating_add(1))
            })
            .is_ok();
        crate::broker_ensure!(reserved, "dynamic partition resource limit reached");
        let stream_id = StreamId::new(stream)?;
        let opened = PartitionLog::open(
            &self.root,
            &stream_id,
            partition,
            self.segment_bytes,
            self.encryption.clone(),
        );
        let (log, replay, repaired) = match opened {
            Ok(value) => value,
            Err(error) => {
                self.dynamic_partition_count.fetch_sub(1, Ordering::AcqRel);
                return Err(error);
            }
        };
        if !replay.is_empty() {
            self.dynamic_partition_count.fetch_sub(1, Ordering::AcqRel);
            return Err(crate::error::BrokerError::msg(
                "dynamic partition contains unreconciled records",
            ));
        }
        let _ = repaired;
        self.dynamic_logs
            .lock()
            .expect("dynamic partition lock poisoned")
            .insert(key, std::sync::Arc::new(Mutex::new(log)));
        Ok(())
    }

    pub(crate) fn append_dynamic(&self, envelope: MessageEnvelope) -> Result<MessageEnvelope> {
        let key = (envelope.stream.as_str().to_string(), envelope.partition);
        let logs = self
            .dynamic_logs
            .lock()
            .expect("dynamic partition lock poisoned");
        let log = logs
            .get(&key)
            .cloned()
            .ok_or_else(|| crate::error::BrokerError::msg("unknown stream partition"))?;
        drop(logs);
        let appended = log
            .lock()
            .expect("dynamic partition log lock poisoned")
            .append(envelope)?;
        Ok(appended)
    }

    pub(crate) fn append_dynamic_committed(
        &self,
        envelope: MessageEnvelope,
    ) -> Result<MessageEnvelope> {
        let key = (envelope.stream.as_str().to_string(), envelope.partition);
        let logs = self
            .dynamic_logs
            .lock()
            .expect("dynamic partition lock poisoned");
        let log = logs
            .get(&key)
            .cloned()
            .ok_or_else(|| crate::error::BrokerError::msg("unknown stream partition"))?;
        drop(logs);
        let appended = log
            .lock()
            .expect("dynamic partition log lock poisoned")
            .append_committed(envelope)?;
        Ok(appended)
    }

    pub(crate) fn load_dynamic(
        &self,
        stream: &str,
        partition: PartitionId,
        offset: u64,
    ) -> Result<Option<MessageEnvelope>> {
        let key = (stream.to_string(), partition);
        let logs = self
            .dynamic_logs
            .lock()
            .expect("dynamic partition lock poisoned");
        let Some(log) = logs.get(&key).cloned() else {
            return Ok(None);
        };
        drop(logs);
        log.lock()
            .expect("dynamic partition log lock poisoned")
            .read_offset(offset)
    }

    pub(crate) fn flush_dynamic_partition(
        &self,
        stream: &str,
        partition: PartitionId,
    ) -> Result<()> {
        let key = (stream.to_string(), partition);
        let logs = self
            .dynamic_logs
            .lock()
            .expect("dynamic partition lock poisoned");
        let log = logs
            .get(&key)
            .ok_or_else(|| crate::error::BrokerError::msg("unknown stream partition"))?
            .clone();
        drop(logs);
        log.lock()
            .expect("dynamic partition log lock poisoned")
            .release_resources()
    }

    pub(crate) fn dynamic_active_resource_count(&self) -> usize {
        self.dynamic_logs
            .lock()
            .expect("dynamic partition lock poisoned")
            .values()
            .filter(|log| {
                log.lock()
                    .expect("dynamic partition log lock poisoned")
                    .has_active_resource()
            })
            .count()
    }
}
