use super::{log::PartitionLog, *};
use crate::error::Result;
use std::sync::Mutex;

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
        let stream_id = StreamId::new(stream)?;
        let (log, replay, repaired) = PartitionLog::open(
            &self.root,
            &stream_id,
            partition,
            self.segment_bytes,
            self.encryption.clone(),
        )?;
        crate::broker_ensure!(
            replay.is_empty(),
            "dynamic partition contains unreconciled records"
        );
        let _ = repaired;
        self.dynamic_logs
            .lock()
            .expect("dynamic partition lock poisoned")
            .insert(key, Mutex::new(log));
        Ok(())
    }

    pub(crate) fn append_dynamic(&self, envelope: MessageEnvelope) -> Result<MessageEnvelope> {
        let key = (envelope.stream.as_str().to_string(), envelope.partition);
        let mut logs = self
            .dynamic_logs
            .lock()
            .expect("dynamic partition lock poisoned");
        let log = logs
            .get_mut(&key)
            .ok_or_else(|| crate::error::BrokerError::msg("unknown stream partition"))?;
        let appended = log
            .get_mut()
            .expect("dynamic partition log lock poisoned")
            .append(envelope)?;
        Ok(appended)
    }

    pub(crate) fn append_dynamic_committed(
        &self,
        envelope: MessageEnvelope,
    ) -> Result<MessageEnvelope> {
        let key = (envelope.stream.as_str().to_string(), envelope.partition);
        let mut logs = self
            .dynamic_logs
            .lock()
            .expect("dynamic partition lock poisoned");
        let log = logs
            .get_mut(&key)
            .ok_or_else(|| crate::error::BrokerError::msg("unknown stream partition"))?;
        let appended = log
            .get_mut()
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
        let mut logs = self
            .dynamic_logs
            .lock()
            .expect("dynamic partition lock poisoned");
        let Some(log) = logs.get_mut(&key) else {
            return Ok(None);
        };
        log.get_mut()
            .expect("dynamic partition log lock poisoned")
            .read_offset(offset)
    }

    pub(crate) fn flush_dynamic_partition(
        &self,
        stream: &str,
        partition: PartitionId,
    ) -> Result<()> {
        let key = (stream.to_string(), partition);
        let mut logs = self
            .dynamic_logs
            .lock()
            .expect("dynamic partition lock poisoned");
        logs.get_mut(&key)
            .ok_or_else(|| crate::error::BrokerError::msg("unknown stream partition"))?
            .get_mut()
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
