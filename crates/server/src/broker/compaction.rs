use super::*;
use crate::{stream::CompactionPolicy, wal::PublishRecord};

pub(super) type CompactionKey = (String, u32, String, Vec<u8>);
const PHYSICAL_COMPACTION_THRESHOLD: usize = 64;

pub(super) fn recover_compaction_index(
    messages: &mut HashMap<u64, PublishRecord>,
    catalog: &crate::stream::StreamCatalog,
) -> HashMap<CompactionKey, (u64, u64)> {
    let compacted_streams = compacted_streams(catalog);
    let mut latest = HashMap::<CompactionKey, (u64, u64)>::new();
    for (seq, record) in messages.iter() {
        let Some((identity, offset)) = compaction_identity(record, &compacted_streams) else {
            continue;
        };
        let candidate = (offset, *seq);
        latest
            .entry(identity)
            .and_modify(|current| *current = (*current).max(candidate))
            .or_insert(candidate);
    }
    messages.retain(|seq, record| {
        let Some((identity, offset)) = compaction_identity(record, &compacted_streams) else {
            return true;
        };
        latest.get(&identity) == Some(&(offset, *seq))
    });
    latest
}

pub(super) fn reconcile_replayed_compaction(
    replay: &mut crate::wal::Replay,
    envelope_by_seq: HashMap<u64, MessageEnvelope>,
    partition_logs: &PartitionLogSet,
    catalog: &crate::stream::StreamCatalog,
) -> Result<HashMap<CompactionKey, (u64, u64)>> {
    replay.partition_appends.retain(|seq, reference| {
        if envelope_by_seq.contains_key(seq) {
            return true;
        }
        let compacted = catalog.definitions().iter().any(|stream| {
            stream.name.as_str() == reference.stream
                && stream.retention.compaction == CompactionPolicy::Key
        });
        !compacted
            && !partition_logs.is_before_retention_floor(
                &reference.stream,
                crate::stream::PartitionId(reference.partition),
                reference.offset,
            )
    });
    for reference in replay.partition_appends.values() {
        let envelope = envelope_by_seq.get(&reference.seq).ok_or_else(|| {
            BrokerError::msg(format!(
                "control WAL references missing stream record {}:{}:{}",
                reference.stream, reference.partition, reference.offset
            ))
        })?;
        crate::broker_ensure!(
            envelope.stream.as_str() == reference.stream
                && envelope.partition.0 == reference.partition
                && envelope.offset == reference.offset,
            "control WAL partition reference does not match stream data"
        );
    }
    replay.messages.retain(|_, record| record.stream.is_none());
    replay.messages.extend(
        envelope_by_seq
            .into_iter()
            .map(|(seq, envelope)| (seq, PublishRecord::from(envelope).into_resident_metadata())),
    );
    Ok(recover_compaction_index(&mut replay.messages, catalog))
}

impl DurableBrokerState {
    pub(super) fn apply_record_compaction(
        &mut self,
        seq: u64,
        catalog: &crate::stream::StreamCatalog,
    ) {
        let compacted_streams = compacted_streams(catalog);
        let Some(record) = self.messages.get(&seq) else {
            return;
        };
        let Some((identity, offset)) = compaction_identity(record, &compacted_streams) else {
            return;
        };
        let candidate = (offset, seq);
        let previous = self.compaction_latest.get(&identity).copied();
        if previous.is_some_and(|latest| latest >= candidate) {
            self.remove_compacted_sequence(seq);
            self.superseded_since_compaction += 1;
            return;
        }
        self.compaction_latest.insert(identity, candidate);
        if let Some((_, previous_seq)) = previous {
            self.remove_compacted_sequence(previous_seq);
            self.superseded_since_compaction += 1;
        }
    }

    pub(super) fn remove_compaction_sequences(&mut self, removed: &HashSet<u64>) {
        self.compaction_latest
            .retain(|_, (_, seq)| !removed.contains(seq));
    }

    pub(super) fn take_physical_compaction_due(&mut self) -> bool {
        if self.superseded_since_compaction < PHYSICAL_COMPACTION_THRESHOLD {
            return false;
        }
        self.superseded_since_compaction = 0;
        true
    }

    pub(super) fn restore_physical_compaction_due(&mut self) {
        self.superseded_since_compaction = self
            .superseded_since_compaction
            .max(PHYSICAL_COMPACTION_THRESHOLD);
    }

    pub(super) fn rebuild_compaction_index(&mut self, catalog: &crate::stream::StreamCatalog) {
        self.compaction_latest = recover_compaction_index(&mut self.messages, catalog);
        self.partition_sequences
            .retain(|_, seq| self.messages.contains_key(seq));
    }

    fn remove_compacted_sequence(&mut self, seq: u64) {
        if let Some(record) = self.messages.remove(&seq)
            && let (Some(stream), Some(partition), Some(offset)) =
                (record.stream, record.partition, record.offset)
        {
            self.partition_sequences
                .remove(&(stream, partition, offset));
        }
        for consumer in self.consumers.values_mut() {
            consumer.pending.remove(&seq);
            consumer.pending_attempts.remove(&seq);
            consumer.in_flight.remove(&seq);
            consumer.acked.remove(&seq);
        }
    }
}

fn compacted_streams(catalog: &crate::stream::StreamCatalog) -> HashSet<&str> {
    catalog
        .definitions()
        .iter()
        .filter(|stream| stream.retention.compaction == CompactionPolicy::Key)
        .map(|stream| stream.name.as_str())
        .collect()
}

fn compaction_identity(
    record: &PublishRecord,
    compacted_streams: &HashSet<&str>,
) -> Option<(CompactionKey, u64)> {
    let stream = record.stream.as_deref()?;
    let partition = record.partition?;
    let offset = record.offset?;
    let key = record.key.as_ref()?;
    compacted_streams.contains(stream).then(|| {
        (
            (
                stream.to_string(),
                partition,
                record.namespace.clone(),
                key.clone(),
            ),
            offset,
        )
    })
}

impl Morrow {
    pub(super) async fn schedule_physical_compaction(&self) {
        if self.compaction_running.load(Ordering::Acquire) {
            return;
        }
        if !self.inner.lock().await.take_physical_compaction_due() {
            return;
        }
        if self
            .compaction_running
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            self.inner.lock().await.restore_physical_compaction_due();
            return;
        }
        let broker = self.clone();
        tokio::spawn(async move {
            loop {
                if let Err(err) = broker.compact_stream_segments().await {
                    error!(error = ?err, "stream compaction error");
                    broker.inner.lock().await.restore_physical_compaction_due();
                    broker.compaction_running.store(false, Ordering::Release);
                    return;
                }
                if !broker.inner.lock().await.take_physical_compaction_due() {
                    broker.compaction_running.store(false, Ordering::Release);
                    if !broker.inner.lock().await.take_physical_compaction_due() {
                        return;
                    }
                    if broker
                        .compaction_running
                        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                        .is_err()
                    {
                        broker.inner.lock().await.restore_physical_compaction_due();
                        return;
                    }
                }
            }
        });
    }

    async fn compact_stream_segments(&self) -> Result<()> {
        let _storage_operation = self.storage_gate.write().await;
        let records = self
            .inner
            .lock()
            .await
            .messages
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let logs = self.partition_logs.clone();
        let catalog = self.config.streams.clone();
        let permit = self
            .storage_permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| BrokerError::msg("storage worker pool closed"))?;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            logs.compact_visible_records(&records, &catalog)
        })
        .await
        .map_err(|err| BrokerError::with_source("stream compaction worker failed", err))?
    }

    #[cfg(test)]
    pub(crate) async fn compact_streams_for_test(&self) -> Result<()> {
        self.compact_stream_segments().await
    }
}
