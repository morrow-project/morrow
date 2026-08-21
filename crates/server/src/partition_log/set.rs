use super::{log::PartitionLog, *};
use crate::error::ResultExt;

#[derive(Debug)]
pub struct PartitionLogSet {
    logs: HashMap<(String, PartitionId), PartitionLog>,
    sticky: HashMap<String, u64>,
    next_legacy_seq: u64,
    pub truncations: u64,
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
        let mut logs = HashMap::new();
        let mut envelopes = Vec::new();
        let mut truncations = 0;
        for stream in catalog.definitions() {
            for partition in 0..stream.partitions {
                let partition = PartitionId(partition);
                let (log, replay, repaired) =
                    PartitionLog::open(&root, &stream.name, partition, segment_bytes)?;
                logs.insert((stream.name.as_str().to_string(), partition), log);
                envelopes.extend(replay);
                truncations += repaired;
            }
        }
        let next_legacy_seq = envelopes
            .iter()
            .map(|envelope| envelope.legacy_seq)
            .max()
            .unwrap_or(0)
            .saturating_add(1);
        Ok((
            Self {
                logs,
                sticky: HashMap::new(),
                next_legacy_seq,
                truncations,
            },
            envelopes,
        ))
    }

    pub fn append(&mut self, request: AppendRequest<'_>) -> Result<MessageEnvelope> {
        let sticky = self
            .sticky
            .entry(request.stream.name.as_str().to_string())
            .or_default();
        let partition = request.partition_hint.unwrap_or_else(|| {
            select_partition(request.stream, request.subject, request.key, *sticky)
        });
        *sticky = sticky.saturating_add(1);
        let legacy_seq = request.legacy_seq.unwrap_or_else(|| {
            let seq = self.next_legacy_seq;
            self.next_legacy_seq = self.next_legacy_seq.saturating_add(1);
            seq
        });
        self.next_legacy_seq = self.next_legacy_seq.max(legacy_seq.saturating_add(1));
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
        self.logs
            .get_mut(&(request.stream.name.as_str().to_string(), partition))
            .expect("catalog partitions are opened together")
            .append(envelope)
    }

    pub fn flush(&mut self) -> Result<()> {
        for log in self.logs.values_mut() {
            log.flush()?;
        }
        Ok(())
    }

    pub fn append_committed(&mut self, envelope: MessageEnvelope) -> Result<MessageEnvelope> {
        self.next_legacy_seq = self
            .next_legacy_seq
            .max(envelope.legacy_seq.saturating_add(1));
        self.logs
            .get_mut(&(envelope.stream.as_str().to_string(), envelope.partition))
            .ok_or_else(|| crate::error::BrokerError::msg("unknown stream partition"))?
            .append_committed(envelope)
    }

    pub(crate) fn rewrite_partition(
        &mut self,
        stream: &str,
        partition: PartitionId,
        records: &[MessageEnvelope],
    ) -> Result<()> {
        self.logs
            .get_mut(&(stream.to_string(), partition))
            .ok_or_else(|| crate::error::BrokerError::msg("unknown stream partition"))?
            .rewrite(records)
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
            .matching_offsets(filter)
    }
}
