use super::*;
use crate::{stream::CompactionPolicy, wal::PublishRecord};

type CompactionKey = (String, u32, String, Vec<u8>);

pub(super) fn compact_stream_records(
    messages: &mut HashMap<u64, PublishRecord>,
    catalog: &crate::stream::StreamCatalog,
) {
    let compacted_streams = catalog
        .definitions()
        .iter()
        .filter(|stream| stream.retention.compaction == CompactionPolicy::Key)
        .map(|stream| stream.name.as_str())
        .collect::<HashSet<_>>();
    if compacted_streams.is_empty() {
        return;
    }

    let mut latest = HashMap::<CompactionKey, (u64, u64)>::new();
    for (seq, record) in messages.iter() {
        let (Some(stream), Some(partition), Some(offset), Some(key)) = (
            record.stream.as_deref(),
            record.partition,
            record.offset,
            record.key.as_ref(),
        ) else {
            continue;
        };
        if !compacted_streams.contains(stream) {
            continue;
        }
        let identity = (
            stream.to_string(),
            partition,
            record.namespace.clone(),
            key.clone(),
        );
        let candidate = (offset, *seq);
        latest
            .entry(identity)
            .and_modify(|current| *current = (*current).max(candidate))
            .or_insert(candidate);
    }

    messages.retain(|seq, record| {
        let (Some(stream), Some(partition), Some(offset), Some(key)) = (
            record.stream.as_deref(),
            record.partition,
            record.offset,
            record.key.as_ref(),
        ) else {
            return true;
        };
        if !compacted_streams.contains(stream) {
            return true;
        }
        latest.get(&(
            stream.to_string(),
            partition,
            record.namespace.clone(),
            key.clone(),
        )) == Some(&(offset, *seq))
    });
}

impl DurableBrokerState {
    pub(super) fn apply_stream_compaction(&mut self, catalog: &crate::stream::StreamCatalog) {
        compact_stream_records(&mut self.messages, catalog);
        self.partition_sequences
            .retain(|_, seq| self.messages.contains_key(seq));
    }
}
