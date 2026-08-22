use crate::{
    error::Result, partition_log::PartitionLogSet, stream::StreamCatalog, wal::PublishRecord,
};
use protocol::{StartPosition, subject};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct PartitionCursor {
    pub stream: String,
    pub partition: u32,
    pub committed_offset: u64,
    pub delivered_offset: Option<u64>,
    pub acknowledged_offsets: BTreeSet<u64>,
    pub retention_gaps: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct ConsumerCursorSet {
    pub partitions: BTreeMap<String, PartitionCursor>,
    pub ack_window: usize,
}

impl Default for ConsumerCursorSet {
    fn default() -> Self {
        Self {
            partitions: BTreeMap::new(),
            ack_window: 1024,
        }
    }
}

impl ConsumerCursorSet {
    pub fn new(
        filter_subject: &str,
        start: StartPosition,
        ack_window: usize,
        catalog: &StreamCatalog,
        messages: &HashMap<u64, PublishRecord>,
    ) -> Self {
        let mut partitions = BTreeMap::new();
        for stream in catalog.streams_for_filter(filter_subject) {
            for partition in 0..stream.partitions {
                let committed_offset =
                    starting_offset(start, stream.name.as_str(), partition, messages.values());
                let cursor = PartitionCursor {
                    stream: stream.name.as_str().to_string(),
                    partition,
                    committed_offset,
                    delivered_offset: None,
                    acknowledged_offsets: BTreeSet::new(),
                    retention_gaps: 0,
                };
                partitions.insert(cursor_key(&cursor.stream, partition), cursor);
            }
        }
        Self {
            partitions,
            ack_window,
        }
    }

    pub fn next_candidate(
        &mut self,
        filter_subject: &str,
        messages: &HashMap<u64, PublishRecord>,
        leased: &HashSet<u64>,
    ) -> Option<u64> {
        self.observe_retention(messages);
        messages
            .iter()
            .filter(|(seq, record)| {
                !leased.contains(seq)
                    && subject::matches(filter_subject, &record.subject)
                    && self.cursor_for(record).is_some_and(|cursor| {
                        record.offset.is_some_and(|offset| {
                            offset >= cursor.committed_offset
                                && !cursor.acknowledged_offsets.contains(&offset)
                        })
                    })
            })
            .min_by_key(|(_, record)| record_position(record))
            .map(|(seq, _)| *seq)
    }

    pub fn next_indexed_candidate(
        &mut self,
        filter_subject: &str,
        messages: &HashMap<u64, PublishRecord>,
        partition_sequences: &BTreeMap<(String, u32, u64), u64>,
        logs: &PartitionLogSet,
        leased: &HashSet<u64>,
    ) -> Option<u64> {
        self.partitions
            .values()
            .filter_map(|cursor| {
                logs.matching_offsets(
                    &cursor.stream,
                    crate::stream::PartitionId(cursor.partition),
                    filter_subject,
                )
                .ok()?
                .offsets
                .into_iter()
                .filter(|offset| {
                    *offset >= cursor.committed_offset
                        && !cursor.acknowledged_offsets.contains(offset)
                })
                .find_map(|offset| {
                    partition_sequences
                        .get(&(cursor.stream.clone(), cursor.partition, offset))
                        .filter(|seq| !leased.contains(seq))
                        .copied()
                })
            })
            .filter_map(|seq| {
                messages
                    .get(&seq)
                    .map(|record| (seq, record_position(record)))
            })
            .min_by_key(|(_, position)| *position)
            .map(|(seq, _)| seq)
    }

    pub fn mark_delivered(&mut self, record: &PublishRecord) {
        let Some(offset) = record.offset else {
            return;
        };
        if let Some(cursor) = self.cursor_for_mut(record) {
            cursor.delivered_offset = Some(cursor.delivered_offset.unwrap_or(0).max(offset));
        }
    }

    pub fn acknowledge(
        &mut self,
        record: &PublishRecord,
        filter_subject: &str,
        messages: &HashMap<u64, PublishRecord>,
    ) -> Result<()> {
        let offset = record
            .offset
            .ok_or_else(|| crate::error::BrokerError::msg("message has no partition offset"))?;
        let ack_window = self.ack_window;
        let cursor = self
            .cursor_for_mut(record)
            .ok_or_else(|| crate::error::BrokerError::msg("consumer has no partition cursor"))?;
        if offset < cursor.committed_offset {
            return Ok(());
        }
        let closes_gap = next_matching_offset(cursor, filter_subject, messages) == Some(offset);
        crate::broker_ensure!(
            cursor.acknowledged_offsets.contains(&offset)
                || cursor.acknowledged_offsets.len() < ack_window
                || closes_gap,
            "consumer acknowledgement window exceeded"
        );
        cursor.acknowledged_offsets.insert(offset);
        advance_committed(cursor, filter_subject, messages);
        Ok(())
    }

    pub fn committed_offset(&self, stream: &str, partition: u32) -> Option<u64> {
        self.partitions
            .get(&cursor_key(stream, partition))
            .map(|cursor| cursor.committed_offset)
    }

    pub fn apply_retention_floor(
        &mut self,
        stream: &str,
        partition: u32,
        earliest_offset: u64,
    ) -> bool {
        let Some(cursor) = self.partitions.get_mut(&cursor_key(stream, partition)) else {
            return false;
        };
        if cursor.committed_offset >= earliest_offset {
            return false;
        }
        cursor.committed_offset = earliest_offset;
        cursor.retention_gaps = cursor.retention_gaps.saturating_add(1);
        cursor
            .acknowledged_offsets
            .retain(|offset| *offset >= earliest_offset);
        true
    }

    fn cursor_for(&self, record: &PublishRecord) -> Option<&PartitionCursor> {
        self.partitions
            .get(&cursor_key(record.stream.as_deref()?, record.partition?))
    }

    fn cursor_for_mut(&mut self, record: &PublishRecord) -> Option<&mut PartitionCursor> {
        self.partitions
            .get_mut(&cursor_key(record.stream.as_deref()?, record.partition?))
    }

    fn observe_retention(&mut self, messages: &HashMap<u64, PublishRecord>) {
        for cursor in self.partitions.values_mut() {
            let earliest = messages
                .values()
                .filter(|record| {
                    record.stream.as_deref() == Some(cursor.stream.as_str())
                        && record.partition == Some(cursor.partition)
                })
                .filter_map(|record| record.offset)
                .min();
            if let Some(earliest) = earliest {
                if cursor.committed_offset < earliest {
                    cursor.committed_offset = earliest;
                    cursor.retention_gaps = cursor.retention_gaps.saturating_add(1);
                    cursor
                        .acknowledged_offsets
                        .retain(|offset| *offset >= earliest);
                }
            }
        }
    }
}

fn starting_offset<'a>(
    start: StartPosition,
    stream: &str,
    partition: u32,
    messages: impl Iterator<Item = &'a PublishRecord>,
) -> u64 {
    let records = messages
        .filter(|record| {
            record.stream.as_deref() == Some(stream) && record.partition == Some(partition)
        })
        .collect::<Vec<_>>();
    let high_watermark = records
        .iter()
        .filter_map(|record| record.offset)
        .max()
        .map(|offset| offset.saturating_add(1))
        .unwrap_or(0);
    match start {
        StartPosition::Earliest | StartPosition::Committed => 0,
        StartPosition::Latest => high_watermark,
        StartPosition::Offset(offset) => offset,
        StartPosition::Timestamp(timestamp) => records
            .iter()
            .filter(|record| record.timestamp_ms >= timestamp)
            .filter_map(|record| record.offset)
            .min()
            .unwrap_or(high_watermark),
    }
}

fn advance_committed(
    cursor: &mut PartitionCursor,
    filter_subject: &str,
    messages: &HashMap<u64, PublishRecord>,
) {
    loop {
        let next = next_matching_offset(cursor, filter_subject, messages);
        let Some(next) = next else {
            break;
        };
        if !cursor.acknowledged_offsets.remove(&next) {
            break;
        }
        cursor.committed_offset = next.saturating_add(1);
    }
}

fn next_matching_offset(
    cursor: &PartitionCursor,
    filter_subject: &str,
    messages: &HashMap<u64, PublishRecord>,
) -> Option<u64> {
    messages
        .values()
        .filter(|record| {
            record.stream.as_deref() == Some(cursor.stream.as_str())
                && record.partition == Some(cursor.partition)
                && subject::matches(filter_subject, &record.subject)
        })
        .filter_map(|record| record.offset)
        .filter(|offset| *offset >= cursor.committed_offset)
        .min()
}

fn record_position(record: &PublishRecord) -> (&str, u32, u64) {
    (
        record.stream.as_deref().unwrap_or_default(),
        record.partition.unwrap_or_default(),
        record.offset.unwrap_or_default(),
    )
}

fn cursor_key(stream: &str, partition: u32) -> String {
    format!("{stream}:{partition}")
}

#[cfg(test)]
#[path = "consumer_cursor/tests.rs"]
mod tests;
