use crate::{
    error::Result, partition_log::PartitionLogSet, stream::StreamCatalog, wal::PublishRecord,
};
use protocol::{StartPosition, subject};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};

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
    #[serde(default)]
    pub frontiers: BTreeMap<String, VecDeque<u64>>,
}

impl Default for ConsumerCursorSet {
    fn default() -> Self {
        Self {
            partitions: BTreeMap::new(),
            ack_window: 1024,
            frontiers: BTreeMap::new(),
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
            frontiers: BTreeMap::new(),
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
        is_leased: impl Fn(u64) -> bool,
    ) -> Option<u64> {
        self.ensure_frontiers(filter_subject, logs);
        self.partitions
            .values()
            .filter_map(|cursor| {
                let frontier = self
                    .frontiers
                    .get_mut(&cursor_key(&cursor.stream, cursor.partition))?;
                while let Some(offset) = frontier.front().copied() {
                    let key = (cursor.stream.clone(), cursor.partition, offset);
                    if offset < cursor.committed_offset || !partition_sequences.contains_key(&key) {
                        frontier.pop_front();
                    } else {
                        break;
                    }
                }
                frontier.iter().find_map(|offset| {
                    if cursor.acknowledged_offsets.contains(offset) {
                        return None;
                    }
                    partition_sequences
                        .get(&(cursor.stream.clone(), cursor.partition, *offset))
                        .filter(|seq| !is_leased(**seq))
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
        let key = cursor_key(
            record.stream.as_deref().unwrap_or_default(),
            record.partition.unwrap_or_default(),
        );
        let mut frontier = self.frontiers.remove(&key);
        let Some(cursor) = self.cursor_for_mut(record) else {
            if let Some(frontier) = frontier {
                self.frontiers.insert(key, frontier);
            }
            return Err(crate::error::BrokerError::msg(
                "consumer has no partition cursor",
            ));
        };
        if offset < cursor.committed_offset {
            if let Some(frontier) = frontier.take() {
                self.frontiers.insert(key, frontier);
            }
            return Ok(());
        }
        let closes_gap = frontier.as_ref().map_or_else(
            || next_matching_offset(cursor, filter_subject, messages) == Some(offset),
            |frontier| next_frontier_offset(cursor, frontier) == Some(offset),
        );
        let allowed = cursor.acknowledged_offsets.contains(&offset)
            || cursor.acknowledged_offsets.len() < ack_window
            || closes_gap;
        if !allowed {
            let _ = cursor;
            if let Some(frontier) = frontier {
                self.frontiers.insert(key, frontier);
            }
            crate::broker_bail!("consumer acknowledgement window exceeded");
        }
        cursor.acknowledged_offsets.insert(offset);
        if let Some(frontier) = frontier.as_ref() {
            advance_committed_from_frontier(cursor, frontier);
        } else {
            advance_committed(cursor, filter_subject, messages);
        }
        if let Some(frontier) = frontier {
            self.frontiers.insert(key, frontier);
        }
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
        let advanced = cursor.committed_offset < earliest_offset;
        if advanced {
            cursor.committed_offset = earliest_offset;
            cursor.retention_gaps = cursor.retention_gaps.saturating_add(1);
            cursor
                .acknowledged_offsets
                .retain(|offset| *offset >= earliest_offset);
        }
        if let Some(frontier) = self.frontiers.get_mut(&cursor_key(stream, partition)) {
            frontier.retain(|offset| *offset >= earliest_offset);
        }
        advanced
    }

    pub fn observe_published_record(&mut self, filter_subject: &str, record: &PublishRecord) {
        let (Some(stream), Some(partition), Some(offset)) =
            (record.stream.as_deref(), record.partition, record.offset)
        else {
            return;
        };
        if !subject::matches(filter_subject, &record.subject) {
            return;
        }
        let Some(frontier) = self.frontiers.get_mut(&cursor_key(stream, partition)) else {
            return;
        };
        if frontier.back().is_none_or(|last| *last < offset) {
            frontier.push_back(offset);
        } else if frontier.binary_search(&offset).is_err() {
            let position = frontier.partition_point(|candidate| *candidate < offset);
            frontier.insert(position, offset);
        }
    }

    pub fn remove_record(&mut self, stream: &str, partition: u32, offset: u64) {
        if let Some(frontier) = self.frontiers.get_mut(&cursor_key(stream, partition)) {
            frontier.retain(|candidate| *candidate != offset);
        }
    }

    fn ensure_frontiers(&mut self, filter_subject: &str, logs: &PartitionLogSet) {
        let missing = self
            .partitions
            .values()
            .filter(|cursor| {
                !self
                    .frontiers
                    .contains_key(&cursor_key(&cursor.stream, cursor.partition))
            })
            .map(|cursor| (cursor.stream.clone(), cursor.partition))
            .collect::<Vec<_>>();
        for (stream, partition) in missing {
            let offsets = logs
                .matching_offsets(
                    &stream,
                    crate::stream::PartitionId(partition),
                    filter_subject,
                )
                .map(|query| query.offsets)
                .unwrap_or_default();
            self.frontiers
                .insert(cursor_key(&stream, partition), offsets.into());
        }
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

fn advance_committed_from_frontier(cursor: &mut PartitionCursor, frontier: &VecDeque<u64>) {
    while let Some(next) = next_frontier_offset(cursor, frontier) {
        if !cursor.acknowledged_offsets.remove(&next) {
            break;
        }
        cursor.committed_offset = next.saturating_add(1);
    }
}

fn next_frontier_offset(cursor: &PartitionCursor, frontier: &VecDeque<u64>) -> Option<u64> {
    frontier
        .iter()
        .copied()
        .find(|offset| *offset >= cursor.committed_offset)
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
