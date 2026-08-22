use super::{codec::*, subject_index::*, *};
use crate::error::{BrokerError, ResultExt};
use std::{
    collections::{BTreeSet, VecDeque},
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::PathBuf,
};

const SEGMENT_EXTENSION: &str = "plog";
const INDEX_EXTENSION: &str = "idx";
const INDEX_STRIDE: u64 = 64;
const RETENTION_OFFSET_FILE: &str = "retention-offset";

#[derive(Debug)]
struct RetentionRecord {
    offset: u64,
    timestamp_ms: u64,
    bytes: u64,
}

#[derive(Debug)]
struct SegmentRange {
    path: PathBuf,
    first_offset: u64,
    last_offset: u64,
}

#[derive(Debug)]
pub(super) struct PartitionLog {
    dir: PathBuf,
    file: File,
    segment_id: u64,
    active_bytes: u64,
    segment_bytes: u64,
    next_offset: u64,
    segment_ranges: Vec<SegmentRange>,
    sealed_subject_segments: Vec<SubjectSegment>,
    active_subjects: Vec<(String, u64)>,
    subject_index_cache_bytes: usize,
    retention_records: VecDeque<RetentionRecord>,
    retained_bytes: u64,
    deleted_messages: u64,
    deleted_bytes: u64,
}

impl PartitionLog {
    pub(super) fn open(
        root: &Path,
        stream: &StreamId,
        partition: PartitionId,
        segment_bytes: u64,
    ) -> Result<(Self, Vec<MessageEnvelope>, u64)> {
        let dir = root
            .join(stream.as_str())
            .join(format!("partition-{:05}", partition.0));
        std::fs::create_dir_all(&dir)
            .with_context(|| format!("creating partition-log directory {}", dir.display()))?;
        let mut paths = segment_paths(&dir)?;
        if paths.is_empty() {
            let path = segment_path(&dir, 1);
            create_segment(&path)?;
            paths.push((1, path));
        }

        let mut envelopes = Vec::new();
        let mut truncations = 0;
        let mut sealed_subject_segments = Vec::new();
        let mut active_subjects = Vec::new();
        let mut subject_index_cache_bytes = 0;
        let mut retention_records = VecDeque::new();
        let mut retained_bytes = 0_u64;
        let mut segment_ranges = Vec::new();
        for (index, (_, path)) in paths.iter().enumerate() {
            let active = index + 1 == paths.len();
            let first_record = envelopes.len();
            let repaired = replay_segment(
                path,
                active,
                stream,
                partition,
                &mut envelopes,
                &mut retention_records,
                &mut retained_bytes,
            )?;
            truncations += repaired;
            if repaired > 0 || !path.with_extension(INDEX_EXTENSION).exists() {
                rebuild_index(path, stream, partition)?;
            }
            let subjects = envelopes[first_record..]
                .iter()
                .map(|envelope| (envelope.subject.clone(), envelope.offset))
                .collect::<Vec<_>>();
            if let (Some(first), Some(last)) = (
                envelopes[first_record..].first(),
                envelopes[first_record..].last(),
            ) {
                segment_ranges.push(SegmentRange {
                    path: path.clone(),
                    first_offset: first.offset,
                    last_offset: last.offset,
                });
            }
            if active {
                active_subjects = subjects;
            } else {
                let mut segment = SubjectSegment::new(path.clone(), subjects);
                subject_index_cache_bytes += segment.rebuild(
                    MAX_PARTITION_INDEX_CACHE_BYTES.saturating_sub(subject_index_cache_bytes),
                )?;
                sealed_subject_segments.push(segment);
            }
        }
        envelopes.sort_by_key(|envelope| envelope.offset);
        let first_offset = envelopes.first().map_or(0, |envelope| envelope.offset);
        for (index, envelope) in envelopes.iter().enumerate() {
            crate::broker_ensure!(
                envelope.offset == first_offset.saturating_add(index as u64),
                "non-contiguous offset {} in stream {} partition {}",
                envelope.offset,
                stream.as_str(),
                partition.0
            );
        }
        let (segment_id, active_path) = paths.pop().unwrap();
        let active_bytes = active_path.metadata()?.len();
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(&active_path)
            .with_context(|| format!("opening partition-log segment {}", active_path.display()))?;
        let persisted_next_offset = read_retention_offset(&dir)?;
        let next_offset = envelopes.last().map_or(persisted_next_offset, |envelope| {
            envelope.offset.saturating_add(1)
        });
        crate::broker_ensure!(
            persisted_next_offset <= next_offset,
            "partition-log retention offset exceeds the durable high watermark"
        );
        Ok((
            Self {
                dir,
                file,
                segment_id,
                active_bytes,
                segment_bytes,
                next_offset,
                segment_ranges,
                sealed_subject_segments,
                active_subjects,
                subject_index_cache_bytes,
                retention_records,
                retained_bytes,
                deleted_messages: 0,
                deleted_bytes: 0,
            },
            envelopes,
            truncations,
        ))
    }

    pub(super) fn append(&mut self, mut envelope: MessageEnvelope) -> Result<MessageEnvelope> {
        envelope.offset = self.next_offset;
        self.append_committed(envelope)
    }

    pub(super) fn append_committed(
        &mut self,
        envelope: MessageEnvelope,
    ) -> Result<MessageEnvelope> {
        if envelope.offset < self.next_offset {
            let existing = self.read_offset(envelope.offset)?;
            crate::broker_ensure!(
                existing.as_ref().map(envelope_checksum).transpose()?
                    == Some(envelope_checksum(&envelope)?),
                "partition-log append conflicts with an immutable committed record"
            );
            return Ok(envelope);
        }
        crate::broker_ensure!(
            envelope.offset == self.next_offset,
            "partition-log append creates an offset gap"
        );
        let batch = encode_batch(&envelope)?;
        if self.active_bytes > SEGMENT_HEADER_LEN
            && self.active_bytes.saturating_add(batch.len() as u64) > self.segment_bytes
        {
            self.rotate()?;
        }
        let position = self.active_bytes;
        self.file.write_all(&batch)?;
        self.active_bytes += batch.len() as u64;
        self.next_offset += 1;
        let bytes = encoded_batch_len(&envelope)?;
        self.retained_bytes = self.retained_bytes.saturating_add(bytes);
        self.retention_records.push_back(RetentionRecord {
            offset: envelope.offset,
            timestamp_ms: envelope.timestamp_ms,
            bytes,
        });
        let active_path = segment_path(&self.dir, self.segment_id);
        if let Some(range) = self
            .segment_ranges
            .iter_mut()
            .find(|range| range.path == active_path)
        {
            range.last_offset = envelope.offset;
        } else {
            self.segment_ranges.push(SegmentRange {
                path: active_path,
                first_offset: envelope.offset,
                last_offset: envelope.offset,
            });
        }
        self.active_subjects
            .push((envelope.subject.clone(), envelope.offset));
        if envelope.offset % INDEX_STRIDE == 0 {
            append_index(
                &segment_path(&self.dir, self.segment_id),
                envelope.offset,
                position,
            )?;
        }
        Ok(envelope)
    }

    pub(super) fn flush(&mut self) -> Result<()> {
        self.file.flush()?;
        self.file.sync_data()?;
        Ok(())
    }

    pub(super) fn rewrite(
        &mut self,
        records: &[MessageEnvelope],
        empty_next_offset: Option<u64>,
    ) -> Result<()> {
        let next_offset = records.last().map_or_else(
            || empty_next_offset.unwrap_or_default(),
            |record| record.offset.saturating_add(1),
        );
        let first_offset = records.first().map_or(next_offset, |record| record.offset);
        self.flush()?;
        for (_, path) in segment_paths(&self.dir)? {
            std::fs::remove_file(&path)?;
            let index = path.with_extension(INDEX_EXTENSION);
            if index.exists() {
                std::fs::remove_file(index)?;
            }
            let subject_index = path.with_extension("sidx");
            if subject_index.exists() {
                std::fs::remove_file(subject_index)?;
            }
        }
        self.segment_id = 1;
        let path = segment_path(&self.dir, self.segment_id);
        self.file = create_segment(&path)?;
        self.active_bytes = SEGMENT_HEADER_LEN;
        self.next_offset = first_offset;
        self.segment_ranges.clear();
        self.sealed_subject_segments.clear();
        self.active_subjects.clear();
        self.subject_index_cache_bytes = 0;
        self.retention_records.clear();
        self.retained_bytes = 0;
        for record in records {
            self.append_committed(record.clone())?;
        }
        crate::broker_ensure!(
            self.next_offset == next_offset,
            "partition rewrite offset mismatch"
        );
        self.flush()?;
        persist_retention_offset(&self.dir, next_offset)
    }

    pub(super) fn enforce_retention(
        &mut self,
        policy: &RetentionPolicy,
        now_ms: u64,
    ) -> Option<u64> {
        let before = self.earliest_offset();
        if let Some(max_age_ms) = policy.max_age_ms {
            while self
                .retention_records
                .front()
                .is_some_and(|record| now_ms.saturating_sub(record.timestamp_ms) > max_age_ms)
            {
                self.remove_oldest_retention_record();
            }
        }
        if let Some(max_bytes) = policy.max_bytes {
            while self.retained_bytes > max_bytes && !self.retention_records.is_empty() {
                self.remove_oldest_retention_record();
            }
        }
        let after = self.earliest_offset();
        (after != before).then_some(after)
    }

    pub(super) fn retention_status(&self, partition: PartitionId) -> PartitionRetentionStatus {
        PartitionRetentionStatus {
            partition: partition.0,
            earliest_offset: self.earliest_offset(),
            next_offset: self.next_offset,
            retained_messages: self.retention_records.len(),
            retained_bytes: self.retained_bytes,
            deleted_messages: self.deleted_messages,
            deleted_bytes: self.deleted_bytes,
        }
    }

    fn earliest_offset(&self) -> u64 {
        self.retention_records
            .front()
            .map_or(self.next_offset, |record| record.offset)
    }

    fn remove_oldest_retention_record(&mut self) {
        if let Some(record) = self.retention_records.pop_front() {
            self.retained_bytes = self.retained_bytes.saturating_sub(record.bytes);
            self.deleted_messages = self.deleted_messages.saturating_add(1);
            self.deleted_bytes = self.deleted_bytes.saturating_add(record.bytes);
        }
    }

    fn rotate(&mut self) -> Result<()> {
        self.flush()?;
        let mut sealed = SubjectSegment::new(
            segment_path(&self.dir, self.segment_id),
            std::mem::take(&mut self.active_subjects),
        );
        self.subject_index_cache_bytes += sealed.rebuild(
            MAX_PARTITION_INDEX_CACHE_BYTES.saturating_sub(self.subject_index_cache_bytes),
        )?;
        self.sealed_subject_segments.push(sealed);
        self.segment_id += 1;
        let path = segment_path(&self.dir, self.segment_id);
        self.file = create_segment(&path)?;
        self.active_bytes = SEGMENT_HEADER_LEN;
        Ok(())
    }

    pub(super) fn matching_offsets(&self, filter: &str) -> Result<SubjectIndexQuery> {
        let mut offsets = BTreeSet::new();
        let mut used_index = false;
        for segment in &self.sealed_subject_segments {
            let query = segment.matching_offsets(filter)?;
            offsets.extend(query.offsets);
            used_index |= query.used_index;
        }
        offsets.extend(active_matching_offsets(&self.active_subjects, filter).offsets);
        Ok(SubjectIndexQuery {
            offsets: offsets.into_iter().collect(),
            used_index,
        })
    }

    pub(super) fn read_offset(&self, offset: u64) -> Result<Option<MessageEnvelope>> {
        for range in &self.segment_ranges {
            if offset < range.first_offset || offset > range.last_offset {
                continue;
            }
            let mut file = OpenOptions::new().read(true).open(&range.path)?;
            validate_segment_header(&mut file, &range.path)?;
            if let Some(position) = indexed_position(&range.path, offset)? {
                file.seek(SeekFrom::Start(position))?;
            }
            while let Some((envelope, _)) = read_batch(&mut file)? {
                if envelope.offset == offset {
                    return Ok(Some(envelope));
                }
                if envelope.offset > offset {
                    break;
                }
            }
        }
        Ok(None)
    }
}

fn indexed_position(path: &Path, target: u64) -> Result<Option<u64>> {
    let index_path = path.with_extension(INDEX_EXTENSION);
    if !index_path.exists() {
        return Ok(None);
    }
    let mut file = File::open(index_path)?;
    let mut entry = [0_u8; 16];
    let mut position = None;
    loop {
        match file.read_exact(&mut entry) {
            Ok(()) => {
                let offset = u64::from_le_bytes(entry[..8].try_into().unwrap());
                if offset > target {
                    break;
                }
                position = Some(u64::from_le_bytes(entry[8..].try_into().unwrap()));
            }
            Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(err) => return Err(err.into()),
        }
    }
    Ok(position)
}

fn read_retention_offset(dir: &Path) -> Result<u64> {
    let path = dir.join(RETENTION_OFFSET_FILE);
    if !path.exists() {
        return Ok(0);
    }
    let bytes = std::fs::read(&path)?;
    crate::broker_ensure!(bytes.len() == 8, "invalid partition retention offset");
    Ok(u64::from_le_bytes(bytes.try_into().unwrap()))
}

fn persist_retention_offset(dir: &Path, next_offset: u64) -> Result<()> {
    let path = dir.join(RETENTION_OFFSET_FILE);
    let tmp = dir.join(format!("{RETENTION_OFFSET_FILE}.tmp"));
    std::fs::write(&tmp, next_offset.to_le_bytes())?;
    OpenOptions::new().read(true).open(&tmp)?.sync_data()?;
    std::fs::rename(tmp, path)?;
    File::open(dir)?.sync_data()?;
    Ok(())
}

fn replay_segment(
    path: &Path,
    active: bool,
    stream: &StreamId,
    partition: PartitionId,
    envelopes: &mut Vec<MessageEnvelope>,
    retention_records: &mut VecDeque<RetentionRecord>,
    retained_bytes: &mut u64,
) -> Result<u64> {
    let mut file = OpenOptions::new().read(true).write(active).open(path)?;
    validate_segment_header(&mut file, path)?;
    let file_len = file.metadata()?.len();
    let mut boundary = SEGMENT_HEADER_LEN;
    loop {
        let before = boundary;
        match read_batch(&mut file) {
            Ok(Some((mut envelope, bytes))) => {
                crate::broker_ensure!(
                    envelope.stream == *stream && envelope.partition == partition,
                    "partition-log envelope stored under the wrong stream or partition"
                );
                boundary += bytes;
                retention_records.push_back(RetentionRecord {
                    offset: envelope.offset,
                    timestamp_ms: envelope.timestamp_ms,
                    bytes,
                });
                *retained_bytes = retained_bytes.saturating_add(bytes);
                envelope.payload.clear();
                envelope.payload.shrink_to_fit();
                envelopes.push(envelope);
            }
            Ok(None) => return Ok(0),
            Err(err)
                if active
                    && (err.kind() == std::io::ErrorKind::UnexpectedEof
                        || (err.kind() == std::io::ErrorKind::InvalidData
                            && file.stream_position()? == file_len)) =>
            {
                file.set_len(before)?;
                return Ok(1);
            }
            Err(err) => {
                return Err(BrokerError::with_source(
                    format!("corrupt partition-log segment {}", path.display()),
                    err,
                ));
            }
        }
    }
}

fn rebuild_index(path: &Path, stream: &StreamId, partition: PartitionId) -> Result<()> {
    let index_path = path.with_extension(INDEX_EXTENSION);
    let tmp_path = path.with_extension("idx.tmp");
    let mut source = OpenOptions::new().read(true).open(path)?;
    validate_segment_header(&mut source, path)?;
    let mut index = File::create(&tmp_path)?;
    let mut position = SEGMENT_HEADER_LEN;
    while let Some((envelope, bytes)) = read_batch(&mut source)? {
        if envelope.stream == *stream
            && envelope.partition == partition
            && envelope.offset % INDEX_STRIDE == 0
        {
            write_index_entry(&mut index, envelope.offset, position)?;
        }
        position += bytes;
    }
    index.flush()?;
    std::fs::rename(tmp_path, index_path)?;
    Ok(())
}

fn append_index(path: &Path, offset: u64, position: u64) -> Result<()> {
    let mut index = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path.with_extension(INDEX_EXTENSION))?;
    write_index_entry(&mut index, offset, position)
}

fn write_index_entry(file: &mut File, offset: u64, position: u64) -> Result<()> {
    file.write_all(&offset.to_le_bytes())?;
    file.write_all(&position.to_le_bytes())?;
    Ok(())
}

fn segment_paths(dir: &Path) -> Result<Vec<(u64, PathBuf)>> {
    let mut paths = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        if path.extension().and_then(|value| value.to_str()) != Some(SEGMENT_EXTENSION) {
            continue;
        }
        let Some(stem) = path.file_stem().and_then(|value| value.to_str()) else {
            continue;
        };
        if stem.len() == 20 && stem.chars().all(|value| value.is_ascii_digit()) {
            let id = stem
                .parse()
                .map_err(|_| BrokerError::msg("invalid partition-log segment id"))?;
            paths.push((id, path));
        }
    }
    paths.sort_by_key(|(id, _)| *id);
    Ok(paths)
}

fn segment_path(dir: &Path, id: u64) -> PathBuf {
    dir.join(format!("{id:020}.{SEGMENT_EXTENSION}"))
}
