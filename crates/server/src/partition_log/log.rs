use super::{codec::*, subject_index::*, *};
use crate::error::{BrokerError, ResultExt};
use std::{
    collections::{BTreeSet, HashSet, VecDeque},
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::PathBuf,
};

const SEGMENT_EXTENSION: &str = "plog";
const INDEX_EXTENSION: &str = "idx";
const INDEX_STRIDE: u64 = 64;
const RETENTION_OFFSET_FILE: &str = "retention-offset";
const REWRITE_TMP_FILE: &str = "rewrite.plog.tmp";
const REWRITE_MARKER_FILE: &str = "rewrite.marker";
const COMPACTION_TMP_FILE: &str = "compact.plog.tmp";
const COMPACTION_MARKER_FILE: &str = "compact.marker";

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
    /// Sealed segment readers are opened on demand and released after each
    /// lookup so descriptor usage does not grow with retained history.
    reader: Option<File>,
    sparse_index: Vec<(u64, u64)>,
}

#[derive(Debug)]
pub(super) struct PartitionLog {
    dir: PathBuf,
    stream: StreamId,
    partition: PartitionId,
    file: Option<File>,
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
    encryption: Option<std::sync::Arc<crate::encryption::KeyRing>>,
}

impl PartitionLog {
    pub(super) fn open(
        root: &Path,
        stream: &StreamId,
        partition: PartitionId,
        segment_bytes: u64,
        encryption: Option<std::sync::Arc<crate::encryption::KeyRing>>,
    ) -> Result<(Self, Vec<MessageEnvelope>, u64)> {
        let dir = root
            .join(stream.as_str())
            .join(format!("partition-{:05}", partition.0));
        std::fs::create_dir_all(&dir)
            .with_context(|| format!("creating partition-log directory {}", dir.display()))?;
        install_pending_rewrite(&dir)?;
        install_pending_segment_compaction(&dir)?;
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
                encryption.as_ref(),
            )?;
            truncations += repaired;
            if repaired > 0 || !path.with_extension(INDEX_EXTENSION).exists() {
                rebuild_index(path, stream, partition, encryption.as_ref())?;
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
                    reader: None,
                    sparse_index: load_sparse_index(path)?,
                });
            }
            if active {
                active_subjects = subjects;
            } else {
                let mut segment = SubjectSegment::new(path.clone(), subjects, encryption.clone());
                subject_index_cache_bytes += segment.rebuild(
                    MAX_PARTITION_INDEX_CACHE_BYTES.saturating_sub(subject_index_cache_bytes),
                )?;
                sealed_subject_segments.push(segment);
            }
        }
        envelopes.sort_by_key(|envelope| envelope.offset);
        for pair in envelopes.windows(2) {
            crate::broker_ensure!(
                pair[0].offset < pair[1].offset,
                "partition offsets are not strictly increasing in stream {} partition {}",
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
            persisted_next_offset.max(envelope.offset.saturating_add(1))
        });
        Ok((
            Self {
                dir,
                stream: stream.clone(),
                partition,
                file: Some(file),
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
                encryption,
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
        let batch = match &self.encryption {
            Some(encryption) => encode_encrypted_batch_with_len(&envelope, encryption)?,
            None => encode_batch_with_len(&envelope)?,
        };
        if self.active_bytes > SEGMENT_HEADER_LEN
            && self.active_bytes.saturating_add(batch.len) > self.segment_bytes
        {
            self.rotate()?;
        }
        let position = self.active_bytes;
        self.active_file()?.write_all(&batch.bytes)?;
        self.active_bytes += batch.len;
        self.next_offset += 1;
        let bytes = batch.len;
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
                path: active_path.clone(),
                first_offset: envelope.offset,
                last_offset: envelope.offset,
                reader: None,
                sparse_index: load_sparse_index(&active_path)?,
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
            if let Some(range) = self
                .segment_ranges
                .iter_mut()
                .find(|range| range.path == active_path)
            {
                range.sparse_index.push((envelope.offset, position));
            }
        }
        Ok(envelope)
    }

    pub(super) fn flush(&mut self) -> Result<()> {
        if let Some(file) = self.file.as_mut() {
            file.flush()?;
            file.sync_data()?;
        }
        Ok(())
    }

    pub(super) fn release_resources(&mut self) -> Result<()> {
        self.flush()?;
        self.file = None;
        Ok(())
    }

    pub(super) fn rewrite(
        &mut self,
        records: &[MessageEnvelope],
        next_offset_floor: Option<u64>,
    ) -> Result<()> {
        self.flush()?;
        let next_offset = records
            .last()
            .map_or(0, |record| record.offset.saturating_add(1))
            .max(next_offset_floor.unwrap_or_default());
        stage_rewrite(&self.dir, records, next_offset, self.encryption.as_ref())?;
        install_pending_rewrite(&self.dir)?;
        let root = self
            .dir
            .parent()
            .and_then(Path::parent)
            .ok_or_else(|| BrokerError::msg("partition log has no stream root"))?;
        let (mut replacement, _, _) = PartitionLog::open(
            root,
            &self.stream,
            self.partition,
            self.segment_bytes,
            self.encryption.clone(),
        )?;
        replacement.deleted_messages = self.deleted_messages;
        replacement.deleted_bytes = self.deleted_bytes;
        *self = replacement;
        Ok(())
    }

    pub(super) fn compact_visible_offsets(
        &mut self,
        visible_offsets: &BTreeSet<u64>,
    ) -> Result<bool> {
        let sealed_ranges = self.segment_ranges.len().saturating_sub(1);
        for range_index in 0..sealed_ranges {
            let path = self.segment_ranges[range_index].path.clone();
            let records = read_segment_records(&path, self.encryption.as_ref())?;
            let retained = records
                .iter()
                .filter(|record| visible_offsets.contains(&record.offset))
                .cloned()
                .collect::<Vec<_>>();
            if retained.len() == records.len() {
                continue;
            }

            stage_segment_compaction(&self.dir, &path, &retained, self.encryption.as_ref())?;
            install_pending_segment_compaction(&self.dir)?;
            rebuild_index(
                &path,
                &self.stream,
                self.partition,
                self.encryption.as_ref(),
            )?;

            let retained_offsets = retained
                .iter()
                .map(|record| record.offset)
                .collect::<HashSet<_>>();
            let old_bytes = self
                .retention_records
                .iter()
                .filter(|record| {
                    record.offset >= self.segment_ranges[range_index].first_offset
                        && record.offset <= self.segment_ranges[range_index].last_offset
                        && !retained_offsets.contains(&record.offset)
                })
                .map(|record| record.bytes)
                .sum::<u64>();
            let removed = self
                .retention_records
                .iter()
                .filter(|record| {
                    record.offset >= self.segment_ranges[range_index].first_offset
                        && record.offset <= self.segment_ranges[range_index].last_offset
                        && !retained_offsets.contains(&record.offset)
                })
                .count() as u64;
            self.retention_records.retain(|record| {
                retained_offsets.contains(&record.offset)
                    || record.offset < self.segment_ranges[range_index].first_offset
                    || record.offset > self.segment_ranges[range_index].last_offset
            });
            self.retained_bytes = self.retained_bytes.saturating_sub(old_bytes);
            self.deleted_messages = self.deleted_messages.saturating_add(removed);
            self.deleted_bytes = self.deleted_bytes.saturating_add(old_bytes);

            if retained.is_empty() {
                self.segment_ranges.remove(range_index);
                self.sealed_subject_segments
                    .retain(|segment| segment.path != path);
                remove_segment_files(&path)?;
            } else {
                let range = &mut self.segment_ranges[range_index];
                range.first_offset = retained[0].offset;
                range.last_offset = retained.last().unwrap().offset;
                range.reader = None;
                range.sparse_index = load_sparse_index(&path)?;
                if let Some(segment) = self
                    .sealed_subject_segments
                    .iter_mut()
                    .find(|segment| segment.path == path)
                {
                    *segment = SubjectSegment::new(
                        path.clone(),
                        retained
                            .iter()
                            .map(|record| (record.subject.clone(), record.offset))
                            .collect(),
                        self.encryption.clone(),
                    );
                    segment.rebuild(0)?;
                }
            }
            return Ok(true);
        }
        Ok(false)
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

    /// Apply a logical retention floor without requiring the broker state to
    /// materialize the retained window. Obsolete sealed segments are removed
    /// before the boundary rewrite; the rewrite path remains the recovery-safe
    /// fallback for a segment that straddles the floor.
    pub(super) fn advance_retention_floor(&mut self, earliest_offset: u64) -> Result<()> {
        if earliest_offset > self.next_offset {
            return Ok(());
        }
        let retained = (earliest_offset..self.next_offset)
            .filter_map(|offset| self.read_offset(offset).transpose())
            .collect::<Result<Vec<_>>>()?;
        self.rewrite(&retained, Some(earliest_offset))
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
            self.encryption.clone(),
        );
        self.subject_index_cache_bytes += sealed.rebuild(
            MAX_PARTITION_INDEX_CACHE_BYTES.saturating_sub(self.subject_index_cache_bytes),
        )?;
        let sealed_path = segment_path(&self.dir, self.segment_id);
        if let Some(range) = self
            .segment_ranges
            .iter_mut()
            .find(|range| range.path == sealed_path)
        {
            range.sparse_index = load_sparse_index(&sealed_path)?;
        }
        self.sealed_subject_segments.push(sealed);
        self.segment_id += 1;
        let path = segment_path(&self.dir, self.segment_id);
        self.file = Some(create_segment(&path)?);
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

    pub(super) fn read_offset(&mut self, offset: u64) -> Result<Option<MessageEnvelope>> {
        for range in &mut self.segment_ranges {
            if offset < range.first_offset || offset > range.last_offset {
                continue;
            }
            let mut reader = open_segment_reader(&range.path)?;
            reader.seek(SeekFrom::Start(SEGMENT_HEADER_LEN))?;
            if let Some(position) = indexed_position(&range.sparse_index, offset) {
                reader.seek(SeekFrom::Start(position))?;
            }
            while let Some((envelope, _)) =
                read_batch_with_key(&mut reader, self.encryption.as_ref())?
            {
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

    #[cfg(test)]
    pub(super) fn stage_rewrite_for_test(
        &self,
        records: &[MessageEnvelope],
        next_offset: u64,
    ) -> Result<()> {
        stage_rewrite(&self.dir, records, next_offset, self.encryption.as_ref())
    }

    fn active_file(&mut self) -> Result<&mut File> {
        if self.file.is_none() {
            let path = segment_path(&self.dir, self.segment_id);
            self.file = Some(
                OpenOptions::new()
                    .read(true)
                    .append(true)
                    .open(&path)
                    .with_context(|| {
                        format!("reopening partition-log segment {}", path.display())
                    })?,
            );
        }
        Ok(self.file.as_mut().expect("active file opened above"))
    }
}

fn open_segment_reader(path: &Path) -> Result<File> {
    let mut file = OpenOptions::new().read(true).open(path)?;
    validate_segment_header(&mut file, path)?;
    Ok(file)
}

fn read_segment_records(
    path: &Path,
    encryption: Option<&std::sync::Arc<crate::encryption::KeyRing>>,
) -> Result<Vec<MessageEnvelope>> {
    let mut file = OpenOptions::new().read(true).open(path)?;
    validate_segment_header(&mut file, path)?;
    let mut records = Vec::new();
    while let Some((record, _)) = read_batch_with_key(&mut file, encryption)? {
        records.push(record);
    }
    Ok(records)
}

fn stage_segment_compaction(
    dir: &Path,
    path: &Path,
    records: &[MessageEnvelope],
    encryption: Option<&std::sync::Arc<crate::encryption::KeyRing>>,
) -> Result<()> {
    let tmp = dir.join(COMPACTION_TMP_FILE);
    if tmp.exists() {
        std::fs::remove_file(&tmp)?;
    }
    let mut file = create_segment(&tmp)?;
    for record in records {
        let batch = match encryption {
            Some(encryption) => encode_encrypted_batch_with_len(record, encryption)?,
            None => encode_batch_with_len(record)?,
        };
        file.write_all(&batch.bytes)?;
    }
    file.flush()?;
    file.sync_data()?;
    let marker = dir.join(COMPACTION_MARKER_FILE);
    let target = path
        .file_name()
        .ok_or_else(|| BrokerError::msg("partition compaction target has no filename"))?;
    std::fs::write(&marker, target.to_string_lossy().as_bytes())?;
    OpenOptions::new()
        .read(true)
        .write(true)
        .open(&marker)?
        .sync_data()?;
    crate::storage::sync_dir(dir)?;
    Ok(())
}

fn install_pending_segment_compaction(dir: &Path) -> Result<()> {
    let marker = dir.join(COMPACTION_MARKER_FILE);
    if !marker.exists() {
        return Ok(());
    }
    let target = String::from_utf8(std::fs::read(&marker)?)
        .map_err(|_| BrokerError::msg("invalid partition compaction marker"))?;
    let target = dir.join(target);
    let tmp = dir.join(COMPACTION_TMP_FILE);
    if tmp.exists() {
        for extension in [INDEX_EXTENSION, "sidx"] {
            let sidecar = target.with_extension(extension);
            if sidecar.exists() {
                std::fs::remove_file(sidecar)?;
            }
        }
        std::fs::rename(&tmp, &target)?;
    }
    std::fs::remove_file(marker)?;
    crate::storage::sync_dir(dir)?;
    Ok(())
}

fn load_sparse_index(path: &Path) -> Result<Vec<(u64, u64)>> {
    let index_path = path.with_extension(INDEX_EXTENSION);
    if !index_path.exists() {
        return Ok(Vec::new());
    }
    let mut file = File::open(index_path)?;
    let mut entries = Vec::new();
    let mut entry = [0_u8; 16];
    loop {
        match file.read_exact(&mut entry) {
            Ok(()) => entries.push((
                u64::from_le_bytes(entry[..8].try_into().unwrap()),
                u64::from_le_bytes(entry[8..].try_into().unwrap()),
            )),
            Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(err) => return Err(err.into()),
        }
    }
    Ok(entries)
}

fn indexed_position(index: &[(u64, u64)], target: u64) -> Option<u64> {
    match index.binary_search_by_key(&target, |(offset, _)| *offset) {
        Ok(position) => Some(index[position].1),
        Err(0) => None,
        Err(position) => Some(index[position - 1].1),
    }
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
    OpenOptions::new()
        .read(true)
        .write(true)
        .open(&tmp)?
        .sync_data()?;
    std::fs::rename(tmp, path)?;
    crate::storage::sync_dir(dir)?;
    Ok(())
}

fn stage_rewrite(
    dir: &Path,
    records: &[MessageEnvelope],
    next_offset: u64,
    encryption: Option<&std::sync::Arc<crate::encryption::KeyRing>>,
) -> Result<()> {
    crate::broker_ensure!(
        records
            .windows(2)
            .all(|pair| pair[0].offset < pair[1].offset),
        "partition rewrite offsets are not strictly increasing"
    );
    crate::broker_ensure!(
        records
            .last()
            .is_none_or(|record| record.offset < next_offset),
        "partition rewrite next offset does not exceed retained offsets"
    );
    let tmp = dir.join(REWRITE_TMP_FILE);
    if tmp.exists() {
        std::fs::remove_file(&tmp)?;
    }
    let mut file = create_segment(&tmp)?;
    for record in records {
        let batch = match encryption {
            Some(encryption) => encode_encrypted_batch_with_len(record, encryption)?,
            None => encode_batch_with_len(record)?,
        };
        file.write_all(&batch.bytes)?;
    }
    file.flush()?;
    file.sync_data()?;
    let marker = dir.join(REWRITE_MARKER_FILE);
    std::fs::write(&marker, next_offset.to_le_bytes())?;
    OpenOptions::new()
        .read(true)
        .write(true)
        .open(&marker)?
        .sync_data()?;
    crate::storage::sync_dir(dir)?;
    Ok(())
}

fn install_pending_rewrite(dir: &Path) -> Result<()> {
    let marker = dir.join(REWRITE_MARKER_FILE);
    if !marker.exists() {
        return Ok(());
    }
    let bytes = std::fs::read(&marker)?;
    crate::broker_ensure!(bytes.len() == 8, "invalid partition rewrite marker");
    let next_offset = u64::from_le_bytes(bytes.try_into().unwrap());
    let tmp = dir.join(REWRITE_TMP_FILE);
    let canonical = segment_path(dir, 1);
    if tmp.exists() {
        for (_, path) in segment_paths(dir)? {
            remove_segment_files(&path)?;
        }
        std::fs::rename(&tmp, &canonical)?;
    } else {
        crate::broker_ensure!(canonical.exists(), "partition rewrite data is missing");
        for (_, path) in segment_paths(dir)? {
            if path != canonical {
                remove_segment_files(&path)?;
            }
        }
    }
    persist_retention_offset(dir, next_offset)?;
    std::fs::remove_file(marker)?;
    crate::storage::sync_dir(dir)?;
    Ok(())
}

fn remove_segment_files(path: &Path) -> Result<()> {
    std::fs::remove_file(path)?;
    for extension in [INDEX_EXTENSION, "sidx"] {
        let sidecar = path.with_extension(extension);
        if sidecar.exists() {
            std::fs::remove_file(sidecar)?;
        }
    }
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
    encryption: Option<&std::sync::Arc<crate::encryption::KeyRing>>,
) -> Result<u64> {
    let mut file = OpenOptions::new().read(true).write(active).open(path)?;
    validate_segment_header(&mut file, path)?;
    let file_len = file.metadata()?.len();
    let mut boundary = SEGMENT_HEADER_LEN;
    loop {
        let before = boundary;
        match read_batch_with_key(&mut file, encryption) {
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

fn rebuild_index(
    path: &Path,
    stream: &StreamId,
    partition: PartitionId,
    encryption: Option<&std::sync::Arc<crate::encryption::KeyRing>>,
) -> Result<()> {
    let index_path = path.with_extension(INDEX_EXTENSION);
    let tmp_path = path.with_extension("idx.tmp");
    let mut source = OpenOptions::new().read(true).open(path)?;
    validate_segment_header(&mut source, path)?;
    let mut index = File::create(&tmp_path)?;
    let mut position = SEGMENT_HEADER_LEN;
    while let Some((envelope, bytes)) = read_batch_with_key(&mut source, encryption)? {
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
