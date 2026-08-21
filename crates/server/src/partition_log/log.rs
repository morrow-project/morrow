use super::{codec::*, subject_index::*, *};
use crate::error::{BrokerError, ResultExt};
use std::{
    collections::{BTreeSet, HashMap},
    fs::{File, OpenOptions},
    io::{Seek, Write},
    path::PathBuf,
};

const SEGMENT_EXTENSION: &str = "plog";
const INDEX_EXTENSION: &str = "idx";
const INDEX_STRIDE: u64 = 64;

#[derive(Debug)]
pub(super) struct PartitionLog {
    dir: PathBuf,
    file: File,
    segment_id: u64,
    active_bytes: u64,
    segment_bytes: u64,
    next_offset: u64,
    record_checksums: HashMap<u64, u32>,
    sealed_subject_segments: Vec<SubjectSegment>,
    active_subjects: Vec<(String, u64)>,
    subject_index_cache_bytes: usize,
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
        for (index, (_, path)) in paths.iter().enumerate() {
            let active = index + 1 == paths.len();
            let first_record = envelopes.len();
            truncations += replay_segment(path, active, stream, partition, &mut envelopes)?;
            rebuild_index(path, &envelopes, stream, partition)?;
            let subjects = envelopes[first_record..]
                .iter()
                .map(|envelope| (envelope.subject.clone(), envelope.offset))
                .collect::<Vec<_>>();
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
        for (expected, envelope) in envelopes.iter().enumerate() {
            crate::broker_ensure!(
                envelope.offset == expected as u64,
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
        let next_offset = envelopes.len() as u64;
        let record_checksums = envelopes
            .iter()
            .map(|envelope| Ok((envelope.offset, envelope_checksum(envelope)?)))
            .collect::<Result<HashMap<_, _>>>()?;
        Ok((
            Self {
                dir,
                file,
                segment_id,
                active_bytes,
                segment_bytes,
                next_offset,
                record_checksums,
                sealed_subject_segments,
                active_subjects,
                subject_index_cache_bytes,
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
            crate::broker_ensure!(
                self.record_checksums.get(&envelope.offset).copied()
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
        self.record_checksums
            .insert(envelope.offset, envelope_checksum(&envelope)?);
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

    pub(super) fn rewrite(&mut self, records: &[MessageEnvelope]) -> Result<()> {
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
        self.next_offset = 0;
        self.record_checksums.clear();
        self.sealed_subject_segments.clear();
        self.active_subjects.clear();
        self.subject_index_cache_bytes = 0;
        for record in records {
            self.append_committed(record.clone())?;
        }
        self.flush()
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
}

fn replay_segment(
    path: &Path,
    active: bool,
    stream: &StreamId,
    partition: PartitionId,
    envelopes: &mut Vec<MessageEnvelope>,
) -> Result<u64> {
    let mut file = OpenOptions::new().read(true).write(active).open(path)?;
    validate_segment_header(&mut file, path)?;
    let file_len = file.metadata()?.len();
    let mut boundary = SEGMENT_HEADER_LEN;
    loop {
        let before = boundary;
        match read_batch(&mut file) {
            Ok(Some((envelope, bytes))) => {
                crate::broker_ensure!(
                    envelope.stream == *stream && envelope.partition == partition,
                    "partition-log envelope stored under the wrong stream or partition"
                );
                boundary += bytes;
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
    all_envelopes: &[MessageEnvelope],
    stream: &StreamId,
    partition: PartitionId,
) -> Result<()> {
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
            && all_envelopes
                .iter()
                .any(|candidate| candidate.offset == envelope.offset)
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
