use super::*;
use crate::encryption::KeyRing;
use crate::error::ResultExt;
use protocol::subject;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::OpenOptions;
use std::path::PathBuf;
use std::sync::Arc;

const SUBJECT_INDEX_EXTENSION: &str = "sidx";
const SUBJECT_INDEX_VERSION: u32 = 1;
const MAX_INDEX_SUBJECTS: usize = 4_096;
const MAX_INDEX_POSTINGS: usize = 65_536;
const MAX_INDEX_BYTES: usize = 4 * 1024 * 1024;
pub(super) const MAX_PARTITION_INDEX_CACHE_BYTES: usize = 4 * 1024 * 1024;

#[derive(Debug)]
pub(super) struct SubjectSegment {
    pub(super) path: PathBuf,
    records: Vec<(String, u64)>,
    record_count: usize,
    source_checksum: u64,
    index: Option<SubjectIndexFile>,
    encryption: Option<Arc<KeyRing>>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct SubjectIndexFile {
    version: u32,
    source_checksum: u64,
    dictionary: Vec<String>,
    postings: Vec<Vec<u64>>,
    index_checksum: u64,
}

impl SubjectSegment {
    pub(super) fn new(
        path: PathBuf,
        records: Vec<(String, u64)>,
        encryption: Option<Arc<KeyRing>>,
    ) -> Self {
        let source_checksum = records_checksum(&records);
        let record_count = records.len();
        Self {
            path,
            records,
            record_count,
            source_checksum,
            index: None,
            encryption,
        }
    }

    pub(super) fn rebuild(&mut self, cache_budget: usize) -> Result<usize> {
        let index_path = self.path.with_extension(SUBJECT_INDEX_EXTENSION);
        let Some(mut index) = build_index(&self.records, self.source_checksum) else {
            if index_path.exists() {
                std::fs::remove_file(index_path)?;
            }
            self.index = None;
            self.release_records();
            return Ok(0);
        };
        index.index_checksum = index_checksum(&index);
        let body = serde_json::to_vec(&index).context("encoding segment subject index")?;
        if body.len() > MAX_INDEX_BYTES {
            if index_path.exists() {
                std::fs::remove_file(index_path)?;
            }
            self.index = None;
            self.release_records();
            return Ok(0);
        }
        let tmp_path = self.path.with_extension("sidx.tmp");
        std::fs::write(&tmp_path, &body)?;
        std::fs::rename(tmp_path, index_path)?;
        let cached_bytes = if body.len() <= cache_budget {
            self.index = Some(index);
            body.len()
        } else {
            self.index = None;
            0
        };
        self.release_records();
        Ok(cached_bytes)
    }

    pub(super) fn matching_offsets(&self, filter: &str) -> Result<SubjectIndexQuery> {
        if let Some(query) = self
            .index
            .as_ref()
            .and_then(|index| indexed_offsets(index, filter, self.record_count / 4))
        {
            return Ok(query);
        }
        scan_segment(&self.path, filter, self.encryption.as_ref())
    }

    fn release_records(&mut self) {
        self.records.clear();
        self.records.shrink_to_fit();
    }
}

pub(super) fn active_matching_offsets(
    records: &[(String, u64)],
    filter: &str,
) -> SubjectIndexQuery {
    SubjectIndexQuery {
        offsets: scan_offsets(records, filter),
        used_index: false,
    }
}

fn build_index(records: &[(String, u64)], source_checksum: u64) -> Option<SubjectIndexFile> {
    let mut postings: BTreeMap<String, Vec<u64>> = BTreeMap::new();
    for (concrete_subject, offset) in records {
        if !postings.contains_key(concrete_subject) && postings.len() >= MAX_INDEX_SUBJECTS {
            return None;
        }
        postings
            .entry(concrete_subject.clone())
            .or_default()
            .push(*offset);
        if records.len() > MAX_INDEX_POSTINGS {
            return None;
        }
    }
    let (dictionary, postings): (Vec<_>, Vec<_>) = postings.into_iter().unzip();
    Some(SubjectIndexFile {
        version: SUBJECT_INDEX_VERSION,
        source_checksum,
        dictionary,
        postings,
        index_checksum: 0,
    })
}

fn scan_offsets(records: &[(String, u64)], filter: &str) -> Vec<u64> {
    records
        .iter()
        .filter(|(concrete_subject, _)| subject::matches(filter, concrete_subject))
        .map(|(_, offset)| *offset)
        .collect()
}

fn scan_segment(
    path: &Path,
    filter: &str,
    encryption: Option<&Arc<KeyRing>>,
) -> Result<SubjectIndexQuery> {
    let mut file = OpenOptions::new().read(true).open(path)?;
    super::codec::validate_segment_header(&mut file, path)?;
    let mut offsets = Vec::new();
    while let Some((envelope, _)) = super::codec::read_batch_with_key(&mut file, encryption)? {
        if subject::matches(filter, &envelope.subject) {
            offsets.push(envelope.offset);
        }
    }
    Ok(SubjectIndexQuery {
        offsets,
        used_index: false,
    })
}

fn indexed_offsets(
    index: &SubjectIndexFile,
    filter: &str,
    wildcard_posting_budget: usize,
) -> Option<SubjectIndexQuery> {
    if !filter.contains('*') && !filter.contains('>') {
        let offsets = index
            .dictionary
            .binary_search_by(|subject| subject.as_str().cmp(filter))
            .ok()
            .map(|position| index.postings[position].clone())
            .unwrap_or_default();
        return Some(SubjectIndexQuery {
            offsets,
            used_index: true,
        });
    }
    if filter == ">" || filter.ends_with("/**") {
        return None;
    }
    let mut matching_postings = Vec::new();
    let mut posting_count = 0usize;
    for (concrete_subject, postings) in index.dictionary.iter().zip(&index.postings) {
        if subject::matches(filter, concrete_subject) {
            posting_count = posting_count.saturating_add(postings.len());
            if posting_count > wildcard_posting_budget {
                return None;
            }
            matching_postings.push(postings);
        }
    }
    let mut offsets = BTreeSet::new();
    for postings in matching_postings {
        offsets.extend(postings.iter().copied());
    }
    Some(SubjectIndexQuery {
        offsets: offsets.into_iter().collect(),
        used_index: true,
    })
}

fn records_checksum(records: &[(String, u64)]) -> u64 {
    let mut hash = 0xcbf29ce484222325;
    for (subject, offset) in records {
        hash = hash_bytes(hash, subject.as_bytes());
        hash = hash_bytes(hash, &offset.to_le_bytes());
    }
    hash
}

fn index_checksum(index: &SubjectIndexFile) -> u64 {
    let mut hash = hash_bytes(0xcbf29ce484222325, &index.version.to_le_bytes());
    hash = hash_bytes(hash, &index.source_checksum.to_le_bytes());
    for (subject, postings) in index.dictionary.iter().zip(&index.postings) {
        hash = hash_bytes(hash, subject.as_bytes());
        for offset in postings {
            hash = hash_bytes(hash, &offset.to_le_bytes());
        }
    }
    hash
}

fn hash_bytes(mut hash: u64, bytes: &[u8]) -> u64 {
    for byte in bytes {
        hash = (hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3);
    }
    hash
}

#[cfg(test)]
#[path = "subject_index/tests.rs"]
mod tests;
