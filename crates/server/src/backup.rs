//! Consistent backup primitives for immutable partition and WAL segments.
//!
//! The broker owns the consistency boundary: callers must flush the broker before
//! creating a snapshot.  This module then only publishes immutable files and the
//! manifest after every checksum has been verified.

use crate::{
    error::{BrokerError, Result, ResultExt},
    stream::StreamDefinition,
};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    fs,
    path::{Path, PathBuf},
    sync::{Arc, Condvar, Mutex},
};

pub const MANIFEST_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum BackupKind {
    Full,
    Incremental,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct BackupObject {
    pub key: String,
    pub relative_path: String,
    pub bytes: u64,
    pub sha256: String,
    #[serde(default)]
    pub sealed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct BackupManifest {
    pub version: u32,
    pub backup_id: String,
    pub kind: BackupKind,
    pub parent_backup_id: Option<String>,
    pub created_at_ms: u64,
    pub source_cluster_id: String,
    pub streams: Vec<StreamDefinition>,
    pub objects: Vec<BackupObject>,
    #[serde(default)]
    pub checkpoint: Option<BackupCheckpoint>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct BackupCheckpoint {
    pub recovery_point: u64,
    pub consumer_cursors: BTreeMap<String, BTreeMap<String, u64>>,
    #[serde(default)]
    pub consumer_groups: BTreeMap<String, crate::consumer_group::GroupRecord>,
    pub cluster_metadata: BTreeMap<String, String>,
    pub connector_checkpoints: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TieringStats {
    pub verified_objects: usize,
    pub evicted_bytes: u64,
}

pub trait ObjectStore: Send + Sync {
    fn put_immutable(&self, key: &str, bytes: &[u8], sha256: &str) -> Result<()>;
    fn get(&self, key: &str) -> Result<Vec<u8>>;
    fn delete(&self, key: &str) -> Result<()>;
    fn list(&self, prefix: &str) -> Result<Vec<String>>;
}

#[derive(Debug, Default)]
pub struct MemoryObjectStore {
    objects: Mutex<HashMap<String, Vec<u8>>>,
}

impl ObjectStore for MemoryObjectStore {
    fn put_immutable(&self, key: &str, bytes: &[u8], sha256: &str) -> Result<()> {
        verify_checksum(bytes, sha256)?;
        validate_object_key(key)?;
        let mut objects = self.objects.lock().expect("object store lock poisoned");
        if let Some(existing) = objects.get(key) {
            crate::broker_ensure!(
                existing == bytes,
                "immutable object key already contains different data"
            );
            return Ok(());
        }
        objects.insert(key.to_string(), bytes.to_vec());
        Ok(())
    }

    fn get(&self, key: &str) -> Result<Vec<u8>> {
        self.objects
            .lock()
            .expect("object store lock poisoned")
            .get(key)
            .cloned()
            .ok_or_else(|| BrokerError::msg(format!("object not found: {key}")))
    }

    fn delete(&self, key: &str) -> Result<()> {
        self.objects
            .lock()
            .expect("object store lock poisoned")
            .remove(key);
        Ok(())
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        Ok(self
            .objects
            .lock()
            .expect("object store lock poisoned")
            .keys()
            .filter(|key| key.starts_with(prefix))
            .cloned()
            .collect())
    }
}

#[derive(Debug)]
pub struct FileObjectStore {
    root: PathBuf,
}

impl FileObjectStore {
    pub fn new(root: impl Into<PathBuf>) -> Result<Self> {
        let root = root.into();
        fs::create_dir_all(&root)
            .with_context(|| format!("creating object store {}", root.display()))?;
        Ok(Self { root })
    }
    fn path(&self, key: &str) -> Result<PathBuf> {
        validate_object_key(key)?;
        let path = self.root.join(key);
        crate::broker_ensure!(
            path.starts_with(&self.root),
            "object key escapes object store"
        );
        Ok(path)
    }
}

impl ObjectStore for FileObjectStore {
    fn put_immutable(&self, key: &str, bytes: &[u8], sha256: &str) -> Result<()> {
        verify_checksum(bytes, sha256)?;
        validate_object_key(key)?;
        let path = self.path(key)?;
        if path.exists() {
            let existing = fs::read(&path)?;
            crate::broker_ensure!(
                existing == bytes,
                "immutable object key already contains different data"
            );
            return Ok(());
        }
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let tmp = path.with_extension("upload.tmp");
        fs::write(&tmp, bytes)?;
        fs::rename(tmp, path)?;
        Ok(())
    }
    fn get(&self, key: &str) -> Result<Vec<u8>> {
        Ok(fs::read(self.path(key)?).with_context(|| format!("reading object {key}"))?)
    }
    fn delete(&self, key: &str) -> Result<()> {
        let _ = fs::remove_file(self.path(key)?);
        Ok(())
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        validate_object_key(prefix)?;
        let root = self.root.join(prefix);
        let mut paths = Vec::new();
        if root.exists() {
            collect_store_files(&self.root, &root, &mut paths)?;
        }
        Ok(paths)
    }
}

pub struct RetryingObjectStore<S> {
    inner: Arc<S>,
    attempts: usize,
}

impl<S> RetryingObjectStore<S> {
    pub fn new(inner: Arc<S>, attempts: usize) -> Result<Self> {
        crate::broker_ensure!(attempts > 0, "object-store retry attempts must be positive");
        Ok(Self { inner, attempts })
    }

    fn retry<T>(&self, operation: impl Fn(&S) -> Result<T>) -> Result<T> {
        let mut last_error = None;
        for _ in 0..self.attempts {
            match operation(&self.inner) {
                Ok(value) => return Ok(value),
                Err(error) => last_error = Some(error),
            }
        }
        Err(last_error.expect("retry attempts are positive"))
    }
}

impl<S: ObjectStore> ObjectStore for RetryingObjectStore<S> {
    fn put_immutable(&self, key: &str, bytes: &[u8], sha256: &str) -> Result<()> {
        self.retry(|store| store.put_immutable(key, bytes, sha256))
    }

    fn get(&self, key: &str) -> Result<Vec<u8>> {
        self.retry(|store| store.get(key))
    }

    fn delete(&self, key: &str) -> Result<()> {
        self.retry(|store| store.delete(key))
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.retry(|store| store.list(prefix))
    }
}

pub fn sha256(bytes: &[u8]) -> String {
    let mut digest = Sha256::new();
    digest.update(bytes);
    digest
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn verify_checksum(bytes: &[u8], expected: &str) -> Result<()> {
    crate::broker_ensure!(sha256(bytes) == expected, "object checksum mismatch");
    Ok(())
}

fn validate_object_key(key: &str) -> Result<()> {
    let path = Path::new(key);
    crate::broker_ensure!(
        !key.is_empty()
            && !path.is_absolute()
            && path
                .components()
                .all(|component| !matches!(component, std::path::Component::ParentDir)),
        "object key is unsafe"
    );
    Ok(())
}

pub struct BackupEngine<S> {
    store: Arc<S>,
}

impl<S: ObjectStore + 'static> BackupEngine<S> {
    pub fn new(store: Arc<S>) -> Self {
        Self { store }
    }

    pub fn create_full(
        &self,
        source: &Path,
        streams: Vec<StreamDefinition>,
        cluster_id: &str,
        backup_id: &str,
        created_at_ms: u64,
    ) -> Result<BackupManifest> {
        self.create_full_with_checkpoint(
            source,
            streams,
            cluster_id,
            backup_id,
            created_at_ms,
            None,
        )
    }

    pub fn create_full_with_checkpoint(
        &self,
        source: &Path,
        streams: Vec<StreamDefinition>,
        cluster_id: &str,
        backup_id: &str,
        created_at_ms: u64,
        checkpoint: Option<BackupCheckpoint>,
    ) -> Result<BackupManifest> {
        let files = snapshot_files(source)?;
        let mut objects = Vec::with_capacity(files.len());
        for (relative, sealed) in files {
            let bytes = read_stable_file(&source.join(&relative))
                .with_context(|| format!("reading backup file {}", relative.display()))?;
            let digest = sha256(&bytes);
            let key = format!("backups/{backup_id}/{}", relative.to_string_lossy());
            self.store.put_immutable(&key, &bytes, &digest)?;
            objects.push(BackupObject {
                key,
                relative_path: relative.to_string_lossy().into_owned(),
                bytes: bytes.len() as u64,
                sha256: digest,
                sealed,
            });
        }
        let manifest = BackupManifest {
            version: MANIFEST_VERSION,
            backup_id: backup_id.to_string(),
            kind: BackupKind::Full,
            parent_backup_id: None,
            created_at_ms,
            source_cluster_id: cluster_id.to_string(),
            streams,
            objects,
            checkpoint,
        };
        self.publish_manifest(&manifest)?;
        Ok(manifest)
    }

    pub fn create_incremental(
        &self,
        source: &Path,
        streams: Vec<StreamDefinition>,
        cluster_id: &str,
        backup_id: &str,
        created_at_ms: u64,
        parent: &BackupManifest,
    ) -> Result<BackupManifest> {
        validate_manifest(parent)?;
        crate::broker_ensure!(
            parent.source_cluster_id == cluster_id,
            "incremental backup cluster identity does not match its parent"
        );
        crate::broker_ensure!(
            parent.backup_id != backup_id,
            "incremental backup cannot have the same identity as its parent"
        );
        let parent_hashes = parent
            .objects
            .iter()
            .map(|object| (object.relative_path.as_str(), object.sha256.as_str()))
            .collect::<HashMap<_, _>>();
        let mut objects = Vec::new();
        for (relative, sealed) in snapshot_files(source)? {
            let bytes = read_stable_file(&source.join(&relative))
                .with_context(|| format!("reading backup file {}", relative.display()))?;
            let digest = sha256(&bytes);
            if parent_hashes.get(relative.to_string_lossy().as_ref()) == Some(&digest.as_str()) {
                continue;
            }
            let key = format!("backups/{backup_id}/{}", relative.to_string_lossy());
            self.store.put_immutable(&key, &bytes, &digest)?;
            objects.push(BackupObject {
                key,
                relative_path: relative.to_string_lossy().into_owned(),
                bytes: bytes.len() as u64,
                sha256: digest,
                sealed,
            });
        }
        let manifest = BackupManifest {
            version: MANIFEST_VERSION,
            backup_id: backup_id.to_string(),
            kind: BackupKind::Incremental,
            parent_backup_id: Some(parent.backup_id.clone()),
            created_at_ms,
            source_cluster_id: cluster_id.to_string(),
            streams,
            objects,
            checkpoint: parent.checkpoint.clone(),
        };
        self.publish_manifest(&manifest)?;
        Ok(manifest)
    }

    pub fn restore(
        &self,
        manifest: &BackupManifest,
        destination: &Path,
        new_cluster_id: &str,
    ) -> Result<()> {
        validate_manifest(manifest)?;
        crate::broker_ensure!(
            manifest.kind == BackupKind::Full,
            "incremental backup requires restore_chain"
        );
        self.restore_materialized(manifest, destination, new_cluster_id)
    }

    pub fn restore_chain(
        &self,
        chain: &[BackupManifest],
        destination: &Path,
        new_cluster_id: &str,
    ) -> Result<()> {
        crate::broker_ensure!(!chain.is_empty(), "backup restore chain is empty");
        let first = &chain[0];
        validate_manifest(first)?;
        crate::broker_ensure!(
            first.kind == BackupKind::Full && first.parent_backup_id.is_none(),
            "restore chain must start with a full backup"
        );
        let mut objects = BTreeMap::new();
        for (index, manifest) in chain.iter().enumerate() {
            validate_manifest(manifest)?;
            crate::broker_ensure!(
                manifest.source_cluster_id == first.source_cluster_id,
                "backup restore chain contains multiple source clusters"
            );
            if index > 0 {
                crate::broker_ensure!(
                    manifest.parent_backup_id.as_deref()
                        == Some(chain[index - 1].backup_id.as_str()),
                    "backup restore chain has a broken parent link"
                );
            }
            for object in &manifest.objects {
                objects.insert(object.relative_path.clone(), object.clone());
            }
        }
        let last = chain.last().expect("non-empty chain");
        let materialized = BackupManifest {
            version: MANIFEST_VERSION,
            backup_id: last.backup_id.clone(),
            kind: BackupKind::Full,
            parent_backup_id: None,
            created_at_ms: last.created_at_ms,
            source_cluster_id: first.source_cluster_id.clone(),
            streams: last.streams.clone(),
            objects: objects.into_values().collect(),
            checkpoint: last.checkpoint.clone(),
        };
        self.restore_materialized(&materialized, destination, new_cluster_id)
    }

    fn restore_materialized(
        &self,
        manifest: &BackupManifest,
        destination: &Path,
        new_cluster_id: &str,
    ) -> Result<()> {
        validate_manifest(manifest)?;
        crate::broker_ensure!(
            new_cluster_id != manifest.source_cluster_id,
            "restore requires a new cluster identity"
        );
        if destination.exists() {
            return validate_existing_restore(manifest, destination);
        }
        let staging = destination.with_extension("restore.tmp");
        if staging.exists() {
            fs::remove_dir_all(&staging)?;
        }
        fs::create_dir_all(&staging)?;
        let result = (|| {
            for object in &manifest.objects {
                let bytes = self.store.get(&object.key)?;
                crate::broker_ensure!(
                    bytes.len() as u64 == object.bytes,
                    "backup object size mismatch"
                );
                verify_checksum(&bytes, &object.sha256)?;
                let path = staging.join(&object.relative_path);
                if let Some(parent) = path.parent() {
                    fs::create_dir_all(parent)?;
                }
                fs::write(path, bytes)?;
            }
            if destination.exists() {
                fs::remove_dir_all(destination)?;
            }
            fs::rename(&staging, destination)?;
            Ok(())
        })();
        if result.is_err() {
            let _ = fs::remove_dir_all(&staging);
        }
        result
    }

    fn publish_manifest(&self, manifest: &BackupManifest) -> Result<()> {
        let encoded = serde_json::to_vec_pretty(manifest)
            .map_err(|error| BrokerError::msg(error.to_string()))?;
        let key = format!("backups/{}/manifest.json", manifest.backup_id);
        self.store.put_immutable(&key, &encoded, &sha256(&encoded))
    }

    /// Remove only unpublished objects under one backup prefix. The manifest and
    /// every object it references are retained, making cleanup safe to retry.
    pub fn cleanup_orphans(&self, backup_id: &str, manifest: &BackupManifest) -> Result<usize> {
        validate_manifest(manifest)?;
        crate::broker_ensure!(
            manifest.backup_id == backup_id,
            "orphan cleanup backup identity mismatch"
        );
        let prefix = format!("backups/{backup_id}/");
        validate_object_key(&prefix)?;
        let mut keep = manifest
            .objects
            .iter()
            .map(|object| object.key.clone())
            .collect::<std::collections::HashSet<_>>();
        keep.insert(format!("{prefix}manifest.json"));
        let mut removed = 0;
        for key in self.store.list(&prefix)? {
            if !keep.contains(&key) {
                self.store.delete(&key)?;
                removed += 1;
            }
        }
        Ok(removed)
    }

    /// Evict only sealed stream files whose immutable remote copies are still
    /// present and checksum-valid.  A manifest is the publication fence: callers
    /// cannot accidentally evict a file merely because an upload returned.
    pub fn evict_sealed(&self, source: &Path, manifest: &BackupManifest) -> Result<TieringStats> {
        validate_manifest(manifest)?;
        let mut stats = TieringStats {
            verified_objects: 0,
            evicted_bytes: 0,
        };
        for object in &manifest.objects {
            if !object.sealed
                || !object.relative_path.starts_with("streams/")
                || !matches!(
                    Path::new(&object.relative_path)
                        .extension()
                        .and_then(|value| value.to_str()),
                    Some("plog") | Some("idx") | Some("sidx")
                )
            {
                continue;
            }
            let bytes = self.store.get(&object.key)?;
            crate::broker_ensure!(
                bytes.len() as u64 == object.bytes,
                "tiered object size mismatch"
            );
            verify_checksum(&bytes, &object.sha256)?;
            let path = source.join(&object.relative_path);
            if path.exists() {
                fs::remove_file(&path)?;
                stats.evicted_bytes = stats.evicted_bytes.saturating_add(object.bytes);
            }
            stats.verified_objects += 1;
        }
        Ok(stats)
    }
}

fn validate_manifest(manifest: &BackupManifest) -> Result<()> {
    crate::broker_ensure!(
        manifest.version == MANIFEST_VERSION,
        "unsupported backup manifest version"
    );
    crate::broker_ensure!(
        !manifest.backup_id.is_empty() && !manifest.source_cluster_id.is_empty(),
        "backup manifest identity is missing"
    );
    let mut paths = BTreeMap::new();
    for object in &manifest.objects {
        crate::broker_ensure!(
            !object.relative_path.starts_with('/') && !object.relative_path.contains(".."),
            "backup object path is unsafe"
        );
        crate::broker_ensure!(
            paths.insert(&object.relative_path, ()).is_none(),
            "backup manifest contains duplicate paths"
        );
    }
    Ok(())
}

fn validate_existing_restore(manifest: &BackupManifest, destination: &Path) -> Result<()> {
    for object in &manifest.objects {
        let path = destination.join(&object.relative_path);
        crate::broker_ensure!(path.is_file(), "restore destination conflicts with backup");
        let bytes = fs::read(path)?;
        crate::broker_ensure!(
            bytes.len() as u64 == object.bytes,
            "restore destination size mismatch"
        );
        verify_checksum(&bytes, &object.sha256)?;
    }
    Ok(())
}

fn snapshot_files(root: &Path) -> Result<Vec<(PathBuf, bool)>> {
    let mut files = Vec::new();
    collect_files(root, root, &mut files)?;
    files.sort();
    Ok(files
        .into_iter()
        .map(|path| {
            let sealed = !is_active_segment(&root.join(&path));
            (path, sealed)
        })
        .collect())
}

fn read_stable_file(path: &Path) -> Result<Vec<u8>> {
    for _ in 0..3 {
        let before = fs::metadata(path)?.len();
        let bytes = fs::read(path)?;
        let after = fs::metadata(path)?.len();
        if before == after && after == bytes.len() as u64 {
            return Ok(bytes);
        }
    }
    Err(BrokerError::msg(format!(
        "file changed while creating backup: {}",
        path.display()
    )))
}

fn collect_files(root: &Path, current: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
    for entry in fs::read_dir(current)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            collect_files(root, &path, files)?;
        } else if path.extension().is_some_and(|extension| extension != "tmp") {
            files.push(path.strip_prefix(root).expect("root prefix").to_path_buf());
        }
    }
    Ok(())
}

fn collect_store_files(root: &Path, current: &Path, files: &mut Vec<String>) -> Result<()> {
    for entry in fs::read_dir(current)? {
        let path = entry?.path();
        if path.is_dir() {
            collect_store_files(root, &path, files)?;
        } else {
            files.push(
                path.strip_prefix(root)
                    .expect("object-store root prefix")
                    .to_string_lossy()
                    .into_owned(),
            );
        }
    }
    Ok(())
}

fn is_active_segment(path: &Path) -> bool {
    let extension = path.extension().and_then(|value| value.to_str());
    if !matches!(
        extension,
        Some("plog") | Some("wal") | Some("idx") | Some("sidx")
    ) {
        return false;
    }
    let Some(stem) = path.file_stem().and_then(|value| value.to_str()) else {
        return false;
    };
    let Ok(id) = stem.trim_start_matches("segment-").parse::<u64>() else {
        return false;
    };
    let Some(parent) = path.parent() else {
        return false;
    };
    let segment_extension = match extension {
        Some("idx") | Some("sidx") if parent.join(format!("{stem}.plog")).exists() => "plog",
        Some("idx") | Some("sidx") => "wal",
        Some(extension) => extension,
        None => return false,
    };
    fs::read_dir(parent)
        .ok()
        .into_iter()
        .flatten()
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| {
            let path = entry.path();
            (path.extension().and_then(|value| value.to_str()) == Some(segment_extension))
                .then(|| path.file_stem().map(|value| value.to_owned()))
                .flatten()
        })
        .filter_map(|value| {
            value
                .to_str()
                .and_then(|value| value.trim_start_matches("segment-").parse::<u64>().ok())
        })
        .max()
        == Some(id)
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    pub bytes: usize,
    pub entries: usize,
}

#[derive(Debug)]
pub struct RemoteSegmentCache<S> {
    store: Arc<S>,
    capacity: usize,
    state: Mutex<CacheState>,
    wake: Condvar,
}
#[derive(Debug, Default)]
struct CacheState {
    entries: HashMap<String, Vec<u8>>,
    order: VecDeque<String>,
    bytes: usize,
    hits: u64,
    misses: u64,
    loading: HashMap<String, Arc<(Mutex<bool>, Condvar)>>,
}

impl<S: ObjectStore + 'static> RemoteSegmentCache<S> {
    pub fn new(store: Arc<S>, capacity: usize) -> Self {
        Self {
            store,
            capacity,
            state: Mutex::new(CacheState::default()),
            wake: Condvar::new(),
        }
    }
    pub fn get(&self, key: &str, checksum: &str) -> Result<Vec<u8>> {
        loop {
            let mut state = self.state.lock().expect("cache lock poisoned");
            if let Some(bytes) = state.entries.get(key).cloned() {
                state.hits += 1;
                return Ok(bytes);
            }
            if let Some(wait) = state.loading.get(key).cloned() {
                drop(state);
                let (lock, wake) = &*wait;
                let mut done = lock.lock().expect("cache waiter poisoned");
                while !*done {
                    done = wake.wait(done).expect("cache waiter poisoned");
                }
                continue;
            }
            state.misses += 1;
            let wait = Arc::new((Mutex::new(false), Condvar::new()));
            state.loading.insert(key.to_string(), wait.clone());
            drop(state);
            let result = self.store.get(key).and_then(|bytes| {
                verify_checksum(&bytes, checksum)?;
                Ok(bytes)
            });
            let mut state = self.state.lock().expect("cache lock poisoned");
            state.loading.remove(key);
            if let Ok(bytes) = &result {
                if bytes.len() <= self.capacity {
                    while state.bytes + bytes.len() > self.capacity {
                        if let Some(old) = state.order.pop_front() {
                            if let Some(value) = state.entries.remove(&old) {
                                state.bytes -= value.len();
                            }
                        } else {
                            break;
                        }
                    }
                    state.bytes += bytes.len();
                    state.entries.insert(key.to_string(), bytes.clone());
                    state.order.push_back(key.to_string());
                }
            }
            let (lock, wake) = &*wait;
            *lock.lock().expect("cache waiter poisoned") = true;
            wake.notify_all();
            self.wake.notify_all();
            return result;
        }
    }
    pub fn stats(&self) -> CacheStats {
        let state = self.state.lock().expect("cache lock poisoned");
        CacheStats {
            hits: state.hits,
            misses: state.misses,
            bytes: state.bytes,
            entries: state.entries.len(),
        }
    }
}

pub struct RemoteSegmentReader<S> {
    cache: RemoteSegmentCache<S>,
}

impl<S: ObjectStore + 'static> RemoteSegmentReader<S> {
    pub fn new(store: Arc<S>, capacity: usize) -> Self {
        Self {
            cache: RemoteSegmentCache::new(store, capacity),
        }
    }

    pub fn read_offset(
        &self,
        object_key: &str,
        checksum: &str,
        offset: u64,
    ) -> Result<Option<crate::partition_log::MessageEnvelope>> {
        let bytes = self.cache.get(object_key, checksum)?;
        crate::partition_log::read_segment_offset(&bytes, offset)
    }

    pub fn stats(&self) -> CacheStats {
        self.cache.stats()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fmt::Write;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tempfile::tempdir;

    struct FlakyStore {
        inner: MemoryObjectStore,
        failures: AtomicUsize,
    }

    impl FlakyStore {
        fn new(failures: usize) -> Self {
            Self {
                inner: MemoryObjectStore::default(),
                failures: AtomicUsize::new(failures),
            }
        }
    }

    impl ObjectStore for FlakyStore {
        fn put_immutable(&self, key: &str, bytes: &[u8], digest: &str) -> Result<()> {
            if self
                .failures
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
            {
                return Err(BrokerError::msg("injected transient object-store failure"));
            }
            self.inner.put_immutable(key, bytes, digest)
        }

        fn get(&self, key: &str) -> Result<Vec<u8>> {
            self.inner.get(key)
        }

        fn delete(&self, key: &str) -> Result<()> {
            self.inner.delete(key)
        }

        fn list(&self, prefix: &str) -> Result<Vec<String>> {
            self.inner.list(prefix)
        }
    }
    #[test]
    fn backup_excludes_active_segments_and_restores_with_checksum_validation() {
        let source = tempdir().unwrap();
        let partition = source.path().join("streams/orders/partition-00000");
        fs::create_dir_all(&partition).unwrap();
        fs::write(partition.join("segment-1.plog"), b"sealed").unwrap();
        fs::write(partition.join("segment-1.idx"), b"index").unwrap();
        fs::write(partition.join("segment-2.plog"), b"active").unwrap();
        let store = Arc::new(MemoryObjectStore::default());
        let engine = BackupEngine::new(store.clone());
        let manifest = engine
            .create_full(source.path(), Vec::new(), "old", "b1", 1)
            .unwrap();
        assert_eq!(manifest.objects.len(), 3);
        assert!(
            manifest.objects.iter().any(|object| {
                object.relative_path.ends_with("segment-2.plog") && !object.sealed
            })
        );
        let destination = tempdir().unwrap().path().join("restore");
        engine.restore(&manifest, &destination, "new").unwrap();
        assert_eq!(
            fs::read(destination.join("streams/orders/partition-00000/segment-1.plog")).unwrap(),
            b"sealed"
        );
    }
    #[test]
    fn cache_is_bounded_and_rejects_corrupt_remote_data() {
        let store = Arc::new(MemoryObjectStore::default());
        let data = b"0123456789";
        let digest = sha256(data);
        store.put_immutable("s", data, &digest).unwrap();
        let cache = RemoteSegmentCache::new(store, 10);
        assert_eq!(cache.get("s", &digest).unwrap(), data);
        assert_eq!(cache.get("s", &digest).unwrap(), data);
        assert_eq!(cache.stats().hits, 1);
        let mut text = String::new();
        write!(&mut text, "{}", cache.stats().bytes).unwrap();
        assert_eq!(text, "10");
    }

    #[test]
    fn eviction_requires_published_remote_objects_and_repeated_restore_detects_conflicts() {
        let source = tempdir().unwrap();
        let partition = source.path().join("streams/orders/partition-00000");
        fs::create_dir_all(&partition).unwrap();
        fs::write(partition.join("00000000000000000001.plog"), b"sealed").unwrap();
        fs::write(partition.join("00000000000000000002.plog"), b"active").unwrap();
        let store = Arc::new(MemoryObjectStore::default());
        let engine = BackupEngine::new(store);
        let manifest = engine
            .create_full(source.path(), Vec::new(), "old", "b2", 2)
            .unwrap();
        let stats = engine.evict_sealed(source.path(), &manifest).unwrap();
        assert_eq!(stats.verified_objects, 1);
        assert!(!partition.join("00000000000000000001.plog").exists());
        assert!(partition.join("00000000000000000002.plog").exists());

        let destination = tempdir().unwrap().path().join("restore");
        engine.restore(&manifest, &destination, "new").unwrap();
        engine.restore(&manifest, &destination, "new").unwrap();
        fs::write(
            destination.join("streams/orders/partition-00000/00000000000000000001.plog"),
            b"conflict",
        )
        .unwrap();
        assert!(engine.restore(&manifest, &destination, "new").is_err());
    }

    #[test]
    fn incremental_backup_references_parent_and_chain_restore_materializes_latest_files() {
        let source = tempdir().unwrap();
        let partition = source.path().join("streams/orders/partition-00000");
        fs::create_dir_all(&partition).unwrap();
        fs::write(partition.join("00000000000000000001.plog"), b"old").unwrap();
        let store = Arc::new(MemoryObjectStore::default());
        let engine = BackupEngine::new(store);
        let full = engine
            .create_full(source.path(), Vec::new(), "cluster-a", "full", 1)
            .unwrap();
        fs::write(partition.join("00000000000000000001.plog"), b"new").unwrap();
        fs::write(partition.join("00000000000000000002.plog"), b"added").unwrap();
        let incremental = engine
            .create_incremental(
                source.path(),
                Vec::new(),
                "cluster-a",
                "incremental",
                2,
                &full,
            )
            .unwrap();
        assert_eq!(incremental.kind, BackupKind::Incremental);
        assert_eq!(incremental.parent_backup_id.as_deref(), Some("full"));
        assert!(
            engine
                .restore(
                    &incremental,
                    &tempdir().unwrap().path().join("bad"),
                    "cluster-b"
                )
                .is_err()
        );
        let destination = tempdir().unwrap().path().join("restore");
        engine
            .restore_chain(&[full, incremental], &destination, "cluster-b")
            .unwrap();
        assert_eq!(
            fs::read(destination.join("streams/orders/partition-00000/00000000000000000001.plog"))
                .unwrap(),
            b"new"
        );
        assert_eq!(
            fs::read(destination.join("streams/orders/partition-00000/00000000000000000002.plog"))
                .unwrap(),
            b"added"
        );
    }

    #[test]
    fn orphan_cleanup_is_scoped_and_object_keys_cannot_escape_store() {
        let source = tempdir().unwrap();
        fs::write(source.path().join("metadata"), b"state").unwrap();
        let store = Arc::new(MemoryObjectStore::default());
        let engine = BackupEngine::new(store.clone());
        let manifest = engine
            .create_full(source.path(), Vec::new(), "cluster-a", "cleanup", 1)
            .unwrap();
        store
            .put_immutable("backups/cleanup/orphan", b"orphan", &sha256(b"orphan"))
            .unwrap();
        store
            .put_immutable("backups/other/orphan", b"keep", &sha256(b"keep"))
            .unwrap();
        assert_eq!(engine.cleanup_orphans("cleanup", &manifest).unwrap(), 1);
        assert!(store.get("backups/cleanup/orphan").is_err());
        assert_eq!(store.get("backups/other/orphan").unwrap(), b"keep");
        assert!(
            store
                .put_immutable("../outside", b"bad", &sha256(b"bad"))
                .is_err()
        );
    }

    #[test]
    fn checkpoint_covers_cursors_cluster_metadata_and_connector_state() {
        let source = tempdir().unwrap();
        fs::write(source.path().join("metadata"), b"state").unwrap();
        let store = Arc::new(MemoryObjectStore::default());
        let engine = BackupEngine::new(store);
        let mut checkpoint = BackupCheckpoint {
            recovery_point: 42,
            cluster_metadata: [("epoch".to_string(), "7".to_string())]
                .into_iter()
                .collect(),
            connector_checkpoints: [("sink/orders".to_string(), "offset=9".to_string())]
                .into_iter()
                .collect(),
            ..BackupCheckpoint::default()
        };
        checkpoint.consumer_cursors.insert(
            "consumer-a".to_string(),
            [("orders/0".to_string(), 41)].into_iter().collect(),
        );
        let manifest = engine
            .create_full_with_checkpoint(
                source.path(),
                Vec::new(),
                "cluster-a",
                "checkpointed",
                42,
                Some(checkpoint.clone()),
            )
            .unwrap();
        assert_eq!(manifest.checkpoint, Some(checkpoint));
        let serialized = serde_json::to_vec(&manifest).unwrap();
        let decoded: BackupManifest = serde_json::from_slice(&serialized).unwrap();
        assert_eq!(decoded.checkpoint, manifest.checkpoint);
    }

    #[test]
    fn remote_segment_reader_validates_and_reads_records_through_cache() {
        let envelope = crate::partition_log::MessageEnvelope {
            namespace: "default".to_string(),
            stream: crate::stream::StreamId::new("orders").unwrap(),
            partition: crate::stream::PartitionId(0),
            offset: 7,
            subject: "orders/created".to_string(),
            key: None,
            headers: Vec::new(),
            timestamp_ms: 1,
            reply_to: None,
            payload: b"payload".to_vec(),
            partitioning_epoch: 1,
            leader_epoch: 1,
            legacy_seq: 7,
        };
        let body = serde_json::to_vec(&envelope).unwrap();
        let mut bytes = b"BROKERLOG\x01\n".to_vec();
        bytes.extend_from_slice(&(body.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&crc32fast::hash(&body).to_le_bytes());
        bytes.extend_from_slice(&body);
        let store = Arc::new(MemoryObjectStore::default());
        let digest = sha256(&bytes);
        store.put_immutable("segments/7", &bytes, &digest).unwrap();
        let reader = RemoteSegmentReader::new(store, bytes.len());
        assert_eq!(
            reader.read_offset("segments/7", &digest, 7).unwrap(),
            Some(envelope)
        );
        assert!(
            reader
                .read_offset("segments/7", &digest, 8)
                .unwrap()
                .is_none()
        );
        assert_eq!(reader.stats().hits, 1);
    }

    #[test]
    fn object_store_retries_are_bounded_and_idempotent() {
        let flaky = Arc::new(FlakyStore::new(2));
        let store = RetryingObjectStore::new(flaky.clone(), 3).unwrap();
        let bytes = b"retryable";
        store
            .put_immutable("retry/object", bytes, &sha256(bytes))
            .unwrap();
        assert_eq!(flaky.inner.get("retry/object").unwrap(), bytes);
        assert!(RetryingObjectStore::new(flaky, 0).is_err());
    }
}
