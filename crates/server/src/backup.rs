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
}

#[derive(Debug, Default)]
pub struct MemoryObjectStore {
    objects: Mutex<HashMap<String, Vec<u8>>>,
}

impl ObjectStore for MemoryObjectStore {
    fn put_immutable(&self, key: &str, bytes: &[u8], sha256: &str) -> Result<()> {
        verify_checksum(bytes, sha256)?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::fmt::Write;
    use tempfile::tempdir;
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
}
