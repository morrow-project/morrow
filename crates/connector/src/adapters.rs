use crate::{Connector, ConnectorBatch, SinkCompletion, SinkTask};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::OpenOptions,
    io::{Read, Seek, SeekFrom, Write},
    path::PathBuf,
};

pub struct ObjectStoreSink {
    name: String,
    generation: u64,
    root: PathBuf,
}

impl ObjectStoreSink {
    pub fn new(name: impl Into<String>, generation: u64, root: impl Into<PathBuf>) -> Self {
        Self {
            name: name.into(),
            generation,
            root: root.into(),
        }
    }
}

impl Connector for ObjectStoreSink {
    fn name(&self) -> &str {
        &self.name
    }

    fn generation(&self) -> u64 {
        self.generation
    }
}

impl SinkTask for ObjectStoreSink {
    fn write_batch(&mut self, batch: &ConnectorBatch) -> Result<SinkCompletion, String> {
        fence(self.generation, batch.generation)?;
        let mut offsets = BTreeMap::new();
        let mut objects = BTreeMap::<PathBuf, Vec<u8>>::new();
        let mut directories = BTreeSet::new();
        for record in &batch.records {
            let stream = safe_component(&record.stream)?;
            let dir = self
                .root
                .join(stream)
                .join(format!("partition-{:05}", record.partition));
            let path = dir.join(format!("{:020}.record", record.offset));
            let body = serde_json::to_vec(record).map_err(display)?;
            if let Some(previous) = objects.insert(path, body.clone())
                && previous != body
            {
                return Err("object key already contains different data".to_string());
            }
            directories.insert(dir);
            offsets.insert((record.stream.clone(), record.partition), record.offset);
        }
        for (path, body) in objects {
            if path.exists() {
                if std::fs::read(&path).map_err(display)? != body {
                    return Err("object key already contains different data".to_string());
                }
                continue;
            }
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).map_err(display)?;
            }
            let temporary = path.with_extension("tmp");
            std::fs::write(&temporary, &body).map_err(display)?;
            OpenOptions::new()
                .read(true)
                .open(&temporary)
                .map_err(display)?
                .sync_all()
                .map_err(display)?;
            std::fs::rename(temporary, path).map_err(display)?;
        }
        for directory in directories {
            crate::storage::sync_dir(&directory).map_err(display)?;
        }
        Ok(SinkCompletion { offsets })
    }

    fn completion_boundary(&self) -> &'static str {
        "record object atomically renamed after file fsync"
    }
}

pub struct AppendDatabaseSink {
    name: String,
    generation: u64,
    path: PathBuf,
    index_path: PathBuf,
    committed: BTreeSet<(String, u32, u64)>,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct AppendIndex {
    log_len: u64,
    committed: BTreeSet<(String, u32, u64)>,
}

impl AppendDatabaseSink {
    pub fn open(
        name: impl Into<String>,
        generation: u64,
        path: impl Into<PathBuf>,
    ) -> Result<Self, String> {
        let path = path.into();
        let index_path = path.with_extension("idx");
        let (committed, log_len) = recover_append_state(&path, &index_path)?;
        if path.exists() {
            persist_append_index(&index_path, log_len, &committed)?;
        }
        Ok(Self {
            name: name.into(),
            generation,
            path,
            index_path,
            committed,
        })
    }
}

impl Connector for AppendDatabaseSink {
    fn name(&self) -> &str {
        &self.name
    }

    fn generation(&self) -> u64 {
        self.generation
    }
}

impl SinkTask for AppendDatabaseSink {
    fn write_batch(&mut self, batch: &ConnectorBatch) -> Result<SinkCompletion, String> {
        fence(self.generation, batch.generation)?;
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent).map_err(display)?;
        }
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(display)?;
        let mut offsets = BTreeMap::new();
        let mut pending = BTreeSet::new();
        for record in &batch.records {
            let identity = (record.stream.clone(), record.partition, record.offset);
            if !self.committed.contains(&identity) && pending.insert(identity) {
                file.write_all(&serde_json::to_vec(record).map_err(display)?)
                    .map_err(display)?;
                file.write_all(b"\n").map_err(display)?;
            }
            offsets.insert((record.stream.clone(), record.partition), record.offset);
        }
        file.flush().map_err(display)?;
        file.sync_data().map_err(display)?;
        self.committed.extend(pending);
        let log_len = file.metadata().map_err(display)?.len();
        persist_append_index(&self.index_path, log_len, &self.committed)?;
        Ok(SinkCompletion { offsets })
    }

    fn completion_boundary(&self) -> &'static str {
        "idempotency key recorded and append log fsynced"
    }
}

fn recover_append_state(
    path: &PathBuf,
    index_path: &PathBuf,
) -> Result<(BTreeSet<(String, u32, u64)>, u64), String> {
    if !path.exists() {
        return Ok((BTreeSet::new(), 0));
    }
    let log_len = std::fs::metadata(path).map_err(display)?.len();
    if let Some(index) = std::fs::read(index_path)
        .ok()
        .and_then(|bytes| serde_json::from_slice::<AppendIndex>(&bytes).ok())
        && index.log_len <= log_len
    {
        match read_append_records(path, index.log_len, index.committed) {
            Ok((committed, valid_len)) => {
                if valid_len < log_len {
                    truncate_append_log(path, valid_len)?;
                }
                return Ok((committed, valid_len));
            }
            Err(_) => {}
        }
    }
    let (committed, valid_len) = read_append_records(path, 0, BTreeSet::new())?;
    if valid_len < log_len {
        truncate_append_log(path, valid_len)?;
    }
    Ok((committed, valid_len))
}

fn truncate_append_log(path: &PathBuf, len: u64) -> Result<(), String> {
    let file = OpenOptions::new().write(true).open(path).map_err(display)?;
    file.set_len(len).map_err(display)?;
    file.sync_data().map_err(display)
}

fn read_append_records(
    path: &PathBuf,
    start: u64,
    mut committed: BTreeSet<(String, u32, u64)>,
) -> Result<(BTreeSet<(String, u32, u64)>, u64), String> {
    let mut file = OpenOptions::new().read(true).open(path).map_err(display)?;
    file.seek(SeekFrom::Start(start)).map_err(display)?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).map_err(display)?;
    let mut valid_len = start;
    let mut cursor = 0;
    for line in bytes.split_inclusive(|byte| *byte == b'\n') {
        let has_newline = line.ends_with(b"\n");
        let body = line.strip_suffix(b"\n").unwrap_or(line);
        if body.is_empty() {
            valid_len = valid_len.saturating_add(line.len() as u64);
            cursor += line.len();
            continue;
        }
        let record = match serde_json::from_slice::<crate::ConnectorRecord>(body) {
            Ok(record) => record,
            Err(_) if !has_newline && cursor + line.len() == bytes.len() => break,
            Err(err) => return Err(display(err)),
        };
        committed.insert((record.stream, record.partition, record.offset));
        valid_len = valid_len.saturating_add(line.len() as u64);
        cursor += line.len();
    }
    Ok((committed, valid_len))
}

fn persist_append_index(
    path: &PathBuf,
    log_len: u64,
    committed: &BTreeSet<(String, u32, u64)>,
) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(display)?;
    }
    let temporary = path.with_extension("idx.tmp");
    let body = serde_json::to_vec(&AppendIndex {
        log_len,
        committed: committed.clone(),
    })
    .map_err(display)?;
    std::fs::write(&temporary, body).map_err(display)?;
    OpenOptions::new()
        .read(true)
        .open(&temporary)
        .map_err(display)?
        .sync_data()
        .map_err(display)?;
    std::fs::rename(temporary, path).map_err(display)?;
    Ok(())
}

fn fence(expected: u64, actual: u64) -> Result<(), String> {
    if expected == actual {
        Ok(())
    } else {
        Err("connector generation is fenced".to_string())
    }
}

fn safe_component(value: &str) -> Result<&str, String> {
    if !value.is_empty()
        && value
            .chars()
            .all(|value| value.is_ascii_alphanumeric() || matches!(value, '-' | '_'))
    {
        Ok(value)
    } else {
        Err("unsafe object-store path component".to_string())
    }
}

fn display(error: impl std::fmt::Display) -> String {
    error.to_string()
}
