use crate::{Connector, ConnectorBatch, SinkCompletion, SinkTask};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::OpenOptions,
    io::Write,
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
        for record in &batch.records {
            let stream = safe_component(&record.stream)?;
            let dir = self
                .root
                .join(stream)
                .join(format!("partition-{:05}", record.partition));
            std::fs::create_dir_all(&dir).map_err(display)?;
            let path = dir.join(format!("{:020}.record", record.offset));
            let body = serde_json::to_vec(record).map_err(display)?;
            if path.exists() {
                if std::fs::read(&path).map_err(display)? != body {
                    return Err("object key already contains different data".to_string());
                }
            } else {
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
            offsets.insert((record.stream.clone(), record.partition), record.offset);
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
    committed: BTreeSet<(String, u32, u64)>,
}

impl AppendDatabaseSink {
    pub fn open(
        name: impl Into<String>,
        generation: u64,
        path: impl Into<PathBuf>,
    ) -> Result<Self, String> {
        let path = path.into();
        let committed = if path.exists() {
            std::fs::read_to_string(&path)
                .map_err(display)?
                .lines()
                .filter_map(|line| serde_json::from_str::<crate::ConnectorRecord>(line).ok())
                .map(|record| (record.stream, record.partition, record.offset))
                .collect()
        } else {
            BTreeSet::new()
        };
        Ok(Self {
            name: name.into(),
            generation,
            path,
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
        Ok(SinkCompletion { offsets })
    }

    fn completion_boundary(&self) -> &'static str {
        "idempotency key recorded and append log fsynced"
    }
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
