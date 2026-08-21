use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectorRecord {
    pub stream: String,
    pub partition: u32,
    pub offset: u64,
    pub subject: String,
    pub key: Option<Vec<u8>>,
    pub payload: Vec<u8>,
    pub schema_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectorBatch {
    pub generation: u64,
    pub records: Vec<ConnectorRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SinkCompletion {
    pub offsets: BTreeMap<(String, u32), u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceRecord {
    pub source_offset: String,
    pub subject: String,
    pub key: Option<Vec<u8>>,
    pub payload: Vec<u8>,
}

pub trait Connector: Send {
    fn name(&self) -> &str;
    fn generation(&self) -> u64;
}

pub trait SourceTask: Connector {
    fn poll(&mut self, max_records: usize, max_bytes: usize) -> Result<Vec<SourceRecord>, String>;
    fn commit_source_offset(&mut self, source_offset: &str) -> Result<(), String>;
}

pub trait SinkTask: Connector {
    fn write_batch(&mut self, batch: &ConnectorBatch) -> Result<SinkCompletion, String>;
    fn completion_boundary(&self) -> &'static str;
}
