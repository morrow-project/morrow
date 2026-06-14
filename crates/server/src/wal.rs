use crate::error::{Result, ResultExt};
use protocol::subject;
use std::{
    collections::{BTreeSet, HashMap, HashSet},
    fs::{File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};
const WAL_FILE: &str = "broker.wal";
const KIND_PUBLISH: u8 = 1;
const KIND_CONSUMER_UPSERT: u8 = 2;
const KIND_DELIVERY_ATTEMPT: u8 = 3;
const KIND_ACK: u8 = 4;
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct PublishRecord {
    pub seq: u64,
    pub subject: String,
    pub reply_to: Option<String>,
    pub payload: Vec<u8>,
}
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct ConsumerRecord {
    pub consumer_id: String,
    pub filter_subject: String,
    pub queue_group: Option<String>,
    pub ack_timeout_ms: u64,
    pub max_in_flight: usize,
}
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct DeliveryAttemptRecord {
    pub seq: u64,
    pub consumer_id: String,
    pub delivery_id: u64,
    pub deadline_ms: u64,
    pub attempt: u32,
}
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct AckRecord {
    pub seq: u64,
    pub consumer_id: String,
    pub delivery_id: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct ReplayedConsumer {
    pub record: ConsumerRecord,
    pub pending: BTreeSet<u64>,
    pub in_flight: HashMap<u64, DeliveryAttemptRecord>,
    pub acked: HashSet<u64>,
}
#[derive(Debug)]
pub struct Replay {
    pub messages: HashMap<u64, PublishRecord>,
    pub consumers: HashMap<String, ReplayedConsumer>,
    pub next_seq: u64,
    pub next_delivery_id: u64,
}
#[derive(Debug)]
pub struct Wal {
    pub(super) file: File,
    pub(super) path: PathBuf,
    pub(super) next_seq: u64,
    pub(super) next_delivery_id: u64,
    pub(super) fsync_interval: Duration,
    pub(super) last_sync: Instant,
}

mod cursor;
mod record_io;
mod replay;
mod wal;

use self::{cursor::*, record_io::*, replay::*};

#[cfg(test)]
mod tests;
