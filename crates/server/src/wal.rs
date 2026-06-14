use std::{
    collections::{BTreeSet, HashMap, HashSet},
    fs::{File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use protocol::subject;

use crate::error::{Result, ResultExt};

const WAL_FILE: &str = "broker.wal";
const KIND_PUBLISH: u8 = 1;
const KIND_CONSUMER_UPSERT: u8 = 2;
const KIND_DELIVERY_ATTEMPT: u8 = 3;
const KIND_ACK: u8 = 4;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishRecord {
    pub seq: u64,
    pub subject: String,
    pub reply_to: Option<String>,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerRecord {
    pub consumer_id: String,
    pub filter_subject: String,
    pub queue_group: Option<String>,
    pub ack_timeout_ms: u64,
    pub max_in_flight: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliveryAttemptRecord {
    pub seq: u64,
    pub consumer_id: String,
    pub delivery_id: u64,
    pub deadline_ms: u64,
    pub attempt: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AckRecord {
    pub seq: u64,
    pub consumer_id: String,
    pub delivery_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
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
    file: File,
    path: PathBuf,
    next_seq: u64,
    next_delivery_id: u64,
    fsync_interval: Duration,
    last_sync: Instant,
}

impl Wal {
    pub fn open(dir: impl AsRef<Path>, fsync_interval: Duration) -> Result<(Self, Replay)> {
        std::fs::create_dir_all(dir.as_ref())
            .with_context(|| format!("creating WAL directory {}", dir.as_ref().display()))?;
        let path = dir.as_ref().join(WAL_FILE);
        let replay = replay_path(&path)?;
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&path)
            .with_context(|| format!("opening WAL {}", path.display()))?;
        let wal = Self {
            file,
            path,
            next_seq: replay.next_seq,
            next_delivery_id: replay.next_delivery_id,
            fsync_interval,
            last_sync: Instant::now(),
        };
        Ok((wal, replay))
    }

    pub fn append_publish(
        &mut self,
        subject: &str,
        reply_to: Option<&str>,
        payload: &[u8],
    ) -> Result<PublishRecord> {
        let seq = self.next_seq;
        self.next_seq += 1;

        let record = PublishRecord {
            seq,
            subject: subject.to_string(),
            reply_to: reply_to.map(str::to_string),
            payload: payload.to_vec(),
        };
        self.write_publish(&record)?;
        Ok(record)
    }

    pub fn append_consumer_upsert(&mut self, record: &ConsumerRecord) -> Result<()> {
        let mut body = Vec::new();
        put_string(&mut body, &record.consumer_id)?;
        put_string(&mut body, &record.filter_subject)?;
        put_option_string(&mut body, record.queue_group.as_deref())?;
        put_u64(&mut body, record.ack_timeout_ms);
        put_u32(
            &mut body,
            record
                .max_in_flight
                .try_into()
                .context("max_in_flight too large")?,
        );
        self.append_record(KIND_CONSUMER_UPSERT, &body)
    }

    pub fn append_delivery_attempt(
        &mut self,
        seq: u64,
        consumer_id: &str,
        deadline_ms: u64,
        attempt: u32,
    ) -> Result<DeliveryAttemptRecord> {
        let delivery_id = self.next_delivery_id;
        self.next_delivery_id += 1;
        let record = DeliveryAttemptRecord {
            seq,
            consumer_id: consumer_id.to_string(),
            delivery_id,
            deadline_ms,
            attempt,
        };
        self.write_delivery_attempt(&record)?;
        Ok(record)
    }

    pub fn append_ack(
        &mut self,
        seq: u64,
        consumer_id: &str,
        delivery_id: u64,
    ) -> Result<AckRecord> {
        let record = AckRecord {
            seq,
            consumer_id: consumer_id.to_string(),
            delivery_id,
        };
        let mut body = Vec::new();
        put_u64(&mut body, record.seq);
        put_string(&mut body, &record.consumer_id)?;
        put_u64(&mut body, record.delivery_id);
        self.append_record(KIND_ACK, &body)?;
        Ok(record)
    }

    pub fn flush_due(&mut self) -> Result<()> {
        if self.last_sync.elapsed() >= self.fsync_interval {
            self.flush()?;
        }
        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        self.file
            .flush()
            .with_context(|| format!("flushing {}", self.path.display()))?;
        self.file
            .sync_data()
            .with_context(|| format!("fsyncing {}", self.path.display()))?;
        self.last_sync = Instant::now();
        Ok(())
    }

    pub fn checkpoint(
        &mut self,
        messages: impl IntoIterator<Item = PublishRecord>,
        consumers: impl IntoIterator<Item = ReplayedConsumer>,
    ) -> Result<()> {
        let tmp = self.path.with_extension("wal.tmp");
        {
            let mut file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&tmp)
                .with_context(|| format!("opening checkpoint {}", tmp.display()))?;
            for consumer in consumers {
                write_consumer_upsert_to(&mut file, &consumer.record)?;
                for acked in consumer.acked {
                    let mut body = Vec::new();
                    put_u64(&mut body, acked);
                    put_string(&mut body, &consumer.record.consumer_id)?;
                    put_u64(&mut body, 0);
                    write_record_to(&mut file, KIND_ACK, &body)?;
                }
                for attempt in consumer.in_flight.into_values() {
                    write_delivery_attempt_to(&mut file, &attempt)?;
                }
            }
            for message in messages {
                write_publish_to(&mut file, &message)?;
            }
            file.flush()?;
            file.sync_data()?;
        }
        std::fs::rename(&tmp, &self.path)
            .with_context(|| format!("renaming checkpoint {}", tmp.display()))?;
        self.file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&self.path)
            .with_context(|| format!("reopening WAL {}", self.path.display()))?;
        self.flush()?;
        Ok(())
    }

    fn write_publish(&mut self, record: &PublishRecord) -> Result<()> {
        write_publish_to(&mut self.file, record)
            .with_context(|| format!("appending publish to {}", self.path.display()))
    }

    fn write_delivery_attempt(&mut self, record: &DeliveryAttemptRecord) -> Result<()> {
        write_delivery_attempt_to(&mut self.file, record)
            .with_context(|| format!("appending delivery attempt to {}", self.path.display()))
    }

    fn append_record(&mut self, kind: u8, body: &[u8]) -> Result<()> {
        write_record_to(&mut self.file, kind, body)
            .with_context(|| format!("appending WAL record to {}", self.path.display()))
    }
}

fn replay_path(path: &Path) -> Result<Replay> {
    let mut file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(path)
        .with_context(|| format!("opening WAL {}", path.display()))?;
    let mut offset = 0;
    let mut max_seq = 0;
    let mut max_delivery_id = 0;
    let mut messages = HashMap::new();
    let mut consumers: HashMap<String, ReplayedConsumer> = HashMap::new();

    loop {
        match read_record(&mut file) {
            Ok(Some((kind, body, bytes_read))) => {
                offset += bytes_read;
                match kind {
                    KIND_PUBLISH => {
                        let record = decode_publish(&body)?;
                        max_seq = max_seq.max(record.seq);
                        for consumer in consumers.values_mut() {
                            if subject::matches(&consumer.record.filter_subject, &record.subject)
                                && !consumer.acked.contains(&record.seq)
                            {
                                consumer.pending.insert(record.seq);
                            }
                        }
                        messages.insert(record.seq, record);
                    }
                    KIND_CONSUMER_UPSERT => {
                        let record = decode_consumer_upsert(&body)?;
                        consumers
                            .entry(record.consumer_id.clone())
                            .and_modify(|consumer| consumer.record = record.clone())
                            .or_insert_with(|| ReplayedConsumer {
                                record,
                                pending: BTreeSet::new(),
                                in_flight: HashMap::new(),
                                acked: HashSet::new(),
                            });
                    }
                    KIND_DELIVERY_ATTEMPT => {
                        let attempt = decode_delivery_attempt(&body)?;
                        max_delivery_id = max_delivery_id.max(attempt.delivery_id);
                        if let Some(consumer) = consumers.get_mut(&attempt.consumer_id) {
                            if !consumer.acked.contains(&attempt.seq) {
                                consumer.pending.remove(&attempt.seq);
                                consumer.in_flight.insert(attempt.seq, attempt);
                            }
                        }
                    }
                    KIND_ACK => {
                        let ack = decode_ack(&body)?;
                        max_delivery_id = max_delivery_id.max(ack.delivery_id);
                        if let Some(consumer) = consumers.get_mut(&ack.consumer_id) {
                            consumer.acked.insert(ack.seq);
                            consumer.pending.remove(&ack.seq);
                            consumer.in_flight.remove(&ack.seq);
                        }
                    }
                    _ => break,
                }
            }
            Ok(None) => break,
            Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(_) => break,
        }
    }

    let len = file.metadata()?.len();
    if offset < len {
        file.set_len(offset)?;
        file.seek(SeekFrom::Start(offset))?;
    }

    for consumer in consumers.values_mut() {
        let expired: Vec<_> = consumer.in_flight.keys().copied().collect();
        for seq in expired {
            consumer.in_flight.remove(&seq);
            if !consumer.acked.contains(&seq) && messages.contains_key(&seq) {
                consumer.pending.insert(seq);
            }
        }
    }

    Ok(Replay {
        messages,
        consumers,
        next_seq: max_seq + 1,
        next_delivery_id: max_delivery_id + 1,
    })
}

fn write_publish_to(file: &mut File, record: &PublishRecord) -> Result<()> {
    let mut body = Vec::new();
    put_u64(&mut body, record.seq);
    put_string(&mut body, &record.subject)?;
    put_option_string(&mut body, record.reply_to.as_deref())?;
    put_bytes(&mut body, &record.payload)?;
    write_record_to(file, KIND_PUBLISH, &body)
}

fn write_consumer_upsert_to(file: &mut File, record: &ConsumerRecord) -> Result<()> {
    let mut body = Vec::new();
    put_string(&mut body, &record.consumer_id)?;
    put_string(&mut body, &record.filter_subject)?;
    put_option_string(&mut body, record.queue_group.as_deref())?;
    put_u64(&mut body, record.ack_timeout_ms);
    put_u32(
        &mut body,
        record
            .max_in_flight
            .try_into()
            .context("max_in_flight too large")?,
    );
    write_record_to(file, KIND_CONSUMER_UPSERT, &body)
}

fn write_delivery_attempt_to(file: &mut File, record: &DeliveryAttemptRecord) -> Result<()> {
    let mut body = Vec::new();
    put_u64(&mut body, record.seq);
    put_string(&mut body, &record.consumer_id)?;
    put_u64(&mut body, record.delivery_id);
    put_u64(&mut body, record.deadline_ms);
    put_u32(&mut body, record.attempt);
    write_record_to(file, KIND_DELIVERY_ATTEMPT, &body)
}

fn write_record_to(file: &mut File, kind: u8, body: &[u8]) -> Result<()> {
    let len = body.len() + 1;
    let len: u32 = len.try_into().context("WAL record too large")?;
    file.write_all(&len.to_le_bytes())?;
    file.write_all(&[kind])?;
    file.write_all(body)?;
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&[kind]);
    hasher.update(body);
    file.write_all(&hasher.finalize().to_le_bytes())?;
    Ok(())
}

fn read_record(file: &mut File) -> io::Result<Option<(u8, Vec<u8>, u64)>> {
    let mut len_bytes = [0; 4];
    match file.read_exact(&mut len_bytes) {
        Ok(()) => {}
        Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(err) => return Err(err),
    }
    let len = u32::from_le_bytes(len_bytes) as usize;
    if len == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "zero-length WAL record",
        ));
    }
    let mut record = vec![0; len];
    file.read_exact(&mut record)?;
    let mut crc_bytes = [0; 4];
    file.read_exact(&mut crc_bytes)?;
    let expected = u32::from_le_bytes(crc_bytes);
    let actual = crc32fast::hash(&record);
    if expected != actual {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "WAL checksum mismatch",
        ));
    }
    let kind = record[0];
    Ok(Some((kind, record[1..].to_vec(), 4 + len as u64 + 4)))
}

fn decode_publish(body: &[u8]) -> Result<PublishRecord> {
    let mut cursor = Cursor {
        bytes: body,
        pos: 0,
    };
    let record = PublishRecord {
        seq: cursor.u64()?,
        subject: cursor.string()?,
        reply_to: cursor.option_string()?,
        payload: cursor.bytes()?,
    };
    cursor.finish()?;
    Ok(record)
}

fn decode_consumer_upsert(body: &[u8]) -> Result<ConsumerRecord> {
    let mut cursor = Cursor {
        bytes: body,
        pos: 0,
    };
    let record = ConsumerRecord {
        consumer_id: cursor.string()?,
        filter_subject: cursor.string()?,
        queue_group: cursor.option_string()?,
        ack_timeout_ms: cursor.u64()?,
        max_in_flight: cursor.u32()? as usize,
    };
    cursor.finish()?;
    Ok(record)
}

fn decode_delivery_attempt(body: &[u8]) -> Result<DeliveryAttemptRecord> {
    let mut cursor = Cursor {
        bytes: body,
        pos: 0,
    };
    let record = DeliveryAttemptRecord {
        seq: cursor.u64()?,
        consumer_id: cursor.string()?,
        delivery_id: cursor.u64()?,
        deadline_ms: cursor.u64()?,
        attempt: cursor.u32()?,
    };
    cursor.finish()?;
    Ok(record)
}

fn decode_ack(body: &[u8]) -> Result<AckRecord> {
    let mut cursor = Cursor {
        bytes: body,
        pos: 0,
    };
    let record = AckRecord {
        seq: cursor.u64()?,
        consumer_id: cursor.string()?,
        delivery_id: cursor.u64()?,
    };
    cursor.finish()?;
    Ok(record)
}

fn put_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_string(out: &mut Vec<u8>, value: &str) -> Result<()> {
    put_bytes(out, value.as_bytes())
}

fn put_option_string(out: &mut Vec<u8>, value: Option<&str>) -> Result<()> {
    match value {
        Some(value) => {
            out.push(1);
            put_string(out, value)
        }
        None => {
            out.push(0);
            Ok(())
        }
    }
}

fn put_bytes(out: &mut Vec<u8>, value: &[u8]) -> Result<()> {
    put_u32(out, value.len().try_into().context("field too large")?);
    out.extend_from_slice(value);
    Ok(())
}

struct Cursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl Cursor<'_> {
    fn u32(&mut self) -> Result<u32> {
        let bytes = self.take(4)?;
        Ok(u32::from_le_bytes(bytes.try_into().unwrap()))
    }

    fn u64(&mut self) -> Result<u64> {
        let bytes = self.take(8)?;
        Ok(u64::from_le_bytes(bytes.try_into().unwrap()))
    }

    fn string(&mut self) -> Result<String> {
        String::from_utf8(self.bytes()?).context("WAL string is not UTF-8")
    }

    fn option_string(&mut self) -> Result<Option<String>> {
        let present = self.take(1)?[0];
        match present {
            0 => Ok(None),
            1 => Ok(Some(self.string()?)),
            _ => crate::broker_bail!("invalid optional string marker"),
        }
    }

    fn bytes(&mut self) -> Result<Vec<u8>> {
        let len = self.u32()? as usize;
        Ok(self.take(len)?.to_vec())
    }

    fn take(&mut self, len: usize) -> Result<&[u8]> {
        if self.pos + len > self.bytes.len() {
            crate::broker_bail!("truncated WAL record");
        }
        let start = self.pos;
        self.pos += len;
        Ok(&self.bytes[start..self.pos])
    }

    fn finish(&self) -> Result<()> {
        crate::broker_ensure!(self.pos == self.bytes.len(), "trailing bytes in WAL record");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs::OpenOptions,
        io::Write,
        path::Path,
        sync::atomic::{AtomicU64, Ordering},
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    use super::*;

    static TEST_DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TestDir(std::path::PathBuf);

    impl TestDir {
        fn new() -> Self {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let counter = TEST_DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "broker-wal-test-{}-{nanos}-{counter}",
                std::process::id()
            ));
            std::fs::create_dir_all(&path).unwrap();
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn replays_consumer_publish_delivery_and_ack_state() {
        let dir = TestDir::new();
        let (mut wal, _) = Wal::open(dir.path(), Duration::from_millis(1)).unwrap();
        let consumer = ConsumerRecord {
            consumer_id: "durable-client-1".into(),
            filter_subject: "orders.*".into(),
            queue_group: None,
            ack_timeout_ms: 30_000,
            max_in_flight: 1024,
        };
        wal.append_consumer_upsert(&consumer).unwrap();
        let first = wal.append_publish("orders.created", None, b"one").unwrap();
        let attempt = wal
            .append_delivery_attempt(first.seq, &consumer.consumer_id, 1_000, 1)
            .unwrap();
        wal.append_ack(first.seq, &consumer.consumer_id, attempt.delivery_id)
            .unwrap();
        let second = wal.append_publish("orders.created", None, b"two").unwrap();
        wal.append_delivery_attempt(second.seq, &consumer.consumer_id, 2_000, 1)
            .unwrap();
        wal.flush().unwrap();
        drop(wal);

        let (_, replay) = Wal::open(dir.path(), Duration::from_millis(1)).unwrap();
        let replayed = replay.consumers.get(&consumer.consumer_id).unwrap();
        assert!(replayed.acked.contains(&first.seq));
        assert!(replayed.pending.contains(&second.seq));
        assert!(replayed.in_flight.is_empty());
    }

    #[test]
    fn later_consumers_do_not_receive_older_publishes_on_replay() {
        let dir = TestDir::new();
        let (mut wal, _) = Wal::open(dir.path(), Duration::from_millis(1)).unwrap();
        wal.append_publish("orders.created", None, b"old").unwrap();
        let consumer = ConsumerRecord {
            consumer_id: "durable-client-1".into(),
            filter_subject: "orders.*".into(),
            queue_group: None,
            ack_timeout_ms: 30_000,
            max_in_flight: 1024,
        };
        wal.append_consumer_upsert(&consumer).unwrap();
        wal.flush().unwrap();
        drop(wal);

        let (_, replay) = Wal::open(dir.path(), Duration::from_millis(1)).unwrap();
        assert!(replay.consumers[&consumer.consumer_id].pending.is_empty());
    }

    #[test]
    fn truncates_partial_record_on_replay() {
        let dir = TestDir::new();
        let path = dir.path().join(WAL_FILE);
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)
            .unwrap();
        file.write_all(&100_u32.to_le_bytes()).unwrap();
        file.write_all(b"partial").unwrap();
        drop(file);

        let (_, replay) = Wal::open(dir.path(), Duration::from_millis(1)).unwrap();
        assert!(replay.messages.is_empty());
        assert_eq!(std::fs::metadata(path).unwrap().len(), 0);
    }
}
