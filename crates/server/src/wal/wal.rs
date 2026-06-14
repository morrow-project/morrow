use super::*;

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

    pub(super) fn write_publish(&mut self, record: &PublishRecord) -> Result<()> {
        write_publish_to(&mut self.file, record)
            .with_context(|| format!("appending publish to {}", self.path.display()))
    }

    pub(super) fn write_delivery_attempt(&mut self, record: &DeliveryAttemptRecord) -> Result<()> {
        write_delivery_attempt_to(&mut self.file, record)
            .with_context(|| format!("appending delivery attempt to {}", self.path.display()))
    }

    pub(super) fn append_record(&mut self, kind: u8, body: &[u8]) -> Result<()> {
        write_record_to(&mut self.file, kind, body)
            .with_context(|| format!("appending WAL record to {}", self.path.display()))
    }
}
