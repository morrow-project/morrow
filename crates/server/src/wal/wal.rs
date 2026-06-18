use super::*;

impl Wal {
    pub fn open(
        dir: impl AsRef<Path>,
        fsync_interval: Duration,
        segment_bytes: u64,
    ) -> Result<(Self, Replay)> {
        let dir = dir.as_ref();
        crate::broker_ensure!(
            segment_bytes > 0,
            "WAL segment size must be greater than zero"
        );
        std::fs::create_dir_all(dir)
            .with_context(|| format!("creating WAL directory {}", dir.display()))?;
        let output = replay_dir(dir)?;
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&output.active_path)
            .with_context(|| format!("opening WAL segment {}", output.active_path.display()))?;
        let mut metrics = WalMetrics {
            last_replay_duration_ms: output.replay.duration_ms,
            truncations: output.replay.truncations,
            ..WalMetrics::default()
        };
        if output.replay.truncations > 0 {
            metrics.truncations = output.replay.truncations;
        }
        let wal = Self {
            file,
            dir: dir.to_path_buf(),
            active_segment_id: output.active_segment_id,
            active_path: output.active_path,
            active_bytes: output.active_bytes,
            sealed_segments: output.sealed_segments,
            segment_bytes,
            next_seq: output.replay.next_seq,
            next_delivery_id: output.replay.next_delivery_id,
            fsync_interval,
            last_sync: Instant::now(),
            metrics,
        };
        Ok((wal, output.replay))
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
        self.append_record(KIND_CONSUMER_UPSERT, &consumer_upsert_body(record)?)
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
        self.append_record(KIND_ACK, &ack_body(&record)?)?;
        Ok(record)
    }

    pub fn flush_due(&mut self) -> Result<()> {
        if self.last_sync.elapsed() >= self.fsync_interval {
            self.flush()?;
        }
        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        let started = Instant::now();
        self.file
            .flush()
            .with_context(|| format!("flushing {}", self.active_path.display()))?;
        self.file
            .sync_data()
            .with_context(|| format!("fsyncing {}", self.active_path.display()))?;
        self.metrics.last_fsync_duration_ms = millis_since(started);
        self.last_sync = Instant::now();
        Ok(())
    }

    pub fn checkpoint(
        &mut self,
        messages: impl IntoIterator<Item = PublishRecord>,
        consumers: impl IntoIterator<Item = ReplayedConsumer>,
    ) -> Result<()> {
        let started = Instant::now();
        self.flush()?;
        let next_id = self.active_segment_id + 1;
        let tmp = tmp_segment_path(&self.dir, next_id);
        let path = segment_path(&self.dir, next_id);
        let mut checkpoint_bytes = SEGMENT_HEADER_LEN;
        {
            let mut file = create_segment(&tmp)?;
            checkpoint_bytes += write_compact_state(&mut file, messages, consumers)?;
            file.flush()?;
            file.sync_data()?;
        }
        std::fs::rename(&tmp, &path)
            .with_context(|| format!("renaming checkpoint segment {}", path.display()))?;
        fsync_dir(&self.dir)?;
        drop(std::mem::replace(
            &mut self.file,
            OpenOptions::new()
                .create(true)
                .read(true)
                .append(true)
                .open(&path)
                .with_context(|| format!("opening checkpoint segment {}", path.display()))?,
        ));

        let mut deleted = delete_segments(&self.sealed_segments)?;
        if self.active_path.exists() {
            std::fs::remove_file(&self.active_path).with_context(|| {
                format!("removing old WAL segment {}", self.active_path.display())
            })?;
            deleted += 1;
        }
        self.metrics.deleted_segments += deleted;
        self.sealed_segments.clear();
        self.active_segment_id = next_id;
        self.active_path = path;
        self.active_bytes = checkpoint_bytes;
        self.metrics.checkpoints += 1;
        self.metrics.last_checkpoint_duration_ms = millis_since(started);
        self.last_sync = Instant::now();
        fsync_dir(&self.dir)?;
        Ok(())
    }

    pub fn status(&self, retained_message_count: usize, consumer_count: usize) -> WalStatus {
        WalStatus {
            active_segment_id: self.active_segment_id,
            active_segment_path: self.active_path.display().to_string(),
            active_segment_bytes: self.active_bytes,
            sealed_segment_count: self.sealed_segments.len(),
            total_wal_bytes: self.total_wal_bytes(),
            retained_message_count,
            consumer_count,
            next_seq: self.next_seq,
            next_delivery_id: self.next_delivery_id,
            last_replay_duration_ms: self.metrics.last_replay_duration_ms,
            last_checkpoint_duration_ms: self.metrics.last_checkpoint_duration_ms,
            last_fsync_duration_ms: self.metrics.last_fsync_duration_ms,
            rotations: self.metrics.rotations,
            checkpoints: self.metrics.checkpoints,
            truncations: self.metrics.truncations,
            deleted_segments: self.metrics.deleted_segments,
        }
    }

    pub(super) fn write_publish(&mut self, record: &PublishRecord) -> Result<()> {
        self.append_record(KIND_PUBLISH, &publish_body(record)?)
    }

    pub(super) fn write_delivery_attempt(&mut self, record: &DeliveryAttemptRecord) -> Result<()> {
        self.append_record(KIND_DELIVERY_ATTEMPT, &delivery_attempt_body(record)?)
    }

    pub(super) fn append_record(&mut self, kind: u8, body: &[u8]) -> Result<()> {
        let bytes = record_size(body)?;
        self.rotate_if_needed(bytes)?;
        write_record_to(&mut self.file, kind, body)
            .with_context(|| format!("appending WAL record to {}", self.active_path.display()))?;
        self.active_bytes += bytes;
        Ok(())
    }

    fn rotate_if_needed(&mut self, next_record_bytes: u64) -> Result<()> {
        if self.active_bytes > SEGMENT_HEADER_LEN
            && self.active_bytes + next_record_bytes > self.segment_bytes
        {
            self.flush()?;
            self.sealed_segments.push(SegmentInfo {
                id: self.active_segment_id,
                path: self.active_path.clone(),
                bytes: self.active_bytes,
            });
            self.active_segment_id += 1;
            self.active_path = segment_path(&self.dir, self.active_segment_id);
            self.file = create_segment(&self.active_path)?;
            self.active_bytes = SEGMENT_HEADER_LEN;
            self.metrics.rotations += 1;
            fsync_dir(&self.dir)?;
        }
        Ok(())
    }

    fn total_wal_bytes(&self) -> u64 {
        self.active_bytes
            + self
                .sealed_segments
                .iter()
                .map(|segment| segment.bytes)
                .sum::<u64>()
    }
}

pub(super) fn segment_path(dir: &Path, id: u64) -> PathBuf {
    dir.join(format!("{id:020}.{SEGMENT_EXTENSION}"))
}

pub(super) fn tmp_segment_path(dir: &Path, id: u64) -> PathBuf {
    dir.join(format!("{id:020}.{SEGMENT_TMP_EXTENSION}"))
}

pub(super) fn create_segment(path: &Path) -> Result<File> {
    let mut file = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .append(true)
        .open(path)
        .with_context(|| format!("creating WAL segment {}", path.display()))?;
    file.write_all(SEGMENT_HEADER)?;
    Ok(file)
}

pub(super) fn write_compact_state(
    file: &mut File,
    messages: impl IntoIterator<Item = PublishRecord>,
    consumers: impl IntoIterator<Item = ReplayedConsumer>,
) -> Result<u64> {
    let mut bytes = 0;
    for consumer in consumers {
        let body = consumer_upsert_body(&consumer.record)?;
        write_record_to(file, KIND_CONSUMER_UPSERT, &body)?;
        bytes += record_size(&body)?;
        let consumer_id = consumer.record.consumer_id.clone();
        for acked in consumer.acked {
            let body = ack_body(&AckRecord {
                seq: acked,
                consumer_id: consumer_id.clone(),
                delivery_id: 0,
            })?;
            write_record_to(file, KIND_ACK, &body)?;
            bytes += record_size(&body)?;
        }
        for pending in consumer.pending {
            let body = delivery_attempt_body(&DeliveryAttemptRecord {
                seq: pending,
                consumer_id: consumer_id.clone(),
                delivery_id: 0,
                deadline_ms: 0,
                attempt: 0,
            })?;
            write_record_to(file, KIND_DELIVERY_ATTEMPT, &body)?;
            bytes += record_size(&body)?;
        }
        for attempt in consumer.in_flight.into_values() {
            let body = delivery_attempt_body(&attempt)?;
            write_record_to(file, KIND_DELIVERY_ATTEMPT, &body)?;
            bytes += record_size(&body)?;
        }
    }
    for message in messages {
        let body = publish_body(&message)?;
        write_record_to(file, KIND_PUBLISH, &body)?;
        bytes += record_size(&body)?;
    }
    Ok(bytes)
}

pub(super) fn segmented_paths(dir: &Path) -> Result<Vec<SegmentInfo>> {
    let mut segments = Vec::new();
    for entry in
        std::fs::read_dir(dir).with_context(|| format!("reading WAL dir {}", dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|value| value.to_str()) != Some(SEGMENT_EXTENSION) {
            continue;
        }
        let Some(stem) = path.file_stem().and_then(|value| value.to_str()) else {
            continue;
        };
        if stem.len() != 20 || !stem.chars().all(|ch| ch.is_ascii_digit()) {
            continue;
        }
        let id = stem
            .parse()
            .with_context(|| format!("parsing WAL segment id {}", path.display()))?;
        let bytes = path.metadata()?.len();
        segments.push(SegmentInfo { id, path, bytes });
    }
    segments.sort_by_key(|segment| segment.id);
    for pair in segments.windows(2) {
        crate::broker_ensure!(pair[0].id != pair[1].id, "duplicate WAL segment id");
    }
    Ok(segments)
}

pub(super) fn remove_tmp_segments(dir: &Path) -> Result<()> {
    for entry in
        std::fs::read_dir(dir).with_context(|| format!("reading WAL dir {}", dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if path
            .file_name()
            .and_then(|value| value.to_str())
            .is_some_and(|name| name.ends_with(".wal.tmp"))
        {
            std::fs::remove_file(&path)
                .with_context(|| format!("removing temporary WAL segment {}", path.display()))?;
        }
    }
    Ok(())
}

pub(super) fn fsync_dir(dir: &Path) -> Result<()> {
    let file = File::open(dir).with_context(|| format!("opening WAL dir {}", dir.display()))?;
    file.sync_all()
        .with_context(|| format!("fsyncing WAL dir {}", dir.display()))
}

pub(super) fn millis_since(started: Instant) -> u64 {
    started.elapsed().as_millis().try_into().unwrap_or(u64::MAX)
}

fn delete_segments(segments: &[SegmentInfo]) -> Result<u64> {
    let mut deleted = 0;
    for segment in segments {
        if segment.path.exists() {
            std::fs::remove_file(&segment.path)
                .with_context(|| format!("removing WAL segment {}", segment.path.display()))?;
            deleted += 1;
        }
    }
    Ok(deleted)
}
