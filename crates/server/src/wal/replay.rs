use super::wal::*;
use super::*;

#[derive(Debug)]
pub(super) struct ReplayOutput {
    pub(super) replay: Replay,
    pub(super) active_segment_id: u64,
    pub(super) active_path: PathBuf,
    pub(super) active_bytes: u64,
    pub(super) sealed_segments: Vec<SegmentInfo>,
}

#[derive(Default)]
struct ReplayState {
    max_seq: u64,
    max_delivery_id: u64,
    messages: HashMap<u64, PublishRecord>,
    consumers: HashMap<String, ReplayedConsumer>,
}

pub(super) fn replay_dir(dir: &Path) -> Result<ReplayOutput> {
    let started = Instant::now();
    remove_tmp_segments(dir)?;
    let mut truncations = 0;
    if segmented_paths(dir)?.is_empty() {
        migrate_legacy_if_present(dir, &mut truncations)?;
    }
    let mut segments = segmented_paths(dir)?;
    if segments.is_empty() {
        let path = segment_path(dir, 1);
        let mut file = create_segment(&path)?;
        file.flush()?;
        file.sync_data()?;
        fsync_dir(dir)?;
        segments.push(SegmentInfo {
            id: 1,
            bytes: SEGMENT_HEADER_LEN,
            path,
        });
    }

    let mut state = ReplayState::default();
    let highest_id = segments.last().map(|segment| segment.id).unwrap_or(1);
    for segment in &mut segments {
        replay_segment(
            segment,
            segment.id == highest_id,
            &mut state,
            &mut truncations,
        )?;
    }
    expire_in_flight(&mut state);
    cleanup_acked_messages(&mut state.messages, &state.consumers);

    let active = segments.pop().expect("at least one WAL segment");
    let replay = Replay {
        messages: state.messages,
        consumers: state.consumers,
        next_seq: state.max_seq + 1,
        next_delivery_id: state.max_delivery_id + 1,
        duration_ms: millis_since(started),
        truncations,
    };
    Ok(ReplayOutput {
        replay,
        active_segment_id: active.id,
        active_path: active.path,
        active_bytes: active.bytes,
        sealed_segments: segments,
    })
}

fn replay_segment(
    segment: &mut SegmentInfo,
    is_active: bool,
    state: &mut ReplayState,
    truncations: &mut u64,
) -> Result<()> {
    let mut file = OpenOptions::new()
        .read(true)
        .write(is_active)
        .open(&segment.path)
        .with_context(|| format!("opening WAL segment {}", segment.path.display()))?;
    let mut header = vec![0; SEGMENT_HEADER.len()];
    file.read_exact(&mut header)
        .with_context(|| format!("reading WAL segment header {}", segment.path.display()))?;
    crate::broker_ensure!(
        header == SEGMENT_HEADER,
        "WAL segment {} has unsupported format",
        segment.path.display()
    );

    let mut offset = SEGMENT_HEADER_LEN;
    loop {
        match read_record(&mut file) {
            Ok(Some((kind, body, bytes_read))) => {
                offset += bytes_read;
                apply_record(kind, &body, state)
                    .with_context(|| format!("replaying WAL segment {}", segment.path.display()))?;
            }
            Ok(None) => break,
            Err(err) if err.kind() == io::ErrorKind::UnexpectedEof && is_active => {
                file.set_len(offset)?;
                file.seek(SeekFrom::Start(offset))?;
                *truncations += 1;
                break;
            }
            Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => {
                return Err(BrokerError::with_source(
                    format!("truncated sealed WAL segment {}", segment.path.display()),
                    err,
                ));
            }
            Err(err) => {
                return Err(BrokerError::with_source(
                    format!("corrupt WAL segment {}", segment.path.display()),
                    err,
                ));
            }
        }
    }
    segment.bytes = offset;
    Ok(())
}

fn apply_record(kind: u8, body: &[u8], state: &mut ReplayState) -> Result<()> {
    match kind {
        KIND_PUBLISH => {
            let record = decode_publish(body)?;
            state.max_seq = state.max_seq.max(record.seq);
            for consumer in state.consumers.values_mut() {
                if subject::matches(&consumer.record.filter_subject, &record.subject)
                    && !consumer.acked.contains(&record.seq)
                {
                    consumer.pending.insert(record.seq);
                }
            }
            state.messages.insert(record.seq, record);
        }
        KIND_CONSUMER_UPSERT => {
            let record = decode_consumer_upsert(body)?;
            state
                .consumers
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
            let attempt = decode_delivery_attempt(body)?;
            state.max_delivery_id = state.max_delivery_id.max(attempt.delivery_id);
            if let Some(consumer) = state.consumers.get_mut(&attempt.consumer_id) {
                if !consumer.acked.contains(&attempt.seq) {
                    consumer.pending.remove(&attempt.seq);
                    consumer.in_flight.insert(attempt.seq, attempt);
                }
            }
        }
        KIND_ACK => {
            let ack = decode_ack(body)?;
            state.max_delivery_id = state.max_delivery_id.max(ack.delivery_id);
            if let Some(consumer) = state.consumers.get_mut(&ack.consumer_id) {
                consumer.acked.insert(ack.seq);
                consumer.pending.remove(&ack.seq);
                consumer.in_flight.remove(&ack.seq);
            }
        }
        _ => crate::broker_bail!("unknown WAL record kind {kind}"),
    }
    Ok(())
}

fn migrate_legacy_if_present(dir: &Path, truncations: &mut u64) -> Result<()> {
    let legacy = dir.join(WAL_FILE);
    if !legacy.exists() {
        return Ok(());
    }
    let mut state = ReplayState::default();
    replay_legacy_path(&legacy, &mut state, truncations)?;
    expire_in_flight(&mut state);
    cleanup_acked_messages(&mut state.messages, &state.consumers);

    let segment = segment_path(dir, 1);
    let tmp = tmp_segment_path(dir, 1);
    {
        let mut file = create_segment(&tmp)?;
        write_compact_state(
            &mut file,
            state.messages.into_values(),
            state.consumers.into_values(),
        )?;
        file.flush()?;
        file.sync_data()?;
    }
    std::fs::rename(&tmp, &segment)
        .with_context(|| format!("renaming migrated WAL segment {}", segment.display()))?;
    std::fs::rename(&legacy, dir.join(LEGACY_WAL_FILE))
        .with_context(|| format!("renaming legacy WAL {}", legacy.display()))?;
    fsync_dir(dir)?;
    Ok(())
}

fn replay_legacy_path(path: &Path, state: &mut ReplayState, truncations: &mut u64) -> Result<()> {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .with_context(|| format!("opening legacy WAL {}", path.display()))?;
    let mut offset = 0;
    loop {
        match read_record(&mut file) {
            Ok(Some((kind, body, bytes_read))) => {
                offset += bytes_read;
                apply_record(kind, &body, state)
                    .with_context(|| format!("replaying legacy WAL {}", path.display()))?;
            }
            Ok(None) => break,
            Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => {
                file.set_len(offset)?;
                *truncations += 1;
                break;
            }
            Err(err) => {
                return Err(BrokerError::with_source(
                    format!("corrupt legacy WAL {}", path.display()),
                    err,
                ));
            }
        }
    }
    Ok(())
}

fn expire_in_flight(state: &mut ReplayState) {
    for consumer in state.consumers.values_mut() {
        let expired: Vec<_> = consumer.in_flight.keys().copied().collect();
        for seq in expired {
            consumer.in_flight.remove(&seq);
            if !consumer.acked.contains(&seq) && state.messages.contains_key(&seq) {
                consumer.pending.insert(seq);
            }
        }
    }
}

pub(super) fn cleanup_acked_messages(
    messages: &mut HashMap<u64, PublishRecord>,
    consumers: &HashMap<String, ReplayedConsumer>,
) {
    let removable = messages
        .keys()
        .copied()
        .filter(|seq| {
            let mut interested = false;
            for consumer in consumers.values() {
                if consumer.pending.contains(seq)
                    || consumer.in_flight.contains_key(seq)
                    || consumer.acked.contains(seq)
                {
                    interested = true;
                    if !consumer.acked.contains(seq) {
                        return false;
                    }
                }
            }
            interested
        })
        .collect::<Vec<_>>();
    for seq in removable {
        messages.remove(&seq);
    }
}
