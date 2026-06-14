use super::*;

pub(super) fn replay_path(path: &Path) -> Result<Replay> {
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
    cleanup_acked_messages(&mut messages, &consumers);

    Ok(Replay {
        messages,
        consumers,
        next_seq: max_seq + 1,
        next_delivery_id: max_delivery_id + 1,
    })
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
