use super::*;

pub(super) fn publish_body(record: &PublishRecord) -> Result<Vec<u8>> {
    let mut body = Vec::new();
    put_u64(&mut body, record.seq);
    put_string(&mut body, &record.subject)?;
    put_option_string(&mut body, record.reply_to.as_deref())?;
    put_bytes(&mut body, &record.payload)?;
    put_option_string(&mut body, record.stream.as_deref())?;
    Ok(body)
}
pub(super) fn consumer_upsert_body(record: &ConsumerRecord) -> Result<Vec<u8>> {
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
    put_bytes(
        &mut body,
        &serde_json::to_vec(&record.start_position).context("encoding consumer start position")?,
    )?;
    put_bytes(
        &mut body,
        &serde_json::to_vec(&record.retry_policy).context("encoding consumer retry policy")?,
    )?;
    Ok(body)
}
pub(super) fn delivery_attempt_body(record: &DeliveryAttemptRecord) -> Result<Vec<u8>> {
    let mut body = Vec::new();
    put_u64(&mut body, record.seq);
    put_string(&mut body, &record.consumer_id)?;
    put_u64(&mut body, record.delivery_id);
    put_u64(&mut body, record.deadline_ms);
    put_u32(&mut body, record.attempt);
    Ok(body)
}
pub(super) fn ack_body(record: &AckRecord) -> Result<Vec<u8>> {
    let mut body = Vec::new();
    put_u64(&mut body, record.seq);
    put_string(&mut body, &record.consumer_id)?;
    put_u64(&mut body, record.delivery_id);
    Ok(body)
}
pub(super) fn dead_letter_body(record: &DeadLetterRecord) -> Result<Vec<u8>> {
    serde_json::to_vec(record).context("encoding dead-letter record")
}
pub(super) fn partition_append_body(record: &PartitionAppendRecord) -> Result<Vec<u8>> {
    let mut body = Vec::new();
    put_u64(&mut body, record.seq);
    put_string(&mut body, &record.stream)?;
    put_u32(&mut body, record.partition);
    put_u64(&mut body, record.offset);
    put_string(&mut body, &record.subject)?;
    Ok(body)
}
pub(super) fn consumer_cursor_body(record: &ConsumerCursorRecord) -> Result<Vec<u8>> {
    let mut body = Vec::new();
    put_string(&mut body, &record.consumer_id)?;
    put_bytes(
        &mut body,
        &serde_json::to_vec(&record.cursors).context("encoding consumer cursors")?,
    )?;
    Ok(body)
}
pub(super) fn consumer_delete_body(record: &ConsumerDeleteRecord) -> Result<Vec<u8>> {
    let mut body = Vec::new();
    put_string(&mut body, &record.consumer_id)?;
    Ok(body)
}
pub(super) fn record_size(body: &[u8]) -> Result<u64> {
    let len = body.len() + 1;
    let len: u32 = len.try_into().context("WAL record too large")?;
    Ok(4 + u64::from(len) + 4)
}
pub(super) fn write_record_to(file: &mut File, kind: u8, body: &[u8]) -> Result<()> {
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
pub(super) fn read_record(file: &mut File) -> io::Result<Option<(u8, Vec<u8>, u64)>> {
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
pub(super) fn decode_publish(body: &[u8]) -> Result<PublishRecord> {
    let mut cursor = Cursor {
        bytes: body,
        pos: 0,
    };
    let seq = cursor.u64()?;
    let subject = cursor.string()?;
    let reply_to = cursor.option_string()?;
    let payload = cursor.bytes()?;
    let stream = if cursor.is_finished() {
        None
    } else {
        cursor.option_string()?
    };
    let record = PublishRecord {
        seq,
        namespace: crate::partition_log::DEFAULT_NAMESPACE.to_string(),
        stream,
        partition: None,
        offset: None,
        subject,
        key: None,
        headers: Vec::new(),
        timestamp_ms: 0,
        reply_to,
        payload,
        partitioning_epoch: 0,
        leader_epoch: 0,
    };
    cursor.finish()?;
    Ok(record)
}
pub(super) fn decode_partition_append(body: &[u8]) -> Result<PartitionAppendRecord> {
    let mut cursor = Cursor {
        bytes: body,
        pos: 0,
    };
    let record = PartitionAppendRecord {
        seq: cursor.u64()?,
        stream: cursor.string()?,
        partition: cursor.u32()?,
        offset: cursor.u64()?,
        subject: cursor.string()?,
    };
    cursor.finish()?;
    Ok(record)
}
pub(super) fn decode_consumer_cursor(body: &[u8]) -> Result<ConsumerCursorRecord> {
    let mut cursor = Cursor {
        bytes: body,
        pos: 0,
    };
    let consumer_id = cursor.string()?;
    let cursors = serde_json::from_slice(&cursor.bytes()?).context("decoding consumer cursors")?;
    cursor.finish()?;
    Ok(ConsumerCursorRecord {
        consumer_id,
        cursors,
    })
}
pub(super) fn decode_consumer_delete(body: &[u8]) -> Result<ConsumerDeleteRecord> {
    let mut cursor = Cursor {
        bytes: body,
        pos: 0,
    };
    let consumer_id = cursor.string()?;
    cursor.finish()?;
    Ok(ConsumerDeleteRecord { consumer_id })
}
pub(super) fn decode_consumer_upsert(body: &[u8]) -> Result<ConsumerRecord> {
    let mut cursor = Cursor {
        bytes: body,
        pos: 0,
    };
    let consumer_id = cursor.string()?;
    let filter_subject = cursor.string()?;
    let queue_group = cursor.option_string()?;
    let ack_timeout_ms = cursor.u64()?;
    let max_in_flight = cursor.u32()? as usize;
    let start_position = if cursor.is_finished() {
        protocol::StartPosition::Latest
    } else {
        serde_json::from_slice(&cursor.bytes()?).context("decoding consumer start position")?
    };
    let retry_policy = if cursor.is_finished() {
        protocol::RetryPolicy::default()
    } else {
        serde_json::from_slice(&cursor.bytes()?).context("decoding consumer retry policy")?
    };
    let record = ConsumerRecord {
        consumer_id,
        filter_subject,
        queue_group,
        ack_timeout_ms,
        max_in_flight,
        start_position,
        retry_policy,
    };
    cursor.finish()?;
    Ok(record)
}
pub(super) fn decode_delivery_attempt(body: &[u8]) -> Result<DeliveryAttemptRecord> {
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
pub(super) fn decode_ack(body: &[u8]) -> Result<AckRecord> {
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
pub(super) fn decode_dead_letter(body: &[u8]) -> Result<DeadLetterRecord> {
    serde_json::from_slice(body).context("decoding dead-letter record")
}
pub(super) fn put_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}
pub(super) fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}
pub(super) fn put_string(out: &mut Vec<u8>, value: &str) -> Result<()> {
    put_bytes(out, value.as_bytes())
}
pub(super) fn put_option_string(out: &mut Vec<u8>, value: Option<&str>) -> Result<()> {
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
pub(super) fn put_bytes(out: &mut Vec<u8>, value: &[u8]) -> Result<()> {
    put_u32(out, value.len().try_into().context("field too large")?);
    out.extend_from_slice(value);
    Ok(())
}
