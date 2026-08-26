use super::*;
use crate::error::{BrokerError, ResultExt};
use std::io::{self, Read, Write};

pub(super) const SEGMENT_HEADER: &[u8] = b"BROKERLOG\x01\n";
pub(super) const SEGMENT_HEADER_LEN: u64 = SEGMENT_HEADER.len() as u64;
pub(super) const BATCH_PREFIX_LEN: u64 = 8;
pub(super) const ENCRYPTED_BODY_MAGIC: &[u8] = b"MORROW-PLOG-ENC1\n";
/// Versioned on-disk envelope body. The outer length and CRC remain unchanged
/// so old segments stay readable while new records carry byte strings directly.
const BINARY_BODY_MAGIC: &[u8] = b"MORROW-PLOG-BIN1\n";
const MAX_BATCH_BYTES: usize = 64 * 1024 * 1024;

pub(super) struct EncodedBatch {
    pub(super) bytes: Vec<u8>,
    pub(super) len: u64,
}

pub(super) fn encode_batch(envelope: &MessageEnvelope) -> Result<Vec<u8>> {
    Ok(encode_batch_with_len(envelope)?.bytes)
}

pub(super) fn encode_batch_with_len(envelope: &MessageEnvelope) -> Result<EncodedBatch> {
    let body = encode_binary_envelope(envelope)?;
    crate::broker_ensure!(
        body.len() <= u32::MAX as usize,
        "partition-log envelope is too large"
    );
    let mut batch = Vec::with_capacity(BATCH_PREFIX_LEN as usize + body.len());
    batch.extend_from_slice(&(body.len() as u32).to_le_bytes());
    batch.extend_from_slice(&crc32fast::hash(&body).to_le_bytes());
    batch.extend_from_slice(&body);
    Ok(EncodedBatch {
        len: batch.len() as u64,
        bytes: batch,
    })
}

pub(super) fn encode_encrypted_batch_with_len(
    envelope: &MessageEnvelope,
    encryption: &std::sync::Arc<crate::encryption::KeyRing>,
) -> Result<EncodedBatch> {
    let body = encode_binary_envelope(envelope)?;
    let encrypted = encryption.encrypt(&body, b"partition-log")?;
    let mut protected = ENCRYPTED_BODY_MAGIC.to_vec();
    protected.extend(
        serde_json::to_vec(&encrypted)
            .map_err(|error| crate::error::BrokerError::msg(error.to_string()))?,
    );
    crate::broker_ensure!(
        protected.len() <= u32::MAX as usize,
        "partition-log envelope is too large"
    );
    let mut batch = Vec::with_capacity(BATCH_PREFIX_LEN as usize + protected.len());
    batch.extend_from_slice(&(protected.len() as u32).to_le_bytes());
    batch.extend_from_slice(&crc32fast::hash(&protected).to_le_bytes());
    batch.extend_from_slice(&protected);
    Ok(EncodedBatch {
        len: batch.len() as u64,
        bytes: batch,
    })
}

pub(super) fn envelope_checksum(envelope: &MessageEnvelope) -> Result<u32> {
    Ok(crc32fast::hash(&encode_binary_envelope(envelope)?))
}

fn encode_binary_envelope(envelope: &MessageEnvelope) -> Result<Vec<u8>> {
    let mut out = BINARY_BODY_MAGIC.to_vec();
    put_string(&mut out, &envelope.namespace)?;
    put_string(&mut out, envelope.stream.as_str())?;
    out.extend_from_slice(&envelope.partition.0.to_le_bytes());
    out.extend_from_slice(&envelope.offset.to_le_bytes());
    put_string(&mut out, &envelope.subject)?;
    put_optional_bytes(&mut out, envelope.key.as_deref())?;
    crate::broker_ensure!(
        envelope.headers.len() <= u32::MAX as usize,
        "too many headers"
    );
    out.extend_from_slice(&(envelope.headers.len() as u32).to_le_bytes());
    for header in &envelope.headers {
        put_string(&mut out, &header.name)?;
        put_string(&mut out, &header.value)?;
    }
    out.extend_from_slice(&envelope.timestamp_ms.to_le_bytes());
    put_optional_string(&mut out, envelope.reply_to.as_deref())?;
    match envelope.schema_id {
        Some(schema_id) => {
            out.push(1);
            out.extend_from_slice(&schema_id.to_le_bytes());
        }
        None => out.push(0),
    }
    put_bytes(&mut out, &envelope.payload)?;
    out.extend_from_slice(&envelope.partitioning_epoch.to_le_bytes());
    out.extend_from_slice(&envelope.leader_epoch.to_le_bytes());
    out.extend_from_slice(&envelope.legacy_seq.to_le_bytes());
    Ok(out)
}

fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) -> Result<()> {
    crate::broker_ensure!(
        bytes.len() <= u32::MAX as usize,
        "partition-log field is too large"
    );
    out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    out.extend_from_slice(bytes);
    Ok(())
}

fn put_string(out: &mut Vec<u8>, value: &str) -> Result<()> {
    put_bytes(out, value.as_bytes())
}

fn put_optional_bytes(out: &mut Vec<u8>, value: Option<&[u8]>) -> Result<()> {
    match value {
        Some(value) => {
            out.push(1);
            put_bytes(out, value)
        }
        None => {
            out.push(0);
            Ok(())
        }
    }
}

fn put_optional_string(out: &mut Vec<u8>, value: Option<&str>) -> Result<()> {
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

struct BinaryReader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> BinaryReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn take(&mut self, len: usize) -> io::Result<&'a [u8]> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or_else(|| invalid("field length overflow"))?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| invalid("truncated binary envelope"))?;
        self.offset = end;
        Ok(value)
    }

    fn u8(&mut self) -> io::Result<u8> {
        Ok(self.take(1)?[0])
    }
    fn u32(&mut self) -> io::Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> io::Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn bytes(&mut self) -> io::Result<Vec<u8>> {
        let len = self.u32()? as usize;
        Ok(self.take(len)?.to_vec())
    }
    fn string(&mut self) -> io::Result<String> {
        String::from_utf8(self.bytes()?).map_err(|_| invalid("binary envelope string is not UTF-8"))
    }
    fn optional_bytes(&mut self) -> io::Result<Option<Vec<u8>>> {
        Ok((self.u8()? == 1).then(|| self.bytes()).transpose()?)
    }
    fn optional_string(&mut self) -> io::Result<Option<String>> {
        Ok((self.u8()? == 1).then(|| self.string()).transpose()?)
    }
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn decode_binary_envelope(body: &[u8]) -> io::Result<MessageEnvelope> {
    let mut reader = BinaryReader::new(&body[BINARY_BODY_MAGIC.len()..]);
    let namespace = reader.string()?;
    let stream =
        crate::stream::StreamId::new(reader.string()?).map_err(|err| invalid(&err.to_string()))?;
    let partition = crate::stream::PartitionId(reader.u32()?);
    let offset = reader.u64()?;
    let subject = reader.string()?;
    let key = reader.optional_bytes()?;
    let header_count = reader.u32()? as usize;
    let mut headers = Vec::with_capacity(header_count);
    for _ in 0..header_count {
        headers.push(crate::partition_log::MessageHeader {
            name: reader.string()?,
            value: reader.string()?,
        });
    }
    let timestamp_ms = reader.u64()?;
    let reply_to = reader.optional_string()?;
    let schema_id = (reader.u8()? == 1).then(|| reader.u64()).transpose()?;
    let payload = reader.bytes()?;
    let partitioning_epoch = reader.u64()?;
    let leader_epoch = reader.u64()?;
    let legacy_seq = reader.u64()?;
    if reader.offset != reader.bytes.len() {
        return Err(invalid("trailing bytes in binary envelope"));
    }
    Ok(MessageEnvelope {
        namespace,
        stream,
        partition,
        offset,
        subject,
        key,
        headers,
        timestamp_ms,
        reply_to,
        schema_id,
        payload,
        partitioning_epoch,
        leader_epoch,
        legacy_seq,
    })
}

pub(super) fn read_batch<R: Read>(file: &mut R) -> io::Result<Option<(MessageEnvelope, u64)>> {
    read_batch_with_key(file, None)
}

pub(super) fn read_batch_with_key<R: Read>(
    file: &mut R,
    encryption: Option<&std::sync::Arc<crate::encryption::KeyRing>>,
) -> io::Result<Option<(MessageEnvelope, u64)>> {
    let mut length = [0; 4];
    let read = file.read(&mut length)?;
    if read == 0 {
        return Ok(None);
    }
    if read != length.len() {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "torn batch length",
        ));
    }
    let body_len = u32::from_le_bytes(length) as usize;
    if body_len > MAX_BATCH_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "partition-log batch length exceeds limit",
        ));
    }
    let mut expected_crc = [0; 4];
    file.read_exact(&mut expected_crc)?;
    let mut body = vec![0; body_len];
    file.read_exact(&mut body)?;
    if crc32fast::hash(&body) != u32::from_le_bytes(expected_crc) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "partition-log batch checksum mismatch",
        ));
    }
    let body = if body.starts_with(ENCRYPTED_BODY_MAGIC) {
        let encryption = encryption.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "encrypted partition log requires key",
            )
        })?;
        let envelope: crate::encryption::EncryptedBlob =
            serde_json::from_slice(&body[ENCRYPTED_BODY_MAGIC.len()..])
                .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err.to_string()))?;
        encryption
            .decrypt(&envelope, b"partition-log")
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err.to_string()))?
    } else {
        body
    };
    let envelope = if body.starts_with(BINARY_BODY_MAGIC) {
        decode_binary_envelope(&body)?
    } else {
        serde_json::from_slice(&body).map_err(|err| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid partition-log envelope: {err}"),
            )
        })?
    };
    Ok(Some((envelope, BATCH_PREFIX_LEN + body_len as u64)))
}

pub(super) fn create_segment(path: &Path) -> Result<std::fs::File> {
    let mut file = std::fs::OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .append(true)
        .open(path)
        .with_context(|| format!("creating partition-log segment {}", path.display()))?;
    file.write_all(SEGMENT_HEADER)?;
    Ok(file)
}

pub(super) fn validate_segment_header(file: &mut std::fs::File, path: &Path) -> Result<()> {
    let mut header = vec![0; SEGMENT_HEADER.len()];
    file.read_exact(&mut header)
        .with_context(|| format!("reading partition-log header {}", path.display()))?;
    if header != SEGMENT_HEADER {
        return Err(BrokerError::msg(format!(
            "unsupported partition-log format in {}",
            path.display()
        )));
    }
    Ok(())
}
