use super::*;
use crate::error::{BrokerError, ResultExt};
use std::io::{self, Read, Write};

pub(super) const SEGMENT_HEADER: &[u8] = b"BROKERLOG\x01\n";
pub(super) const SEGMENT_HEADER_LEN: u64 = SEGMENT_HEADER.len() as u64;
pub(super) const BATCH_PREFIX_LEN: u64 = 8;
const MAX_BATCH_BYTES: usize = 64 * 1024 * 1024;

pub(super) struct EncodedBatch {
    pub(super) bytes: Vec<u8>,
    pub(super) len: u64,
}

pub(super) fn encode_batch(envelope: &MessageEnvelope) -> Result<Vec<u8>> {
    Ok(encode_batch_with_len(envelope)?.bytes)
}

pub(super) fn encode_batch_with_len(envelope: &MessageEnvelope) -> Result<EncodedBatch> {
    let body = serde_json::to_vec(envelope).context("encoding partition-log envelope")?;
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

pub(super) fn envelope_checksum(envelope: &MessageEnvelope) -> Result<u32> {
    Ok(crc32fast::hash(
        &serde_json::to_vec(envelope).context("encoding partition-log envelope")?,
    ))
}

pub(super) fn read_batch<R: Read>(file: &mut R) -> io::Result<Option<(MessageEnvelope, u64)>> {
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
    let envelope = serde_json::from_slice(&body).map_err(|err| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid partition-log envelope: {err}"),
        )
    })?;
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
