use crate::{SourceRecord, SourceTask};
use client::{BatchPublishRequest, Client, protocol::AckLevel};

#[derive(Debug, Clone)]
pub struct BrokerSourceConfig {
    pub max_records: usize,
    pub max_bytes: usize,
}

pub async fn run_source_batch<T: SourceTask + ?Sized>(
    client: &mut Client,
    config: &BrokerSourceConfig,
    source: &mut T,
) -> Result<usize, String> {
    if config.max_records == 0 || config.max_bytes == 0 {
        return Err("source batch limits must be positive".to_string());
    }
    let records = source.poll(config.max_records, config.max_bytes)?;
    validate_batch(&records, config)?;
    let message_ids = records
        .iter()
        .map(|record| {
            format!(
                "source-{:016x}-{}",
                stable_hash(record.source_offset.as_bytes()),
                source.generation()
            )
        })
        .collect::<Vec<_>>();
    let requests = records
        .iter()
        .zip(&message_ids)
        .map(|(record, message_id)| BatchPublishRequest {
            subject: &record.subject,
            payload: &record.payload,
            level: AckLevel::HighDurability,
            msg_id: message_id,
            key: record.key.as_deref(),
        })
        .collect::<Vec<_>>();
    let acknowledgements = client
        .publish_batch_with_qos_and_key(&requests)
        .await
        .map_err(display)?;
    for ack in acknowledgements {
        if !ack.retained || ack.offset.is_none() {
            return Err("broker did not commit source record to a durable stream".to_string());
        }
    }
    source.commit_source_offsets(
        &records
            .iter()
            .map(|record| record.source_offset.clone())
            .collect::<Vec<_>>(),
    )?;
    Ok(records.len())
}

fn validate_batch(records: &[SourceRecord], config: &BrokerSourceConfig) -> Result<(), String> {
    if records.len() > config.max_records {
        return Err("source returned more records than requested".to_string());
    }
    let bytes = records.iter().try_fold(0usize, |total, record| {
        total
            .checked_add(record.payload.len())
            .and_then(|total| total.checked_add(record.key.as_ref().map_or(0, String::len)))
            .ok_or_else(|| "source batch byte count overflowed".to_string())
    })?;
    if bytes > config.max_bytes {
        return Err("source returned more bytes than requested".to_string());
    }
    Ok(())
}

fn stable_hash(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf29ce484222325, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3)
    })
}

fn display(error: impl std::fmt::Display) -> String {
    error.to_string()
}

#[cfg(test)]
#[path = "broker_source/tests.rs"]
mod tests;
