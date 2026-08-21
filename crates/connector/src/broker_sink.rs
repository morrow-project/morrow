use crate::{CheckpointStore, ConnectorBatch, ConnectorRecord, SinkTask};
use client::Client;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct BrokerSinkConfig {
    pub consumer: String,
    pub max_messages: usize,
    pub max_bytes: usize,
    pub max_wait: Duration,
}

pub async fn run_sink_batch<T: SinkTask + ?Sized>(
    client: &mut Client,
    config: &BrokerSinkConfig,
    sink: &mut T,
    checkpoints: &mut CheckpointStore,
) -> Result<usize, String> {
    let deliveries = client
        .fetch(
            &config.consumer,
            config.max_messages,
            config.max_bytes,
            config.max_wait,
        )
        .await
        .map_err(display)?;
    if deliveries.is_empty() {
        return Ok(0);
    }
    let records = deliveries
        .iter()
        .map(|delivery| ConnectorRecord {
            stream: delivery.stream.clone(),
            partition: delivery.partition,
            offset: delivery.offset,
            subject: delivery.subject.clone(),
            key: delivery.key.clone(),
            payload: delivery.payload.clone(),
            schema_id: delivery
                .headers
                .iter()
                .find(|(name, _)| name.eq_ignore_ascii_case("schema-id"))
                .map(|(_, value)| value.clone()),
        })
        .collect::<Vec<_>>();
    let generation = sink.generation();
    let completion = sink.write_batch(&ConnectorBatch {
        generation,
        records,
    })?;
    checkpoints.commit(generation, &completion.offsets)?;
    for delivery in &deliveries {
        client.ack_delivery(delivery).await.map_err(display)?;
    }
    Ok(deliveries.len())
}

fn display(error: impl std::fmt::Display) -> String {
    error.to_string()
}
