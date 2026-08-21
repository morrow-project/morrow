use client::Client;
use connector::{
    AppendDatabaseSink, BrokerSinkConfig, CheckpointStore, ObjectStoreSink, SinkTask,
    run_sink_batch,
};
use serde::Deserialize;
use std::{net::SocketAddr, path::PathBuf, time::Duration};

#[derive(Deserialize)]
struct Config {
    broker: SocketAddr,
    durable_id: String,
    consumer: String,
    filter_subject: String,
    generation: u64,
    checkpoint_file: PathBuf,
    #[serde(flatten)]
    target: Target,
}

#[derive(Deserialize)]
#[serde(tag = "target", rename_all = "snake_case")]
enum Target {
    ObjectStore { directory: PathBuf },
    AppendDatabase { file: PathBuf },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args()
        .nth(1)
        .ok_or("usage: broker-connector CONFIG.json")?;
    let config: Config = serde_json::from_slice(&std::fs::read(path)?)?;
    let mut client = Client::connect(config.broker, 1024 * 1024).await?;
    client.read_info().await?;
    client
        .connect_durable(&config.durable_id, false, 30_000, 256)
        .await?;
    let _ = client
        .create_consumer(
            &config.consumer,
            &config.filter_subject,
            client::protocol::StartPosition::Committed,
        )
        .await;
    let mut checkpoints = CheckpointStore::open(&config.checkpoint_file, config.generation)?;
    let sink_config = BrokerSinkConfig {
        consumer: config.consumer,
        max_messages: 128,
        max_bytes: 8 * 1024 * 1024,
        max_wait: Duration::from_secs(1),
    };
    let mut sink: Box<dyn SinkTask> = match config.target {
        Target::ObjectStore { directory } => Box::new(ObjectStoreSink::new(
            &config.durable_id,
            config.generation,
            directory,
        )),
        Target::AppendDatabase { file } => Box::new(AppendDatabaseSink::open(
            &config.durable_id,
            config.generation,
            file,
        )?),
    };
    loop {
        if let Err(error) =
            run_sink_batch(&mut client, &sink_config, sink.as_mut(), &mut checkpoints).await
        {
            eprintln!("connector batch failed: {error}");
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}
