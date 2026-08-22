use connector::{
    AppendDatabaseSink, BrokerSinkConfig, CheckpointStore, ConnectorConfig, ConnectorTarget,
    ControlRecordKind, ObjectStoreSink, SinkTask, run_sink_batch, store_control_record,
};
use std::time::Duration;

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::args_os().skip(1).any(|arg| arg == "--version") {
        println!("morrow-connector {VERSION}");
        return Ok(());
    }

    let path = std::env::args()
        .nth(1)
        .ok_or("usage: morrow-connector CONFIG.json")?;
    let config_bytes = std::fs::read(path)?;
    let config: ConnectorConfig = serde_json::from_slice(&config_bytes)?;
    let descriptor = config.descriptor_json()?;
    let (mut client, redactor) = config.connect_broker().await?;
    store_control_record(
        &mut client,
        ControlRecordKind::Config,
        &config.durable_id,
        &descriptor,
        &format!("connect-config-{}", config.generation),
    )
    .await?;
    store_control_record(
        &mut client,
        ControlRecordKind::Status,
        &config.durable_id,
        br#"{"state":"running"}"#,
        &format!("connect-status-{}-start", config.generation),
    )
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
        ConnectorTarget::ObjectStore { directory } => Box::new(ObjectStoreSink::new(
            &config.durable_id,
            config.generation,
            directory,
        )),
        ConnectorTarget::AppendDatabase { file } => Box::new(AppendDatabaseSink::open(
            &config.durable_id,
            config.generation,
            file,
        )?),
    };
    let mut attempt = 0u64;
    loop {
        attempt = attempt.saturating_add(1);
        match run_sink_batch(&mut client, &sink_config, sink.as_mut(), &mut checkpoints).await {
            Ok(0) => {}
            Ok(_) => {
                let checkpoint = std::fs::read(checkpoints.path())?;
                store_control_record(
                    &mut client,
                    ControlRecordKind::Offset,
                    &config.durable_id,
                    &checkpoint,
                    &format!("connect-offset-{}-{attempt}", config.generation),
                )
                .await?;
            }
            Err(error) => {
                let error = redactor.redact(&error);
                eprintln!("connector batch failed: {error}");
                let status = serde_json::to_vec(&serde_json::json!({
                    "state": "retrying",
                    "error": error,
                }))?;
                let _ = store_control_record(
                    &mut client,
                    ControlRecordKind::Status,
                    &config.durable_id,
                    &status,
                    &format!("connect-status-{}-{attempt}", config.generation),
                )
                .await;
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}
