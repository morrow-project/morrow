mod adapters;
mod broker_sink;
mod checkpoint;
mod runtime;
mod spi;

pub use adapters::{AppendDatabaseSink, ObjectStoreSink};
pub use broker_sink::{
    BrokerSinkConfig, CONFIG_SUBJECT, OFFSET_SUBJECT, SCHEMA_SUBJECT, STATUS_SUBJECT,
    run_sink_batch,
};
pub use checkpoint::CheckpointStore;
pub use runtime::ConnectorWorker;
pub use spi::*;

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
