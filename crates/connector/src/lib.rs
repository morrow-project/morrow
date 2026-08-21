mod adapters;
mod broker_sink;
mod broker_source;
mod checkpoint;
mod control;
mod runtime;
mod spi;

pub use adapters::{AppendDatabaseSink, ObjectStoreSink};
pub use broker_sink::{BrokerSinkConfig, run_sink_batch};
pub use broker_source::{BrokerSourceConfig, run_source_batch};
pub use checkpoint::CheckpointStore;
pub use client::protocol::connector_control::{
    CONFIG_SUBJECT, CONTROL_PLANE_VERSION, OFFSET_SUBJECT, SCHEMA_SUBJECT, STATUS_SUBJECT,
};
pub use control::{ControlRecordKind, store_control_record};
pub use runtime::ConnectorWorker;
pub use spi::*;

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
