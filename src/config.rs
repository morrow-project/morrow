use std::{net::SocketAddr, path::PathBuf, time::Duration};

use clap::Parser;

use crate::error::{Result, ResultExt};

#[derive(Debug, Clone, Parser)]
#[command(author, version, about = "Single-node WAL-backed NATS-core broker")]
pub struct Config {
    #[arg(long, default_value = "127.0.0.1:4222")]
    pub listen: SocketAddr,

    #[arg(long, default_value = "./broker-wal")]
    pub wal_dir: PathBuf,

    #[arg(long, default_value_t = 5)]
    pub fsync_interval_ms: u64,

    #[arg(long, default_value_t = 1_048_576)]
    pub max_payload: usize,

    #[arg(long, default_value_t = false)]
    pub verbose: bool,
}

impl Config {
    pub fn parse_args() -> Self {
        Self::parse()
    }

    pub fn fsync_interval(&self) -> Duration {
        Duration::from_millis(self.fsync_interval_ms)
    }

    pub fn validate(&self) -> Result<()> {
        crate::broker_ensure!(
            self.max_payload > 0,
            "--max-payload must be greater than zero"
        );
        crate::broker_ensure!(
            self.fsync_interval_ms > 0,
            "--fsync-interval-ms must be greater than zero"
        );
        std::fs::create_dir_all(&self.wal_dir)
            .with_context(|| format!("creating WAL directory {}", self.wal_dir.display()))?;
        Ok(())
    }
}
