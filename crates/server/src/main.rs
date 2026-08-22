use server::{Config, Morrow};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> server::error::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    if Config::help_requested() {
        println!("{}", Config::usage());
        return Ok(());
    }

    let config = Config::load_from_args()?;
    let broker = Morrow::open(config)?;
    broker.serve().await
}
