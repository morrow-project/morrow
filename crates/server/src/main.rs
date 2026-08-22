use server::{Config, Morrow};
use tracing_subscriber::EnvFilter;

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[tokio::main]
async fn main() -> server::error::Result<()> {
    if std::env::args_os().skip(1).any(|arg| arg == "--version") {
        println!("morrow-server {VERSION}");
        return Ok(());
    }

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
