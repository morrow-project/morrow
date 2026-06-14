use server::{Broker, Config};

#[tokio::main]
async fn main() -> server::error::Result<()> {
    let config = Config::load_from_args()?;
    let broker = Broker::open(config)?;
    broker.serve().await
}
