use broker::{Broker, Config};

#[tokio::main]
async fn main() -> broker::error::Result<()> {
    let config = Config::load_from_args()?;
    let broker = Broker::open(config)?;
    broker.serve().await
}
