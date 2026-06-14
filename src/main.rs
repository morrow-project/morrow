use broker::{Broker, Config};

#[tokio::main]
async fn main() -> broker::error::Result<()> {
    let config = Config::parse_args();
    let broker = Broker::open(config)?;
    broker.serve().await
}
