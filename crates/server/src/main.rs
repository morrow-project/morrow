use broker_server::{Broker, Config};

#[tokio::main]
async fn main() -> broker_server::error::Result<()> {
    let config = Config::load_from_args()?;
    let broker = Broker::open(config)?;
    broker.serve().await
}
