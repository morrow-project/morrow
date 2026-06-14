#[tokio::main]
async fn main() {
    if let Err(err) = cli::run(std::env::args()).await {
        eprintln!("{err}");
        std::process::exit(1);
    }
}
