use opentelemetry::{global, trace::TracerProvider};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use server::{Config, Morrow};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[tokio::main]
async fn main() -> server::error::Result<()> {
    if std::env::args_os().skip(1).any(|arg| arg == "--version") {
        println!("morrow-server {VERSION}");
        return Ok(());
    }

    let tracer_provider = init_tracing()?;

    if Config::help_requested() {
        println!("{}", Config::usage());
        return Ok(());
    }

    let config = Config::load_from_args()?;
    let broker = Morrow::open(config)?;
    let result = broker.serve().await;
    if let Some(provider) = tracer_provider {
        let _ = provider.shutdown();
    }
    result
}

fn init_tracing() -> server::error::Result<Option<opentelemetry_sdk::trace::SdkTracerProvider>> {
    global::set_text_map_propagator(TraceContextPropagator::new());
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let otlp_configured = std::env::var_os("OTEL_EXPORTER_OTLP_ENDPOINT").is_some()
        || std::env::var_os("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT").is_some();
    if !otlp_configured {
        tracing_subscriber::fmt().with_env_filter(filter).init();
        return Ok(None);
    }

    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .build()
        .map_err(|err| server::error::BrokerError::with_source("building OTLP exporter", err))?;
    let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .build();
    opentelemetry::global::set_tracer_provider(provider.clone());
    let tracer = provider.tracer("morrow-server");
    let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);
    tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer())
        .with(otel_layer)
        .init();
    Ok(Some(provider))
}
