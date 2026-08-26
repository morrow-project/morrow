use super::*;
use std::{
    collections::HashSet,
    sync::Arc,
    time::{Duration, Instant},
};

#[path = "bench/stats.rs"]
pub(super) mod stats;
#[path = "bench/workloads.rs"]
mod workloads;
use stats::*;
use workloads::*;

#[derive(Debug, serde::Serialize)]
pub(super) struct BenchmarkResult {
    pub valid: bool,
    pub mode: BenchmarkMode,
    pub target: String,
    pub endpoint: String,
    pub network_mode: &'static str,
    pub configuration: ResultConfiguration,
    pub aggregate: Metrics,
    pub roles: Vec<RoleMetrics>,
    pub clients: Vec<ClientMetrics>,
    pub acknowledgement: AcknowledgementResult,
    pub environment: Environment,
}

#[derive(Debug, serde::Serialize)]
pub(super) struct ResultConfiguration {
    messages: Option<usize>,
    duration_ms: Option<u64>,
    clients: usize,
    publishers: usize,
    subscribers: usize,
    concurrency: usize,
    throughput: u64,
    payload_size: usize,
    payload_source: String,
    headers: Vec<(String, String)>,
    publish_mode: PublishMode,
    max_in_flight: usize,
    batch_size: usize,
    subjects: usize,
    subject_order: SubjectOrder,
    key_cardinality: usize,
    sleep_ms: u64,
    warmup_ms: u64,
    seed: u64,
    queue: Option<String>,
    explicit_ack: bool,
    durable_id: Option<String>,
    timeout_ms: u64,
    max_bytes: usize,
}

#[derive(Debug, serde::Serialize)]
pub(super) struct Metrics {
    operations: u64,
    bytes: u64,
    elapsed_ms: u64,
    messages_per_second: f64,
    payload_mib_per_second: f64,
    errors: u64,
    timeouts: u64,
    reconnects: u64,
    duplicates: u64,
    acknowledgements: u64,
    latency_us: LatencyDistribution,
}

#[derive(Debug, serde::Serialize)]
pub(super) struct ClientMetrics {
    client: usize,
    role: String,
    #[serde(flatten)]
    metrics: Metrics,
}

#[derive(Debug, serde::Serialize)]
pub(super) struct RoleMetrics {
    role: String,
    #[serde(flatten)]
    metrics: Metrics,
}

#[derive(Debug, serde::Serialize)]
pub(super) struct AcknowledgementResult {
    requested_level: Option<String>,
    observed_level: Option<String>,
    requested_contract_version: Option<u16>,
    observed_contract_version: Option<u16>,
}

#[derive(Debug, serde::Serialize)]
pub(super) struct Environment {
    cli_version: &'static str,
    protocol_version: u32,
    revision: Option<&'static str>,
    os: &'static str,
    architecture: &'static str,
    cpu_cores: usize,
}

pub(super) async fn run_benchmark(
    config: CliConfig,
    mode: BenchmarkMode,
    target: String,
    mut options: BenchmarkOptions,
) -> Result<BenchmarkResult> {
    let payload = match &options.payload_file {
        Some(path) => fs::read(path).map_err(|error| {
            CliError::with_source(
                format!("reading benchmark payload {}", path.display()),
                error,
            )
        })?,
        None => vec![0_u8; options.payload_size],
    };
    if payload.len() > config.max_payload {
        return Err(CliError::msg(format!(
            "benchmark payload size {} exceeds configured maximum {}",
            payload.len(),
            config.max_payload
        )));
    }
    let payload_source = options.payload_file.as_ref().map_or_else(
        || "generated".to_string(),
        |path| path.display().to_string(),
    );
    let workload = PreparedWorkload {
        target: target.clone(),
        payload: Arc::new(payload),
        options: options.clone(),
        measure_delivery: mode == BenchmarkMode::PubSub,
    };
    if options.warmup_ms > 0 {
        let mut warmup = workload.clone();
        warmup.options.messages = None;
        warmup.options.duration_ms = Some(options.warmup_ms);
        warmup.options.warmup_ms = 0;
        run_workload(config.clone(), mode, warmup).await?;
    }
    options.payload_size = workload.payload.len();
    let workers = run_workload(config.clone(), mode, workload).await?;
    build_result(config, mode, target, options, payload_source, workers)
}

fn build_result(
    config: CliConfig,
    mode: BenchmarkMode,
    target: String,
    options: BenchmarkOptions,
    payload_source: String,
    workers: Vec<WorkerStats>,
) -> Result<BenchmarkResult> {
    let elapsed = workers
        .iter()
        .filter(|worker| mode != BenchmarkMode::PubSub || worker.role == "publisher")
        .map(|worker| worker.elapsed)
        .max()
        .unwrap_or(Duration::from_millis(1));
    let aggregate_workers = workers
        .iter()
        .filter(|worker| mode != BenchmarkMode::PubSub || worker.role == "publisher")
        .cloned()
        .collect::<Vec<_>>();
    let mut all_samples = aggregate_workers
        .iter()
        .flat_map(|worker| worker.latencies_us.iter().copied())
        .collect::<Vec<_>>();
    let aggregate = metrics_for(&aggregate_workers, elapsed, distribution(&mut all_samples));
    let mut role_names = workers.iter().map(|worker| worker.role).collect::<Vec<_>>();
    role_names.sort_unstable();
    role_names.dedup();
    let mut roles = Vec::with_capacity(role_names.len());
    for role in role_names {
        let role_workers = workers
            .iter()
            .filter(|worker| worker.role == role)
            .cloned()
            .collect::<Vec<_>>();
        let role_elapsed = if mode == BenchmarkMode::PubSub {
            elapsed
        } else {
            role_workers
                .iter()
                .map(|worker| worker.elapsed)
                .max()
                .unwrap_or(elapsed)
        };
        let mut samples = role_workers
            .iter()
            .flat_map(|worker| worker.latencies_us.iter().copied())
            .collect::<Vec<_>>();
        roles.push(RoleMetrics {
            role: role.to_string(),
            metrics: metrics_for(&role_workers, role_elapsed, distribution(&mut samples)),
        });
    }
    let mut clients = Vec::with_capacity(workers.len());
    for mut worker in workers.clone() {
        let latency = distribution(&mut worker.latencies_us);
        clients.push(ClientMetrics {
            client: worker.id,
            role: worker.role.to_string(),
            metrics: metrics_for(
                std::slice::from_ref(&worker),
                if mode == BenchmarkMode::PubSub {
                    elapsed
                } else {
                    worker.elapsed
                },
                latency,
            ),
        });
    }
    let requested = options.ack_level;
    let observed = workers.iter().find_map(|worker| worker.observed_ack_level);
    let observed_contract = workers
        .iter()
        .find_map(|worker| worker.observed_ack_contract);
    if workers.iter().any(|worker| {
        worker.errors > 0 || worker.timeouts > 0 || worker.duplicates > 0 || worker.ack_mismatch
    }) {
        return Err(CliError::msg("benchmark validation failed"));
    }
    Ok(BenchmarkResult {
        valid: true,
        mode,
        target,
        endpoint: config.server.to_string(),
        network_mode: if config.server.ip().is_loopback() {
            "local"
        } else {
            "remote"
        },
        configuration: ResultConfiguration {
            messages: options.messages,
            duration_ms: options.duration_ms,
            clients: options.clients,
            publishers: options.publishers,
            subscribers: options.subscribers,
            concurrency: options.concurrency,
            throughput: options.throughput,
            payload_size: options.payload_size,
            payload_source,
            headers: options.headers,
            publish_mode: options.publish_mode,
            max_in_flight: options.max_in_flight,
            batch_size: options.batch_size,
            subjects: options.subjects,
            subject_order: options.subject_order,
            key_cardinality: options.key_cardinality,
            sleep_ms: options.sleep_ms,
            warmup_ms: options.warmup_ms,
            seed: options.seed,
            queue: options.queue,
            explicit_ack: options.ack,
            durable_id: options.durable_id,
            timeout_ms: options.timeout_ms,
            max_bytes: options.max_bytes,
        },
        aggregate,
        roles,
        clients,
        acknowledgement: AcknowledgementResult {
            requested_level: requested.map(ack_level_name),
            observed_level: observed.map(ack_level_name),
            requested_contract_version: requested
                .map(|_| client::protocol::model::ACK_CONTRACT_VERSION),
            observed_contract_version: observed_contract,
        },
        environment: Environment {
            cli_version: VERSION,
            protocol_version: 1,
            revision: option_env!("MORROW_GIT_REVISION"),
            os: std::env::consts::OS,
            architecture: std::env::consts::ARCH,
            cpu_cores: std::thread::available_parallelism().map_or(1, usize::from),
        },
    })
}

fn metrics_for(
    workers: &[WorkerStats],
    elapsed: Duration,
    latency_us: LatencyDistribution,
) -> Metrics {
    let operations = workers.iter().map(|worker| worker.operations).sum();
    let bytes = workers.iter().map(|worker| worker.bytes).sum();
    let seconds = elapsed.as_secs_f64().max(0.000_001);
    Metrics {
        operations,
        bytes,
        elapsed_ms: elapsed.as_millis().max(1) as u64,
        messages_per_second: operations as f64 / seconds,
        payload_mib_per_second: bytes as f64 / (1024.0 * 1024.0) / seconds,
        errors: workers.iter().map(|worker| worker.errors).sum(),
        timeouts: workers.iter().map(|worker| worker.timeouts).sum(),
        reconnects: workers.iter().map(|worker| worker.reconnects).sum(),
        duplicates: workers.iter().map(|worker| worker.duplicates).sum(),
        acknowledgements: workers.iter().map(|worker| worker.acknowledgements).sum(),
        latency_us,
    }
}

impl BenchmarkResult {
    pub(super) fn print_human(&self) {
        println!(
            "{} benchmark: {} ({})",
            mode_name(self.mode),
            self.endpoint,
            self.network_mode
        );
        println!("valid: {}", self.valid);
        println!(
            "aggregate: {} operations, {:.2} msg/s, {:.2} MiB/s, {} ms",
            self.aggregate.operations,
            self.aggregate.messages_per_second,
            self.aggregate.payload_mib_per_second,
            self.aggregate.elapsed_ms
        );
        print_latency("latency", &self.aggregate.latency_us);
        println!(
            "counters: errors={} timeouts={} reconnects={} duplicates={} acknowledgements={}",
            self.aggregate.errors,
            self.aggregate.timeouts,
            self.aggregate.reconnects,
            self.aggregate.duplicates,
            self.aggregate.acknowledgements
        );
        println!(
            "settings: clients={} publishers={} subscribers={} concurrency={} payload={} bytes throughput={} msg/s seed={} warmup={} ms",
            self.configuration.clients,
            self.configuration.publishers,
            self.configuration.subscribers,
            self.configuration.concurrency,
            self.configuration.payload_size,
            self.configuration.throughput,
            self.configuration.seed,
            self.configuration.warmup_ms
        );
        for role in &self.roles {
            println!(
                "role {}: {} operations, {:.2} msg/s, {:.2} MiB/s",
                role.role,
                role.metrics.operations,
                role.metrics.messages_per_second,
                role.metrics.payload_mib_per_second
            );
        }
        for client in &self.clients {
            println!(
                "client {} ({}): {} operations, {:.2} msg/s, {:.2} MiB/s",
                client.client,
                client.role,
                client.metrics.operations,
                client.metrics.messages_per_second,
                client.metrics.payload_mib_per_second
            );
        }
        println!(
            "acknowledgement: requested={} observed={} contract={}",
            self.acknowledgement
                .requested_level
                .as_deref()
                .unwrap_or("none"),
            self.acknowledgement
                .observed_level
                .as_deref()
                .unwrap_or("none"),
            self.acknowledgement
                .observed_contract_version
                .map_or_else(|| "none".to_string(), |version| version.to_string())
        );
        println!(
            "environment: cli={} protocol={} os={}/{} cores={} revision={}",
            self.environment.cli_version,
            self.environment.protocol_version,
            self.environment.os,
            self.environment.architecture,
            self.environment.cpu_cores,
            self.environment.revision.unwrap_or("unknown")
        );
    }

    pub(super) fn write_csv(&self, path: &Path) -> Result<()> {
        let mut output = String::from(
            "mode,target,endpoint,network,client,role,operations,bytes,elapsed_ms,messages_per_second,payload_mib_per_second,errors,timeouts,reconnects,duplicates,acknowledgements,min_us,mean_us,stddev_us,p50_us,p90_us,p95_us,p99_us,p99_9_us,max_us,valid,messages,duration_ms,clients,publishers,subscribers,concurrency,throughput,payload_size,payload_source,headers,publish_mode,max_in_flight,batch_size,subjects,subject_order,key_cardinality,sleep_ms,warmup_ms,seed,queue,explicit_ack,durable_id,timeout_ms,max_bytes,requested_ack_level,observed_ack_level,requested_ack_contract,observed_ack_contract,cli_version,protocol_version,revision,os,architecture,cpu_cores\n",
        );
        write_csv_row(&mut output, self, "aggregate", "aggregate", &self.aggregate);
        for role in &self.roles {
            write_csv_row(&mut output, self, "aggregate", &role.role, &role.metrics);
        }
        for client in &self.clients {
            write_csv_row(
                &mut output,
                self,
                &client.client.to_string(),
                &client.role,
                &client.metrics,
            );
        }
        fs::write(path, output).map_err(|error| {
            CliError::with_source(format!("writing benchmark CSV {}", path.display()), error)
        })
    }
}

fn write_csv_row(
    output: &mut String,
    result: &BenchmarkResult,
    client: &str,
    role: &str,
    metrics: &Metrics,
) {
    let latency = &metrics.latency_us;
    let configuration = &result.configuration;
    let values = vec![
        mode_name(result.mode).to_string(),
        result.target.clone(),
        result.endpoint.clone(),
        result.network_mode.to_string(),
        client.to_string(),
        role.to_string(),
        metrics.operations.to_string(),
        metrics.bytes.to_string(),
        metrics.elapsed_ms.to_string(),
        format!("{:.6}", metrics.messages_per_second),
        format!("{:.6}", metrics.payload_mib_per_second),
        metrics.errors.to_string(),
        metrics.timeouts.to_string(),
        metrics.reconnects.to_string(),
        metrics.duplicates.to_string(),
        metrics.acknowledgements.to_string(),
        format!("{:.3}", latency.min),
        format!("{:.3}", latency.mean),
        format!("{:.3}", latency.stddev),
        format!("{:.3}", latency.p50),
        format!("{:.3}", latency.p90),
        format!("{:.3}", latency.p95),
        format!("{:.3}", latency.p99),
        format!("{:.3}", latency.p99_9),
        format!("{:.3}", latency.max),
        result.valid.to_string(),
        configuration
            .messages
            .map_or_else(String::new, |value| value.to_string()),
        configuration
            .duration_ms
            .map_or_else(String::new, |value| value.to_string()),
        configuration.clients.to_string(),
        configuration.publishers.to_string(),
        configuration.subscribers.to_string(),
        configuration.concurrency.to_string(),
        configuration.throughput.to_string(),
        configuration.payload_size.to_string(),
        configuration.payload_source.clone(),
        configuration
            .headers
            .iter()
            .map(|(name, value)| format!("{name}:{value}"))
            .collect::<Vec<_>>()
            .join(";"),
        publish_mode_name(configuration.publish_mode).to_string(),
        configuration.max_in_flight.to_string(),
        configuration.batch_size.to_string(),
        configuration.subjects.to_string(),
        subject_order_name(configuration.subject_order).to_string(),
        configuration.key_cardinality.to_string(),
        configuration.sleep_ms.to_string(),
        configuration.warmup_ms.to_string(),
        configuration.seed.to_string(),
        configuration.queue.clone().unwrap_or_default(),
        configuration.explicit_ack.to_string(),
        configuration.durable_id.clone().unwrap_or_default(),
        configuration.timeout_ms.to_string(),
        configuration.max_bytes.to_string(),
        result
            .acknowledgement
            .requested_level
            .clone()
            .unwrap_or_default(),
        result
            .acknowledgement
            .observed_level
            .clone()
            .unwrap_or_default(),
        result
            .acknowledgement
            .requested_contract_version
            .map_or_else(String::new, |value| value.to_string()),
        result
            .acknowledgement
            .observed_contract_version
            .map_or_else(String::new, |value| value.to_string()),
        result.environment.cli_version.to_string(),
        result.environment.protocol_version.to_string(),
        result.environment.revision.unwrap_or("").to_string(),
        result.environment.os.to_string(),
        result.environment.architecture.to_string(),
        result.environment.cpu_cores.to_string(),
    ];
    output.push_str(
        &values
            .iter()
            .map(|value| csv_escape(value))
            .collect::<Vec<_>>()
            .join(","),
    );
    output.push('\n');
}

fn csv_escape(value: &str) -> String {
    if value.contains([',', '"', '\n']) {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

fn ack_level_name(level: AckLevel) -> String {
    match level {
        AckLevel::Accepted => "accepted",
        AckLevel::Durable => "durable",
        AckLevel::HighDurability => "high-durability",
        AckLevel::ClusterDurable => "cluster-durable",
    }
    .to_string()
}

fn mode_name(mode: BenchmarkMode) -> &'static str {
    match mode {
        BenchmarkMode::Pub => "pub",
        BenchmarkMode::Sub => "sub",
        BenchmarkMode::PubSub => "pubsub",
        BenchmarkMode::Request => "request",
        BenchmarkMode::Serve => "serve",
        BenchmarkMode::Consume => "consume",
        BenchmarkMode::Fetch => "fetch",
    }
}

fn publish_mode_name(mode: PublishMode) -> &'static str {
    match mode {
        PublishMode::FireAndForget => "fire-and-forget",
        PublishMode::Sync => "sync",
        PublishMode::Async => "async",
        PublishMode::Batch => "batch",
    }
}

fn subject_order_name(order: SubjectOrder) -> &'static str {
    match order {
        SubjectOrder::Sequential => "sequential",
        SubjectOrder::Random => "random",
    }
}

fn print_latency(name: &str, latency: &LatencyDistribution) {
    println!(
        "{name} (us): min={:.0} mean={:.0} stddev={:.0} p50={:.0} p90={:.0} p95={:.0} p99={:.0} p99.9={:.0} max={:.0}",
        latency.min,
        latency.mean,
        latency.stddev,
        latency.p50,
        latency.p90,
        latency.p95,
        latency.p99,
        latency.p99_9,
        latency.max
    );
}
