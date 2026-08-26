use super::*;
use std::{
    collections::HashSet,
    sync::Arc,
    time::{Duration, Instant},
};

#[path = "bench/report.rs"]
mod report;
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
    phases: PhaseMetrics,
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
    let partition_metadata = options
        .partition_metadata
        .as_ref()
        .map(|path| fs::read(path).map(Arc::new))
        .transpose()
        .map_err(|error| CliError::with_source("reading benchmark partition metadata", error))?;
    let workload = PreparedWorkload {
        target: target.clone(),
        payload: Arc::new(payload),
        options: options.clone(),
        measure_delivery: mode == BenchmarkMode::PubSub,
        partition_metadata,
    };
    let mut warmup_operations = 0;
    if options.warmup_ms > 0 {
        let mut warmup = workload.clone();
        warmup.options.messages = None;
        warmup.options.duration_ms = Some(options.warmup_ms);
        warmup.options.warmup_ms = 0;
        let warmup_workers = run_workload(config.clone(), mode, warmup).await?;
        warmup_operations = warmup_workers.iter().map(|worker| worker.operations).sum();
    }
    options.payload_size = workload.payload.len();
    let active_started = Instant::now();
    let workers = run_workload(config.clone(), mode, workload).await?;
    let wall_elapsed = active_started.elapsed();
    build_result(
        config,
        mode,
        target,
        options,
        payload_source,
        workers,
        warmup_operations,
        wall_elapsed,
    )
}

fn build_result(
    config: CliConfig,
    mode: BenchmarkMode,
    target: String,
    options: BenchmarkOptions,
    payload_source: String,
    workers: Vec<WorkerStats>,
    warmup_operations: u64,
    wall_elapsed: Duration,
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
    let measured_operations = aggregate.operations;
    let active_elapsed = options
        .duration_ms
        .map(Duration::from_millis)
        .unwrap_or(wall_elapsed);
    let drain_elapsed = wall_elapsed.saturating_sub(active_elapsed);
    let steady_state = options
        .duration_ms
        .is_some_and(|_| steady_state_eligible(active_elapsed, measured_operations));
    let phases = PhaseMetrics {
        warmup_ms: options.warmup_ms,
        active_ms: active_elapsed.as_millis().min(u128::from(u64::MAX)) as u64,
        drain_ms: drain_elapsed.as_millis().min(u128::from(u64::MAX)) as u64,
        warmup_operations,
        measured_operations,
        total_operations: warmup_operations.saturating_add(measured_operations),
        offered_messages_per_second: options.throughput as f64,
        achieved_messages_per_second: aggregate.messages_per_second,
        steady_state,
    };
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
        phases,
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

fn ack_level_name(level: AckLevel) -> String {
    match level {
        AckLevel::Accepted => "accepted",
        AckLevel::Durable => "durable",
        AckLevel::HighDurability => "high-durability",
        AckLevel::ClusterDurable => "cluster-durable",
    }
    .to_string()
}
