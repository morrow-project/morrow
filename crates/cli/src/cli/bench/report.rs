use super::*;

impl BenchmarkResult {
    pub(crate) fn print_human(&self) {
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
        println!(
            "phases: warmup={} ms active={} ms drain={} ms steady_state={}",
            self.phases.warmup_ms,
            self.phases.active_ms,
            self.phases.drain_ms,
            self.phases.steady_state
        );
    }

    pub(crate) fn write_csv(&self, path: &Path) -> Result<()> {
        let mut output = String::from(
            "mode,target,endpoint,network,client,role,operations,bytes,elapsed_ms,messages_per_second,payload_mib_per_second,errors,timeouts,reconnects,duplicates,acknowledgements,min_us,mean_us,stddev_us,p50_us,p90_us,p95_us,p99_us,p99_9_us,max_us,valid,messages,duration_ms,clients,publishers,subscribers,concurrency,throughput,payload_size,payload_source,headers,publish_mode,max_in_flight,batch_size,subjects,subject_order,key_cardinality,sleep_ms,warmup_ms,seed,queue,explicit_ack,durable_id,timeout_ms,max_bytes,requested_ack_level,observed_ack_level,requested_ack_contract,observed_ack_contract,cli_version,protocol_version,revision,os,architecture,cpu_cores,warmup_phase_ms,active_phase_ms,drain_phase_ms,warmup_operations,measured_operations,total_operations,offered_messages_per_second,achieved_messages_per_second,steady_state\n",
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
            .map_or_else(String::new, |v| v.to_string()),
        configuration
            .duration_ms
            .map_or_else(String::new, |v| v.to_string()),
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
            .map(|(n, v)| format!("{n}:{v}"))
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
            .map_or_else(String::new, |v| v.to_string()),
        result
            .acknowledgement
            .observed_contract_version
            .map_or_else(String::new, |v| v.to_string()),
        result.environment.cli_version.to_string(),
        result.environment.protocol_version.to_string(),
        result.environment.revision.unwrap_or("").to_string(),
        result.environment.os.to_string(),
        result.environment.architecture.to_string(),
        result.environment.cpu_cores.to_string(),
        result.phases.warmup_ms.to_string(),
        result.phases.active_ms.to_string(),
        result.phases.drain_ms.to_string(),
        result.phases.warmup_operations.to_string(),
        result.phases.measured_operations.to_string(),
        result.phases.total_operations.to_string(),
        format!("{:.6}", result.phases.offered_messages_per_second),
        format!("{:.6}", result.phases.achieved_messages_per_second),
        result.phases.steady_state.to_string(),
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
