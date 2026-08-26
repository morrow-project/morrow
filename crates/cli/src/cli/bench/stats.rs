use super::*;

pub(super) const MAX_SAMPLES_PER_CLIENT: usize = 10_000;

#[derive(Debug, Clone, Default)]
pub(super) struct WorkerStats {
    pub id: usize,
    pub role: &'static str,
    pub operations: u64,
    pub bytes: u64,
    pub errors: u64,
    pub timeouts: u64,
    pub reconnects: u64,
    pub duplicates: u64,
    pub acknowledgements: u64,
    pub elapsed: Duration,
    pub latencies_us: Vec<u64>,
    pub measurement_ids: Vec<u64>,
    pub observed_ack_level: Option<AckLevel>,
    pub observed_ack_contract: Option<u16>,
    pub ack_mismatch: bool,
}

impl WorkerStats {
    pub fn sample(&mut self, elapsed: Duration) {
        if self.latencies_us.len() < MAX_SAMPLES_PER_CLIENT {
            self.latencies_us
                .push(elapsed.as_micros().min(u128::from(u64::MAX)) as u64);
        }
    }

    pub fn observe_ack(&mut self, ack: &client::ProducerAck) {
        self.acknowledgements += 1;
        if self
            .observed_ack_level
            .is_some_and(|level| level != ack.level)
        {
            self.ack_mismatch = true;
        }
        self.observed_ack_level.get_or_insert(ack.level);
        if self
            .observed_ack_contract
            .is_some_and(|version| Some(version) != ack.ack_contract_version)
        {
            self.ack_mismatch = true;
        }
        self.observed_ack_contract = ack.ack_contract_version;
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub(crate) struct LatencyDistribution {
    pub samples: usize,
    pub min: f64,
    pub mean: f64,
    pub stddev: f64,
    pub p50: f64,
    pub p90: f64,
    pub p95: f64,
    pub p99: f64,
    pub p99_9: f64,
    pub max: f64,
}

pub(crate) fn distribution(values: &mut [u64]) -> LatencyDistribution {
    values.sort_unstable();
    let samples = values.len();
    let mean = if samples == 0 {
        0.0
    } else {
        values.iter().map(|value| *value as f64).sum::<f64>() / samples as f64
    };
    let stddev = if samples == 0 {
        0.0
    } else {
        (values
            .iter()
            .map(|value| {
                let delta = *value as f64 - mean;
                delta * delta
            })
            .sum::<f64>()
            / samples as f64)
            .sqrt()
    };
    let percentile = |numerator: usize, denominator: usize| {
        if values.is_empty() {
            return 0.0;
        }
        let index = (values.len().saturating_sub(1) * numerator).div_ceil(denominator);
        values[index.min(values.len() - 1)] as f64
    };
    LatencyDistribution {
        samples,
        min: values.first().copied().unwrap_or_default() as f64,
        mean,
        stddev,
        p50: percentile(50, 100),
        p90: percentile(90, 100),
        p95: percentile(95, 100),
        p99: percentile(99, 100),
        p99_9: percentile(999, 1000),
        max: values.last().copied().unwrap_or_default() as f64,
    }
}

pub(crate) fn work_share(total: usize, workers: usize, index: usize) -> (usize, usize) {
    let base = total / workers;
    let extra = total % workers;
    let count = base + usize::from(index < extra);
    let offset = index * base + index.min(extra);
    (offset, count)
}

pub(crate) fn deterministic_index(sequence: u64, cardinality: usize, seed: u64) -> usize {
    if cardinality <= 1 {
        return 0;
    }
    let mut value = sequence ^ seed;
    value ^= value >> 12;
    value ^= value << 25;
    value ^= value >> 27;
    (value.wrapping_mul(0x2545_f491_4f6c_dd1d) % cardinality as u64) as usize
}

pub(crate) fn pacing_delay(
    local_operation: usize,
    workers: usize,
    throughput: u64,
    sleep_ms: u64,
) -> Duration {
    let throughput_delay = if throughput == 0 {
        Duration::ZERO
    } else {
        Duration::from_nanos(
            ((local_operation as u128 * workers as u128 * 1_000_000_000) / throughput as u128)
                .min(u128::from(u64::MAX)) as u64,
        )
    };
    let sleep_delay = Duration::from_millis(sleep_ms.saturating_mul(local_operation as u64));
    throughput_delay.max(sleep_delay)
}

pub(crate) fn measurement_elapsed(options: &BenchmarkOptions, start: Instant) -> Duration {
    options
        .duration_ms
        .map(Duration::from_millis)
        .unwrap_or_else(|| start.elapsed())
}

pub(super) fn measurement_headers(
    workload: &PreparedWorkload,
    sequence: usize,
    measured_start: Instant,
) -> Vec<(String, String)> {
    if !workload.measure_delivery {
        return workload.options.headers.clone();
    }
    let mut headers = Vec::with_capacity(workload.options.headers.len() + 2);
    headers.push(("Bench-Sequence".to_string(), sequence.to_string()));
    headers.push((
        "Bench-Sent-Us".to_string(),
        measured_start.elapsed().as_micros().to_string(),
    ));
    headers.extend(workload.options.headers.iter().cloned());
    headers
}

pub(super) fn header_u64(headers: &[(String, String)], name: &str) -> Option<u64> {
    headers
        .iter()
        .find(|(header, _)| header.eq_ignore_ascii_case(name))
        .and_then(|(_, value)| value.parse().ok())
}

pub(super) fn validate_acknowledgements(
    options: &BenchmarkOptions,
    stats: &WorkerStats,
) -> Result<()> {
    let Some(requested) = options.ack_level else {
        return Ok(());
    };
    if stats.acknowledgements != stats.operations {
        return Err(CliError::msg(
            "producer acknowledgement count does not match published operations",
        ));
    }
    if stats.observed_ack_level != Some(requested) || stats.ack_mismatch {
        return Err(CliError::msg(
            "producer acknowledgement level did not match the requested level",
        ));
    }
    if stats.observed_ack_contract != Some(client::protocol::model::ACK_CONTRACT_VERSION) {
        return Err(CliError::msg(
            "producer acknowledgement contract version was missing or mismatched",
        ));
    }
    Ok(())
}
