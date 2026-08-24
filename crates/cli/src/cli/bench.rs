use super::*;
use std::{
    collections::HashSet,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::{Barrier, Mutex};

#[derive(Debug, Clone)]
pub(super) struct PubSubOptions {
    pub subject: String,
    pub messages: Option<usize>,
    pub duration_ms: Option<u64>,
    pub payload_size: usize,
    pub publishers: usize,
    pub subscribers: usize,
    pub concurrency: usize,
    pub ack: bool,
    pub durable_id: Option<String>,
}

#[derive(Debug, serde::Serialize)]
pub(super) struct BenchmarkResult {
    endpoint: String,
    network_mode: &'static str,
    messages_published: u64,
    messages_received: u64,
    duplicates: u64,
    elapsed_ms: u64,
    payload_size: usize,
    publishers: usize,
    subscribers: usize,
    concurrency: usize,
    ack: bool,
    publish_messages_per_second: f64,
    delivery_messages_per_second: f64,
    publish_latency_us: Percentiles,
    end_to_end_latency_us: Percentiles,
}

#[derive(Debug, serde::Serialize)]
struct Percentiles {
    p50: f64,
    p95: f64,
    p99: f64,
    max: f64,
}

struct SubscriberStats {
    received: u64,
    duplicates: u64,
    latencies_us: Vec<u64>,
}

pub(super) async fn run_pubsub(
    config: CliConfig,
    options: PubSubOptions,
) -> Result<BenchmarkResult> {
    let publisher_tasks = options
        .publishers
        .checked_mul(options.concurrency)
        .ok_or_else(|| CliError::msg("publisher count and concurrency are too large"))?;
    let start = Instant::now();
    let deadline = options
        .duration_ms
        .map(|duration| start + Duration::from_millis(duration));
    let published = Arc::new(AtomicU64::new(0));
    let next_sequence = Arc::new(AtomicU64::new(0));
    let publishers_remaining = Arc::new(AtomicU64::new(publisher_tasks as u64));
    let publish_latencies = Arc::new(Mutex::new(Vec::new()));
    let start_barrier = Arc::new(Barrier::new(publisher_tasks + options.subscribers));

    let mut subscribers = Vec::with_capacity(options.subscribers);
    for index in 0..options.subscribers {
        let config = config.clone();
        let subject = options.subject.clone();
        let barrier = start_barrier.clone();
        let published = published.clone();
        let publishers_remaining = publishers_remaining.clone();
        let ack = options.ack;
        let expected = options.messages.map(|messages| messages as u64);
        let durable_id = options.durable_id.clone();
        subscribers.push(tokio::spawn(async move {
            let mut client = config.client_options_for(&format!("bench-sub-{index}"))?;
            if let Some(durable_id) = durable_id {
                client.durable_id = Some(format!("{durable_id}-{index}"));
            }
            let mut client = Client::connect_with_options(&client).await?;
            client
                .subscribe(&subject, &format!("bench-sub-{index}"))
                .await?;
            client.ping_roundtrip().await?;
            barrier.wait().await;

            let mut seen = HashSet::new();
            let mut stats = SubscriberStats {
                received: 0,
                duplicates: 0,
                latencies_us: Vec::new(),
            };
            loop {
                let target_reached = expected.is_some_and(|target| stats.received >= target)
                    || (expected.is_none()
                        && publishers_remaining.load(Ordering::Acquire) == 0
                        && stats.received >= published.load(Ordering::Acquire));
                if target_reached {
                    break;
                }
                let wait = if expected.is_some() {
                    Duration::from_secs(2)
                } else if publishers_remaining.load(Ordering::Acquire) == 0 {
                    Duration::from_secs(2)
                } else {
                    Duration::from_millis(250)
                };
                let message = tokio::time::timeout(wait, client.next_message())
                    .await
                    .map_err(|_| CliError::msg("benchmark subscriber timed out"))??;
                if message.payload.len() < 16 {
                    return Err(CliError::msg(
                        "benchmark received a short measurement payload",
                    ));
                }
                let sequence = u64::from_le_bytes(message.payload[..8].try_into().unwrap());
                let sent_ns = u64::from_le_bytes(message.payload[8..16].try_into().unwrap());
                if !seen.insert(sequence) {
                    stats.duplicates += 1;
                }
                stats.received += 1;
                let now_ns = start.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64;
                stats
                    .latencies_us
                    .push(now_ns.saturating_sub(sent_ns) / 1_000);
                if ack {
                    if let Some(ack_subject) = &message.ack_subject {
                        client.ack(ack_subject).await?;
                    }
                }
            }
            Ok::<_, CliError>(stats)
        }));
    }

    let mut publishers = Vec::with_capacity(publisher_tasks);
    for index in 0..publisher_tasks {
        let config = config.clone();
        let subject = options.subject.clone();
        let barrier = start_barrier.clone();
        let published = published.clone();
        let next_sequence = next_sequence.clone();
        let publishers_remaining = publishers_remaining.clone();
        let publish_latencies = publish_latencies.clone();
        let payload_size = options.payload_size;
        let messages = options.messages.map(|messages| messages as u64);
        let deadline = deadline;
        let ack = options.ack;
        publishers.push(tokio::spawn(async move {
            let options = config.client_options_for(&format!("bench-pub-{index}"))?;
            let mut client = Client::connect_with_options(&options).await?;
            barrier.wait().await;
            loop {
                let sequence = next_sequence.fetch_add(1, Ordering::AcqRel);
                if messages.is_some_and(|target| sequence >= target)
                    || deadline.is_some_and(|deadline| Instant::now() >= deadline)
                {
                    break;
                }
                let mut payload = vec![0_u8; payload_size];
                payload[..8].copy_from_slice(&sequence.to_le_bytes());
                let sent_ns = start.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64;
                payload[8..16].copy_from_slice(&sent_ns.to_le_bytes());
                let publish_start = Instant::now();
                if ack {
                    client
                        .publish_with_qos(
                            &subject,
                            None,
                            &payload,
                            AckLevel::Accepted,
                            &format!("bench-{index}-{sequence}"),
                        )
                        .await?;
                } else {
                    client.publish(&subject, &payload).await?;
                }
                published.fetch_add(1, Ordering::AcqRel);
                publish_latencies.lock().await.push(
                    publish_start
                        .elapsed()
                        .as_micros()
                        .min(u128::from(u64::MAX)) as u64,
                );
            }
            publishers_remaining.fetch_sub(1, Ordering::AcqRel);
            Ok::<_, CliError>(())
        }));
    }

    for publisher in publishers {
        publisher
            .await
            .map_err(|err| CliError::with_source("joining benchmark publisher", err))??;
    }
    let mut stats = Vec::with_capacity(options.subscribers);
    for subscriber in subscribers {
        stats.push(
            subscriber
                .await
                .map_err(|err| CliError::with_source("joining benchmark subscriber", err))??,
        );
    }
    let elapsed_ms = start.elapsed().as_millis().max(1) as u64;
    let messages_published = published.load(Ordering::Acquire);
    let messages_received = stats.iter().map(|stats| stats.received).sum();
    let duplicates = stats.iter().map(|stats| stats.duplicates).sum();
    let mut end_to_end = stats
        .into_iter()
        .flat_map(|stats| stats.latencies_us)
        .collect::<Vec<_>>();
    let mut publish_latencies = Arc::try_unwrap(publish_latencies)
        .map_err(|_| CliError::msg("benchmark latency collection is still in use"))?
        .into_inner();
    let expected_received = messages_published * options.subscribers as u64;
    if messages_received != expected_received || duplicates != 0 {
        return Err(CliError::msg(format!(
            "benchmark correctness failure: published {messages_published}, received {messages_received}, expected {expected_received}, duplicates {duplicates}"
        )));
    }
    Ok(BenchmarkResult {
        endpoint: config.server.to_string(),
        network_mode: if config.server.ip().is_loopback() {
            "local"
        } else {
            "remote"
        },
        messages_published,
        messages_received,
        duplicates,
        elapsed_ms,
        payload_size: options.payload_size,
        publishers: options.publishers,
        subscribers: options.subscribers,
        concurrency: options.concurrency,
        ack: options.ack,
        publish_messages_per_second: messages_published as f64 * 1_000.0 / elapsed_ms as f64,
        delivery_messages_per_second: messages_received as f64 * 1_000.0 / elapsed_ms as f64,
        publish_latency_us: percentiles(&mut publish_latencies),
        end_to_end_latency_us: percentiles(&mut end_to_end),
    })
}

impl BenchmarkResult {
    pub(super) fn print_human(&self) {
        println!("endpoint: {} ({})", self.endpoint, self.network_mode);
        println!(
            "messages: {} published, {} received, {} duplicates",
            self.messages_published, self.messages_received, self.duplicates
        );
        println!(
            "rate: {:.2} msg/s published, {:.2} msg/s delivered",
            self.publish_messages_per_second, self.delivery_messages_per_second
        );
        println!("duration: {} ms", self.elapsed_ms);
        println!(
            "settings: payload={} bytes publishers={} subscribers={} concurrency={} ack={}",
            self.payload_size, self.publishers, self.subscribers, self.concurrency, self.ack
        );
        print_percentiles("publish latency", &self.publish_latency_us);
        print_percentiles("end-to-end latency", &self.end_to_end_latency_us);
    }
}

fn percentiles(values: &mut Vec<u64>) -> Percentiles {
    values.sort_unstable();
    let value = |percentile: usize| {
        values
            .get(
                ((values.len().saturating_sub(1) * percentile) / 100)
                    .min(values.len().saturating_sub(1)),
            )
            .copied()
            .unwrap_or_default() as f64
    };
    Percentiles {
        p50: value(50),
        p95: value(95),
        p99: value(99),
        max: values.last().copied().unwrap_or_default() as f64,
    }
}

fn print_percentiles(name: &str, values: &Percentiles) {
    println!(
        "{name} (us): p50={:.0} p95={:.0} p99={:.0} max={:.0}",
        values.p50, values.p95, values.p99, values.max
    );
}
