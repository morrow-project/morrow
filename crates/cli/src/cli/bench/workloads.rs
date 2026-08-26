use super::*;
use client::BatchPublishRequestWithHeaders;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tokio::sync::{mpsc, watch};

#[path = "workloads/services.rs"]
mod services;
use services::*;

#[derive(Clone)]
pub(super) struct PreparedWorkload {
    pub target: String,
    pub payload: Arc<Vec<u8>>,
    pub options: BenchmarkOptions,
    pub measure_delivery: bool,
}

pub(super) async fn run_workload(
    config: CliConfig,
    mode: BenchmarkMode,
    workload: PreparedWorkload,
) -> Result<Vec<WorkerStats>> {
    match mode {
        BenchmarkMode::Pub => run_publish(config, workload).await,
        BenchmarkMode::Sub => run_subscribe(config, workload).await,
        BenchmarkMode::PubSub => run_pubsub(config, workload).await,
        BenchmarkMode::Request => run_request(config, workload).await,
        BenchmarkMode::Serve => run_serve(config, workload).await,
        BenchmarkMode::Consume => run_fetch(config, workload, false).await,
        BenchmarkMode::Fetch => run_fetch(config, workload, true).await,
    }
}

async fn run_publish(config: CliConfig, workload: PreparedWorkload) -> Result<Vec<WorkerStats>> {
    let workers = workload.options.clients;
    let (ready_tx, mut ready_rx) = mpsc::channel(workers);
    let (start_tx, start_rx) = watch::channel(None::<Instant>);
    let mut tasks = Vec::with_capacity(workers);
    for index in 0..workers {
        let config = config.clone();
        let workload = workload.clone();
        let ready = ready_tx.clone();
        let start = start_rx.clone();
        tasks.push(tokio::spawn(async move {
            publish_worker(config, workload, index, workers, ready, start).await
        }));
    }
    start_workers(workers, &mut ready_rx, start_tx).await?;
    join_workers(tasks).await
}

async fn publish_worker(
    config: CliConfig,
    workload: PreparedWorkload,
    index: usize,
    workers: usize,
    ready: mpsc::Sender<()>,
    mut start: watch::Receiver<Option<Instant>>,
) -> Result<WorkerStats> {
    let mut client_options = config.client_options_for(&format!("bench-pub-{index}"))?;
    if workload.options.ack_level.is_some() {
        client_options.ack_contract_version = Some(client::protocol::model::ACK_CONTRACT_VERSION);
    }
    let mut client = Client::connect_with_options(&client_options).await?;
    ready
        .send(())
        .await
        .map_err(|_| CliError::msg("benchmark start coordinator stopped"))?;
    let measured_start = wait_for_start(&mut start).await?;
    let (offset, limit) = workload
        .options
        .messages
        .map(|total| work_share(total, workers, index))
        .unwrap_or((0, usize::MAX));
    let deadline = workload
        .options
        .duration_ms
        .map(|duration| measured_start + Duration::from_millis(duration));
    let mut stats = WorkerStats {
        id: index,
        role: "publisher",
        ..Default::default()
    };
    let chunk_size = match workload.options.publish_mode {
        PublishMode::Async => workload.options.max_in_flight,
        PublishMode::Batch => workload.options.batch_size,
        _ => 1,
    };
    let mut local = 0usize;
    while local < limit && deadline.is_none_or(|deadline| Instant::now() < deadline) {
        pace(&workload.options, measured_start, local, workers).await;
        let remaining = limit.saturating_sub(local);
        let size = chunk_size.min(remaining).max(1);
        let first_sequence = if workload.options.messages.is_some() {
            offset + local
        } else {
            local.saturating_mul(workers).saturating_add(index)
        };
        let stride = if workload.options.messages.is_some() {
            1
        } else {
            workers
        };
        if matches!(
            workload.options.publish_mode,
            PublishMode::Async | PublishMode::Batch
        ) {
            let actual = publish_batch(
                &mut client,
                &workload,
                index,
                first_sequence,
                stride,
                size,
                deadline,
                measured_start,
                &mut stats,
            )
            .await?;
            local += actual;
            if actual == 0 {
                break;
            }
        } else {
            publish_one(
                &mut client,
                &workload,
                index,
                first_sequence,
                measured_start,
                &mut stats,
            )
            .await?;
            local += 1;
        }
    }
    if workload.options.publish_mode == PublishMode::FireAndForget {
        client.ping_roundtrip().await?;
    }
    stats.elapsed = measured_start.elapsed();
    validate_acknowledgements(&workload.options, &stats)?;
    Ok(stats)
}

async fn publish_one(
    client: &mut Client,
    workload: &PreparedWorkload,
    worker: usize,
    sequence: usize,
    measured_start: Instant,
    stats: &mut WorkerStats,
) -> Result<()> {
    let subject = selected_subject(workload, sequence as u64);
    let key = selected_key(workload, sequence as u64);
    let headers = measurement_headers(workload, sequence, measured_start);
    let started = Instant::now();
    if let Some(level) = workload.options.ack_level {
        let ack = client
            .publish_with_qos_key_and_headers(
                &subject,
                None,
                &workload.payload,
                level,
                &format!("bench-{worker}-{sequence}"),
                key.as_deref(),
                &headers,
            )
            .await?;
        stats.observe_ack(&ack);
    } else if let Some(key) = key {
        client
            .publish_with_key_and_headers(&subject, None, &workload.payload, &key, &headers)
            .await?;
    } else if headers.is_empty() {
        client.publish(&subject, &workload.payload).await?;
    } else {
        client
            .publish_with_headers(&subject, None, &workload.payload, &headers)
            .await?;
    }
    stats.sample(started.elapsed());
    stats.operations += 1;
    stats.bytes += workload.payload.len() as u64;
    Ok(())
}

async fn publish_batch(
    client: &mut Client,
    workload: &PreparedWorkload,
    worker: usize,
    first: usize,
    stride: usize,
    requested: usize,
    deadline: Option<Instant>,
    measured_start: Instant,
    stats: &mut WorkerStats,
) -> Result<usize> {
    let mut operations = Vec::with_capacity(requested);
    for slot in 0..requested {
        if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            break;
        }
        let sequence = first.saturating_add(slot.saturating_mul(stride));
        operations.push((
            selected_subject(workload, sequence as u64),
            format!("bench-{worker}-{sequence}"),
            selected_key(workload, sequence as u64),
            measurement_headers(workload, sequence, measured_start),
        ));
    }
    let level = workload.options.ack_level.ok_or_else(|| {
        CliError::msg("async and batch publishing require a producer acknowledgement level")
    })?;
    let requests = operations
        .iter()
        .map(
            |(subject, message_id, key, headers)| BatchPublishRequestWithHeaders {
                subject,
                payload: &workload.payload,
                level,
                msg_id: message_id,
                key: key.as_deref(),
                headers,
            },
        )
        .collect::<Vec<_>>();
    if requests.is_empty() {
        return Ok(0);
    }
    let started = Instant::now();
    let acknowledgements = client
        .publish_batch_with_qos_key_and_headers(&requests)
        .await?;
    let elapsed = started.elapsed();
    if acknowledgements.len() != requests.len() {
        return Err(CliError::msg(
            "batch returned fewer acknowledgements than messages",
        ));
    }
    for ack in &acknowledgements {
        stats.observe_ack(ack);
        stats.sample(elapsed);
    }
    stats.operations += requests.len() as u64;
    stats.bytes += (requests.len() * workload.payload.len()) as u64;
    Ok(requests.len())
}

async fn run_subscribe(config: CliConfig, workload: PreparedWorkload) -> Result<Vec<WorkerStats>> {
    run_receivers(config, workload, None, None).await
}

async fn run_pubsub(config: CliConfig, workload: PreparedWorkload) -> Result<Vec<WorkerStats>> {
    let publishers = workload.options.publishers;
    let subscribers = workload.options.subscribers;
    let total_workers = publishers + subscribers;
    let (ready_tx, mut ready_rx) = mpsc::channel(total_workers);
    let (start_tx, start_rx) = watch::channel(None::<Instant>);
    let published = Arc::new(AtomicU64::new(0));
    let publishing_done = Arc::new(AtomicBool::new(false));
    let mut publisher_tasks = Vec::with_capacity(publishers);
    for index in 0..publishers {
        let config = config.clone();
        let workload = workload.clone();
        let ready = ready_tx.clone();
        let start = start_rx.clone();
        let published = published.clone();
        publisher_tasks.push(tokio::spawn(async move {
            let stats = publish_worker(config, workload, index, publishers, ready, start).await?;
            published.fetch_add(stats.operations, Ordering::AcqRel);
            Ok::<_, CliError>(stats)
        }));
    }
    let queue_group = workload.options.queue.is_some();
    let mut receiver_tasks = spawn_receivers(
        config,
        workload,
        subscribers,
        ready_tx,
        start_rx,
        Some(published.clone()),
        Some(publishing_done.clone()),
    );
    start_workers(total_workers, &mut ready_rx, start_tx).await?;
    let mut stats = join_workers(publisher_tasks).await?;
    publishing_done.store(true, Ordering::Release);
    let mut receiver_stats = join_workers(std::mem::take(&mut receiver_tasks)).await?;
    if queue_group {
        let mut identities = HashSet::new();
        for identity in receiver_stats
            .iter()
            .flat_map(|worker| worker.measurement_ids.iter().copied())
        {
            if !identities.insert(identity) {
                return Err(CliError::msg(
                    "queue benchmark delivered the same measurement identity more than once",
                ));
            }
        }
        let expected = published.load(Ordering::Acquire) as usize;
        if identities.len() != expected {
            return Err(CliError::msg(format!(
                "queue benchmark received {} unique messages but published {expected}",
                identities.len()
            )));
        }
    }
    stats.append(&mut receiver_stats);
    Ok(stats)
}

async fn run_receivers(
    config: CliConfig,
    workload: PreparedWorkload,
    published: Option<Arc<AtomicU64>>,
    publishing_done: Option<Arc<AtomicBool>>,
) -> Result<Vec<WorkerStats>> {
    let workers = workload.options.clients;
    let (ready_tx, mut ready_rx) = mpsc::channel(workers);
    let (start_tx, start_rx) = watch::channel(None::<Instant>);
    let tasks = spawn_receivers(
        config,
        workload,
        workers,
        ready_tx,
        start_rx,
        published,
        publishing_done,
    );
    start_workers(workers, &mut ready_rx, start_tx).await?;
    join_workers(tasks).await
}

fn spawn_receivers(
    config: CliConfig,
    workload: PreparedWorkload,
    workers: usize,
    ready: mpsc::Sender<()>,
    start: watch::Receiver<Option<Instant>>,
    published: Option<Arc<AtomicU64>>,
    publishing_done: Option<Arc<AtomicBool>>,
) -> Vec<tokio::task::JoinHandle<Result<WorkerStats>>> {
    let group_received = workload
        .options
        .queue
        .as_ref()
        .map(|_| Arc::new(AtomicU64::new(0)));
    (0..workers)
        .map(|index| {
            let config = config.clone();
            let workload = workload.clone();
            let ready = ready.clone();
            let start = start.clone();
            let published = published.clone();
            let publishing_done = publishing_done.clone();
            let group_received = group_received.clone();
            tokio::spawn(async move {
                receiver_worker(
                    config,
                    workload,
                    index,
                    workers,
                    ready,
                    start,
                    published,
                    publishing_done,
                    group_received,
                )
                .await
            })
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
async fn receiver_worker(
    config: CliConfig,
    workload: PreparedWorkload,
    index: usize,
    workers: usize,
    ready: mpsc::Sender<()>,
    mut start: watch::Receiver<Option<Instant>>,
    published: Option<Arc<AtomicU64>>,
    publishing_done: Option<Arc<AtomicBool>>,
    group_received: Option<Arc<AtomicU64>>,
) -> Result<WorkerStats> {
    let mut options = config.client_options_for(&format!("bench-sub-{index}"))?;
    if let Some(durable) = &workload.options.durable_id {
        options.durable_id = Some(format!("{durable}-{index}"));
    }
    let mut client = Client::connect_with_options(&options).await?;
    let subscription = subscription_subject(&workload);
    let sid = format!("bench-sub-{index}");
    if let Some(queue) = &workload.options.queue {
        client.subscribe_queue(&subscription, queue, &sid).await?;
    } else {
        client.subscribe(&subscription, &sid).await?;
    }
    client.ping_roundtrip().await?;
    ready
        .send(())
        .await
        .map_err(|_| CliError::msg("benchmark start coordinator stopped"))?;
    let measured_start = wait_for_start(&mut start).await?;
    let fixed_target = workload.options.messages.map(|total| total as u64);
    let deadline = workload
        .options
        .duration_ms
        .map(|ms| measured_start + Duration::from_millis(ms));
    let mut stats = WorkerStats {
        id: index,
        role: "subscriber",
        ..Default::default()
    };
    let mut seen = HashSet::new();
    loop {
        let expected = fixed_target.or_else(|| {
            publishing_done
                .as_ref()
                .filter(|done| done.load(Ordering::Acquire))
                .and_then(|_| published.as_ref())
                .map(|count| count.load(Ordering::Acquire))
        });
        let progress = group_received
            .as_ref()
            .map_or(stats.operations, |count| count.load(Ordering::Acquire));
        if expected.is_some_and(|target| progress >= target) {
            break;
        }
        if published.is_none() && deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            break;
        }
        let wait = if deadline.is_some_and(|deadline| Instant::now() < deadline) {
            deadline
                .unwrap()
                .saturating_duration_since(Instant::now())
                .min(Duration::from_millis(250))
        } else {
            Duration::from_secs(2)
        };
        let message = match tokio::time::timeout(wait, client.next_message()).await {
            Ok(message) => message?,
            Err(_)
                if group_received.as_ref().is_some_and(|count| {
                    expected.is_some_and(|target| count.load(Ordering::Acquire) >= target)
                }) =>
            {
                break;
            }
            Err(_) if published.is_none() && deadline.is_some() => break,
            Err(_)
                if publishing_done
                    .as_ref()
                    .is_some_and(|done| !done.load(Ordering::Acquire)) =>
            {
                continue;
            }
            Err(_) => {
                return Err(CliError::msg(
                    "benchmark delivery was incomplete before drain timeout",
                ));
            }
        };
        if let Some(sequence) = header_u64(&message.headers, "Bench-Sequence") {
            if !seen.insert(sequence) {
                stats.duplicates += 1;
            }
            stats.measurement_ids.push(sequence);
        }
        if let Some(sent_us) = header_u64(&message.headers, "Bench-Sent-Us") {
            let now_us = measured_start
                .elapsed()
                .as_micros()
                .min(u128::from(u64::MAX)) as u64;
            stats.latencies_us.push(now_us.saturating_sub(sent_us));
            stats.latencies_us.truncate(MAX_SAMPLES_PER_CLIENT);
        }
        if workload.options.ack {
            let ack_subject = message.ack_subject.as_deref().ok_or_else(|| {
                CliError::msg("explicit acknowledgement requested but delivery has no ACK identity")
            })?;
            client.ack(ack_subject).await?;
            stats.acknowledgements += 1;
        }
        stats.operations += 1;
        if let Some(received) = &group_received {
            received.fetch_add(1, Ordering::AcqRel);
        }
        stats.bytes += message.payload.len() as u64;
        pace(
            &workload.options,
            measured_start,
            stats.operations as usize,
            workers,
        )
        .await;
    }
    stats.elapsed = measured_start.elapsed();
    if stats.duplicates > 0 {
        return Err(CliError::msg(format!(
            "benchmark received {} duplicate deliveries",
            stats.duplicates
        )));
    }
    Ok(stats)
}

pub(super) async fn start_workers(
    workers: usize,
    ready: &mut mpsc::Receiver<()>,
    start: watch::Sender<Option<Instant>>,
) -> Result<Instant> {
    for _ in 0..workers {
        ready
            .recv()
            .await
            .ok_or_else(|| CliError::msg("benchmark worker failed during setup"))?;
    }
    let measured_start = Instant::now();
    start
        .send(Some(measured_start))
        .map_err(|_| CliError::msg("benchmark workers stopped before measurement"))?;
    Ok(measured_start)
}

pub(super) async fn wait_for_start(
    start: &mut watch::Receiver<Option<Instant>>,
) -> Result<Instant> {
    loop {
        if let Some(started) = *start.borrow() {
            return Ok(started);
        }
        start
            .changed()
            .await
            .map_err(|_| CliError::msg("benchmark start coordinator stopped"))?;
    }
}

pub(super) async fn join_workers(
    tasks: Vec<tokio::task::JoinHandle<Result<WorkerStats>>>,
) -> Result<Vec<WorkerStats>> {
    let mut stats = Vec::with_capacity(tasks.len());
    for task in tasks {
        stats.push(
            task.await
                .map_err(|error| CliError::with_source("joining benchmark worker", error))??,
        );
    }
    Ok(stats)
}

pub(super) async fn pace(options: &BenchmarkOptions, start: Instant, local: usize, workers: usize) {
    let target = start + pacing_delay(local, workers, options.throughput, options.sleep_ms);
    if target > Instant::now() {
        tokio::time::sleep_until(target.into()).await;
    }
}

pub(super) fn selected_subject(workload: &PreparedWorkload, sequence: u64) -> String {
    if workload.options.subjects == 1 {
        return workload.target.clone();
    }
    let index = match workload.options.subject_order {
        SubjectOrder::Sequential => sequence as usize % workload.options.subjects,
        SubjectOrder::Random => {
            deterministic_index(sequence, workload.options.subjects, workload.options.seed)
        }
    };
    format!("{}/{index}", workload.target)
}

pub(super) fn subscription_subject(workload: &PreparedWorkload) -> String {
    if workload.options.subjects == 1 {
        workload.target.clone()
    } else {
        format!("{}/*", workload.target)
    }
}

pub(super) fn selected_key(workload: &PreparedWorkload, sequence: u64) -> Option<String> {
    (workload.options.key_cardinality > 0).then(|| {
        let index = deterministic_index(
            sequence,
            workload.options.key_cardinality,
            workload.options.seed,
        );
        format!("key-{index}")
    })
}
