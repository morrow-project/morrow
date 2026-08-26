use super::*;

pub(super) async fn run_request(
    config: CliConfig,
    workload: PreparedWorkload,
) -> Result<Vec<WorkerStats>> {
    let workers = workload.options.clients;
    let (ready_tx, mut ready_rx) = mpsc::channel(workers);
    let (start_tx, start_rx) = watch::channel(None::<Instant>);
    let mut tasks = Vec::with_capacity(workers);
    for index in 0..workers {
        let config = config.clone();
        let workload = workload.clone();
        let ready = ready_tx.clone();
        let mut start = start_rx.clone();
        tasks.push(tokio::spawn(async move {
            let mut client = Client::connect_with_options(
                &config.client_options_for(&format!("bench-request-{index}"))?,
            )
            .await?;
            ready
                .send(())
                .await
                .map_err(|_| CliError::msg("benchmark start coordinator stopped"))?;
            let measured_start = wait_for_start(&mut start).await?;
            let (_, limit) = workload
                .options
                .messages
                .map(|total| work_share(total, workers, index))
                .unwrap_or((0, usize::MAX));
            let deadline = workload
                .options
                .duration_ms
                .map(|ms| measured_start + Duration::from_millis(ms));
            let mut stats = WorkerStats {
                id: index,
                role: "requester",
                ..Default::default()
            };
            while stats.operations < limit as u64
                && deadline.is_none_or(|deadline| Instant::now() < deadline)
            {
                pace(
                    &workload.options,
                    measured_start,
                    stats.operations as usize,
                    workers,
                )
                .await;
                let subject = selected_subject(&workload, stats.operations);
                let key = selected_key(&workload, stats.operations);
                let started = Instant::now();
                match client
                    .request_with_key_and_headers(
                        &subject,
                        &workload.payload,
                        key.as_deref(),
                        &workload.options.headers,
                        Duration::from_millis(workload.options.timeout_ms),
                    )
                    .await
                {
                    Ok(response) => {
                        stats.sample(started.elapsed());
                        stats.operations += 1;
                        stats.bytes += (workload.payload.len() + response.payload.len()) as u64;
                    }
                    Err(error) if error.to_string().contains("timed out") => {
                        stats.timeouts += 1;
                        return Err(CliError::with_source("benchmark request timed out", error));
                    }
                    Err(error) => return Err(error.into()),
                }
            }
            stats.elapsed = measurement_elapsed(&workload.options, measured_start);
            Ok::<_, CliError>(stats)
        }));
    }
    start_workers(workers, &mut ready_rx, start_tx).await?;
    join_workers(tasks).await
}

pub(super) async fn run_serve(
    config: CliConfig,
    workload: PreparedWorkload,
) -> Result<Vec<WorkerStats>> {
    let workers = workload.options.clients;
    let (ready_tx, mut ready_rx) = mpsc::channel(workers);
    let (start_tx, start_rx) = watch::channel(None::<Instant>);
    let group_operations = workload
        .options
        .queue
        .as_ref()
        .map(|_| Arc::new(AtomicU64::new(0)));
    let mut tasks = Vec::with_capacity(workers);
    for index in 0..workers {
        let config = config.clone();
        let workload = workload.clone();
        let ready = ready_tx.clone();
        let mut start = start_rx.clone();
        let group_operations = group_operations.clone();
        tasks.push(tokio::spawn(async move {
            let mut client = Client::connect_with_options(
                &config.client_options_for(&format!("bench-serve-{index}"))?,
            )
            .await?;
            let sid = format!("bench-serve-{index}");
            if let Some(queue) = &workload.options.queue {
                client
                    .subscribe_queue(&subscription_subject(&workload), queue, &sid)
                    .await?;
            } else {
                client
                    .subscribe(&subscription_subject(&workload), &sid)
                    .await?;
            }
            client.ping_roundtrip().await?;
            ready
                .send(())
                .await
                .map_err(|_| CliError::msg("benchmark start coordinator stopped"))?;
            let measured_start = wait_for_start(&mut start).await?;
            let limit = workload.options.messages.unwrap_or(usize::MAX);
            let deadline = workload
                .options
                .duration_ms
                .map(|ms| measured_start + Duration::from_millis(ms));
            let mut stats = WorkerStats {
                id: index,
                role: "responder",
                ..Default::default()
            };
            loop {
                let progress = group_operations
                    .as_ref()
                    .map_or(stats.operations, |count| count.load(Ordering::Acquire));
                if progress >= limit as u64
                    || deadline.is_some_and(|deadline| Instant::now() >= deadline)
                {
                    break;
                }
                let wait = deadline
                    .map(|deadline| deadline.saturating_duration_since(Instant::now()))
                    .unwrap_or_else(|| {
                        if group_operations.is_some() {
                            Duration::from_secs(2)
                        } else {
                            Duration::from_millis(workload.options.timeout_ms)
                        }
                    });
                let message = match tokio::time::timeout(wait, client.next_message()).await {
                    Ok(message) => message?,
                    Err(_) if deadline.is_some() => break,
                    Err(_)
                        if group_operations
                            .as_ref()
                            .is_some_and(|count| count.load(Ordering::Acquire) >= limit as u64) =>
                    {
                        break;
                    }
                    Err(_) => return Err(CliError::msg("benchmark responder timed out")),
                };
                let started = Instant::now();
                client
                    .respond_with_headers(&message, &workload.payload, &workload.options.headers)
                    .await?;
                if let Some(ack_subject) = &message.ack_subject {
                    client.ack(ack_subject).await?;
                    stats.acknowledgements += 1;
                }
                stats.sample(started.elapsed());
                stats.operations += 1;
                if let Some(operations) = &group_operations {
                    operations.fetch_add(1, Ordering::AcqRel);
                }
                stats.bytes += (message.payload.len() + workload.payload.len()) as u64;
                pace(
                    &workload.options,
                    measured_start,
                    stats.operations as usize,
                    workers,
                )
                .await;
            }
            stats.elapsed = measurement_elapsed(&workload.options, measured_start);
            Ok::<_, CliError>(stats)
        }));
    }
    start_workers(workers, &mut ready_rx, start_tx).await?;
    join_workers(tasks).await
}

pub(super) async fn run_fetch(
    config: CliConfig,
    workload: PreparedWorkload,
    fetch_mode: bool,
) -> Result<Vec<WorkerStats>> {
    let workers = workload.options.clients;
    let (ready_tx, mut ready_rx) = mpsc::channel(workers);
    let (start_tx, start_rx) = watch::channel(None::<Instant>);
    let mut tasks = Vec::with_capacity(workers);
    for index in 0..workers {
        let config = config.clone();
        let workload = workload.clone();
        let ready = ready_tx.clone();
        let mut start = start_rx.clone();
        tasks.push(tokio::spawn(async move {
            let mut client_options = config.client_options_for(&format!("bench-fetch-{index}"))?;
            if let Some(durable) = &workload.options.durable_id {
                client_options.durable_id = Some(format!("{durable}-{index}"));
            }
            let mut client = Client::connect_with_options(&client_options).await?;
            ready
                .send(())
                .await
                .map_err(|_| CliError::msg("benchmark start coordinator stopped"))?;
            let measured_start = wait_for_start(&mut start).await?;
            let (_, limit) = workload
                .options
                .messages
                .map(|total| work_share(total, workers, index))
                .unwrap_or((0, usize::MAX));
            let deadline = workload
                .options
                .duration_ms
                .map(|ms| measured_start + Duration::from_millis(ms));
            let mut stats = WorkerStats {
                id: index,
                role: if fetch_mode { "fetcher" } else { "consumer" },
                ..Default::default()
            };
            while stats.operations < limit as u64
                && deadline.is_none_or(|deadline| Instant::now() < deadline)
            {
                let remaining = limit.saturating_sub(stats.operations as usize);
                let batch = workload.options.batch_size.min(remaining).max(1);
                let started = Instant::now();
                let messages = client
                    .fetch(
                        &workload.target,
                        batch,
                        workload.options.max_bytes,
                        Duration::from_millis(workload.options.timeout_ms),
                    )
                    .await?;
                if messages.is_empty() {
                    if deadline.is_some() {
                        continue;
                    }
                    return Err(CliError::msg(
                        "fetch returned no messages before count target was reached",
                    ));
                }
                let elapsed = started.elapsed();
                for message in &messages {
                    if workload.options.ack {
                        client.ack_delivery(message).await?;
                        stats.acknowledgements += 1;
                    }
                    stats.sample(elapsed);
                    stats.operations += 1;
                    stats.bytes += message.payload.len() as u64;
                }
                pace(
                    &workload.options,
                    measured_start,
                    stats.operations as usize,
                    workers,
                )
                .await;
            }
            stats.elapsed = measurement_elapsed(&workload.options, measured_start);
            Ok::<_, CliError>(stats)
        }));
    }
    start_workers(workers, &mut ready_rx, start_tx).await?;
    join_workers(tasks).await
}
