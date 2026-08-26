use super::*;

pub(super) fn parse_command(args: Vec<String>) -> Result<Command> {
    let mut args = args.into_iter();
    let command = args.next().ok_or_else(usage)?;
    match command.as_str() {
        "ping" => {
            ensure_no_more(args, "ping")?;
            Ok(Command::Ping)
        }
        "pub" => {
            let subject = args
                .next()
                .ok_or_else(|| CliError::msg("pub requires a subject"))?;
            let payload = args
                .next()
                .ok_or_else(|| CliError::msg("pub requires a payload"))?
                .into_bytes();
            let mut qos = None;
            let mut msg_id = None;
            while let Some(arg) = args.next() {
                match arg.as_str() {
                    "--qos" => {
                        let value = args
                            .next()
                            .ok_or_else(|| CliError::msg("--qos requires a value"))?;
                        qos = Some(parse_ack_level(&value)?);
                    }
                    "--msg-id" => {
                        msg_id = Some(
                            args.next()
                                .ok_or_else(|| CliError::msg("--msg-id requires a value"))?,
                        );
                    }
                    _ => return Err(CliError::msg(format!("unknown pub option {arg}"))),
                }
            }
            if qos.is_some() && msg_id.is_none() {
                return Err(CliError::msg("--msg-id is required when --qos is set"));
            }
            if qos.is_none() && msg_id.is_some() {
                return Err(CliError::msg("--qos is required when --msg-id is set"));
            }
            Ok(Command::Pub {
                subject,
                payload,
                qos,
                msg_id,
            })
        }
        "sub" => parse_sub(args),
        "request" => parse_request(args),
        "reply" => parse_reply(args),
        "bench" => parse_bench(args),
        _ => Err(usage()),
    }
}

fn parse_bench(mut args: impl Iterator<Item = String>) -> Result<Command> {
    let mode = match args.next().as_deref() {
        Some("pub") => BenchmarkMode::Pub,
        Some("sub") => BenchmarkMode::Sub,
        Some("pubsub") => BenchmarkMode::PubSub,
        Some("request") => BenchmarkMode::Request,
        Some("serve") => BenchmarkMode::Serve,
        Some("consume") => BenchmarkMode::Consume,
        Some("fetch") => BenchmarkMode::Fetch,
        _ => {
            return Err(CliError::msg(
                "bench requires a mode: pub, sub, pubsub, request, serve, consume, or fetch",
            ));
        }
    };
    parse_benchmark(mode, args)
}

fn parse_benchmark(mode: BenchmarkMode, mut args: impl Iterator<Item = String>) -> Result<Command> {
    let target = args.next().ok_or_else(|| {
        CliError::msg(format!("bench {} requires a target", bench_mode_name(mode)))
    })?;
    let mut messages = None;
    let mut duration_ms = None;
    let mut clients = 1;
    let mut clients_set = false;
    let mut payload_size = 128;
    let mut payload_size_set = false;
    let mut payload_file = None;
    let mut headers = Vec::new();
    let mut publishers = 1;
    let mut publishers_set = false;
    let mut subscribers = 1;
    let mut subscribers_set = false;
    let mut concurrency = 1;
    let mut throughput = 0;
    let mut publish_mode = PublishMode::FireAndForget;
    let mut ack = false;
    let mut ack_level = None;
    let mut max_in_flight = 1024;
    let mut max_in_flight_set = false;
    let mut batch_size = if mode == BenchmarkMode::Consume {
        1
    } else {
        100
    };
    let mut batch_size_set = false;
    let mut subjects = 1;
    let mut subject_order = SubjectOrder::Sequential;
    let mut key_cardinality = 0;
    let mut sleep_ms = 0;
    let mut warmup_ms = 0;
    let mut seed = 1;
    let mut queue = None;
    let mut durable_id = None;
    let mut timeout_ms = 30_000;
    let mut max_bytes = DEFAULT_MAX_PAYLOAD;
    let mut json = false;
    let mut csv = None;
    let mut stream = None;
    let mut partition_metadata = None;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--messages" => messages = Some(parse_usize(&mut args, "--messages")?),
            "--duration" => duration_ms = Some(parse_duration_ms(&mut args, "--duration")?),
            "--clients" => {
                clients = parse_usize(&mut args, "--clients")?;
                clients_set = true;
            }
            "--payload-size" => {
                payload_size = parse_usize(&mut args, "--payload-size")?;
                payload_size_set = true;
            }
            "--payload" => payload_file = Some(PathBuf::from(next_value(&mut args, "--payload")?)),
            "--header" => headers.push(parse_header(&next_value(&mut args, "--header")?)?),
            "--publishers" => {
                publishers = parse_usize(&mut args, "--publishers")?;
                publishers_set = true;
            }
            "--subscribers" => {
                subscribers = parse_usize(&mut args, "--subscribers")?;
                subscribers_set = true;
            }
            "--concurrency" => concurrency = parse_usize(&mut args, "--concurrency")?,
            "--throughput" => throughput = parse_u64(&mut args, "--throughput")?,
            "--mode" => {
                publish_mode = match next_value(&mut args, "--mode")?.as_str() {
                    "fire-and-forget" => PublishMode::FireAndForget,
                    "sync" => PublishMode::Sync,
                    "async" => PublishMode::Async,
                    "batch" => PublishMode::Batch,
                    _ => {
                        return Err(CliError::msg(
                            "--mode must be fire-and-forget, sync, async, or batch",
                        ));
                    }
                }
            }
            "--ack" => {
                ack = true;
            }
            "--ack-level" => {
                let value = next_value(&mut args, "--ack-level")?;
                ack_level = Some(parse_bench_ack_level(&value)?);
            }
            "--max-in-flight" => {
                max_in_flight = parse_usize(&mut args, "--max-in-flight")?;
                max_in_flight_set = true;
            }
            "--batch-size" => {
                batch_size = parse_usize(&mut args, "--batch-size")?;
                batch_size_set = true;
            }
            "--subjects" => subjects = parse_usize(&mut args, "--subjects")?,
            "--subject-order" => {
                subject_order = match next_value(&mut args, "--subject-order")?.as_str() {
                    "sequential" => SubjectOrder::Sequential,
                    "random" => SubjectOrder::Random,
                    _ => {
                        return Err(CliError::msg(
                            "--subject-order must be sequential or random",
                        ));
                    }
                }
            }
            "--key-cardinality" => key_cardinality = parse_usize(&mut args, "--key-cardinality")?,
            "--sleep" => sleep_ms = parse_duration_ms(&mut args, "--sleep")?,
            "--warmup" => warmup_ms = parse_duration_ms(&mut args, "--warmup")?,
            "--seed" => seed = parse_u64(&mut args, "--seed")?,
            "--queue" => queue = Some(next_value(&mut args, "--queue")?),
            "--durable-id" => durable_id = Some(next_value(&mut args, "--durable-id")?),
            "--timeout" => timeout_ms = parse_duration_ms(&mut args, "--timeout")?,
            "--max-bytes" => max_bytes = parse_usize(&mut args, "--max-bytes")?,
            "--json" => json = true,
            "--csv" => csv = Some(PathBuf::from(next_value(&mut args, "--csv")?)),
            "--stream" => stream = Some(next_value(&mut args, "--stream")?),
            "--partition-metadata" => {
                partition_metadata = Some(PathBuf::from(next_value(
                    &mut args,
                    "--partition-metadata",
                )?))
            }
            _ => {
                return Err(CliError::msg(format!(
                    "unknown bench {} option {arg}",
                    bench_mode_name(mode)
                )));
            }
        }
    }
    if messages.is_some() && duration_ms.is_some() {
        return Err(CliError::msg(
            "--messages and --duration are mutually exclusive",
        ));
    }
    if messages.is_none() && duration_ms.is_none() {
        messages = Some(10_000);
    }
    if messages == Some(0) {
        return Err(CliError::msg("--messages must be greater than zero"));
    }
    if payload_file.is_some() && payload_size_set {
        return Err(CliError::msg(
            "--payload and --payload-size are mutually exclusive",
        ));
    }
    for (name, value) in [
        ("--clients", clients),
        ("--publishers", publishers),
        ("--subscribers", subscribers),
        ("--concurrency", concurrency),
        ("--payload-size", payload_size),
        ("--max-in-flight", max_in_flight),
        ("--batch-size", batch_size),
        ("--subjects", subjects),
        ("--max-bytes", max_bytes),
    ] {
        if value == 0 {
            return Err(CliError::msg(format!("{name} must be greater than zero")));
        }
    }
    if mode != BenchmarkMode::PubSub && (publishers != 1 || subscribers != 1 || concurrency != 1) {
        return Err(CliError::msg(
            "--publishers, --subscribers, and --concurrency are legacy pubsub-only flags; use --clients",
        ));
    }
    if mode == BenchmarkMode::PubSub {
        if clients_set && (publishers_set || subscribers_set || concurrency != 1) {
            return Err(CliError::msg(
                "--clients cannot be combined with legacy --publishers, --subscribers, or --concurrency",
            ));
        }
        if clients_set {
            publishers = clients;
            subscribers = clients;
        }
        publishers = publishers
            .checked_mul(concurrency)
            .ok_or_else(|| CliError::msg("publisher count is too large"))?;
    } else {
        publishers = clients;
        subscribers = clients;
        concurrency = 1;
    }
    let publishing = matches!(mode, BenchmarkMode::Pub | BenchmarkMode::PubSub);
    if (stream.is_some() || partition_metadata.is_some()) && !publishing {
        return Err(CliError::msg(
            "--stream and --partition-metadata apply only to publish workloads",
        ));
    }
    if partition_metadata.is_some() && stream.is_none() {
        return Err(CliError::msg(
            "--stream is required when --partition-metadata is set",
        ));
    }
    if partition_metadata.is_some()
        && matches!(publish_mode, PublishMode::Async | PublishMode::Batch)
    {
        return Err(CliError::msg(
            "direct partition routing currently supports fire-and-forget and sync modes",
        ));
    }
    if mode == BenchmarkMode::PubSub
        && ack
        && ack_level.is_none()
        && publish_mode == PublishMode::FireAndForget
    {
        ack_level = Some(AckLevel::Durable);
        publish_mode = PublishMode::Sync;
    }
    if ack_level.is_some() && publish_mode == PublishMode::FireAndForget {
        // Legacy pubsub used --ack-level without a mode and meant synchronous QoS.
        if mode == BenchmarkMode::PubSub {
            publish_mode = PublishMode::Sync;
        } else {
            return Err(CliError::msg(
                "--ack-level is invalid with --mode fire-and-forget",
            ));
        }
    }
    if matches!(
        publish_mode,
        PublishMode::Sync | PublishMode::Async | PublishMode::Batch
    ) && publishing
        && ack_level.is_none()
    {
        ack_level = Some(AckLevel::Durable);
    }
    if !publishing && (publish_mode != PublishMode::FireAndForget || ack_level.is_some()) {
        return Err(CliError::msg(
            "--mode and --ack-level apply only to publish workloads",
        ));
    }
    if max_in_flight_set && !(publishing && publish_mode == PublishMode::Async) {
        return Err(CliError::msg(
            "--max-in-flight requires a publish workload with --mode async",
        ));
    }
    if batch_size_set
        && !((publishing && publish_mode == PublishMode::Batch)
            || matches!(mode, BenchmarkMode::Consume | BenchmarkMode::Fetch))
    {
        return Err(CliError::msg(
            "--batch-size requires --mode batch, consume, or fetch",
        ));
    }
    if queue.is_some()
        && !matches!(
            mode,
            BenchmarkMode::Sub | BenchmarkMode::PubSub | BenchmarkMode::Serve
        )
    {
        return Err(CliError::msg(
            "--queue applies only to sub, pubsub, and serve workloads",
        ));
    }
    if durable_id.is_some()
        && !matches!(
            mode,
            BenchmarkMode::Sub
                | BenchmarkMode::PubSub
                | BenchmarkMode::Consume
                | BenchmarkMode::Fetch
        )
    {
        return Err(CliError::msg(
            "--durable-id applies only to delivery workloads",
        ));
    }
    if matches!(
        mode,
        BenchmarkMode::Sub | BenchmarkMode::Consume | BenchmarkMode::Fetch
    ) && (payload_size_set || payload_file.is_some() || !headers.is_empty())
    {
        return Err(CliError::msg(
            "payload and header options apply only to generated-message workloads",
        ));
    }
    if matches!(mode, BenchmarkMode::Consume | BenchmarkMode::Fetch) && subjects != 1 {
        return Err(CliError::msg(
            "--subjects does not apply when the target is a durable consumer",
        ));
    }
    if key_cardinality > 0
        && !matches!(
            mode,
            BenchmarkMode::Pub | BenchmarkMode::PubSub | BenchmarkMode::Request
        )
    {
        return Err(CliError::msg(
            "--key-cardinality applies only to pub, pubsub, and request workloads",
        ));
    }
    if ack
        && !matches!(
            mode,
            BenchmarkMode::Sub
                | BenchmarkMode::PubSub
                | BenchmarkMode::Consume
                | BenchmarkMode::Fetch
        )
    {
        return Err(CliError::msg("--ack applies only to delivery workloads"));
    }
    Ok(Command::Bench {
        mode,
        target,
        options: BenchmarkOptions {
            messages,
            duration_ms,
            clients,
            publishers,
            subscribers,
            concurrency,
            throughput,
            payload_size,
            payload_file,
            headers,
            publish_mode,
            ack_level,
            max_in_flight,
            batch_size,
            subjects,
            subject_order,
            key_cardinality,
            sleep_ms,
            warmup_ms,
            seed,
            queue,
            ack,
            durable_id,
            timeout_ms,
            max_bytes,
            json,
            csv,
            stream,
            partition_metadata,
        },
    })
}

fn bench_mode_name(mode: BenchmarkMode) -> &'static str {
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

fn next_value(args: &mut impl Iterator<Item = String>, option: &str) -> Result<String> {
    args.next()
        .ok_or_else(|| CliError::msg(format!("{option} requires a value")))
}

fn parse_header(value: &str) -> Result<(String, String)> {
    let (name, value) = value
        .split_once(':')
        .ok_or_else(|| CliError::msg("--header must use K:V syntax"))?;
    let normalized = name.to_ascii_lowercase();
    if name.is_empty()
        || normalized.starts_with("morrow-")
        || matches!(normalized.as_str(), "bench-sequence" | "bench-sent-us")
    {
        return Err(CliError::msg("--header name is empty or reserved"));
    }
    if !name
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        || value.bytes().any(|byte| matches!(byte, b'\r' | b'\n'))
    {
        return Err(CliError::msg("--header contains invalid characters"));
    }
    Ok((name.to_string(), value.to_string()))
}

fn parse_usize(args: &mut impl Iterator<Item = String>, option: &str) -> Result<usize> {
    args.next()
        .ok_or_else(|| CliError::msg(format!("{option} requires a value")))?
        .parse()
        .map_err(|_| CliError::msg(format!("{option} must be a positive integer")))
}

fn parse_u64(args: &mut impl Iterator<Item = String>, option: &str) -> Result<u64> {
    next_value(args, option)?
        .parse()
        .map_err(|_| CliError::msg(format!("{option} must be an unsigned integer")))
}

fn parse_duration_ms(args: &mut impl Iterator<Item = String>, option: &str) -> Result<u64> {
    let value = args
        .next()
        .ok_or_else(|| CliError::msg(format!("{option} requires a value")))?;
    let (number, multiplier) = if let Some(value) = value.strip_suffix("ms") {
        (value, 1_u64)
    } else if let Some(value) = value.strip_suffix('s') {
        (value, 1_000)
    } else if let Some(value) = value.strip_suffix('m') {
        (value, 60_000)
    } else {
        return Err(CliError::msg(format!(
            "{option} must use ms, s, or m (for example 30s)"
        )));
    };
    number
        .parse::<u64>()
        .ok()
        .and_then(|value| value.checked_mul(multiplier))
        .filter(|value| *value > 0)
        .ok_or_else(|| CliError::msg(format!("{option} must be greater than zero")))
}

fn parse_ack_level(value: &str) -> Result<AckLevel> {
    match value {
        "0" => Ok(AckLevel::Accepted),
        "1" => Ok(AckLevel::Durable),
        "2" => Ok(AckLevel::HighDurability),
        "3" => Ok(AckLevel::ClusterDurable),
        _ => Err(CliError::msg("--qos must be 0, 1, 2, or 3")),
    }
}

fn parse_bench_ack_level(value: &str) -> Result<AckLevel> {
    match value {
        "accepted" => Ok(AckLevel::Accepted),
        "durable" => Ok(AckLevel::Durable),
        "high-durability" => Ok(AckLevel::HighDurability),
        "cluster-durable" => Ok(AckLevel::ClusterDurable),
        _ => Err(CliError::msg(
            "--ack-level must be accepted, durable, high-durability, or cluster-durable",
        )),
    }
}

pub(super) fn parse_request(mut args: impl Iterator<Item = String>) -> Result<Command> {
    let subject = args
        .next()
        .ok_or_else(|| CliError::msg("request requires a subject"))?;
    let payload = args
        .next()
        .ok_or_else(|| CliError::msg("request requires a payload"))?
        .into_bytes();
    let mut timeout_ms = 30_000;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--timeout-ms" => {
                let value = args
                    .next()
                    .ok_or_else(|| CliError::msg("--timeout-ms requires a value"))?;
                timeout_ms = value
                    .parse()
                    .map_err(|_| CliError::msg("--timeout-ms must be an integer"))?;
            }
            _ => return Err(CliError::msg(format!("unknown request option {arg}"))),
        }
    }
    Ok(Command::Request {
        subject,
        payload,
        timeout_ms,
    })
}

pub(super) fn parse_reply(mut args: impl Iterator<Item = String>) -> Result<Command> {
    let subject = args
        .next()
        .ok_or_else(|| CliError::msg("reply requires a subject"))?;
    let mut queue = None;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--queue" => {
                queue = Some(
                    args.next()
                        .ok_or_else(|| CliError::msg("--queue requires a value"))?,
                );
            }
            _ => return Err(CliError::msg(format!("unknown reply option {arg}"))),
        }
    }
    Ok(Command::Reply { subject, queue })
}

pub(super) fn parse_sub(mut args: impl Iterator<Item = String>) -> Result<Command> {
    let subject = args
        .next()
        .ok_or_else(|| CliError::msg("sub requires a subject"))?;
    let mut sid = DEFAULT_SID.to_string();
    let mut queue = None;
    let mut ack = false;
    let mut max_messages = None;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--sid" => {
                sid = args
                    .next()
                    .ok_or_else(|| CliError::msg("--sid requires a value"))?;
            }
            "--queue" => {
                queue = Some(
                    args.next()
                        .ok_or_else(|| CliError::msg("--queue requires a value"))?,
                );
            }
            "--ack" => ack = true,
            "--max-messages" => {
                let value = args
                    .next()
                    .ok_or_else(|| CliError::msg("--max-messages requires a value"))?;
                max_messages = Some(
                    value
                        .parse()
                        .map_err(|_| CliError::msg("--max-messages must be an integer"))?,
                );
            }
            _ => return Err(CliError::msg(format!("unknown sub option {arg}"))),
        }
    }
    Ok(Command::Sub {
        subject,
        sid,
        queue,
        ack,
        max_messages,
    })
}

pub(super) fn ensure_no_more(mut args: impl Iterator<Item = String>, command: &str) -> Result<()> {
    if let Some(arg) = args.next() {
        return Err(CliError::msg(format!(
            "{command} received unexpected argument {arg}"
        )));
    }
    Ok(())
}
