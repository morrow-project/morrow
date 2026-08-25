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
    match args.next().as_deref() {
        Some("pubsub") => parse_bench_pubsub(args),
        _ => Err(CliError::msg("bench requires a mode: pubsub <subject>")),
    }
}

fn parse_bench_pubsub(mut args: impl Iterator<Item = String>) -> Result<Command> {
    let subject = args
        .next()
        .ok_or_else(|| CliError::msg("bench pubsub requires a subject"))?;
    let mut messages = None;
    let mut duration_ms = None;
    let mut payload_size = 1024;
    let mut publishers = 1;
    let mut subscribers = 1;
    let mut concurrency = 1;
    let mut ack = false;
    let mut ack_level = None;
    let mut durable_id = None;
    let mut json = false;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--messages" => messages = Some(parse_usize(&mut args, "--messages")?),
            "--duration" => duration_ms = Some(parse_duration_ms(&mut args, "--duration")?),
            "--payload-size" => payload_size = parse_usize(&mut args, "--payload-size")?,
            "--publishers" => publishers = parse_usize(&mut args, "--publishers")?,
            "--subscribers" => subscribers = parse_usize(&mut args, "--subscribers")?,
            "--concurrency" => concurrency = parse_usize(&mut args, "--concurrency")?,
            "--ack" => {
                ack = true;
                ack_level.get_or_insert(AckLevel::Durable);
            }
            "--ack-level" => {
                let value = args
                    .next()
                    .ok_or_else(|| CliError::msg("--ack-level requires a value"))?;
                ack_level = Some(parse_bench_ack_level(&value)?);
            }
            "--durable-id" => {
                durable_id = Some(
                    args.next()
                        .ok_or_else(|| CliError::msg("--durable-id requires a value"))?,
                )
            }
            "--json" => json = true,
            _ => return Err(CliError::msg(format!("unknown bench pubsub option {arg}"))),
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
    if payload_size < 16 {
        return Err(CliError::msg("--payload-size must be at least 16 bytes"));
    }
    for (name, value) in [
        ("--publishers", publishers),
        ("--subscribers", subscribers),
        ("--concurrency", concurrency),
    ] {
        if value == 0 {
            return Err(CliError::msg(format!("{name} must be greater than zero")));
        }
    }
    Ok(Command::BenchPubSub {
        subject,
        messages,
        duration_ms,
        payload_size,
        publishers,
        subscribers,
        concurrency,
        ack,
        ack_level,
        durable_id,
        json,
    })
}

fn parse_usize(args: &mut impl Iterator<Item = String>, option: &str) -> Result<usize> {
    args.next()
        .ok_or_else(|| CliError::msg(format!("{option} requires a value")))?
        .parse()
        .map_err(|_| CliError::msg(format!("{option} must be a positive integer")))
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

pub(super) async fn read_stdin_line() -> Result<String> {
    tokio::task::spawn_blocking(|| {
        let mut line = String::new();
        let read = std::io::stdin()
            .lock()
            .read_line(&mut line)
            .map_err(|err| CliError::with_source("reading response from stdin", err))?;
        if read == 0 {
            return Err(CliError::msg("stdin closed before response was provided"));
        }
        while line.ends_with('\n') || line.ends_with('\r') {
            line.pop();
        }
        Ok(line)
    })
    .await
    .map_err(|err| CliError::with_source("joining stdin reader task", err))?
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

pub(super) fn usage() -> CliError {
    CliError::msg(
        "usage: morrow-cli [--config client.json] [--server host:port] <ping|pub|sub|request|reply|bench>\n\
         pub <subject> <payload>\n\
         sub <subject> [--sid sid] [--queue group] [--ack] [--max-messages n]\n\
         request <subject> <payload> [--timeout-ms n]\n\
         reply <subject> [--queue group]\n\
         bench pubsub <subject> [--messages n|--duration 30s] [--payload-size n]\n\
             [--publishers n] [--subscribers n] [--concurrency n]\n\
             [--ack|--ack-level accepted|durable|high-durability|cluster-durable]\n\
             [--durable-id id] [--json]",
    )
}
