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
        _ => Err(usage()),
    }
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
        "usage: morrow-cli [--config client.json] <ping|pub|sub|request|reply>\n\
         pub <subject> <payload>\n\
         sub <subject> [--sid sid] [--queue group] [--ack] [--max-messages n]\n\
         request <subject> <payload> [--timeout-ms n]\n\
         reply <subject> [--queue group]",
    )
}
