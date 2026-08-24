use super::*;

#[derive(Debug)]
pub struct CliError {
    pub(super) message: String,
    pub(super) source: Option<Box<dyn Error + Send + Sync + 'static>>,
}

pub type Result<T> = std::result::Result<T, CliError>;

pub async fn run(args: impl IntoIterator<Item = String>) -> Result<()> {
    let args = Args::parse(args)?;
    if args.command == Command::Version {
        println!("morrow-cli {VERSION}");
        return Ok(());
    }
    let config = CliConfig::load(&args.config_path, !args.config_path_explicit)?;
    run_command(&config, args.command).await
}

pub(super) async fn run_command(config: &CliConfig, command: Command) -> Result<()> {
    match command {
        Command::Version => unreachable!("version is handled before loading configuration"),
        Command::Ping => {
            let mut client = config.connect_client().await?;
            client.ping_roundtrip().await?;
            println!("PONG");
            Ok(())
        }
        Command::Pub {
            subject,
            payload,
            qos,
            msg_id,
        } => {
            let mut client = config.connect_client().await?;
            if let Some(qos) = qos {
                let msg_id = msg_id.expect("parser requires msg-id when qos is set");
                let ack = client
                    .publish_with_qos(&subject, None, &payload, qos, &msg_id)
                    .await?;
                println!(
                    "P-ACK {} {} OK {} {}",
                    ack.msg_id,
                    ack.level as u8,
                    ack.retained,
                    ack.seq
                        .map(|seq| seq.to_string())
                        .unwrap_or_else(|| "-".to_string())
                );
            } else {
                client.publish(&subject, &payload).await?;
            }
            if qos.is_none() && config.connect.verbose {
                expect_ok(&mut client).await?;
            }
            Ok(())
        }
        Command::Request {
            subject,
            payload,
            timeout_ms,
        } => {
            let mut client = config.connect_client().await?;
            let response = client
                .request(&subject, &payload, Duration::from_millis(timeout_ms))
                .await?;
            println!("{}", String::from_utf8_lossy(&response.payload));
            Ok(())
        }
        Command::Reply { subject, queue } => {
            let mut client = config.connect_client().await?;
            match queue {
                Some(queue) => {
                    client
                        .subscribe_queue(&subject, &queue, DEFAULT_SID)
                        .await?
                }
                None => client.subscribe(&subject, DEFAULT_SID).await?,
            }
            client.ping_roundtrip().await?;
            loop {
                let message = client.next_message().await?;
                println!(
                    "{} {} {}",
                    message.subject,
                    message.sid,
                    String::from_utf8_lossy(&message.payload)
                );
                let response = read_stdin_line().await?;
                client.respond(&message, response.as_bytes()).await?;
                if let Some(ack_subject) = &message.ack_subject {
                    client.ack(ack_subject).await?;
                }
            }
        }
        Command::Sub {
            subject,
            sid,
            queue,
            ack,
            max_messages,
        } => {
            let mut client = config.connect_client().await?;
            match queue {
                Some(queue) => client.subscribe_queue(&subject, &queue, &sid).await?,
                None => client.subscribe(&subject, &sid).await?,
            }
            client.ping_roundtrip().await?;
            let mut received = 0_usize;
            loop {
                let message = client.next_message().await?;
                println!(
                    "{} {} {}",
                    message.subject,
                    message.sid,
                    String::from_utf8_lossy(&message.payload)
                );
                if ack {
                    if let Some(ack_subject) = &message.ack_subject {
                        client.ack(ack_subject).await?;
                    }
                }
                received += 1;
                if max_messages.is_some_and(|limit| received >= limit) {
                    return Ok(());
                }
            }
        }
    }
}

pub(super) async fn expect_ok(client: &mut Client) -> Result<()> {
    loop {
        match client.next_frame().await? {
            Some(ServerFrame::Ok) => return Ok(()),
            Some(ServerFrame::Err(err)) => return Err(CliError::msg(err)),
            Some(ServerFrame::Pong) => {}
            Some(frame) => {
                return Err(CliError::msg(format!(
                    "expected +OK after publish, got {frame:?}"
                )));
            }
            None => return Err(CliError::msg("connection closed before +OK")),
        }
    }
}
