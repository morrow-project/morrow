use std::{
    collections::{BTreeSet, HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use tokio::{
    io::{AsyncRead, AsyncWrite, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::{Mutex, mpsc},
};
use tokio_rustls::TlsAcceptor;

use crate::{
    config::Config,
    error::{BrokerError, Result, ResultExt},
    protocol::{self, AckSubject, Command},
    subject,
    wal::{ConsumerRecord, PublishRecord, ReplayedConsumer, Wal},
};

const DEFAULT_ACK_TIMEOUT_MS: u64 = 30_000;
const DEFAULT_MAX_IN_FLIGHT: usize = 1024;
const REDELIVERY_SCAN_INTERVAL_MS: u64 = 50;

#[derive(Clone)]
pub struct Broker {
    inner: Arc<Mutex<Inner>>,
    next_connection_id: Arc<AtomicU64>,
    config: Config,
    tls_acceptor: Option<TlsAcceptor>,
}

struct Inner {
    wal: Wal,
    clients: HashMap<u64, Client>,
    consumers: HashMap<String, Consumer>,
    messages: HashMap<u64, PublishRecord>,
}

struct Client {
    sender: mpsc::Sender<Vec<u8>>,
    verbose: bool,
    durable_id: Option<String>,
    ack_timeout_ms: u64,
    max_in_flight: usize,
}

#[derive(Debug, Clone)]
struct Consumer {
    record: ConsumerRecord,
    members: HashMap<u64, String>,
    pending: BTreeSet<u64>,
    pending_attempts: HashMap<u64, u32>,
    in_flight: HashMap<u64, InFlight>,
    acked: HashSet<u64>,
    delivered: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct InFlight {
    delivery_id: u64,
    deadline_ms: u64,
    attempt: u32,
}

struct Delivery {
    sender: mpsc::Sender<Vec<u8>>,
    frame: Vec<u8>,
}

impl Broker {
    pub fn open(config: Config) -> Result<Self> {
        config.validate()?;
        let (wal, replay) = Wal::open(&config.wal_dir, config.fsync_interval())?;
        let tls_acceptor = config
            .tls
            .as_ref()
            .map(crate::tls::load_acceptor)
            .transpose()?;
        let consumers = replay
            .consumers
            .into_iter()
            .map(|(id, consumer)| (id, Consumer::from_replay(consumer)))
            .collect();
        Ok(Self {
            inner: Arc::new(Mutex::new(Inner {
                wal,
                clients: HashMap::new(),
                consumers,
                messages: replay.messages,
            })),
            next_connection_id: Arc::new(AtomicU64::new(1)),
            config,
            tls_acceptor,
        })
    }

    pub async fn serve(self) -> Result<()> {
        let listener = TcpListener::bind(self.config.listen)
            .await
            .with_context(|| format!("binding {}", self.config.listen))?;
        let redeliver = self.clone();
        tokio::spawn(async move {
            redeliver.redelivery_loop().await;
        });

        loop {
            tokio::select! {
                accepted = listener.accept() => {
                    let (stream, _) = accepted.context("accepting client connection")?;
                    let broker = self.clone();
                    tokio::spawn(async move {
                        if let Err(err) = broker.handle_accepted(stream).await {
                            eprintln!("client error: {err:#}");
                        }
                    });
                }
                signal = tokio::signal::ctrl_c() => {
                    signal.context("waiting for shutdown signal")?;
                    self.shutdown().await?;
                    return Ok(());
                }
            }
        }
    }

    pub async fn shutdown(&self) -> Result<()> {
        let mut inner = self.inner.lock().await;
        inner.wal.flush()?;
        Ok(())
    }

    async fn handle_accepted(&self, stream: TcpStream) -> Result<()> {
        if let Some(acceptor) = &self.tls_acceptor {
            let timeout_ms = self
                .config
                .tls
                .as_ref()
                .map(|tls| tls.handshake_timeout_ms)
                .unwrap_or(2_000);
            let stream =
                tokio::time::timeout(Duration::from_millis(timeout_ms), acceptor.accept(stream))
                    .await
                    .map_err(|_| BrokerError::msg("TLS handshake timed out"))?
                    .context("accepting TLS client connection")?;
            self.handle_client(stream).await
        } else {
            self.handle_client(stream).await
        }
    }

    async fn handle_client<S>(&self, stream: S) -> Result<()>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let id = self.next_connection_id.fetch_add(1, Ordering::Relaxed);
        let (reader, mut writer) = tokio::io::split(stream);
        let (sender, mut receiver) = mpsc::channel::<Vec<u8>>(256);
        self.add_client(id, sender).await;

        writer
            .write_all(&protocol::info_line(self.config.max_payload))
            .await?;
        let writer_task = tokio::spawn(async move {
            while let Some(frame) = receiver.recv().await {
                writer.write_all(&frame).await?;
            }
            Ok::<(), BrokerError>(())
        });

        let mut reader = BufReader::new(reader);
        loop {
            match protocol::read_command(&mut reader, self.config.max_payload).await {
                Ok(Some(command)) => {
                    if let Err(err) = self.handle_command(id, command).await {
                        let _ = self.send_to(id, protocol::err(&err.to_string())).await;
                    }
                }
                Ok(None) => break,
                Err(err) => {
                    let _ = self.send_to(id, protocol::err(&err.to_string())).await;
                    break;
                }
            }
        }

        self.remove_client(id).await?;
        writer_task.abort();
        Ok(())
    }

    async fn handle_command(&self, connection_id: u64, command: Command) -> Result<()> {
        match command {
            Command::Connect {
                verbose,
                durable_id,
                ack_timeout_ms,
                max_in_flight,
            } => {
                self.configure_client(
                    connection_id,
                    verbose,
                    durable_id,
                    ack_timeout_ms,
                    max_in_flight,
                )
                .await
            }
            Command::Ping => self.send_to(connection_id, protocol::pong().to_vec()).await,
            Command::Pong => Ok(()),
            Command::Sub {
                subject,
                queue,
                sid,
            } => self.subscribe(connection_id, subject, queue, sid).await,
            Command::Unsub { sid, max_messages } => {
                self.unsubscribe(connection_id, &sid, max_messages).await
            }
            Command::Pub {
                subject,
                reply_to,
                payload,
            } => {
                self.publish(connection_id, subject, reply_to, payload)
                    .await
            }
        }
    }

    async fn add_client(&self, id: u64, sender: mpsc::Sender<Vec<u8>>) {
        let mut inner = self.inner.lock().await;
        inner.clients.insert(
            id,
            Client {
                sender,
                verbose: self.config.verbose,
                durable_id: None,
                ack_timeout_ms: DEFAULT_ACK_TIMEOUT_MS,
                max_in_flight: DEFAULT_MAX_IN_FLIGHT,
            },
        );
    }

    async fn configure_client(
        &self,
        id: u64,
        verbose: bool,
        durable_id: Option<String>,
        ack_timeout_ms: Option<u64>,
        max_in_flight: Option<usize>,
    ) -> Result<()> {
        let mut inner = self.inner.lock().await;
        let client = inner
            .clients
            .get_mut(&id)
            .ok_or_else(|| BrokerError::msg("unknown connection"))?;
        client.verbose = verbose || self.config.verbose;
        client.durable_id = durable_id;
        client.ack_timeout_ms = ack_timeout_ms.unwrap_or(DEFAULT_ACK_TIMEOUT_MS);
        client.max_in_flight = max_in_flight.unwrap_or(DEFAULT_MAX_IN_FLIGHT);
        crate::broker_ensure!(
            client.ack_timeout_ms > 0,
            "ack_timeout_ms must be greater than zero"
        );
        crate::broker_ensure!(
            client.max_in_flight > 0,
            "max_in_flight must be greater than zero"
        );
        Ok(())
    }

    async fn subscribe(
        &self,
        connection_id: u64,
        sub_subject: String,
        queue: Option<String>,
        sid: String,
    ) -> Result<()> {
        crate::broker_ensure!(
            subject::validate_subscription(&sub_subject),
            "invalid subscription subject"
        );
        protocol::validate_identifier("sid", &sid)?;
        if let Some(queue) = &queue {
            protocol::validate_identifier("queue group", queue)?;
        }

        let should_deliver = {
            let mut inner = self.inner.lock().await;
            let client = inner
                .clients
                .get(&connection_id)
                .ok_or_else(|| BrokerError::msg("unknown connection"))?;
            if let Some(durable_id) = &client.durable_id {
                let consumer_id = consumer_id(durable_id, queue.as_deref(), &sub_subject, &sid);
                let record = ConsumerRecord {
                    consumer_id: consumer_id.clone(),
                    filter_subject: sub_subject,
                    queue_group: queue,
                    ack_timeout_ms: client.ack_timeout_ms,
                    max_in_flight: client.max_in_flight,
                };
                inner.wal.append_consumer_upsert(&record)?;
                inner.wal.flush_due()?;
                let consumer = inner
                    .consumers
                    .entry(consumer_id)
                    .or_insert_with(|| Consumer {
                        record: record.clone(),
                        members: HashMap::new(),
                        pending: BTreeSet::new(),
                        pending_attempts: HashMap::new(),
                        in_flight: HashMap::new(),
                        acked: HashSet::new(),
                        delivered: 0,
                    });
                consumer.record = record;
                consumer.members.insert(connection_id, sid);
                true
            } else {
                crate::broker_bail!("CONNECT durable_id is required before SUB")
            }
        };

        if should_deliver {
            self.deliver_pending().await?;
        }
        Ok(())
    }

    async fn unsubscribe(
        &self,
        connection_id: u64,
        sid: &str,
        max_messages: Option<usize>,
    ) -> Result<()> {
        let mut inner = self.inner.lock().await;
        let mut found = false;
        for consumer in inner.consumers.values_mut() {
            if consumer
                .members
                .get(&connection_id)
                .is_some_and(|member_sid| member_sid == sid)
            {
                consumer.members.remove(&connection_id);
                found = true;
            }
        }
        if let Some(max_messages) = max_messages {
            crate::broker_ensure!(
                max_messages > 0,
                "durable UNSUB max_messages must be greater than zero"
            );
        }
        crate::broker_ensure!(found, "unknown sid");
        Ok(())
    }

    async fn publish(
        &self,
        publisher_id: u64,
        subject_name: String,
        reply_to: Option<String>,
        payload: Vec<u8>,
    ) -> Result<()> {
        if let Some(ack) = protocol::parse_ack_subject(&subject_name) {
            self.ack(ack).await?;
            self.send_verbose_ok(publisher_id).await?;
            return Ok(());
        }
        crate::broker_ensure!(
            !subject_name.starts_with("_BROKER."),
            "reserved broker subject"
        );
        crate::broker_ensure!(
            subject::validate_subject(&subject_name),
            "invalid publish subject"
        );
        crate::broker_ensure!(
            payload.len() <= self.config.max_payload,
            "payload exceeds max payload"
        );

        let (has_durable, verbose) = {
            let mut inner = self.inner.lock().await;
            let verbose = inner
                .clients
                .get(&publisher_id)
                .map(|client| client.verbose)
                .unwrap_or(self.config.verbose);
            let matching_consumers: Vec<String> = inner
                .consumers
                .iter()
                .filter(|(_, consumer)| {
                    subject::matches(&consumer.record.filter_subject, &subject_name)
                })
                .map(|(consumer_id, _)| consumer_id.clone())
                .collect();
            let has_durable = !matching_consumers.is_empty();
            let record = if has_durable {
                let record =
                    inner
                        .wal
                        .append_publish(&subject_name, reply_to.as_deref(), &payload)?;
                for consumer_id in matching_consumers {
                    if let Some(consumer) = inner.consumers.get_mut(&consumer_id) {
                        consumer.pending.insert(record.seq);
                    }
                }
                inner.messages.insert(record.seq, record);
                true
            } else {
                inner.wal.flush_due()?;
                false
            };

            (record, verbose)
        };

        if has_durable {
            tokio::time::sleep(self.config.fsync_interval()).await;
            let mut inner = self.inner.lock().await;
            inner.wal.flush()?;
        }

        self.deliver_pending().await?;

        if verbose {
            self.send_to(publisher_id, protocol::ok().to_vec()).await?;
        }

        Ok(())
    }

    async fn ack(&self, ack: AckSubject) -> Result<()> {
        let mut inner = self.inner.lock().await;
        let mut should_cleanup = false;
        let valid = inner
            .consumers
            .get(&ack.consumer_id)
            .and_then(|consumer| consumer.in_flight.get(&ack.seq))
            .is_some_and(|in_flight| in_flight.delivery_id == ack.delivery_id);
        if valid {
            inner
                .wal
                .append_ack(ack.seq, &ack.consumer_id, ack.delivery_id)?;
            let consumer = inner.consumers.get_mut(&ack.consumer_id).unwrap();
            consumer.in_flight.remove(&ack.seq);
            consumer.pending.remove(&ack.seq);
            consumer.pending_attempts.remove(&ack.seq);
            consumer.acked.insert(ack.seq);
            should_cleanup = true;
        }
        inner.wal.flush_due()?;
        if should_cleanup {
            inner.cleanup_acked_messages();
        }
        Ok(())
    }

    async fn send_verbose_ok(&self, publisher_id: u64) -> Result<()> {
        let verbose = {
            let inner = self.inner.lock().await;
            inner
                .clients
                .get(&publisher_id)
                .map(|client| client.verbose)
                .unwrap_or(self.config.verbose)
        };
        if verbose {
            self.send_to(publisher_id, protocol::ok().to_vec()).await?;
        }
        Ok(())
    }

    async fn deliver_pending(&self) -> Result<()> {
        let deliveries = {
            let mut inner = self.inner.lock().await;
            inner.prepare_durable_deliveries()?
        };

        for delivery in deliveries {
            let _ = delivery.sender.send(delivery.frame).await;
        }
        Ok(())
    }

    async fn redelivery_loop(self) {
        let mut interval =
            tokio::time::interval(Duration::from_millis(REDELIVERY_SCAN_INTERVAL_MS));
        loop {
            interval.tick().await;
            if let Err(err) = self.expire_and_redeliver().await {
                eprintln!("redelivery error: {err:#}");
            }
        }
    }

    async fn expire_and_redeliver(&self) -> Result<()> {
        {
            let mut inner = self.inner.lock().await;
            let now = now_ms();
            for consumer in inner.consumers.values_mut() {
                let expired: Vec<_> = consumer
                    .in_flight
                    .iter()
                    .filter(|(_, in_flight)| in_flight.deadline_ms <= now)
                    .map(|(seq, _)| *seq)
                    .collect();
                for seq in expired {
                    if let Some(in_flight) = consumer.in_flight.remove(&seq) {
                        consumer.pending.insert(seq);
                        consumer
                            .pending_attempts
                            .insert(seq, in_flight.attempt.saturating_add(1));
                    }
                }
            }
        }
        self.deliver_pending().await
    }

    async fn send_to(&self, connection_id: u64, frame: Vec<u8>) -> Result<()> {
        let sender = {
            let inner = self.inner.lock().await;
            inner
                .clients
                .get(&connection_id)
                .map(|client| client.sender.clone())
                .ok_or_else(|| BrokerError::msg("unknown connection"))?
        };
        sender
            .send(frame)
            .await
            .map_err(|_| BrokerError::msg("connection closed"))
    }

    async fn remove_client(&self, connection_id: u64) -> Result<()> {
        let mut inner = self.inner.lock().await;
        inner.clients.remove(&connection_id);
        for consumer in inner.consumers.values_mut() {
            consumer.members.remove(&connection_id);
        }
        Ok(())
    }
}

impl Inner {
    fn prepare_durable_deliveries(&mut self) -> Result<Vec<Delivery>> {
        let now = now_ms();
        let mut deliveries = Vec::new();
        let consumer_ids: Vec<_> = self.consumers.keys().cloned().collect();
        for consumer_id in consumer_ids {
            loop {
                let Some((seq, connection_id, sid, attempt, deadline_ms)) =
                    self.next_delivery_for(&consumer_id, now)
                else {
                    break;
                };
                let Some(message) = self.messages.get(&seq).cloned() else {
                    if let Some(consumer) = self.consumers.get_mut(&consumer_id) {
                        consumer.pending.remove(&seq);
                    }
                    continue;
                };
                let delivery =
                    self.wal
                        .append_delivery_attempt(seq, &consumer_id, deadline_ms, attempt)?;
                let ack_subject = protocol::ack_subject(&consumer_id, seq, delivery.delivery_id);
                if let Some(consumer) = self.consumers.get_mut(&consumer_id) {
                    consumer.pending.remove(&seq);
                    consumer.pending_attempts.remove(&seq);
                    consumer.in_flight.insert(
                        seq,
                        InFlight {
                            delivery_id: delivery.delivery_id,
                            deadline_ms: delivery.deadline_ms,
                            attempt: delivery.attempt,
                        },
                    );
                    consumer.delivered += 1;
                }
                if let Some(client) = self.clients.get(&connection_id) {
                    deliveries.push(Delivery {
                        sender: client.sender.clone(),
                        frame: protocol::msg(
                            &message.subject,
                            &sid,
                            Some(&ack_subject),
                            &message.payload,
                        ),
                    });
                }
            }
        }
        self.wal.flush_due()?;
        Ok(deliveries)
    }

    fn next_delivery_for(
        &self,
        consumer_id: &str,
        now: u64,
    ) -> Option<(u64, u64, String, u32, u64)> {
        let consumer = self.consumers.get(consumer_id)?;
        if consumer.in_flight.len() >= consumer.record.max_in_flight || consumer.members.is_empty()
        {
            return None;
        }
        let seq = *consumer.pending.iter().next()?;
        let (connection_id, sid) = consumer
            .members
            .iter()
            .filter(|(connection_id, _)| self.clients.contains_key(connection_id))
            .min_by_key(|(connection_id, _)| **connection_id)?;
        let attempt = consumer.pending_attempts.get(&seq).copied().unwrap_or(1);
        let deadline_ms = now.saturating_add(consumer.record.ack_timeout_ms);
        Some((seq, *connection_id, sid.clone(), attempt, deadline_ms))
    }

    fn cleanup_acked_messages(&mut self) {
        let removable: Vec<_> = self
            .messages
            .iter()
            .filter(|(seq, _)| {
                let mut interested = false;
                for consumer in self.consumers.values() {
                    if consumer.pending.contains(seq)
                        || consumer.in_flight.contains_key(seq)
                        || consumer.acked.contains(seq)
                    {
                        interested = true;
                        if !consumer.acked.contains(seq) {
                            return false;
                        }
                    }
                }
                interested
            })
            .map(|(seq, _)| *seq)
            .collect();
        for seq in removable {
            self.messages.remove(&seq);
        }
    }
}

impl Consumer {
    fn from_replay(replay: ReplayedConsumer) -> Self {
        Self {
            record: replay.record,
            members: HashMap::new(),
            pending: replay.pending,
            pending_attempts: HashMap::new(),
            in_flight: replay
                .in_flight
                .into_iter()
                .map(|(seq, attempt)| {
                    (
                        seq,
                        InFlight {
                            delivery_id: attempt.delivery_id,
                            deadline_ms: attempt.deadline_ms,
                            attempt: attempt.attempt,
                        },
                    )
                })
                .collect(),
            acked: replay.acked,
            delivered: 0,
        }
    }
}

fn consumer_id(durable_id: &str, queue: Option<&str>, subject: &str, sid: &str) -> String {
    match queue {
        Some(queue) => format!("queue-{queue}-{}", hex(subject.as_bytes())),
        None => format!("durable-{durable_id}-{sid}"),
    }
}

fn hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use std::{
        path::Path,
        sync::atomic::{AtomicU64, Ordering},
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    use tokio::sync::mpsc;

    use super::*;

    static TEST_DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TestDir(std::path::PathBuf);

    impl TestDir {
        fn new() -> Self {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let counter = TEST_DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "broker-test-{}-{nanos}-{counter}",
                std::process::id(),
            ));
            std::fs::create_dir_all(&path).unwrap();
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn test_config(dir: &std::path::Path) -> Config {
        Config {
            listen: "127.0.0.1:0".parse().unwrap(),
            wal_dir: dir.to_path_buf(),
            fsync_interval_ms: 1,
            max_payload: 1024,
            verbose: false,
            tls: None,
        }
    }

    async fn durable_client(
        broker: &Broker,
        connection_id: u64,
        durable_id: &str,
    ) -> mpsc::Receiver<Vec<u8>> {
        let (tx, rx) = mpsc::channel(8);
        broker.add_client(connection_id, tx).await;
        broker
            .configure_client(
                connection_id,
                false,
                Some(durable_id.into()),
                Some(25),
                Some(1024),
            )
            .await
            .unwrap();
        rx
    }

    #[tokio::test]
    async fn sub_requires_durable_connect() {
        let dir = TestDir::new();
        let broker = Broker::open(test_config(dir.path())).unwrap();
        let (tx, _rx) = mpsc::channel(8);
        broker.add_client(1, tx).await;
        let err = broker
            .subscribe(1, "orders.*".into(), None, "sid1".into())
            .await
            .unwrap_err();
        assert!(err.to_string().contains("durable_id is required"));
    }

    #[tokio::test]
    async fn publish_without_matching_durable_consumer_is_not_retained() {
        let dir = TestDir::new();
        let broker = Broker::open(test_config(dir.path())).unwrap();
        broker
            .publish(2, "orders.created".into(), None, b"hello".to_vec())
            .await
            .unwrap();
        assert!(broker.inner.lock().await.messages.is_empty());
    }

    #[tokio::test]
    async fn durable_delivery_requires_ack() {
        let dir = TestDir::new();
        let broker = Broker::open(test_config(dir.path())).unwrap();
        let mut rx = durable_client(&broker, 1, "client1").await;
        broker
            .subscribe(1, "orders.*".into(), None, "sid1".into())
            .await
            .unwrap();
        broker
            .publish(2, "orders.created".into(), None, b"hello".to_vec())
            .await
            .unwrap();

        let frame = tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .unwrap()
            .unwrap();
        let text = String::from_utf8(frame).unwrap();
        assert!(text.starts_with("MSG orders.created sid1 _BROKER.ACK.durable-client1-sid1."));
        assert!(text.ends_with("5\r\nhello\r\n"));

        let ack_subject = text.split_whitespace().nth(3).unwrap().to_string();
        broker
            .publish(2, ack_subject, None, Vec::new())
            .await
            .unwrap();
        assert!(
            broker
                .inner
                .lock()
                .await
                .consumers
                .get("durable-client1-sid1")
                .unwrap()
                .in_flight
                .is_empty()
        );
    }

    #[tokio::test]
    async fn unacked_message_redelivers_after_timeout() {
        let dir = TestDir::new();
        let broker = Broker::open(test_config(dir.path())).unwrap();
        let mut rx = durable_client(&broker, 1, "client1").await;
        broker
            .subscribe(1, "orders.*".into(), None, "sid1".into())
            .await
            .unwrap();
        broker
            .publish(2, "orders.created".into(), None, b"hello".to_vec())
            .await
            .unwrap();
        rx.recv().await.unwrap();

        tokio::time::sleep(Duration::from_millis(30)).await;
        broker.expire_and_redeliver().await.unwrap();
        let redelivery = tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(String::from_utf8(redelivery).unwrap().contains("hello"));
    }

    #[tokio::test]
    async fn durable_queue_group_delivers_one_copy() {
        let dir = TestDir::new();
        let broker = Broker::open(test_config(dir.path())).unwrap();
        let mut rx1 = durable_client(&broker, 1, "client1").await;
        let mut rx2 = durable_client(&broker, 2, "client2").await;
        broker
            .subscribe(1, "orders.*".into(), Some("workers".into()), "a".into())
            .await
            .unwrap();
        broker
            .subscribe(2, "orders.*".into(), Some("workers".into()), "b".into())
            .await
            .unwrap();
        broker
            .publish(3, "orders.created".into(), None, b"hello".to_vec())
            .await
            .unwrap();

        let first = rx1.try_recv().ok();
        let second = rx2.try_recv().ok();
        assert_eq!(first.is_some() as usize + second.is_some() as usize, 1);
    }

    #[tokio::test]
    async fn acked_message_does_not_redeliver_after_restart() {
        let dir = TestDir::new();
        {
            let broker = Broker::open(test_config(dir.path())).unwrap();
            let mut rx = durable_client(&broker, 1, "client1").await;
            broker
                .subscribe(1, "orders.*".into(), None, "sid1".into())
                .await
                .unwrap();
            broker
                .publish(2, "orders.created".into(), None, b"hello".to_vec())
                .await
                .unwrap();
            let frame = String::from_utf8(rx.recv().await.unwrap()).unwrap();
            let ack_subject = frame.split_whitespace().nth(3).unwrap().to_string();
            broker
                .publish(2, ack_subject, None, Vec::new())
                .await
                .unwrap();
            broker.shutdown().await.unwrap();
        }

        let broker = Broker::open(test_config(dir.path())).unwrap();
        let mut rx = durable_client(&broker, 1, "client1").await;
        broker
            .subscribe(1, "orders.*".into(), None, "sid1".into())
            .await
            .unwrap();
        assert!(rx.try_recv().is_err());
    }
}
