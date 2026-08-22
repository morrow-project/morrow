use super::*;

#[tokio::test]
async fn connect_rejects_server_limit_overflow_before_configuring_client() {
    let dir = TempDir::new().unwrap();
    let clock = Arc::new(ManualClock::new(1_000));
    let mut config = test_config(dir.path());
    config.max_ack_timeout_ms = 25;
    config.max_in_flight = 1_024;
    let broker = deterministic_broker(config, clock, None);

    let mut timeout = TestClient::connect(&broker).await;
    timeout
        .write_line(r#"CONN {"ack_timeout_ms":26,"max_in_flight":1}"#)
        .await;
    timeout
        .expect_err_contains("CONN ack_timeout_ms exceeds server limit 25")
        .await;

    let mut window = TestClient::connect(&broker).await;
    window
        .write_line(r#"CONN {"ack_timeout_ms":25,"max_in_flight":1025}"#)
        .await;
    window
        .expect_err_contains("CONN max_in_flight exceeds server limit 1024")
        .await;

    let connections = broker.connections.lock().await;
    assert_eq!(
        connections
            .clients
            .values()
            .filter(|client| client.configured)
            .count(),
        0
    );
}

#[tokio::test]
async fn fetch_limits_reject_without_creating_leases_and_allow_the_boundary() {
    let dir = TempDir::new().unwrap();
    let clock = Arc::new(ManualClock::new(1_000));
    let mut config = test_config(dir.path());
    config.max_fetch_messages = 1;
    config.max_fetch_bytes = 3;
    config.max_encoded_batch_bytes = 1_024;
    let broker = deterministic_broker(config, clock, None);
    let mut consumer = TestClient::connect_pull(&broker, "puller", 25).await;
    consumer
        .write_line("CONSUMER CREATE worker orders/* @earliest")
        .await;
    assert_eq!(consumer.read_frame().await, "C-OK CREATE worker\r\n");
    let mut publisher = TestClient::connect_durable(&broker, "publisher", 25).await;
    publisher.publish("orders/created", b"one").await;
    publisher.ping_roundtrip().await;

    consumer.write_line("FETCH worker 2 3 0").await;
    consumer
        .expect_err_contains("FETCH max messages exceeds server limit 1")
        .await;
    consumer.write_line("FETCH worker 1 4 0").await;
    consumer
        .expect_err_contains("FETCH max bytes exceeds server limit 3")
        .await;
    let consumer_id = "pull-70756c6c6572-776f726b6572";
    assert!(
        broker.inner.lock().await.consumers[consumer_id]
            .in_flight
            .is_empty()
    );

    consumer.write_line("FETCH worker 1 3 0").await;
    assert_eq!(consumer.read_frame().await, "BATCH worker 1 3\r\n");
    assert!(consumer.read_frame().await.starts_with("DDELIVER worker "));
}

#[tokio::test]
async fn encoded_batch_limit_rejects_before_creating_leases() {
    let dir = TempDir::new().unwrap();
    let clock = Arc::new(ManualClock::new(1_000));
    let mut config = test_config(dir.path());
    config.max_payload = 16;
    config.max_encoded_batch_bytes = 16;
    let broker = deterministic_broker(config, clock, None);
    let mut consumer = TestClient::connect_pull(&broker, "puller", 25).await;
    consumer
        .write_line("CONSUMER CREATE worker orders/* @earliest")
        .await;
    assert_eq!(consumer.read_frame().await, "C-OK CREATE worker\r\n");
    let mut publisher = TestClient::connect_durable(&broker, "publisher", 25).await;
    publisher.publish("orders/created", b"one").await;
    publisher.ping_roundtrip().await;

    consumer.write_line("FETCH worker 1 3 0").await;
    consumer
        .expect_err_contains("FETCH encoded batch exceeds server limit")
        .await;
    let consumer_id = "pull-70756c6c6572-776f726b6572";
    assert!(
        broker.inner.lock().await.consumers[consumer_id]
            .in_flight
            .is_empty()
    );
}

#[tokio::test]
async fn nack_and_extend_reject_deadlines_above_the_server_limit() {
    let dir = TempDir::new().unwrap();
    let clock = Arc::new(ManualClock::new(1_000));
    let mut config = test_config(dir.path());
    config.max_ack_timeout_ms = 25;
    let broker = deterministic_broker(config, clock, None);
    let mut consumer = TestClient::connect_pull(&broker, "puller", 25).await;
    consumer
        .write_line("CONSUMER CREATE worker orders/* @earliest")
        .await;
    assert_eq!(consumer.read_frame().await, "C-OK CREATE worker\r\n");
    let mut publisher = TestClient::connect_durable(&broker, "publisher", 25).await;
    publisher.publish("orders/created", b"one").await;
    publisher.ping_roundtrip().await;
    consumer.write_line("FETCH worker 1 3 0").await;
    assert_eq!(consumer.read_frame().await, "BATCH worker 1 3\r\n");
    let delivery = consumer.read_frame().await;
    assert!(delivery.starts_with("DDELIVER worker "));

    consumer.write_line("NACK worker 1 1 26").await;
    consumer
        .expect_err_contains("NACK delay exceeds server limit 25")
        .await;
    consumer.write_line("EXTEND worker 1 1 26").await;
    consumer
        .expect_err_contains("EXTEND duration exceeds server limit 25")
        .await;

    let consumer_id = "pull-70756c6c6572-776f726b6572";
    assert_eq!(
        broker.inner.lock().await.consumers[consumer_id].in_flight[&1].deadline_ms,
        1_025
    );
}
