use super::*;

#[tokio::test]
async fn version_two_push_requires_bounded_message_and_byte_credit() {
    let scenario = Scenario::new();
    let mut subscriber = TestClient::connect_pull(scenario.broker(), "client", 25).await;
    let mut publisher = scenario.connect_durable("publisher", 25).await;
    subscriber.subscribe("orders/*", "sid").await;
    subscriber.ping_roundtrip().await;
    publisher.publish("orders/created", b"one").await;
    publisher.publish("orders/created", b"two").await;
    subscriber.expect_no_frame_short().await;
    {
        let inner = scenario.broker().inner.lock().await;
        assert!(inner.consumers["durable-client-sid"].in_flight.is_empty());
    }

    subscriber.write_line("CREDIT sid 1 3").await;
    let first = subscriber.expect_hmsg().await;
    assert!(first.ends_with("\r\n\r\none\r\n"));
    subscriber.expect_no_frame_short().await;
    {
        let inner = scenario.broker().inner.lock().await;
        assert_eq!(inner.consumers["durable-client-sid"].in_flight.len(), 1);
    }

    subscriber.write_line("CREDIT sid 1 3").await;
    let second = subscriber.expect_hmsg().await;
    assert!(second.ends_with("\r\n\r\ntwo\r\n"));
}

#[tokio::test]
async fn version_one_receives_clear_pull_compatibility_error() {
    let scenario = Scenario::new();
    let mut client = scenario.connect_durable("legacy", 25).await;
    client
        .write_line("CONSUMER CREATE worker orders/* @earliest")
        .await;
    client.expect_err_contains("protocol version 2").await;
}

#[tokio::test]
async fn nack_delay_uses_the_durable_lease_deadline() {
    let scenario = Scenario::new();
    let mut consumer = TestClient::connect_pull(scenario.broker(), "puller", 25).await;
    consumer
        .write_line("CONSUMER CREATE worker orders/* @earliest")
        .await;
    assert_eq!(consumer.read_frame().await, "C-OK CREATE worker\r\n");
    let mut publisher = scenario.connect_durable("publisher", 25).await;
    publisher.publish("orders/created", b"one").await;
    publisher.ping_roundtrip().await;

    consumer.write_line("FETCH worker 1 3 0").await;
    assert_eq!(consumer.read_frame().await, "BATCH worker 1 3\r\n");
    assert!(
        consumer
            .read_frame()
            .await
            .starts_with("DDELIVER worker orders/created - orders 0 0 - 1000 1 ")
    );
    consumer.write_line("NACK worker 1 1 20").await;
    assert_eq!(consumer.read_frame().await, "D-OK NACK worker 1 1\r\n");

    consumer.write_line("FETCH worker 1 3 0").await;
    assert_eq!(consumer.read_frame().await, "BATCH worker 0 0\r\n");
    scenario.clock.advance_ms(20);
    consumer.write_line("FETCH worker 1 3 0").await;
    assert_eq!(consumer.read_frame().await, "BATCH worker 1 3\r\n");
    assert!(
        consumer
            .read_frame()
            .await
            .starts_with("DDELIVER worker orders/created - orders 0 0 - 1000 2 ")
    );
}

#[tokio::test]
async fn exhausted_pull_delivery_is_written_once_to_dead_letters() {
    let mut scenario = Scenario::new();
    let mut consumer = TestClient::connect_pull(scenario.broker(), "puller", 25).await;
    consumer
        .write_line("CONSUMER CREATE worker orders/* @earliest retry=1:fixed:0:1000:0:dead_letter")
        .await;
    assert_eq!(consumer.read_frame().await, "C-OK CREATE worker\r\n");
    let mut publisher = scenario.connect_durable("publisher", 25).await;
    publisher.publish("orders/created", b"poison").await;
    publisher.ping_roundtrip().await;

    consumer.write_line("FETCH worker 1 16 0").await;
    assert_eq!(consumer.read_frame().await, "BATCH worker 1 6\r\n");
    let first = consumer.read_frame().await;
    assert!(first.contains("poison"));
    scenario.advance_ms(25);
    scenario.tick_redelivery().await;
    consumer.write_line("FETCH worker 1 16 0").await;
    assert_eq!(consumer.read_frame().await, "BATCH worker 0 0\r\n");

    let inner = scenario.broker().inner.lock().await;
    assert_eq!(inner.dead_letters.len(), 1);
    let dead_letter = inner.dead_letters.values().next().unwrap();
    assert_eq!(dead_letter.source_seq, 1);
    assert!(dead_letter.consumer_id.starts_with("pull-"));
    assert_eq!(dead_letter.attempt_count, 1);
    assert!(dead_letter.payload.is_empty());
    let state_consumer = inner.consumers.get(&dead_letter.consumer_id).unwrap();
    assert_eq!(
        state_consumer.cursors.committed_offset("orders", 0),
        Some(1)
    );
    drop(inner);
    consumer.disconnect().await;
    publisher.disconnect().await;
    scenario.restart_broker().await;
    assert_eq!(scenario.broker().inner.lock().await.dead_letters.len(), 1);
}

#[tokio::test]
async fn pull_lease_attempt_survives_restart() {
    let mut scenario = Scenario::new();
    let mut consumer = TestClient::connect_pull(scenario.broker(), "puller", 25).await;
    consumer
        .write_line("CONSUMER CREATE worker orders/* @earliest")
        .await;
    assert_eq!(consumer.read_frame().await, "C-OK CREATE worker\r\n");
    let mut publisher = scenario.connect_durable("publisher", 25).await;
    publisher.publish("orders/created", b"one").await;
    publisher.ping_roundtrip().await;
    consumer.write_line("FETCH worker 1 3 0").await;
    assert_eq!(consumer.read_frame().await, "BATCH worker 1 3\r\n");
    let first = consumer.read_frame().await;
    assert!(first.starts_with("DDELIVER worker orders/created - orders 0 0 - 1000 1 "));
    consumer.disconnect().await;
    publisher.disconnect().await;

    scenario.restart_broker().await;
    let mut consumer = TestClient::connect_pull(scenario.broker(), "puller", 25).await;
    consumer.write_line("FETCH worker 1 3 0").await;
    assert_eq!(consumer.read_frame().await, "BATCH worker 1 3\r\n");
    let redelivery = consumer.read_frame().await;
    assert!(redelivery.starts_with("DDELIVER worker orders/created - orders 0 0 - 1000 2 "));
    consumer.write_line("CONSUMER DELETE worker").await;
    assert_eq!(consumer.read_frame().await, "C-OK DELETE worker\r\n");
    consumer.disconnect().await;
    scenario.restart_broker().await;
    assert!(scenario.broker().inner.lock().await.consumers.is_empty());
}

#[tokio::test]
async fn fake_cluster_replicates_pull_fetch_and_ack() {
    let scenario = Scenario::new_fake_cluster(5);
    let mut consumer = TestClient::connect_pull(scenario.broker(), "puller", 25).await;
    consumer
        .write_line("CONSUMER CREATE worker orders/* @earliest")
        .await;
    assert_eq!(consumer.read_frame().await, "C-OK CREATE worker\r\n");
    let mut publisher = scenario.connect_durable("publisher", 25).await;
    publisher.publish("orders/created", b"one").await;
    publisher.ping_roundtrip().await;

    consumer.write_line("FETCH worker 1 3 0").await;
    assert_eq!(consumer.read_frame().await, "BATCH worker 1 3\r\n");
    let delivery = consumer.read_frame().await;
    assert!(delivery.starts_with("DDELIVER worker orders/created - orders 0 0 - 1000 1 "));
    consumer.write_line("ACK worker 1 1").await;
    assert_eq!(consumer.read_frame().await, "D-OK ACK worker 1 1\r\n");

    let inner = scenario.broker().inner.lock().await;
    let consumer = inner.consumers.values().next().unwrap();
    assert!(consumer.in_flight.is_empty());
    assert_eq!(consumer.cursors.committed_offset("orders", 0), Some(1));
}

#[tokio::test]
async fn fetch_observes_publish_before_and_after_waiter_registration() {
    let scenario = Scenario::new();
    let mut consumer = TestClient::connect_pull(scenario.broker(), "puller", 25).await;
    consumer
        .write_line("CONSUMER CREATE worker orders/* @earliest")
        .await;
    assert_eq!(consumer.read_frame().await, "C-OK CREATE worker\r\n");
    let mut publisher = scenario.connect_durable("publisher", 25).await;

    publisher.publish("orders/created", b"one").await;
    publisher.ping_roundtrip().await;
    consumer.write_line("FETCH worker 1 3 5000").await;
    assert_eq!(
        tokio::time::timeout(Duration::from_millis(100), consumer.read_frame())
            .await
            .unwrap(),
        "BATCH worker 1 3\r\n"
    );
    let first = consumer.read_frame().await;
    assert!(first.ends_with("\r\n\r\none\r\n"));

    consumer.write_line("FETCH worker 1 3 5000").await;
    wait_for_waiter_count(scenario.broker(), 1).await;
    publisher.publish("orders/created", b"two").await;
    publisher.ping_roundtrip().await;
    assert_eq!(
        tokio::time::timeout(Duration::from_millis(100), consumer.read_frame())
            .await
            .unwrap(),
        "BATCH worker 1 3\r\n"
    );
    let second = consumer.read_frame().await;
    assert!(second.ends_with("\r\n\r\ntwo\r\n"));
}

#[tokio::test]
async fn idle_fetch_waiters_have_no_periodic_state_checks_and_disconnect_promptly() {
    let scenario = Scenario::new();
    let mut consumers = Vec::new();
    for index in 0..16 {
        let mut consumer =
            TestClient::connect_pull(scenario.broker(), &format!("puller-{index}"), 25).await;
        consumer
            .write_line("CONSUMER CREATE worker orders/* @earliest")
            .await;
        assert_eq!(consumer.read_frame().await, "C-OK CREATE worker\r\n");
        consumer.write_line("FETCH worker 1 3 5000").await;
        consumers.push(consumer);
    }
    wait_for_waiter_count(scenario.broker(), consumers.len()).await;
    let checks = scenario.broker().pull_waiters.fetch_check_count();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(scenario.broker().pull_waiters.fetch_check_count(), checks);

    for consumer in consumers {
        consumer.disconnect().await;
    }
    wait_for_waiter_count(scenario.broker(), 0).await;
}

#[tokio::test]
async fn deleting_a_consumer_cancels_its_waiter() {
    let scenario = Scenario::new();
    let mut consumer = TestClient::connect_pull(scenario.broker(), "puller", 25).await;
    consumer
        .write_line("CONSUMER CREATE worker orders/* @earliest")
        .await;
    assert_eq!(consumer.read_frame().await, "C-OK CREATE worker\r\n");
    consumer.write_line("FETCH worker 1 3 5000").await;
    wait_for_waiter_count(scenario.broker(), 1).await;

    consumer.write_line("CONSUMER DELETE worker").await;
    wait_for_waiter_count(scenario.broker(), 0).await;
    let first = tokio::time::timeout(Duration::from_millis(100), consumer.read_frame())
        .await
        .unwrap();
    let second = tokio::time::timeout(Duration::from_millis(100), consumer.read_frame())
        .await
        .unwrap();
    assert!(
        [first.as_str(), second.as_str()].contains(&"C-OK DELETE worker\r\n"),
        "missing delete acknowledgement: {first:?}, {second:?}"
    );
    assert!(
        first.contains("unknown consumer")
            || first.contains("FETCH cancelled")
            || second.contains("unknown consumer")
            || second.contains("FETCH cancelled"),
        "missing fetch cancellation: {first:?}, {second:?}"
    );
}

#[tokio::test]
async fn fetch_deadline_returns_an_empty_batch_and_removes_the_waiter() {
    let scenario = Scenario::new();
    let mut consumer = TestClient::connect_pull(scenario.broker(), "puller", 25).await;
    consumer
        .write_line("CONSUMER CREATE worker orders/* @earliest")
        .await;
    assert_eq!(consumer.read_frame().await, "C-OK CREATE worker\r\n");

    consumer.write_line("FETCH worker 1 3 20").await;
    assert_eq!(
        tokio::time::timeout(Duration::from_millis(200), consumer.read_frame())
            .await
            .unwrap(),
        "BATCH worker 0 0\r\n"
    );
    wait_for_waiter_count(scenario.broker(), 0).await;
}

#[test]
fn pull_waiter_registry_bounds_connections_and_consumers() {
    let registry = PullWaiterRegistry::default();
    let first = registry.register(1, "consumer-a", "orders/*").unwrap();
    assert!(
        registry
            .register(1, "consumer-b", "orders/*")
            .err()
            .unwrap()
            .to_string()
            .contains("connection")
    );
    drop(first);

    let mut waiters = Vec::new();
    for connection_id in 1..=64 {
        waiters.push(
            registry
                .register(connection_id, "consumer-a", "orders/*")
                .unwrap(),
        );
    }
    assert!(
        registry
            .register(65, "consumer-a", "orders/*")
            .err()
            .unwrap()
            .to_string()
            .contains("consumer")
    );
    registry.shutdown();
    assert!(waiters.iter().all(PullWaiter::is_cancelled));
    assert!(
        registry
            .register(65, "consumer-b", "orders/*")
            .err()
            .unwrap()
            .to_string()
            .contains("shutting down")
    );
}

async fn wait_for_waiter_count(broker: &Morrow, expected: usize) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    while tokio::time::Instant::now() < deadline {
        if broker.pull_waiters.waiter_count() == expected {
            return;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    panic!(
        "expected {expected} pull waiters, got {}",
        broker.pull_waiters.waiter_count()
    );
}
