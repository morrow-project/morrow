use super::*;

#[tokio::test]
async fn version_two_push_requires_bounded_message_and_byte_credit() {
    let scenario = Scenario::new();
    let mut subscriber = TestClient::connect_pull(scenario.broker(), "client", 25).await;
    let mut publisher = scenario.connect_durable("publisher", 25).await;
    subscriber.subscribe("orders.*", "sid").await;
    subscriber.ping_roundtrip().await;
    publisher.publish("orders.created", b"one").await;
    publisher.publish("orders.created", b"two").await;
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
        .write_line("CONSUMER CREATE worker orders.* @earliest")
        .await;
    client.expect_err_contains("protocol version 2").await;
}

#[tokio::test]
async fn nack_delay_uses_the_durable_lease_deadline() {
    let scenario = Scenario::new();
    let mut consumer = TestClient::connect_pull(scenario.broker(), "puller", 25).await;
    consumer
        .write_line("CONSUMER CREATE worker orders.* @earliest")
        .await;
    assert_eq!(consumer.read_frame().await, "C-OK CREATE worker\r\n");
    let mut publisher = scenario.connect_durable("publisher", 25).await;
    publisher.publish("orders.created", b"one").await;
    publisher.ping_roundtrip().await;

    consumer.write_line("FETCH worker 1 3 0").await;
    assert_eq!(consumer.read_frame().await, "BATCH worker 1 3\r\n");
    assert!(
        consumer
            .read_frame()
            .await
            .starts_with("DMSG worker orders.created - orders 0 0 1 ")
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
            .starts_with("DMSG worker orders.created - orders 0 0 2 ")
    );
}

#[tokio::test]
async fn pull_lease_attempt_survives_restart() {
    let mut scenario = Scenario::new();
    let mut consumer = TestClient::connect_pull(scenario.broker(), "puller", 25).await;
    consumer
        .write_line("CONSUMER CREATE worker orders.* @earliest")
        .await;
    assert_eq!(consumer.read_frame().await, "C-OK CREATE worker\r\n");
    let mut publisher = scenario.connect_durable("publisher", 25).await;
    publisher.publish("orders.created", b"one").await;
    publisher.ping_roundtrip().await;
    consumer.write_line("FETCH worker 1 3 0").await;
    assert_eq!(consumer.read_frame().await, "BATCH worker 1 3\r\n");
    let first = consumer.read_frame().await;
    assert!(first.starts_with("DMSG worker orders.created - orders 0 0 1 "));
    consumer.disconnect().await;
    publisher.disconnect().await;

    scenario.restart_broker().await;
    let mut consumer = TestClient::connect_pull(scenario.broker(), "puller", 25).await;
    consumer.write_line("FETCH worker 1 3 0").await;
    assert_eq!(consumer.read_frame().await, "BATCH worker 1 3\r\n");
    let redelivery = consumer.read_frame().await;
    assert!(redelivery.starts_with("DMSG worker orders.created - orders 0 0 2 "));
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
        .write_line("CONSUMER CREATE worker orders.* @earliest")
        .await;
    assert_eq!(consumer.read_frame().await, "C-OK CREATE worker\r\n");
    let mut publisher = scenario.connect_durable("publisher", 25).await;
    publisher.publish("orders.created", b"one").await;
    publisher.ping_roundtrip().await;

    consumer.write_line("FETCH worker 1 3 0").await;
    assert_eq!(consumer.read_frame().await, "BATCH worker 1 3\r\n");
    let delivery = consumer.read_frame().await;
    assert!(delivery.starts_with("DMSG worker orders.created - orders 0 0 1 "));
    consumer.write_line("ACK worker 1 1").await;
    assert_eq!(consumer.read_frame().await, "D-OK ACK worker 1 1\r\n");

    let inner = scenario.broker().inner.lock().await;
    let consumer = inner.consumers.values().next().unwrap();
    assert!(consumer.in_flight.is_empty());
    assert_eq!(consumer.cursors.committed_offset("orders", 0), Some(1));
}
