use super::*;

#[tokio::test]
async fn accepted_qos_acks_without_wal_retention() {
    let scenario = Scenario::new();
    let mut subscriber = scenario.connect_durable("client1", 25).await;
    let mut publisher = scenario.connect_durable("publisher1", 25).await;

    subscriber.subscribe("orders.*", "sid1").await;
    subscriber.ping_roundtrip().await;
    publisher
        .publish_qos(
            "orders.created",
            b"hello",
            protocol::AckLevel::Accepted,
            "msg-accepted",
        )
        .await;

    publisher
        .expect_producer_ack("msg-accepted", 0, false, "-")
        .await;
    subscriber.expect_no_frame_short().await;
    let inner = scenario.broker().inner.lock().await;
    assert!(inner.messages.is_empty());
    assert!(inner.consumers["durable-client1-sid1"].pending.is_empty());
}

#[tokio::test]
async fn durable_qos_acks_after_local_wal_append() {
    let scenario = Scenario::new();
    let mut subscriber = scenario.connect_durable("client1", 25).await;
    let mut publisher = scenario.connect_durable("publisher1", 25).await;

    subscriber.subscribe("orders.*", "sid1").await;
    subscriber.ping_roundtrip().await;
    publisher
        .publish_qos(
            "orders.created",
            b"hello",
            protocol::AckLevel::Durable,
            "msg-durable",
        )
        .await;

    publisher
        .expect_producer_ack("msg-durable", 1, true, "1")
        .await;
    let delivery = subscriber.expect_msg().await;
    assert!(delivery.starts_with("MSG orders.created sid1 _BROKER.ACK.durable-client1-sid1.1."));
}

#[tokio::test]
async fn high_durability_qos_acks_after_local_flush() {
    let scenario = Scenario::new();
    let mut subscriber = scenario.connect_durable("client1", 25).await;
    let mut publisher = scenario.connect_durable("publisher1", 25).await;

    subscriber.subscribe("orders.*", "sid1").await;
    subscriber.ping_roundtrip().await;
    publisher
        .publish_qos(
            "orders.created",
            b"hello",
            protocol::AckLevel::HighDurability,
            "msg-high",
        )
        .await;

    publisher
        .expect_producer_ack("msg-high", 2, true, "1")
        .await;
    let inner = scenario.broker().inner.lock().await;
    assert!(inner.messages.contains_key(&1));
}

#[tokio::test]
async fn qos_no_durable_match_acks_not_retained() {
    let scenario = Scenario::new();
    let mut publisher = scenario.connect_durable("publisher1", 25).await;

    publisher
        .publish_qos(
            "orders.created",
            b"hello",
            protocol::AckLevel::Durable,
            "msg-none",
        )
        .await;

    publisher
        .expect_producer_ack("msg-none", 1, false, "-")
        .await;
}

#[tokio::test]
async fn cluster_durable_qos_errors_when_cluster_disabled() {
    let scenario = Scenario::new();
    let mut publisher = scenario.connect_durable("publisher1", 25).await;

    publisher
        .publish_qos(
            "orders.created",
            b"hello",
            protocol::AckLevel::ClusterDurable,
            "msg-cluster",
        )
        .await;

    publisher
        .expect_err_contains("CLUSTER_DURABLE requires clustered mode")
        .await;
}

#[tokio::test]
async fn cluster_durable_qos_waits_for_fake_cluster_commit() {
    let scenario = Scenario::new_fake_cluster(3);
    scenario.set_delay_writes(true);
    let mut subscriber = scenario.connect_durable("client1", 25).await;
    let mut publisher = scenario.connect_durable("publisher1", 25).await;

    subscriber.subscribe("orders.*", "sid1").await;
    subscriber.write_line("PING").await;
    subscriber.expect_no_frame_short().await;
    assert_eq!(scenario.queued_write_count(), 1);
    assert!(scenario.drain_one().is_some());
    subscriber.expect_pong().await;
    publisher
        .publish_qos(
            "orders.created",
            b"hello",
            protocol::AckLevel::ClusterDurable,
            "msg-cluster",
        )
        .await;

    publisher.expect_no_frame_short().await;
    assert_eq!(scenario.queued_write_count(), 1);
    scenario.drain_one();
    publisher
        .expect_producer_ack("msg-cluster", 3, true, "1")
        .await;
}

#[tokio::test]
async fn qos_publish_does_not_also_emit_verbose_ok() {
    let scenario = Scenario::new();
    let mut publisher = scenario.connect().await;
    publisher
        .write_line(
            r#"CONNECT {"durable_id":"publisher1","verbose":true,"ack_timeout_ms":25,"max_in_flight":1024}"#,
        )
        .await;

    publisher
        .publish_qos(
            "orders.created",
            b"hello",
            protocol::AckLevel::Durable,
            "msg-verbose",
        )
        .await;

    publisher
        .expect_producer_ack("msg-verbose", 1, false, "-")
        .await;
    publisher.expect_no_frame_short().await;
}
