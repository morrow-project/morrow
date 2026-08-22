use super::*;

#[tokio::test]
async fn earliest_consumer_replays_publications_created_before_it() {
    let scenario = Scenario::new();
    let mut publisher = scenario.connect_durable("publisher", 25).await;
    for payload in [b"one".as_slice(), b"two", b"three"] {
        publisher.publish("orders/created", payload).await;
    }
    publisher.ping_roundtrip().await;

    let mut subscriber = scenario.connect_durable("late", 25).await;
    subscriber
        .subscribe_at("orders/*", "sid", "@earliest")
        .await;
    let first = subscriber.expect_msg().await;
    let second = subscriber.expect_msg().await;
    let third = subscriber.expect_msg().await;
    assert!(first.ends_with("3\r\none\r\n"));
    assert!(second.ends_with("3\r\ntwo\r\n"));
    assert!(third.ends_with("5\r\nthree\r\n"));
}

#[tokio::test]
async fn consumers_keep_independent_partition_positions() {
    let scenario = Scenario::new();
    let mut publisher = scenario.connect_durable("publisher", 25).await;
    publisher.publish("orders/created", b"one").await;
    publisher.publish("orders/created", b"two").await;
    publisher.ping_roundtrip().await;

    let mut first = scenario.connect_durable("first", 25).await;
    let mut second = scenario.connect_durable("second", 25).await;
    first.subscribe_at("orders/*", "sid", "@earliest").await;
    second.subscribe_at("orders/*", "sid", "@earliest").await;
    let first_delivery = first.expect_msg().await;
    first.expect_msg().await;
    second.expect_msg().await;
    second.expect_msg().await;
    first.ack(&ack_subject(&first_delivery)).await;
    first.ping_roundtrip().await;

    let inner = scenario.broker().inner.lock().await;
    assert_eq!(
        inner.consumers["durable-first-sid"]
            .cursors
            .committed_offset("orders", 0),
        Some(1)
    );
    assert_eq!(
        inner.consumers["durable-second-sid"]
            .cursors
            .committed_offset("orders", 0),
        Some(0)
    );
}

#[tokio::test]
async fn out_of_order_acks_close_cursor_gap() {
    let scenario = Scenario::new();
    let mut publisher = scenario.connect_durable("publisher", 25).await;
    for payload in [b"one".as_slice(), b"two", b"three"] {
        publisher.publish("orders/created", payload).await;
    }
    publisher.ping_roundtrip().await;

    let mut subscriber = scenario.connect_durable("worker", 25).await;
    subscriber
        .subscribe_at("orders/*", "sid", "@earliest")
        .await;
    let first = subscriber.expect_msg().await;
    let second = subscriber.expect_msg().await;
    subscriber.expect_msg().await;
    subscriber.ack(&ack_subject(&second)).await;
    subscriber.ping_roundtrip().await;
    {
        let inner = scenario.broker().inner.lock().await;
        let cursor = &inner.consumers["durable-worker-sid"].cursors.partitions["orders:0"];
        assert_eq!(cursor.committed_offset, 0);
        assert_eq!(cursor.acknowledged_offsets, [1].into_iter().collect());
    }

    subscriber.ack(&ack_subject(&first)).await;
    subscriber.ping_roundtrip().await;
    let inner = scenario.broker().inner.lock().await;
    assert_eq!(
        inner.consumers["durable-worker-sid"]
            .cursors
            .committed_offset("orders", 0),
        Some(2)
    );
}
