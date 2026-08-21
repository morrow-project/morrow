use super::*;

#[tokio::test]
async fn stream_retains_publish_before_consumer_and_across_restart() {
    let mut scenario = Scenario::new();
    let mut publisher = scenario.connect_durable("publisher1", 25).await;

    publisher.publish("orders.created", b"hello").await;
    publisher.ping_roundtrip().await;

    {
        let inner = scenario.broker().inner.lock().await;
        assert_eq!(inner.messages.len(), 1);
        assert_eq!(inner.messages[&1].stream.as_deref(), Some("orders"));
        assert!(inner.consumers.is_empty());
    }

    publisher.disconnect().await;
    scenario.restart_broker().await;

    let inner = scenario.broker().inner.lock().await;
    assert_eq!(inner.messages.len(), 1);
    assert_eq!(inner.messages[&1].stream.as_deref(), Some("orders"));
    assert!(inner.consumers.is_empty());
}

#[tokio::test]
async fn matching_consumers_share_one_stream_append() {
    let scenario = Scenario::new();
    let mut first = scenario.connect_durable("first", 25).await;
    let mut second = scenario.connect_durable("second", 25).await;
    let mut publisher = scenario.connect_durable("publisher", 25).await;

    first.subscribe("orders.*", "one").await;
    first.ping_roundtrip().await;
    second.subscribe("orders.*", "two").await;
    second.ping_roundtrip().await;
    publisher.publish("orders.created", b"hello").await;
    publisher.ping_roundtrip().await;

    let inner = scenario.broker().inner.lock().await;
    assert_eq!(inner.messages.len(), 1);
    assert!(
        inner.consumers["durable-first-one"]
            .in_flight
            .contains_key(&1)
    );
    assert!(
        inner.consumers["durable-second-two"]
            .in_flight
            .contains_key(&1)
    );
}

#[tokio::test]
async fn unbound_publish_remains_transient() {
    let scenario = Scenario::new();
    let mut subscriber = scenario.connect().await;
    let mut publisher = scenario.connect().await;

    subscriber.write_line("CONNECT {}").await;
    publisher.write_line("CONNECT {}").await;
    subscriber.subscribe("events.*", "live").await;
    subscriber.ping_roundtrip().await;
    publisher.publish("events.created", b"hello").await;

    assert_eq!(
        subscriber.expect_msg().await,
        "MSG events.created live 5\r\nhello\r\n"
    );
    assert!(scenario.broker().inner.lock().await.messages.is_empty());
}

#[tokio::test]
async fn clustered_stream_retains_publish_without_consumer() {
    let scenario = Scenario::new_fake_cluster(3);
    let mut publisher = scenario.connect_durable("publisher", 25).await;

    publisher.publish("orders.created", b"hello").await;
    publisher.ping_roundtrip().await;

    let inner = scenario.broker().inner.lock().await;
    assert_eq!(inner.messages.len(), 1);
    assert_eq!(inner.messages[&1].stream.as_deref(), Some("orders"));
    assert!(inner.consumers.is_empty());
}

#[tokio::test]
async fn ack_does_not_delete_stream_owned_record() {
    let scenario = Scenario::new();
    let mut subscriber = scenario.connect_durable("consumer", 25).await;
    let mut publisher = scenario.connect_durable("publisher", 25).await;

    subscriber.subscribe("orders.*", "sid").await;
    subscriber.ping_roundtrip().await;
    publisher.publish("orders.created", b"hello").await;
    let delivery = subscriber.expect_msg().await;
    subscriber.publish(&ack_subject(&delivery), b"").await;
    subscriber.ping_roundtrip().await;

    let inner = scenario.broker().inner.lock().await;
    assert_eq!(inner.messages.len(), 1);
    assert_eq!(inner.messages[&1].stream.as_deref(), Some("orders"));
    assert!(inner.consumers["durable-consumer-sid"].acked.contains(&1));
}
