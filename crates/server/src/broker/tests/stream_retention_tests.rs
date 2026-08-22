use super::*;

#[tokio::test]
async fn stream_retains_publish_before_consumer_and_across_restart() {
    let mut scenario = Scenario::new();
    let mut publisher = scenario.connect_durable("publisher1", 25).await;

    publisher.publish("orders/created", b"hello").await;
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

    first.subscribe("orders/*", "one").await;
    first.ping_roundtrip().await;
    second.subscribe("orders/*", "two").await;
    second.ping_roundtrip().await;
    publisher.publish("orders/created", b"hello").await;
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

    subscriber.write_line("CONN {}").await;
    publisher.write_line("CONN {}").await;
    subscriber.subscribe("events/*", "live").await;
    subscriber.ping_roundtrip().await;
    publisher.publish("events/created", b"hello").await;

    assert_eq!(
        subscriber.expect_msg().await,
        "DELIVER events/created live 5\r\nhello\r\n"
    );
    assert!(scenario.broker().inner.lock().await.messages.is_empty());
}

#[tokio::test]
async fn clustered_stream_retains_publish_without_consumer() {
    let scenario = Scenario::new_fake_cluster(3);
    let mut publisher = scenario.connect_durable("publisher", 25).await;

    publisher.publish("orders/created", b"hello").await;
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

    subscriber.subscribe("orders/*", "sid").await;
    subscriber.ping_roundtrip().await;
    publisher.publish("orders/created", b"hello").await;
    let delivery = subscriber.expect_msg().await;
    subscriber.ack(&ack_subject(&delivery)).await;
    subscriber.ping_roundtrip().await;

    let inner = scenario.broker().inner.lock().await;
    assert_eq!(inner.messages.len(), 1);
    assert_eq!(inner.messages[&1].stream.as_deref(), Some("orders"));
    assert_eq!(
        inner.consumers["durable-consumer-sid"]
            .cursors
            .committed_offset("orders", 0),
        Some(1)
    );
}

#[tokio::test]
async fn keyed_headers_and_envelope_metadata_survive_restart_and_delivery() {
    let mut scenario = Scenario::new();
    let mut subscriber = TestClient::connect_pull(scenario.broker(), "consumer", 25).await;
    let mut publisher = scenario.connect_durable("tenant-a", 25).await;
    subscriber.subscribe("orders/*", "sid").await;
    subscriber.write_line("CREDIT sid 1 1024").await;
    subscriber.ping_roundtrip().await;

    publisher
        .publish_hpub(
            "orders/created",
            &[("Morrow-Key", "customer-7"), ("Trace-Id", "trace-1")],
            b"hello",
        )
        .await;
    let delivery = subscriber.read_frame().await;
    assert!(delivery.starts_with("HDELIVER orders/created sid "));
    assert!(delivery.contains("Trace-Id: trace-1\r\n"));
    assert!(!delivery.contains("Morrow-Key:"));
    assert!(delivery.contains("Morrow-Key-Hex: 637573746f6d65722d37\r\n"));
    assert!(delivery.contains("Morrow-Timestamp: 1000\r\n"));

    subscriber.disconnect().await;
    publisher.disconnect().await;
    scenario.restart_broker().await;
    let inner = scenario.broker().inner.lock().await;
    let record = &inner.messages[&1];
    assert_eq!(record.namespace, "tenant-a");
    assert_eq!(record.key.as_deref(), Some(b"customer-7".as_slice()));
    assert_eq!(record.headers[0].name, "Trace-Id");
    assert_eq!(record.partition, Some(0));
    assert_eq!(record.offset, Some(0));
    assert_eq!(record.partitioning_epoch, 1);
    drop(inner);

    let mut restarted = TestClient::connect_pull(scenario.broker(), "consumer", 25).await;
    restarted.subscribe("orders/*", "sid").await;
    restarted.write_line("CREDIT sid 1 1024").await;
    let redelivery = restarted.read_frame().await;
    assert!(redelivery.starts_with("HDELIVER orders/created sid "));
    assert!(redelivery.contains("Trace-Id: trace-1\r\n"));
}

#[tokio::test]
async fn transitional_stream_wal_records_migrate_to_partition_history() {
    let dir = TempDir::new().unwrap();
    let config = test_config(dir.path());
    let (mut wal, _) = Wal::open(
        dir.path(),
        config.fsync_interval(),
        config.wal_segment_bytes,
    )
    .unwrap();
    wal.append_stream_publish("orders", "orders/created", None, b"legacy")
        .unwrap();
    wal.flush().unwrap();
    drop(wal);

    let broker = Morrow::open(config.clone()).unwrap();
    let metadata = {
        let inner = broker.inner.lock().await;
        assert_eq!(inner.messages[&1].partition, Some(0));
        assert_eq!(inner.messages[&1].offset, Some(0));
        inner.messages[&1].clone()
    };
    assert_eq!(
        broker
            .partition_logs
            .load_record(&metadata)
            .unwrap()
            .payload,
        b"legacy"
    );
    broker.shutdown().await.unwrap();

    let (_, replay) = Wal::open(
        dir.path(),
        config.fsync_interval(),
        config.wal_segment_bytes,
    )
    .unwrap();
    assert!(replay.messages.is_empty());
    assert_eq!(replay.partition_appends[&1].stream, "orders");
}
