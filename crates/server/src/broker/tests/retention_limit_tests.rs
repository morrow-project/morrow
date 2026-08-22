use super::*;
use crate::stream::{CompactionPolicy, StreamCatalog};

fn retention_config(
    dir: &Path,
    max_age_ms: Option<u64>,
    max_bytes: Option<u64>,
    compaction: CompactionPolicy,
) -> Config {
    let mut config = test_config(dir);
    let mut definitions = config.streams.definitions().to_vec();
    let orders = definitions
        .iter_mut()
        .find(|stream| stream.name.as_str() == "orders")
        .unwrap();
    orders.retention.max_age_ms = max_age_ms;
    orders.retention.max_bytes = max_bytes;
    orders.retention.compaction = compaction;
    config.streams = StreamCatalog::new(definitions).unwrap();
    config
}

#[tokio::test]
async fn age_retention_advances_cursor_rewrites_disk_and_reports_status() {
    let dir = TempDir::new().unwrap();
    let clock = Arc::new(ManualClock::new(1_000));
    let config = retention_config(dir.path(), Some(10), None, CompactionPolicy::None);
    let broker = deterministic_broker(config.clone(), clock.clone(), None);
    let mut subscriber = TestClient::connect_durable(&broker, "consumer", 25).await;
    let mut publisher = TestClient::connect_durable(&broker, "publisher", 25).await;
    subscriber.subscribe("orders.*", "sid").await;
    subscriber.ping_roundtrip().await;
    publisher.publish("orders.created", b"first").await;
    publisher.ping_roundtrip().await;

    clock.advance_ms(11);
    broker.tick_redelivery_for_test().await.unwrap();
    {
        let inner = broker.inner.lock().await;
        assert!(inner.messages.is_empty());
        let cursor = &inner.consumers["durable-consumer-sid"].cursors.partitions["orders:0"];
        assert_eq!(cursor.committed_offset, 1);
        assert_eq!(cursor.retention_gaps, 1);
        assert!(inner.consumers["durable-consumer-sid"].in_flight.is_empty());
    }
    let status = serde_json::to_value(broker.streams_response().await).unwrap();
    let orders = status["streams"]
        .as_array()
        .unwrap()
        .iter()
        .find(|stream| stream["name"] == "orders")
        .unwrap();
    assert_eq!(orders["retained_messages"], 0);
    assert_eq!(orders["partition_status"][0]["earliest_offset"], 1);
    assert_eq!(orders["partition_status"][0]["deleted_messages"], 1);

    subscriber.disconnect().await;
    publisher.disconnect().await;
    broker.shutdown().await.unwrap();
    let restarted = deterministic_broker(config, clock, None);
    assert!(restarted.inner.lock().await.messages.is_empty());
    let mut publisher = TestClient::connect_durable(&restarted, "publisher", 25).await;
    publisher.publish("orders.created", b"second").await;
    publisher.ping_roundtrip().await;
    let inner = restarted.inner.lock().await;
    assert_eq!(inner.messages.values().next().unwrap().offset, Some(1));
}

#[tokio::test]
async fn byte_retention_is_a_hard_bound_for_oversized_records() {
    let dir = TempDir::new().unwrap();
    let clock = Arc::new(ManualClock::new(1_000));
    let config = retention_config(dir.path(), None, Some(1), CompactionPolicy::None);
    let broker = deterministic_broker(config, clock, None);
    let mut publisher = TestClient::connect_durable(&broker, "publisher", 25).await;

    publisher
        .publish("orders.created", b"larger-than-one-byte")
        .await;
    publisher.ping_roundtrip().await;
    assert!(broker.inner.lock().await.messages.is_empty());
    let status = broker.streams_response().await;
    let orders = status
        .streams
        .iter()
        .find(|stream| stream.definition.name.as_str() == "orders")
        .unwrap();
    assert_eq!(orders.retained_messages, 0);
    assert_eq!(orders.retained_bytes, 0);
}

#[tokio::test]
async fn age_retention_removes_compacted_records_from_physical_history() {
    let dir = TempDir::new().unwrap();
    let clock = Arc::new(ManualClock::new(1_000));
    let config = retention_config(dir.path(), Some(10), None, CompactionPolicy::Key);
    let broker = deterministic_broker(config.clone(), clock.clone(), None);
    let mut publisher = TestClient::connect_durable(&broker, "publisher", 25).await;
    publisher
        .publish_hpub("orders.created", &[("Broker-Key", "customer-1")], b"first")
        .await;
    publisher.ping_roundtrip().await;
    publisher
        .publish_hpub("orders.created", &[("Broker-Key", "customer-1")], b"second")
        .await;
    publisher.ping_roundtrip().await;
    assert_eq!(broker.inner.lock().await.messages.len(), 1);

    clock.advance_ms(11);
    broker.tick_redelivery_for_test().await.unwrap();
    assert!(broker.inner.lock().await.messages.is_empty());
    publisher.disconnect().await;
    broker.shutdown().await.unwrap();

    let (_, replay) =
        PartitionLogSet::open(dir.path(), &config.streams, config.wal_segment_bytes).unwrap();
    assert!(replay.is_empty());
}

#[tokio::test]
async fn startup_applies_age_retention_before_replay() {
    let dir = TempDir::new().unwrap();
    let clock = Arc::new(ManualClock::new(1_000));
    let config = retention_config(dir.path(), Some(10), None, CompactionPolicy::None);
    let broker = deterministic_broker(config.clone(), clock.clone(), None);
    let mut subscriber = TestClient::connect_durable(&broker, "consumer", 25).await;
    let mut publisher = TestClient::connect_durable(&broker, "publisher", 25).await;
    subscriber.subscribe("orders.*", "sid").await;
    subscriber.ping_roundtrip().await;
    publisher.publish("orders.created", b"first").await;
    publisher.ping_roundtrip().await;
    publisher.disconnect().await;
    subscriber.disconnect().await;
    broker.shutdown().await.unwrap();

    clock.advance_ms(11);
    let restarted = deterministic_broker(config, clock, None);
    let inner = restarted.inner.lock().await;
    assert!(inner.messages.is_empty());
    let consumer = &inner.consumers["durable-consumer-sid"];
    assert!(consumer.pending.is_empty());
    assert!(consumer.in_flight.is_empty());
    assert_eq!(consumer.cursors.partitions["orders:0"].committed_offset, 1);
    drop(inner);
    let status = restarted.streams_response().await;
    let orders = status
        .streams
        .iter()
        .find(|stream| stream.definition.name.as_str() == "orders")
        .unwrap();
    assert_eq!(orders.partition_status[0].earliest_offset, 1);
}

#[tokio::test]
async fn clustered_sync_does_not_reintroduce_retained_records() {
    let dir = TempDir::new().unwrap();
    let clock = Arc::new(ManualClock::new(1_000));
    let config = retention_config(dir.path(), Some(10), None, CompactionPolicy::None);
    let cluster = FakeClusterRuntime::new(3, 1, Some(1));
    let broker = deterministic_broker(config, clock.clone(), Some(ClusterRuntime::Fake(cluster)));
    let mut publisher = TestClient::connect_durable(&broker, "publisher", 25).await;
    publisher.publish("orders.created", b"first").await;
    publisher.ping_roundtrip().await;

    clock.advance_ms(11);
    broker.tick_redelivery_for_test().await.unwrap();
    assert!(broker.inner.lock().await.messages.is_empty());
    publisher.publish("orders.created", b"second").await;
    publisher.ping_roundtrip().await;
    let inner = broker.inner.lock().await;
    assert_eq!(inner.messages.len(), 1);
    assert_eq!(inner.messages.values().next().unwrap().offset, Some(1));
}
