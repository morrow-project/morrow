use super::*;
use crate::stream::connector_control_streams;

#[tokio::test]
async fn connector_control_stream_keeps_latest_key_across_restart() {
    let dir = TempDir::new().unwrap();
    let clock = Arc::new(ManualClock::new(1_000));
    let mut config = test_config(dir.path());
    config.streams = crate::stream::StreamCatalog::new(connector_control_streams(
        crate::stream::StoragePolicy::default(),
    ))
    .unwrap();
    let broker = deterministic_broker(config.clone(), clock.clone(), None);
    let mut publisher = TestClient::connect_durable(&broker, "connector-a", 1_000).await;

    publisher
        .publish_hpub(
            protocol::connector_control::CONFIG_SUBJECT,
            &[("Broker-Key", "connector-a")],
            br#"{"generation":1}"#,
        )
        .await;
    publisher
        .publish_hpub(
            protocol::connector_control::CONFIG_SUBJECT,
            &[("Broker-Key", "connector-a")],
            br#"{"generation":2}"#,
        )
        .await;
    publisher
        .publish_hpub(
            protocol::connector_control::SCHEMA_SUBJECT,
            &[("Broker-Key", "connector-a:v1")],
            br#"{"schema":"v1"}"#,
        )
        .await;
    publisher
        .publish_hpub(
            protocol::connector_control::SCHEMA_SUBJECT,
            &[("Broker-Key", "connector-a:v2")],
            br#"{"schema":"v2"}"#,
        )
        .await;
    publisher.ping_roundtrip().await;

    let current = {
        let inner = broker.inner.lock().await;
        assert_eq!(inner.messages.len(), 3);
        inner
            .messages
            .values()
            .find(|record| record.subject == protocol::connector_control::CONFIG_SUBJECT)
            .unwrap()
            .clone()
    };
    assert_eq!(current.offset, Some(1));
    assert_eq!(
        broker.partition_logs.load_record(&current).unwrap().payload,
        br#"{"generation":2}"#
    );
    publisher.disconnect().await;
    broker.shutdown().await.unwrap();

    let broker = deterministic_broker(config, clock, None);
    let current = {
        let inner = broker.inner.lock().await;
        assert_eq!(inner.messages.len(), 3);
        let current = inner
            .messages
            .values()
            .find(|record| record.subject == protocol::connector_control::CONFIG_SUBJECT)
            .unwrap()
            .clone();
        assert_eq!(
            inner
                .messages
                .values()
                .filter(|record| record.subject == protocol::connector_control::SCHEMA_SUBJECT)
                .count(),
            2
        );
        current
    };
    assert_eq!(
        broker.partition_logs.load_record(&current).unwrap().payload,
        br#"{"generation":2}"#
    );
    let mut consumer = TestClient::connect_durable(&broker, "observer", 1_000).await;
    consumer
        .subscribe_at(
            protocol::connector_control::CONFIG_SUBJECT,
            "config",
            "@earliest",
        )
        .await;
    let delivery = consumer.expect_msg().await;
    assert!(delivery.ends_with("16\r\n{\"generation\":2}\r\n"));
    consumer.disconnect().await;
    broker.shutdown().await.unwrap();
}
