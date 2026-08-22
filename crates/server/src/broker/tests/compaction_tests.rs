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
            &[("Morrow-Key", "connector-a")],
            br#"{"generation":1}"#,
        )
        .await;
    publisher
        .publish_hpub(
            protocol::connector_control::CONFIG_SUBJECT,
            &[("Morrow-Key", "connector-a")],
            br#"{"generation":2}"#,
        )
        .await;
    publisher
        .publish_hpub(
            protocol::connector_control::SCHEMA_SUBJECT,
            &[("Morrow-Key", "connector-a:v1")],
            br#"{"schema":"v1"}"#,
        )
        .await;
    publisher
        .publish_hpub(
            protocol::connector_control::SCHEMA_SUBJECT,
            &[("Morrow-Key", "connector-a:v2")],
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

#[tokio::test]
async fn physical_compaction_converges_and_preserves_the_high_watermark() {
    let dir = TempDir::new().unwrap();
    let clock = Arc::new(ManualClock::new(1_000));
    let mut config = test_config(dir.path());
    config.wal_segment_bytes = 512;
    config.streams = crate::stream::StreamCatalog::new(connector_control_streams(
        crate::stream::StoragePolicy::default(),
    ))
    .unwrap();
    let broker = deterministic_broker(config.clone(), clock.clone(), None);
    let mut publisher = TestClient::connect_durable(&broker, "connector-a", 1_000).await;
    publisher.ping_roundtrip().await;
    let publisher_id = *broker
        .connections
        .lock()
        .await
        .clients
        .iter()
        .find(|(_, client)| client.durable_id.as_deref() == Some("connector-a"))
        .unwrap()
        .0;
    broker
        .publish(
            publisher_id,
            protocol::connector_control::CONFIG_SUBJECT.to_string(),
            None,
            Vec::new(),
            Some(b"anchor".to_vec()),
            b"anchor".to_vec(),
            None,
        )
        .await
        .unwrap();
    for generation in 0..70 {
        broker
            .publish(
                publisher_id,
                protocol::connector_control::CONFIG_SUBJECT.to_string(),
                None,
                Vec::new(),
                Some(b"connector-a".to_vec()),
                format!("generation-{generation:04}").into_bytes(),
                None,
            )
            .await
            .unwrap();
    }
    broker.compact_streams_for_test().await.unwrap();

    let partition = dir
        .path()
        .join("streams/morrow-connect-config/partition-00000");
    let physical_bytes = std::fs::read_dir(partition)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().and_then(|value| value.to_str()) == Some("plog"))
        .map(|path| path.metadata().unwrap().len())
        .sum::<u64>();
    assert!(physical_bytes < 1_024);
    assert_eq!(broker.inner.lock().await.messages.len(), 2);

    broker
        .publish(
            publisher_id,
            protocol::connector_control::CONFIG_SUBJECT.to_string(),
            None,
            Vec::new(),
            Some(b"connector-a".to_vec()),
            b"generation-0070".to_vec(),
            None,
        )
        .await
        .unwrap();
    let current = broker
        .inner
        .lock()
        .await
        .messages
        .values()
        .find(|record| record.key.as_deref() == Some(b"connector-a"))
        .unwrap()
        .clone();
    assert_eq!(current.offset, Some(71));
    broker.shutdown().await.unwrap();

    let broker = deterministic_broker(config, clock, None);
    let current = broker
        .inner
        .lock()
        .await
        .messages
        .values()
        .find(|record| record.key.as_deref() == Some(b"connector-a"))
        .unwrap()
        .clone();
    assert_eq!(current.offset, Some(71));
    assert_eq!(
        broker.partition_logs.load_record(&current).unwrap().payload,
        b"generation-0070"
    );
}

#[tokio::test]
#[ignore = "manual incremental key-compaction benchmark"]
async fn benchmark_incremental_compaction_append_cost() {
    let dir = TempDir::new().unwrap();
    let clock = Arc::new(ManualClock::new(1_000));
    let mut config = test_config(dir.path());
    config.streams = crate::stream::StreamCatalog::new(connector_control_streams(
        crate::stream::StoragePolicy::default(),
    ))
    .unwrap();
    let broker = deterministic_broker(config, clock, None);
    let mut publisher = TestClient::connect_durable(&broker, "connector-a", 1_000).await;
    publisher
        .publish_hpub(
            protocol::connector_control::CONFIG_SUBJECT,
            &[("Morrow-Key", "connector-a")],
            b"template",
        )
        .await;
    publisher.ping_roundtrip().await;

    let mut inner = broker.inner.lock().await;
    let template = inner.messages.values().next().unwrap().clone();
    let started = std::time::Instant::now();
    for seq in 2..=100_000_u64 {
        let mut record = template.clone();
        record.seq = seq;
        record.offset = Some(seq - 1);
        inner
            .partition_sequences
            .insert(("morrow-connect-config".to_string(), 0, seq - 1), seq);
        inner.messages.insert(seq, record);
        inner.apply_record_compaction(seq, &broker.config.streams);
    }
    assert_eq!(inner.messages.len(), 1);
    eprintln!("updates=100000 elapsed={:?}", started.elapsed());
}
