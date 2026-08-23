use super::*;

#[tokio::test]
async fn auth_enabled_generates_fresh_nonce_per_connection() {
    let dir = TempDir::new().unwrap();
    let mut config = test_config(dir.path());
    config.auth.enabled = true;
    config.auth.clients.insert(
        "client1".into(),
        crate::config::AuthClientConfig {
            public_key: "abcd".into(),
            permissions: None,
        },
    );
    let broker = Morrow::open(config).unwrap();
    let (tx1, _rx1) = test_outbound_queue(&broker, 8);
    let (tx2, _rx2) = test_outbound_queue(&broker, 8);

    broker.add_client(1, tx1, None).await.unwrap();
    broker.add_client(2, tx2, None).await.unwrap();

    let connections = broker.connections.lock().await;
    let first = connections
        .clients
        .get(&1)
        .unwrap()
        .auth_nonce
        .as_ref()
        .unwrap();
    let second = connections
        .clients
        .get(&2)
        .unwrap()
        .auth_nonce
        .as_ref()
        .unwrap();
    assert_ne!(first, second);
    assert_eq!(first.len(), 64);
    assert_eq!(second.len(), 64);
}
#[tokio::test]
async fn non_durable_connect_subscribes_as_transient_core() {
    let scenario = Scenario::new();
    let mut subscriber = scenario.connect().await;
    let mut publisher = scenario.connect_durable("publisher1", 25).await;

    subscriber.write_line("CONN {}").await;
    subscriber.subscribe("orders/*", "sid1").await;
    subscriber.ping_roundtrip().await;
    publisher.publish("orders/created", b"hello").await;

    let frame = subscriber.expect_msg().await;
    assert_eq!(frame, "DELIVER orders/created sid1 5\r\nhello\r\n");
    let inner = scenario.broker().inner.lock().await;
    assert!(inner.consumers.is_empty());
    drop(inner);
    assert_eq!(
        scenario.broker().transient.lock().await.subscriptions.len(),
        1
    );
}
#[tokio::test]
async fn durable_subscribe_publish_delivery_and_ack_are_deterministic() {
    let scenario = Scenario::new();
    let mut subscriber = scenario.connect_durable("client1", 25).await;
    let mut publisher = scenario.connect_durable("publisher1", 25).await;

    subscriber.subscribe("orders/*", "sid1").await;
    subscriber.ping_roundtrip().await;
    publisher.publish("orders/created", b"hello").await;

    let frame = subscriber.expect_msg().await;
    assert!(frame.starts_with("DELIVER orders/created sid1 _MORROW/ACK/durable-client1-sid1/"));
    assert!(frame.ends_with("5\r\nhello\r\n"));
    publisher.ack(&ack_subject(&frame)).await;
    publisher.ping_roundtrip().await;

    let inner = scenario.broker().inner.lock().await;
    let consumer = inner.consumers.get("durable-client1-sid1").unwrap();
    assert!(consumer.pending.is_empty());
    assert!(consumer.in_flight.is_empty());
    assert_eq!(consumer.cursors.committed_offset("orders", 0), Some(1));
    assert_eq!(inner.messages[&1].stream.as_deref(), Some("orders"));
}
#[tokio::test]
async fn redelivery_waits_for_manual_clock_deadline() {
    let scenario = Scenario::new();
    let mut subscriber = scenario.connect_durable("client1", 25).await;
    let mut publisher = scenario.connect_durable("publisher1", 25).await;

    subscriber.subscribe("orders/*", "sid1").await;
    subscriber.ping_roundtrip().await;
    publisher.publish("orders/created", b"hello").await;
    let first = subscriber.expect_msg().await;
    assert!(first.contains("/1/1 "));

    scenario.advance_ms(24);
    scenario.tick_redelivery().await;
    {
        let inner = scenario.broker().inner.lock().await;
        let consumer = inner.consumers.get("durable-client1-sid1").unwrap();
        assert!(consumer.pending.is_empty());
        assert_eq!(consumer.in_flight.get(&1).unwrap().delivery_id, 1);
    }
    subscriber.expect_no_frame_short().await;

    scenario.advance_ms(1);
    scenario.tick_redelivery().await;
    let second = subscriber.expect_msg().await;
    assert!(second.starts_with("DELIVER orders/created sid1 _MORROW/ACK/durable-client1-sid1/1/2"));
    assert!(second.ends_with("5\r\nhello\r\n"));
}

#[tokio::test]
async fn scheduled_publish_waits_for_committed_delivery_time() {
    let scenario = Scenario::new();
    let mut subscriber = scenario.connect_durable("client1", 25).await;
    let mut publisher = scenario.connect_durable("publisher1", 25).await;

    subscriber.subscribe("orders/*", "sid1").await;
    subscriber.ping_roundtrip().await;
    publisher
        .publish_hpub(
            "orders/created",
            &[("Morrow-Scheduled-At", "2000")],
            b"hello",
        )
        .await;

    scenario.tick_redelivery().await;
    subscriber.expect_no_frame_short().await;
    scenario.advance_ms(999);
    scenario.tick_redelivery().await;
    subscriber.expect_no_frame_short().await;
    scenario.advance_ms(1);
    scenario.tick_redelivery().await;
    let frame = subscriber.expect_hmsg().await;
    assert!(frame.starts_with("HDELIVER orders/created sid1"));
    assert!(frame.ends_with("\r\nhello\r\n"));
}

#[tokio::test]
async fn idempotent_producer_retry_returns_original_position_without_append() {
    let mut scenario = Scenario::new();
    let mut publisher = scenario.connect_durable("publisher", 25).await;
    let headers = [
        ("Morrow-QoS", "1"),
        ("Morrow-Msg-Id", "msg-1"),
        ("Morrow-Producer-Id", "producer-a"),
        ("Morrow-Producer-Epoch", "1"),
        ("Morrow-Producer-Sequence", "1"),
    ];
    publisher
        .publish_hpub("orders/created", &headers, b"one")
        .await;
    let first = publisher.read_frame().await;
    assert!(first.contains("OK true 1"));
    publisher
        .publish_hpub("orders/created", &headers, b"one")
        .await;
    let duplicate = publisher.read_frame().await;
    assert_eq!(duplicate, first);
    let inner = scenario.broker().inner.lock().await;
    assert_eq!(inner.partition_sequences.len(), 1);
    assert_eq!(inner.producer_sequences.len(), 1);
    drop(inner);
    publisher.disconnect().await;
    scenario.restart_broker().await;
    let mut reconnected = scenario.connect_durable("publisher", 25).await;
    reconnected
        .publish_hpub("orders/created", &headers, b"one")
        .await;
    assert_eq!(reconnected.read_frame().await, first);
}

#[tokio::test]
async fn idempotent_producer_rejects_gaps_and_conflicting_content() {
    let scenario = Scenario::new();
    let mut publisher = scenario.connect_durable("publisher", 25).await;
    let base_headers = [
        ("Morrow-QoS", "1"),
        ("Morrow-Msg-Id", "msg-1"),
        ("Morrow-Producer-Id", "producer-a"),
        ("Morrow-Producer-Epoch", "1"),
        ("Morrow-Producer-Sequence", "1"),
    ];
    publisher
        .publish_hpub("orders/created", &base_headers, b"one")
        .await;
    publisher.read_frame().await;
    let conflicting = [
        ("Morrow-QoS", "1"),
        ("Morrow-Msg-Id", "msg-1"),
        ("Morrow-Producer-Id", "producer-a"),
        ("Morrow-Producer-Epoch", "1"),
        ("Morrow-Producer-Sequence", "1"),
    ];
    publisher
        .publish_hpub("orders/created", &conflicting, b"different")
        .await;
    assert!(publisher.read_frame().await.starts_with("-ERR "));
    let gap = [
        ("Morrow-QoS", "1"),
        ("Morrow-Msg-Id", "msg-3"),
        ("Morrow-Producer-Id", "producer-a"),
        ("Morrow-Producer-Epoch", "1"),
        ("Morrow-Producer-Sequence", "3"),
    ];
    publisher
        .publish_hpub("orders/created", &gap, b"three")
        .await;
    assert!(
        publisher
            .read_frame()
            .await
            .contains("producer sequence gap")
    );
}
#[tokio::test]
async fn acked_message_does_not_redeliver_after_manual_ticks() {
    let scenario = Scenario::new();
    let mut subscriber = scenario.connect_durable("client1", 25).await;
    let mut publisher = scenario.connect_durable("publisher1", 25).await;

    subscriber.subscribe("orders/*", "sid1").await;
    subscriber.ping_roundtrip().await;
    publisher.publish("orders/created", b"hello").await;
    let frame = subscriber.expect_msg().await;
    publisher.ack(&ack_subject(&frame)).await;
    publisher.ping_roundtrip().await;

    scenario.advance_ms(1_000);
    scenario.tick_redelivery().await;
    subscriber.expect_no_frame_short().await;
    let inner = scenario.broker().inner.lock().await;
    assert_eq!(inner.messages[&1].stream.as_deref(), Some("orders"));
    let consumer = inner.consumers.get("durable-client1-sid1").unwrap();
    assert!(consumer.pending.is_empty());
    assert!(consumer.in_flight.is_empty());
}
#[tokio::test]
async fn wal_replay_preserves_unacked_delivery_state_and_next_ids() {
    let mut scenario = Scenario::new();
    {
        let mut subscriber = scenario.connect_durable("client1", 25).await;
        let mut publisher = scenario.connect_durable("publisher1", 25).await;
        subscriber.subscribe("orders/*", "sid1").await;
        subscriber.ping_roundtrip().await;
        publisher.publish("orders/created", b"hello").await;
        let first = subscriber.expect_msg().await;
        assert!(first.contains("/1/1 "));
        subscriber.disconnect().await;
        publisher.disconnect().await;
    }

    scenario.restart_broker().await;
    scenario.advance_ms(25);
    let mut subscriber = scenario.connect_durable("client1", 25).await;
    subscriber.subscribe("orders/*", "sid1").await;
    scenario.tick_redelivery().await;

    let redelivery = subscriber.expect_msg().await;
    assert!(
        redelivery.starts_with("DELIVER orders/created sid1 _MORROW/ACK/durable-client1-sid1/1/2")
    );
}
#[tokio::test]
async fn acked_message_does_not_redeliver_after_restart() {
    let mut scenario = Scenario::new();
    {
        let mut subscriber = scenario.connect_durable("client1", 25).await;
        let mut publisher = scenario.connect_durable("publisher1", 25).await;
        subscriber.subscribe("orders/*", "sid1").await;
        subscriber.ping_roundtrip().await;
        publisher.publish("orders/created", b"hello").await;
        let frame = subscriber.expect_msg().await;
        publisher.ack(&ack_subject(&frame)).await;
        publisher.ping_roundtrip().await;
        subscriber.disconnect().await;
        publisher.disconnect().await;
    }

    scenario.restart_broker().await;
    let mut subscriber = scenario.connect_durable("client1", 25).await;
    subscriber.subscribe("orders/*", "sid1").await;
    subscriber.ping_roundtrip().await;
    scenario.advance_ms(1_000);
    scenario.tick_redelivery().await;
    subscriber.expect_no_frame_short().await;
    let inner = scenario.broker().inner.lock().await;
    assert!(inner.consumers["durable-client1-sid1"].pending.is_empty());
    assert!(inner.consumers["durable-client1-sid1"].in_flight.is_empty());
    assert_eq!(
        inner.consumers["durable-client1-sid1"]
            .cursors
            .committed_offset("orders", 0),
        Some(1)
    );
}

#[tokio::test]
async fn wal_rotation_and_shutdown_checkpoint_preserve_durable_state() {
    let mut scenario = Scenario::new_with_wal_segment_bytes(128);
    {
        let mut subscriber = scenario.connect_durable("client1", 25).await;
        let mut publisher = scenario.connect_durable("publisher1", 25).await;
        subscriber.subscribe("orders/*", "sid1").await;
        subscriber.ping_roundtrip().await;
        publisher.publish("orders/one", b"first").await;
        let first = subscriber.expect_msg().await;
        publisher.ack(&ack_subject(&first)).await;
        publisher.publish("orders/two", b"second").await;
        let second = subscriber.expect_msg().await;
        assert!(second.contains("/2/2 "));
        subscriber.disconnect().await;
        publisher.disconnect().await;
    }

    let before_checkpoint = wal_segment_count(scenario._dir.path());
    assert!(before_checkpoint > 1, "expected rotated WAL segments");
    scenario.restart_broker().await;
    assert_eq!(wal_segment_count(scenario._dir.path()), 1);

    scenario.advance_ms(25);
    let mut subscriber = scenario.connect_durable("client1", 25).await;
    subscriber.subscribe("orders/*", "sid1").await;
    scenario.tick_redelivery().await;

    let redelivery = subscriber.expect_msg().await;
    assert!(redelivery.starts_with("DELIVER orders/two sid1 _MORROW/ACK/durable-client1-sid1/2/3"));
    let inner = scenario.broker().inner.lock().await;
    assert_eq!(inner.messages[&1].stream.as_deref(), Some("orders"));
    assert!(inner.messages.contains_key(&2));
    assert_eq!(
        inner.consumers["durable-client1-sid1"]
            .cursors
            .committed_offset("orders", 0),
        Some(1)
    );
}

#[tokio::test]
async fn request_reply_inbox_delivery_is_transient() {
    let scenario = Scenario::new();
    let mut responder = scenario.connect_durable("responder1", 25).await;
    let mut requester = scenario.connect_durable("requester1", 25).await;

    responder.subscribe("service/echo", "sid1").await;
    responder.ping_roundtrip().await;
    requester
        .subscribe("_MORROW/INBOX/requester/1", "inbox1")
        .await;
    requester.ping_roundtrip().await;
    requester
        .publish_with_reply("service/echo", Some("_MORROW/INBOX/requester/1"), b"hello")
        .await;

    let request = responder.expect_hmsg().await;
    assert!(request.starts_with("HDELIVER service/echo sid1 _MORROW/INBOX/requester/1 "));
    assert!(request.contains("\r\nMorrow-Ack: _MORROW/ACK/durable-responder1-sid1/"));
    responder
        .publish("_MORROW/INBOX/requester/1", b"world")
        .await;

    let response = requester.expect_msg().await;
    assert_eq!(
        response,
        "DELIVER _MORROW/INBOX/requester/1 inbox1 5\r\nworld\r\n"
    );
    let inner = scenario.broker().inner.lock().await;
    assert!(inner.messages.contains_key(&1));
    assert_eq!(inner.consumers.len(), 1);
    drop(inner);
    assert_eq!(
        scenario.broker().transient.lock().await.subscriptions.len(),
        1
    );
}
#[tokio::test]
async fn publish_without_stream_binding_is_not_retained() {
    let scenario = Scenario::new();
    let mut publisher = scenario.connect_durable("publisher1", 25).await;

    publisher.publish("unbound.created", b"hello").await;
    publisher.ping_roundtrip().await;
    assert!(scenario.broker().inner.lock().await.messages.is_empty());
}
#[tokio::test]
async fn durable_queue_group_delivers_one_copy() {
    let scenario = Scenario::new();
    let mut first = scenario.connect_durable("client1", 25).await;
    let mut second = scenario.connect_durable("client2", 25).await;
    let mut publisher = scenario.connect_durable("publisher1", 25).await;

    first.subscribe_queue("orders/*", "workers", "a").await;
    first.ping_roundtrip().await;
    second.subscribe_queue("orders/*", "workers", "b").await;
    second.ping_roundtrip().await;
    publisher.publish("orders/created", b"hello").await;
    publisher.ping_roundtrip().await;

    let inner = scenario.broker().inner.lock().await;
    let consumer = inner
        .consumers
        .get("queue-workers-6f72646572732f2a")
        .unwrap();
    assert_eq!(consumer.delivered, 1);
    assert_eq!(consumer.in_flight.len(), 1);
}
#[tokio::test]
async fn unsub_with_max_receives_one_more_durable_delivery_then_detaches() {
    let scenario = Scenario::new();
    let mut subscriber = scenario.connect_durable("client1", 25).await;
    let mut publisher = scenario.connect_durable("publisher1", 25).await;

    subscriber.subscribe("orders/*", "sid1").await;
    subscriber.ping_roundtrip().await;
    subscriber.write_line("UNSUB sid1 1").await;
    publisher.publish("orders/created", b"one").await;
    let first = subscriber.expect_msg().await;
    assert!(first.ends_with("3\r\none\r\n"));

    publisher.publish("orders/created", b"two").await;
    publisher.ping_roundtrip().await;
    subscriber.expect_no_frame_short().await;
    let inner = scenario.broker().inner.lock().await;
    assert!(inner.consumers["durable-client1-sid1"].members.is_empty());
}
#[tokio::test]
async fn queue_unsub_with_max_detaches_only_that_member_after_count() {
    let scenario = Scenario::new();
    let mut first = scenario.connect_durable("client1", 25).await;
    let mut second = scenario.connect_durable("client2", 25).await;
    let mut publisher = scenario.connect_durable("publisher1", 25).await;

    first.subscribe_queue("orders/*", "workers", "a").await;
    first.ping_roundtrip().await;
    second.subscribe_queue("orders/*", "workers", "b").await;
    second.ping_roundtrip().await;
    first.write_line("UNSUB a 2").await;

    publisher.publish("orders/created", b"one").await;
    assert!(first.expect_msg().await.ends_with("3\r\none\r\n"));
    publisher.publish("orders/created", b"two").await;
    assert!(first.expect_msg().await.ends_with("3\r\ntwo\r\n"));
    publisher.publish("orders/created", b"three").await;
    assert!(second.expect_msg().await.ends_with("5\r\nthree\r\n"));
    first.expect_no_frame_short().await;

    let inner = scenario.broker().inner.lock().await;
    let consumer = inner
        .consumers
        .get("queue-workers-6f72646572732f2a")
        .unwrap();
    assert_eq!(consumer.members.len(), 1);
    assert!(consumer.members.values().any(|member| member.sid == "b"));
}
#[tokio::test]
async fn transient_unsub_with_max_receives_one_more_live_message_then_detaches() {
    let scenario = Scenario::new();
    let mut subscriber = scenario.connect_durable("client1", 25).await;
    let mut publisher = scenario.connect_durable("publisher1", 25).await;

    subscriber
        .subscribe("_MORROW/INBOX/client1/1", "inbox1")
        .await;
    subscriber.ping_roundtrip().await;
    subscriber.write_line("UNSUB inbox1 1").await;
    publisher.publish("_MORROW/INBOX/client1/1", b"one").await;
    let first = subscriber.expect_msg().await;
    assert_eq!(first, "DELIVER _MORROW/INBOX/client1/1 inbox1 3\r\none\r\n");

    publisher.publish("_MORROW/INBOX/client1/1", b"two").await;
    publisher.ping_roundtrip().await;
    subscriber.expect_no_frame_short().await;
    assert!(
        scenario
            .broker()
            .transient
            .lock()
            .await
            .subscriptions
            .is_empty()
    );
}
#[tokio::test]
async fn route_origin_publish_delivers_only_to_transient_subscribers() {
    let scenario = Scenario::new();
    let mut transient = scenario.connect().await;
    let mut durable = scenario.connect_durable("client1", 25).await;

    transient.write_line("CONN {}").await;
    transient.subscribe("orders/*", "sid1").await;
    durable.subscribe("orders/*", "durable1").await;
    transient.ping_roundtrip().await;
    durable.ping_roundtrip().await;

    scenario
        .broker()
        .deliver_route_publish("orders/created", None, b"hello")
        .await
        .unwrap();

    let frame = transient.expect_msg().await;
    assert_eq!(frame, "DELIVER orders/created sid1 5\r\nhello\r\n");
    durable.expect_no_frame_short().await;
    let inner = scenario.broker().inner.lock().await;
    assert!(inner.messages.is_empty());
    assert!(
        inner.consumers["durable-client1-durable1"]
            .pending
            .is_empty()
    );
}
#[tokio::test]
async fn disconnected_in_flight_message_redelivers_after_reconnect() {
    let scenario = Scenario::new();
    let mut subscriber = scenario.connect_durable("client1", 25).await;
    let mut publisher = scenario.connect_durable("publisher1", 25).await;

    subscriber.subscribe("orders/*", "sid1").await;
    subscriber.ping_roundtrip().await;
    publisher.publish("orders/created", b"hello").await;
    let first = subscriber.expect_msg().await;
    assert!(first.starts_with("DELIVER orders/created sid1 _MORROW/ACK/durable-client1-sid1/1/1"));
    subscriber.disconnect().await;

    scenario.advance_ms(25);
    let mut subscriber = scenario.connect_durable("client1", 25).await;
    subscriber.subscribe("orders/*", "sid1").await;
    subscriber.ping_roundtrip().await;
    scenario.tick_redelivery().await;

    let redelivery = subscriber.expect_msg().await;
    assert!(
        redelivery.starts_with("DELIVER orders/created sid1 _MORROW/ACK/durable-client1-sid1/1/2")
    );
    assert!(redelivery.ends_with("5\r\nhello\r\n"));
}
#[tokio::test]
async fn disconnected_in_flight_message_does_not_redeliver_before_deadline() {
    let scenario = Scenario::new();
    let mut subscriber = scenario.connect_durable("client1", 25).await;
    let mut publisher = scenario.connect_durable("publisher1", 25).await;

    subscriber.subscribe("orders/*", "sid1").await;
    subscriber.ping_roundtrip().await;
    publisher.publish("orders/created", b"hello").await;
    let first = subscriber.expect_msg().await;
    assert!(first.starts_with("DELIVER orders/created sid1 _MORROW/ACK/durable-client1-sid1/1/1"));
    subscriber.disconnect().await;

    scenario.advance_ms(24);
    let mut subscriber = scenario.connect_durable("client1", 25).await;
    subscriber.subscribe("orders/*", "sid1").await;
    subscriber.ping_roundtrip().await;
    scenario.tick_redelivery().await;
    subscriber.expect_no_frame_short().await;
    {
        let inner = scenario.broker().inner.lock().await;
        let consumer = inner.consumers.get("durable-client1-sid1").unwrap();
        assert!(consumer.pending.is_empty());
        assert_eq!(consumer.in_flight.get(&1).unwrap().delivery_id, 1);
    }

    scenario.advance_ms(1);
    scenario.tick_redelivery().await;
    let redelivery = subscriber.expect_msg().await;
    assert!(
        redelivery.starts_with("DELIVER orders/created sid1 _MORROW/ACK/durable-client1-sid1/1/2")
    );
}
#[tokio::test]
async fn ack_after_reconnect_survives_restart() {
    let mut scenario = Scenario::new();
    let mut subscriber = scenario.connect_durable("client1", 25).await;
    let mut publisher = scenario.connect_durable("publisher1", 25).await;

    subscriber.subscribe("orders/*", "sid1").await;
    subscriber.ping_roundtrip().await;
    publisher.publish("orders/created", b"hello").await;
    let first = subscriber.expect_msg().await;
    assert!(first.starts_with("DELIVER orders/created sid1 _MORROW/ACK/durable-client1-sid1/1/1"));
    subscriber.disconnect().await;

    scenario.advance_ms(25);
    let mut subscriber = scenario.connect_durable("client1", 25).await;
    subscriber.subscribe("orders/*", "sid1").await;
    subscriber.ping_roundtrip().await;
    scenario.tick_redelivery().await;
    let redelivery = subscriber.expect_msg().await;
    assert!(
        redelivery.starts_with("DELIVER orders/created sid1 _MORROW/ACK/durable-client1-sid1/1/2")
    );
    publisher.ack(&ack_subject(&redelivery)).await;
    publisher.ping_roundtrip().await;
    subscriber.disconnect().await;
    publisher.disconnect().await;

    scenario.restart_broker().await;
    {
        let inner = scenario.broker().inner.lock().await;
        let consumer = &inner.consumers["durable-client1-sid1"];
        assert_eq!(consumer.cursors.committed_offset("orders", 0), Some(1));
        assert!(consumer.pending.is_empty());
    }
    let mut subscriber = scenario.connect_durable("client1", 25).await;
    subscriber.subscribe("orders/*", "sid1").await;
    subscriber.ping_roundtrip().await;
    scenario.advance_ms(1_000);
    scenario.tick_redelivery().await;
    subscriber.expect_no_frame_short().await;

    let inner = scenario.broker().inner.lock().await;
    let consumer = inner.consumers.get("durable-client1-sid1").unwrap();
    assert!(consumer.pending.is_empty());
    assert!(consumer.in_flight.is_empty());
    assert_eq!(consumer.cursors.committed_offset("orders", 0), Some(1));
}
#[tokio::test]
async fn fake_cluster_runtime_drives_morrow_flow_across_100_nodes() {
    let scenario = Scenario::new_fake_cluster(100);
    let mut subscriber = scenario.connect_durable("client1", 25).await;
    let mut publisher = scenario.connect_durable("publisher1", 25).await;

    assert_eq!(scenario.fake_cluster().node_count(), 100);
    assert_eq!(scenario.broker().cluster_leader().await, Some(1));

    subscriber.subscribe("orders/*", "sid1").await;
    subscriber.ping_roundtrip().await;
    publisher.publish("orders/created", b"hello").await;

    let delivery = subscriber.expect_msg().await;
    assert!(
        delivery.starts_with("DELIVER orders/created sid1 _MORROW/ACK/durable-client1-sid1/1/1")
    );
    assert!(delivery.ends_with("5\r\nhello\r\n"));

    publisher.ack(&ack_subject(&delivery)).await;
    publisher.ping_roundtrip().await;

    let inner = scenario.broker().inner.lock().await;
    let consumer = inner.consumers.get("durable-client1-sid1").unwrap();
    assert!(consumer.pending.is_empty());
    assert!(consumer.in_flight.is_empty());
    assert_eq!(consumer.cursors.committed_offset("orders", 0), Some(1));
    assert_eq!(inner.messages[&1].stream.as_deref(), Some("orders"));
    drop(inner);
    assert_eq!(scenario.fake_cluster().write_count(), 2);
    assert_eq!(scenario.fake_cluster().data_write_count(), 1);
    assert!(
        scenario
            .fake_cluster()
            .inner
            .lock()
            .unwrap()
            .state
            .messages
            .is_empty()
    );
}
#[tokio::test]
async fn http_cluster_endpoint_reports_standalone_node() {
    let scenario = Scenario::new();

    let response = http_request(scenario.broker(), "/cluster").await;

    assert!(response.starts_with("HTTP/1.1 200 OK\r\n"));
    assert!(response.contains("\"cluster_size\":1"));
    assert!(response.contains("\"cluster_status\":\"standalone\""));
    assert!(response.contains("\"node_id\":null"));
    assert!(response.contains("\"role\":\"standalone\""));
    assert!(response.contains("\"leader_id\":null"));
    assert!(response.contains("\"peers\":[]"));
}
#[tokio::test]
async fn http_cluster_endpoint_reports_cluster_role_and_leader() {
    let scenario = Scenario::new_fake_cluster_local_node(3, 1, Some(1));

    let response = http_request(scenario.broker(), "/cluster").await;

    assert!(response.starts_with("HTTP/1.1 200 OK\r\n"));
    assert!(response.contains("\"cluster_size\":3"));
    assert!(response.contains("\"cluster_status\":\"ready\""));
    assert!(response.contains("\"node_id\":1"));
    assert!(response.contains("\"role\":\"leader\""));
    assert!(response.contains("\"leader_id\":1"));
}
#[tokio::test]
async fn cluster_response_reports_follower_role_and_leader() {
    let scenario = Scenario::new_fake_cluster_local_node(3, 2, Some(1));

    let status = scenario.broker().cluster_response().await;

    assert_eq!(status.cluster_size, 3);
    assert_eq!(status.cluster_status, "ready");
    assert_eq!(status.node_id, Some(2));
    assert_eq!(status.role, "follower");
    assert_eq!(status.leader_id, Some(1));
}
#[tokio::test]
async fn cluster_response_reports_forming_without_leader() {
    let scenario = Scenario::new_fake_cluster_local_node(3, 2, None);

    let status = scenario.broker().cluster_response().await;

    assert_eq!(status.cluster_size, 3);
    assert_eq!(status.cluster_status, "forming");
    assert_eq!(status.node_id, Some(2));
    assert_eq!(status.role, "unknown");
    assert_eq!(status.leader_id, None);
}

fn wal_segment_count(dir: &std::path::Path) -> usize {
    std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.path().extension().and_then(|ext| ext.to_str()) == Some("wal"))
        .count()
}
