use super::*;

#[tokio::test]
async fn http_connections_endpoint_reports_live_client_metadata() {
    let scenario = Scenario::new();
    let mut client = scenario.connect_durable("client1", 25).await;
    client.subscribe("orders.*", "sid1").await;
    client.subscribe("_INBOX.client1.1", "inbox1").await;
    client.ping_roundtrip().await;

    let response = http_request(scenario.broker(), "/connections").await;

    assert!(response.starts_with("HTTP/1.1 200 OK\r\n"));
    assert!(response.contains("\"count\":1"));
    assert!(response.contains("\"id\":1"));
    assert!(response.contains("\"remote_addr\":null"));
    assert!(response.contains("\"durable_id\":\"client1\""));
    assert!(response.contains("\"authenticated\":false"));
    assert!(response.contains("\"connected_at_ms\":1000"));
    assert!(response.contains("\"ack_timeout_ms\":25"));
    assert!(response.contains("\"max_in_flight\":1024"));
    assert!(response.contains("\"subscriptions\":1"));
    assert!(response.contains("\"transient_subscriptions\":1"));
}
#[tokio::test]
async fn http_subscriptions_endpoint_reports_durable_and_transient_state() {
    let scenario = Scenario::new();
    let mut first = scenario.connect_durable("client1", 25).await;
    let mut second = scenario.connect_durable("client2", 50).await;
    first.subscribe("orders.*", "sid1").await;
    first.subscribe("_INBOX.client1.1", "inbox1").await;
    second
        .subscribe_queue("orders.*", "workers", "worker1")
        .await;
    first.ping_roundtrip().await;
    second.ping_roundtrip().await;

    let response = http_request(scenario.broker(), "/subscriptions").await;

    assert!(response.starts_with("HTTP/1.1 200 OK\r\n"));
    assert!(response.contains("\"durable_consumers\""));
    assert!(response.contains("\"consumer_id\":\"durable-client1-sid1\""));
    assert!(response.contains("\"filter_subject\":\"orders.*\""));
    assert!(response.contains("\"queue_group\":null"));
    assert!(response.contains("\"connection_id\":1"));
    assert!(response.contains("\"sid\":\"sid1\""));
    assert!(response.contains("\"consumer_id\":\"queue-workers-6f72646572732e2a\""));
    assert!(response.contains("\"queue_group\":\"workers\""));
    assert!(response.contains("\"sid\":\"worker1\""));
    assert!(response.contains("\"transient_subscriptions\""));
    assert!(response.contains("\"subject\":\"_INBOX.client1.1\""));
    assert!(response.contains("\"sid\":\"inbox1\""));
}

#[tokio::test]
async fn http_streams_endpoint_reports_effective_bindings() {
    let mut scenario = Scenario::new();
    scenario.broker.config.streams =
        crate::stream::StreamCatalog::new(vec![crate::stream::StreamDefinition {
            name: crate::stream::StreamId::new("orders").unwrap(),
            subjects: vec!["orders.>".to_string()],
            partitions: 8,
            partitioning: Default::default(),
            storage: Default::default(),
            retention: Default::default(),
        }])
        .unwrap();

    let response = http_request(scenario.broker(), "/streams").await;

    assert!(response.starts_with("HTTP/1.1 200 OK\r\n"));
    assert!(response.contains("\"name\":\"orders\""));
    assert!(response.contains("\"subjects\":[\"orders.>\"]"));
    assert!(response.contains("\"partitions\":8"));
}
#[tokio::test]
async fn http_status_and_unknown_paths_return_not_found() {
    let scenario = Scenario::new();

    let status = http_request(scenario.broker(), "/status").await;
    let unknown = http_request(scenario.broker(), "/nope").await;

    assert!(status.starts_with("HTTP/1.1 404 Not Found\r\n"));
    assert!(status.ends_with("{\"error\":\"not found\"}"));
    assert!(unknown.starts_with("HTTP/1.1 404 Not Found\r\n"));
    assert!(unknown.ends_with("{\"error\":\"not found\"}"));
}

#[tokio::test]
async fn http_status_requires_valid_bearer_token() {
    let scenario = Scenario::new();

    let missing = http_request_with_auth(scenario.broker(), "/cluster", None).await;
    let wrong = http_request_with_auth(scenario.broker(), "/cluster", Some("wrong")).await;
    let ok = http_request(scenario.broker(), "/cluster").await;

    assert!(missing.starts_with("HTTP/1.1 401 Unauthorized\r\n"));
    assert!(missing.contains("www-authenticate: Bearer"));
    assert!(wrong.starts_with("HTTP/1.1 401 Unauthorized\r\n"));
    assert!(ok.starts_with("HTTP/1.1 200 OK\r\n"));
}

#[tokio::test]
async fn http_wal_endpoint_requires_auth_and_reports_metrics() {
    let scenario = Scenario::new_with_wal_segment_bytes(128);
    let mut subscriber = scenario.connect_durable("client1", 25).await;
    let mut publisher = scenario.connect_durable("publisher1", 25).await;
    subscriber.subscribe("orders.*", "sid1").await;
    subscriber.ping_roundtrip().await;
    publisher.publish("orders.created", b"hello").await;
    let frame = subscriber.expect_msg().await;
    publisher.publish(&ack_subject(&frame), b"").await;
    publisher.ping_roundtrip().await;

    let missing = http_request_with_auth(scenario.broker(), "/wal", None).await;
    let ok = http_request(scenario.broker(), "/wal").await;

    assert!(missing.starts_with("HTTP/1.1 401 Unauthorized\r\n"));
    assert!(ok.starts_with("HTTP/1.1 200 OK\r\n"));
    assert!(ok.contains("\"active_segment_id\""));
    assert!(ok.contains("\"active_segment_path\""));
    assert!(ok.contains("\"active_segment_bytes\""));
    assert!(ok.contains("\"sealed_segment_count\""));
    assert!(ok.contains("\"total_wal_bytes\""));
    assert!(ok.contains("\"retained_message_count\":1"));
    assert!(ok.contains("\"consumer_count\":1"));
    assert!(ok.contains("\"next_seq\":2"));
    assert!(ok.contains("\"next_delivery_id\":2"));
    assert!(ok.contains("\"last_replay_duration_ms\""));
    assert!(ok.contains("\"last_checkpoint_duration_ms\""));
    assert!(ok.contains("\"last_fsync_duration_ms\""));
    assert!(ok.contains("\"rotations\""));
    assert!(ok.contains("\"checkpoints\""));
    assert!(ok.contains("\"truncations\""));
    assert!(ok.contains("\"deleted_segments\""));
}

#[tokio::test]
async fn route_frame_rejects_invalid_auth_token() {
    let frame = AuthenticatedRouteFrame {
        auth_token: "wrong-token".into(),
        frame: RouteFrame::Ping,
    };
    let payload = serde_json::to_vec(&frame).unwrap();
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    bytes.extend_from_slice(&payload);
    let mut reader = &bytes[..];

    let err = read_route_frame(&mut reader, "right-token")
        .await
        .unwrap_err();
    assert!(err.to_string().contains("invalid route auth token"));
}
#[tokio::test]
async fn fake_cluster_follower_without_known_leader_returns_error() {
    let scenario = Scenario::new_fake_cluster_local_node(3, 1, None);
    let (client_stream, server_stream) = tokio::io::duplex(4096);
    let broker = scenario.broker().clone();
    let task = tokio::spawn(async move {
        broker
            .handle_accepted_for_test(server_stream)
            .await
            .unwrap();
    });
    let mut client = BufReader::new(client_stream);
    let mut frame = Vec::new();

    client.read_until(b'\n', &mut frame).await.unwrap();

    let frame = String::from_utf8(frame).unwrap();
    assert!(frame.starts_with("-ERR "), "expected -ERR, got {frame:?}");
    assert!(frame.contains("no known leader"));
    task.await.unwrap();
}
#[tokio::test]
async fn fake_cluster_follower_proxies_raw_bytes_to_known_leader() {
    let scenario = Scenario::new_fake_cluster_local_node(3, 1, Some(2));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    scenario.set_client_addr(2, listener.local_addr().unwrap());
    let leader_task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = [0; 5];
        stream.read_exact(&mut request).await.unwrap();
        assert_eq!(&request, b"ping!");
        stream.write_all(b"pong!").await.unwrap();
    });
    let (mut client_stream, server_stream) = tokio::io::duplex(4096);
    let broker = scenario.broker().clone();
    let broker_task = tokio::spawn(async move {
        broker
            .handle_accepted_for_test(server_stream)
            .await
            .unwrap();
    });

    client_stream.write_all(b"ping!").await.unwrap();
    let mut response = [0; 5];
    client_stream.read_exact(&mut response).await.unwrap();

    assert_eq!(&response, b"pong!");
    drop(client_stream);
    leader_task.await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), broker_task)
        .await
        .unwrap()
        .unwrap();
}
#[tokio::test]
async fn fake_cluster_leader_change_from_remote_to_local_handles_protocol_locally() {
    let scenario = Scenario::new_fake_cluster_local_node(3, 1, Some(2));
    scenario.set_leader(Some(1));
    let mut subscriber = scenario.connect_accepted().await;
    subscriber.send_durable_connect("client1", 25).await;
    let mut publisher = scenario.connect_accepted().await;
    publisher.send_durable_connect("publisher1", 25).await;

    subscriber.subscribe("orders.*", "sid1").await;
    subscriber.ping_roundtrip().await;
    publisher.publish("orders.created", b"hello").await;

    let delivery = subscriber.expect_msg().await;
    assert!(delivery.starts_with("MSG orders.created sid1 _BROKER.ACK.durable-client1-sid1.1.1"));
    assert!(delivery.ends_with("5\r\nhello\r\n"));
}
#[tokio::test]
async fn fake_cluster_local_leader_accepts_durable_flow_through_accepted_path() {
    let scenario = Scenario::new_fake_cluster_local_node(5, 1, Some(1));
    let mut subscriber = scenario.connect_accepted().await;
    subscriber.send_durable_connect("client1", 25).await;
    let mut publisher = scenario.connect_accepted().await;
    publisher.send_durable_connect("publisher1", 25).await;

    subscriber.subscribe("orders.*", "sid1").await;
    subscriber.ping_roundtrip().await;
    publisher.publish("orders.created", b"hello").await;

    let delivery = subscriber.expect_msg().await;
    assert!(delivery.ends_with("5\r\nhello\r\n"));
    publisher.publish(&ack_subject(&delivery), b"").await;
    publisher.ping_roundtrip().await;

    let durable = scenario.fake_cluster().durable_state();
    let consumer = durable.consumers.get("durable-client1-sid1").unwrap();
    assert!(consumer.in_flight.is_empty());
    assert!(consumer.acked.contains(&1));
}
#[tokio::test]
async fn fake_cluster_quorum_loss_rejects_subscribe() {
    let scenario = Scenario::new_fake_cluster(5);
    scenario.partition_available([1, 2]);
    let mut subscriber = scenario.connect_durable("client1", 25).await;

    subscriber.subscribe("orders.*", "sid1").await;

    subscriber.expect_err_contains("quorum unavailable").await;
    assert!(scenario.fake_cluster().durable_state().consumers.is_empty());
    assert_eq!(scenario.fake_cluster().write_count(), 0);
}
#[tokio::test]
async fn fake_cluster_not_leader_rejects_publish() {
    let scenario = Scenario::new_fake_cluster(5);
    let mut subscriber = scenario.connect_durable("client1", 25).await;
    let mut publisher = scenario.connect_durable("publisher1", 25).await;
    subscriber.subscribe("orders.*", "sid1").await;
    subscriber.ping_roundtrip().await;

    scenario.set_leader(Some(2));
    publisher.publish("orders.created", b"hello").await;

    publisher.expect_err_contains("not leader").await;
    subscriber.expect_no_frame_short().await;
    assert_eq!(scenario.fake_cluster().write_count(), 1);
}
#[tokio::test]
async fn route_enabled_follower_rejects_durable_writes() {
    let scenario = Scenario::new_fake_route_cluster_local_node(3, 2, Some(1));
    let cluster = scenario.broker().cluster_runtime().await.unwrap();
    let err = scenario
        .broker()
        .cluster_write(
            &cluster,
            BrokerCommand::Publish {
                stream: Some("orders".into()),
                subject: "orders.created".into(),
                reply_to: None,
                payload: b"hello".to_vec(),
            },
        )
        .await
        .unwrap_err();

    assert!(err.to_string().contains("not leader"));
    assert_eq!(scenario.fake_cluster().write_count(), 0);
}
#[tokio::test]
async fn clustered_transient_publish_without_stream_binding_does_not_propose_raft() {
    let scenario = Scenario::new_fake_cluster_local_node(3, 2, Some(1));
    let mut subscriber = scenario.connect().await;
    let mut publisher = scenario.connect().await;

    subscriber.write_line("CONNECT {}").await;
    publisher.write_line("CONNECT {}").await;
    subscriber.subscribe("live.*", "sid1").await;
    subscriber.ping_roundtrip().await;

    publisher.publish("live.topic", b"hello").await;
    publisher.ping_roundtrip().await;

    let delivery = subscriber.expect_msg().await;
    assert_eq!(delivery, "MSG live.topic sid1 5\r\nhello\r\n");
    assert_eq!(scenario.fake_cluster().write_count(), 0);
}
#[tokio::test]
async fn fake_cluster_partition_blocks_then_restore_allows_write() {
    let scenario = Scenario::new_fake_cluster(5);
    let mut subscriber = scenario.connect_durable("client1", 25).await;
    let mut publisher = scenario.connect_durable("publisher1", 25).await;
    subscriber.subscribe("orders.*", "sid1").await;
    subscriber.ping_roundtrip().await;

    scenario.partition_available([1, 2]);
    publisher.publish("orders.created", b"blocked").await;
    publisher.expect_err_contains("quorum unavailable").await;
    subscriber.expect_no_frame_short().await;

    scenario.restore_all_nodes();
    publisher.publish("orders.created", b"hello").await;
    let delivery = subscriber.expect_msg().await;
    assert!(delivery.ends_with("5\r\nhello\r\n"));
}
#[tokio::test]
async fn fake_cluster_delays_consumer_upsert_until_drained() {
    let scenario = Scenario::new_fake_cluster(5);
    scenario.set_delay_writes(true);
    let mut subscriber = scenario.connect_durable("client1", 25).await;

    subscriber.subscribe("orders.*", "sid1").await;
    subscriber.write_line("PING").await;

    subscriber.expect_no_frame_short().await;
    assert_eq!(scenario.queued_write_count(), 1);
    assert!(
        !scenario
            .broker()
            .inner
            .lock()
            .await
            .consumers
            .contains_key("durable-client1-sid1")
    );

    assert!(scenario.drain_one().is_some());
    subscriber.expect_pong().await;
    assert!(
        scenario
            .broker()
            .inner
            .lock()
            .await
            .consumers
            .contains_key("durable-client1-sid1")
    );
}
#[tokio::test]
async fn fake_cluster_delays_publish_delivery_until_drained() {
    let scenario = Scenario::new_fake_cluster(5);
    let mut subscriber = scenario.connect_durable("client1", 25).await;
    let mut publisher = scenario.connect_durable("publisher1", 25).await;
    subscriber.subscribe("orders.*", "sid1").await;
    subscriber.ping_roundtrip().await;

    scenario.set_delay_writes(true);
    publisher.publish("orders.created", b"hello").await;
    publisher.write_line("PING").await;

    subscriber.expect_no_frame_short().await;
    assert_eq!(scenario.queued_write_count(), 1);
    assert!(scenario.drain_one().is_some());
    tokio::task::yield_now().await;
    subscriber.expect_no_frame_short().await;
    assert_eq!(scenario.queued_write_count(), 1);
    assert!(scenario.drain_one().is_some());

    let delivery = subscriber.expect_msg().await;
    assert!(delivery.ends_with("5\r\nhello\r\n"));
    publisher.expect_pong().await;
}
#[tokio::test]
async fn fake_cluster_delays_ack_until_drained() {
    let scenario = Scenario::new_fake_cluster(5);
    let mut subscriber = scenario.connect_durable("client1", 25).await;
    let mut publisher = scenario.connect_durable("publisher1", 25).await;
    subscriber.subscribe("orders.*", "sid1").await;
    subscriber.ping_roundtrip().await;
    publisher.publish("orders.created", b"hello").await;
    let delivery = subscriber.expect_msg().await;

    scenario.set_delay_writes(true);
    publisher.publish(&ack_subject(&delivery), b"").await;
    publisher.write_line("PING").await;
    publisher.expect_no_frame_short().await;
    assert_eq!(scenario.queued_write_count(), 1);
    {
        let durable = scenario.fake_cluster().durable_state();
        assert!(
            durable.consumers["durable-client1-sid1"]
                .in_flight
                .contains_key(&1)
        );
    }

    assert!(scenario.drain_one().is_some());
    publisher.expect_pong().await;
    let durable = scenario.fake_cluster().durable_state();
    let consumer = durable.consumers.get("durable-client1-sid1").unwrap();
    assert!(consumer.in_flight.is_empty());
    assert!(consumer.acked.contains(&1));
}
#[tokio::test]
async fn fake_cluster_leader_change_back_to_local_allows_writes() {
    let scenario = Scenario::new_fake_cluster(5);
    scenario.set_leader(Some(2));
    let mut subscriber = scenario.connect_durable("client1", 25).await;

    subscriber.subscribe("orders.*", "sid1").await;
    subscriber.expect_err_contains("not leader").await;
    assert!(scenario.fake_cluster().durable_state().consumers.is_empty());

    scenario.set_leader(Some(1));
    subscriber.subscribe("orders.*", "sid1").await;
    subscriber.ping_roundtrip().await;
    assert!(
        scenario
            .fake_cluster()
            .durable_state()
            .consumers
            .contains_key("durable-client1-sid1")
    );
}
