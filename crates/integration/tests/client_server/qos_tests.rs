use super::*;

#[tokio::test]
async fn client_publish_with_qos_receives_producer_ack() {
    let harness = Harness::start().await;

    let mut subscriber = Client::connect(harness.addr, harness.max_payload)
        .await
        .unwrap();
    subscriber.read_info().await.unwrap();
    subscriber
        .connect_durable("subscriber1", false, 5_000, 16)
        .await
        .unwrap();
    subscriber.subscribe("orders.*", "sid1").await.unwrap();
    subscriber.ping_roundtrip().await.unwrap();

    let mut publisher = Client::connect(harness.addr, harness.max_payload)
        .await
        .unwrap();
    publisher.read_info().await.unwrap();
    publisher
        .connect_durable("publisher1", false, 5_000, 16)
        .await
        .unwrap();
    let ack = publisher
        .publish_with_qos(
            "orders.created",
            None,
            b"hello",
            client::protocol::AckLevel::Durable,
            "msg-1",
        )
        .await
        .unwrap();

    assert_eq!(ack.msg_id, "msg-1");
    assert_eq!(ack.level, client::protocol::AckLevel::Durable);
    assert!(ack.retained);
    assert_eq!(ack.seq, Some(1));
    harness.shutdown().await;
}

#[tokio::test]
async fn unauthorized_qos_publish_returns_error() {
    let publisher_auth = ClientAuth::from_seed("publisher1", [8; 32]);
    let harness = Harness::start_with_config(
        auth_config_with_permissions(vec![(
            &publisher_auth,
            Some(vec!["orders.*".to_string()]),
            None,
        )]),
        None,
    )
    .await;

    let mut publisher = Client::connect(harness.addr, harness.max_payload)
        .await
        .unwrap();
    let info = publisher.read_info().await.unwrap();
    publisher
        .connect_authenticated(&info, &publisher_auth, false, 5_000, 16)
        .await
        .unwrap();
    let err = publisher
        .publish_with_qos(
            "events.created",
            None,
            b"blocked",
            client::protocol::AckLevel::Durable,
            "msg-blocked",
        )
        .await
        .unwrap_err();
    assert!(err.to_string().contains("publish not authorized"));
    harness.shutdown().await;
}
