use super::*;

#[tokio::test]
async fn client_can_subscribe_publish_receive_and_ack_against_server() {
    let harness = Harness::start().await;
    let mut subscriber = Client::connect(harness.addr, harness.max_payload)
        .await
        .unwrap();
    let info = subscriber.read_info().await.unwrap();
    assert!(!info.auth_required);
    assert!(info.nonce.is_none());
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
    publisher.publish("orders.created", b"hello").await.unwrap();

    let message = subscriber.next_message().await.unwrap();
    assert_eq!(message.subject, "orders.created");
    assert_eq!(message.sid, "sid1");
    assert_eq!(message.payload, b"hello");
    for (name, expected) in [
        ("Broker-Stream", "orders"),
        ("Broker-Partition", "0"),
        ("Broker-Offset", "0"),
        ("Broker-Attempt", "1"),
    ] {
        assert!(
            message
                .headers
                .iter()
                .any(|(header, value)| header == name && value == expected),
            "missing {name} delivery metadata"
        );
    }
    let ack_subject = message
        .ack_subject
        .expect("durable messages carry ack subject");
    subscriber.ack(&ack_subject).await.unwrap();
    subscriber.ping_roundtrip().await.unwrap();

    harness.shutdown().await;
}
#[tokio::test]
async fn client_request_receives_response_from_durable_responder() {
    let harness = Harness::start().await;
    let mut responder = Client::connect(harness.addr, harness.max_payload)
        .await
        .unwrap();
    responder.read_info().await.unwrap();
    responder
        .connect_durable("responder1", false, 5_000, 16)
        .await
        .unwrap();
    responder.subscribe("service.echo", "sid1").await.unwrap();
    responder.ping_roundtrip().await.unwrap();

    let responder_task = tokio::spawn(async move {
        let message = responder.next_message().await.unwrap();
        assert_eq!(message.subject, "service.echo");
        assert_eq!(message.payload, b"hello");
        assert!(
            message
                .reply_to
                .as_deref()
                .is_some_and(|reply| reply.starts_with("_INBOX."))
        );
        assert!(
            message
                .ack_subject
                .as_deref()
                .is_some_and(|ack| ack.starts_with("_BROKER.ACK."))
        );
        responder.respond(&message, b"world").await.unwrap();
        responder
            .ack(message.ack_subject.as_deref().unwrap())
            .await
            .unwrap();
    });

    let mut requester = Client::connect(harness.addr, harness.max_payload)
        .await
        .unwrap();
    requester.read_info().await.unwrap();
    requester
        .connect_durable("requester1", false, 5_000, 16)
        .await
        .unwrap();
    let response = requester
        .request("service.echo", b"hello", Duration::from_secs(3))
        .await
        .unwrap();
    assert!(response.subject.starts_with("_INBOX."));
    assert_eq!(response.payload, b"world");
    assert!(response.ack_subject.is_none());
    responder_task.await.unwrap();

    harness.shutdown().await;
}
#[tokio::test]
async fn authenticated_client_can_subscribe_publish_receive_and_ack() {
    let subscriber_auth = ClientAuth::from_seed("subscriber1", [7; 32]);
    let publisher_auth = ClientAuth::from_seed("publisher1", [8; 32]);
    let harness = Harness::start_with_auth(&[&subscriber_auth, &publisher_auth]).await;

    let mut subscriber = Client::connect(harness.addr, harness.max_payload)
        .await
        .unwrap();
    let info = subscriber.read_info().await.unwrap();
    assert!(info.auth_required);
    assert_eq!(info.nonce.as_ref().unwrap().len(), 64);
    subscriber
        .connect_authenticated(&info, &subscriber_auth, false, 5_000, 16)
        .await
        .unwrap();
    subscriber.subscribe("orders.*", "sid1").await.unwrap();
    subscriber.ping_roundtrip().await.unwrap();

    let mut publisher = Client::connect(harness.addr, harness.max_payload)
        .await
        .unwrap();
    let info = publisher.read_info().await.unwrap();
    publisher
        .connect_authenticated(&info, &publisher_auth, false, 5_000, 16)
        .await
        .unwrap();
    publisher.publish("orders.created", b"hello").await.unwrap();

    let message = subscriber.next_message().await.unwrap();
    assert_eq!(message.subject, "orders.created");
    assert_eq!(message.sid, "sid1");
    assert_eq!(message.payload, b"hello");
    let ack_subject = message
        .ack_subject
        .expect("durable messages carry ack subject");
    subscriber.ack(&ack_subject).await.unwrap();
    subscriber.ping_roundtrip().await.unwrap();

    harness.shutdown().await;
}
#[tokio::test]
async fn authenticated_client_with_permissions_can_subscribe_publish_receive_and_ack() {
    let subscriber_auth = ClientAuth::from_seed("subscriber1", [7; 32]);
    let publisher_auth = ClientAuth::from_seed("publisher1", [8; 32]);
    let harness = Harness::start_with_config(
        auth_config_with_permissions(vec![
            (&subscriber_auth, None, Some(vec!["orders.*".to_string()])),
            (&publisher_auth, Some(vec!["orders.>".to_string()]), None),
        ]),
        None,
    )
    .await;

    let mut subscriber = Client::connect(harness.addr, harness.max_payload)
        .await
        .unwrap();
    let info = subscriber.read_info().await.unwrap();
    subscriber
        .connect_authenticated(&info, &subscriber_auth, false, 5_000, 16)
        .await
        .unwrap();
    subscriber.subscribe("orders.*", "sid1").await.unwrap();
    subscriber.ping_roundtrip().await.unwrap();

    let mut publisher = Client::connect(harness.addr, harness.max_payload)
        .await
        .unwrap();
    let info = publisher.read_info().await.unwrap();
    publisher
        .connect_authenticated(&info, &publisher_auth, false, 5_000, 16)
        .await
        .unwrap();
    publisher.publish("orders.created", b"hello").await.unwrap();

    let message = subscriber.next_message().await.unwrap();
    assert_eq!(message.subject, "orders.created");
    assert_eq!(message.sid, "sid1");
    assert_eq!(message.payload, b"hello");
    subscriber
        .ack(message.ack_subject.as_deref().unwrap())
        .await
        .unwrap();

    harness.shutdown().await;
}
#[tokio::test]
async fn authenticated_permissions_reject_unauthorized_subscribe_and_publish() {
    let subscriber_auth = ClientAuth::from_seed("subscriber1", [7; 32]);
    let publisher_auth = ClientAuth::from_seed("publisher1", [8; 32]);
    let harness = Harness::start_with_config(
        auth_config_with_permissions(vec![
            (&subscriber_auth, None, Some(vec!["orders.*".to_string()])),
            (&publisher_auth, Some(vec!["orders.*".to_string()]), None),
        ]),
        None,
    )
    .await;

    let mut subscriber = Client::connect(harness.addr, harness.max_payload)
        .await
        .unwrap();
    let info = subscriber.read_info().await.unwrap();
    subscriber
        .connect_authenticated(&info, &subscriber_auth, false, 5_000, 16)
        .await
        .unwrap();
    subscriber.subscribe("events.*", "sid1").await.unwrap();
    match subscriber.next_frame().await.unwrap().unwrap() {
        ServerFrame::Err(error) => assert!(error.contains("subscribe not authorized")),
        frame => panic!("expected subscribe auth error, got {frame:?}"),
    }

    let mut publisher = Client::connect(harness.addr, harness.max_payload)
        .await
        .unwrap();
    let info = publisher.read_info().await.unwrap();
    publisher
        .connect_authenticated(&info, &publisher_auth, false, 5_000, 16)
        .await
        .unwrap();
    publisher
        .publish("events.created", b"blocked")
        .await
        .unwrap();
    match publisher.next_frame().await.unwrap().unwrap() {
        ServerFrame::Err(error) => assert!(error.contains("publish not authorized")),
        frame => panic!("expected publish auth error, got {frame:?}"),
    }

    harness.shutdown().await;
}
async fn authenticated_connect_rejects_invalid_signature() {
    let configured_auth = ClientAuth::from_seed("client1", [7; 32]);
    let wrong_auth = ClientAuth::from_seed("client1", [9; 32]);
    let harness = Harness::start_with_auth(&[&configured_auth]).await;

    let mut client = Client::connect(harness.addr, harness.max_payload)
        .await
        .unwrap();
    let info = client.read_info().await.unwrap();
    client
        .connect_authenticated(&info, &wrong_auth, false, 5_000, 16)
        .await
        .unwrap();

    match client.next_frame().await.unwrap().unwrap() {
        ServerFrame::Err(error) => assert!(error.contains("invalid public key signature")),
        frame => panic!("expected auth error, got {frame:?}"),
    }

    harness.shutdown().await;
}
#[tokio::test]
async fn auth_nonce_is_fresh_per_connection() {
    let auth = ClientAuth::from_seed("client1", [7; 32]);
    let harness = Harness::start_with_auth(&[&auth]).await;

    let mut first = Client::connect(harness.addr, harness.max_payload)
        .await
        .unwrap();
    let first_info = first.read_info().await.unwrap();
    let mut second = Client::connect(harness.addr, harness.max_payload)
        .await
        .unwrap();
    let second_info = second.read_info().await.unwrap();

    assert!(first_info.auth_required);
    assert!(second_info.auth_required);
    assert_ne!(first_info.nonce, second_info.nonce);

    harness.shutdown().await;
}
#[tokio::test]
async fn tls_client_can_subscribe_publish_receive_and_ack() {
    let harness = Harness::start_tls().await;
    let mut subscriber = Client::connect_tls(
        harness.addr,
        TLS_SERVER_NAME,
        tls_ca_cert_file(),
        harness.max_payload,
    )
    .await
    .unwrap();
    let info = subscriber.read_info().await.unwrap();
    assert!(!info.auth_required);
    subscriber
        .connect_durable("subscriber1", false, 5_000, 16)
        .await
        .unwrap();
    subscriber.subscribe("orders.*", "sid1").await.unwrap();
    subscriber.ping_roundtrip().await.unwrap();

    let mut publisher = Client::connect_tls(
        harness.addr,
        TLS_SERVER_NAME,
        tls_ca_cert_file(),
        harness.max_payload,
    )
    .await
    .unwrap();
    publisher.read_info().await.unwrap();
    publisher
        .connect_durable("publisher1", false, 5_000, 16)
        .await
        .unwrap();
    publisher.publish("orders.created", b"hello").await.unwrap();

    let message = subscriber.next_message().await.unwrap();
    assert_eq!(message.subject, "orders.created");
    assert_eq!(message.sid, "sid1");
    assert_eq!(message.payload, b"hello");
    let ack_subject = message
        .ack_subject
        .expect("durable messages carry ack subject");
    subscriber.ack(&ack_subject).await.unwrap();
    subscriber.ping_roundtrip().await.unwrap();

    harness.shutdown().await;
}
#[tokio::test]
async fn tls_authenticated_client_can_subscribe_publish_receive_and_ack() {
    let subscriber_auth = ClientAuth::from_seed("subscriber1", [7; 32]);
    let publisher_auth = ClientAuth::from_seed("publisher1", [8; 32]);
    let harness = Harness::start_tls_with_auth(&[&subscriber_auth, &publisher_auth]).await;

    let mut subscriber = Client::connect_tls(
        harness.addr,
        TLS_SERVER_NAME,
        tls_ca_cert_file(),
        harness.max_payload,
    )
    .await
    .unwrap();
    let info = subscriber.read_info().await.unwrap();
    assert!(info.auth_required);
    subscriber
        .connect_authenticated(&info, &subscriber_auth, false, 5_000, 16)
        .await
        .unwrap();
    subscriber.subscribe("orders.*", "sid1").await.unwrap();
    subscriber.ping_roundtrip().await.unwrap();

    let mut publisher = Client::connect_tls(
        harness.addr,
        TLS_SERVER_NAME,
        tls_ca_cert_file(),
        harness.max_payload,
    )
    .await
    .unwrap();
    let info = publisher.read_info().await.unwrap();
    publisher
        .connect_authenticated(&info, &publisher_auth, false, 5_000, 16)
        .await
        .unwrap();
    publisher.publish("orders.created", b"hello").await.unwrap();

    let message = subscriber.next_message().await.unwrap();
    assert_eq!(message.subject, "orders.created");
    assert_eq!(message.sid, "sid1");
    assert_eq!(message.payload, b"hello");
    let ack_subject = message
        .ack_subject
        .expect("durable messages carry ack subject");
    subscriber.ack(&ack_subject).await.unwrap();
    subscriber.ping_roundtrip().await.unwrap();

    harness.shutdown().await;
}
#[tokio::test]
async fn plain_client_does_not_complete_info_against_tls_listener() {
    let harness = Harness::start_tls().await;
    let mut client = Client::connect(harness.addr, harness.max_payload)
        .await
        .unwrap();

    let read = tokio::time::timeout(Duration::from_secs(1), client.read_info()).await;
    assert!(
        read.is_err() || read.unwrap().is_err(),
        "plain client unexpectedly read INFO from TLS listener"
    );

    harness.shutdown().await;
}
#[tokio::test]
async fn clustered_follower_proxies_client_to_leader() {
    let harness = ClusterHarness::start_three().await;
    let leader = harness.wait_for_leader().await;
    let follower = harness
        .nodes
        .iter()
        .find(|node| node.node_id != leader)
        .expect("three node cluster has a follower");
    harness
        .wait_until_follower_knows_leader(follower.node_id, leader)
        .await;

    let mut subscriber = Client::connect(follower.client_addr, harness.max_payload)
        .await
        .unwrap();
    subscriber.read_info().await.unwrap();
    subscriber
        .connect_durable("subscriber1", false, 5_000, 16)
        .await
        .unwrap();
    subscriber.subscribe("orders.*", "sid1").await.unwrap();
    subscriber.ping_roundtrip().await.unwrap();

    let mut publisher = Client::connect(follower.client_addr, harness.max_payload)
        .await
        .unwrap();
    publisher.read_info().await.unwrap();
    publisher
        .connect_durable("publisher1", false, 5_000, 16)
        .await
        .unwrap();
    publisher.publish("orders.created", b"hello").await.unwrap();

    let message = tokio::time::timeout(Duration::from_secs(5), subscriber.next_message())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(message.subject, "orders.created");
    assert_eq!(message.payload, b"hello");
    subscriber
        .ack(message.ack_subject.as_deref().unwrap())
        .await
        .unwrap();

    harness.shutdown().await;
}
#[tokio::test]
async fn routed_cluster_forms_full_mesh_and_forwards_transient_publish() {
    let harness = ClusterHarness::start_three_routed().await;
    harness.wait_for_full_route_mesh().await;

    let mut subscriber = Client::connect(harness.nodes[1].client_addr, harness.max_payload)
        .await
        .unwrap();
    subscriber.read_info().await.unwrap();
    subscriber.connect_transient(false).await.unwrap();
    subscriber.subscribe("orders.*", "sid1").await.unwrap();
    subscriber.ping_roundtrip().await.unwrap();

    harness.wait_for_route_interest(0, "orders.*").await;

    let mut publisher = Client::connect(harness.nodes[2].client_addr, harness.max_payload)
        .await
        .unwrap();
    publisher.read_info().await.unwrap();
    publisher.connect_transient(false).await.unwrap();
    publisher.publish("orders.created", b"hello").await.unwrap();

    let message = tokio::time::timeout(Duration::from_secs(5), subscriber.next_message())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(message.subject, "orders.created");
    assert_eq!(message.sid, "sid1");
    assert_eq!(message.payload, b"hello");
    assert!(message.ack_subject.is_none());

    harness.shutdown().await;
}
#[tokio::test]
async fn routed_cluster_forwards_inbox_request_reply() {
    let harness = ClusterHarness::start_three_routed().await;
    harness.wait_for_full_route_mesh().await;

    let inbox = "_INBOX.requester.1";
    let mut requester = Client::connect(harness.nodes[0].client_addr, harness.max_payload)
        .await
        .unwrap();
    requester.read_info().await.unwrap();
    requester.connect_transient(false).await.unwrap();
    requester.subscribe(inbox, "reply").await.unwrap();
    requester.ping_roundtrip().await.unwrap();

    let mut responder = Client::connect(harness.nodes[1].client_addr, harness.max_payload)
        .await
        .unwrap();
    responder.read_info().await.unwrap();
    responder.connect_transient(false).await.unwrap();
    responder.subscribe("service.echo", "svc").await.unwrap();
    responder.ping_roundtrip().await.unwrap();

    harness.wait_for_route_interest(2, "service.echo").await;
    harness.wait_for_route_interest(2, inbox).await;

    let mut publisher = Client::connect(harness.nodes[2].client_addr, harness.max_payload)
        .await
        .unwrap();
    publisher.read_info().await.unwrap();
    publisher.connect_transient(false).await.unwrap();
    publisher
        .publish_with_reply("service.echo", Some(inbox), b"hello")
        .await
        .unwrap();

    let request = tokio::time::timeout(Duration::from_secs(5), responder.next_message())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(request.subject, "service.echo");
    assert_eq!(request.reply_to.as_deref(), Some(inbox));
    responder.respond(&request, b"world").await.unwrap();

    let response = tokio::time::timeout(Duration::from_secs(5), requester.next_message())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(response.subject, inbox);
    assert_eq!(response.payload, b"world");

    harness.shutdown().await;
}
