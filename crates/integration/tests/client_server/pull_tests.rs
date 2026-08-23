use super::*;

#[tokio::test]
async fn dynamic_group_fences_old_member_and_assigns_pull_partition() {
    let harness = Harness::start().await;
    let mut first = Client::connect(harness.addr, harness.max_payload)
        .await
        .unwrap();
    first.read_info().await.unwrap();
    first
        .connect_durable("group-worker", false, 1_000, 8)
        .await
        .unwrap();
    first
        .create_consumer(
            "worker",
            "orders/*",
            client::protocol::StartPosition::Earliest,
        )
        .await
        .unwrap();
    let first_assignment = first
        .group_join(
            "worker",
            "member-a",
            1,
            client::protocol::GroupAssignmentStrategy::Sticky,
            Some("instance-a"),
        )
        .await
        .unwrap();
    assert_eq!(first_assignment.partitions, vec![0]);

    let mut second = Client::connect(harness.addr, harness.max_payload)
        .await
        .unwrap();
    second.read_info().await.unwrap();
    second
        .connect_durable("group-worker", false, 1_000, 8)
        .await
        .unwrap();
    let second_assignment = second
        .group_join(
            "worker",
            "member-b",
            1,
            client::protocol::GroupAssignmentStrategy::Sticky,
            Some("instance-b"),
        )
        .await
        .unwrap();
    assert!(second_assignment.partitions.is_empty());

    let mut publisher = Client::connect(harness.addr, harness.max_payload)
        .await
        .unwrap();
    publisher.read_info().await.unwrap();
    publisher
        .connect_durable("group-publisher", false, 5_000, 8)
        .await
        .unwrap();
    publisher
        .publish_with_qos_and_key(
            "orders/created",
            None,
            b"before-leave",
            client::protocol::AckLevel::Durable,
            "group-before-leave",
            Some("group-key-1"),
        )
        .await
        .unwrap();
    let first_assignment = first
        .group_heartbeat("worker", "member-a", first_assignment.generation)
        .await
        .unwrap();
    let first_delivery = first.fetch("worker", 1, 64, Duration::ZERO).await.unwrap();
    assert_eq!(first_delivery.len(), 1);
    first.ack_delivery(&first_delivery[0]).await.unwrap();

    first
        .group_leave("worker", "member-a", first_assignment.generation)
        .await
        .unwrap();
    let second_assignment = second
        .group_join(
            "worker",
            "member-b",
            1,
            client::protocol::GroupAssignmentStrategy::Sticky,
            Some("instance-b"),
        )
        .await
        .unwrap();
    assert_eq!(second_assignment.partitions, vec![0]);
    assert!(first.fetch("worker", 1, 64, Duration::ZERO).await.is_err());

    publisher
        .publish_with_qos_and_key(
            "orders/created",
            None,
            b"after-leave",
            client::protocol::AckLevel::Durable,
            "group-after-leave",
            Some("group-key-2"),
        )
        .await
        .unwrap();
    let second_delivery = second.fetch("worker", 1, 64, Duration::ZERO).await.unwrap();
    assert_eq!(second_delivery[0].payload, b"after-leave");
    second.ack_delivery(&second_delivery[0]).await.unwrap();
}

#[tokio::test]
async fn client_pull_consumer_supports_bounded_fetch_and_delivery_controls() {
    let harness = Harness::start().await;
    let mut consumer = Client::connect(harness.addr, harness.max_payload)
        .await
        .unwrap();
    let info = consumer.read_info().await.unwrap();
    assert_eq!(info.proto, 2);
    assert_eq!(info.protocol_versions, vec![1, 2]);
    consumer
        .connect_durable("puller", false, 1_000, 8)
        .await
        .unwrap();
    consumer
        .create_consumer(
            "worker",
            "orders/*",
            client::protocol::StartPosition::Earliest,
        )
        .await
        .unwrap();

    let mut publisher = Client::connect(harness.addr, harness.max_payload)
        .await
        .unwrap();
    publisher.read_info().await.unwrap();
    publisher
        .connect_durable("publisher", false, 5_000, 8)
        .await
        .unwrap();
    publisher
        .publish_with_qos_and_key(
            "orders/created",
            None,
            b"one",
            client::protocol::AckLevel::HighDurability,
            "pull-keyed-1",
            Some("customer-7"),
        )
        .await
        .unwrap();

    let first = consumer
        .fetch("worker", 1, 3, Duration::ZERO)
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(first.payload, b"one");
    assert_eq!(first.stream, "orders");
    assert_eq!(first.partition, 0);
    assert_eq!(first.offset, 0);
    assert_eq!(first.key.as_deref(), Some(b"customer-7".as_slice()));
    assert!(first.timestamp_ms > 0);
    assert_eq!(first.attempt, 1);

    consumer
        .extend_lease(&first, Duration::from_secs(1))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        consumer
            .fetch("worker", 1, 3, Duration::from_millis(5))
            .await
            .unwrap()
            .is_empty()
    );
    consumer
        .nack_delivery(&first, Duration::from_millis(200))
        .await
        .unwrap();
    assert!(
        consumer
            .fetch("worker", 1, 3, Duration::ZERO)
            .await
            .unwrap()
            .is_empty()
    );
    tokio::time::sleep(Duration::from_millis(250)).await;
    let redelivery = consumer
        .fetch("worker", 1, 3, Duration::ZERO)
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(redelivery.attempt, 2);
    assert_ne!(redelivery.delivery_id, first.delivery_id);
    assert!(consumer.ack_delivery(&first).await.is_err());
    consumer.ack_delivery(&redelivery).await.unwrap();

    publisher
        .publish("orders/created", b"123456")
        .await
        .unwrap();
    publisher.ping_roundtrip().await.unwrap();
    assert!(
        consumer
            .fetch("worker", 2, 5, Duration::ZERO)
            .await
            .unwrap()
            .is_empty()
    );
    let bounded = consumer
        .fetch("worker", 1, 6, Duration::ZERO)
        .await
        .unwrap();
    assert_eq!(bounded.len(), 1);
    assert_eq!(bounded[0].payload, b"123456");
    consumer.ack_delivery(&bounded[0]).await.unwrap();

    let publish_task = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(20)).await;
        publisher.publish("orders/created", b"late").await.unwrap();
    });
    let started = tokio::time::Instant::now();
    let arrived = consumer
        .fetch("worker", 1, 16, Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(arrived[0].payload, b"late");
    assert!(started.elapsed() < Duration::from_millis(500));
    consumer.ack_delivery(&arrived[0]).await.unwrap();
    publish_task.await.unwrap();

    assert!(
        consumer
            .fetch("worker", 8, 64, Duration::from_millis(10))
            .await
            .unwrap()
            .is_empty()
    );
    consumer.delete_consumer("worker").await.unwrap();
    assert!(
        consumer
            .fetch("worker", 1, 16, Duration::ZERO)
            .await
            .is_err()
    );

    harness.shutdown().await;
}
