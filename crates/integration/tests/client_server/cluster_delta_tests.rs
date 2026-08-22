use super::*;

#[tokio::test]
async fn follower_applies_committed_consumer_and_partition_deltas() {
    let harness = ClusterHarness::start_three_routed().await;
    let leader = harness.wait_for_leader().await;
    let leader_node = harness
        .nodes
        .iter()
        .find(|node| node.node_id == leader)
        .unwrap();
    let follower = harness
        .nodes
        .iter()
        .find(|node| node.node_id != leader)
        .unwrap();

    let mut subscriber = Client::connect(leader_node.client_addr, harness.max_payload)
        .await
        .unwrap();
    subscriber.read_info().await.unwrap();
    subscriber
        .connect_durable("delta-subscriber", false, 5_000, 16)
        .await
        .unwrap();
    subscriber.subscribe("orders.*", "sid-delta").await.unwrap();
    subscriber.ping_roundtrip().await.unwrap();

    wait_for_admin(follower.http_addr, "/subscriptions", |value| {
        value["durable_consumers"]
            .as_array()
            .is_some_and(|consumers| {
                consumers
                    .iter()
                    .any(|consumer| consumer["filter_subject"] == "orders.*")
            })
    })
    .await;

    let mut publisher = Client::connect(leader_node.client_addr, harness.max_payload)
        .await
        .unwrap();
    publisher.read_info().await.unwrap();
    publisher
        .connect_durable("delta-publisher", false, 5_000, 16)
        .await
        .unwrap();
    for message in 0..3 {
        publisher
            .publish_with_qos(
                "orders.created",
                None,
                b"delta",
                client::protocol::AckLevel::Durable,
                &format!("delta-message-{message}"),
            )
            .await
            .unwrap();
    }

    wait_for_admin(follower.http_addr, "/wal", |value| {
        value["retained_message_count"]
            .as_u64()
            .is_some_and(|count| count > 0)
    })
    .await;
    let cluster = wait_for_admin(follower.http_addr, "/cluster", |value| {
        value["state_application"]["delta_applications"]
            .as_u64()
            .is_some_and(|count| count > 0)
    })
    .await;
    assert_eq!(
        cluster["state_application"]["full_reconciliations"].as_u64(),
        Some(1)
    );
    harness.shutdown().await;
}

async fn wait_for_admin(
    addr: SocketAddr,
    path: &str,
    ready: impl Fn(&serde_json::Value) -> bool,
) -> serde_json::Value {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(value) = admin_json(addr, path).await
            && ready(&value)
        {
            return value;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "admin state did not converge for {path}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
