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
    subscriber.subscribe("orders/*", "sid-delta").await.unwrap();
    subscriber.ping_roundtrip().await.unwrap();

    wait_for_admin(follower.http_addr, "/subscriptions", |value| {
        value["durable_consumers"]
            .as_array()
            .is_some_and(|consumers| {
                consumers
                    .iter()
                    .any(|consumer| consumer["filter_subject"] == "orders/*")
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
                "orders/created",
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

#[tokio::test]
async fn follower_applies_consumer_group_generation_and_assignment() {
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
    let mut member = Client::connect(leader_node.client_addr, harness.max_payload)
        .await
        .unwrap();
    member.read_info().await.unwrap();
    member
        .connect_durable("group-cluster-member", false, 5_000, 16)
        .await
        .unwrap();
    let assignment = member
        .group_join(
            "cluster-worker",
            "member-a",
            3,
            client::protocol::GroupAssignmentStrategy::RoundRobin,
            None,
        )
        .await
        .unwrap();
    assert_eq!(assignment.partitions, vec![0, 1, 2]);

    let state = wait_for_admin(follower.http_addr, "/groups", |value| {
        value.as_array().is_some_and(|groups| {
            groups.iter().any(|entry| {
                entry[0] == "cluster-worker"
                    && entry[1]["generation"] == assignment.generation
                    && entry[1]["assignments"][0]["partitions"] == serde_json::json!([0, 1, 2])
            })
        })
    })
    .await;
    assert_eq!(state.as_array().unwrap().len(), 1);
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
