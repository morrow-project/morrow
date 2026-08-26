use super::*;

#[tokio::test]
async fn concurrent_cluster_publishers_commit_unique_contiguous_offsets() {
    const PUBLISHERS: usize = 5;
    const MESSAGES_PER_PUBLISHER: usize = 20;

    let harness = ClusterHarness::start_three().await;
    let leader = harness.wait_for_leader().await;
    let leader_addr = harness
        .nodes
        .iter()
        .find(|node| node.node_id == leader)
        .expect("elected leader is in the harness")
        .client_addr;
    let start = Arc::new(tokio::sync::Barrier::new(PUBLISHERS));
    let mut publishers = tokio::task::JoinSet::new();

    for publisher_id in 0..PUBLISHERS {
        let start = start.clone();
        publishers.spawn(async move {
            let mut client = Client::connect(leader_addr, 1024).await?;
            client.read_info().await?;
            client
                .connect_durable(
                    &format!("concurrent-publisher-{publisher_id}"),
                    false,
                    5_000,
                    16,
                )
                .await?;
            start.wait().await;

            let mut offsets = Vec::with_capacity(MESSAGES_PER_PUBLISHER);
            for message_id in 0..MESSAGES_PER_PUBLISHER {
                let ack = client
                    .publish_with_qos(
                        "orders/created",
                        None,
                        b"concurrent",
                        client::protocol::AckLevel::Durable,
                        &format!("concurrent-{publisher_id}-{message_id}"),
                    )
                    .await?;
                offsets.push(ack.offset.expect("durable cluster ACK has an offset"));
            }
            Ok::<_, client::ClientError>(offsets)
        });
    }

    let offsets = tokio::time::timeout(Duration::from_secs(30), async {
        let mut offsets = Vec::with_capacity(PUBLISHERS * MESSAGES_PER_PUBLISHER);
        while let Some(result) = publishers.join_next().await {
            offsets.extend(result.expect("publisher task completes")?);
        }
        Ok::<_, client::ClientError>(offsets)
    })
    .await
    .expect("concurrent publishers complete before the timeout")
    .expect("all concurrent publishes receive durable ACKs");

    let mut sorted = offsets;
    sorted.sort_unstable();
    assert_eq!(
        sorted,
        (0..(PUBLISHERS * MESSAGES_PER_PUBLISHER) as u64).collect::<Vec<_>>()
    );

    harness.shutdown().await;
}
