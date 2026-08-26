use super::*;

const SAMPLES: usize = 250;
const HISTORY_LEVELS: &[usize] = &[0, 1_000, 10_000];

#[tokio::test]
#[ignore = "release-mode standalone durability baseline"]
async fn benchmark_standalone_durable_publish_latency() {
    let harness = Harness::start().await;
    let mut publisher = Client::connect(harness.addr, harness.max_payload)
        .await
        .unwrap();
    publisher.read_info().await.unwrap();
    publisher
        .connect_durable("standalone-benchmark-publisher", false, 5_000, 16)
        .await
        .unwrap();

    let started = std::time::Instant::now();
    let mut latencies = Vec::with_capacity(SAMPLES);
    for sample in 0..SAMPLES {
        let before = std::time::Instant::now();
        let ack = publisher
            .publish_with_qos(
                "orders/created",
                None,
                b"benchmark-payload",
                client::protocol::AckLevel::Durable,
                &format!("standalone-benchmark-{sample}"),
            )
            .await
            .unwrap();
        assert_eq!(ack.level, client::protocol::AckLevel::Durable);
        latencies.push(before.elapsed());
    }
    let elapsed = started.elapsed();
    latencies.sort_unstable();
    let percentile = |percent: usize| latencies[(SAMPLES * percent / 100).min(SAMPLES - 1)];
    eprintln!(
        "standalone_durable_baseline samples={SAMPLES} history=0 topology=standalone ack_level=durable throughput={:.1}/s p50_us={} p95_us={} p99_us={}",
        SAMPLES as f64 / elapsed.as_secs_f64(),
        percentile(50).as_micros(),
        percentile(95).as_micros(),
        percentile(99).as_micros(),
    );
    harness.shutdown().await;
}

#[tokio::test]
#[ignore = "release-mode history scaling benchmark"]
async fn benchmark_standalone_durable_publish_latency_with_history() {
    for &history in HISTORY_LEVELS {
        let harness = Harness::start().await;
        let mut publisher = Client::connect(harness.addr, harness.max_payload)
            .await
            .unwrap();
        publisher.read_info().await.unwrap();
        let durable_id = format!("history-{history}-publisher");
        publisher
            .connect_durable(&durable_id, false, 5_000, 16)
            .await
            .unwrap();
        for sample in 0..history {
            publisher
                .publish_with_qos(
                    "orders/created",
                    None,
                    b"history-payload",
                    client::protocol::AckLevel::Durable,
                    &format!("history-{history}-{sample}"),
                )
                .await
                .unwrap();
        }
        let started = std::time::Instant::now();
        let mut latencies = Vec::with_capacity(SAMPLES);
        for sample in 0..SAMPLES {
            let before = std::time::Instant::now();
            publisher
                .publish_with_qos(
                    "orders/created",
                    None,
                    b"benchmark-payload",
                    client::protocol::AckLevel::Durable,
                    &format!("measure-{history}-{sample}"),
                )
                .await
                .unwrap();
            latencies.push(before.elapsed());
        }
        let elapsed = started.elapsed();
        latencies.sort_unstable();
        let percentile = |percent: usize| latencies[(SAMPLES * percent / 100).min(SAMPLES - 1)];
        eprintln!(
            "standalone_durable_history history={history} samples={SAMPLES} throughput={:.1}/s p50_us={} p95_us={} p99_us={}",
            SAMPLES as f64 / elapsed.as_secs_f64(),
            percentile(50).as_micros(),
            percentile(95).as_micros(),
            percentile(99).as_micros(),
        );
        harness.shutdown().await;
    }
}

#[tokio::test]
#[ignore = "release-mode three-node persistence benchmark"]
async fn benchmark_cluster_durable_publish_latency() {
    let harness = ClusterHarness::start_three().await;
    let leader = harness.wait_for_leader().await;
    let node = harness
        .nodes
        .iter()
        .find(|node| node.node_id == leader)
        .unwrap();
    let mut publisher = Client::connect(node.client_addr, harness.max_payload)
        .await
        .unwrap();
    publisher.read_info().await.unwrap();
    publisher
        .connect_durable("benchmark-publisher", false, 5_000, 16)
        .await
        .unwrap();

    let started = std::time::Instant::now();
    let mut latencies = Vec::with_capacity(SAMPLES);
    for sample in 0..SAMPLES {
        let before = std::time::Instant::now();
        publisher
            .publish_with_qos(
                "orders/created",
                None,
                b"benchmark-payload",
                client::protocol::AckLevel::Durable,
                &format!("benchmark-{sample}"),
            )
            .await
            .unwrap();
        latencies.push(before.elapsed());
    }
    let elapsed = started.elapsed();
    latencies.sort_unstable();
    let percentile = |percent: usize| latencies[(SAMPLES * percent / 100).min(SAMPLES - 1)];
    eprintln!(
        "cluster_durable_baseline samples={SAMPLES} history=0 topology=three-node ack_level=durable throughput={:.1}/s p50_us={} p95_us={} p99_us={}",
        SAMPLES as f64 / elapsed.as_secs_f64(),
        percentile(50).as_micros(),
        percentile(95).as_micros(),
        percentile(99).as_micros(),
    );
    harness.shutdown().await;
}

#[tokio::test]
#[ignore = "release-mode slow-follower and quorum benchmark"]
async fn benchmark_cluster_durable_publish_with_slow_follower() {
    let harness = ClusterHarness::start_three().await;
    let leader = harness.wait_for_leader().await;
    let leader_node = harness
        .nodes
        .iter()
        .find(|node| node.node_id == leader)
        .unwrap();
    let mut publisher = Client::connect(leader_node.client_addr, harness.max_payload)
        .await
        .unwrap();
    publisher.read_info().await.unwrap();
    publisher
        .connect_durable("slow-follower-benchmark", false, 5_000, 16)
        .await
        .unwrap();

    let mut healthy = Vec::with_capacity(SAMPLES);
    for sample in 0..SAMPLES {
        let before = std::time::Instant::now();
        publisher
            .publish_with_qos(
                "orders/created",
                None,
                b"healthy",
                client::protocol::AckLevel::Durable,
                &format!("healthy-{sample}"),
            )
            .await
            .unwrap();
        healthy.push(before.elapsed());
    }

    let follower_index = harness
        .nodes
        .iter()
        .position(|node| node.node_id != leader)
        .unwrap();
    harness.brokers[follower_index].shutdown().await.unwrap();
    harness.server_tasks[follower_index].abort();

    let started = std::time::Instant::now();
    let mut degraded = Vec::with_capacity(SAMPLES);
    for sample in 0..SAMPLES {
        let before = std::time::Instant::now();
        publisher
            .publish_with_qos(
                "orders/created",
                None,
                b"degraded",
                client::protocol::AckLevel::Durable,
                &format!("degraded-{sample}"),
            )
            .await
            .unwrap();
        degraded.push(before.elapsed());
    }
    let elapsed = started.elapsed();
    healthy.sort_unstable();
    degraded.sort_unstable();
    let percentile = |samples: &[std::time::Duration], percent: usize| {
        samples[(samples.len() * percent / 100).min(samples.len() - 1)]
    };
    eprintln!(
        "cluster_slow_follower samples={SAMPLES} healthy_p95_us={} degraded_p95_us={} degraded_p99_us={} degraded_throughput={:.1}/s",
        percentile(&healthy, 95).as_micros(),
        percentile(&degraded, 95).as_micros(),
        percentile(&degraded, 99).as_micros(),
        SAMPLES as f64 / elapsed.as_secs_f64(),
    );
    harness.shutdown().await;
}
