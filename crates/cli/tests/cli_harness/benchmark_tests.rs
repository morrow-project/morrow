use super::*;

#[tokio::test]
async fn cli_bench_pubsub_reports_json_results() {
    let _guard = CLI_TEST_LOCK.lock().await;
    let harness = Harness::start().await;
    let output = run_cli([
        "--server",
        &harness.addr.to_string(),
        "bench",
        "pubsub",
        "orders/bench",
        "--messages",
        "20",
        "--payload-size",
        "64",
        "--publishers",
        "2",
        "--subscribers",
        "2",
        "--ack-level",
        "durable",
        "--json",
    ])
    .await;
    assert!(output.status.success(), "{}", stderr(&output));
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["valid"], true);
    assert_eq!(result["aggregate"]["operations"], 20);
    assert_eq!(result["roles"][0]["operations"], 20);
    assert_eq!(result["roles"][1]["operations"], 40);
    assert_eq!(result["aggregate"]["duplicates"], 0);
    assert_eq!(result["clients"].as_array().unwrap().len(), 4);
    assert_eq!(result["network_mode"], "local");
    assert_eq!(result["acknowledgement"]["requested_level"], "durable");
    assert_eq!(result["acknowledgement"]["observed_level"], "durable");
    assert_eq!(result["acknowledgement"]["observed_contract_version"], 1);
    assert_eq!(result["phases"]["warmup_operations"], 0);
    assert_eq!(result["phases"]["measured_operations"], 20);
    assert_eq!(result["phases"]["total_operations"], 20);
    assert_eq!(result["phases"]["steady_state"], false);
    harness.shutdown().await;
}

#[tokio::test]
async fn cli_bench_publish_modes_support_parallel_clients() {
    let _guard = CLI_TEST_LOCK.lock().await;
    let harness = Harness::start().await;
    let output_dir = TestDir::new("morrow-cli-benchmark-output");
    for (mode, extra) in [
        ("fire-and-forget", None),
        ("sync", None),
        ("async", Some("8")),
        ("batch", Some("5")),
    ] {
        let csv_path = output_dir.path().join(format!("{mode}.csv"));
        let mut arguments = vec![
            "--server".to_string(),
            harness.addr.to_string(),
            "bench".into(),
            "pub".into(),
            "orders/bench".into(),
            "--messages".into(),
            "20".into(),
            "--clients".into(),
            "2".into(),
            "--payload-size".into(),
            "64".into(),
            "--mode".into(),
            mode.into(),
            "--json".into(),
            "--csv".into(),
            csv_path.display().to_string(),
        ];
        if mode != "fire-and-forget" {
            arguments.extend(["--ack-level".into(), "durable".into()]);
        }
        if let Some(value) = extra {
            arguments.extend([
                if mode == "batch" {
                    "--batch-size"
                } else {
                    "--max-in-flight"
                }
                .into(),
                value.into(),
            ]);
        }
        let output = run_cli_args(arguments).await;
        assert!(output.status.success(), "{mode}: {}", stderr(&output));
        let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(result["aggregate"]["operations"], 20, "{mode}");
        assert_eq!(result["valid"], true, "{mode}");
        assert_eq!(result["phases"]["measured_operations"], 20, "{mode}");
        let csv = fs::read_to_string(csv_path).unwrap();
        assert!(csv.starts_with("mode,target,endpoint,network"));
        assert!(csv.lines().count() >= 4);
    }
    harness.shutdown().await;
}

#[tokio::test]
async fn cli_bench_queue_group_delivers_each_message_once() {
    let _guard = CLI_TEST_LOCK.lock().await;
    let harness = Harness::start().await;
    let output = run_cli([
        "--server",
        &harness.addr.to_string(),
        "bench",
        "pubsub",
        "orders/queue",
        "--clients",
        "2",
        "--messages",
        "20",
        "--payload-size",
        "64",
        "--queue",
        "workers",
        "--json",
    ])
    .await;
    assert!(output.status.success(), "{}", stderr(&output));
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["roles"][1]["operations"], 20);
    assert_eq!(result["roles"][1]["duplicates"], 0);
    harness.shutdown().await;
}

#[tokio::test]
async fn cli_bench_independent_publish_and_fanout_subscribe_modes_interoperate() {
    let _guard = CLI_TEST_LOCK.lock().await;
    let harness = Harness::start().await;
    let subscriber = Command::new(cli_bin())
        .args([
            "--server",
            &harness.addr.to_string(),
            "bench",
            "sub",
            "orders/fanout",
            "--clients",
            "2",
            "--messages",
            "20",
            "--json",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    wait_for_subscription(harness.admin_addr, "orders/fanout", "bench-sub-0").await;
    wait_for_subscription(harness.admin_addr, "orders/fanout", "bench-sub-1").await;
    let publisher = run_cli([
        "--server",
        &harness.addr.to_string(),
        "bench",
        "pub",
        "orders/fanout",
        "--clients",
        "2",
        "--messages",
        "20",
        "--payload-size",
        "64",
        "--json",
    ])
    .await;
    assert!(publisher.status.success(), "{}", stderr(&publisher));
    let subscriber = wait_output(subscriber, CLI_COMMAND_TIMEOUT).await;
    assert!(subscriber.status.success(), "{}", stderr(&subscriber));
    let result: serde_json::Value = serde_json::from_slice(&subscriber.stdout).unwrap();
    assert_eq!(result["aggregate"]["operations"], 40);
    harness.shutdown().await;
}

#[tokio::test]
async fn cli_bench_request_and_service_modes_complete_parallel_work() {
    let _guard = CLI_TEST_LOCK.lock().await;
    let harness = Harness::start().await;
    let server = Command::new(cli_bin())
        .args([
            "--server",
            &harness.addr.to_string(),
            "bench",
            "serve",
            "service/echo",
            "--clients",
            "2",
            "--messages",
            "20",
            "--payload-size",
            "64",
            "--queue",
            "responders",
            "--json",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    wait_for_subscription(harness.admin_addr, "service/echo", "bench-serve-0").await;
    wait_for_subscription(harness.admin_addr, "service/echo", "bench-serve-1").await;

    let request = run_cli([
        "--server",
        &harness.addr.to_string(),
        "bench",
        "request",
        "service/echo",
        "--clients",
        "2",
        "--messages",
        "20",
        "--payload-size",
        "64",
        "--json",
    ])
    .await;
    let response = wait_output(server, CLI_COMMAND_TIMEOUT).await;
    assert!(
        request.status.success(),
        "request: {}; responder: {}",
        stderr(&request),
        stderr(&response)
    );
    let request_result: serde_json::Value = serde_json::from_slice(&request.stdout).unwrap();
    assert_eq!(request_result["aggregate"]["operations"], 20);

    assert!(response.status.success(), "{}", stderr(&response));
    let response_result: serde_json::Value = serde_json::from_slice(&response.stdout).unwrap();
    assert_eq!(response_result["aggregate"]["operations"], 20);
    assert_eq!(response_result["aggregate"]["acknowledgements"], 20);
    harness.shutdown().await;
}

#[tokio::test]
async fn cli_bench_consume_and_fetch_use_existing_durable_consumers() {
    let _guard = CLI_TEST_LOCK.lock().await;
    let harness = Harness::start().await;
    let mut client = Client::connect(harness.addr, harness.max_payload)
        .await
        .unwrap();
    client.read_info().await.unwrap();
    client
        .connect_durable("benchmark-pull-0", false, 5_000, 32)
        .await
        .unwrap();
    for consumer in ["bench-consume", "bench-fetch"] {
        client
            .create_consumer(
                consumer,
                "orders/bench",
                client::protocol::StartPosition::Earliest,
            )
            .await
            .unwrap();
    }
    for index in 0..20 {
        client
            .publish_with_qos(
                "orders/bench",
                None,
                b"payload",
                client::protocol::AckLevel::Durable,
                &format!("benchmark-pull-{index}"),
            )
            .await
            .unwrap();
    }
    for (mode, consumer) in [("consume", "bench-consume"), ("fetch", "bench-fetch")] {
        let output = run_cli([
            "--server",
            &harness.addr.to_string(),
            "bench",
            mode,
            consumer,
            "--messages",
            "20",
            "--batch-size",
            "4",
            "--durable-id",
            "benchmark-pull",
            "--ack",
            "--json",
        ])
        .await;
        assert!(output.status.success(), "{mode}: {}", stderr(&output));
        let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(result["aggregate"]["operations"], 20, "{mode}");
        assert_eq!(result["aggregate"]["acknowledgements"], 20, "{mode}");
    }
    harness.shutdown().await;
}
