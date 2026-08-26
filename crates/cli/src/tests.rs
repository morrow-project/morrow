use super::*;
use std::time::Instant;

#[test]
fn parses_client_config_defaults() {
    let config = CliConfig::from_json(&serde_json::json!({})).unwrap();
    assert_eq!(config.server, DEFAULT_SERVER.parse().unwrap());
    assert_eq!(config.max_payload, DEFAULT_MAX_PAYLOAD);
    assert!(config.tls.is_none());
    assert!(config.auth.is_none());
    assert!(!config.connect.verbose);
    assert_eq!(config.connect.ack_timeout_ms, DEFAULT_ACK_TIMEOUT_MS);
    assert_eq!(config.connect.max_in_flight, DEFAULT_MAX_IN_FLIGHT);
}

#[test]
fn uses_defaults_when_implicit_config_is_missing() {
    let path =
        std::env::temp_dir().join(format!("morrow-cli-missing-config-{}", std::process::id()));
    let config = CliConfig::load(&path, true).unwrap();
    assert_eq!(config.server, DEFAULT_SERVER.parse().unwrap());
}

#[test]
fn rejects_missing_explicit_config() {
    let path = std::env::temp_dir().join(format!(
        "morrow-cli-explicit-missing-config-{}",
        std::process::id()
    ));
    let err = CliConfig::load(&path, false).unwrap_err();
    assert!(err.to_string().contains("reading"));
}

#[test]
fn rejects_invalid_server_address() {
    let err = CliConfig::from_json(&serde_json::json!({"server": "bad"})).unwrap_err();
    assert!(err.to_string().contains("server"));
}

#[test]
fn rejects_auth_without_client_id() {
    let err = CliConfig::from_json(&serde_json::json!({
        "auth": {"enabled": true, "private_key_seed_hex": "00".repeat(32)}
    }))
    .unwrap_err();
    assert!(err.to_string().contains("auth.client_id"));
}

#[test]
fn rejects_auth_without_seed() {
    let err = CliConfig::from_json(&serde_json::json!({
        "auth": {"enabled": true, "client_id": "client1"}
    }))
    .unwrap_err();
    assert!(err.to_string().contains("private_key_seed_hex"));
}

#[test]
fn rejects_malformed_seed() {
    let err = CliConfig::from_json(&serde_json::json!({
        "auth": {
            "enabled": true,
            "client_id": "client1",
            "private_key_seed_hex": "bad"
        }
    }))
    .unwrap_err();
    assert!(err.to_string().contains("private_key_seed_hex"));
}

#[test]
fn parses_ping_args() {
    let args = Args::parse(["morrow-cli", "ping"].into_iter().map(str::to_string)).unwrap();
    assert_eq!(args.config_path, PathBuf::from(DEFAULT_CONFIG_PATH));
    assert!(!args.config_path_explicit);
    assert_eq!(args.command, Command::Ping);
}

#[test]
fn parses_server_override_and_bench_pubsub_args() {
    let args = Args::parse(
        [
            "morrow-cli",
            "--server",
            "127.0.0.1:4222",
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
            "3",
            "--concurrency",
            "2",
            "--ack",
            "--json",
        ]
        .into_iter()
        .map(str::to_string),
    )
    .unwrap();
    assert_eq!(args.server, Some("127.0.0.1:4222".parse().unwrap()));
    let Command::Bench {
        mode,
        target,
        options,
    } = args.command
    else {
        panic!("expected benchmark command");
    };
    assert_eq!(mode, BenchmarkMode::PubSub);
    assert_eq!(target, "orders/bench");
    assert_eq!(options.messages, Some(20));
    assert_eq!(options.payload_size, 64);
    assert_eq!(options.publishers, 4);
    assert_eq!(options.subscribers, 3);
    assert!(options.ack);
    assert_eq!(options.publish_mode, PublishMode::Sync);
    assert_eq!(options.ack_level, Some(AckLevel::Durable));
    assert!(options.json);
}

#[test]
fn parses_bench_ack_levels() {
    let args = Args::parse(
        [
            "morrow-cli",
            "bench",
            "pubsub",
            "orders",
            "--ack-level",
            "high-durability",
        ]
        .into_iter()
        .map(str::to_string),
    )
    .unwrap();
    assert!(matches!(
        args.command,
        Command::Bench {
            options: BenchmarkOptions {
                ack: false,
                ack_level: Some(AckLevel::HighDurability),
                publish_mode: PublishMode::Sync,
                ..
            },
            ..
        }
    ));

    let legacy = Args::parse(
        ["morrow-cli", "bench", "pubsub", "orders", "--ack"]
            .into_iter()
            .map(str::to_string),
    )
    .unwrap();
    assert!(matches!(
        legacy.command,
        Command::Bench {
            options: BenchmarkOptions {
                ack: true,
                ack_level: Some(AckLevel::Durable),
                publish_mode: PublishMode::Sync,
                ..
            },
            ..
        }
    ));
}

#[test]
fn parses_bench_duration() {
    let args = Args::parse(
        [
            "morrow-cli",
            "bench",
            "pubsub",
            "orders",
            "--duration",
            "2s",
        ]
        .into_iter()
        .map(str::to_string),
    )
    .unwrap();
    assert!(matches!(
        args.command,
        Command::Bench {
            options: BenchmarkOptions {
                duration_ms: Some(2_000),
                messages: None,
                ..
            },
            ..
        }
    ));
}

#[test]
fn parses_all_benchmark_modes_and_common_controls() {
    for mode in [
        "pub", "sub", "pubsub", "request", "serve", "consume", "fetch",
    ] {
        let args = Args::parse(
            [
                "morrow-cli",
                "bench",
                mode,
                "target",
                "--clients",
                "2",
                "--messages",
                "9",
                "--seed",
                "42",
                "--json",
            ]
            .into_iter()
            .map(str::to_string),
        )
        .unwrap();
        let Command::Bench { options, .. } = args.command else {
            panic!("expected benchmark command");
        };
        assert_eq!(options.clients, 2);
        assert_eq!(options.messages, Some(9));
        assert_eq!(options.payload_size, 128);
        assert_eq!(options.seed, 42);
    }
}

#[test]
fn parses_advanced_publish_controls() {
    let args = Args::parse(
        [
            "morrow-cli",
            "bench",
            "pub",
            "orders",
            "--clients",
            "3",
            "--duration",
            "2s",
            "--throughput",
            "5000",
            "--header",
            "Trace-Id:abc",
            "--mode",
            "async",
            "--ack-level",
            "cluster-durable",
            "--max-in-flight",
            "64",
            "--subjects",
            "8",
            "--subject-order",
            "random",
            "--key-cardinality",
            "4",
            "--sleep",
            "1ms",
            "--warmup",
            "3s",
            "--csv",
            "result.csv",
        ]
        .into_iter()
        .map(str::to_string),
    )
    .unwrap();
    let Command::Bench { options, .. } = args.command else {
        panic!("expected benchmark command");
    };
    assert_eq!(options.publish_mode, PublishMode::Async);
    assert_eq!(options.ack_level, Some(AckLevel::ClusterDurable));
    assert_eq!(options.headers, vec![("Trace-Id".into(), "abc".into())]);
    assert_eq!(options.max_in_flight, 64);
    assert_eq!(options.subjects, 8);
    assert_eq!(options.key_cardinality, 4);
    assert_eq!(options.csv, Some(PathBuf::from("result.csv")));
}

#[test]
fn rejects_invalid_benchmark_combinations() {
    for arguments in [
        vec![
            "bench",
            "pub",
            "orders",
            "--messages",
            "1",
            "--duration",
            "1s",
        ],
        vec![
            "bench",
            "pub",
            "orders",
            "--payload",
            "body.bin",
            "--payload-size",
            "10",
        ],
        vec!["bench", "pub", "orders", "--ack-level", "durable"],
        vec!["bench", "request", "service", "--mode", "sync"],
        vec!["bench", "pub", "orders", "--header", "Morrow-QoS:1"],
        vec!["bench", "pub", "orders", "--messages", "0"],
    ] {
        let result = Args::parse(
            std::iter::once("morrow-cli")
                .chain(arguments)
                .map(str::to_string),
        );
        assert!(result.is_err(), "arguments should fail validation");
    }
}

#[test]
fn benchmark_work_division_is_complete_and_deterministic() {
    let shares = (0..3)
        .map(|index| bench::stats::work_share(10, 3, index))
        .collect::<Vec<_>>();
    assert_eq!(shares, vec![(0, 4), (4, 3), (7, 3)]);
    assert_eq!(shares.iter().map(|(_, count)| count).sum::<usize>(), 10);
}

#[test]
fn benchmark_latency_distribution_includes_tail_percentiles() {
    let mut values = vec![1, 2, 3, 4, 100];
    let result = bench::stats::distribution(&mut values);
    assert_eq!(result.samples, 5);
    assert_eq!(result.min, 1.0);
    assert_eq!(result.p50, 3.0);
    assert_eq!(result.p90, 100.0);
    assert_eq!(result.max, 100.0);
    assert!((result.mean - 22.0).abs() < f64::EPSILON);
}

#[test]
fn benchmark_rate_limiting_uses_the_slower_constraint() {
    assert_eq!(
        bench::stats::pacing_delay(10, 2, 100, 5),
        Duration::from_millis(200)
    );
    assert_eq!(
        bench::stats::pacing_delay(10, 1, 1_000, 5),
        Duration::from_millis(50)
    );
}

#[test]
fn duration_results_exclude_setup_and_drain_time() {
    let args = Args::parse(
        ["morrow-cli", "bench", "pub", "orders", "--duration", "50ms"]
            .into_iter()
            .map(str::to_string),
    )
    .unwrap();
    let Command::Bench { options, .. } = args.command else {
        panic!("expected benchmark command");
    };
    assert_eq!(
        bench::stats::measurement_elapsed(&options, Instant::now() - Duration::from_secs(1)),
        Duration::from_millis(50)
    );
}

#[test]
fn benchmark_random_selection_is_seeded_and_bounded() {
    let first = (0..100)
        .map(|sequence| bench::stats::deterministic_index(sequence, 7, 42))
        .collect::<Vec<_>>();
    let again = (0..100)
        .map(|sequence| bench::stats::deterministic_index(sequence, 7, 42))
        .collect::<Vec<_>>();
    assert_eq!(first, again);
    assert!(first.iter().all(|index| *index < 7));
}

#[test]
fn parses_version_args_without_a_config_path() {
    let args = Args::parse(["morrow-cli", "--version"].into_iter().map(str::to_string)).unwrap();
    assert_eq!(args.config_path, PathBuf::from(DEFAULT_CONFIG_PATH));
    assert!(!args.config_path_explicit);
    assert_eq!(args.command, Command::Version);
}

#[test]
fn parses_pub_args() {
    let args = Args::parse(
        [
            "morrow-cli",
            "--config",
            "custom.json",
            "pub",
            "orders/created",
            "hello",
        ]
        .into_iter()
        .map(str::to_string),
    )
    .unwrap();
    assert_eq!(args.config_path, PathBuf::from("custom.json"));
    assert!(args.config_path_explicit);
    assert_eq!(
        args.command,
        Command::Pub {
            subject: "orders/created".into(),
            payload: b"hello".to_vec(),
            qos: None,
            msg_id: None,
        }
    );
}

#[test]
fn parses_qos_pub_args() {
    let args = Args::parse(
        [
            "morrow-cli",
            "pub",
            "orders/created",
            "hello",
            "--qos",
            "2",
            "--msg-id",
            "msg-1",
        ]
        .into_iter()
        .map(str::to_string),
    )
    .unwrap();
    assert_eq!(
        args.command,
        Command::Pub {
            subject: "orders/created".into(),
            payload: b"hello".to_vec(),
            qos: Some(AckLevel::HighDurability),
            msg_id: Some("msg-1".into()),
        }
    );
}

#[test]
fn parses_sub_args() {
    let args = Args::parse(
        [
            "morrow-cli",
            "sub",
            "orders/*",
            "--sid",
            "sid2",
            "--queue",
            "workers",
            "--ack",
            "--max-messages",
            "2",
        ]
        .into_iter()
        .map(str::to_string),
    )
    .unwrap();
    assert_eq!(
        args.command,
        Command::Sub {
            subject: "orders/*".into(),
            sid: "sid2".into(),
            queue: Some("workers".into()),
            ack: true,
            max_messages: Some(2),
        }
    );
}

#[test]
fn parses_request_args() {
    let args = Args::parse(
        [
            "morrow-cli",
            "request",
            "orders/lookup",
            "hello",
            "--timeout-ms",
            "500",
        ]
        .into_iter()
        .map(str::to_string),
    )
    .unwrap();
    assert_eq!(
        args.command,
        Command::Request {
            subject: "orders/lookup".into(),
            payload: b"hello".to_vec(),
            timeout_ms: 500,
        }
    );
}

#[test]
fn parses_reply_args() {
    let args = Args::parse(
        ["morrow-cli", "reply", "orders/lookup", "--queue", "workers"]
            .into_iter()
            .map(str::to_string),
    )
    .unwrap();
    assert_eq!(
        args.command,
        Command::Reply {
            subject: "orders/lookup".into(),
            queue: Some("workers".into()),
        }
    );
}
