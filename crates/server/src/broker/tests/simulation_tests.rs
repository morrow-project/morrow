use super::*;
use simulation::{EventTrace, Simulation};

#[derive(Debug)]
enum ClusterEvent {
    Publish {
        payload: String,
        expect_success: bool,
    },
    LoseQuorum,
    RestoreQuorum,
    ChangeLeader(u64),
    Restart,
}

#[derive(Debug)]
enum ConsumerEvent {
    AdvanceTo(u64),
    TickRedelivery,
}

#[tokio::test]
async fn virtual_time_drives_durable_redelivery_without_sleeping() {
    let scenario = Scenario::new();
    let mut subscriber = scenario.connect_durable("client1", 25).await;
    let mut publisher = scenario.connect_durable("publisher1", 25).await;
    subscriber.subscribe("orders/*", "sid1").await;
    subscriber.ping_roundtrip().await;
    publisher.publish("orders/created", b"hello").await;
    let first = subscriber.expect_msg().await;
    assert!(first.contains("/1/1 "));

    let mut simulation = Simulation::new(0xfeed, 1_000);
    simulation.schedule_at(1_024, ConsumerEvent::AdvanceTo(1_024));
    simulation.schedule_at(1_024, ConsumerEvent::TickRedelivery);
    simulation.schedule_at(1_025, ConsumerEvent::AdvanceTo(1_025));
    simulation.schedule_at(1_025, ConsumerEvent::TickRedelivery);

    while let Some(event) = simulation.step().unwrap() {
        let time_ms = simulation.clock.now_ms();
        simulation.trace.record(time_ms, format!("{event:?}"));
        match event {
            ConsumerEvent::AdvanceTo(time_ms) => scenario.clock.set_ms(time_ms),
            ConsumerEvent::TickRedelivery => scenario.tick_redelivery().await,
        }
        if time_ms == 1_024 {
            let inner = scenario.broker().inner.lock().await;
            let consumer = inner.consumers.get("durable-client1-sid1").unwrap();
            assert!(consumer.pending.is_empty());
            assert_eq!(consumer.in_flight.get(&1).unwrap().delivery_id, 1);
        }
    }

    let second = subscriber.expect_msg().await;
    assert!(second.starts_with("DELIVER orders/created sid1 _MORROW/ACK/durable-client1-sid1/1/2"));
    assert_eq!(simulation.trace.seed, 0xfeed);
}

#[tokio::test]
async fn seeded_cluster_scenario_replays_with_the_same_trace() {
    for seed in [0x5eed, 1, 2, 3, 0xdead_beef] {
        let first = run_seeded_cluster(seed).await;
        let second = run_seeded_cluster(seed).await;

        let mut replayer = first.0.replayer();
        for event in second.0.events.clone() {
            replayer
                .record(event)
                .unwrap_or_else(|error| panic!("seed {seed:#x} failed replay: {error}"));
        }
        replayer
            .finish()
            .unwrap_or_else(|error| panic!("seed {seed:#x} ended replay early: {error}"));
        assert_eq!(first.0, second.0, "seed {seed:#x} produced a new trace");
        assert_eq!(first.1, second.1, "seed {seed:#x} produced new state");
        assert_eq!(first.1, 3, "seed {seed:#x} lost a committed record");
    }
}

async fn run_seeded_cluster(seed: u64) -> (EventTrace, usize) {
    let mut scenario = Scenario::new_fake_cluster(3);
    let mut simulation = Simulation::new(seed, 1_000);
    let payload =
        |simulation: &mut Simulation<ClusterEvent>| format!("seed-{}", simulation.rng.next_u64());
    let first_payload = payload(&mut simulation);
    simulation.schedule_at(
        1_000,
        ClusterEvent::Publish {
            payload: first_payload,
            expect_success: true,
        },
    );
    simulation.schedule_at(1_001, ClusterEvent::LoseQuorum);
    let blocked_payload = payload(&mut simulation);
    simulation.schedule_at(
        1_002,
        ClusterEvent::Publish {
            payload: blocked_payload,
            expect_success: false,
        },
    );
    simulation.schedule_at(1_003, ClusterEvent::RestoreQuorum);
    simulation.schedule_at(1_004, ClusterEvent::ChangeLeader(2));
    let second_payload = payload(&mut simulation);
    simulation.schedule_at(
        1_005,
        ClusterEvent::Publish {
            payload: second_payload,
            expect_success: false,
        },
    );
    simulation.schedule_at(1_006, ClusterEvent::ChangeLeader(1));
    let recovered_payload = payload(&mut simulation);
    simulation.schedule_at(
        1_007,
        ClusterEvent::Publish {
            payload: recovered_payload,
            expect_success: true,
        },
    );
    simulation.schedule_at(1_008, ClusterEvent::Restart);
    let restarted_payload = payload(&mut simulation);
    simulation.schedule_at(
        1_009,
        ClusterEvent::Publish {
            payload: restarted_payload,
            expect_success: true,
        },
    );

    while let Some(event) = simulation.step().unwrap() {
        let time_ms = simulation.clock.now_ms();
        simulation.trace.record(time_ms, format!("{event:?}"));
        match event {
            ClusterEvent::Publish {
                payload,
                expect_success,
            } => {
                let mut publisher = scenario.connect_durable("simulation-publisher", 25).await;
                publisher
                    .publish("orders/created", payload.as_bytes())
                    .await;
                if expect_success {
                    publisher.ping_roundtrip().await;
                } else {
                    let expected_error = if scenario.fake_cluster().quorum_available() {
                        "not leader"
                    } else {
                        "quorum unavailable"
                    };
                    publisher.expect_err_contains(expected_error).await;
                }
                publisher.disconnect().await;
            }
            ClusterEvent::LoseQuorum => scenario.partition_available([1]),
            ClusterEvent::RestoreQuorum => scenario.restore_all_nodes(),
            ClusterEvent::ChangeLeader(leader) => scenario.set_leader(Some(leader)),
            ClusterEvent::Restart => scenario.restart_broker().await,
        }
    }

    let inner = scenario.broker().inner.lock().await;
    let mut offsets = inner
        .messages
        .values()
        .filter_map(|record| {
            (record.stream.as_deref() == Some("orders") && record.subject == "orders/created")
                .then_some(record.offset)
        })
        .collect::<Vec<_>>();
    offsets.sort_unstable();
    assert_eq!(offsets, vec![Some(0), Some(1), Some(2)]);
    let messages = inner.messages.len();
    (simulation.trace, messages)
}
