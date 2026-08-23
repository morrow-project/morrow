use super::*;

#[tokio::test]
async fn idle_delivery_tick_does_not_read_retained_history() {
    let scenario = Scenario::new();
    let mut publisher = scenario.connect_durable("publisher", 1_000).await;
    for _ in 0..16 {
        publisher.publish("orders/created", b"history").await;
    }
    publisher.ping_roundtrip().await;
    scenario.broker().inner.lock().await.ready_consumers.clear();

    let logs = scenario.broker().partition_logs.clone();
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let holder = std::thread::spawn(move || {
        logs.with_partition_lock_for_test("orders", crate::stream::PartitionId(0), || {
            ready_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        });
    });
    ready_rx.recv().unwrap();

    let idle_tick = tokio::time::timeout(
        Duration::from_millis(100),
        scenario.broker().deliver_pending(),
    )
    .await;

    release_tx.send(()).unwrap();
    holder.join().unwrap();
    idle_tick
        .expect("idle delivery tick read partition history")
        .unwrap();
}

#[tokio::test]
async fn one_redelivery_tick_expires_at_most_the_work_limit() {
    let scenario = Scenario::new();
    let mut subscriber = scenario.connect_durable("client1", 25).await;
    subscriber.subscribe("orders/*", "sid1").await;
    subscriber.ping_roundtrip().await;

    let mut inner = scenario.broker().inner.lock().await;
    let consumer_id = "durable-client1-sid1";
    inner.lease_deadlines.clear();
    inner
        .consumers
        .get_mut(consumer_id)
        .unwrap()
        .in_flight
        .clear();
    for seq in 1..=MAX_EXPIRED_LEASES_PER_TICK as u64 + 500 {
        let lease = DeliveryAttemptRecord {
            seq,
            consumer_id: consumer_id.to_string(),
            delivery_id: seq,
            deadline_ms: 1,
            attempt: 1,
        };
        inner
            .consumers
            .get_mut(consumer_id)
            .unwrap()
            .in_flight
            .insert(
                seq,
                InFlight {
                    delivery_id: seq,
                    deadline_ms: 1,
                    attempt: 1,
                },
            );
        inner.schedule_lease(consumer_id, seq, &lease);
    }

    assert_eq!(
        inner
            .expire_due_leases(1, MAX_EXPIRED_LEASES_PER_TICK)
            .unwrap(),
        MAX_EXPIRED_LEASES_PER_TICK
    );
    assert_eq!(inner.consumers[consumer_id].in_flight.len(), 500);
    assert_eq!(inner.next_lease_deadline(), Some(1));
}

#[tokio::test]
async fn rescheduled_lease_ignores_its_stale_deadline() {
    let scenario = Scenario::new();
    let mut subscriber = scenario.connect_durable("client1", 25).await;
    let mut publisher = scenario.connect_durable("publisher", 25).await;
    subscriber.subscribe("orders/*", "sid1").await;
    subscriber.ping_roundtrip().await;
    publisher.publish("orders/created", b"hello").await;
    subscriber.expect_msg().await;

    let mut inner = scenario.broker().inner.lock().await;
    let consumer_id = "durable-client1-sid1";
    let lease = inner.consumers[consumer_id].in_flight[&1].clone();
    let extended = DeliveryAttemptRecord {
        seq: 1,
        consumer_id: consumer_id.to_string(),
        delivery_id: lease.delivery_id,
        deadline_ms: lease.deadline_ms + 100,
        attempt: lease.attempt,
    };
    inner
        .consumers
        .get_mut(consumer_id)
        .unwrap()
        .in_flight
        .get_mut(&1)
        .unwrap()
        .deadline_ms = extended.deadline_ms;
    inner.schedule_lease(consumer_id, 1, &extended);

    assert_eq!(inner.expire_due_leases(lease.deadline_ms, 10).unwrap(), 0);
    assert_eq!(inner.next_lease_deadline(), Some(extended.deadline_ms));
}

#[tokio::test]
#[ignore = "manual high-cardinality delivery-index benchmark"]
async fn benchmark_high_history_and_consumer_cardinality() {
    let scenario = Scenario::new();
    let mut publisher = scenario.connect_durable("publisher", 1_000).await;
    publisher.publish("orders/created", b"history").await;
    publisher.ping_roundtrip().await;

    let mut inner = scenario.broker().inner.lock().await;
    let template = inner.messages[&1].clone();
    for seq in 2..=10_000_u64 {
        let mut record = template.clone();
        record.seq = seq;
        record.offset = Some(seq - 1);
        inner
            .partition_sequences
            .insert(("orders".to_string(), 0, seq - 1), seq);
        inner.messages.insert(seq, record);
    }
    for id in 0..1_000 {
        inner
            .consumer_interest_index
            .insert("orders/*", format!("benchmark-{id}"));
    }
    inner.ready_consumers.clear();
    let started = std::time::Instant::now();
    inner.mark_subject_ready("orders/created");
    assert_eq!(inner.ready_consumers.len(), 1_000);
    eprintln!(
        "history=10000 consumers=1000 elapsed={:?}",
        started.elapsed()
    );
}
