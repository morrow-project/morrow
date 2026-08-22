use super::*;

#[tokio::test]
async fn durable_state_lock_does_not_stall_transient_routing() {
    let scenario = Scenario::new();
    let mut subscriber = scenario.connect().await;
    let mut publisher = scenario.connect().await;
    subscriber.write_line("CONNECT {}").await;
    publisher.write_line("CONNECT {}").await;
    subscriber.subscribe("events.*", "sid-1").await;
    subscriber.ping_roundtrip().await;

    let durable_state = scenario.broker().inner.lock().await;
    publisher.publish("events.created", b"hello").await;
    let frame = tokio::time::timeout(Duration::from_millis(100), subscriber.expect_msg())
        .await
        .expect("transient routing waited for durable state");
    assert_eq!(frame, "MSG events.created sid-1 5\r\nhello\r\n");
    drop(durable_state);
}

#[tokio::test]
async fn blocked_partition_does_not_stall_connections_or_transient_routing() {
    let scenario = Scenario::new();
    let mut subscriber = scenario.connect().await;
    let mut publisher = scenario.connect().await;
    subscriber.write_line("CONNECT {}").await;
    publisher.write_line("CONNECT {}").await;
    subscriber.subscribe("events.*", "sid-1").await;
    subscriber.ping_roundtrip().await;

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

    let connections = tokio::time::timeout(
        Duration::from_millis(100),
        scenario.broker().connections_response(),
    )
    .await;
    publisher.publish("events.created", b"hello").await;
    let delivery = tokio::time::timeout(Duration::from_millis(100), subscriber.expect_msg()).await;

    release_tx.send(()).unwrap();
    holder.join().unwrap();
    assert_eq!(
        connections.expect("admin read waited for partition").count,
        2
    );
    assert_eq!(
        delivery.expect("transient routing waited for partition"),
        "MSG events.created sid-1 5\r\nhello\r\n"
    );
}
