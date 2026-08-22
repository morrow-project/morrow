use super::*;

fn quotas() -> crate::config::ResourceQuotaConfig {
    crate::config::ResourceQuotaConfig {
        max_connections: 2,
        max_connections_per_identity: 1,
        max_transient_subscriptions: 2,
        max_transient_subscriptions_per_identity: 1,
        max_durable_consumers: 2,
        max_durable_consumers_per_identity: 1,
        max_outbound_bytes_per_connection: 1_024,
        max_http_connections: 1,
        max_raft_connections: 1,
        max_route_connections: 1,
        client_idle_timeout_ms: 5_000,
        http_header_timeout_ms: 5_000,
    }
}

#[tokio::test]
async fn global_connection_quota_releases_after_disconnect() {
    let mut limits = quotas();
    limits.max_connections = 1;
    let scenario = Scenario::new_with_quotas(limits);
    let (sender, _receiver) = test_outbound_queue(scenario.broker(), 1);
    scenario.broker().add_client(1, sender, None).await.unwrap();
    let (sender, _receiver) = test_outbound_queue(scenario.broker(), 1);
    let err = scenario
        .broker()
        .add_client(2, sender, None)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("connection quota"));
    scenario.broker().remove_client(1).await.unwrap();
    let (sender, _receiver) = test_outbound_queue(scenario.broker(), 1);
    scenario.broker().add_client(2, sender, None).await.unwrap();
}

#[tokio::test]
async fn per_identity_connection_quota_releases_after_disconnect() {
    let scenario = Scenario::new_with_quotas(quotas());
    let mut first = scenario.connect().await;
    first.send_durable_connect("shared", 1_000).await;
    let mut second = scenario.connect().await;
    second.send_durable_connect("shared", 1_000).await;
    second
        .expect_err_contains("connection quota exceeded for identity")
        .await;
    first.disconnect().await;
    second.send_durable_connect("shared", 1_000).await;
    second.ping_roundtrip().await;
    second.disconnect().await;
}

#[tokio::test]
async fn transient_subscription_quotas_reject_without_state_and_release() {
    let scenario = Scenario::new_with_quotas(quotas());
    let mut first = scenario.connect_durable("one", 1_000).await;
    first.write_line("SUB _INBOX.one.a sid-1 @latest").await;
    first.write_line("SUB _INBOX.one.b sid-2 @latest").await;
    first
        .expect_err_contains("transient subscription quota exceeded")
        .await;
    assert_eq!(
        scenario.broker().transient.lock().await.subscriptions.len(),
        1
    );

    let mut second = scenario.connect_durable("two", 1_000).await;
    second.write_line("SUB _INBOX.two.a sid-2 @latest").await;
    second.ping_roundtrip().await;
    assert_eq!(
        scenario.broker().transient.lock().await.subscriptions.len(),
        2
    );
    second.write_line("SUB _INBOX.two.b sid-3 @latest").await;
    second
        .expect_err_contains("transient subscription quota exceeded")
        .await;
    first.disconnect().await;
    second.write_line("UNSUB sid-2").await;
    second.ping_roundtrip().await;
    second.write_line("SUB _INBOX.two.b sid-3 @latest").await;
    second.ping_roundtrip().await;
    let mut third = scenario.connect_durable("three", 1_000).await;
    third.write_line("SUB _INBOX.three.a sid-4 @latest").await;
    third.ping_roundtrip().await;
    assert_eq!(
        scenario.broker().transient.lock().await.subscriptions.len(),
        2
    );
    second.disconnect().await;
    third.disconnect().await;
}

#[tokio::test]
async fn durable_consumer_quotas_reject_before_wal_state() {
    let scenario = Scenario::new_with_quotas(quotas());
    let mut first = scenario.connect_durable("one", 1_000).await;
    first.write_line("SUB orders.one sid-1 @earliest").await;
    first.write_line("SUB orders.two sid-2 @earliest").await;
    first
        .expect_err_contains("durable consumer quota exceeded")
        .await;
    assert_eq!(scenario.broker().inner.lock().await.consumers.len(), 1);

    let mut second = scenario.connect_durable("two", 1_000).await;
    second.write_line("SUB orders.two sid-2 @earliest").await;
    second.ping_roundtrip().await;
    assert_eq!(scenario.broker().inner.lock().await.consumers.len(), 2);
    second.write_line("SUB orders.three sid-3 @earliest").await;
    second
        .expect_err_contains("durable consumer quota exceeded")
        .await;
    assert_eq!(scenario.broker().inner.lock().await.consumers.len(), 2);
    first.disconnect().await;
    second.disconnect().await;
}

#[tokio::test]
async fn outbound_byte_quota_is_bounded_and_releases_on_drain() {
    let mut limits = quotas();
    limits.max_outbound_bytes_per_connection = 8;
    let scenario = Scenario::new_with_quotas(limits);
    let (queue, mut receiver) = test_outbound_queue(scenario.broker(), 8);
    queue.send(vec![0; 8]).await.unwrap();
    assert!(queue.send(vec![1]).await.is_err());
    drop(receiver.recv().await.unwrap());
    queue.send(vec![1]).await.unwrap();
    assert_eq!(scenario.broker().quotas.snapshot().outbound_rejections, 1);
}

#[test]
fn listener_semaphores_bound_connection_floods_and_release() {
    let mut limits = quotas();
    limits.max_connections = 1;
    let runtime = crate::quota::QuotaRuntime::new(&limits);
    for acquire in [
        crate::quota::QuotaRuntime::try_client,
        crate::quota::QuotaRuntime::try_http,
        crate::quota::QuotaRuntime::try_raft,
        crate::quota::QuotaRuntime::try_route,
    ] {
        let permit = acquire(&runtime).unwrap();
        for _ in 0..1_000 {
            assert!(acquire(&runtime).is_none());
        }
        drop(permit);
        assert!(acquire(&runtime).is_some());
    }
}

#[tokio::test]
async fn configured_idle_client_is_closed_at_deadline() {
    let mut limits = quotas();
    limits.client_idle_timeout_ms = 20;
    let scenario = Scenario::new_with_quotas(limits);
    let (mut client, server) = tokio::io::duplex(4_096);
    let broker = scenario.broker().clone();
    let task = tokio::spawn(async move { broker.handle_client_for_test(server).await });
    let mut info = Vec::new();
    BufReader::new(&mut client)
        .read_until(b'\n', &mut info)
        .await
        .unwrap();
    client
        .write_all(b"CONNECT {\"durable_id\":\"idle\"}\r\n")
        .await
        .unwrap();
    let err = tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert!(err.to_string().contains("client idle read timed out"));
    assert!(
        scenario
            .broker()
            .connections
            .lock()
            .await
            .clients
            .is_empty()
    );
}

#[tokio::test]
async fn admin_header_deadline_closes_slowloris_and_metrics_report_rejections() {
    let mut limits = quotas();
    limits.http_header_timeout_ms = 20;
    let scenario = Scenario::new_with_quotas(limits);
    let (mut client, server) = tokio::io::duplex(1_024);
    let broker = scenario.broker().clone();
    let task = tokio::spawn(async move { broker.handle_http_status(server).await });
    client.write_all(b"GET /quotas HTTP/1.1\r\n").await.unwrap();
    let err = tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert!(err.to_string().contains("header read timed out"));

    scenario.broker().quotas.reject_state();
    scenario.broker().quotas.reject_outbound();
    let response = http_request(scenario.broker(), "/quotas").await;
    assert!(response.contains("\"state_rejections\":1"));
    assert!(response.contains("\"outbound_rejections\":1"));
    assert!(response.contains("\"outbound_bytes_per_connection_limit\":1024"));
}
