use super::*;

#[tokio::test]
async fn configured_http_listener_bind_failure_fails_server_startup() {
    let dir = TempDir::new().unwrap();
    let occupied = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http_addr = occupied.local_addr().unwrap();
    let native_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();

    let mut config = test_config(dir.path());
    config.http_listen = Some(http_addr);
    let broker = Morrow::open(config).unwrap();

    let error = broker
        .serve_listener(native_listener)
        .await
        .expect_err("startup should fail when the configured HTTP listener is occupied");
    assert!(error.to_string().contains("HTTP status listener"));
}

#[tokio::test]
async fn shutdown_marks_readiness_unavailable_before_cleanup() {
    let dir = TempDir::new().unwrap();
    let broker = Morrow::open(test_config(dir.path())).unwrap();

    broker.shutdown().await.unwrap();

    let health = broker.health_response().await;
    assert_eq!(health.status, "degraded");
    assert_eq!(health.reason, Some("shutting_down"));
}
