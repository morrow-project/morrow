use super::*;

#[tokio::test]
async fn wildcard_route_bind_announces_hostname_and_peers_by_node_id() {
    let dir = TempDir::new().unwrap();
    let mut config = test_config(dir.path());
    let mut cluster = fake_cluster_config(dir.path(), 3, 1);
    cluster.route_listen = Some("0.0.0.0:6222".parse().unwrap());
    cluster.route_advertise = Some("broker-1:6222".to_string());
    for node in &mut cluster.nodes {
        node.route_addr = Some(format!("broker-{}:6222", node.node_id));
    }
    config.cluster = Some(cluster);
    let broker = deterministic_broker(config, Arc::new(ManualClock::new(1_000)), None);
    let mesh = broker.route_mesh.clone().unwrap();

    assert!(matches!(
        mesh.hello().await,
        RouteFrame::Hello { node_id: 1, route_addr, .. }
            if route_addr == "broker-1:6222"
    ));
    let (competing_sender, _competing_frames) = mpsc::channel(1);
    assert_eq!(
        mesh.register_peer(
            RoutePeerInfo {
                node_id: 2,
                route_addr: "broker-2:6222".to_string(),
                client_addr: "127.0.0.1:4222".parse().unwrap(),
            },
            RouteDirection::Outbound,
            competing_sender,
        )
        .await,
        None
    );

    let (first_sender, _first_frames) = mpsc::channel(1);
    assert_eq!(
        mesh.register_peer(
            RoutePeerInfo {
                node_id: 2,
                route_addr: "broker-2:6222".to_string(),
                client_addr: "127.0.0.1:4222".parse().unwrap(),
            },
            RouteDirection::Inbound,
            first_sender.clone(),
        )
        .await,
        Some(true)
    );
    let (replacement_sender, _replacement_frames) = mpsc::channel(1);
    assert_eq!(
        mesh.register_peer(
            RoutePeerInfo {
                node_id: 2,
                route_addr: "broker-2-new:6222".to_string(),
                client_addr: "127.0.0.1:4222".parse().unwrap(),
            },
            RouteDirection::Inbound,
            replacement_sender.clone(),
        )
        .await,
        Some(false)
    );
    let (duplicate_sender, _duplicate_frames) = mpsc::channel(1);
    assert_eq!(
        mesh.register_peer(
            RoutePeerInfo {
                node_id: 3,
                route_addr: "broker-2-new:6222".to_string(),
                client_addr: "127.0.0.1:4223".parse().unwrap(),
            },
            RouteDirection::Inbound,
            duplicate_sender,
        )
        .await,
        None
    );

    mesh.remove_peer(2, &first_sender).await;
    assert_eq!(mesh.connected_peer_count().await, 1);
    mesh.remove_peer(2, &replacement_sender).await;
    assert_eq!(mesh.connected_peer_count().await, 0);
}

#[tokio::test]
async fn route_interests_use_reference_counted_deltas_off_the_publish_path() {
    let scenario = Scenario::new_fake_route_cluster_local_node(2, 1, Some(1));
    let mesh = scenario.broker().route_mesh.clone().unwrap();
    let (sender, mut frames) = mpsc::channel(16);
    assert_eq!(
        mesh.register_peer(
            RoutePeerInfo {
                node_id: 2,
                route_addr: "127.0.0.1:39999".to_string(),
                client_addr: "127.0.0.1:19999".parse().unwrap(),
            },
            RouteDirection::Inbound,
            sender,
        )
        .await,
        Some(true)
    );

    let mut first = scenario.connect().await;
    first.subscribe("orders.*", "first").await;
    first.ping_roundtrip().await;
    assert_interest_delta(frames.recv().await.unwrap(), 1, &["orders.*"], &[]);

    let snapshot = mesh.interests().await;
    assert!(matches!(
        snapshot,
        RouteFrame::Interests { version: 1, subjects }
            if subjects == ["orders.*"]
    ));

    let mut duplicate = scenario.connect().await;
    duplicate.subscribe("orders.*", "duplicate").await;
    duplicate.ping_roundtrip().await;
    assert!(frames.try_recv().is_err());

    let mut publisher = scenario.connect_durable("publisher", 25).await;
    publisher.publish("topic", b"unchanged").await;
    publisher.ping_roundtrip().await;
    assert!(frames.try_recv().is_err());

    first.write_line("UNSUB first").await;
    first.ping_roundtrip().await;
    assert!(frames.try_recv().is_err());

    duplicate.disconnect().await;
    assert_interest_delta(frames.recv().await.unwrap(), 2, &[], &["orders.*"]);

    first.subscribe("topic", "limited").await;
    first.write_line("UNSUB limited 1").await;
    first.ping_roundtrip().await;
    assert_interest_delta(frames.recv().await.unwrap(), 3, &["topic"], &[]);
    publisher.publish("topic", b"last").await;
    publisher.ping_roundtrip().await;
    assert_interest_delta(frames.recv().await.unwrap(), 4, &[], &["topic"]);
}

#[tokio::test]
async fn route_interest_versions_detect_gaps_and_accept_full_resync() {
    let scenario = Scenario::new_fake_route_cluster_local_node(2, 1, Some(1));
    let mesh = scenario.broker().route_mesh.clone().unwrap();
    let (sender, _frames) = mpsc::channel(4);
    mesh.register_peer(
        RoutePeerInfo {
            node_id: 2,
            route_addr: "127.0.0.1:39999".to_string(),
            client_addr: "127.0.0.1:19999".parse().unwrap(),
        },
        RouteDirection::Inbound,
        sender,
    )
    .await
    .unwrap();

    mesh.set_remote_interests(2, 7, vec!["orders.*".to_string()])
        .await;
    assert!(
        !mesh
            .apply_remote_interest_delta(2, 9, vec!["topic".to_string()], Vec::new())
            .await
    );
    {
        let state = mesh.inner.lock().await;
        let peer = &state.peers[&2];
        assert_eq!(peer.remote_interest_version, 7);
        assert_eq!(
            peer.remote_interests.iter().cloned().collect::<Vec<_>>(),
            ["orders.*"]
        );
    }

    mesh.set_remote_interests(2, 10, vec!["service.*".to_string()])
        .await;
    assert!(
        mesh.apply_remote_interest_delta(
            2,
            11,
            vec!["topic".to_string()],
            vec!["service.*".to_string()],
        )
        .await
    );
    let state = mesh.inner.lock().await;
    let peer = &state.peers[&2];
    assert_eq!(peer.remote_interest_version, 11);
    assert_eq!(
        peer.remote_interests.iter().cloned().collect::<Vec<_>>(),
        ["topic"]
    );
}

fn assert_interest_delta(
    frame: RouteFrame,
    expected_version: u64,
    expected_added: &[&str],
    expected_removed: &[&str],
) {
    let RouteFrame::InterestDelta {
        version,
        added,
        removed,
    } = frame
    else {
        panic!("expected interest delta");
    };
    assert_eq!(version, expected_version);
    assert_eq!(added, expected_added);
    assert_eq!(removed, expected_removed);
}
