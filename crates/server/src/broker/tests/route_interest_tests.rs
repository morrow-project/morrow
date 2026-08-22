use super::*;

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
            RouteDirection::Outbound,
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
