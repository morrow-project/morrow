use super::*;
use protocol::broker_control::{
    BROKER_CONTROL_PROTOCOL_VERSION, BrokerHeartbeat, BrokerRegistration, CapacitySummary,
};

fn registration(broker_id: u64, incarnation: u64, last_revision: u64) -> BrokerRegistration {
    BrokerRegistration {
        protocol_version: BROKER_CONTROL_PROTOCOL_VERSION,
        broker_id,
        incarnation,
        client_addr: format!("127.0.0.1:{}", 10_000 + broker_id),
        replication_addr: None,
        capacity: CapacitySummary::default(),
        feature_gates: vec!["assignments-v1".to_string()],
        security_references: vec!["default".to_string()],
        last_revision,
    }
}

#[tokio::test]
async fn registration_fences_old_incarnations_and_accepts_heartbeats() {
    let broker = Scenario::new().broker().clone();
    let first = broker.register_broker(registration(9, 1, 0)).await.unwrap();
    assert_eq!(first.accepted.session_id, 1);
    assert!(
        broker
            .heartbeat_broker(BrokerHeartbeat {
                protocol_version: BROKER_CONTROL_PROTOCOL_VERSION,
                broker_id: 9,
                incarnation: 1,
                session_id: first.accepted.session_id,
                capacity: CapacitySummary {
                    partition_count: 3,
                    ..Default::default()
                },
            })
            .await
            .is_ok()
    );
    let second = broker.register_broker(registration(9, 2, 0)).await.unwrap();
    assert_eq!(second.fenced_session, Some(first.accepted.session_id));
    assert!(
        broker
            .heartbeat_broker(BrokerHeartbeat {
                protocol_version: BROKER_CONTROL_PROTOCOL_VERSION,
                broker_id: 9,
                incarnation: 1,
                session_id: first.accepted.session_id,
                capacity: CapacitySummary::default(),
            })
            .await
            .is_err()
    );
}

#[tokio::test]
async fn metadata_updates_are_resumable_and_fall_back_to_snapshot() {
    let broker = Scenario::new().broker().clone();
    for payload in [b"one".to_vec(), b"two".to_vec(), b"three".to_vec()] {
        broker.publish_broker_metadata_update(payload).await;
    }
    assert_eq!(
        broker.broker_metadata_updates_after(1).await.unwrap().len(),
        2
    );
    let bounded = BrokerControlRegistry::with_update_window(2);
    bounded.publish_update(b"a".to_vec()).await;
    bounded.publish_update(b"b".to_vec()).await;
    bounded.publish_update(b"c".to_vec()).await;
    assert!(bounded.updates_after(0).await.is_none());
}
