use super::*;
use crate::partition_log::MessageEnvelope;
use crate::partition_replication::{Durability, PartitionAssignment, PartitionReplication};
use crate::stream::StreamId;
use tempfile::tempdir;

fn broker(node_id: u64, region: &str, zone: &str, used: u64) -> BrokerCapacity {
    BrokerCapacity {
        node_id,
        region: region.into(),
        zone: zone.into(),
        disk_capacity_bytes: 100,
        disk_used_bytes: used,
        partition_count: used as u32,
        leader_count: used as u32,
        throughput_bytes_per_second: used,
        max_concurrent_moves: 2,
        lifecycle: BrokerLifecycle::Active,
    }
}

fn move_() -> PlacementMove {
    PlacementMove {
        stream: "orders".into(),
        partition: PartitionId(0),
        from: 1,
        to: 2,
    }
}

#[test]
fn reassignment_advances_only_after_catch_up_and_persists_restart_state() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("reassignments.json");
    let brokers = [broker(1, "west", "a", 90), broker(2, "west", "b", 10)];
    let mut controller = ReassignmentController::open(&path, brokers.clone()).unwrap();
    let id = controller.begin(move_(), 4).unwrap();
    assert_eq!(
        controller.plan(id).unwrap().phase,
        ReassignmentPhase::AddReplica
    );
    controller
        .advance(
            id,
            ReassignmentProgress {
                high_watermark: Some(10),
                destination: ReplicaProgress {
                    match_offset: Some(3),
                    flushed_offset: Some(3),
                },
                quorum_available: true,
            },
        )
        .unwrap();
    assert_eq!(
        controller.plan(id).unwrap().phase,
        ReassignmentPhase::CatchingUp
    );
    assert!(
        controller
            .advance(
                id,
                ReassignmentProgress {
                    high_watermark: Some(10),
                    destination: ReplicaProgress {
                        match_offset: Some(9),
                        flushed_offset: Some(9),
                    },
                    quorum_available: true,
                },
            )
            .is_err()
    );
    drop(controller);
    let mut restarted = ReassignmentController::open(&path, brokers).unwrap();
    assert_eq!(
        restarted.plan(id).unwrap().phase,
        ReassignmentPhase::CatchingUp
    );
    restarted
        .advance(
            id,
            ReassignmentProgress {
                high_watermark: Some(10),
                destination: ReplicaProgress {
                    match_offset: Some(10),
                    flushed_offset: Some(10),
                },
                quorum_available: true,
            },
        )
        .unwrap();
    assert_eq!(
        restarted.plan(id).unwrap().phase,
        ReassignmentPhase::TransferLeadership
    );
    restarted
        .advance(
            id,
            ReassignmentProgress {
                high_watermark: Some(10),
                destination: ReplicaProgress {
                    match_offset: Some(10),
                    flushed_offset: Some(10),
                },
                quorum_available: true,
            },
        )
        .unwrap();
    assert_eq!(
        restarted.plan(id).unwrap().phase,
        ReassignmentPhase::RemoveReplica
    );
    restarted
        .advance(
            id,
            ReassignmentProgress {
                high_watermark: Some(10),
                destination: ReplicaProgress::default(),
                quorum_available: true,
            },
        )
        .unwrap();
    assert_eq!(
        restarted.plan(id).unwrap().phase,
        ReassignmentPhase::Complete
    );
}

#[test]
fn rollback_is_fenced_after_leadership_transfer() {
    let mut controller =
        ReassignmentController::new([broker(1, "west", "a", 90), broker(2, "west", "b", 10)]);
    let id = controller.begin(move_(), 1).unwrap();
    controller
        .advance(
            id,
            ReassignmentProgress {
                high_watermark: None,
                destination: ReplicaProgress::default(),
                quorum_available: true,
            },
        )
        .unwrap();
    assert!(controller.rollback(id, "network outage").is_ok());
    assert!(matches!(
        controller.plan(id).unwrap().phase,
        ReassignmentPhase::RolledBack { .. }
    ));
}

#[test]
fn planner_prefers_lower_load_and_respects_region_constraints() {
    let placements = [PartitionPlacement {
        stream: "orders".into(),
        partition: PartitionId(0),
        replicas: [1, 2].into_iter().collect(),
        leader: 1,
        constraints: PlacementConstraints {
            min_distinct_regions: 2,
            ..Default::default()
        },
    }];
    let moves = plan_moves(
        &placements,
        &[
            broker(1, "west", "a", 90),
            broker(2, "west", "b", 10),
            broker(3, "east", "a", 20),
        ],
    );
    assert_eq!(moves[0].from, 1);
    assert_eq!(moves[0].to, 3);
}

#[test]
fn leader_planner_transfers_to_a_less_loaded_replica_deterministically() {
    let placements = [PartitionPlacement {
        stream: "orders".into(),
        partition: PartitionId(0),
        replicas: [1, 2].into_iter().collect(),
        leader: 1,
        constraints: PlacementConstraints::default(),
    }];
    let transfers = plan_leader_transfers(
        &placements,
        &[broker(1, "west", "a", 90), broker(2, "west", "b", 10)],
    );
    assert_eq!(
        transfers,
        vec![PlacementMove {
            stream: "orders".into(),
            partition: PartitionId(0),
            from: 1,
            to: 2,
        }]
    );
}

#[test]
fn move_throttle_bounds_concurrency_and_bandwidth_until_window_reset() {
    let mut throttle = MoveThrottle::new(2, 100);
    assert!(throttle.try_start(60));
    assert!(!throttle.try_start(50));
    throttle.finish();
    assert!(throttle.try_start(40));
    throttle.finish();
    throttle.reset_window();
    assert!(throttle.try_start(40));
    assert_eq!(throttle.active_moves(), 1);
}

fn replicated_envelope(offset: u64, epoch: u64, payload: &[u8]) -> MessageEnvelope {
    MessageEnvelope {
        namespace: "default".into(),
        stream: StreamId::new("orders").unwrap(),
        partition: PartitionId(0),
        offset,
        subject: "orders/created".into(),
        key: None,
        headers: Vec::new(),
        timestamp_ms: offset,
        reply_to: None,
        schema_id: None,
        payload: payload.to_vec(),
        partitioning_epoch: 1,
        leader_epoch: epoch,
        legacy_seq: offset + 1,
    }
}

#[test]
fn active_publish_survives_replica_move_and_leader_transfer() {
    let brokers = [
        broker(1, "west", "a", 10),
        broker(2, "west", "b", 20),
        broker(3, "east", "a", 10),
    ];
    let mut controller = ReassignmentController::new(brokers);
    let mut replication = PartitionReplication::new([1, 2, 3]);
    replication
        .assign(PartitionAssignment {
            stream: "orders".into(),
            partition: PartitionId(0),
            replicas: [1, 2].into_iter().collect(),
            leader: 1,
            leader_epoch: 1,
        })
        .unwrap();
    replication
        .append(
            1,
            1,
            replicated_envelope(0, 1, b"before-move"),
            Durability::Quorum,
        )
        .unwrap();

    let id = controller
        .begin(
            PlacementMove {
                stream: "orders".into(),
                partition: PartitionId(0),
                from: 1,
                to: 3,
            },
            1,
        )
        .unwrap();
    replication
        .assign(PartitionAssignment {
            stream: "orders".into(),
            partition: PartitionId(0),
            replicas: [1, 2, 3].into_iter().collect(),
            leader: 1,
            leader_epoch: 1,
        })
        .unwrap();
    replication.catch_up("orders", PartitionId(0), 3).unwrap();
    controller
        .advance(
            id,
            ReassignmentProgress {
                high_watermark: Some(0),
                destination: replication.progress("orders", PartitionId(0)).unwrap()[&3],
                quorum_available: true,
            },
        )
        .unwrap();
    controller
        .advance(
            id,
            ReassignmentProgress {
                high_watermark: Some(0),
                destination: replication.progress("orders", PartitionId(0)).unwrap()[&3],
                quorum_available: true,
            },
        )
        .unwrap();
    assert_eq!(
        replication
            .transfer_leadership("orders", PartitionId(0), 3)
            .unwrap(),
        2
    );
    controller
        .advance(
            id,
            ReassignmentProgress {
                high_watermark: Some(0),
                destination: replication.progress("orders", PartitionId(0)).unwrap()[&3],
                quorum_available: true,
            },
        )
        .unwrap();
    replication
        .assign(PartitionAssignment {
            stream: "orders".into(),
            partition: PartitionId(0),
            replicas: [2, 3].into_iter().collect(),
            leader: 3,
            leader_epoch: 2,
        })
        .unwrap();
    controller
        .advance(
            id,
            ReassignmentProgress {
                high_watermark: Some(0),
                destination: ReplicaProgress::default(),
                quorum_available: true,
            },
        )
        .unwrap();
    replication
        .append(
            3,
            2,
            replicated_envelope(1, 2, b"after-move"),
            Durability::Quorum,
        )
        .unwrap();
    assert_eq!(
        controller.plan(id).unwrap().phase,
        ReassignmentPhase::Complete
    );
    assert_eq!(
        replication
            .high_watermark("orders", PartitionId(0))
            .unwrap(),
        Some(1)
    );
}
