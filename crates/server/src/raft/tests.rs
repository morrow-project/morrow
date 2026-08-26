use super::*;
use crate::partition_log::MessageEnvelope;
use crate::stream::PartitionId;
use crate::stream::{
    PartitionFallback, PartitioningPolicy, PartitioningStrategy, RetentionPolicy, StoragePolicy,
    StreamDefinition, StreamId,
};

fn nodes() -> BTreeMap<u64, BasicNode> {
    [(1, BasicNode::new("127.0.0.1:5221"))]
        .into_iter()
        .collect()
}

fn stream() -> StreamDefinition {
    StreamDefinition {
        name: StreamId::new("orders").unwrap(),
        subjects: vec!["orders/**".into()],
        partitions: 1,
        partitioning: PartitioningPolicy {
            strategy: PartitioningStrategy::Key,
            fallback: PartitionFallback::Sticky,
            epoch: 1,
        },
        storage: StoragePolicy::default(),
        retention: RetentionPolicy::default(),
    }
}

#[test]
fn replica_data_retention_rewrites_physical_history() {
    let dir = tempfile::TempDir::new().unwrap();
    let mut definition = stream();
    definition.retention.max_age_ms = Some(10);
    let catalog = crate::stream::StreamCatalog::new(vec![definition.clone()]).unwrap();
    let mut store = ReplicaDataStore::open(dir.path(), &catalog, 256).unwrap();
    for (offset, timestamp_ms) in [(0_u64, 0), (1_u64, 20)] {
        store
            .append(&DataAppendRequest {
                leader_id: 1,
                leader_epoch: 1,
                replica_set_generation: 1,
                fsync: false,
                committed_high_watermark: offset.checked_sub(1),
                predecessor_offset: offset.checked_sub(1),
                predecessor_checksum: None,
                batch_digest: crate::partition_log::committed_envelope_checksum(
                    &crate::partition_log::MessageEnvelope {
                        namespace: "default".into(),
                        stream: definition.name.clone(),
                        partition: crate::stream::PartitionId(0),
                        offset,
                        subject: "orders/created".into(),
                        key: None,
                        headers: vec![],
                        timestamp_ms,
                        reply_to: None,
                        schema_id: None,
                        payload: vec![offset as u8],
                        partitioning_epoch: 0,
                        leader_epoch: 1,
                        legacy_seq: offset,
                    },
                )
                .unwrap(),
                durability: DurabilityBoundary::Memory,
                envelope: crate::partition_log::MessageEnvelope {
                    namespace: "default".into(),
                    stream: definition.name.clone(),
                    partition: crate::stream::PartitionId(0),
                    offset,
                    subject: "orders/created".into(),
                    key: None,
                    headers: vec![],
                    timestamp_ms,
                    reply_to: None,
                    schema_id: None,
                    payload: vec![offset as u8],
                    partitioning_epoch: 1,
                    leader_epoch: 1,
                    legacy_seq: offset + 1,
                },
            })
            .unwrap();
    }
    store.enforce_retention(&[definition], 21).unwrap();
    drop(store);

    let (_, replay) =
        crate::partition_log::PartitionLogSet::open(dir.path(), &catalog, 256).unwrap();
    assert_eq!(replay.len(), 1);
    assert_eq!(replay[0].offset, 1);
}

fn assignment() -> HashMap<String, PartitionAssignmentMetadata> {
    [(
        partition_key("orders", 0),
        PartitionAssignmentMetadata {
            replicas: [1].into_iter().collect(),
            active_commit_set: [1].into_iter().collect(),
            replica_set_generation: 1,
            phase: PartitionReconfigurationPhase::Stable,
            leader_id: 1,
            leader_epoch: 1,
        },
    )]
    .into_iter()
    .collect()
}

#[tokio::test]
async fn raft_request_rejects_invalid_auth_token() {
    let request = AuthenticatedRaftRequest {
        node_id: 1,
        auth_token: "wrong-token".into(),
        request: RaftRequest::FullSnapshot {
            vote: Vote::new_committed(1, 1),
            meta: SnapshotMeta {
                last_log_id: None,
                last_membership: StoredMembership::new(None, Membership::new(vec![], nodes())),
                snapshot_id: "test".into(),
            },
            data: Vec::new(),
        },
    };
    let mut frame = Vec::new();
    write_frame(&mut frame, &request).await.unwrap();
    let mut reader = &frame[..];

    let err = read_authenticated_request(&mut reader, "right-token", None)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("invalid Raft auth token"));
}

#[tokio::test]
async fn raft_request_rejects_node_id_that_differs_from_certificate() {
    let request = AuthenticatedRaftRequest {
        node_id: 1,
        auth_token: "right-token".into(),
        request: RaftRequest::FullSnapshot {
            vote: Vote::new_committed(1, 1),
            meta: SnapshotMeta {
                last_log_id: None,
                last_membership: StoredMembership::new(None, Membership::new(vec![], nodes())),
                snapshot_id: "test".into(),
            },
            data: Vec::new(),
        },
    };
    let mut frame = Vec::new();
    write_frame(&mut frame, &request).await.unwrap();
    let mut reader = &frame[..];

    let err = read_authenticated_request(&mut reader, "right-token", Some(2))
        .await
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("Raft request node ID does not match peer certificate")
    );
}

#[tokio::test]
async fn binary_partition_frames_preserve_byte_strings_and_reject_unknown_versions() {
    let request = RaftRequest::DataAppend(DataAppendRequest {
        leader_id: 1,
        leader_epoch: 2,
        replica_set_generation: 1,
        fsync: true,
        committed_high_watermark: Some(4),
        predecessor_offset: Some(4),
        predecessor_checksum: Some(7),
        batch_digest: 0,
        durability: DurabilityBoundary::LocalFlush,
        envelope: MessageEnvelope {
            namespace: "default".into(),
            stream: StreamId::new("orders").unwrap(),
            partition: PartitionId(0),
            offset: 5,
            subject: "orders.created".into(),
            key: Some(vec![0, 1, 255]),
            headers: Vec::new(),
            timestamp_ms: 1,
            reply_to: None,
            schema_id: None,
            payload: vec![0, 127, 255],
            partitioning_epoch: 1,
            leader_epoch: 2,
            legacy_seq: 0,
        },
    });
    let mut frame = Vec::new();
    write_frame(&mut frame, &request).await.unwrap();
    assert_eq!(frame[4], RAFT_PROTOCOL_VERSION);
    assert!(!frame.windows(3).any(|window| window == [0, b',', 1]));
    let mut reader = &frame[..];
    let decoded: RaftRequest = read_frame(&mut reader).await.unwrap();
    assert!(matches!(decoded, RaftRequest::DataAppend(_)));

    let batch = RaftRequest::DataAppendBatch(vec![match request {
        RaftRequest::DataAppend(request) => request,
        _ => unreachable!(),
    }]);
    let mut batch_frame = Vec::new();
    write_frame(&mut batch_frame, &batch).await.unwrap();
    let mut batch_reader = &batch_frame[..];
    assert!(matches!(
        read_frame::<_, RaftRequest>(&mut batch_reader).await.unwrap(),
        RaftRequest::DataAppendBatch(requests) if requests.len() == 1
    ));

    frame[4] = RAFT_PROTOCOL_VERSION.saturating_add(1);
    let mut reader = &frame[..];
    let error = read_frame::<_, RaftRequest>(&mut reader).await.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("unsupported Raft protocol version")
    );
}

#[test]
fn metadata_bootstrap_and_partition_commit_contain_no_message_data() {
    let mut state = DurableState::new(nodes());
    assert_eq!(
        state.apply_command(BrokerCommand::MetadataBootstrap {
            streams: vec![stream()],
            assignments: assignment(),
            security_references: ["cluster-auth-token".into()].into_iter().collect(),
            feature_gates: ["controller-directed-replication-v1".into()]
                .into_iter()
                .collect(),
        }),
        BrokerResponse::MetadataBootstrap
    );
    assert_eq!(
        state.apply_command(BrokerCommand::PartitionCommit {
            stream: "orders".into(),
            partition: 0,
            offset: 0,
            checksum: 7,
            leader_id: 1,
            leader_epoch: 1,
        }),
        BrokerResponse::PartitionCommit {
            high_watermark: 0,
            leader_epoch: 1,
        }
    );
    assert!(state.messages.is_empty());
    let encoded = serde_json::to_vec(&state).unwrap();
    assert!(
        !encoded
            .windows(b"payload-marker".len())
            .any(|bytes| bytes == b"payload-marker")
    );
    assert!(encoded.len() < 4_096);
}

#[test]
fn partition_commit_is_idempotent_but_rejects_gaps_and_stale_epochs() {
    let mut state = DurableState::new(nodes());
    state.apply_command(BrokerCommand::MetadataBootstrap {
        streams: vec![stream()],
        assignments: assignment(),
        security_references: BTreeSet::new(),
        feature_gates: BTreeSet::new(),
    });
    let first = BrokerCommand::PartitionCommit {
        stream: "orders".into(),
        partition: 0,
        offset: 0,
        checksum: 7,
        leader_id: 1,
        leader_epoch: 1,
    };
    assert!(matches!(
        state.apply_command(first.clone()),
        BrokerResponse::PartitionCommit { .. }
    ));
    assert!(matches!(
        state.apply_command(first),
        BrokerResponse::PartitionCommit { .. }
    ));
    assert_eq!(
        state.apply_command(BrokerCommand::PartitionCommit {
            stream: "orders".into(),
            partition: 0,
            offset: 1,
            checksum: 8,
            leader_id: 2,
            leader_epoch: 0,
        }),
        BrokerResponse::Noop
    );
    assert_eq!(
        state.apply_command(BrokerCommand::PartitionCommit {
            stream: "orders".into(),
            partition: 0,
            offset: 2,
            checksum: 9,
            leader_id: 2,
            leader_epoch: 2,
        }),
        BrokerResponse::Noop
    );
}

#[test]
fn partition_leader_epoch_must_be_committed_before_the_new_leader_can_commit_data() {
    let mut state = DurableState::new(nodes());
    let mut assignments = assignment();
    assignments
        .get_mut(&partition_key("orders", 0))
        .unwrap()
        .replicas
        .insert(2);
    assignments
        .get_mut(&partition_key("orders", 0))
        .unwrap()
        .active_commit_set
        .insert(2);
    state.apply_command(BrokerCommand::MetadataBootstrap {
        streams: vec![stream()],
        assignments,
        security_references: BTreeSet::new(),
        feature_gates: BTreeSet::new(),
    });
    assert_eq!(
        state.apply_command(BrokerCommand::PartitionCommit {
            stream: "orders".into(),
            partition: 0,
            offset: 0,
            checksum: 7,
            leader_id: 2,
            leader_epoch: 2,
        }),
        BrokerResponse::Noop
    );
    assert_eq!(
        state.apply_command(BrokerCommand::PartitionLeaderUpdate {
            stream: "orders".into(),
            partition: 0,
            leader_id: 2,
            leader_epoch: 2,
        }),
        BrokerResponse::PartitionLeaderUpdate {
            leader_id: 2,
            leader_epoch: 2,
        }
    );
    assert_eq!(
        state.apply_command(BrokerCommand::PartitionCommit {
            stream: "orders".into(),
            partition: 0,
            offset: 0,
            checksum: 7,
            leader_id: 2,
            leader_epoch: 2,
        }),
        BrokerResponse::PartitionCommit {
            high_watermark: 0,
            leader_epoch: 2,
        }
    );
}

#[test]
fn fenced_reconfiguration_requires_catch_up_before_activation_and_fences_old_generation() {
    let mut state = DurableState::new(nodes());
    let mut assignments = assignment();
    let metadata = assignments.get_mut(&partition_key("orders", 0)).unwrap();
    metadata.replicas.insert(2);
    state.apply_command(BrokerCommand::MetadataBootstrap {
        streams: vec![stream()],
        assignments,
        security_references: BTreeSet::new(),
        feature_gates: BTreeSet::new(),
    });
    assert!(matches!(
        state.apply_command(BrokerCommand::PartitionCommit {
            stream: "orders".into(),
            partition: 0,
            offset: 0,
            checksum: 7,
            leader_id: 1,
            leader_epoch: 1,
        }),
        BrokerResponse::PartitionCommit { .. }
    ));
    assert!(matches!(
        state.apply_command(BrokerCommand::PartitionReconfiguration {
            stream: "orders".into(),
            partition: 0,
            generation: 1,
            phase: PartitionReconfigurationPhase::CatchingUp {
                candidate: 2,
                committed_offset: 0,
                digest: 7
            },
            replicas: [1, 2].into_iter().collect(),
            active_commit_set: [1].into_iter().collect(),
            leader_id: 1,
            leader_epoch: 1,
            committed_offset: None,
            committed_checksum: None,
        }),
        BrokerResponse::PartitionReconfiguration { .. }
    ));
    assert_eq!(
        state.apply_command(BrokerCommand::PartitionReconfiguration {
            stream: "orders".into(),
            partition: 0,
            generation: 2,
            phase: PartitionReconfigurationPhase::Activating { candidate: 2 },
            replicas: [1, 2].into_iter().collect(),
            active_commit_set: [1, 2].into_iter().collect(),
            leader_id: 1,
            leader_epoch: 2,
            committed_offset: Some(0),
            committed_checksum: Some(7),
        }),
        BrokerResponse::PartitionReconfiguration {
            generation: 2,
            phase: PartitionReconfigurationPhase::Activating { candidate: 2 },
        }
    );
    assert_eq!(
        state.apply_command(BrokerCommand::PartitionCommit {
            stream: "orders".into(),
            partition: 0,
            offset: 1,
            checksum: 8,
            leader_id: 1,
            leader_epoch: 2,
        }),
        BrokerResponse::Noop
    );
    assert!(matches!(
        state.apply_command(BrokerCommand::PartitionReconfiguration {
            stream: "orders".into(),
            partition: 0,
            generation: 2,
            phase: PartitionReconfigurationPhase::Stable,
            replicas: [1, 2].into_iter().collect(),
            active_commit_set: [1, 2].into_iter().collect(),
            leader_id: 2,
            leader_epoch: 3,
            committed_offset: None,
            committed_checksum: None,
        }),
        BrokerResponse::PartitionReconfiguration { .. }
    ));
    assert!(matches!(
        state.apply_command(BrokerCommand::PartitionCommit {
            stream: "orders".into(),
            partition: 0,
            offset: 1,
            checksum: 8,
            leader_id: 2,
            leader_epoch: 3,
        }),
        BrokerResponse::PartitionCommit { .. }
    ));
}

#[test]
fn consumer_metadata_upsert_and_delete_are_consensus_managed() {
    let mut state = DurableState::new(nodes());
    let record = ConsumerRecord {
        consumer_id: "durable-client-sid".into(),
        filter_subject: "orders/*".into(),
        queue_group: None,
        ack_timeout_ms: 30_000,
        max_in_flight: 1024,
        start_position: protocol::StartPosition::Latest,
        retry_policy: protocol::RetryPolicy::default(),
    };
    assert_eq!(
        state.apply_command(BrokerCommand::ConsumerUpsert {
            record: record.clone(),
        }),
        BrokerResponse::ConsumerUpsert
    );
    assert_eq!(state.consumers["durable-client-sid"].record, record);
    assert_eq!(
        state.apply_command(BrokerCommand::ConsumerDelete {
            consumer_id: "durable-client-sid".into(),
        }),
        BrokerResponse::ConsumerDelete
    );
    assert!(state.consumers.is_empty());
}

#[test]
fn consumer_group_metadata_and_offsets_are_consensus_managed() {
    let mut coordinator =
        crate::consumer_group::GroupCoordinator::new(3, Default::default()).unwrap();
    coordinator.join("member-a", None, 0).unwrap();
    let record = coordinator.record();
    let mut state = DurableState::new(nodes());
    assert_eq!(
        state.apply_command(BrokerCommand::GroupUpsert {
            group: "orders".into(),
            record: record.clone(),
        }),
        BrokerResponse::GroupUpsert
    );
    assert_eq!(state.groups["orders"], record);
    assert_eq!(
        state.groups["orders"].snapshot.committed_offsets,
        record.snapshot.committed_offsets
    );
}

#[test]
fn policy_replacements_are_monotonic_and_consensus_managed() {
    let mut state = DurableState::new(nodes());
    let snapshot = crate::tenancy::PolicySnapshot {
        generation: 4,
        roles: [(
            "publisher".to_string(),
            crate::tenancy::Role {
                name: "publisher".to_string(),
                permissions: [crate::tenancy::Permission::Publish].into_iter().collect(),
            },
        )]
        .into_iter()
        .collect(),
        bindings: Vec::new(),
    };
    assert_eq!(
        state.apply_command(BrokerCommand::PolicyReplace {
            snapshot: snapshot.clone(),
        }),
        BrokerResponse::PolicyReplace { generation: 4 }
    );
    assert_eq!(state.policy, Some(snapshot.clone()));
    assert_eq!(
        state.apply_command(BrokerCommand::PolicyReplace {
            snapshot: crate::tenancy::PolicySnapshot {
                generation: 3,
                ..snapshot
            },
        }),
        BrokerResponse::Noop
    );
}
