use super::*;
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
        subjects: vec!["orders.>".into()],
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
                fsync: false,
                committed_high_watermark: offset.checked_sub(1),
                envelope: crate::partition_log::MessageEnvelope {
                    namespace: "default".into(),
                    stream: definition.name.clone(),
                    partition: crate::stream::PartitionId(0),
                    offset,
                    subject: "orders.created".into(),
                    key: None,
                    headers: vec![],
                    timestamp_ms,
                    reply_to: None,
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
fn consumer_metadata_upsert_and_delete_are_consensus_managed() {
    let mut state = DurableState::new(nodes());
    let record = ConsumerRecord {
        consumer_id: "durable-client-sid".into(),
        filter_subject: "orders.*".into(),
        queue_group: None,
        ack_timeout_ms: 30_000,
        max_in_flight: 1024,
        start_position: protocol::StartPosition::Latest,
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
