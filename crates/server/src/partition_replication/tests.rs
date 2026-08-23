use super::*;
use crate::stream::StreamId;

fn assignment() -> PartitionAssignment {
    PartitionAssignment {
        stream: "orders".into(),
        partition: PartitionId(0),
        replicas: [1, 2, 3].into_iter().collect(),
        leader: 1,
        leader_epoch: 7,
    }
}

fn envelope(offset: u64, epoch: u64, payload: &[u8]) -> MessageEnvelope {
    MessageEnvelope {
        namespace: "default".into(),
        stream: StreamId::new("orders").unwrap(),
        partition: PartitionId(0),
        offset,
        subject: "orders/created".into(),
        key: None,
        headers: Vec::new(),
        timestamp_ms: 1,
        reply_to: None,
        schema_id: None,
        payload: payload.to_vec(),
        partitioning_epoch: 1,
        leader_epoch: epoch,
        legacy_seq: offset + 1,
    }
}

fn cluster() -> PartitionReplication {
    let mut cluster = PartitionReplication::new([1, 2, 3]);
    cluster.assign(assignment()).unwrap();
    cluster
}

#[test]
fn quorum_and_quorum_fsync_track_match_and_flush_positions() {
    let mut cluster = cluster();
    let first = cluster
        .append(1, 7, envelope(0, 7, b"one"), Durability::Quorum)
        .unwrap();
    assert_eq!(first.replicated, 3);
    assert_eq!(first.flushed, 0);
    let second = cluster
        .append(1, 7, envelope(1, 7, b"two"), Durability::QuorumFsync)
        .unwrap();
    assert_eq!(second.high_watermark, 1);
    assert_eq!(second.flushed, 3);
    assert_eq!(
        cluster.progress("orders", PartitionId(0)).unwrap()[&2].match_offset,
        Some(1)
    );
}

#[test]
fn follower_lag_catches_up_before_safe_leadership_transfer() {
    let mut cluster = cluster();
    cluster.set_available([1, 2]);
    cluster
        .append(1, 7, envelope(0, 7, b"one"), Durability::Quorum)
        .unwrap();
    assert!(
        cluster
            .transfer_leadership("orders", PartitionId(0), 3)
            .is_err()
    );
    cluster.set_available([1, 2, 3]);
    cluster.catch_up("orders", PartitionId(0), 3).unwrap();
    assert_eq!(
        cluster
            .transfer_leadership("orders", PartitionId(0), 3)
            .unwrap(),
        8
    );
}

#[test]
fn quorum_loss_rejects_append_and_restore_commits_retry() {
    let mut cluster = cluster();
    cluster.set_available([1]);
    let record = envelope(0, 7, b"one");
    assert!(
        cluster
            .append(1, 7, record.clone(), Durability::Quorum)
            .is_err()
    );
    assert_eq!(
        cluster.high_watermark("orders", PartitionId(0)).unwrap(),
        None
    );
    cluster.set_available([1, 2]);
    assert_eq!(
        cluster
            .append(1, 7, record, Durability::Quorum)
            .unwrap()
            .high_watermark,
        0
    );
}

#[test]
fn divergent_uncommitted_suffix_is_truncated_during_replication() {
    let mut cluster = cluster();
    cluster.set_available([1]);
    let committed = envelope(0, 7, b"committed");
    assert!(
        cluster
            .append(1, 7, committed.clone(), Durability::Quorum)
            .is_err()
    );
    cluster
        .inject_uncommitted("orders", PartitionId(0), 2, envelope(0, 7, b"divergent"))
        .unwrap();
    cluster.set_available([1, 2]);
    cluster.append(1, 7, committed, Durability::Quorum).unwrap();
    assert_eq!(
        cluster
            .committed_records("orders", PartitionId(0), 2)
            .unwrap()[0]
            .payload,
        b"committed"
    );
}

#[test]
fn stale_leader_is_fenced_after_transfer() {
    let mut cluster = cluster();
    cluster
        .transfer_leadership("orders", PartitionId(0), 2)
        .unwrap();
    assert!(
        cluster
            .append(1, 7, envelope(0, 7, b"stale"), Durability::Quorum)
            .is_err()
    );
    assert!(
        cluster
            .append(2, 7, envelope(0, 7, b"epoch"), Durability::Quorum)
            .is_err()
    );
    cluster
        .append(2, 8, envelope(0, 8, b"current"), Durability::Quorum)
        .unwrap();
}

#[test]
fn leader_failure_reports_no_safe_replica_instead_of_losing_committed_data() {
    let mut cluster = cluster();
    cluster.set_available([1, 2]);
    cluster
        .append(1, 7, envelope(0, 7, b"one"), Durability::Quorum)
        .unwrap();
    cluster.set_available([3]);
    let error = cluster
        .elect_safe_leader("orders", PartitionId(0))
        .unwrap_err();
    assert!(error.to_string().contains("no safe replica"));
}

#[test]
#[ignore = "manual strategy microbenchmark"]
fn benchmark_controller_directed_against_per_partition_raft_encoding() {
    const RECORDS: u64 = 256;
    const PAYLOAD_BYTES: usize = 64 * 1024;
    let payload = vec![0x5a; PAYLOAD_BYTES];

    let mut controller = cluster();
    let started = std::time::Instant::now();
    for offset in 0..RECORDS {
        controller
            .append(1, 7, envelope(offset, 7, &payload), Durability::Quorum)
            .unwrap();
    }
    let controller_elapsed = started.elapsed();

    let started = std::time::Instant::now();
    let mut encoded_bytes = 0usize;
    for offset in 0..RECORDS {
        let record = envelope(offset, 7, &payload);
        for _ in 0..3 {
            encoded_bytes += std::hint::black_box(serde_json::to_vec(&record).unwrap()).len();
        }
    }
    let raft_encoding_elapsed = started.elapsed();
    println!(
        "records={RECORDS} payload_bytes={PAYLOAD_BYTES} controller_directed_ms={} per_partition_raft_encoding_proxy_ms={} encoded_bytes={encoded_bytes}",
        controller_elapsed.as_millis(),
        raft_encoding_elapsed.as_millis(),
    );
}
