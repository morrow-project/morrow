use super::*;
use tempfile::tempdir;

fn policy() -> ReplicationPolicy {
    ReplicationPolicy {
        max_lag_offsets: 10,
        max_bandwidth_bytes_per_second: 64,
        max_in_flight_chunks: 1,
    }
}

fn chunk(offset: u64, bytes: &[u8]) -> SegmentChunk {
    SegmentChunk::new(
        "orders",
        PartitionId(0),
        offset,
        offset,
        offset,
        bytes.to_vec(),
    )
}

#[test]
fn replication_resumes_from_durable_checkpoint_and_deduplicates() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("replication.json");
    let mut standby = CrossRegionReplicator::open(&path, policy()).unwrap();
    let token = standby.fencing_token();
    assert!(standby.ship(&chunk(0, b"one"), token).unwrap());
    assert!(!standby.ship(&chunk(0, b"one"), token).unwrap());
    drop(standby);
    let mut restarted = CrossRegionReplicator::open(&path, policy()).unwrap();
    assert_eq!(
        restarted.checkpoint("orders", PartitionId(0)).last_offset,
        Some(0)
    );
    assert!(restarted.ship(&chunk(1, b"two"), token).unwrap());
    assert_eq!(restarted.lag("orders", PartitionId(0), 2), 0);
}

#[test]
fn replication_rejects_conflicts_gaps_bad_tokens_and_throttles() {
    let mut standby = CrossRegionReplicator::new(
        ReplicationPolicy {
            max_lag_offsets: 1,
            max_bandwidth_bytes_per_second: 3,
            max_in_flight_chunks: 1,
        },
        RegionRole::Standby,
    );
    let token = standby.fencing_token();
    assert!(standby.ship(&chunk(0, b"one"), token).is_ok());
    assert!(standby.ship(&chunk(1, b"two"), token).is_err());
    standby.reset_bandwidth_window();
    let mut conflicting = chunk(0, b"bad");
    conflicting.checksum = sha256(b"not-the-body");
    assert!(standby.ship(&conflicting, token).is_err());
    assert!(
        standby
            .ship(&chunk(1, b"two"), token.saturating_add(1))
            .is_err()
    );
}

#[test]
fn promotion_fences_old_primary_and_changes_token() {
    let mut region = CrossRegionReplicator::new(policy(), RegionRole::Standby);
    let old = region.fencing_token();
    let new = region.promote(old).unwrap();
    assert_eq!(region.role(), RegionRole::Primary);
    assert_ne!(old, new);
    assert!(region.promote(old).is_err());
    region.fence().unwrap();
    assert!(region.ship(&chunk(0, b"write"), new).is_err());
}
