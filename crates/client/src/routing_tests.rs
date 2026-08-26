use super::routing::*;

fn metadata(epoch: u64) -> StreamMetadata {
    StreamMetadata {
        name: "orders".to_string(),
        partitions: 2,
        partitioning_epoch: epoch,
        partitioning: Partitioning::Key,
        leaders: vec![
            PartitionLeader {
                partition: 0,
                leader_epoch: 1,
                address: "a".to_string(),
            },
            PartitionLeader {
                partition: 1,
                leader_epoch: 1,
                address: "b".to_string(),
            },
        ],
    }
}

#[test]
fn cache_routes_keys_and_rejects_stale_metadata() {
    let mut cache = PartitionLeaderCache::new(1).unwrap();
    assert!(cache.insert(metadata(2)));
    assert!(!cache.insert(metadata(1)));
    let route = cache
        .route("orders", "orders.created", Some(b"key"), 0)
        .unwrap();
    assert!(route.address == "a" || route.address == "b");
    assert_eq!(
        cache.partition_for("orders", "orders.created", Some(b"key"), 0),
        Some(route.partition)
    );
    cache.invalidate("orders", 2);
    assert!(
        cache
            .route("orders", "orders.created", Some(b"key"), 0)
            .is_none()
    );
}
