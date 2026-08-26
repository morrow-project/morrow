use super::state_shards::*;

#[test]
fn shard_selection_is_stable_and_bounded() {
    let key = StateShardKey::Partition {
        stream: "orders",
        partition: 7,
    };
    let selected = shard_for(key.clone(), 16);
    assert!(selected < 16);
    assert_eq!(selected, shard_for(key, 16));
}

#[test]
fn ownership_domains_are_distinct() {
    assert_ne!(
        shard_for(StateShardKey::Producer("orders"), 31),
        shard_for(StateShardKey::Tenant("orders"), 31)
    );
}

#[test]
fn every_ownership_domain_is_bounded_and_restart_stable() {
    let keys = [
        StateShardKey::Partition {
            stream: "orders",
            partition: 3,
        },
        StateShardKey::Consumer("consumer-1"),
        StateShardKey::Producer("producer-1"),
        StateShardKey::Tenant("tenant-1"),
    ];
    for key in keys {
        let first = shard_for(key.clone(), 64);
        assert!(first < 64);
        assert_eq!(first, shard_for(key, 64));
    }
}
