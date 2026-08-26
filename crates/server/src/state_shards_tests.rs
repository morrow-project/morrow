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
