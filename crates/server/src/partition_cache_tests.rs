use super::partition_cache::*;

#[test]
fn cache_evicts_idle_resources_at_a_hard_bound() {
    let mut cache = PartitionResourceCache::new(2).unwrap();
    cache.insert("a", 1);
    cache.insert("b", 2);
    assert_eq!(cache.get(&"a"), Some(&1));
    cache.insert("c", 3);
    assert_eq!(cache.len(), 2);
    assert!(cache.get(&"b").is_none());
    assert_eq!(cache.evictions(), 1);
}
