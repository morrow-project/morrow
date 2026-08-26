use super::partition_expansion::*;

#[test]
fn expansion_requires_prepared_partitions_before_epoch_activation() {
    let mut expansion = PartitionExpansion::new(2, 7).unwrap();
    assert_eq!(expansion.begin(4).unwrap().epoch, 8);
    assert!(!expansion.activate());
    expansion.mark_prepared(4);
    assert!(expansion.activate());
    assert_eq!(expansion.current(), (4, 8));
    assert_eq!(
        expansion.decide(7),
        EpochDecision::RefreshRequired { current_epoch: 8 }
    );
}

#[test]
fn expansion_rejects_non_monotonic_or_overlapping_plans() {
    let mut expansion = PartitionExpansion::new(2, 1).unwrap();
    assert!(expansion.begin(1).is_none());
    assert!(expansion.begin(3).is_some());
    assert!(expansion.begin(4).is_none());
    assert!(!expansion.mark_prepared(5));
}
