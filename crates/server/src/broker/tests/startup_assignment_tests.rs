use super::*;

#[test]
fn startup_assignment_filter_excludes_controller_nodes() {
    let dir = TempDir::new().unwrap();
    let mut config = test_config(dir.path());
    config.cluster = Some(fake_cluster_config(dir.path(), 4, 3));
    config.cluster.as_mut().unwrap().role = crate::config::ClusterRole::Broker;
    config.cluster.as_mut().unwrap().controller_voters = vec![1, 2];

    let assigned = crate::broker::broker_lifecycle::startup_assigned_partitions(&config).unwrap();
    assert!(!assigned.is_empty());
    assert!(assigned.iter().all(|(_, partition)| *partition < 1));
}

#[test]
fn startup_assignment_filter_opens_no_partitions_on_controller_nodes() {
    let dir = TempDir::new().unwrap();
    let mut config = test_config(dir.path());
    config.cluster = Some(fake_cluster_config(dir.path(), 3, 1));
    config.cluster.as_mut().unwrap().role = crate::config::ClusterRole::Controller;

    assert_eq!(
        crate::broker::broker_lifecycle::startup_assigned_partitions(&config),
        Some(BTreeSet::new())
    );
}

#[test]
fn startup_assignment_filter_falls_back_when_unassigned_data_exists() {
    let dir = TempDir::new().unwrap();
    let mut config = test_config(dir.path());
    config.cluster = Some(fake_cluster_config(dir.path(), 4, 3));
    config.cluster.as_mut().unwrap().role = crate::config::ClusterRole::Broker;
    config.cluster.as_mut().unwrap().controller_voters = vec![1, 2];
    std::fs::create_dir_all(dir.path().join("streams/orders/partition-00001")).unwrap();

    assert!(
        crate::broker::broker_lifecycle::startup_assigned_partitions(&config).is_none(),
        "existing data outside the deterministic assignment must trigger safe full recovery"
    );
}
