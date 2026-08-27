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

#[test]
fn startup_assignment_balances_partition_order_across_streams() {
    let dir = TempDir::new().unwrap();
    let mut config = test_config(dir.path());
    let mut second = config.streams.definitions()[0].clone();
    second.name = crate::stream::StreamId::new("payments").unwrap();
    second.subjects = vec!["payments/**".into()];
    config.streams =
        crate::stream::StreamCatalog::new(vec![config.streams.definitions()[0].clone(), second])
            .unwrap();
    config.cluster = Some(fake_cluster_config(dir.path(), 4, 4));
    config.cluster.as_mut().unwrap().role = crate::config::ClusterRole::Broker;
    config.cluster.as_mut().unwrap().controller_voters = vec![1, 2];

    let assigned = crate::broker::broker_lifecycle::startup_assigned_partitions(&config).unwrap();
    let data_nodes = [3, 4];
    let orders_owned =
        crate::raft::runtime::initial_partition_replicas("orders", 0, &data_nodes, 1).contains(&4);
    let payments_owned =
        crate::raft::runtime::initial_partition_replicas("payments", 0, &data_nodes, 1)
            .contains(&4);
    assert_eq!(assigned.contains(&("orders".to_string(), 0)), orders_owned);
    assert_eq!(
        assigned.contains(&("payments".to_string(), 0)),
        payments_owned
    );
}
