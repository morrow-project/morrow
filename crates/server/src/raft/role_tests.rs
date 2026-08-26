use super::runtime::data_node_ids_for_role;
use super::state_machine::StateMachineStore;
use crate::config::ClusterRole;
use crate::raft::{DurableState, MetadataSnapshot};
use openraft::BasicNode;
use std::collections::BTreeMap;

#[test]
fn separated_controller_keeps_non_voters_as_data_nodes_for_bootstrap() {
    let nodes = data_node_ids_for_role(ClusterRole::Controller, &[1, 2, 3], [1, 2, 3, 4, 5]);
    assert_eq!(nodes.into_iter().collect::<Vec<_>>(), vec![4, 5]);
}

#[test]
fn combined_cluster_keeps_all_nodes_as_data_nodes() {
    let nodes = data_node_ids_for_role(ClusterRole::Combined, &[1, 2, 3], [1, 2, 3, 4, 5]);
    assert_eq!(nodes.into_iter().collect::<Vec<_>>(), vec![1, 2, 3, 4, 5]);
}

#[test]
fn broker_metadata_snapshot_survives_restart() {
    let directory = tempfile::TempDir::new().unwrap();
    let open = || {
        StateMachineStore::open(
            directory.path().join("state.journal"),
            directory.path().join("snapshot.json"),
            directory.path().join("legacy.json"),
            directory.path().join("legacy-snapshot.json"),
            BTreeMap::from([(1, BasicNode::new("127.0.0.1:1".to_string()))]),
        )
        .unwrap()
    };
    let store = open();
    let state = DurableState::new(BTreeMap::new());
    let metadata = MetadataSnapshot::from_state(&state);
    store.install_metadata(metadata.clone());
    drop(store);
    assert_eq!(
        MetadataSnapshot::from_state(&open().durable_state()),
        metadata
    );
}
