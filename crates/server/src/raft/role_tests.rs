use super::runtime::data_node_ids_for_role;
use crate::config::ClusterRole;

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
