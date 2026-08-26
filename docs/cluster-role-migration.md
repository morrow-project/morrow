# Cluster role migration

Morrow supports three cluster roles:

- `combined`: runs the controller and broker paths in one process; this is the
  simplest development and small-cluster deployment.
- `controller`: participates in the metadata quorum and serves controller
  administration, but does not open the client, partition, or route-mesh data
  paths.
- `broker`: serves clients and owns partition storage/replication, but is not a
  metadata-quorum voter.

## Rolling migration

1. Keep the existing combined nodes in the controller voter set.
2. Start the replacement controller nodes with the same metadata directory
   and an explicit `controller_voters` list. Verify controller readiness before
   changing membership.
3. Add broker-only nodes with node IDs outside `controller_voters`. Verify that
   they report broker readiness and receive assignments.
4. Move partition assignments to the new brokers, then remove old combined
   brokers from the data placement set.
5. After all metadata has been replicated to the dedicated controllers, remove
   the old combined nodes from the controller voter set through the explicit
   controller-membership workflow.

The controller voter list is independent from the broker fleet. Adding or
removing a broker therefore does not resize the metadata quorum.

## Rollback limits

Rollback is safe while at least one compatible controller quorum remains and
metadata directories are preserved. Do not reuse a controller data directory
with an older binary that cannot read its metadata format. A broker can be
removed from placement only after its assigned partitions have been moved or
recovered elsewhere. If the old combined nodes have already been removed from
the voter set, restore them first through the controller membership workflow;
starting them with stale membership data is not a rollback mechanism.

For production migrations, use a three- or five-voter controller quorum and
upgrade all nodes to a version that understands the role fields before changing
roles.
