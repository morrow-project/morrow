# Cluster node replacement

Symptoms: a node is unavailable, unhealthy, or permanently decommissioned.

Safety: fence the old node and verify it cannot rejoin with stale storage or
credentials. Do not assign leadership to an uncaught-up replacement.

Diagnosis: use health, cluster, routes, storage, and metrics to record node
ID, role, assignments, leader epochs, and lag. Optional node-local checks are
secondary.

Recovery: provision the replacement with a new identity, approved certificates,
and capacity. Add it as a replica, catch up, verify quorum, transfer leadership
only when safe, then remove the old replica through the reassignment workflow.

Verification: expected quorum and routes are healthy, no stale node is visible,
and canary traffic survives a restart. Escalate when a partition has no safe
replica or when the old identity cannot be fenced.
