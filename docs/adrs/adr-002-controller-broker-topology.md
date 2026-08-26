# ADR-002: Fixed controller quorum and independently scalable brokers

## Status

Accepted; introduced incrementally through the P8 topology milestones.

## Decision

Morrow distinguishes a node's process role from its controller-voter
membership. `cluster.role` is `combined` (the backwards-compatible default),
`controller`, or `broker`. `cluster.controller_voters` is an explicit fixed
set of node IDs; when omitted it defaults to the configured nodes for
compatibility with existing combined clusters.

Only controller voters form the OpenRaft metadata membership. A combined node
may be a voter or a non-voting data-plane member. A broker role must not be a
voter, while a controller role must be a voter. Adding data-plane members does
not implicitly change the voter set.

The current release keeps the authenticated internal control and replication
listeners for compatibility. Controller-only processes do not accept client
connections or start the route mesh; broker-only processes serve the data
plane but remain outside the metadata voter set. A fully independent
controller-to-broker registration transport remains a follow-up concern.

## Compatibility and migration

Omitting both new fields preserves the current combined topology and derives
the voter set from `cluster.nodes`. Operators can migrate by first declaring a
fixed voter list on every existing combined node, then adding non-voting broker
members, and finally moving controller and broker roles to dedicated processes.
Rollback is safe while all nodes still understand these fields; older binaries
must not be started with `role` or `controller_voters` until the configuration
is reverted to the legacy combined form.

## Consequences

Metadata quorum size is no longer coupled to the number of data-plane members.
Controller elections and snapshots remain bounded by the explicit voter set,
while partition leadership and replication can expand independently. Dedicated
listener/lifecycle behavior, registration, and rolling migration are tracked by
issues #213 and #214 and must preserve this contract.
