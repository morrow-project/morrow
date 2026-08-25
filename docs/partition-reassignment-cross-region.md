# Partition reassignment and cross-region replication

Partition movement is a controller operation over the existing quorum
replication data plane. A move advances through `AddReplica`, `CatchingUp`,
`TransferLeadership`, `RemoveReplica`, and `Complete`. The controller persists
the phase, source epoch, destination, and last observed high watermark in an
atomic JSON state file. A restart therefore resumes at the last safe boundary;
it does not infer completion from an incomplete data copy.

Leadership transfer is permitted only when the destination is a configured
replica, available, caught up through the committed high watermark, and the
source quorum remains available. Every transfer increments the leader epoch,
which fences stale writers. A move can be rolled back while adding or catching
up a replica; after leadership transfer it must complete or be recovered by the
controller rather than silently reverting the leader.

Global Raft also persists each partition's replica-set generation, active commit
set, leader epoch, and reconfiguration phase. The `/cluster` administration
response exposes `replicas`, `active_commit_set`, `replica_set_generation`, and
`phase` for every partition. A candidate is activated only after its committed
offset and digest match the controller's partition commit; non-stable phases
fence foreground partition commits. Legacy snapshots normalize missing fields
to generation 1, the full replica set, and `Stable`.

Peer partition RPCs use the versioned, length-delimited binary Raft frame
codec. CBOR byte strings carry payloads and keys without JSON integer-array
expansion; the configured frame limit and read deadline bound allocation and
slowloris exposure. Unknown protocol versions are rejected before decoding,
so rolling upgrades cannot silently fall back to a weaker contract.

The placement planner orders partitions deterministically and scores brokers by
disk utilization, partition count, leader count, throughput, and node ID. It
filters draining/decommissioned brokers and applies allowed-region and replica
diversity constraints. `MoveThrottle` bounds concurrent moves and bytes per
window so movement cannot consume all foreground bandwidth.

## Regional replication

Cross-region shipping is asynchronous and never part of the local publish
acknowledgement path. Each immutable segment chunk carries stream, partition,
offset range, and SHA-256 checksum. The standby persists a checkpoint only
after checksum, contiguous-offset, and fencing-token validation. Replayed
chunks are idempotent when their checksum matches; conflicting bytes or gaps
are rejected. Checkpoint restart tests cover outage recovery and duplicate
delivery.

The primary and standby share a monotonically increasing fencing token. A
promotion increments the token; the former primary must be fenced before it
can accept writes, preventing split-brain. Operators should record recovery
point (the latest checkpoint/high-watermark difference) and recovery time in
failover drills. `ReadLocality` expresses whether reads are primary-only,
local-preferred, or local-only; an unavailable local region must not silently
turn a local-only read into a remote read.

Tenant placement, encryption, residency, and authorization checks belong at the
controller admission boundary before a move is started. Regional checkpoints
contain positions and checksums, never key material or message payloads.
