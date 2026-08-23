# Dynamic consumer groups

Morrow consumer groups provide broker-managed membership and partition
ownership for pull consumers. A group has a monotonically increasing
generation. Every join, leave, partition expansion, or recovered state creates
a new generation; delivery control from an older generation is rejected by the
client's group assignment path.

## Joining and heartbeats

The group name is the pull-consumer name whose deliveries the member will
fetch. Create that durable consumer first, then join it:

```text
GROUP JOIN worker member-a 8 sticky instance-a
G-OK JOIN worker 1 0,1,2,3,4,5,6,7
```

The assignment strategies are `range`, `round_robin`, and `sticky`. Sticky
assignment keeps existing ownership where possible while balancing newly
added members. Member identifiers may rejoin; a rejoin fences the old session
and advances the generation. Heartbeats refresh leases and return the current
assignment:

```text
GROUP HEARTBEAT worker member-a 1
G-OK HEARTBEAT worker 1 0,1,2,3,4,5,6,7
```

Members that observe a newer generation must use the returned generation for
subsequent fetch and commit operations. A member that leaves, expires, or no
longer has an assignment cannot fetch through that group.

## Offsets and recovery

Offsets are committed monotonically and are fenced by both member and
generation:

```text
GROUP COMMIT worker member-a 1 3 420
G-OK COMMIT
```

Group metadata and committed offsets are written to the local WAL checkpoint
and, when clustering is enabled, to the Raft metadata state. Membership leases
remain ephemeral. After process recovery or full cluster reconciliation,
Morrow bumps the generation and requires members to rejoin, preserving the
committed offsets without allowing stale sessions to acknowledge work.

The administrative endpoints `/groups` and `/api/v1/groups` expose generation,
members, assignments, committed offsets, and cooperative-rebalance state.
Metrics include `morrow_consumer_groups`,
`morrow_consumer_group_members`, and
`morrow_consumer_group_moved_partitions`.

Group metadata is bounded to 10,000 members and 10,000 partitions per group by
default. Partition expansion is supported; partition-count reductions are
rejected because they could invalidate committed positions.
