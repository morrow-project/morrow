# Partition replication strategy

## Decision

The broker uses controller-directed leader/follower replication for partition
data. OpenRaft remains the metadata control plane; it is not the message-data
transport.

Metadata consensus contains stream definitions, partition replica assignments,
leader IDs and epochs, committed high-watermarks, consumer definitions, security
reference names, and feature gates. A partition commit contains only the stream,
partition, offset, checksum, leader, and epoch. Message subjects, headers, reply
subjects, keys, and payloads are absent from the metadata log and its JSON
snapshots.

Partition replicas use authenticated data-plane RPCs on the cluster listener:

1. A newly elected controller commits its partition-leader identity and next
   epoch through metadata consensus before accepting partition data.
2. The partition leader reads each follower's match position.
3. It sends missing committed envelopes, then the proposed envelope, directly to
   each replica in parallel.
4. A quorum append requires the configured majority to report the offset.
   Quorum-fsync additionally requires that majority to report a durable flush.
5. The leader appends locally only after enough followers respond, then sends a
   fenced partition-local commit notification to the required replicas. Each
   replica persists the committed high-watermark in its checksummed
   `commit-state.journal`; global metadata Raft is not proposed per message.
6. Producer acknowledgements report the committed partition offset and leader
   epoch.

Partition writes are authorized by the cached assignment (leader ID, leader
epoch, replica-set generation, and active commit set), not by the current
OpenRaft leader. The `partition-local-commit-v1` feature gate is required for
this path; older metadata that would need a per-record metadata-Raft commit is
rejected rather than silently coupling broker throughput to controller
leadership. Controllers are still required for bootstrap, reassignment, and
leader-epoch changes.

Simple clients may connect to any broker and continue through the shared raw TCP
leader-proxy path. TLS is still terminated by the leader. Advanced clients can
use the authenticated cluster/stream metadata endpoints to discover the current
leader address and connect directly. Initial assignments use deterministic
rendezvous ordering over `(stream, partition, broker ID)`: only the configured
replica count is selected, and adding a broker does not re-index every existing
partition. The first selected broker is the initial partition leader; later
reassignment can explicitly move replicas or leadership.

## Failure and recovery rules

- A broker accepts a partition append only while it is the current OpenRaft
  leader and its local replica contains every committed high-watermark.
  Otherwise it returns `not partition leader` or `no safe replica available`.
- A stale leader epoch cannot commit metadata. Replica suffixes above the
  committed high-watermark may be truncated and rewritten; committed offsets
  remain immutable.
- Lagging followers report their match position and receive the missing committed
  prefix before the next append.
- Partition quorum loss prevents the local high-watermark commit. Data already
  sent to replicas remains an invisible uncommitted suffix and is repaired on
  retry or leadership change; it is never exposed as committed history.
- A candidate missing the committed high-watermark is not safe and is not
  automatically promoted by the data plane. If OpenRaft elects such a node, the
  broker refuses partition writes until catch-up or operator recovery.
- Delivery attempts, ACKs, NACKs, lease extensions, and cursor movement stay out
  of metadata Raft. They are persisted in the broker WAL. Cluster node IDs occupy
  the high 16 bits of delivery IDs, preventing stale delivery identities from a
  failed node from colliding with a new leader. Failover remains at-least-once and
  may redeliver records whose local cursor checkpoint had not moved.
- Route-mesh publications remain transient and never enter partition replication.

The deterministic three-node model tests leader transfer, follower lag/catch-up,
quorum loss and restore, divergent uncommitted suffix repair, epoch fencing, and
the no-safe-replica state. Real integration coverage publishes durable data
through a follower proxy into a three-node OpenRaft cluster.

## Strategy benchmark

The repository includes an ignored release-mode microbenchmark:

```bash
cargo test -p server --release \
  benchmark_controller_directed_against_per_partition_raft_encoding \
  -- --ignored --nocapture
```

On 2026-08-21, the local release run processed 256 records with 64 KiB payloads:

| Prototype | Elapsed | Extra encoded bytes |
| --- | ---: | ---: |
| Controller-directed three-replica quorum model | 5 ms | 0 |
| Per-partition-Raft three-copy JSON encoding proxy | 145 ms | 151,164,018 |

This is a directional CPU/copy microbenchmark, not a network throughput or
tail-latency claim. It deliberately excludes sockets, fsync latency, elections,
and the OpenRaft scheduler. Its purpose is to test the architectural shape: the
existing metadata Raft representation serializes large byte arrays as JSON and
would amplify every payload through a separate consensus group.

## Rejected alternative

Per-partition Raft was not selected for this implementation. It offers a mature
consensus protocol per partition, but introduces one election/log/snapshot state
machine per partition, increases scheduler and file-descriptor cardinality, and
duplicates payload serialization already handled by the immutable partition-log
codec. The measured encoding proxy was about 29 times slower in this narrow run.

Revisit that decision if controller-directed recovery becomes the dominant
operational cost, or if a binary per-partition consensus transport demonstrates
better end-to-end throughput and tail latency at representative partition counts.
Any replacement must retain the same committed-history immutability, safe-replica,
epoch-fencing, and no-safe-replica semantics.
