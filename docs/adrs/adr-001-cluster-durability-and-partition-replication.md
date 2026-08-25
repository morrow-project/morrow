# ADR-001: Cluster durability and partition replication

- Status: Accepted
- Accepted: 2026-08-26
- Date: 2026-08-26
- Decision owners: Morrow maintainers
- Related issues: #134, #135, #136, #142, #143, #144, #145, #162, #163
- Refines: `docs/partition-replication-strategy.md`

## Context

Morrow currently separates message data from metadata consensus. Partition
payloads are copied through authenticated data-plane RPCs, while OpenRaft owns
cluster metadata and records a payload-free partition high-watermark after each
publish.

That separation avoids putting message payloads and high-cardinality partition
logs into the global metadata state machine. The current publish protocol still
has several costs that are incompatible with the intended broker workload:

- normal replication can load and clone committed partition history before
  determining the suffix a follower needs;
- partition data-plane calls create short-lived authenticated connections;
- client completion waits for every follower, including replicas outside the
  durability quorum;
- each message causes a global Raft proposal for its committed high-watermark;
- applying that proposal clones unrelated durable metadata and persists it;
- synchronous partition and WAL operations can block asynchronous runtime
  workers; and
- sequential publishers pay storage barriers individually instead of joining a
  bounded group commit.

A three-node release benchmark of 250 sequential small durable publishes
measured approximately 19.4 messages per second, with p50 50.4 ms, p95 72.5 ms,
and p99 82.5 ms. This run is a latency baseline rather than a capacity result,
but it confirms that the clustered commit path has a high fixed cost before
history, concurrency, and follower lag are increased.

The architecture needs a precise answer to four questions before the related
performance issues are implemented:

1. Which system owns message ordering and commit authority?
2. Which state belongs in global Raft and which state is partition-local?
3. What does each producer acknowledgement prove?
4. How does a new leader preserve every acknowledged record without a
   per-message global metadata commit?

## Decision

Morrow will use global OpenRaft as the cluster control plane and a separate,
leader-based replication protocol for partition data. Global Raft will not
participate in the normal per-message commit path.

Each partition will have one metadata-assigned leader, a replica set, and a
stable active commit set. The active commit set is a controller-selected subset
of the replica set whose size satisfies the configured write quorum. Every
member of the active commit set must contain every acknowledged record. Other
replicas are observers that catch up asynchronously and do not delay normal
quorum acknowledgements.

This is not an arbitrary fastest-majority protocol. The active commit set is
stable across publishes and changes only through a fenced control-plane
transition. That constraint makes any healthy active member eligible to recover
the acknowledged prefix without requiring a global Raft write for every
message.

### Control-plane responsibilities

Global OpenRaft owns only low-frequency cluster state:

- cluster membership and node identities;
- stream and partition definitions;
- replica assignments;
- the active commit set for each partition;
- partition leader identities and monotonically increasing leader epochs;
- replica-set and commit-set reconfiguration phases;
- feature gates, security references, and protocol compatibility; and
- durable recovery or operator decisions such as `no safe replica`.

Global OpenRaft does not store message envelopes, per-message checksums,
per-message offsets, follower progress, or a partition high-watermark entry for
every publish. It may store a periodically checkpointed diagnostic watermark,
but that checkpoint is not the authority used to acknowledge or expose a
record.

### Partition ownership and record identity

One ordered worker or actor owns each active partition. It assigns offsets,
batches appends, coordinates replication, and advances the partition-local
committed high-watermark. Different partitions can progress independently.

Every replicated batch carries at least:

- stream and partition identity;
- replica-set generation;
- leader identity and leader epoch;
- first and last offset;
- the previous contiguous offset and digest;
- a deterministic batch digest;
- producer idempotency identities where supplied; and
- a versioned binary sequence of message envelopes.

Followers reject stale epochs, unexpected predecessors, conflicting bytes at an
existing offset, gaps, and unknown incompatible versions. Replaying an
identical batch is idempotent.

### Steady-state publish protocol

For each bounded batch, the partition leader:

1. validates the request, reserves bounded queue capacity, assigns offsets, and
   appends the batch to its local partition worker;
2. sends the batch over persistent authenticated sessions to active commit-set
   members and, independently, to observer replicas;
3. waits for every active commit-set member to reach the durability boundary
   required by the requested acknowledgement level;
4. advances the partition-local committed high-watermark through the highest
   contiguous batch that meets that boundary;
5. makes that committed prefix eligible for delivery and retention accounting;
   and
6. completes all producer acknowledgements covered by the batch.

Observer failure or latency does not delay the normal quorum boundary. Observer
replication continues through supervised, bounded queues. A slow or failed
active member requires an explicit commit-set reconfiguration; the leader does
not silently choose a different quorum independently for each publish.

The leader propagates its committed high-watermark on subsequent append and
heartbeat frames. An idle partition sends a bounded commit notification so
followers do not require another publish to learn the watermark. High-watermark
propagation is partition-local and batched; it is not a global Raft proposal.

### Producer acknowledgement contract

Acknowledgement levels are explicit storage boundaries rather than synonyms for
socket completion:

| Level | Standalone boundary | Clustered boundary |
| --- | --- | --- |
| `ACCEPTED` | Validation and bounded admission completed; loss is permitted | Validation and bounded leader admission completed; loss is permitted |
| `DURABLE` | Partition and required local metadata appended | Every active commit-set member appended the batch; an operating-system flush is not implied |
| `HIGH_DURABILITY` | Partition and required local metadata flushed through group commit | Every active commit-set member flushed the batch through group commit |
| `CLUSTER_DURABLE` | Rejected | Every currently assigned replica flushed the batch; unavailable or lagging observers make this explicit stronger level wait or fail |

All non-accepted acknowledgements include the stream, partition, committed
offset, leader epoch, and effective acknowledgement level. The server never
reports a stronger level than it actually completed.

Changing the meaning of an existing acknowledgement level requires the normal
protocol-evolution process, capability negotiation, SDK tests, and release
notes. Until that migration is implemented, existing behavior remains the
shipped contract; this table defines the target contract.

Timeout or connection loss before an acknowledgement is an uncertain result.
The record may later become committed. Producers use immutable message IDs and
idempotent retries rather than assuming that an unacknowledged record is absent.

### Durability and commit-state persistence

Partition data and compact partition-local state are persisted separately from
global metadata Raft. Partition-local state contains the replica-set generation,
leader epoch, committed high-watermark, high-watermark digest, and the minimum
information needed to validate replay.

Append and high-watermark records use a versioned, checksummed journal with
periodic atomic checkpoints. Group commit assigns appends to bounded epochs and
performs the required storage barriers once per epoch. Acknowledgements are
released only after the corresponding epoch reaches its documented durability
boundary.

The implementation must preserve these invariants:

- an acknowledged offset is never replaced by different bytes;
- committed offsets form a contiguous prefix;
- every active commit-set member contains the acknowledged prefix;
- a stale leader epoch cannot append, commit, truncate, or acknowledge data;
- observers never become leaders until they are caught up and admitted to the
  active commit set; and
- cancellation cannot discard an append already accepted by a storage worker,
  even when the client no longer waits for it.

### Leader election and recovery

The control plane grants a new leader epoch before the candidate accepts
publishes. A candidate is eligible only if it was an active commit-set member
for the previous generation or has completed a safe catch-up and
reconfiguration.

During recovery, the candidate compares offset, epoch, and digest manifests
with the surviving active members. Because all active members were required to
store every acknowledged record, their longest common valid prefix contains the
acknowledged prefix. A common suffix for which the client did not receive an
acknowledgement may also be retained and committed; this is permitted by the
uncertain-result contract. Conflicting or incomplete suffixes above the safe
prefix are truncated before new writes begin.

If the surviving replicas cannot prove a common prefix covering the last
locally committed watermark, the partition enters `no safe replica` and refuses
writes and committed reads. It does not trade acknowledged-message safety for
availability.

### Commit-set reconfiguration

Changing an active commit set is a joint, fenced transition:

1. Global Raft records the intended new member and transition generation.
2. The prospective member catches up through the current committed
   high-watermark and validates its digest.
3. Foreground writes either pause at a bounded fence or replicate to the union
   of the old and new commit sets.
4. Global Raft activates the new commit set and increments the partition epoch.
5. The removed member becomes an observer or is removed from the replica set.

The new member cannot acknowledge foreground data or become leader before step
4. The old set cannot continue acknowledging after observing the new epoch.
Interrupted transitions resume or roll back from their persisted phase without
inventing membership from local files.

### Catch-up and snapshots

The leader maintains bounded in-memory progress for each peer and checkpoints
progress only for diagnostics and restart acceleration. Progress is queried
before records are read from disk.

- A caught-up peer receives only the new batch.
- A peer with bounded lag receives indexed, bounded offset ranges.
- A peer behind retention or beyond a configured catch-up threshold receives
  sealed segments or a partition snapshot plus subsequent deltas.
- Catch-up never materializes or clones complete retained history in leader
  memory.

Each segment or snapshot transfer includes its partition identity, offset
range, generation, digest, and size. Installation is atomic, restart-safe, and
fenced by leader epoch.

### Transport, backpressure, and scheduling

Each peer uses a persistent authenticated session with reconnect coalescing,
bounded per-peer queues, protocol-version negotiation, and credential or
certificate rotation. Partition append, progress, heartbeat, and snapshot
frames use a versioned binary codec. Message payloads are represented as byte
strings and are not JSON integer arrays.

Partition workers perform blocking filesystem operations on dedicated ordered
storage workers, not asynchronous network executors. Queue capacity is enforced
in bytes and records. Saturation applies explicit backpressure or a documented
overload error; work is never accumulated without a bound.

One slow observer cannot block healthy peers. A slow active member can delay the
configured quorum and trigger controller reconfiguration, but reconfiguration
is rate-limited to prevent transient latency from causing membership churn.

### Observability

The implementation exposes bounded-cardinality metrics for:

- active commit-set generation and size;
- leader epoch and leadership changes;
- per-peer match, durable, and learned committed offsets;
- replication and fsync batch sizes;
- queue depth and queued bytes;
- quorum wait, storage service, and acknowledgement latency;
- reconnects and authentication failures;
- delta and segment/snapshot catch-up bytes and duration;
- observer lag and commit-set reconfiguration; and
- `no safe replica`, fencing, checksum, and conflicting-suffix failures.

Metrics and administrative reads consume immutable snapshots and do not acquire
partition storage locks or clone retained history.

## Safety argument

The design relies on a stable active commit set rather than choosing an
arbitrary fastest majority for every publish.

For a producer acknowledgement, every active member has stored the same record
at the same offset and epoch. Global Raft serializes changes to that set and
leader epoch. A replacement leader must come from the active set, or a new
member must first copy and validate the committed prefix before a joint
transition admits it. Therefore leadership cannot move to a replica that is
known to omit an acknowledged record.

This preserves acknowledged records without placing a high-watermark mutation
for each message in global Raft. The availability cost is deliberate: if the
remaining replicas cannot prove the safe prefix or complete a fenced
reconfiguration, the partition refuses service.

## Consequences

### Positive

- Normal publish work is bounded by the current batch and replication factor,
  not retained history or total durable metadata.
- Global metadata consensus is removed from the per-message critical path.
- Persistent sessions remove repeated connection and authentication setup.
- Normal quorum latency does not follow non-commit observer latency.
- Partition ordering, batching, and storage backpressure have one explicit
  owner.
- Group commit amortizes storage barriers without weakening acknowledgement
  semantics.
- Safe recovery and reconfiguration rules are testable as state-machine
  invariants.

### Negative

- Morrow owns a partition replication state machine in addition to global
  OpenRaft.
- A slow active member blocks normal durable acknowledgement until it recovers
  or the controller safely changes the commit set.
- Reconfiguration requires joint fencing and substantially more deterministic
  failure testing.
- Partition-local high-watermark journals and checkpoints add a storage format
  that must support migration and repair.
- The target acknowledgement table requires a versioned protocol transition
  from the current pre-1.0 behavior.
- `CLUSTER_DURABLE` intentionally follows the slowest assigned replica and is
  unsuitable as the default throughput mode.

## Rejected alternatives

### Put every message in global OpenRaft

This gives one consensus implementation but couples global metadata progress to
message volume, serializes unrelated partitions, and amplifies payload encoding,
logs, and snapshots. It is rejected for the broker data plane.

### Commit every partition high-watermark through global OpenRaft

This is the current architectural bottleneck. Batching those commands is a
useful migration technique but preserves a global consensus dependency in the
steady-state publish path. It is rejected as the target design.

### Choose any fastest majority independently for every batch

This improves best-case latency but leaves future leader selection without a
stable set known to contain every acknowledged record. Recreating the necessary
election and quorum-history proof would amount to an implicit consensus
protocol. It is rejected unless a complete safety design replaces the active
commit-set model.

### Wait for every assigned replica at every durability level

This makes a slow observer part of the normal tail-latency boundary and reduces
availability unnecessarily. Waiting for all assigned replicas remains available
only through the explicit `CLUSTER_DURABLE` level.

### Run an independent OpenRaft group per partition

Per-partition Raft provides mature election and log-safety rules, but creates
one consensus group, log, snapshot lifecycle, and scheduler footprint per
partition. It also requires a binary payload transport and evidence that group
cardinality remains operationally bounded. It is not selected now. Revisit it
if the custom partition protocol cannot satisfy deterministic safety tests or
representative scale benchmarks.

## Implementation gates and sequencing

This ADR is a design gate for issues #134, #135, #136, #144, and #145. Those
issues must be revised against the accepted decision before implementation.

Recommended order:

1. Correct benchmark acknowledgement semantics in #162 and capture the current
   baseline from the initial portion of #163.
2. Review and accept, amend, or reject this ADR.
3. Specify versioned partition frames, replica manifests, active commit-set
   transitions, and the acknowledgement migration.
4. Add deterministic model tests for the invariants and failures below.
5. Implement persistent peer sessions and bounded partition workers.
6. Implement delta replication, partition-local high-watermarks, group commit,
   and safe leader recovery.
7. Remove per-message partition commits from global Raft after migration and
   rollback tests pass.
8. Complete the end-to-end concurrency, history, lag, recovery, and overload
   benchmark matrix in #163.

Independent standalone bounded-work improvements may proceed alongside the
clustered implementation, provided they do not cement the superseded commit
path.

## Required implementation evidence

Acceptance of this architectural decision does not waive implementation and
release verification. The design may ship only when the protocol and test plan
cover:

- leader crash before append, during replication, after quorum, and before
  acknowledgement;
- acknowledgement loss followed by an idempotent producer retry;
- stale leader writes after a new epoch is committed;
- active member loss, observer loss, quorum loss, and quorum restoration;
- interrupted commit-set reconfiguration at every persisted phase;
- divergent uncommitted suffixes and checksum disagreement;
- follower restart, bounded delta catch-up, retention gaps, and snapshot
  installation;
- slow disk without stalled Raft heartbeats or unrelated partitions;
- rolling upgrade with mixed compatible frame versions;
- zero acknowledged-message loss across deterministic fault campaigns; and
- throughput and p95 remaining approximately flat as retained history grows
  from 1,000 to 100,000 records under otherwise identical conditions.

The performance suite must separately report `ACCEPTED`, `DURABLE`,
`HIGH_DURABILITY`, and `CLUSTER_DURABLE`, along with concurrency, batch size,
payload size, topology, storage configuration, follower lag, and commit-set
generation.
