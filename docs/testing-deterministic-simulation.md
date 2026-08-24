# Deterministic simulation testing

Issue #68 adds a deterministic test layer for Morrow's stateful subsystems.
The simulation layer exercises the same broker state-machine code as the
production runtime while replacing external timing and I/O boundaries with
controllable implementations.

## Invariants

The initial cluster and partition-replication scenarios must continuously
check these properties:

- A committed record is never lost or replaced by a different record at the
  same stream, partition, and offset.
- A leader epoch fences writes from an old leader.
- A partition commit is not reported until the configured quorum has accepted
  the record, and quorum loss rejects new writes.
- A recovered follower can catch up to the committed high watermark without
  creating duplicate committed records.
- Metadata changes are applied in log order and are idempotent when replayed.
- A node that is paused, crashed, or partitioned cannot make progress on
  behalf of an unavailable quorum.
- Every liveness scenario either reaches its expected state or produces a
  bounded, replayable trace explaining why it did not.

## Architecture

`crates/simulation` contains reusable deterministic primitives:

- `VirtualClock` advances time explicitly.
- `DeterministicScheduler` orders same-time events by insertion sequence.
- `DeterministicRng` provides seed-driven scenario choices.
- `SimulatedTransport` models delay, drop, duplication, reordering, disconnect,
  and symmetric partitions.
- `SimulatedStorage` models failed and partial writes and restart persistence.
- `EventTrace` records a seed and ordered event descriptions for replay.

These primitives do not implement broker behavior. Production state machines
remain the source of truth; test seams inject the clock, cluster runtime,
transport, and storage boundary used by a scenario.

## Replay and extension path

Every generated scenario records its seed, virtual-time transitions, fault
changes, and state-machine events. A regression test should preserve that
seed and trace as a fixed-seed scenario. New subsystems should add a small
adapter around the same primitives rather than introduce a second production
implementation.

Real-process integration tests remain necessary for sockets, TLS, filesystem
semantics, process lifecycle, and platform behavior. Simulation tests
complement those tests; they do not replace them.
