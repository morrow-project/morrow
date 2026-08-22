# Task 028: Remove blocking work from the broker state lock

## Goal

Keep synchronous storage, WAL, and WebAssembly work out of the broker-wide
`inner` mutex critical section used by push delivery and pull fetch.

## Dependencies

- [Task 018: Shard broker state and remove blocking I/O from the global lock](018-shard-broker-state-and-offload-io.md).

## Scope

- Split push delivery and pull fetch into short state-reservation and
  state-commit phases around asynchronous or blocking work.
- Do not hold `inner` while reading partition records, executing middleware,
  waiting for WAL responses, or encoding delivery frames.
- Detect and safely retry or cancel reservations invalidated by ACK, retention,
  disconnect, consumer deletion, or cluster reconciliation.
- Bound concurrent delivery preparation so removing the lock does not create
  unbounded storage or middleware work.

## Required invariants

- A message cannot be delivered twice because two workers reserve it concurrently.
- Consumer credit, lease identity, ordering, and maximum in-flight limits remain
  atomic from the protocol's perspective.
- Failed preparation releases reservations without losing a deliverable record.

## Acceptance criteria

- Instrumented tests prove no partition read, WAL request, middleware execution,
  or frame encoding runs while `inner` is held.
- A deliberately slow storage read or middleware call does not stall unrelated
  publish, ACK, transient delivery, or admin-state access.
- Existing push, pull, redelivery, retention, and cluster tests remain green.

## Verification

```bash
cargo test -p server
cargo test -p integration
cargo test --workspace
git diff --check
```
