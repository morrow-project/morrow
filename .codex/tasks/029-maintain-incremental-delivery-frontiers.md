# Task 029: Maintain incremental delivery frontiers

## Goal

Select the next durable record without rebuilding leased sets or querying and
merging complete partition subject-offset lists for every delivery.

## Dependencies

- [Task 022: Index delivery and redelivery scheduling](022-index-delivery-and-redelivery.md).
- [Task 028: Remove blocking work from the broker state lock](028-remove-blocking-work-from-broker-state-lock.md).

## Scope

- Maintain a per-consumer frontier for each matching stream partition.
- Advance frontiers incrementally on delivery, ACK, NACK, lease expiration,
  retention, compaction, and consumer reconfiguration.
- Select the globally next eligible partition record from bounded frontier state.
- Reuse in-flight membership directly instead of collecting a new `HashSet` for
  each candidate lookup.
- Rebuild frontier state deterministically during recovery and reconciliation.

## Required invariants

- Selection preserves partition order and the configured cross-partition order.
- Out-of-order ACKs and redelivery cannot skip an eligible record.
- Retention and compaction cannot leave a frontier pointing at removed data.

## Acceptance criteria

- Candidate-selection work is independent of retained history and sealed-segment
  count after frontier initialization.
- A benchmark covers at least 100 consumers, 100 partitions, and a large retained
  history, reporting p50/p95/p99 selection cost.
- Existing cursor, queue-group, push, pull, and redelivery tests pass.

## Verification

```bash
cargo test -p server delivery_index
cargo test -p server cursor
cargo test -p integration
cargo test --workspace
git diff --check
```
