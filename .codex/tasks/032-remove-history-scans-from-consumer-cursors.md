# Task 032: Remove history scans from consumer cursor updates

## Goal

Make retention observation, ACK advancement, and cursor initialization operate
from partition metadata and indexes instead of scanning all resident messages.

## Dependencies

- [Task 029: Maintain incremental delivery frontiers](029-maintain-incremental-delivery-frontiers.md).

## Scope

- Read retention floors and high watermarks directly from partition state.
- Advance committed offsets using the consumer frontier or a next-matching-offset
  index rather than filtering the global message map.
- Initialize earliest, latest, offset, timestamp, and committed positions from
  partition indexes with bounded work.
- Remove or isolate the full-history fallback so production delivery and ACK paths
  cannot invoke it.
- Update cursor tests for sparse subject matches and large out-of-order ACK windows.

## Required invariants

- Cursor advancement skips only records that do not match the consumer filter.
- Retention gaps remain observable and are counted exactly once.
- Timestamp starts select the same first eligible record as the current behavior.

## Acceptance criteria

- ACK and retention-observation cost does not grow with unrelated retained history.
- Creating a consumer does not rescan the complete message map per partition.
- Benchmarks cover long histories with sparse matching subjects and out-of-order ACKs.

## Verification

```bash
cargo test -p server cursor
cargo test -p server stream_retention
cargo test -p integration
cargo test --workspace
git diff --check
```
