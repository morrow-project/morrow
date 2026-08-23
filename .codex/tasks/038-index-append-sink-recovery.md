# Task 038: Index append-sink recovery

## Goal

Avoid rereading and JSON-decoding the complete append-database sink history on
every connector startup.

## Dependencies

- [Task 008: Add the programmable plane and connectors](008-programmable-plane-and-connectors.md).

## Scope

- Persist a compact idempotency or high-watermark index by stream partition.
- Recover incrementally from the indexed boundary and validate a torn or stale index.
- Compact or checkpoint the append log without losing retry detection.
- Bound resident idempotency state independently of total historical records where
  partition ordering makes that safe.
- Migrate existing append logs without requiring data loss or manual conversion.

## Required invariants

- Restart never admits a duplicate record that the current full-history scan rejects.
- Torn index updates recover from the append log deterministically.
- Out-of-order offsets retain exact idempotency semantics.

## Acceptance criteria

- Warm startup work is proportional to data written since the latest valid checkpoint.
- Resident recovery state is reported and bounded for ordered partitions.
- Tests cover legacy logs, corrupt indexes, torn tails, duplicates, and gaps.

## Verification

```bash
cargo test -p connector
cargo test --workspace
git diff --check
```
