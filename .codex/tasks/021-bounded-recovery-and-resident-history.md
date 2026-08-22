# Task 021: Bound recovery work and resident message history

## Goal

Avoid loading every retained payload and per-record checksum into memory during
startup and steady-state operation.

## Dependencies

- [Task 009: Enforce stream retention limits](009-enforce-stream-retention-limits.md).

## Scope

- Keep payloads in partition logs and retain bounded indexes and metadata in memory.
- Load records lazily for delivery and FETCH using sparse segment indexes.
- Bound or replace per-record checksum and active-subject collections.
- Parallelize independent partition recovery within an I/O limit.
- Expose recovery progress and memory consumption.

## Required invariants

- Lazy loading returns the same ordered records as full replay.
- Corruption and torn-tail recovery retain current semantics.
- Resident memory is bounded independently of retained payload bytes.

## Acceptance criteria

- Startup memory and time are measured across increasing histories.
- A large-history broker serves without materializing every payload.
- Delivery, restart, corruption, and retention tests pass with lazy loading.

## Verification

```bash
cargo test -p server
cargo test -p integration
cargo test --workspace
git diff --check
```
