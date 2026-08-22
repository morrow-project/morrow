# Task 022: Index delivery and redelivery scheduling

## Goal

Replace full message and consumer scans with indexed candidate selection and
deadline-driven redelivery.

## Dependencies

- [Task 018: Shard broker state and remove blocking I/O from the global lock](018-shard-broker-state-and-offload-io.md).
- [Task 021: Bound recovery work and resident message history](021-bounded-recovery-and-resident-history.md).

## Scope

- Maintain per-partition cursor indexes for the next deliverable record.
- Map subject interests to consumers without scanning message history.
- Schedule lease expiration in a deadline heap rather than a fixed global scan.
- Update indexes on append, acknowledgment, retention, disconnect, and deletion.
- Bound work performed by one delivery tick.

## Required invariants

- Indexed selection preserves partition order and queue-group single delivery.
- Redelivery occurs no earlier than its lease deadline.
- Retention and out-of-order acknowledgments cannot leave stale candidates.

## Acceptance criteria

- Idle CPU does not grow linearly with retained message count.
- Benchmarks cover high consumer and history cardinality.
- Existing cursor, acknowledgment, and redelivery tests pass.

## Verification

```bash
cargo test -p server
cargo test -p integration
cargo test --workspace
git diff --check
```
