# Task 033: Encode pull deliveries once

## Goal

Enforce pull batch byte limits without serializing every durable delivery once
for sizing and again for the returned frame.

## Dependencies

- [Task 005: Add pull-based durable consumption](005-pull-fetch-protocol.md).

## Scope

- Add an exact encoded-length calculation that does not construct a frame, or
  encode each candidate once into a reusable batch buffer.
- Account exactly for dynamic numeric fields, headers, keys, reply subjects, and
  protocol framing.
- Avoid temporary header-reference vectors where a streaming encoder can borrow
  the stored headers directly.
- Preallocate the final batch frame from the validated encoded size.

## Required invariants

- `max_encoded_batch_bytes` is never exceeded.
- Message and byte limits retain their current boundary behavior.
- Rejected final candidates do not acquire leases or advance cursors.

## Acceptance criteria

- Each accepted pull delivery is encoded at most once.
- Boundary tests cover exact-fit and one-byte-over limits with maximum-width IDs.
- Allocation and CPU benchmarks cover small and large payload batches.

## Verification

```bash
cargo test -p server pull
cargo test -p client
cargo test --workspace
git diff --check
```
