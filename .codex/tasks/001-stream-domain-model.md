# Task 001: Introduce the stream and partition domain model

## Goal

Add first-class stream definitions so durable capture is configured independently
from subscriptions and consumers.

## Dependencies

- None.

## Scope

- Define stable identifiers and configuration types for streams and partitions.
- Model subject bindings, partition count, partition-selection strategy, retention,
  storage mode, and replication settings.
- Treat a stream as a durable capture policy over one or more subject expressions.
- Validate publish expressions as concrete subjects and stream bindings as wildcard
  expressions using the existing subject grammar.
- Detect ambiguous overlapping primary bindings. Reject them by default; leave
  explicit mirror/fan-out capture as a future extension.
- Add administrative read paths for listing stream definitions and resolved
  bindings without changing the current publish behavior yet.
- Keep the new types independent of transport framing and consumer membership.

## Required invariants

- A subject and a stream are never represented by the same identifier or type.
- Stream definitions remain valid without any connected consumer.
- Partition count is positive and immutable within a partitioning epoch.
- Every accepted durable publication resolves to at most one primary stream.
- Request/reply inbox subjects are not captured unless an explicit future policy
  permits it.

## Acceptance criteria

- Configuration can define a stream with subject bindings, partition count,
  partitioning policy, retention limits, and storage mode.
- Invalid or ambiguous definitions fail startup with actionable errors.
- An administrative query reports configured streams and their effective subject
  bindings.
- Existing transient and durable-consumer behavior remains unchanged in this task.
- Deterministic tests cover exact, `*`, and `>` bindings, overlap rejection,
  invalid partition counts, and inbox exclusion.

## Verification

```bash
cargo test -p server
cargo test -p integration
cargo test --workspace
git diff --check
```
