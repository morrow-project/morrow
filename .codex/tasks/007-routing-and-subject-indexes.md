# Task 007: Add trie routing and sealed-segment subject indexes

## Goal

Optimize high-cardinality subject routing and selective durable reads after the
stream, partition, and consumer semantics are stable.

## Dependencies

- [Task 001: Introduce the stream and partition domain model](001-stream-domain-model.md).
- [Task 003: Build immutable partition logs and message envelopes](003-partition-log-envelope.md).
- [Task 004: Replace message ownership with partition cursors](004-consumer-cursors.md).
- [Task 006: Split metadata consensus from partition replication](006-split-metadata-and-data-replication.md).

## Scope

- Replace full scans of transient subscriptions, consumer filters, stream
  bindings, and middleware matches with a compiled trie/radix-style interest
  structure.
- Preserve exact NATS-style `*` and terminal `>` semantics.
- Integrate local and remote route interests without creating a durable object for
  every concrete subject.
- Add an optional sealed-segment subject index containing a concrete-subject
  dictionary and postings lists of record offsets.
- Resolve wildcard filters against the dictionary once per segment and merge
  postings for selective reads.
- Bound subject-index memory and cardinality; fall back to a correct sequential
  scan when the budget is exceeded.
- Make indexes rebuildable optimizations rather than sources of truth.

## Required invariants

- Indexed and scan-based matching return identical ordered results.
- Routing changes do not alter transient fan-out or queue/group semantics.
- Index loss or corruption never makes committed records inaccessible.
- High subject cardinality cannot cause unbounded index memory use.

## Acceptance criteria

- Model/property tests compare trie matching with the existing reference matcher.
- Durable filtering tests compare subject-index results with full partition scans.
- Missing and deliberately corrupted indexes rebuild or fall back safely.
- Benchmarks cover exact, `*`, and `>` matching across representative subject and
  subscription cardinalities.
- The benchmark record justifies keeping, revising, or removing the sealed-segment
  subject index.

## Verification

```bash
cargo test -p protocol
cargo test -p server
cargo test -p integration
cargo test --workspace
git diff --check
```
