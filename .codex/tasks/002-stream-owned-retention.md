# Task 002: Make retention depend on stream binding

## Goal

Move the durable-retention decision from active durable consumers to configured
stream bindings.

## Dependencies

- [Task 001: Introduce the stream and partition domain model](001-stream-domain-model.md).

## Scope

- Resolve the primary stream binding after ingress validation and before durable
  append.
- Retain a matching publication even when no consumer exists or is connected.
- Stop using `has_matching_durable_consumer` as the durability boundary.
- Preserve the low-latency transient path and explicitly document whether live
  delivery occurs before or after durable commit.
- Return a durable-binding error when a producer requests durable or clustered
  durability for a subject with no stream binding. Do not silently downgrade it
  to a successful transient publication.
- Keep `_INBOX.*` request/reply traffic transient.
- Introduce transitional stream ownership metadata in persisted records if the
  partition-log format from Task 003 has not landed yet.

## Required invariants

- Consumer creation and deletion never determine whether new publications are
  retained.
- A publication is appended once to its primary stream even if multiple consumers
  match it.
- Transient-only publications do not enter durable state.
- Producer acknowledgements state whether the publication was accepted,
  durably bound, and committed at the requested level.

## Acceptance criteria

- Publishing to a configured stream before creating a consumer leaves a durable
  record owned by that stream.
- Adding or removing matching consumers does not change the append count.
- A durable QoS request without a stream binding returns a specific error.
- A transient publication without a binding continues to route live.
- Restart preserves stream-owned records without requiring consumer state.
- Behavior-level tests cover local and clustered modes.

## Verification

```bash
cargo test -p server
cargo test -p integration
cargo test --workspace
git diff --check
```
