# Task 005: Add pull-based durable consumption

## Goal

Make bounded pull fetches the primary durable-consumer API while retaining
credit-controlled push as an optional compatibility facade.

## Dependencies

- [Task 004: Replace message ownership with partition cursors](004-consumer-cursors.md).

## Scope

- Add explicit consumer create/delete and fetch operations to the protocol and
  client library.
- Define fetch limits for message count, total bytes, and maximum wait time.
- Return partition, offset, delivery attempt, lease deadline, and ACK identity with
  every durable delivery.
- Add explicit ACK, NACK-with-delay, and lease-extension operations rather than
  requiring all control through publish subjects.
- Enforce byte and message limits before reading or allocating response bodies.
- Implement durable push, if retained, on top of explicit byte/message credits.
- Preserve lightweight transient `SUB` delivery and request/reply behavior.
- Version the protocol so existing clients receive a clear compatibility outcome.

## Required invariants

- A fetch never returns more messages or bytes than requested.
- The broker does not accumulate an unbounded pending push queue.
- Empty ACK payloads remain valid where the compatibility ACK-subject path remains.
- A timed-out fetch does not alter a consumer cursor.
- NACK and lease extension are fenced by the active delivery identity.

## Acceptance criteria

- The Rust client can create a durable consumer and fetch a bounded batch.
- Fetch supports empty timeout responses, byte limits, message limits, ACK, NACK,
  lease extension, and redelivery.
- Slow or disconnected clients cannot make broker memory grow without bound.
- Existing transient subscriptions and inbox request/reply continue to work.
- Protocol documentation and golden frame/parser tests cover every new operation.

## Verification

```bash
cargo test -p protocol
cargo test -p client
cargo test -p server
cargo test -p integration
cargo test --workspace
git diff --check
```
