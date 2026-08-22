# Task 025: Notify pull consumers instead of polling

## Goal

Replace 10 ms FETCH retry loops with event-driven wakeups.

## Dependencies

- None.

## Scope

- Register bounded waiters by consumer or relevant stream interest.
- Wake waiters on publication, redelivery, credit changes, deletion, disconnect,
  and shutdown.
- Preserve maximum wait deadlines without periodic state locking.
- Prevent lost wakeups between an empty fetch and waiter registration.
- Limit waiters per connection and consumer.

## Required invariants

- An available message cannot remain asleep until the full FETCH deadline.
- Timeout, deletion, and disconnect remove waiter state promptly.
- Multiple waiters cannot receive the same exclusive delivery.

## Acceptance criteria

- Tests cover publish-before-register and publish-after-register races.
- Large numbers of idle FETCH requests do not cause periodic CPU wakeups.
- Pull protocol compatibility and deadline behavior remain unchanged.

## Verification

```bash
cargo test -p server pull
cargo test -p integration
cargo test --workspace
git diff --check
```
