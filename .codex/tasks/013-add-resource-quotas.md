# Task 013: Add connection and state resource quotas

## Goal

Bound concurrent sockets, background tasks, subscriptions, consumers, and idle
connection lifetime.

## Dependencies

- None.

## Scope

- Add global and per-identity limits for client connections, transient
  subscriptions, durable consumers, and queued outbound bytes.
- Limit concurrent HTTP, Raft, and route sockets with listener-specific semaphores.
- Add configurable idle/read deadlines after CONNECT and an HTTP header deadline.
- Define overload behavior, metrics, and protocol errors.
- Ensure disconnect cleanup releases every quota permit and state entry.

## Required invariants

- Accepted work cannot exceed configured concurrency and state ceilings.
- Slow clients cannot retain a task or outbound queue indefinitely.
- Quota rejection does not create durable consumer or subscription state.

## Acceptance criteria

- Deterministic tests exercise every limit and permit release after disconnect.
- Slowloris-style tests close idle sockets within the configured deadline.
- Load tests demonstrate bounded task and memory counts under connection floods.
- Admin metrics expose usage, limits, and rejections.

## Verification

```bash
cargo test -p server
cargo test -p integration
cargo test --workspace
git diff --check
```
