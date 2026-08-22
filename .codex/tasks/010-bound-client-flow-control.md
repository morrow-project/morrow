# Task 010: Bound client flow-control values

## Goal

Prevent client-selected CONNECT, FETCH, and acknowledgment settings from causing
unbounded allocation or durable-state growth.

## Dependencies

- None.

## Scope

- Add server-configured maxima for `max_in_flight`, `ack_timeout_ms`, FETCH
  messages, FETCH bytes, and encoded batch bytes.
- Validate CONNECT and FETCH values before allocating state or multiplying sizes.
- Build FETCH responses incrementally or reject batches that cannot fit a bounded
  response buffer.
- Bound cursor acknowledgment windows and in-flight lease state independently of
  platform `usize` width.
- Return stable protocol errors that identify the exceeded limit.

## Required invariants

- No client-supplied integer directly determines an unbounded allocation.
- Size calculations use checked arithmetic and server-owned caps.
- Existing defaults remain valid and interoperable.

## Acceptance criteria

- Boundary and overflow tests cover CONNECT and FETCH on supported protocols.
- Oversized requests fail without materially increasing process memory.
- Valid maximum-sized batches still encode and deliver correctly.
- Limits are documented and exposed in server configuration.

## Verification

```bash
cargo test -p protocol
cargo test -p server
cargo test -p integration
cargo test --workspace
git diff --check
```
