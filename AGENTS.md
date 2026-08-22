# Agent Instructions

## Project Overview

This repository is a Rust workspace for a WAL-backed, Morrow-style broker.
Durable consumer semantics are the core behavior: durable `CONN`, `SUB`,
`PUB`, explicit Morrow ACK identities, redelivery after ack timeout, request/reply inbox
delivery, and optional clustered durability through OpenRaft.

Workspace crates:

- `crates/server`: broker runtime, WAL, TLS, auth, Raft integration, and most
  deterministic broker tests.
- `crates/protocol`: protocol parsing/encoding, subject matching, and auth
  helpers.
- `crates/client`: reusable TCP/TLS/auth client.
- `crates/cli`: command-line client.
- `crates/integration`: real cross-crate TCP/OpenRaft smoke tests.

## Command Rules

- Prefer `rg` / `rg --files` for searching.
- Use `cargo fmt` before final verification when editing Rust.
- Use `apply_patch` for manual file edits.
- Do not stage, commit, or rewrite history unless the user explicitly asks.

## Rust Source Layout

- Keep Rust source and tests in separate files. Do not add inline
  `#[cfg(test)] mod tests` blocks to production source files.
- Put unit and deterministic in-process tests in sibling or crate-local test
  modules/files, and keep production modules focused on runtime code.
- No Rust source or test file may exceed 600 lines. When a change would push a
  file over that limit, split the code or tests into smaller focused modules as
  part of the same change.
- When touching an existing Rust file that is already over 600 lines, do not add
  more code to it; first move the relevant code or tests into smaller files.

## Standard Verification

For server or broker semantics changes, run:

```bash
cargo test -p server
cargo test -p integration
cargo test --workspace
git diff --check
```

For narrower changes, at minimum run the affected package tests plus
`git diff --check`. Report any skipped verification explicitly.

## Testing Strategy

- Prefer deterministic, in-process server tests for broker semantics, but keep
  those tests in separate test files rather than inline in production modules.
- Keep full TCP/OpenRaft tests in `crates/integration` as smoke coverage, not as
  the primary place for timing-sensitive behavior.
- Deterministic broker tests should avoid real ports, wall-clock sleeps,
  timestamp-derived paths, and background timing races where possible.
- Use the existing test-private `Scenario`, `ManualClock`, `TestClient`, and
  fake cluster helpers instead of adding public testing APIs.
- Use `TempDir` for filesystem isolation in tests.
- Use manual clock advancement plus explicit redelivery ticks for redelivery
  assertions.
- Negative frame assertions may use short timeouts, but correctness should come
  from deterministic state and explicit ticks.

## Server-Crate Conventions

- Keep production behavior unchanged when adding test seams.
- Test hooks should remain crate-private or `#[cfg(test)]`.
- `Broker::open(config)` is the production entrypoint. Test-only construction
  should go through `Broker::open_with_hooks`.
- Keep TLS handling on the real accepted TCP path. Duplex tests should use the
  test-only accepted/client entrypoints.
- In clustered mode, durable mutations should go through the cluster runtime so
  local WAL and Raft/fake-cluster behavior stay aligned.
- Follower routing should use the shared proxy/routing path so deterministic
  tests and production TCP behavior exercise the same logic.

## Fake Cluster Runtime

The fake cluster in `crates/server/src/broker.rs` is test-only. Use it for
deterministic coverage of:

- quorum loss and restore;
- not-leader errors;
- leader changes;
- delayed commits via queued writes;
- follower routing and proxy behavior;
- large cluster happy paths such as 100-node scenarios.

Do not turn the fake cluster into a supported public API. It models quorum and
leader behavior for broker semantics; it is not a full Raft simulator.

## Integration Tests

- Keep `crates/integration` focused on real TCP/TLS/OpenRaft wiring.
- Avoid adding timing-sensitive assertions there if an in-process server test can
  cover the semantic behavior.
- When choosing a follower for proxy tests, wait until that follower knows the
  elected leader before connecting through it.

## Protocol Notes

- Durable clients connect with a durable identity before subscribing.
- Durable deliveries include Morrow ACK identities.
- ACKs use the explicit `ACK` command; publishing to an ACK path is invalid.
- Request/reply inbox delivery is transient and should not enter durable state.
- Followers proxy raw client TCP bytes to the known leader; TLS clients complete
  TLS with the leader.
