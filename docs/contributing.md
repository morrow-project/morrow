# Contributing to Morrow

## Before making a change

Read the repository instructions in [`AGENTS.md`](../AGENTS.md). Keep protocol,
storage, routing, and deployment changes explicit because they can affect
interoperability or persisted state.

## Code organization

- Keep reusable protocol and client behavior in their respective crates.
- Keep server semantics tests deterministic and in `crates/server`.
- Keep real TCP/TLS/OpenRaft smoke tests in `crates/integration`.
- Keep Rust production and test code in separate files.
- Do not grow a Rust source or test file beyond 600 lines; split it first.
- Use the existing `Scenario`, `ManualClock`, `TestClient`, and fake-cluster
  helpers for deterministic server tests.

## Test expectations

For server or broker behavior changes, run:

```bash
cargo fmt --all
cargo test -p server --locked
cargo test -p integration --locked
cargo test --workspace --locked
git diff --check
```

For protocol changes, add coverage for both encoding and rejection behavior.
For durable delivery changes, prefer manual-clock advancement and explicit
redelivery ticks over wall-clock sleeps.

## Documentation

Update the relevant crate README and the protocol or operations guide when a
public command, configuration field, deployment path, or runtime behavior
changes. Keep the top-level README focused on installation and first use.

## Commits

Whenever the agent modifies repository files, it must verify, stage the intended
changes, and create a commit before ending the turn. Do not leave agent changes
uncommitted or rewrite history.

Human contributors should use concise imperative commit subjects and include
the relevant tests in the commit description when a change is non-obvious.
