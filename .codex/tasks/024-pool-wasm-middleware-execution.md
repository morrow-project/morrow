# Task 024: Pool WebAssembly middleware execution state

## Goal

Reduce per-message Wasmtime setup and cloning while preserving isolation and
budget enforcement.

## Dependencies

- [Task 008: Add the programmable plane and connectors](008-programmable-plane-and-connectors.md).

## Scope

- Prebuild linkers and typed function handles per middleware generation.
- Evaluate safe instance or store pooling with complete reset between messages.
- Reduce payload and header cloning across matching middleware stages.
- Bound pool size and define backpressure when instances are busy.
- Preserve fuel, memory, recursion, deadline, and capability limits per call.

## Required invariants

- Guest memory and host state never leak between messages or identities.
- Hot upgrade and rollback atomically switch complete generations.
- Pool reuse cannot bypass an execution budget.

## Acceptance criteria

- Benchmarks report throughput and p50/p95/p99 overhead before and after.
- Isolation tests detect dirty memory, capability, or emitted-message state.
- Trap, deadline, recursion, and generation tests remain green.

## Verification

```bash
cargo test -p server middleware
cargo test --workspace
git diff --check
```
