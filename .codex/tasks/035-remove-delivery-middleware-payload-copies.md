# Task 035: Remove delivery middleware payload copies

## Goal

Avoid cloning message payloads and unchanged metadata before and after
`BeforeDeliver` middleware execution.

## Dependencies

- [Task 024: Pool WebAssembly middleware execution state](024-pool-wasm-middleware-execution.md).
- [Task 028: Remove blocking work from the broker state lock](028-remove-blocking-work-from-broker-state-lock.md).

## Scope

- Represent middleware input as borrowed or shared immutable message fields.
- Allocate owned replacements only for fields actually changed by middleware.
- Pass unchanged records directly to frame encoding without cloning the full
  `PublishRecord`.
- Preserve host allocation accounting when guest code reads or replaces fields.
- Cover middleware chains where one stage mutates and a later stage only reads.

## Required invariants

- Middleware cannot mutate persisted records or observe another execution's state.
- Allocation, fuel, capability, deadline, and emitted-message limits remain enforced.
- Dropped or rejected deliveries retain their existing ACK and retry semantics.

## Acceptance criteria

- A no-op `BeforeDeliver` pipeline performs no payload-sized host clone.
- A header-only mutation does not copy the payload.
- Benchmarks report bytes allocated per delivery for no-op and mutating middleware.

## Verification

```bash
cargo test -p server middleware
cargo test -p server delivery_index
cargo test --workspace
git diff --check
```
