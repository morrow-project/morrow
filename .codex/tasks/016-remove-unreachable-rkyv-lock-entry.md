# Task 016: Remove the unreachable vulnerable rkyv lock entry

## Goal

Eliminate the stale `rkyv 0.7.46` entry reported by RUSTSEC-2026-0235 and keep
the lockfile aligned with the actual dependency graph.

## Dependencies

- None.

## Scope

- Confirm `rkyv` has no active path for any supported target or feature set.
- Regenerate or minimally update `Cargo.lock` to remove unreachable packages.
- If an active path exists, upgrade to `rkyv >= 0.8.17` and validate archives.
- Add dependency auditing to CI to prevent silent reintroduction.

## Required invariants

- Supported builds do not resolve an advisory-affected `rkyv` version.
- Lockfile cleanup does not unintentionally upgrade unrelated dependencies.

## Acceptance criteria

- `cargo tree --target all -i rkyv@0.7.46` reports no active dependency.
- `cargo audit` no longer reports RUSTSEC-2026-0235.
- Supported target checks and workspace tests pass.

## Verification

```bash
cargo tree --target all -i rkyv@0.7.46
cargo audit
cargo test --workspace
git diff --check
```
