# Task 026: Update route interests only when subscriptions change

## Goal

Stop cloning, sorting, and broadcasting the full transient-interest set on every
publication.

## Dependencies

- [Task 007: Add routing and subject indexes](007-routing-and-subject-indexes.md).

## Scope

- Trigger interest updates only on SUB, UNSUB, disconnect, peer join, and resync.
- Maintain a reference-counted canonical interest set incrementally.
- Send versioned deltas where practical and retain full snapshots for resync.
- Coalesce bursts of subscription changes without delaying correctness.
- Remove publication-path recomputation of unchanged interests.

## Required invariants

- Peers eventually observe the exact active interest set after reconnect.
- Removing one duplicate local interest does not withdraw the shared route.
- Publications do not mutate or broadcast interest state.

## Acceptance criteria

- Publishing with unchanged subscriptions sends no interest frame.
- Tests cover duplicate interests, disconnect, reconnect, and full resync.
- Publication cost is independent of subscription count for interest maintenance.

## Verification

```bash
cargo test -p server
cargo test -p integration
cargo test --workspace
git diff --check
```
