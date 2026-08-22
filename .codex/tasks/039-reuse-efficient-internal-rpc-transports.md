# Task 039: Reuse efficient internal RPC transports

## Goal

Reduce connection setup, JSON serialization, allocation, and split-write overhead
for Raft RPC and high-volume route frames.

## Dependencies

- [Task 006: Split metadata consensus from partition replication](006-split-metadata-and-data-replication.md).
- [Task 012: Secure internal cluster and admin transports](012-secure-internal-transports.md).
- [Task 027: Separate route bind and advertised addresses](027-fix-route-address-advertisement.md).

## Scope

- Measure connection, TLS, encoding, allocation, and syscall costs separately for
  Raft and route traffic.
- Reuse authenticated Raft connections with bounded request multiplexing or
  ordered per-peer connection pools.
- Evaluate a compact versioned binary encoding for internal frames while retaining
  explicit size limits and rolling-upgrade compatibility.
- Coalesce frame prefix and body writes where supported.
- Add reconnect, backpressure, timeout, and peer-identity handling for reused links.

## Required invariants

- Connection reuse cannot mix peer identities or bypass TLS and token authentication.
- Raft response correlation and ordering remain correct across reconnects.
- Nodes with adjacent supported protocol versions can form a cluster during rollout.

## Acceptance criteria

- Normal Raft traffic does not require a new TCP/TLS connection per request.
- Benchmarks report RPCs or frames per second, bytes, allocations, and p50/p95/p99
  latency before and after the transport changes.
- Cluster formation, replication, route recovery, and rolling-version tests pass.

## Verification

```bash
cargo test -p server raft
cargo test -p integration
cargo test --workspace
git diff --check
```
