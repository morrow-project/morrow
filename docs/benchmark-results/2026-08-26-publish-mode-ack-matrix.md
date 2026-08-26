# Publish mode and acknowledgement matrix: 2026-08-26

This document records local release-build publish benchmarks for the CLI
introduced by issue #222. These are development-machine comparisons, not
production capacity claims.

## Environment

- Branch: `codex/issue-222-cli-benchmarks`
- Benchmark base revision: `4ca820f` plus the correctness and fixture fixes in
  the same worktree as this report
- CLI and server version: `0.5.2`
- Machine: MacBook Pro (`MacBookPro18,3`) with Apple M1 Pro
- CPU: 8 cores (6 performance, 2 efficiency)
- Memory: 16 GB
- Operating system: macOS 26.6.2, arm64
- Transport: loopback TCP
- Binaries: Cargo release profile
- Clients: 5
- Measured duration: 60 seconds per case
- Payload: 128 generated application bytes
- Warm-up: none
- Managed stream: one partition and a fresh WAL for every case
- Standalone storage: `local`, one replica
- Cluster storage: `quorum_fsync`, three replicas, minimum two ACK replicas

Apple Silicon does not expose one fixed clock-speed value for this workload;
frequency changes dynamically with power and thermal state.

## Standalone five-client matrix

Every case was valid and reported zero errors, timeouts, reconnects, and
duplicates. The stream-bound fire-and-forget case used an offered-load cap of
150 msg/s so its post-window drain remained bounded. The unbound baseline and
all acknowledged cases were unlimited.

| Mode | ACK level | Operations | Messages/s | MiB/s | p50 | p95 | p99 |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Fire-and-forget, unbound | None | 21,181,440 | 353,024.00 | 43.094 | 0.013 ms | 0.024 ms | 0.045 ms |
| Fire-and-forget, bound stream | None | 9,005 | 150.08 | 0.018 | 0.057 ms | 0.432 ms | 1.912 ms |
| Sync | Accepted | 569,827 | 9,497.12 | 1.159 | 0.303 ms | 3.201 ms | 4.277 ms |
| Sync | Durable | 533,585 | 8,893.08 | 1.086 | 0.328 ms | 3.417 ms | 4.330 ms |
| Sync | High durability | 11,534 | 192.23 | 0.023 | 25.582 ms | 30.756 ms | 37.321 ms |
| Async, window 256 | Accepted | 719,360 | 11,989.33 | 1.464 | 106.346 ms | 113.464 ms | 114.608 ms |
| Async, window 256 | Durable | 656,640 | 10,944.00 | 1.336 | 116.166 ms | 127.021 ms | 137.150 ms |
| Async, window 256 | High durability | 12,800 | 213.33 | 0.026 | 6,499.562 ms | 6,644.225 ms | 6,653.103 ms |
| Batch, size 100 | Accepted | 702,500 | 11,708.33 | 1.429 | 43.583 ms | 61.132 ms | 112.431 ms |
| Batch, size 100 | Durable | 652,100 | 10,868.33 | 1.327 | 45.394 ms | 50.318 ms | 54.450 ms |
| Batch, size 100 | High durability | 12,000 | 200.00 | 0.024 | 2,596.023 ms | 2,674.718 ms | 2,792.567 ms |

## Three-node five-client matrix

Every case was valid and reported zero errors, timeouts, reconnects, and
duplicates after the concurrent partition-commit and fixture-readiness defects
described below were fixed. The stream-bound fire-and-forget case used an
offered-load cap of 25 msg/s. The unbound baseline and all acknowledged cases
were unlimited.

| Mode | ACK level | Operations | Messages/s | MiB/s | p50 | p95 | p99 |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Fire-and-forget, unbound | None | 21,971,280 | 366,188.00 | 44.701 | 0.013 ms | 0.023 ms | 0.045 ms |
| Fire-and-forget, bound stream | None | 1,505 | 25.08 | 0.003 | 0.015 ms | 0.046 ms | 0.070 ms |
| Sync | Accepted | 1,114 | 18.57 | 0.002 | 238.047 ms | 524.535 ms | 562.504 ms |
| Sync | Durable | 1,112 | 18.53 | 0.002 | 239.430 ms | 518.255 ms | 547.234 ms |
| Sync | High durability | 999 | 16.65 | 0.002 | 279.118 ms | 513.528 ms | 536.333 ms |
| Sync | Cluster durable | 985 | 16.42 | 0.002 | 277.738 ms | 526.764 ms | 695.147 ms |
| Async, window 256 | Accepted | 1,280 | 21.33 | 0.003 | 79,825.494 ms | 80,196.491 ms | 80,196.491 ms |
| Async, window 256 | Durable | 1,280 | 21.33 | 0.003 | 80,175.989 ms | 80,337.734 ms | 80,337.734 ms |
| Async, window 256 | High durability | 1,280 | 21.33 | 0.003 | 96,784.264 ms | 97,180.629 ms | 97,180.629 ms |
| Async, window 256 | Cluster durable | 1,280 | 21.33 | 0.003 | 98,807.923 ms | 99,208.121 ms | 99,208.121 ms |
| Batch, size 100 | Accepted | 1,500 | 25.00 | 0.003 | 33,747.459 ms | 58,766.039 ms | 58,766.039 ms |
| Batch, size 100 | Durable | 1,500 | 25.00 | 0.003 | 32,275.083 ms | 59,344.267 ms | 59,344.267 ms |
| Batch, size 100 | High durability | 1,500 | 25.00 | 0.003 | 37,535.826 ms | 65,822.770 ms | 65,822.770 ms |
| Batch, size 100 | Cluster durable | 1,500 | 25.00 | 0.003 | 37,184.724 ms | 65,462.574 ms | 65,462.574 ms |

The async rows all completed exactly one 256-message window per client, and
the batch rows completed three 100-message batches per client. Their displayed
messages/s values divide completed operations by the configured 60-second
measurement window even though the final window or batch can finish later.
They therefore describe this fixture's bounded-window result, not a stable
cluster saturation rate. The very large latency values make that distinction
visible.

## Correctness defects found during the run

The first five-client cluster attempt failed with `partition commit checksum
mismatch` and `partition commit bytes conflict`. Concurrent publishers read the
same high watermark and assigned the same next offset to different messages.
The server now serializes offset assignment, replication, and commit per
partition while allowing different partitions to progress independently.

The cluster commit monitor also reapplied a committed record after its payload
had intentionally been removed from resident metadata. Its idempotency check
compared the payload-free metadata with the full record and incorrectly
reported `committed partition delta conflicts with local state`. The check now
compares the same resident representation and still rejects genuinely changed
committed bytes.

Finally, the managed fixture used to start traffic as soon as only node 1 knew
a Raft leader. A follower could still lack the partition assignment and reject
the first data append as `fenced partition leader epoch`. The fixture now waits
until all three authenticated `/cluster` responses contain the expected
partition assignment. Its heartbeat and election intervals also leave enough
scheduling headroom for local fsync benchmarks.

Each failed case was rerun after its corresponding fix. The tables contain only
successful reruns.

## Reading the latency columns

- Fire-and-forget latency is client write latency. A final `PING`/`PONG` occurs
  after generation stops and waits for previously written frames to be handled.
- Sync latency covers one publish through its matching producer ACK.
- Async and batch record the completion time of the entire window or batch for
  every ACK in that group. These are group-completion latencies, not independent
  per-message ACK latencies.
- `durable` proves append at the documented durability boundary but does not
  request an explicit disk flush.
- `high-durability` includes the required local flush boundary.
- `cluster-durable` requires clustered mode and waits for every assigned
  replica to reach the flushed boundary.

## Reproducing the matrix

The fixture builds release binaries by default, creates fresh temporary storage
for each managed case, writes JSON and CSV results, records server logs and
effective configurations, and stops all processes afterward. Output directories
must not already exist, preventing accidental overwrite of earlier evidence.

Run the same standalone matrix:

```bash
scripts/run-publish-benchmark-matrix.sh \
  --clients 5 \
  --duration 60s \
  --fire-throughput 150 \
  --output-dir target/benchmarks/my-standalone-run
```

Run the same three-node matrix:

```bash
scripts/run-publish-benchmark-matrix.sh \
  --topology cluster \
  --clients 5 \
  --duration 60s \
  --fire-throughput 25 \
  --output-dir target/benchmarks/my-cluster-run
```

Use `--help` for payload size, subject and partition counts, aggregate
throughput, warm-up, key cardinality, async-window, batch-size, mode, and
ACK-level controls. The default payload size is 128 bytes.

### Running against an existing Morrow deployment

Use `external` topology to prevent the fixture from starting or stopping any
server. The supplied client configuration is used unchanged for TLS,
authentication, durable identity, and other connection settings. `--server`
optionally overrides only its endpoint.

```bash
scripts/run-publish-benchmark-matrix.sh \
  --topology external \
  --no-build \
  --cli-bin "$(command -v morrow-cli)" \
  --client-config ./client.json \
  --server 192.0.2.10:4222 \
  --clients 5 \
  --duration 60s \
  --modes fire-and-forget,sync,async,batch \
  --ack-levels accepted,durable,high-durability,cluster-durable \
  --output-dir ./publish-benchmark-results
```

Before running against an existing deployment:

1. Bind the target subject, `bench/publish` by default, to a durable stream.
2. Ensure the client identity may publish to that subject.
3. Include `cluster-durable` only when all assigned replicas are available.
4. Set a finite `--throughput` for fire-and-forget if the target can accept
   frames faster than it can commit them; the external topology uses the global
   throughput value and does not alter the deployment.
5. Reset storage between cases if a fresh-history comparison is required. The
   external runner deliberately does not mutate server configuration or data.
6. Keep each JSON/CSV result together with host load, storage type, topology,
   Morrow revision, and client configuration metadata.
