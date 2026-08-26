# Publish mode and acknowledgement matrix: 2026-08-26

This document records local release-build publish benchmarks for the CLI
introduced by issue #222. These are development-machine comparisons, not
production capacity claims.

## Environment

- Revision: `9d348eb`
- CLI and server version: `0.5.2`
- Machine: MacBook Pro with Apple M1 Pro
- CPU: 8 cores (6 performance, 2 efficiency)
- Memory: 16 GB
- Operating system: macOS 26.6.2, arm64
- Transport: loopback TCP
- Payload: 1,024 generated application bytes
- Target rate: unlimited
- Warm-up: none
- Managed stream: one partition, fresh WAL for every case
- Standalone stream storage: `local`, one replica
- Cluster stream storage: `quorum_fsync`, three replicas, minimum two ACK
  replicas

Apple Silicon does not expose one fixed clock-speed value for this workload;
frequency changes dynamically with power and thermal state.

## Initial unbound 60-second result

The first run used the default standalone server with no stream bound to
`bench/publish`. It therefore measured transient protocol ingestion rather than
durable storage.

```text
clients=5 duration=60s payload=1024 mode=fire-and-forget
operations=20,861,371 throughput=347,689.52 msg/s payload=339.54 MiB/s
p50=12 us p95=30 us p99=60 us p99.9=198 us max=958 us
errors=0 timeouts=0 reconnects=0
```

Command:

```bash
target/release/morrow-cli \
  --config /tmp/morrow-bench-client.json \
  bench pub bench/publish \
  --clients 5 \
  --duration 60s \
  --mode fire-and-forget \
  --payload-size 1024 \
  --json
```

## Standalone five-client matrix

The fixture ran each case for 10 seconds against a newly started server and a
fresh WAL. Every row was valid, reported the requested acknowledgement level,
and had zero errors, timeouts, or reconnects.

| Mode | ACK level | Messages/s | MiB/s | p50 | p95 | p99 |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Fire-and-forget, unbound | None | 327,242.4 | 319.573 | 0.007 ms | 0.019 ms | 0.049 ms |
| Fire-and-forget, bound stream | None | 541.6 | 0.529 | 0.010 ms | 0.025 ms | 0.062 ms |
| Sync | Accepted | 8,452.3 | 8.254 | 0.341 ms | 4.072 ms | 4.384 ms |
| Sync | Durable | 7,789.3 | 7.607 | 0.363 ms | 4.151 ms | 4.430 ms |
| Sync | High durability | 222.5 | 0.217 | 22.082 ms | 25.882 ms | 31.452 ms |
| Async, window 256 | Accepted | 10,496.0 | 10.250 | 121.908 ms | 134.187 ms | 143.323 ms |
| Async, window 256 | Durable | 9,472.0 | 9.250 | 135.336 ms | 145.010 ms | 153.050 ms |
| Async, window 256 | High durability | 256.0 | 0.250 | 5,879.342 ms | 5,981.862 ms | 5,981.862 ms |
| Batch, size 100 | Accepted | 10,100.0 | 9.863 | 46.726 ms | 55.277 ms | 61.454 ms |
| Batch, size 100 | Durable | 9,300.0 | 9.082 | 52.905 ms | 60.714 ms | 67.252 ms |
| Batch, size 100 | High durability | 250.0 | 0.244 | 2,379.590 ms | 2,462.237 ms | 2,463.087 ms |

The bound fire-and-forget case is intentionally distinct from the unbound
baseline. It does not wait for a producer ACK, but the server still performs
the configured stream work and TCP backpressure eventually reaches the client.

## Three-node five-client matrix

The same 10-second matrix was attempted on a newly created three-node cluster
for every case. The unbound fire-and-forget baseline completed successfully:

```text
operations=3,103,406 throughput=310,340.6 msg/s payload=303.067 MiB/s
p50=7 us p95=19 us p99=48 us errors=0
```

Every stream-bound combination failed before producing a valid measurement.
The observed errors were:

| Mode | Accepted | Durable | High durability | Cluster durable |
| --- | --- | --- | --- | --- |
| Sync | Commit checksum mismatch | Commit checksum mismatch | Commit bytes conflict | Commit checksum mismatch |
| Async | Commit checksum mismatch | Commit bytes conflict | Commit checksum mismatch | Commit checksum mismatch |
| Batch | Commit checksum mismatch | Commit checksum mismatch | Commit checksum mismatch | Commit checksum mismatch |

Bound fire-and-forget failed with `partition commit bytes conflict`.

This is a correctness result, not a low-throughput result. A failed case must
not be converted into a messages-per-second number. Because each case had a
fresh cluster and the one-client control below succeeded, the evidence points
to concurrent publishers racing partition commit assignment or application.

## Three-node one-client control matrix

To isolate concurrent connections from the basic cluster fixture, the full
matrix was repeated with one client for five seconds. Every combination was
valid with zero reported errors, including all-replica `cluster-durable` ACKs.

| Mode | ACK level | Messages/s | MiB/s | p50 | p95 | p99 |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Fire-and-forget, unbound | None | 252,449.0 | 246.532 | 0.002 ms | 0.011 ms | 0.028 ms |
| Fire-and-forget, bound stream | None | 158.2 | 0.154 | 0.003 ms | 0.010 ms | 0.037 ms |
| Sync | Accepted | 43.6 | 0.043 | 22.084 ms | 31.419 ms | 64.197 ms |
| Sync | Durable | 44.0 | 0.043 | 21.942 ms | 30.750 ms | 58.984 ms |
| Sync | High durability | 32.2 | 0.031 | 30.039 ms | 39.961 ms | 64.829 ms |
| Sync | Cluster durable | 31.6 | 0.031 | 31.082 ms | 41.042 ms | 52.914 ms |
| Async, window 256 | Accepted | 51.2 | 0.050 | 6,039.062 ms | 6,039.062 ms | 6,039.062 ms |
| Async, window 256 | Durable | 51.2 | 0.050 | 6,118.423 ms | 6,118.423 ms | 6,118.423 ms |
| Async, window 256 | High durability | 51.2 | 0.050 | 8,994.295 ms | 8,994.295 ms | 8,994.295 ms |
| Async, window 256 | Cluster durable | 51.2 | 0.050 | 9,014.042 ms | 9,014.042 ms | 9,014.042 ms |
| Batch, size 100 | Accepted | 60.0 | 0.059 | 2,592.489 ms | 3,251.063 ms | 3,251.063 ms |
| Batch, size 100 | Durable | 60.0 | 0.059 | 2,511.014 ms | 3,264.661 ms | 3,264.661 ms |
| Batch, size 100 | High durability | 40.0 | 0.039 | 3,613.928 ms | 3,613.928 ms | 3,613.928 ms |
| Batch, size 100 | Cluster durable | 40.0 | 0.039 | 3,530.099 ms | 3,530.099 ms | 3,530.099 ms |

## Reading the latency columns

- Fire-and-forget latency is client write latency. Its final `PING`/`PONG`
  flush occurs after the measured window, so the command can take longer than
  the recorded duration while queued work drains.
- Sync latency covers one publish through its matching producer ACK.
- Async and batch currently record the completion time of the entire window or
  batch for every ACK in that group. Those percentiles are window-completion
  latency, not an independently timestamped per-message ACK latency.
- `durable` proves append at the documented boundary but not an explicit disk
  flush. `high-durability` includes the required flush boundary.
- `cluster-durable` requires clustered mode and waits for every assigned
  replica to reach the flushed boundary.

## Reproducing the matrix

The repository fixture builds release binaries by default, creates fresh
temporary storage for each managed case, writes JSON and CSV results, records
server logs and effective configurations, and stops all processes afterward.

Run the complete standalone matrix:

```bash
scripts/run-publish-benchmark-matrix.sh \
  --clients 5 \
  --duration 10s \
  --output-dir target/benchmarks/my-standalone-run
```

Run the three-node matrix, continuing after failed combinations:

```bash
scripts/run-publish-benchmark-matrix.sh \
  --topology cluster \
  --clients 1 \
  --duration 10s \
  --keep-going \
  --output-dir target/benchmarks/my-cluster-run
```

Use `--help` for payload size, subject and partition counts, throughput,
warm-up, key cardinality, async-window, batch-size, mode, and ACK-level
controls. Output directories must not already exist, preventing accidental
overwrites of earlier evidence.

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
3. Include `cluster-durable` only when the target is a cluster with the
   required assigned replicas available.
4. Reset storage between cases if a fresh-history comparison is required. The
   external runner deliberately does not mutate server configuration or data.
5. Keep each JSON/CSV result together with host load, storage type, topology,
   Morrow revision, and client configuration metadata.
