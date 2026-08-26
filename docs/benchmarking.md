# Benchmarking

`morrow-cli bench` runs reproducible workloads through the normal Morrow client
protocol. It supports independent publish, subscribe, combined publish/subscribe,
request, response-service, durable consume, and fetch modes:

```bash
morrow-cli bench pub orders/bench --clients 4 --messages 100000
morrow-cli bench sub orders/bench --clients 2 --duration 30s
morrow-cli bench pubsub orders/bench --publishers 4 --subscribers 2 --messages 100000
morrow-cli bench request service/echo --clients 8 --duration 30s
morrow-cli bench serve service/echo --clients 4 --queue responders --duration 30s
morrow-cli bench consume orders-worker --clients 2 --messages 10000 --ack
morrow-cli bench fetch orders-worker --clients 2 --messages 10000 --batch-size 100 --ack
```

For `request`, start `serve` separately. For `sub`, start `pub` separately.
`consume` and `fetch` use an existing durable consumer named by the positional
argument; they do not create or reset it.

## Reproducible publish matrix

The repository includes a fixture that runs publish modes and acknowledgement
levels as an isolated matrix and retains JSON, CSV, logs, and effective server
configuration for each case:

```bash
scripts/run-publish-benchmark-matrix.sh --clients 5 --duration 10s
scripts/run-publish-benchmark-matrix.sh --topology cluster --clients 1 --duration 10s
```

It can also use an existing server and an installed CLI without managing the
deployment:

```bash
scripts/run-publish-benchmark-matrix.sh \
  --topology external \
  --no-build \
  --cli-bin "$(command -v morrow-cli)" \
  --client-config ./client.json \
  --server 192.0.2.10:4222 \
  --ack-levels accepted,durable,high-durability,cluster-durable
```

Run the script with `--help` for workload controls. The recorded local results,
methodology, latency interpretation, and observed concurrent cluster failures
are in
[`benchmark-results/2026-08-26-publish-mode-ack-matrix.md`](benchmark-results/2026-08-26-publish-mode-ack-matrix.md).

## Publishing behavior

`--mode` separates different client costs:

- `fire-and-forget` measures client enqueue/write latency and performs one final
  `PING`/`PONG` protocol flush. It does not request producer acknowledgements.
- `sync` sends one acknowledged message at a time on each client.
- `async` pipelines up to `--max-in-flight` acknowledged messages per client.
- `batch` sends up to `--batch-size` messages through the batch client API and
  validates every acknowledgement. A final partial batch is allowed.

Acknowledged modes default to `durable`. `--ack-level` can explicitly request
`accepted`, `durable`, `high-durability`, or `cluster-durable`. Results include
the requested and observed level and acknowledgement-contract version. A
missing, mixed, or downgraded acknowledgement makes the run fail.

Examples:

```bash
morrow-cli bench pub orders/bench --clients 8 --messages 100000 --mode fire-and-forget
morrow-cli bench pub orders/bench --clients 8 --messages 100000 --mode sync --ack-level durable
morrow-cli bench pub orders/bench --clients 8 --messages 100000 --mode async --max-in-flight 256
morrow-cli bench pub orders/bench --clients 8 --messages 100000 --mode batch --batch-size 100
```

## Workload flags

| Flag | Meaning |
|---|---|
| `--clients N` | Opens `N` independent connections for the mode's active role. A count is divided across them, not multiplied. |
| `--messages N` | Runs an aggregate fixed count. Fan-out subscribers each expect `N`; a queue group expects `N` total. Mutually exclusive with `--duration`. |
| `--duration D` | Runs the measured phase for a duration such as `30s`. Setup, warm-up, and drain are outside it. |
| `--throughput N` | Limits aggregate generated operations per second. Zero or omission is unlimited. |
| `--sleep D` | Adds a per-client delay between operations. When combined with `--throughput`, the slower limit wins. |
| `--payload-size N` | Reuses one generated payload of exactly `N` application bytes. |
| `--payload FILE` | Reuses the exact file bytes as each payload. Mutually exclusive with `--payload-size`. |
| `--header K:V` | Adds a repeatable application header. Newlines and reserved `Morrow-*` and benchmark identity headers are rejected. |
| `--subjects N` | Uses the base subject for `1`; otherwise derives `base/0` through `base/N-1`. |
| `--subject-order sequential\|random` | Cycles subjects or selects them with the seeded deterministic generator. |
| `--key-cardinality N` | Generates `N` deterministic routing keys; zero sends no generated key. |
| `--seed N` | Controls deterministic subject and key selection and is recorded in results. |
| `--warmup D` | Runs the workload before measurement; its operations and latency samples are excluded, while broker state remains. |
| `--queue GROUP` | Uses queue/group delivery for subscribe, pub/sub, or service-response workloads. |
| `--ack` | Explicitly acknowledges push or pull deliveries. For legacy `bench pubsub`, it also retains the old durable producer-ack behavior. |
| `--durable-id ID` | Uses `ID` as the per-client durable identity prefix. |
| `--timeout D` | Sets request, response wait, or fetch wait timeout. |
| `--max-bytes N` | Sets the maximum bytes returned by one pull fetch. |
| `--json` | Prints the complete result as JSON. |
| `--csv FILE` | Writes stable aggregate and per-client CSV rows; it can be combined with human or JSON output. |

The legacy `bench pubsub` flags `--publishers`, `--subscribers`, and
`--concurrency` remain supported. `--concurrency` multiplies the legacy
publisher count. New scripts should prefer independent `pub` and `sub` commands
with `--clients` when measuring producer and delivery capacity separately.

## Measurement and output

All connections finish setup and signal readiness before a common measured
start instant. Workers own bounded latency samples, so publishing does not take
a global per-message statistics lock. Payload buffers are reused. Optional
warm-up runs before the measured phase; combined pub/sub drains and validates
delivery after publishing without adding drain time to publisher throughput.

Human, JSON, and CSV results carry the same workload identity and include:

- aggregate and per-client operations, messages/second, payload MiB/second,
  elapsed time, errors, timeouts, reconnects, duplicates, and acknowledgements;
- latency min, mean, standard deviation, p50, p90, p95, p99, p99.9, and max;
- endpoint and local/remote classification, CLI and protocol versions, optional
  build revision, OS, architecture, CPU core count, effective seed, and config.
- explicit `phases` metrics: warm-up operations, measured operations, active
  duration, post-generation drain duration, total operations, offered rate,
  achieved rate, and a `steady_state` validity flag. A duration run is marked
  steady-state only when it lasts at least one second and records operations.

The publish matrix fixture also writes a bounded `*.resources.txt` sample stream
alongside each case. Samples are taken once per second and include timestamp,
free disk, host memory counters, and managed server CPU/RSS when available.
These sidecars make storage pressure and process resource usage visible without
changing the benchmark's measured client path.

Latency means the timed operation boundary for each mode. Fire-and-forget is
client enqueue/write latency; acknowledged publish is acknowledgement latency;
request is round-trip latency; response service is response-write latency;
consume/fetch is fetch-batch latency; combined pub/sub subscriber samples are
end-to-end delivery latency. Maximum-throughput results should use unlimited
rate and no sleep.

Incomplete delivery, duplicates, request timeouts, missing acknowledgements,
mixed acknowledgement levels, and acknowledgement contract mismatches fail the
command instead of silently producing a valid headline result.

## Remote brokers

Benchmarks use the normal `client.json` TLS, authentication, and connection
settings. `--server` overrides only the endpoint:

```bash
morrow-cli --server 192.0.2.10:4222 bench pub orders/bench \
  --clients 8 --duration 30s --mode async --max-in-flight 256 --json \
  --csv results.csv
```

Remote plaintext runs emit a warning. Keep the machine-readable result with
each comparison: host load, network placement, storage state, and build revision
can materially change throughput and latency.
