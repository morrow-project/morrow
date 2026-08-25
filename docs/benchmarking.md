# Benchmarking

`morrow-cli` includes a pub/sub benchmark for measuring broker throughput and
latency. Start a local server first, then run a fixed-message benchmark:

```bash
morrow-cli bench pubsub orders/bench \
  --messages 100000 \
  --payload-size 1024 \
  --publishers 2 \
  --subscribers 1
```

The benchmark publishes a measurement header with every payload and verifies
that every subscriber receives exactly the expected number of messages. It
reports publish and end-to-end latency percentiles, throughput, elapsed time,
and duplicate counts. Use `--json` for machine-readable output.

To run for a time limit instead of a fixed count, use `--duration`:

```bash
morrow-cli bench pubsub orders/bench --duration 30s --json
```

`--messages` and `--duration` are mutually exclusive. `--publishers` controls
the number of publisher workers, while `--concurrency` multiplies that worker
count. `--ack` is a compatibility alias for `--ack-level durable` and also
acknowledges durable deliveries. Use `--ack-level` to select the publisher
acknowledgement boundary explicitly:

```bash
morrow-cli bench pubsub orders/bench --messages 10000 --ack-level durable
morrow-cli bench pubsub orders/bench --messages 10000 --ack-level high-durability
```

The accepted values are `accepted`, `durable`, `high-durability`, and
`cluster-durable`. `cluster-durable` requires a clustered server and waits for
every assigned replica; the benchmark reports a server error rather than
silently downgrading when the level is unsupported. Human-readable and JSON
output report both the requested acknowledgement level and the level observed
in producer acknowledgements. The default payload is 1,024 bytes and the
default fixed run publishes 10,000 messages.

## Remote brokers

The benchmark uses the same `client.json` configuration as the other CLI
commands. A direct endpoint can override the configured server:

```bash
morrow-cli --server 192.0.2.10:4222 bench pubsub orders/bench \
  --messages 100000 --payload-size 4096
```

Command-line options take precedence over the configuration file, which takes
precedence over built-in defaults. TLS and authentication settings in
`client.json` are honored. Remote plaintext connections produce a warning and
should only be used for controlled testing.

Benchmarking is intentionally excluded from normal CI performance gates: host
load, network placement, and broker storage state can make throughput and
latency results noisy. Use it for local comparisons or a dedicated benchmark
environment, and retain the JSON output with the server and client settings
used for each comparison.
