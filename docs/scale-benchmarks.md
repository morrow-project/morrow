# Scale validation fixture

`scripts/run-scale-benchmark.sh` records the commit, host, payload, broker-count
labels, topic counts, partition counts, and per-case publish benchmark output.
It targets an already running deployment so the same fixture can exercise either
combined nodes or a separated controller/broker topology. Broker-count labels
must describe the deployment under test; the fixture does not silently change
cluster membership.

Example:

```sh
scripts/run-scale-benchmark.sh \
  --server 127.0.0.1:4222 \
  --client-config ./client.json \
  --broker-counts 3,5 \
  --topics 1,10 \
  --partitions 1,4 \
  --clients 5 --duration 60s --payload-size 128
```

Each case is written below the output directory as JSON/CSV benchmark artifacts,
with a machine-readable `manifest.json` at the root. The manifest captures the
UTC start time, host and kernel, online CPU count, total memory when the host
exposes it, CPU model, and nominal CPU frequency. Empty or unavailable host
values are recorded as an empty string or `null`, rather than guessed. Keep
these artifacts with the release commit when comparing throughput, p95 latency,
resource use, and controller activity across topology sizes.
