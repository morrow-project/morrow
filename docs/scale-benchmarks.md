# Scale validation fixture

`scripts/run-scale-benchmark.sh` records the commit, host, payload, broker-count
labels, topic counts, partition counts, and per-case publish benchmark output.
It targets an already running deployment so the same fixture can exercise either
combined nodes or a separated controller/broker topology. Broker-count labels
must describe the deployment under test; the fixture does not silently change
cluster membership. Set `--controller-voters` once per run; it is recorded as a
single scalar so comparing broker-count cases cannot accidentally imply that the
metadata quorum grew. Use `--deployment-profile combined` with
`--roles-share-process true` for combined nodes, or `--deployment-profile
separated --roles-share-process false` for dedicated controllers and brokers.

Example:

```sh
scripts/run-scale-benchmark.sh \
  --server 127.0.0.1:4222 \
  --client-config ./client.json \
  --broker-counts 3,5 \
  --topics 1,10 \
  --partitions 1,4 \
  --deployment-profile separated --controller-voters 3 \
  --roles-share-process false \
  --clients 5 --duration 60s --payload-size 128 --throughput 0
```

`--throughput` applies an aggregate publish rate limit (`0` means unlimited),
and `--fire-throughput` can set a separate limit for fire-and-forget cases.
Each case keeps its resource snapshots beside a fresh `results/` directory so
the wrapper can be rerun without colliding with the nested matrix output.

Each case is written below the output directory as JSON/CSV benchmark artifacts,
with a machine-readable `manifest.json` at the root. The manifest captures the
UTC start time, host and kernel, online CPU count, total memory when the host
exposes it, CPU model, and nominal CPU frequency. Empty or unavailable host
values are recorded as an empty string or `null`, rather than guessed. Keep
these artifacts with the release commit when comparing throughput, p95 latency,
resource use, and controller activity across topology sizes.

Pass `--metrics-url http://host:admin-port/metrics` to capture a Prometheus
snapshot as `metrics.prom` inside every case directory. This makes controller
activity and broker queue/replication counters available beside the benchmark
result instead of requiring a second, unsynchronised scrape.
When supplied, the fixture also fails a case if the endpoint reports a different
controller-voter count or process role than the selected topology profile.
For a real broker-fleet gate, also pass `--expected-brokers N`. Each metrics
snapshot must then report exactly `N` registered brokers; broker-count labels are
otherwise descriptive only and do not change cluster membership.

Pass `--server-pid PID` to capture `resources-before.json` and
`resources-after.json` in every case directory. These snapshots report resident
memory, CPU time, thread count, and open file descriptors; macOS also records
the sampled process CPU percentage. The PID is explicit so a host running
multiple Morrow processes cannot silently report the wrong process, and it must
remain alive for the duration of the run.

Partition recovery is capped at eight workers by default. Operators can lower
that concurrency for large catalogs or constrained hosts with
`MORROW_PARTITION_RECOVERY_WORKERS`; values are clamped to the safe range
`1..=8`.

For clustered catch-up sensitivity, vary the bounded append batch with
`MORROW_DATA_APPEND_BATCH_RECORDS` and `MORROW_DATA_APPEND_BATCH_BYTES`. Record
these values with the benchmark manifest when comparing replication throughput;
the receiver enforces the same hard maxima.
