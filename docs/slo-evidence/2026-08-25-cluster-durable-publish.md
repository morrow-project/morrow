# Initial SLO evidence: clustered durable publish

Date: 2026-08-25 UTC  
Topology: repository `ClusterHarness::start_three()` three-node integration
harness  
Payload: `benchmark-payload` (small payload class)  
Acknowledgement: protocol level 1 (`Durable`)  
Environment: local macOS arm64 development host; not a production capacity
claim

Command:

```sh
cargo test -p integration --release --test client_server \
  benchmark_cluster_durable_publish_latency -- --ignored --nocapture
```

Retained output:

```text
running 1 test
test raft_storage_benchmark::benchmark_cluster_durable_publish_latency ... cluster_raft_storage samples=250 throughput=19.6/s p50_us=49918 p95_us=70539 p99_us=91126
ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 26 filtered out; finished in 12.98s
```

Interpretation: the run’s 91.126 ms p99 is below the controlled-pilot small
payload durable-publish target of 100 ms. It is a representative three-node
capacity probe, not a 30-day availability measurement, a large-payload result,
or evidence for the future GA target. The tightening gate therefore remains
open until the three-run/10,000-sample evidence requirement in
`docs/service-slos.md` is met.
