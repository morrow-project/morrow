# Pre-1.0 service-level objectives

This is the controlled-pilot contract for Morrow. It is deliberately separate
from the future general-availability (GA) target: pilot values are the initial
operational promise, while GA values are tightening targets that require the
evidence described at the end of this document. Dashboards are not required;
the SLOs use black-box probes, the authenticated Morrow metrics endpoint, and
the Prometheus rules in `monitoring/prometheus/morrow-alerts.yml`.

## Measurement contract

All windows are rolling 30-day windows unless stated otherwise. The probe
region, broker version, topology, and payload class are recorded with every
sample. A sample is successful only when the user-visible operation completes
within its deadline and returns the expected response. Prometheus is scraped at
30 seconds or less and evaluated at 30 seconds.

Exclusions are narrow: a pre-announced maintenance interval with an approved
start/end time and tested rollback; probe downtime caused by invalid credentials,
malformed requests, or an explicitly rejected request that the client generated
outside its contract; and an experiment explicitly marked before it starts.
Network failures, broker crashes, quota exhaustion, storage saturation, leader
elections, and operator mistakes remain in the SLO unless the maintenance
exclusion was declared in advance. No exclusion may be added after observing
the result.

## Availability objectives

| SLO | User-visible SLI | Controlled pilot target | Future GA target | Error budget | Evidence and owner |
| --- | --- | ---: | ---: | ---: | --- |
| Client data plane | Successful TCP `INFO` plus contract-valid publish/ack probe divided by eligible probes | 99.5% / 30d | 99.95% / 30d | 216 min / 21.6 min | External probe; platform on-call; `MorrowReadinessUnavailable` |
| Authenticated admin diagnostics | Successful authenticated `GET /api/v1/health/ready` and `/api/v1/metrics` response within 2s | 99.5% / 30d | 99.95% / 30d | 216 min / 21.6 min | External probe; platform on-call; `MorrowMetricsMissing` |

Standalone availability is measured against one broker. Clustered availability
is measured against the service endpoint and requires the configured quorum
policy; an individual follower being unavailable is not a user-visible outage
when the service remains available, but is recorded for the recovery SLO.

## Publish latency objectives

The SLI is the elapsed time from a publish write accepted by the client library
until the matching producer acknowledgement. Measure each acknowledgement
level separately: `0` accepted, `1` durable, `2` high durability, and `3`
cluster durable. Run separate payload classes: 0–1 KiB, 1–64 KiB, and 64–256
KiB. Samples below 10,000 publishes per level and payload class per topology
in a window are reported as insufficient evidence, not silently combined.

The aggregate server metric `morrow_publish_latency_us` is a diagnostic
cross-check; it must not replace the per-level black-box SLI because it has no
QoS or payload-size labels.

| Ack level | Pilot p99 target (small / medium / large) | Future GA p99 target (small / medium / large) | Error budget |
| --- | --- | --- | ---: |
| Accepted | 50 / 100 / 250 ms | 25 / 50 / 125 ms | 5% / 1% of samples |
| Durable | 100 / 250 / 500 ms | 50 / 125 / 250 ms | 5% / 1% of samples |
| High durability | 250 / 500 / 1,000 ms | 125 / 250 / 500 ms | 5% / 1% of samples |
| Cluster durable | 500 / 1,000 / 2,000 ms | 250 / 500 / 1,000 ms | 5% / 1% of samples |

Standalone level 3 is rejected by contract and is not an availability or
latency failure. Clustered measurements must identify whether the publish was
served by the local leader or proxied to one; both are included in the cluster
SLO unless the test explicitly measures a topology-specific SLI.

## Durability and acknowledged-message loss

The SLI is the fraction of acknowledged messages whose immutable message ID,
payload digest, and stream position can be recovered and verified after the
specified failure or restore exercise. The target is zero acknowledged-message
loss for levels 1–3 in both pilot and GA; the error budget is zero. Level 0 is
accepted/transient and carries no durability guarantee. Level 1 guarantees a
local durable append or partition-replica quorum append; level 2 guarantees a
local flush or quorum fsync; level 3 additionally requires a clustered metadata
high-watermark commit. These are the exact protocol guarantees, not a claim
that a client which never receives an acknowledgement was persisted.

Delivery is at-least-once. A crash, lease expiry, reconnect, or uncertain
acknowledgement may produce duplicates; consumers must acknowledge idempotently.
Retry exhaustion creates a durable dead-letter record according to the
configured policy. A duplicate is not counted as loss, but an acknowledged ID
whose digest cannot be recovered is a zero-budget incident.

## Consumer lag and redelivery

| SLO | SLI and target | Pilot / GA error budget | Measurement |
| --- | --- | ---: | --- |
| Consumer lag | 99% / 99.9% of one-minute samples have end-to-end age ≤60s under declared capacity | 1% / 0.1% | Timestamped enqueue/consume probe plus `morrow_consumer_lag_messages` |
| Redelivery | 99% / 99.9% of successfully acknowledged deliveries require no more than one delivery attempt | 1% / 0.1% | Delivery ID/attempt probe plus `morrow_redeliveries_total` |
| Scheduled delivery | 99% / 99.9% of due messages are delivered within 60s of due time | 1% / 0.1% | Due-time probe plus `morrow_scheduled_delivery_due_lag_ms` |

Client-caused pauses and intentionally unacknowledged deliveries are labeled
in the test record and excluded only when they match the pre-declared client
contract. Broker redelivery, storage delay, and consumer-group recovery remain
included.

## Recovery objectives

RPO is the newest acknowledged data that may be absent; RTO is time from the
declared failure event until the user-visible SLI is healthy and a canary
operation succeeds. Record start/end UTC timestamps, topology, node IDs, and
the verification output.

| Failure exercise | Controlled pilot RTO / RPO | Future GA RTO / RPO | Runbook owner |
| --- | --- | --- | --- |
| Process restart | ≤60s / 0 for levels 1–3 | ≤30s / 0 | Platform; `docs/runbooks/upgrade-rollback.md` |
| Node replacement | ≤15m / 0 for cluster-durable data | ≤5m / 0 | Platform; `docs/runbooks/node-replacement.md` |
| Leader election | ≤30s / 0 for acknowledged cluster data | ≤10s / 0 | Platform; `docs/runbooks/quorum-loss.md` |
| Quorum restoration | ≤10m / 0 after the committed high watermark | ≤5m / 0 | Platform; `docs/runbooks/quorum-loss.md` |
| Backup restore | ≤30m / backup checkpoint interval ≤5m | ≤10m / checkpoint interval ≤1m | Storage; `docs/runbooks/backup-restore.md` |

Backup-restore RPO is intentionally separate from cross-node RPO: it is bounded
by the last verified backup checkpoint, while a healthy cluster with level 3
acknowledgements has zero acknowledged-message RPO. Standalone deployments do
not claim node-replacement, leader-election, or quorum-restoration objectives;
they use process-restart and backup-restore objectives only.

## Alerting and error-budget policy

The Prometheus rules map directly to these SLOs: readiness and quorum alerts
cover availability; `MorrowFsyncLatencyHigh` and
`MorrowDurablePublishLatencyHigh` cover latency; consumer/redelivery/dead-letter
alerts cover delivery; WAL, filesystem, and audit alerts cover durability
risks. Alert owners are platform for availability/cluster/storage, messaging
operations for consumer delivery, and security operations for audit failures.
Every alert has a runbook path in the rule file.

When an error budget is exhausted, freeze target-tightening and non-essential
feature rollout, open an incident, identify the consumed budget by topology and
payload class, and require a reviewed corrective action. A pilot may continue
only with an explicit risk acceptance; GA targets cannot be adopted from a
window with insufficient samples or an unverified recovery exercise.

## Review and tightening evidence

Review this document quarterly and after every material storage, replication,
protocol, or recovery change. To tighten pilot targets toward GA, retain at
least three representative soak/capacity runs per topology, each with ≥10,000
samples per latency bucket and a documented failure/recovery exercise. Attach
raw probe output, Prometheus exports, broker version/configuration, and the
runbook verification record. The initial representative clustered durable
publish run is retained in
[`docs/slo-evidence/2026-08-25-cluster-durable-publish.md`](slo-evidence/2026-08-25-cluster-durable-publish.md);
it validates the draft’s pilot scale but is explicitly insufficient to claim GA
capacity.
