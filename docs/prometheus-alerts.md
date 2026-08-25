# Prometheus alert rules

The deployment-neutral rule file is
[`monitoring/prometheus/morrow-alerts.yml`](../monitoring/prometheus/morrow-alerts.yml).
It uses only aggregate Morrow metrics and bounded `resource` values emitted by
the authenticated `/api/v1/metrics` endpoint. No tenant, stream, subject,
consumer, or message identifiers are used as labels.

## Evaluation and scrape contract

Use a scrape interval of 30 seconds or less and an evaluation interval of 30
seconds. The rules assume the Morrow metrics endpoint is scraped with a stable
`job` and `instance` label. Filesystem and inode alerts additionally require
node-exporter metrics for the Morrow data filesystem. If node-exporter is not
deployed, `MorrowNodeExporterMissing` is intentional: capacity and inode
coverage is unavailable rather than silently green.

The rules use five-minute or longer `for` windows for paging signals. WAL
warning and critical thresholds are 50 GiB and 80 GiB; operators should keep
them below the actual filesystem capacity and may override them in a reviewed
deployment overlay. Consumer lag is measured in aggregate messages, scheduled
delivery lag in milliseconds, and latency thresholds use microseconds.

`MorrowLeaderChangeRateHigh` is a bounded readiness-transition proxy because
the current exporter does not expose an unbounded leader-id label. Confirm it
with `/api/v1/cluster` and the quorum runbook before changing cluster
membership. `MorrowWALCheckpointStalled` requires publish activity so an idle
broker does not page. `MorrowWALMetricsMissing` and `MorrowMetricsMissing`
explicitly alert when the exporter is absent or incompatible.

## Rule validation

Install Prometheus or use its pinned container image and run:

```sh
promtool check rules monitoring/prometheus/morrow-alerts.yml
promtool test rules monitoring/prometheus/morrow-alerts.test.yml
```

The repository helper `packaging/tests/prometheus-rules-test.sh` runs both
commands when `promtool` is available and otherwise performs a fixture-presence
check. CI installs a version-pinned Prometheus tool container and runs the same
syntax and unit tests.

## Alert ownership

Readiness, quorum, route, and Raft alerts are owned by the broker/platform
on-call. WAL, filesystem, fsync, and checkpoint alerts are owned jointly by
storage and platform on-call. Consumer, scheduled-delivery, redelivery, and
dead-letter alerts are owned by messaging operations. Audit alerts are owned by
security operations. Every alert links to a runbook path; dashboards are
deliberately out of scope.
