# Workload isolation

Replication and maintenance work uses explicit record, byte, and concurrency
budgets per work class. Control and foreground publish traffic have independent
reservations, so observer catch-up or compaction cannot consume their permits.
Saturation is reported as a rejection and callers can pause or retry; accepted
durable work is released only after completion or cancellation cleanup.

The scheduler has separate budgets for control, foreground publish, observer
replication, replica catch-up, snapshots, reassignment, retention, and
compaction. Active commit-set catch-up uses the control lane; observer catch-up
uses the catch-up lane, so background recovery cannot consume the control
reservation. Prometheus exposes bounded `morrow_work_<class>_active` and
`morrow_work_<class>_rejections_total` gauges/counters for each lane.
