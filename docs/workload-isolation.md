# Workload isolation

Replication and maintenance work uses explicit record, byte, and concurrency
budgets per work class. Control and foreground publish traffic have independent
reservations, so observer catch-up or compaction cannot consume their permits.
Saturation is reported as a rejection and callers can pause or retry; accepted
durable work is released only after completion or cancellation cleanup.
