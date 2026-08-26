# Storage concurrency

The broker keeps independently synchronized state for connections, transient
subscriptions, durable consumers, the control WAL, and partition logs. A slow
durable operation therefore does not hold the connection or transient-routing
locks. Middleware execution is owned by the shared middleware runtime and runs
outside those state locks.

Each `(stream, partition)` log has its own append mutex. That mutex is the
partition's ordering owner: concurrent appends to the same partition receive
monotonic offsets, while appends and flushes for different partitions can make
progress independently. High-durability publication flushes only the selected
partition before flushing the control WAL.

Partition appends and fsyncs run on Tokio's blocking pool behind a 64-permit
semaphore. Every control-WAL operation is ordered by a dedicated worker with a
128-command bounded queue; fsync and checkpoint callers wait from Tokio's
blocking pool. Waiting for a permit or queue capacity is the backpressure boundary
between protocol tasks and storage workers; work is never queued without a
bound.

An asynchronous read/write gate lets partition operations proceed concurrently
but gives shutdown an exclusive fence before it flushes logs and checkpoints
durable state.

An `ACCEPTED` producer acknowledgement may precede durable storage. Other local
producer acknowledgements are sent only after the partition append and control
WAL record complete. `HIGH_DURABILITY` additionally waits for the selected
partition and control WAL fsyncs. Cluster durability continues to use the
replication boundary selected by the producer acknowledgement level.
# Grouped durability barriers

High-durability publishes append their record first, then join a bounded WAL
flush epoch. The first waiter opens an epoch whose maximum delay is
`fsync_interval_ms`; concurrent waiters share the same flush and are released
together. This keeps sparse traffic bounded while allowing busy partitions to
amortize one storage barrier across many records. The append and acknowledgement
ordering remains per record, so an accepted record cannot be lost if a client
disconnects while a flush epoch is open.

The `/wal` administrative status also reports `partition_append_batches`,
`partition_append_records`, `partition_append_bytes`, and `flushes`. These
counters make it possible to verify that a workload is sharing append and
durability work rather than merely measuring concurrent single-record writes.
