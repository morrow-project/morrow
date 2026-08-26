# Partition cardinality and lazy resources

Partition descriptors (assignment, epoch, generation, and committed watermark)
must remain available for every configured partition, while expensive open log
handles and decoded indexes should be activated only for local replicas. Local
startup already filters recovery to assigned replicas and caps recovery workers
with `MORROW_PARTITION_RECOVERY_WORKERS` (1 through 8).

The `server::partition_cache::PartitionResourceCache` currently provides a hard
LRU bound for decoded envelope metadata. Its capacity is controlled by
`MORROW_PARTITION_METADATA_CACHE_CAPACITY` (default 4096). Retention and rewrite
operations invalidate cached records, while cache hits avoid reopening segment
files. Active segment handles are released after a successful flush and are
reopened on the next append. Full whole-partition lazy activation and eviction
of all inactive runtime state remains a follow-up requirement.

Dynamic partition resources are bounded separately with
`MORROW_MAX_ACTIVE_DYNAMIC_PARTITIONS` (default 4096, hard maximum 65536).
Activation fails closed at the limit so a large topic catalog cannot allocate
unbounded segment handles or per-partition locks.

Clustered replica catch-up batches are bounded to 256 records and 8 MiB by
default. Operators may lower those limits with `MORROW_DATA_APPEND_BATCH_RECORDS`
and `MORROW_DATA_APPEND_BATCH_BYTES`; values are clamped to safe positive limits
and apply consistently at both the sender and receiver.

The clustered publish ingress coalescer uses a bounded queue per active
partition. `MORROW_PARTITION_INGRESS_QUEUE_LIMIT` controls the maximum number of
partition queues (default 4096, hard maximum 65536); attempts to create more
queues receive explicit backpressure instead of allocating unbounded runtime
state. `MORROW_PARTITION_INGRESS_BATCH_RECORDS` (default 32, maximum 256),
`MORROW_PARTITION_INGRESS_BATCH_BYTES` (default 1 MiB, maximum 8 MiB), and
`MORROW_PARTITION_INGRESS_BATCH_DELAY_MS` (default 2 ms, maximum 100 ms) tune
coalescing within those hard bounds. `MORROW_PARTITION_INGRESS_QUEUE_BYTES`
limits queued envelope memory per partition (default 64 MiB, hard maximum 256
MiB); a saturated byte budget returns explicit backpressure.
