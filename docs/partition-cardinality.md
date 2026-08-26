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

Clustered replica catch-up batches are bounded to 256 records and 8 MiB by
default. Operators may lower those limits with `MORROW_DATA_APPEND_BATCH_RECORDS`
and `MORROW_DATA_APPEND_BATCH_BYTES`; values are clamped to safe positive limits
and apply consistently at both the sender and receiver.
