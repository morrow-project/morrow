# Partition cardinality and lazy resources

Partition descriptors (assignment, epoch, generation, and committed watermark)
must remain available for every configured partition, while expensive open log
handles and decoded indexes should be activated only for local replicas. The
`server::partition_cache::PartitionResourceCache` provides a hard LRU bound for
those active resources. Eviction removes only the runtime resource; durable
descriptors remain authoritative and can be used to reopen the partition on its
next access.
