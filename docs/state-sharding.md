# Hot-path state ownership

State that is ordered by a partition, consumer, producer, or tenant should be
owned by that domain's shard rather than by a broker-wide lock. Morrow exposes a
stable, allocation-free FNV-1a shard selector in `server::state_shards`; its
mapping is deterministic across restarts and bounded by the configured shard
count.

Cross-domain operations follow the lock order **partition → consumer/producer →
tenant → administration**. Filesystem I/O, network waits, middleware, and WAL
responses happen after releasing all state-shard locks. Reserve/validate/commit
is preferred when a mutation spans more than one domain.
