# Hot-path state ownership

State that is ordered by a partition, consumer, producer, or tenant should be
owned by that domain's shard rather than by a broker-wide lock. Morrow exposes a
stable, allocation-free FNV-1a shard selector in `server::state_shards`; its
mapping is deterministic across restarts and bounded by the configured shard
count.

The broker uses 64 shards by default. Operators can tune the gate count for a
larger or smaller CPU budget with `MORROW_STATE_SHARD_COUNT`; values are
clamped to `1..=4096` and the effective count is exported as
`morrow_state_shard_count`. Increasing the count reduces collisions between
independent partition keys, but does not remove the separate durable-state
mutex, storage gate, or filesystem limits.

Cross-domain operations follow the lock order **partition → consumer/producer →
tenant → administration**. Filesystem I/O, network waits, middleware, and WAL
responses happen after releasing all state-shard locks. Reserve/validate/commit
is preferred when a mutation spans more than one domain.
