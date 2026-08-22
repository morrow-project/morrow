# Incremental Raft storage

Raft control state uses two append-only, versioned journals:
`raft-log.journal` for votes, committed indexes, entries, truncation, and purge,
and `raft-state.journal` for applied state-machine entries. Each record has a
length and CRC32 checksum. A partial final record is removed during recovery;
checksum failure in a complete record is treated as corruption.

Mutations append and `sync_data` only the new record. The filesystem work runs
on Tokio blocking workers. The log journal is atomically checkpointed when it
reaches either 1,024 records or 8 MiB, while OpenRaft snapshot/purge bounds the
retained log image. State snapshots are written and synced to a temporary file,
renamed, and followed by journal rotation. Recovery loads the atomic snapshot
first and replays only journal entries newer than its last-applied index, so
interruption on either side of the rename/rotation boundary is safe.

Existing `raft-log.json`, `raft-state.json`, and `raft-snapshot.json` files are
migrated on first open. The new journal or snapshot is synced before the source
is renamed with a `.migrated` suffix. An existing new-format file always wins,
making migration restart-safe and preventing an ambiguous downgrade.

## Three-node benchmark

The ignored integration benchmark runs 250 sequential durable QoS publishes
through the elected leader of an in-process three-node cluster:

```bash
cargo test -p integration --release benchmark_cluster_durable_publish_latency \
  -- --ignored --nocapture
```

Results recorded on 2026-08-22 on the same development machine and release
profile:

| Backend | Revision | Throughput | p50 | p95 | p99 |
| --- | --- | ---: | ---: | ---: | ---: |
| Full-file JSON | `5a390f8` | 8.6/s | 115.078 ms | 136.543 ms | 175.699 ms |
| Incremental journals | Task 019 working tree | 20.2/s | 48.945 ms | 64.669 ms | 78.986 ms |

This is a local, sequential, fsync-sensitive comparison rather than a capacity
claim. It includes Raft replication, state-machine persistence, broker storage,
TCP framing, and producer acknowledgement. Re-run it on deployment storage for
capacity planning.
