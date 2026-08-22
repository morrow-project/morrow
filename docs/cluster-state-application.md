# Incremental clustered state application

The Raft state machine retains a bounded stream of committed deltas keyed by log
index. Brokers perform one full reconciliation at startup, record that applied
index, and then apply only newer consumer or partition changes. Blank and
membership entries remain in the stream so the broker can prove that every log
index is contiguous even when no local data structure changes.

Consumer upserts update the subject-interest index in place and preserve local
members, leases, attempts, and advanced cursors. Deletes remove only the named
consumer and its index entry. A partition commit retrieves only its replicated
envelope, appends that partition idempotently, and updates the message and
partition-position maps. Partition payloads remain outside metadata snapshots.
If metadata reaches a replica before its data RPC, that log index remains
pending and blocks later deltas until partition catch-up supplies the envelope;
nodes outside the replica set advance without materializing the payload.

The stream retains 4,096 entries. Startup, snapshot installation, or a broker
that falls behind that window triggers a full reconciliation; ordinary writes
never do. Duplicate deltas verify their existing partition identity or preserve
the consumer's advanced cursor, making retry safe across leadership changes.

`GET /cluster` exposes cumulative `state_application.delta_applications` and
`state_application.full_reconciliations` counters. A rising full-reconciliation
counter after startup indicates snapshot catch-up or a broker lagging beyond the
bounded delta window.

The Task 019 three-node benchmark was repeated after this change on 2026-08-22:
250 sequential durable writes measured 19.3/s with p50 50.711 ms, p95 66.117 ms,
and p99 74.826 ms, versus 20.2/s, 48.945 ms, 64.669 ms, and 78.986 ms before.
The runs are within local fsync variance; sequential acknowledgement remains
storage-latency bound. Constant application work is instead guarded by the
randomized differential test, which grows partition history while asserting
zero hot-path full reconciliations after every committed mutation.
