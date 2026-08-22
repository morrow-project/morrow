# Bounded partition recovery

Partition payloads remain canonical in the segmented partition logs. Recovery
validates each checksummed record, records its small routing and cursor metadata,
and releases the payload allocation before scanning the next record. The broker
therefore keeps no retained partition payloads or per-record checksum map in its
steady-state message table.

Push delivery, pull `FETCH`, acknowledgement middleware, replica catch-up, and
retention rewrites resolve the full record from its `(stream, partition,
offset)` position. Reads use the per-segment sparse offset index and still verify
the stored record checksum before returning data. Empty payloads remain valid and
are distinguished by the durable position rather than payload length.

Independent partitions recover concurrently. The worker count is capped at
eight and also limited by available parallelism and configured partition count.
The authenticated `/streams` response exposes the completed and total partition
counts, records scanned, recovery duration, worker count, and an estimate of the
resident replay metadata bytes.

The manual `benchmark_recovery_across_increasing_histories` test measures open
time for increasing retained histories. The regular large-history test verifies
that recovered payload capacities are zero and spot-checks lazy, ordered reads
against the originally appended envelopes.
