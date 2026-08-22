# Incremental key compaction

Key-compacted streams maintain a latest-value index keyed by namespace, stream,
partition, and key. A committed append compares only its own key position,
updates that entry, and removes at most one superseded broker sequence. Startup
and full cluster reconciliation rebuild the index once from recovered metadata;
ordinary append cost is independent of compacted history.

Logical removal also clears stale pending, lease, acknowledgement, partition,
and delivery-index state. Offsets are never renumbered. Consumers select the
remaining sparse offsets through the partition subject index and preserve the
same per-partition high watermark.

After 64 supersessions, the broker schedules physical reclamation on the
bounded storage worker pool. A write gate coordinates the snapshot with local
append and retention, while each partition mutex coordinates lazy readers. The
rewrite keeps all visible keyed and unkeyed records in original offset order and
persists the pre-compaction next offset.

Physical replacement is crash recoverable. The broker writes and syncs a new
segment, then syncs a rewrite marker containing the next offset before replacing
old segments. Startup completes any marked replacement, so an interruption
chooses the fully written compacted segment and cannot resurrect superseded
records. Replica transport logs remain available as the committed catch-up
source while broker-visible partition segments are reclaimed.
