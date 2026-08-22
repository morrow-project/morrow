# Indexed delivery and redelivery

Durable delivery no longer discovers work by scanning retained messages. A
subject-interest trie marks only matching consumers ready when a partition
record is appended, a member gains credit, an acknowledgement advances a
cursor, retention advances a floor, or a lease expires. Idle delivery ticks can
therefore return without reading partition history.

Candidate selection uses each consumer's partition cursors and the persisted
per-segment subject indexes. The ordered `(stream, partition, offset)` map turns
the selected offset into the broker sequence without a message-table scan.

Delivery leases are scheduled in a min-deadline heap. Heap entries include the
delivery identity, so acknowledgements and extensions make older entries stale
without an eager heap deletion. Stale entries are discarded at the head and the
heap is rebuilt when stale growth exceeds a bounded ratio. One expiration tick
processes at most 1,024 live leases. The background loop sleeps until the next
lease deadline, with a one-second upper bound retained for age-retention work.
