# Online partition expansion

Expansion is a two-phase, epoch-fenced transition. The controller first creates
and prepares every new partition replica, then atomically activates the next
partitioning epoch. A client using any other epoch receives an explicit refresh
decision; it must not silently route a key under a conflicting map. The
`server::partition_expansion::PartitionExpansion` state machine preserves the
current map until preparation is complete and rejects overlapping or shrinking
plans.
