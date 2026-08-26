# Online partition expansion

Expansion is a two-phase, epoch-fenced transition. The controller first creates
and prepares every new partition replica, then atomically activates the next
partitioning epoch. A client using any other epoch receives an explicit refresh
decision; it must not silently route a key under a conflicting map. The
`server::partition_expansion::PartitionExpansion` state machine preserves the
current map until preparation is complete and rejects overlapping or shrinking
plans.

Direct publishers carry the epoch they used to select a leader in the
`Morrow-Partitioning-Epoch` header. The broker strips this routing header from
stored application metadata and rejects a stale value with the current epoch
in the error text. A routed client can then refresh metadata and retry a
durable publish with the same producer message ID; publishers that do not send
the header remain compatible and are assigned using the broker's current map.
