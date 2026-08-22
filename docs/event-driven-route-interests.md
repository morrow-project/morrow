# Event-driven route interests

Transient subscriptions maintain a canonical, sorted subject map with a local
reference count for every subject. SUB adds a route interest only on the first
local reference. UNSUB, disconnect, and automatic maximum-delivery exhaustion
withdraw it only after the last reference is removed. Disconnects coalesce all
of their withdrawals into one update.

Each effective change increments a local interest version and broadcasts one
`interest_delta` frame containing added and removed subjects. Ordinary local or
routed publication no longer clones, sorts, deduplicates, or broadcasts the
subscription set. Publication only emits a delta when delivering the final
message of an `UNSUB <sid> <max>` subscription actually changes that set.

New peers receive a complete `interests` snapshot with the current version.
Peers apply only the next contiguous delta. A version gap leaves the last known
state intact and sends `interest_resync`; the sender answers with a fresh full
snapshot. Reconnected peers therefore rebuild the exact active set without
depending on missed deltas.

Remote indexes are updated incrementally from deltas and rebuilt only for a
full snapshot. Removing one duplicate local subscription cannot withdraw the
shared route, and interest maintenance on an unchanged publication is constant
work independent of the number of subscriptions.
