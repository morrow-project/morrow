# Transactions, schema governance, and materialized views

## Transaction visibility

Transactions have an explicit `Open -> Prepared -> Committed` or
`Open/Prepared -> Aborted` state. Writes, consumer offsets, and view mutations
are accumulated in one bounded record. Ordinary readers must consult only
`Committed` batches; prepared data is intent and is not visible. A producer
epoch fences older open or prepared transactions, and the durable coordinator
aborts timed-out in-doubt records during recovery. Limits cover message count,
bytes, distinct partitions, duration, and concurrent transactions.

The coordinator is the atomic batch boundary. Applying a committed batch to
partition logs, consumer cursors, and views must be done from the returned
batch, in that order, with the transaction ID used for idempotent retry. A
crash before the terminal commit record exposes nothing; a crash after it
replays the same complete batch.

## Schema governance

Schemas are scoped by tenant and subject and receive immutable IDs and
monotonic versions. JSON Schema, Protobuf, and Avro definitions are validated
at registration time. Compatibility is deterministic and configured per
tenant/subject as `None`, `Backward`, `Forward`, or `Full`; references must
resolve within the same tenant. Deletion is soft so historical IDs remain
auditable, and rollback reactivates a selected version without rewriting
messages. Routing does not deserialize payloads: a producer carries the schema
ID as immutable message metadata, and consumers resolve it only when they need
the contract.

## Materialized views

A view is a bounded key/value projection over a compacted stream. Each update
records its source stream, partition, and offset; the view exposes these
consistency positions with point reads. Snapshots include the complete bounded
map and positions. Rebuild sorts retained history by source position, making
recovery deterministic. Watch cursors are bounded and expire explicitly rather
than causing unbounded replay. View values, entries, and watch events are
quota-limited and persisted atomically.

Administration should audit schema changes, transaction terminal transitions,
view rebuilds, and snapshot restores. Tenant authorization and storage
encryption are admission requirements for every operation; no payload or key
material belongs in metrics or audit metadata.
