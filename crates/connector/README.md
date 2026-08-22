# Connector crate

The `connector` crate builds `morrow-connector`, a runtime for moving records
between Morrow and external source or sink adapters. Connector control subjects
are defined by the [`protocol`](../protocol/README.md) crate.

Connector configuration, middleware boundaries, checkpointing, and adapter
behavior are described in
[`docs/middleware-and-connectors.md`](../../docs/middleware-and-connectors.md).
