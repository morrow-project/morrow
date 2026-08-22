# Integration crate

The `integration` crate contains cross-crate smoke tests. It exercises real
TCP, TLS, authentication, routing, partition replication, and OpenRaft paths.

Keep deterministic semantic coverage in the server crate; use this crate for
wiring and end-to-end behavior that cannot be covered in-process.
