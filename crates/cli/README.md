# CLI crate

This crate builds the `morrow-cli` binary. It is a client application, not the
broker server; run `morrow-server` separately.

The CLI reads `client.json` and provides `ping`, `pub`, `sub`, `request`, and
`reply`. Examples and the client configuration shape are documented in the
[top-level README](../../README.md). The CLI delegates transport,
authentication, and protocol handling to the [`client`](../client/README.md)
crate.
