# Client crate

The `client` crate is the reusable Rust SDK for Morrow. It handles TCP and TLS
connections, Ed25519 authentication, `CONN`, publishing, subscriptions,
request/reply, pull consumers, delivery controls, and explicit ACKs.

The main entry points are `Client`, `ClientOptions`, `ClientAuth`,
`ClientTlsOptions`, `Message`, `DurableMessage`, and `ServerFrame`.

Applications and `morrow-cli` depend on this crate rather than implementing
wire framing themselves. See the [protocol reference](../protocol/PROTOCOL.md)
for the frames exposed by the client.
