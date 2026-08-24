# Protocol v1 SDK generation

The protocol v1 semantic model is implemented in `crates/protocol/src/model.rs`
and described for CBOR tooling by
`crates/protocol/schema/protocol-v1.cddl`. The CDDL file is the machine-readable
wire contract; the Rust model is the reference semantic implementation.

SDK generators should produce model types and codecs from the schema, while
transport clients remain responsible for connection lifecycle, request
correlation, flow control, and retries. Generated types must preserve unknown
fields where the schema marks them as optional extensions.

Every generated SDK must be tested against the shared protocol conformance
vectors before release. A hand-written client may be used as a reference, but
it must not define wire semantics independently of the schema.
