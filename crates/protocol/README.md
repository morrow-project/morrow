# Protocol crate

The `protocol` crate defines Morrow's wire-level behavior without opening
network connections. It provides command parsing, payload framing,
`MORROW/1.0` header parsing, server frames, subject validation, wildcard
matching, authentication helpers, and connector-control subjects.

Protocol v1 also exposes a protocol-independent semantic model in
[`MODEL.md`](MODEL.md) and `src/model.rs`. The CBOR and text codecs are expected
to map to that model rather than implement separate broker semantics.

The CBOR envelope is specified in [`CBOR.md`](CBOR.md) and implemented in
`src/cbor.rs`.

Stable protocol error codes are listed in [`ERRORS.md`](ERRORS.md).

Authentication negotiation is described in [`AUTH.md`](AUTH.md).

The authoritative wire reference is [`PROTOCOL.md`](PROTOCOL.md). Protocol
changes should update that document and add encoding and rejection tests.
