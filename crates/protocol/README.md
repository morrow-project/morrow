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

The machine-readable CDDL schema and SDK guidance are documented in
[`../docs/protocol-sdk-generation.md`](../docs/protocol-sdk-generation.md).

The model-backed debug frontend is implemented in `src/text.rs`; the original
command-oriented text grammar remains in `src/protocol.rs`.

Versioning and extension rules are documented in
[`../docs/protocol-evolution.md`](../docs/protocol-evolution.md).

Conformance, fuzzing, and benchmark commands are documented in
[`../docs/protocol-conformance.md`](../docs/protocol-conformance.md).

The authoritative wire reference is [`PROTOCOL.md`](PROTOCOL.md). Protocol
changes should update that document and add encoding and rejection tests.
