# Protocol crate

The `protocol` crate defines Morrow's wire-level behavior without opening
network connections. It provides command parsing, payload framing,
`MORROW/1.0` header parsing, server frames, subject validation, wildcard
matching, authentication helpers, and connector-control subjects.

The authoritative wire reference is [`PROTOCOL.md`](PROTOCOL.md). Protocol
changes should update that document and add encoding and rejection tests.
