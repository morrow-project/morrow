# Protocol v1 evolution policy

Protocol v1 is the first semantic protocol contract. The current pre-1.0
command grammar may be replaced, but once the v1 model is published, changes
follow these rules.

## Compatible changes

The following changes are compatible when the old behavior remains valid:

- adding optional map fields;
- adding optional capabilities or encodings;
- adding new error detail fields;
- adding new headers outside the reserved broker namespace;
- increasing negotiated limits when the client opts into them.

Decoders must ignore unknown optional fields. Encoders must not emit a field
unless the peer advertised the capability that gives it meaning.

## Capability-gated changes

Changes that alter resource usage or operation semantics require a named
capability, including compression, transactions, server-side filtering, new
authentication mechanisms, and new delivery controls. A peer that does not
advertise the capability must receive a stable `UNSUPPORTED_ENCODING` or
`INVALID_REQUEST` error as appropriate.

## Version-breaking changes

A protocol version is required when a change removes a required field, changes
the meaning or type of an existing field, changes acknowledgement identity,
changes framing, or changes delivery ordering/durability guarantees. Version 2
must not be used as a silent fallback for a v1 semantic request.

Frame kinds and schema field numbers are reserved before implementation. A
removed field remains reserved and is never reused. CDDL, model types, codecs,
documentation, and conformance vectors change together.

## Compatibility matrix

| Peer | Text | CBOR | Unknown optional fields |
| --- | --- | --- | --- |
| v1 | supported | supported | ignored |
| future version | capability-negotiated | capability-negotiated | ignored when marked optional |

The text frontend is a diagnostic representation of the same semantic model;
it is not permitted to introduce behavior unavailable through CBOR.
