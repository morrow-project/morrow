# Protocol v1 authentication

Authentication is negotiated independently from the wire encoding. A server
advertises supported mechanisms in its challenge; the client returns a typed
proof containing the mechanism, client identity, proof bytes, and optional
channel-binding bytes.

The initial mechanism is `ed25519`. Proofs and nonces are binary values in the
CBOR model and must not be rendered into human-readable errors or logs.
Authentication, authorization, and TLS failures use separate error codes.

The challenge and proof model is intentionally extensible so a future
mechanism can be added without changing message or delivery semantics.
