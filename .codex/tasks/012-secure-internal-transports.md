# Task 012: Secure internal cluster and admin transports

## Goal

Provide confidentiality, peer identity, and replay resistance for Raft, route,
and administrative traffic.

## Dependencies

- None.

## Scope

- Add separate TLS configuration for Raft, route-mesh, and admin listeners.
- Require authenticated peer identities, preferably mutual TLS with node-ID binding.
- Stop sending reusable bearer tokens over plaintext sockets.
- Define token rotation or certificate rollover without cluster-wide downtime.
- Preserve frame-size and read-timeout protections after transport wrapping.

## Required invariants

- A peer cannot authenticate as a different configured node.
- Captured traffic cannot reveal a reusable cluster or admin credential.
- TLS verification fails closed for an unknown CA, hostname, or node identity.

## Acceptance criteria

- Integration tests cover trusted peers, unknown peers, wrong node identities,
  expired credentials, and admin TLS.
- Plaintext connections cannot complete a protected internal protocol.
- Deployment documentation includes certificate generation and rotation.

## Verification

```bash
cargo test -p server
cargo test -p integration
cargo test --workspace
git diff --check
```
