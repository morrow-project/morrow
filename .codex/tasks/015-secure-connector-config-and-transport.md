# Task 015: Secure connector configuration and transport

## Goal

Keep connector secrets out of durable control streams and support authenticated,
encrypted broker connections.

## Dependencies

- [Task 008: Add the programmable plane and connectors](008-programmable-plane-and-connectors.md).

## Scope

- Publish a redacted, versioned descriptor instead of raw configuration bytes.
- Represent credentials as external secret references resolved only by the connector.
- Add connector options for TLS, server-name verification, and client authentication.
- Validate permissions on local secret and key material.
- Prevent errors and status records from echoing secrets.

## Required invariants

- Durable streams never contain connector secret contents.
- Production traffic authenticates both broker and connector identity.
- Redaction remains safe when new configuration fields are added.

## Acceptance criteria

- Tests inspect control records and prove configured secrets are absent.
- Authenticated TLS integration succeeds and invalid credentials fail.
- Non-secret target settings remain available for reconciliation.
- Migration for previously stored raw configs is documented.

## Verification

```bash
cargo test -p connector
cargo test -p integration
cargo test --workspace
git diff --check
```
