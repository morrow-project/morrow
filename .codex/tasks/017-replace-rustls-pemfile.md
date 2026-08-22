# Task 017: Replace the unmaintained rustls-pemfile dependency

## Goal

Remove the directly used crate covered by RUSTSEC-2025-0134 while preserving
strict certificate and private-key parsing.

## Dependencies

- None.

## Scope

- Select a maintained PEM parser compatible with the pinned Rustls API.
- Migrate server certificate/key loading and client CA loading.
- Preserve rejection of empty sets, unsupported keys, malformed PEM, and invalid
  trailing material.
- Remove `rustls-pemfile` from workspace and crate manifests.
- Document accepted certificate and key encodings.

## Required invariants

- Invalid or ambiguous key material fails closed.
- Client certificate validation and server key loading retain current behavior.
- No unmaintained replacement is introduced.

## Acceptance criteria

- Existing TLS integration tests pass with the replacement parser.
- Tests cover malformed, empty, multiple-key, PKCS#8, and supported legacy inputs.
- `cargo audit` no longer reports RUSTSEC-2025-0134.

## Verification

```bash
cargo test -p client
cargo test -p server
cargo test -p integration
cargo audit
cargo test --workspace
git diff --check
```
