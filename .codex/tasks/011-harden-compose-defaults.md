# Task 011: Harden Compose deployment defaults

## Goal

Make the checked-in Docker Compose cluster safe to start on a developer machine
without unintentionally exposing an unauthenticated plaintext broker.

## Dependencies

- None.

## Scope

- Bind published client and admin ports to loopback by default.
- Remove committed placeholder admin and cluster credentials.
- Source required secrets from environment variables or mounted secret files and
  fail clearly when they are absent.
- Enable client authentication and TLS in a secure example, or separate an
  explicitly labeled local-only development profile.
- Document host-visible and internal-only ports.

## Required invariants

- Default Compose startup does not expose unauthenticated services beyond localhost.
- No reusable credential is committed in configuration.
- All nodes use the same supplied cluster credential without printing it.

## Acceptance criteria

- `docker compose config` contains no placeholder secret.
- Host port inspection shows only loopback bindings by default.
- Unauthorized client and admin requests are rejected.
- A documented local startup path brings up all three nodes.

## Verification

```bash
docker compose config
docker compose up --build -d
docker compose ps
cargo test --workspace
git diff --check
```
