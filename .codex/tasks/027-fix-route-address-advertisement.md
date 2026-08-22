# Task 027: Separate route bind and advertised addresses

## Goal

Make the Docker cluster form a functioning route mesh when listeners bind to
wildcard or shared container ports.

## Dependencies

- [Task 007: Add routing and subject indexes](007-routing-and-subject-indexes.md).

## Scope

- Represent route bind and advertised addresses separately.
- Require a routable advertised hostname or derive it from cluster node metadata.
- Compare peer identity primarily by node ID; do not reject distinct nodes merely
  because wildcard bind addresses are equal.
- Validate self-address, duplicate-node, and duplicate-advertisement conflicts.
- Update Compose without restoring static container IPs.

## Required invariants

- Wildcard addresses are bind targets, never routable advertised identities.
- A node never establishes a route to itself.
- Distinct nodes using the same port form a full mesh through DNS hostnames.

## Acceptance criteria

- All three Compose nodes report two connected peers and discovered topology.
- Transient publish and request/reply cross every pair of nodes.
- Restarting one node restores the mesh without static IP configuration.
- Integration tests cover equal wildcard binds with distinct advertised hostnames.

## Verification

```bash
cargo test -p server
cargo test -p integration
docker compose up --build -d
docker compose ps
cargo test --workspace
git diff --check
```
