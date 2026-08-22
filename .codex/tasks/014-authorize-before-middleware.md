# Task 014: Authorize publications before middleware execution

## Goal

Prevent unauthorized subjects from invoking programmable middleware or its host
capabilities.

## Dependencies

- [Task 008: Add the programmable plane and connectors](008-programmable-plane-and-connectors.md).

## Scope

- Authorize the original subject before ingress middleware executes.
- Revalidate reserved-subject rules and authorize the final transformed subject.
- Apply equivalent authorization to every emitted publication.
- Define any trusted authority allowed to cross an authorization boundary.
- Audit denied attempts without logging payloads or credentials.

## Required invariants

- A client cannot consume middleware fuel or capabilities for a denied subject.
- Subject mutation never grants permissions the publisher did not hold.
- Emitted messages follow the same authorization rules as direct publications.

## Acceptance criteria

- Tests prove unauthorized publishes do not execute middleware.
- Tests cover allowed original subjects transformed to denied or reserved subjects.
- Cluster followers and direct leaders enforce identical ordering.

## Verification

```bash
cargo test -p server
cargo test -p integration
cargo test --workspace
git diff --check
```
