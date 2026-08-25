# Multi-tenancy, RBAC, encryption, and audit model

Morrow evaluates the dynamic policy before a publish or subscribe operation is
allowed to reach subject-pattern authorization, middleware, connector, or
storage work. Policy bindings are scoped by tenant and namespace, have an
optional expiry, and are read on every authorization decision so revocation
does not require a restart.

## Security invariants

- A request must be authorized before any tenant-owned operation is scheduled.
- Tenant and namespace identifiers are bounded and restricted to a stable
  identifier alphabet.
- Authorization failures do not reveal another tenant's resources, names, or
  existence.
- Key material is supplied through a `KeyProvider`; it is not serialized,
  logged, or included in an encrypted envelope's debug output.
- Envelopes authenticate both ciphertext and caller-provided storage context
  (AAD). A key version is carried with each envelope so rotation does not
  synchronously rewrite old data.
- Audit records form an append-only SHA-256 chain. Verification checks sequence,
  links, and event hashes, detecting deletion, modification, insertion, and
  reordering.
- Tenant quota configuration defines independent connection, memory, disk,
  foreground-task, and background-task budgets. Connection admission and
  authenticated-tenant transfer are enforced today; the other dimensions are
  reported as reserved budget until their owning allocation paths opt in.

## Operational threat model

The KMS boundary may be unavailable, throttled, or revoke an old key. Reads
must fail closed when a referenced key cannot be loaded; rotation is safe when
the new key is available, and old data remains readable until its version is
revoked. Operators should verify exported audit chains before shipping them to
an external audit stream and should treat a failed verification as an
integrity incident.

The current default bootstrap maps unauthenticated connection admission to the
`default` tenant. Authenticated clients can declare a bounded tenant,
namespace, external subject, and expiry in configuration; mixed-tenant mode
requires the tenant/namespace subject prefix. To enable storage encryption in
the broker, set `encryption_key_dir` to a directory containing exact
32-byte `key-<version>.bin` files and optionally set
`encryption_active_key_version`; startup then passes one key ring to both WAL
and partition-log recovery and append paths. The key directory contains key
material and must be protected separately from the broker configuration.

Tenant quota usage is node-local. It is not a cluster-wide aggregate unless a
deployment applies the same limits and aggregates usage externally; replicated
data does not make admission counters globally consistent.

Break-glass access is explicitly out of band: an operator must use a separate
identity, a tenant-scoped policy snapshot, and an audited change window. The
broker provides no implicit administrator backdoor. Recovery restores the
encrypted object chain, loads every referenced KMS key version, verifies the
audit chain, and only then re-enables tenant traffic.

## Isolation matrix

| Surface | Boundary enforced before work | Cross-tenant behavior |
| --- | --- | --- |
| PUB/SUB protocol | Dynamic permission plus tenant/namespace subject prefix | Deny before middleware, storage, or cluster scheduling |
| Durable consumers | Subscription authorization and scoped subject | No consumer state is created |
| Metrics | Aggregate-only counters with no tenant labels | No names or resource existence are exposed |
| Middleware/connector ingress | Authorization precedes ingress execution | Deny before plugin/connector invocation |
| Backup/object storage | Object-key AAD and encrypted artifact adapter | Wrong context fails AEAD authentication |
| Policy/audit administration | Monotonic snapshots and append-only chain | Unauthorized changes are denied and recorded |
