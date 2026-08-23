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
- Tenant quota accounting bounds connections, memory, disk, foreground tasks,
  and background tasks independently of global listener quotas.

## Operational threat model

The KMS boundary may be unavailable, throttled, or revoke an old key. Reads
must fail closed when a referenced key cannot be loaded; rotation is safe when
the new key is available, and old data remains readable until its version is
revoked. Operators should verify exported audit chains before shipping them to
an external audit stream and should treat a failed verification as an
integrity incident.

The current default bootstrap maps unauthenticated connection admission to the
`default` tenant. Authentication-to-tenant identity mapping and propagation of
tenant scope through every protocol/API, connector, middleware, storage, and
cluster endpoint remain required integration work before multi-tenant mode is
advertised as complete.
