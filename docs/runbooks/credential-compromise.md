# Client, cluster, or admin credential compromise

Treat suspected disclosure as active compromise. Record the time window,
identity, scopes, and affected tenants without copying the secret.

Containment: revoke or disable the credential at its issuer, rotate the
admin/cluster/client secret through the approved secret manager, and fence any
possibly promoted or stale node. Do not rely on a process restart alone.

Diagnosis: use audit export/verification, authorization-denial metrics,
cluster/routes status, and provider logs. Remote checks are preferred; secrets
are never requested from operators through chat or tickets.

Recovery: issue least-privilege replacements, update one transport at a time,
and restore traffic only after audit verification and tenant policy review.

Verification: old credentials fail, new canary credentials work only in their
scope, no unexpected leader or route remains, and audit integrity verifies.
Escalate to security and legal response owners when data access is plausible.
