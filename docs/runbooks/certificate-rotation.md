# Client, admin, route, and Raft certificate rotation

Symptoms: TLS handshake failures, route/raft peer disconnects, or clients
rejecting the presented certificate.

Prerequisites: approved certificates and trust chain, expiry window, rollback
copy, and an owner for each transport. Never put private keys in tickets.

Diagnosis: query health/cluster/routes and inspect TLS alert metrics. Optional
node-local checks may confirm the configured file and permissions.

Recovery: stage the new certificate and trust chain, reload or restart one
transport/node at a time according to deployment procedure, and keep the old
trust chain during overlap. Rotate client, admin, route, and Raft trust
domains independently when required.

Verification: remote health and route/cluster status remain healthy, then test
one canary client and admin request. Roll back to the prior certificate only
if the documented overlap is still valid; otherwise escalate to security.
