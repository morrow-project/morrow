# Backup verification and restore

Prerequisites: backup manifest, checksum, encryption key versions, target
capacity, and a recovery cluster with a different cluster identity.

Diagnosis: verify the manifest and every artifact checksum before touching the
target. Confirm the required key versions are available through the KMS/key
provider; do not paste keys into commands.

Safety: restore into an isolated target first. Never overwrite the source
cluster or reuse its Raft identity. Preserve the original backup and failed
restore attempt.

Recovery: restore the verified point, configure a new cluster identity, start
one node, validate storage/health, then add peers and traffic gradually.

Verification: audit chain verification succeeds, streams and consumer cursors
match the recovery point, encryption reads succeed, and canary publish/consume
works. Record RPO, RTO, version, and validation date; escalate on checksum or
key-version mismatch.
