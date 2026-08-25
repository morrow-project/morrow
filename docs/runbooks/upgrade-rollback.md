# Upgrade rollback and incompatible storage

Symptoms: startup rejects configuration/storage, readiness fails after an
upgrade, or protocol/client compatibility errors rise.

Safety: stop the rollout; do not downgrade over a storage format that the old
binary cannot read. Preserve logs, manifests, and the current data directory.

Diagnosis: collect version, health, storage, cluster, and metrics responses.
Compare the release's documented storage/protocol compatibility and inspect
logs only through approved node-local access if available.

Recovery: if the format is compatible, roll back one node at a time and verify
readiness. If incompatible, restore a verified backup into a new cluster
identity or follow the release migration procedure; never force-open data.

Verification: cluster quorum, storage recovery, audit verification, and canary
publish/consume all pass. Escalate when rollback would risk data loss or mixed
versions cannot be isolated.
