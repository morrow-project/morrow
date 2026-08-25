# Disk full and inode exhaustion

Symptoms: storage errors, rising filesystem usage, failed WAL/partition writes,
or unavailable audit/checkpoint persistence.

Safety: do not delete WAL, partition logs, audit history, or backups manually.
Stop rollouts and reduce nonessential traffic while preserving evidence.

Diagnosis: use `/api/v1/storage`, `/api/v1/metrics`, and readiness remotely.
Node-local filesystem/inode checks are optional and must use the deployment's
approved read-only mechanism.

Containment: pause nonessential consumers or retention-insensitive workloads;
do not lower retention blindly. Expand capacity or attach approved storage.

Recovery: after capacity is available, verify the storage endpoint, flush a
canary record, and restart only one node at a time if required.

Verification: writes, audit append, checkpoints, and backup creation succeed;
usage and inode alerts clear. Escalate if capacity cannot be expanded without
deleting durable evidence.
