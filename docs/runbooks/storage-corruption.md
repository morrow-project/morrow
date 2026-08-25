# WAL, partition-log, or Raft storage corruption

Symptoms: readiness is degraded, storage recovery reports checksum/segment
errors, or writes fail with corruption messages.

Safety: stop the affected node's writes, preserve the original storage, and
do not run repair tools in place. Never discard a segment before a verified
backup or forensic copy exists.

Diagnosis: collect `/api/v1/health/ready`, `/api/v1/storage`, `/api/v1/cluster`,
and metrics. Optional node-local checks may inspect process logs and filesystem
checksums; shell access is not assumed.

Containment: fence the node from leadership and routes if the cluster can
serve safely elsewhere. In standalone mode, take an immutable copy and declare
the service read/write unavailable.

Recovery: replace the node from a verified backup or rebuild it with a new
cluster identity as appropriate. Restore only after checksum and manifest
verification; never reuse a stale Raft identity.

Verification: all recovery endpoints are healthy, checksums validate, and a
canary publish/consume plus restart succeeds. Escalate on any mismatch or
uncertain point-in-time boundary.
