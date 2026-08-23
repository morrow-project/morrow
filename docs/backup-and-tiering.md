# Backup and tiered storage

The server backup primitives use a versioned `BackupManifest` as the recovery-point
publication fence. Callers must flush the broker before creating a backup. The
backup copies stream segments, sparse/subject indexes, and WAL files with a stable
read check and records a SHA-256 digest for every object. Active files are copied
for recovery but are never eligible for local eviction.

`ObjectStore` is the storage boundary for an S3-compatible implementation. Object
keys are immutable: a retry is accepted only when the existing bytes match the
requested checksum. The manifest is published after all data objects, so a partial
upload cannot authorize eviction. Credentials, endpoints, and encryption settings
belong to the object-store implementation and are not represented in manifests.
If an upload fails after some objects are written, `cleanup_orphans` removes only
unreferenced objects below that backup's prefix and preserves both the published
manifest and objects referenced by it.

Restore validates the manifest, downloads and verifies every object into a staging
directory, and publishes the directory only after all files pass size and checksum
validation. A different cluster identity is required. Repeating an identical
restore is safe; a conflicting destination is rejected.

Incremental backups contain only files whose digest changed since their parent.
Restore them with the ordered full-to-incremental chain; the chain must have one
source cluster, an unbroken parent link at every step, and a full manifest at its
root. Restoring an incremental manifest by itself is rejected.

Sealed stream files can be tiered with `BackupEngine::evict_sealed`. Each remote
object is fetched and checksum-verified immediately before its local copy is
removed. Reads of evicted segments should use `RemoteSegmentCache`, which bounds
resident bytes and coalesces concurrent requests for the same object.

The current API is intentionally storage-provider-neutral. An S3 adapter should
map `put_immutable`, `get`, and `delete` to conditional/object-versioned requests,
use multipart uploads for large objects, retry idempotently, and expose request
quotas and latency metrics without logging credentials or object authorization
headers.
