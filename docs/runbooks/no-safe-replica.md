# No safe partition replica

Symptoms: a partition has no eligible leader, publishes fail with quorum or
leadership errors, or cluster status shows a missing/stale replica.

Safety: do not force leadership to a stale replica and do not remove the last
copy. Freeze reassignment and destructive retention changes.

Diagnosis: use `/api/v1/cluster`, `/api/v1/streams`, and `/api/v1/storage` to
record assignment, leader epoch, replica, and recovery status. Compare the
partition's committed offset with replica progress.

Containment: keep the partition unavailable rather than accepting divergent
writes. Restore the failed source or destination node if possible.

Recovery: add or restore a replica, allow it to catch up, verify quorum and
checksum status, then transfer leadership only through the reassignment
workflow. Roll back an uncommitted move if the destination cannot catch up.

Verification: the partition has a current leader epoch, quorum acknowledgements
work, and a canary read matches the last committed record. Escalate if no
verified copy exists; recovery then requires the backup/restore procedure.
