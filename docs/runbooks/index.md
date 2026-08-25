# Morrow operations runbooks

These runbooks are written for locked-down deployments. Start with the remote
admin checks below; node-local checks are optional and must not be treated as a
prerequisite. Do not paste bearer tokens, private keys, or credential material
into tickets or shell history.

| Incident | Runbook |
| --- | --- |
| Metadata quorum loss | [quorum-loss.md](quorum-loss.md) |
| No safe partition replica | [no-safe-replica.md](no-safe-replica.md) |
| Storage corruption | [storage-corruption.md](storage-corruption.md) |
| Disk or inode exhaustion | [disk-full.md](disk-full.md) |
| Certificate rotation | [certificate-rotation.md](certificate-rotation.md) |
| Credential compromise | [credential-compromise.md](credential-compromise.md) |
| Backup verification and restore | [backup-restore.md](backup-restore.md) |
| Cluster node replacement | [node-replacement.md](node-replacement.md) |
| Upgrade rollback | [upgrade-rollback.md](upgrade-rollback.md) |

## Common remote checks

```sh
curl -fsS https://BROKER/api/v1/health/ready
curl -fsS -H 'Authorization: Bearer REDACTED' https://BROKER/api/v1/cluster
curl -fsS -H 'Authorization: Bearer REDACTED' https://BROKER/api/v1/routes
curl -fsS -H 'Authorization: Bearer REDACTED' https://BROKER/api/v1/storage
curl -fsS -H 'Authorization: Bearer REDACTED' https://BROKER/api/v1/metrics
```

Replace `BROKER` and `REDACTED` out of band. Record UTC timestamps, broker
version, node IDs, and response bodies with secrets removed. Each runbook must
be exercised in a staging/tabletop environment and its validation date and
observed recovery time recorded in the incident ticket. Review this index and
the linked procedures quarterly, owned by the on-call platform team.
