# Metadata quorum loss

Symptoms and alerts: readiness reports a non-ready cluster, `/cluster` has no
leader or insufficient peers, and Raft/cluster error counters increase.

Safety: do not restart multiple nodes, delete Raft data, or promote a standby
while the authoritative quorum may still be alive. In standalone mode, treat
this as a local storage/process incident rather than a quorum incident.

Prerequisites: admin access and an incident owner. Run the common remote checks
from the index, then compare peer IDs, roles, and last known leader.

Containment: stop automated rollouts and writes that are not required for
recovery. If a leader is known, restore network reachability to a majority.
Use node-local process/log checks only if remote evidence cannot distinguish a
crash from a network partition.

Recovery: restore the minimum number of failed nodes, one at a time, and wait
for readiness after each. Never copy Raft state between node identities.

Verification: readiness is `ready`, `/cluster` reports a stable leader and
expected peers, and a canary publish/consume succeeds. Escalate when quorum
cannot reform without data deletion or when peer identities disagree.

Validation record: staging/tabletop date, Morrow version, and recovery time are
required in the incident ticket.
