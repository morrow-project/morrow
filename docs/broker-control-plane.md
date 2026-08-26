# Broker control plane

Morrow’s controller quorum uses a small, versioned control stream to track
data-plane brokers. A broker registers its stable ID together with a new
incarnation, client and replication endpoints, bounded capacity summary,
feature gates, and security references. Registration returns a session ID and
the controller revision from which the broker can resume.

Heartbeats carry the session and incarnation, so a restarted process fences the
old connection. A heartbeat for an older incarnation or session is rejected.
This keeps stale brokers from accepting new assignments without adding them to
the fixed OpenRaft voter set.

Metadata changes are numbered revisions. Each update carries a CRC32 checksum;
brokers may request updates after a known revision. Controllers retain only a
bounded update window. If a broker falls behind that window, registration or
resume reports `snapshot_required`, allowing an atomic snapshot instead of an
unbounded replay or a full-cluster resynchronization.

The wire format is `protocol::broker_control::BrokerControlFrame`: a four-byte
big-endian length followed by a CBOR frame. The protocol version is currently
`1`; unknown versions must be rejected. The registry and codec are intentionally
usable by combined nodes as well as dedicated broker-only processes.
