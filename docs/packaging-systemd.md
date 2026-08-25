# Packaged systemd service

The Debian and RPM builders install the same unit from
`packaging/systemd/morrow.service`. Keeping one source makes sandboxing,
shutdown, restart, and resource behavior identical across package formats.

## Resource sizing

The unit starts with `LimitNOFILE=65536`. Size an override from the larger of
the expected client connection count and the broker's durable resource fanout:

```text
LimitNOFILE >= client_connections
                  + 2 * partition_count
                  + 2 * cluster_peer_connections
                  + safety_margin
```

Count TLS, WebSocket, HTTP, Raft, and route sockets separately when enabled.
Add headroom for rolling restarts and descriptors used by WAL segments,
partition logs, certificates, and middleware. Verify the effective value with:

```sh
systemctl show morrow.service -p LimitNOFILE
systemctl cat morrow.service
```

For a larger deployment, place an administrator-owned drop-in at
`/etc/systemd/system/morrow.service.d/limits.conf`:

```ini
[Service]
LimitNOFILE=131072
```

The state directory is `/var/lib/morrow`; override storage placement only when
the replacement path is persistent and owned by `morrow`:

```ini
[Service]
StateDirectory=
ReadWritePaths=/srv/morrow
WorkingDirectory=/srv/morrow
```

Keep `/etc/morrow/morrow.json`, certificate files, trust roots, and secret
files readable by the service but not writable by it. The unit uses a strict
read-only system view and grants write access only to its state and runtime
directories.

## Shutdown and hardening

The broker handles SIGTERM as a graceful shutdown signal: it stops readiness,
rejects new client work, flushes partition logs, checkpoints durable state,
and flushes the WAL. `TimeoutStopSec=120s` gives those operations a bounded
window; the unit then sends SIGKILL if the process does not exit. A clean exit
is reported with `SuccessExitStatus=0`; unexpected exits restart after five
seconds, with five starts allowed in a 60-second interval.

The unit restricts capabilities, devices, namespaces, address families,
kernel interfaces, personality changes, realtime scheduling, and set-ID
transitions. It permits IPv4, IPv6, and Unix sockets because those are the
supported listener families. `SystemCallFilter` is deliberately not enabled:
the broker's Wasmtime JIT and supported middleware may need platform-specific
memory and syscall behavior that a generic allowlist would break. Deployments
that do not use Wasmtime may test a locally maintained syscall allowlist in a
staging environment.

## Validation and smoke test

Run the package-level unit checks from the repository root:

```sh
packaging/tests/systemd-unit-test.sh
```

On a host with systemd, the script runs `systemd-analyze verify` and records
`systemd-analyze security` output. For a disposable package-install test,
provide a built package and configuration:

```sh
MORROW_PACKAGE=/tmp/morrow.deb \
MORROW_CONFIG=/tmp/morrow.json \
  packaging/tests/systemd-package-smoke.sh
```

The smoke test installs the package, copies the supplied configuration without
overwriting it, starts the unit, checks `/health/ready`, stops and restarts the
unit, verifies configuration and state remain present, and removes the
package. It requires root and a disposable systemd host.
