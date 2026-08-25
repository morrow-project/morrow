#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
unit="${repo_root}/packaging/systemd/morrow.service"

test -r "${unit}"
grep -q '^KillSignal=SIGTERM$' "${unit}"
grep -q '^TimeoutStopSec=120s$' "${unit}"
grep -q '^LimitNOFILE=65536$' "${unit}"
grep -q '^ProtectSystem=strict$' "${unit}"
grep -q '^ProtectHome=read-only$' "${unit}"
grep -q '^PrivateDevices=true$' "${unit}"
grep -q '^RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6$' "${unit}"
grep -q '^ReadWritePaths=/var/lib/morrow /run/morrow$' "${unit}"
grep -q '^MemoryDenyWriteExecute=false$' "${unit}"
grep -q 'morrow.service' "${repo_root}/packaging/debian/build-deb.sh"
grep -q 'morrow.service' "${repo_root}/packaging/rpm/build-rpm.sh"

if command -v systemd-analyze >/dev/null 2>&1; then
  systemd-analyze verify "${unit}"
  security_output="${repo_root}/target/morrow-systemd-security.txt"
  mkdir -p "$(dirname "${security_output}")"
  systemd-analyze security "${unit}" >"${security_output}" || true
  echo "recorded ${security_output}"
else
  echo "systemd-analyze unavailable; static unit checks passed" >&2
fi
