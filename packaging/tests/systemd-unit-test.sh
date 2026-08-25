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
  verify_root="$(mktemp -d)"
  trap 'rm -rf "${verify_root}"' EXIT
  mkdir -p "${verify_root}/etc/systemd/system" \
    "${verify_root}/usr/lib/systemd/system" \
    "${verify_root}/etc/morrow" \
    "${verify_root}/etc/ssl" \
    "${verify_root}/etc/pki" \
    "${verify_root}/usr/bin" \
    "${verify_root}/var/lib/morrow" \
    "${verify_root}/run/morrow"
  cp "${unit}" "${verify_root}/etc/systemd/system/morrow.service"
  printf '[Unit]\nDescription=network target\n' \
    >"${verify_root}/usr/lib/systemd/system/network.target"
  printf '[Unit]\nDescription=network online target\nAfter=network.target\n' \
    >"${verify_root}/usr/lib/systemd/system/network-online.target"
  printf '[Unit]\nDescription=system initialization target\n' \
    >"${verify_root}/usr/lib/systemd/system/sysinit.target"
  cp /usr/bin/test "${verify_root}/usr/bin/test"
  touch "${verify_root}/usr/bin/morrow-server" "${verify_root}/etc/morrow/morrow.json"
  chmod 0755 "${verify_root}/usr/bin/morrow-server"
  printf 'root:x:0:0:root:/root:/bin/sh\nmorrow:x:999:999::/var/lib/morrow:/usr/sbin/nologin\n' \
    >"${verify_root}/etc/passwd"
  printf 'root:x:0:\nmorrow:x:999:\n' >"${verify_root}/etc/group"
  security_output="${repo_root}/target/morrow-systemd-security.txt"
  mkdir -p "$(dirname "${security_output}")"
  verify_output="${security_output%.txt}-verify.txt"
  if ! systemd-analyze --root="${verify_root}" verify morrow.service \
      >"${verify_output}" 2>&1; then
    cat "${verify_output}" >&2
    exit 1
  fi
  systemd-analyze security "${unit}" >"${security_output}" || true
  echo "recorded ${verify_output} and ${security_output}"
else
  echo "systemd-analyze unavailable; static unit checks passed" >&2
fi
