#!/usr/bin/env bash
set -euo pipefail

: "${MORROW_PACKAGE:?set MORROW_PACKAGE to a disposable .deb or .rpm}"
: "${MORROW_CONFIG:?set MORROW_CONFIG to a test configuration}"

if [[ "$(id -u)" -ne 0 ]]; then
  echo "systemd package smoke test requires root on a disposable host" >&2
  exit 2
fi
command -v systemctl >/dev/null
command -v curl >/dev/null

case "${MORROW_PACKAGE}" in
  *.deb) dpkg --unpack "${MORROW_PACKAGE}"; dpkg --configure morrow ;;
  *.rpm) rpm -U --replacepkgs "${MORROW_PACKAGE}" ;;
  *) echo "MORROW_PACKAGE must end in .deb or .rpm" >&2; exit 2 ;;
esac

install -o morrow -g morrow -m 0640 "${MORROW_CONFIG}" /etc/morrow/morrow.json
systemctl daemon-reload
systemctl enable --now morrow.service
trap 'systemctl disable --now morrow.service || true' EXIT

for _ in {1..30}; do
  if curl --fail --silent http://127.0.0.1:8222/health/ready >/dev/null; then
    break
  fi
  sleep 1
done
curl --fail --silent http://127.0.0.1:8222/health/ready >/dev/null
systemctl stop morrow.service
systemctl start morrow.service
test -s /etc/morrow/morrow.json
test -d /var/lib/morrow

systemctl disable --now morrow.service
case "${MORROW_PACKAGE}" in
  *.deb) dpkg --purge morrow ;;
  *.rpm) rpm -e morrow ;;
esac
test -s /etc/morrow/morrow.json
test -d /var/lib/morrow
