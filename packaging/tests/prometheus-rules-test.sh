#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
rules="${repo_root}/monitoring/prometheus/morrow-alerts.yml"
tests="${repo_root}/monitoring/prometheus/morrow-alerts.test.yml"

test -r "${rules}"
test -r "${tests}"
grep -q '^groups:' "${rules}"
grep -q '^rule_files:' "${tests}"

if command -v promtool >/dev/null 2>&1; then
  promtool check rules "${rules}"
  promtool test rules "${tests}"
else
  echo "promtool unavailable; Prometheus rule fixtures were found" >&2
fi
