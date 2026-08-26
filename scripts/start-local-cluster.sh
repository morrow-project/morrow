#!/bin/sh
set -eu

usage() {
  echo "Usage: $0 --base-config FILE [options]"
  echo "  --base-config FILE    JSON config used as the node template"
  echo "  --server-bin FILE     morrow-server binary (default: target/release/morrow-server)"
  echo "  --controllers N        dedicated controller count (default: 3)"
  echo "  --brokers N            broker count (default: 2)"
  echo "  --work-dir DIR        generated configs and data (default: target/local-cluster)"
  echo "  --keep-running        leave processes running after the script exits"
}

die() { echo "error: $*" >&2; exit 1; }

base_config=
server_bin=target/release/morrow-server
controllers=3
brokers=2
work_dir=target/local-cluster
keep_running=false

while test "$#" -gt 0; do
  case "$1" in
    --base-config) test "$#" -ge 2 || die "$1 requires a value"; base_config=$2; shift 2 ;;
    --server-bin) test "$#" -ge 2 || die "$1 requires a value"; server_bin=$2; shift 2 ;;
    --controllers) test "$#" -ge 2 || die "$1 requires a value"; controllers=$2; shift 2 ;;
    --brokers) test "$#" -ge 2 || die "$1 requires a value"; brokers=$2; shift 2 ;;
    --work-dir) test "$#" -ge 2 || die "$1 requires a value"; work_dir=$2; shift 2 ;;
    --keep-running) keep_running=true; shift ;;
    -h|--help) usage; exit 0 ;;
    *) die "unknown option $1" ;;
  esac
done

test -n "$base_config" || die "--base-config is required"
test -f "$base_config" || die "base config does not exist: $base_config"
test -x "$server_bin" || die "server binary is not executable: $server_bin"
case "$controllers" in ''|*[!0-9]*|0) die "--controllers must be a positive integer" ;; esac
case "$brokers" in ''|*[!0-9]*|0) die "--brokers must be a positive integer" ;; esac

mkdir -p "$work_dir/config" "$work_dir/log"

python3 - "$base_config" "$work_dir/config" "$controllers" "$brokers" <<'PY'
import json
import pathlib
import sys

base_path, output_dir, controller_count, broker_count = sys.argv[1:]
controller_count = int(controller_count)
broker_count = int(broker_count)
base = json.loads(pathlib.Path(base_path).read_text())
total = controller_count + broker_count
nodes = []
for node_id in range(1, total + 1):
    is_controller = node_id <= controller_count
    client_port = 8000 + node_id
    raft_port = 9000 + node_id
    node = {
        "node_id": node_id,
        "raft_addr": f"127.0.0.1:{raft_port}",
        "client_addr": f"127.0.0.1:{client_port}",
    }
    if not is_controller:
        node["route_addr"] = f"127.0.0.1:{10000 + node_id}"
    nodes.append(node)

voters = list(range(1, controller_count + 1))
for node_id in range(1, total + 1):
    config = json.loads(json.dumps(base))
    is_controller = node_id <= controller_count
    config["listen"] = f"127.0.0.1:{8000 + node_id}"
    config["http_listen"] = f"127.0.0.1:{11000 + node_id}"
    config["wal_dir"] = str(pathlib.Path(output_dir).parent / f"node-{node_id}" / "wal")
    cluster = config.setdefault("cluster", {})
    cluster.update({
        "enabled": True,
        "role": "controller" if is_controller else "broker",
        "node_id": node_id,
        "raft_listen": f"127.0.0.1:{9000 + node_id}",
        "raft_dir": str(pathlib.Path(output_dir).parent / f"node-{node_id}" / "raft"),
        "bootstrap": node_id == 1,
        "nodes": nodes,
        "controller_voters": voters,
    })
    if is_controller:
        cluster.pop("route_listen", None)
        cluster.pop("route_advertise", None)
        cluster["routes"] = []
    else:
        route = f"127.0.0.1:{10000 + node_id}"
        cluster["route_listen"] = route
        cluster["route_advertise"] = route
        cluster["routes"] = [
            f"127.0.0.1:{10000 + peer_id}"
            for peer_id in range(controller_count + 1, total + 1)
            if peer_id != node_id
        ]
    path = pathlib.Path(output_dir) / f"node-{node_id}.json"
    path.write_text(json.dumps(config, indent=2) + "\n")
PY

pids=
cleanup() {
  test "$keep_running" = true && return 0
  for pid in $pids; do kill "$pid" 2>/dev/null || true; done
}
trap cleanup EXIT INT TERM

for config in "$work_dir"/config/node-*.json; do
  node=$(basename "$config" .json)
  "$server_bin" "$config" >"$work_dir/log/$node.log" 2>&1 &
  pids="$pids $!"
done

echo "started $((controllers + brokers)) Morrow processes"
echo "controllers: $controllers, brokers: $brokers"
echo "configs: $work_dir/config"
echo "logs: $work_dir/log"
echo "pids:$pids"

if test "$keep_running" = true; then
  trap - EXIT INT TERM
  exit 0
fi

while :; do
  sleep 1
done
