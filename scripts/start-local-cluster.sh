#!/bin/sh
set -eu

usage() {
  echo "Usage: $0 --base-config FILE [options]"
  echo "  --base-config FILE    JSON config used as the node template"
  echo "  --server-bin FILE     morrow-server binary (default: target/release/morrow-server)"
  echo "  --controllers N        dedicated controller count (default: 3)"
  echo "  --brokers N            broker count (default: 2)"
  echo "  --work-dir DIR        generated configs and data (default: target/local-cluster)"
  echo "  --client-port N       first client port (default: 8001)"
  echo "  --raft-port N         first Raft port (default: 9001)"
  echo "  --route-port N        first route port (default: 10001)"
  echo "  --http-port N         first admin HTTP port (default: 11001)"
  echo "  --keep-running        leave processes running after the script exits"
}

die() { echo "error: $*" >&2; exit 1; }

base_config=
server_bin=target/release/morrow-server
controllers=3
brokers=2
work_dir=target/local-cluster
client_port=8001
raft_port=9001
route_port=10001
http_port=11001
keep_running=false

while test "$#" -gt 0; do
  case "$1" in
    --base-config) test "$#" -ge 2 || die "$1 requires a value"; base_config=$2; shift 2 ;;
    --server-bin) test "$#" -ge 2 || die "$1 requires a value"; server_bin=$2; shift 2 ;;
    --controllers) test "$#" -ge 2 || die "$1 requires a value"; controllers=$2; shift 2 ;;
    --brokers) test "$#" -ge 2 || die "$1 requires a value"; brokers=$2; shift 2 ;;
    --work-dir) test "$#" -ge 2 || die "$1 requires a value"; work_dir=$2; shift 2 ;;
    --client-port) test "$#" -ge 2 || die "$1 requires a value"; client_port=$2; shift 2 ;;
    --raft-port) test "$#" -ge 2 || die "$1 requires a value"; raft_port=$2; shift 2 ;;
    --route-port) test "$#" -ge 2 || die "$1 requires a value"; route_port=$2; shift 2 ;;
    --http-port) test "$#" -ge 2 || die "$1 requires a value"; http_port=$2; shift 2 ;;
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
for port in "$client_port" "$raft_port" "$route_port" "$http_port"; do
  case "$port" in ''|*[!0-9]*|0) die "port bases must be positive integers" ;; esac
done

mkdir -p "$work_dir/config" "$work_dir/log"

python3 - "$base_config" "$work_dir/config" "$controllers" "$brokers" "$client_port" "$raft_port" "$route_port" "$http_port" <<'PY'
import json
import pathlib
import sys

base_path, output_dir, controller_count, broker_count, client_base, raft_base, route_base, http_base = sys.argv[1:]
controller_count = int(controller_count)
broker_count = int(broker_count)
client_base = int(client_base)
raft_base = int(raft_base)
route_base = int(route_base)
http_base = int(http_base)
base = json.loads(pathlib.Path(base_path).read_text())
total = controller_count + broker_count
nodes = []
for node_id in range(1, total + 1):
    is_controller = node_id <= controller_count
    client_port = client_base + node_id - 1
    raft_port = raft_base + node_id - 1
    node = {
        "node_id": node_id,
        "raft_addr": f"127.0.0.1:{raft_port}",
        "client_addr": f"127.0.0.1:{client_port}",
    }
    if not is_controller:
        node["route_addr"] = f"127.0.0.1:{route_base + node_id - 1}"
    nodes.append(node)

voters = list(range(1, controller_count + 1))
for node_id in range(1, total + 1):
    config = json.loads(json.dumps(base))
    is_controller = node_id <= controller_count
    config["listen"] = f"127.0.0.1:{client_base + node_id - 1}"
    config["http_listen"] = f"127.0.0.1:{http_base + node_id - 1}"
    config["wal_dir"] = str(pathlib.Path(output_dir).parent / f"node-{node_id}" / "wal")
    cluster = config.setdefault("cluster", {})
    cluster.update({
        "enabled": True,
        "role": "controller" if is_controller else "broker",
        "node_id": node_id,
        "raft_listen": f"127.0.0.1:{raft_base + node_id - 1}",
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
        route = f"127.0.0.1:{route_base + node_id - 1}"
        cluster["route_listen"] = route
        cluster["route_advertise"] = route
        cluster["routes"] = [
            f"127.0.0.1:{route_base + peer_id - 1}"
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
