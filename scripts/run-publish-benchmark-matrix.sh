#!/bin/sh
set -eu

usage() {
  cat <<'EOF'
Usage: scripts/run-publish-benchmark-matrix.sh [options]

Run the publish-mode/acknowledgement matrix and retain JSON, CSV, server logs,
and the effective fixture configuration for every case.

Options:
  --topology standalone|cluster|external
                                  Managed one-node fixture, managed three-node
                                  fixture, or an already-running deployment
                                  (default: standalone)
  --modes LIST                    Comma-separated modes (default:
                                  fire-and-forget,sync,async,batch)
  --ack-levels LIST               Comma-separated acknowledgement levels
                                  (default: accepted,durable,high-durability;
                                  cluster also includes cluster-durable)
  --clients N                     Publisher connections (default: 5)
  --duration D                    Measured duration per case (default: 10s)
  --warmup D                      Unmeasured warm-up per case (default: 0s)
  --payload-size N                Application payload bytes (default: 1024)
  --throughput N                  Aggregate rate limit; 0 is unlimited
                                  (default: 0)
  --subjects N                    Number of generated subjects (default: 1)
  --partitions N                  Partitions in managed fixtures (default: 1)
  --key-cardinality N             Number of generated routing keys (default: 0)
  --max-in-flight N               Async window per client (default: 256)
  --batch-size N                  Batch size per client (default: 100)
  --subject SUBJECT               Bound base subject (default: bench/publish)
  --output-dir DIR                Result directory (default: a timestamped
                                  directory under target/benchmarks)
  --client-config FILE            Client config for --topology external
  --server ADDRESS                Override the external client endpoint
  --cli-bin PATH                  CLI binary for external or prebuilt runs
  --server-bin PATH               Server binary for managed runs
  --no-build                      Do not build the release binaries
  --quiet                         Print case names, not complete JSON reports
  --keep-going                    Run the remaining cases after a failure, then
                                  return a non-zero status (default: fail fast)
  --include-unbound               Also run an unbound fire-and-forget baseline
                                  (default for standalone and cluster)
  --no-unbound                    Skip the unbound baseline
  -h, --help                      Show this help

Examples:
  scripts/run-publish-benchmark-matrix.sh --duration 60s --clients 5
  scripts/run-publish-benchmark-matrix.sh --topology cluster --duration 30s
  scripts/run-publish-benchmark-matrix.sh --topology external \
    --client-config ./client.json --server 192.0.2.10:4222 \
    --ack-levels durable,high-durability,cluster-durable --duration 60s
EOF
}

die() {
  echo "error: $*" >&2
  exit 1
}

invocation_dir=$(pwd)
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repo_dir=$(CDPATH= cd -- "$script_dir/.." && pwd)
cd "$repo_dir"

topology=standalone
modes=fire-and-forget,sync,async,batch
ack_levels=
clients=5
duration=10s
warmup=0s
payload_size=1024
throughput=0
subjects=1
partitions=1
key_cardinality=0
max_in_flight=256
batch_size=100
subject=bench/publish
output_dir=
client_config=
server_override=
cli_bin=
server_bin=
build=true
quiet=false
keep_going=false
include_unbound=true

while test "$#" -gt 0; do
  case "$1" in
    --topology)
      test "$#" -ge 2 || die "$1 requires a value"
      topology=$2
      shift 2
      ;;
    --modes)
      test "$#" -ge 2 || die "$1 requires a value"
      modes=$2
      shift 2
      ;;
    --ack-levels)
      test "$#" -ge 2 || die "$1 requires a value"
      ack_levels=$2
      shift 2
      ;;
    --clients)
      test "$#" -ge 2 || die "$1 requires a value"
      clients=$2
      shift 2
      ;;
    --duration)
      test "$#" -ge 2 || die "$1 requires a value"
      duration=$2
      shift 2
      ;;
    --warmup)
      test "$#" -ge 2 || die "$1 requires a value"
      warmup=$2
      shift 2
      ;;
    --payload-size)
      test "$#" -ge 2 || die "$1 requires a value"
      payload_size=$2
      shift 2
      ;;
    --throughput)
      test "$#" -ge 2 || die "$1 requires a value"
      throughput=$2
      shift 2
      ;;
    --subjects)
      test "$#" -ge 2 || die "$1 requires a value"
      subjects=$2
      shift 2
      ;;
    --partitions)
      test "$#" -ge 2 || die "$1 requires a value"
      partitions=$2
      shift 2
      ;;
    --key-cardinality)
      test "$#" -ge 2 || die "$1 requires a value"
      key_cardinality=$2
      shift 2
      ;;
    --max-in-flight)
      test "$#" -ge 2 || die "$1 requires a value"
      max_in_flight=$2
      shift 2
      ;;
    --batch-size)
      test "$#" -ge 2 || die "$1 requires a value"
      batch_size=$2
      shift 2
      ;;
    --subject)
      test "$#" -ge 2 || die "$1 requires a value"
      subject=$2
      shift 2
      ;;
    --output-dir)
      test "$#" -ge 2 || die "$1 requires a value"
      output_dir=$2
      shift 2
      ;;
    --client-config)
      test "$#" -ge 2 || die "$1 requires a value"
      client_config=$2
      shift 2
      ;;
    --server)
      test "$#" -ge 2 || die "$1 requires a value"
      server_override=$2
      shift 2
      ;;
    --cli-bin)
      test "$#" -ge 2 || die "$1 requires a value"
      cli_bin=$2
      shift 2
      ;;
    --server-bin)
      test "$#" -ge 2 || die "$1 requires a value"
      server_bin=$2
      shift 2
      ;;
    --no-build)
      build=false
      shift
      ;;
    --quiet)
      quiet=true
      shift
      ;;
    --keep-going)
      keep_going=true
      shift
      ;;
    --include-unbound)
      include_unbound=true
      shift
      ;;
    --no-unbound)
      include_unbound=false
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      die "unknown option $1"
      ;;
  esac
done

case "$client_config" in
  ''|/*) ;;
  *) client_config=$invocation_dir/$client_config ;;
esac
case "$cli_bin" in
  ''|/*) ;;
  */*) cli_bin=$invocation_dir/$cli_bin ;;
esac
case "$server_bin" in
  ''|/*) ;;
  */*) server_bin=$invocation_dir/$server_bin ;;
esac
case "$output_dir" in
  ''|/*) ;;
  *) output_dir=$invocation_dir/$output_dir ;;
esac

case "$topology" in
  standalone|cluster|external) ;;
  *) die "--topology must be standalone, cluster, or external" ;;
esac

case "$subject" in
  ''|*[!A-Za-z0-9_./-]*)
    die "--subject may contain only letters, digits, _, ., /, and -"
    ;;
esac

for value in "$clients" "$payload_size" "$subjects" "$partitions" "$max_in_flight" "$batch_size"; do
  case "$value" in
    ''|*[!0-9]*|0) die "positive integer options must be greater than zero" ;;
  esac
done
for value in "$throughput" "$key_cardinality"; do
  case "$value" in
    ''|*[!0-9]*) die "rate and cardinality options must be non-negative integers" ;;
  esac
done

if test -z "$ack_levels"; then
  if test "$topology" = cluster; then
    ack_levels=accepted,durable,high-durability,cluster-durable
  else
    ack_levels=accepted,durable,high-durability
  fi
fi

if test "$topology" = external; then
  test -n "$client_config" || die "--topology external requires --client-config"
  test -f "$client_config" || die "client config not found: $client_config"
  include_unbound=false
elif test -n "$server_override"; then
  die "--server applies only to --topology external"
fi

if test "$build" = true; then
  if test "$topology" = external && test -n "$cli_bin"; then
    :
  else
    cargo build --release -p cli --bin morrow-cli --locked
  fi
  if test "$topology" != external && test -z "$server_bin"; then
    cargo build --release -p server --bin morrow-server --locked
  fi
fi

test -n "$cli_bin" || cli_bin=$repo_dir/target/release/morrow-cli
if test ! -x "$cli_bin"; then
  resolved_cli=$(command -v "$cli_bin" 2>/dev/null || true)
  test -n "$resolved_cli" && cli_bin=$resolved_cli
fi
test -x "$cli_bin" || die "CLI binary is not executable: $cli_bin"
if test "$topology" != external; then
  test -n "$server_bin" || server_bin=$repo_dir/target/release/morrow-server
  if test ! -x "$server_bin"; then
    resolved_server=$(command -v "$server_bin" 2>/dev/null || true)
    test -n "$resolved_server" && server_bin=$resolved_server
  fi
  test -x "$server_bin" || die "server binary is not executable: $server_bin"
fi

timestamp=$(date -u +%Y%m%dT%H%M%SZ)
test -n "$output_dir" || output_dir=$repo_dir/target/benchmarks/$timestamp-$$
test ! -e "$output_dir" || die "output directory already exists: $output_dir"
mkdir -p "$output_dir"
output_dir=$(CDPATH= cd -- "$output_dir" && pwd)

work_dir=$(mktemp -d "${TMPDIR:-/tmp}/morrow-publish-bench.XXXXXX")
server_pids=
failed_cases=

stop_servers() {
  for pid in $server_pids; do
    kill "$pid" 2>/dev/null || true
  done
  for pid in $server_pids; do
    wait "$pid" 2>/dev/null || true
  done
  server_pids=
}

cleanup() {
  stop_servers
  rm -rf "$work_dir"
}
trap cleanup EXIT HUP INT TERM

write_client_config() {
  endpoint=$1
  path=$2
  cat > "$path" <<EOF
{
  "server": "$endpoint",
  "connect": {
    "durable_id": "publish-benchmark"
  }
}
EOF
}

write_stream() {
  storage=$1
  cat <<EOF
  "streams": [
    {
      "name": "publish-benchmark",
      "subjects": ["$subject", "$subject/**"],
      "partitions": $partitions,
      "storage": $storage
    }
  ]
EOF
}

write_standalone_config() {
  case_dir=$1
  config=$2
  stream=$(write_stream '{"mode":"local","replicas":1,"min_ack_replicas":1}')
  cat > "$config" <<EOF
{
  "listen": "127.0.0.1:14222",
  "wal_dir": "$case_dir/wal",
  "fsync_interval_ms": 5,
$stream
}
EOF
}

write_cluster_config() {
  case_dir=$1
  node=$2
  bootstrap=$3
  routes=$4
  config=$5
  client_port=$((14220 + node))
  raft_port=$((15220 + node))
  route_port=$((16220 + node))
  http_port=$((18220 + node))
  stream=$(write_stream '{"mode":"quorum_fsync","replicas":3,"min_ack_replicas":2}')
  cat > "$config" <<EOF
{
  "listen": "127.0.0.1:$client_port",
  "http_listen": "127.0.0.1:$http_port",
  "admin_token": "publish-benchmark-admin",
  "wal_dir": "$case_dir/node-$node/wal",
  "fsync_interval_ms": 5,
$stream,
  "cluster": {
    "enabled": true,
    "node_id": $node,
    "auth_token": "publish-benchmark-cluster",
    "raft_listen": "127.0.0.1:$raft_port",
    "allow_insecure_internal_transports": true,
    "route_listen": "127.0.0.1:$route_port",
    "route_advertise": "127.0.0.1:$route_port",
    "routes": $routes,
    "route_reconnect_ms": 100,
    "raft_dir": "$case_dir/node-$node/raft",
    "bootstrap": $bootstrap,
    "nodes": [
      {"node_id":1,"raft_addr":"127.0.0.1:15221","client_addr":"127.0.0.1:14221"},
      {"node_id":2,"raft_addr":"127.0.0.1:15222","client_addr":"127.0.0.1:14222"},
      {"node_id":3,"raft_addr":"127.0.0.1:15223","client_addr":"127.0.0.1:14223"}
    ],
    "election_timeout_min_ms": 150,
    "election_timeout_max_ms": 300,
    "heartbeat_interval_ms": 50,
    "snapshot_threshold": 10000
  }
}
EOF
}

run_cli() {
  if test -n "$server_override"; then
    "$cli_bin" --config "$client_config" --server "$server_override" "$@"
  else
    "$cli_bin" --config "$client_config" "$@"
  fi
}

wait_for_ping() {
  attempts=0
  until run_cli ping >/dev/null 2>&1; do
    attempts=$((attempts + 1))
    test "$attempts" -lt 100 || return 1
    sleep 0.1
  done
}

wait_for_cluster() {
  attempts=0
  until curl -fsS http://127.0.0.1:18221/health/ready >/dev/null 2>&1; do
    attempts=$((attempts + 1))
    test "$attempts" -lt 200 || return 1
    sleep 0.1
  done
}

start_fixture() {
  case_name=$1
  case_dir=$work_dir/$case_name
  mkdir -p "$case_dir"
  stop_servers
  if test "$topology" = standalone; then
    fixture_config=$case_dir/server.json
    write_standalone_config "$case_dir" "$fixture_config"
    cp "$fixture_config" "$output_dir/$case_name-server.json"
    write_client_config 127.0.0.1:14222 "$case_dir/client.json"
    client_config=$case_dir/client.json
    "$server_bin" "$fixture_config" > "$output_dir/$case_name-server.log" 2>&1 &
    server_pids=$!
    wait_for_ping || die "standalone fixture did not become ready; see $case_name-server.log"
  elif test "$topology" = cluster; then
    write_cluster_config "$case_dir" 1 true '[]' "$case_dir/node-1.json"
    write_cluster_config "$case_dir" 2 false '["127.0.0.1:16221"]' "$case_dir/node-2.json"
    write_cluster_config "$case_dir" 3 false '["127.0.0.1:16221"]' "$case_dir/node-3.json"
    cp "$case_dir/node-1.json" "$output_dir/$case_name-node-1.json"
    cp "$case_dir/node-2.json" "$output_dir/$case_name-node-2.json"
    cp "$case_dir/node-3.json" "$output_dir/$case_name-node-3.json"
    write_client_config 127.0.0.1:14221 "$case_dir/client.json"
    client_config=$case_dir/client.json
    "$server_bin" "$case_dir/node-1.json" > "$output_dir/$case_name-node-1.log" 2>&1 &
    server_pids=$!
    "$server_bin" "$case_dir/node-2.json" > "$output_dir/$case_name-node-2.log" 2>&1 &
    server_pids="$server_pids $!"
    "$server_bin" "$case_dir/node-3.json" > "$output_dir/$case_name-node-3.log" 2>&1 &
    server_pids="$server_pids $!"
    wait_for_ping || die "cluster fixture did not accept clients; see $case_name-node-*.log"
    wait_for_cluster || die "cluster fixture did not become ready; see $case_name-node-*.log"
  fi
}

run_case() {
  case_name=$1
  target=$2
  shift 2
  if test "$topology" != external; then
    start_fixture "$case_name"
  fi
  result_json=$output_dir/$case_name.json
  result_csv=$output_dir/$case_name.csv
  result_stderr=$output_dir/$case_name.stderr.txt
  echo "running $case_name"
  case_ok=true
  case "$warmup" in
    0|0s|0ms)
      if ! run_cli bench pub "$target" \
          --clients "$clients" \
          --duration "$duration" \
          --payload-size "$payload_size" \
          --throughput "$throughput" \
          --subjects "$subjects" \
          --key-cardinality "$key_cardinality" \
          "$@" \
          --json \
          --csv "$result_csv" > "$result_json" 2> "$result_stderr"; then
        case_ok=false
      fi
      ;;
    *)
      if ! run_cli bench pub "$target" \
          --clients "$clients" \
          --duration "$duration" \
          --warmup "$warmup" \
          --payload-size "$payload_size" \
          --throughput "$throughput" \
          --subjects "$subjects" \
          --key-cardinality "$key_cardinality" \
          "$@" \
          --json \
          --csv "$result_csv" > "$result_json" 2> "$result_stderr"; then
        case_ok=false
      fi
      ;;
  esac
  if test "$case_ok" = false; then
    echo "case failed: $case_name" >&2
    cat "$result_stderr" >&2
    failed_cases="$failed_cases $case_name"
    if test "$topology" != external; then
      stop_servers
    fi
    test "$keep_going" = true && return 0
    return 1
  fi
  if test "$quiet" = false; then
    cat "$result_json"
  fi
  if test "$topology" != external; then
    stop_servers
  fi
}

revision=$(git rev-parse HEAD 2>/dev/null || echo unknown)
cat > "$output_dir/run.txt" <<EOF
revision=$revision
topology=$topology
modes=$modes
ack_levels=$ack_levels
clients=$clients
duration=$duration
warmup=$warmup
payload_size=$payload_size
throughput=$throughput
subjects=$subjects
partitions=$partitions
key_cardinality=$key_cardinality
max_in_flight=$max_in_flight
batch_size=$batch_size
subject=$subject
cli_bin=$cli_bin
server_bin=${server_bin:-external}
EOF

if test "$include_unbound" = true; then
  run_case "$topology-unbound-fire-and-forget" "$subject-unbound" \
    --mode fire-and-forget
fi

old_ifs=$IFS
IFS=,
for mode in $modes; do
  IFS=$old_ifs
  case "$mode" in
    fire-and-forget)
      run_case "$topology-stream-fire-and-forget" "$subject" \
        --mode fire-and-forget
      ;;
    sync|async|batch)
      IFS=,
      for ack_level in $ack_levels; do
        IFS=$old_ifs
        case "$ack_level" in
          accepted|durable|high-durability|cluster-durable) ;;
          *) die "unsupported acknowledgement level: $ack_level" ;;
        esac
        if test "$ack_level" = cluster-durable && test "$topology" = standalone; then
          echo "skipping standalone $mode/$ack_level: clustered mode is required" >&2
          IFS=,
          continue
        fi
        case_name=$topology-$mode-$ack_level
        if test "$mode" = async; then
          run_case "$case_name" "$subject" --mode async \
            --ack-level "$ack_level" --max-in-flight "$max_in_flight"
        elif test "$mode" = batch; then
          run_case "$case_name" "$subject" --mode batch \
            --ack-level "$ack_level" --batch-size "$batch_size"
        else
          run_case "$case_name" "$subject" --mode sync \
            --ack-level "$ack_level"
        fi
        IFS=,
      done
      IFS=$old_ifs
      ;;
    *)
      die "unsupported publish mode: $mode"
      ;;
  esac
  IFS=,
done
IFS=$old_ifs

echo "results written to $output_dir"
if test -n "$failed_cases"; then
  echo "failed cases:$failed_cases" >&2
  exit 1
fi
