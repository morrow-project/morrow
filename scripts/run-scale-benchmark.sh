#!/bin/sh
set -eu
usage() {
  echo "Usage: $0 --server ADDRESS --client-config FILE [options]"
  echo "  --deployment-profile combined|separated (default: combined)"
  echo "  --controller-voters N                    (default: 3)"
  echo "  --roles-share-process true|false         (default follows profile)"
  echo "  --metrics-url URL                        (optional endpoint captured per case)"
  echo "  --server-pid PID                         (optional per-case resource snapshots)"
  echo "  --modes LIST                             (default: fire-and-forget,sync,async,batch)"
  echo "  --ack-levels LIST                        (default: accepted,durable,high-durability)"
}
die() { echo "error: $*" >&2; exit 1; }
cpu_cores() {
  getconf _NPROCESSORS_ONLN 2>/dev/null || true
}
memory_bytes() {
  case "$(uname -s)" in
    Darwin) sysctl -n hw.memsize 2>/dev/null || true ;;
    Linux) awk '/^MemTotal:/ { print $2 * 1024; exit }' /proc/meminfo 2>/dev/null || true ;;
  esac
}
cpu_model() {
  case "$(uname -s)" in
    Darwin) sysctl -n machdep.cpu.brand_string 2>/dev/null || true ;;
    Linux) awk -F: '/^model name/ { sub(/^ /, "", $2); print $2; exit }' /proc/cpuinfo 2>/dev/null || true ;;
  esac
}
cpu_hz() {
  case "$(uname -s)" in
    Darwin) sysctl -n hw.cpufrequency 2>/dev/null || true ;;
    Linux) awk -F: '/^cpu MHz/ { printf "%.0f", $2 * 1000000; exit }' /proc/cpuinfo 2>/dev/null || true ;;
  esac
}
json_escape() {
  printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'
}
validate_topology_metrics() {
  metrics_file=$1
  expected_role=broker
  test "$deployment_profile" = combined && expected_role=combined
  actual_voters=$(awk '/^morrow_controller_voters / { print $2; exit }' "$metrics_file")
  test "$actual_voters" = "$controller_voters" || die "topology metrics report $actual_voters controller voters; expected $controller_voters"
  grep -Fq "morrow_node_role{role=\"$expected_role\"} 1" "$metrics_file" || die "topology metrics do not identify the endpoint as role $expected_role"
}
capture_resources() {
  output_file=$1
  test -n "$server_pid" || return 0
  rss_kib=; cpu_percent=; threads=; fd_count=; cpu_time_ticks=
  case "$(uname -s)" in
    Linux)
      if test -r "/proc/$server_pid/status"; then
        rss_kib=$(awk '/^VmRSS:/ { print $2; exit }' "/proc/$server_pid/status")
        threads=$(awk '/^Threads:/ { print $2; exit }' "/proc/$server_pid/status")
        cpu_time_ticks=$(awk '{ print $14 + $15 }' "/proc/$server_pid/stat" 2>/dev/null || true)
        fd_count=$(/usr/bin/find "/proc/$server_pid/fd" -maxdepth 1 -type l 2>/dev/null | wc -l | tr -d ' ')
      fi
      ;;
    Darwin)
      ps_line=$(ps -p "$server_pid" -o rss=,pcpu=,nlwp= 2>/dev/null || true)
      set -- $ps_line
      rss_kib=${1:-}; cpu_percent=${2:-}; threads=${3:-}
      fd_count=$(lsof -p "$server_pid" 2>/dev/null | tail -n +2 | wc -l | tr -d ' ')
      ;;
  esac
  printf '{"pid":%s,"rss_kib":%s,"cpu_percent":%s,"threads":%s,"fd_count":%s,"cpu_time_ticks":%s}\n' \
    "$server_pid" "${rss_kib:-null}" "${cpu_percent:-null}" "${threads:-null}" "${fd_count:-null}" "${cpu_time_ticks:-null}" >"$output_file"
}
server=; client_config=; broker_counts=1,3,5; topics=1,10,100; partitions=1,4,16
clients=5; duration=10s; payload_size=128
deployment_profile=combined; controller_voters=3; roles_share_process=true
metrics_url=
server_pid=
modes=fire-and-forget,sync,async,batch
ack_levels=accepted,durable,high-durability
output_dir="target/scale-benchmarks/$(date -u +%Y%m%dT%H%M%SZ)"; build=true
while test "$#" -gt 0; do
  case "$1" in
    --server) test "$#" -ge 2 || die "$1 requires a value"; server=$2; shift 2 ;;
    --client-config) test "$#" -ge 2 || die "$1 requires a value"; client_config=$2; shift 2 ;;
    --broker-counts) test "$#" -ge 2 || die "$1 requires a value"; broker_counts=$2; shift 2 ;;
    --topics) test "$#" -ge 2 || die "$1 requires a value"; topics=$2; shift 2 ;;
    --partitions) test "$#" -ge 2 || die "$1 requires a value"; partitions=$2; shift 2 ;;
    --deployment-profile) test "$#" -ge 2 || die "$1 requires a value"; deployment_profile=$2; shift 2 ;;
    --controller-voters) test "$#" -ge 2 || die "$1 requires a value"; controller_voters=$2; shift 2 ;;
    --roles-share-process) test "$#" -ge 2 || die "$1 requires a value"; roles_share_process=$2; shift 2 ;;
    --metrics-url) test "$#" -ge 2 || die "$1 requires a value"; metrics_url=$2; shift 2 ;;
    --server-pid) test "$#" -ge 2 || die "$1 requires a value"; server_pid=$2; shift 2 ;;
    --modes) test "$#" -ge 2 || die "$1 requires a value"; modes=$2; shift 2 ;;
    --ack-levels) test "$#" -ge 2 || die "$1 requires a value"; ack_levels=$2; shift 2 ;;
    --clients) test "$#" -ge 2 || die "$1 requires a value"; clients=$2; shift 2 ;;
    --duration) test "$#" -ge 2 || die "$1 requires a value"; duration=$2; shift 2 ;;
    --payload-size) test "$#" -ge 2 || die "$1 requires a value"; payload_size=$2; shift 2 ;;
    --output-dir) test "$#" -ge 2 || die "$1 requires a value"; output_dir=$2; shift 2 ;;
    --no-build) build=false; shift ;;
    -h|--help) usage; exit 0 ;;
    *) die "unknown option $1" ;;
  esac
done
test -n "$server" || die "--server is required"
test -f "$client_config" || die "--client-config must name a file"
if test -n "$server_pid"; then
  case "$server_pid" in ''|*[!0-9]*) die "--server-pid must be a positive integer" ;; esac
  test "$server_pid" -gt 0 || die "--server-pid must be a positive integer"
  kill -0 "$server_pid" 2>/dev/null || die "server process $server_pid is not running"
fi
case "$deployment_profile" in combined|separated) ;; *) die "--deployment-profile must be combined or separated" ;; esac
case "$roles_share_process" in true|false) ;; *) die "--roles-share-process must be true or false" ;; esac
case "$controller_voters" in ''|*[!0-9]*|0) die "--controller-voters must be a positive integer" ;; esac
if test "$deployment_profile" = separated && test "$roles_share_process" = true; then
  die "separated deployments must set --roles-share-process false"
fi
if test "$deployment_profile" = combined && test "$roles_share_process" = false; then
  die "combined deployments must set --roles-share-process true"
fi
if "$build"; then cargo build --release -p cli --locked; fi
test -x target/release/morrow || die "target/release/morrow is not executable"
mkdir -p "$output_dir"
case_index="$output_dir/cases.ndjson"
: > "$case_index"
started_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
batch_records=${MORROW_DATA_APPEND_BATCH_RECORDS:-256}
batch_bytes=${MORROW_DATA_APPEND_BATCH_BYTES:-8388608}
metadata_cache_capacity=${MORROW_PARTITION_METADATA_CACHE_CAPACITY:-4096}
case "$batch_records" in
  ''|*[!0-9]*) batch_records=256 ;;
esac
case "$batch_bytes" in
  ''|*[!0-9]*) batch_bytes=8388608 ;;
esac
case "$metadata_cache_capacity" in
  ''|*[!0-9]*) metadata_cache_capacity=4096 ;;
esac
test "$batch_records" -ge 1 || batch_records=1
test "$batch_records" -le 256 || batch_records=256
test "$batch_bytes" -ge 1 || batch_bytes=1
test "$batch_bytes" -le 8388608 || batch_bytes=8388608
test "$metadata_cache_capacity" -ge 1 || metadata_cache_capacity=1
test "$metadata_cache_capacity" -le 1000000 || metadata_cache_capacity=1000000
hostname_value=$(hostname 2>/dev/null || true)
os_name=$(uname -s 2>/dev/null || true)
os_release=$(uname -r 2>/dev/null || true)
printf '{"commit":"%s","server":"%s","clients":%s,"duration":"%s","payload_size":%s,"broker_counts":[%s],"topics":[%s],"partitions":[%s],"modes":"%s","ack_levels":"%s","deployment_profile":"%s","controller_voter_count":%s,"roles_share_process":%s,"batch_records":%s,"batch_bytes":%s,"metadata_cache_capacity":%s,"started_at":"%s","hostname":"%s","os":"%s","kernel":"%s","cpu_cores":%s,"memory_bytes":%s,"cpu_model":"%s","cpu_hz":%s,"uname":"%s"}\n' \
  "$(git rev-parse HEAD)" "$server" "$clients" "$duration" "$payload_size" "$broker_counts" "$topics" "$partitions" \
  "$modes" "$ack_levels" "$deployment_profile" "$controller_voters" "$roles_share_process" "$batch_records" "$batch_bytes" "$metadata_cache_capacity" "$started_at" "$(json_escape "$hostname_value")" "$(json_escape "$os_name")" "$(json_escape "$os_release")" \
  "${cpu_cores:-null}" "${memory_bytes:-null}" "$(json_escape "$(cpu_model)")" "${cpu_hz:-null}" "$(json_escape "$(uname -a)")" >"$output_dir/manifest.json"
old_ifs=$IFS
IFS=,
for broker_count in $broker_counts; do
  for topic_count in $topics; do
    for partition_count in $partitions; do
      case_dir="$output_dir/brokers-$broker_count/topics-$topic_count/partitions-$partition_count"
      mkdir -p "$case_dir"
      capture_resources "$case_dir/resources-before.json"
      scripts/run-publish-benchmark-matrix.sh --topology external --client-config "$client_config" --server "$server" --clients "$clients" --duration "$duration" --payload-size "$payload_size" --subjects "$topic_count" --partitions "$partition_count" --modes "$modes" --ack-levels "$ack_levels" --output-dir "$case_dir" --quiet
      capture_resources "$case_dir/resources-after.json"
      if test -n "$metrics_url"; then
        curl -sSfL "$metrics_url" >"$case_dir/metrics.prom" || die "failed to capture metrics from $metrics_url"
        validate_topology_metrics "$case_dir/metrics.prom"
      fi
      printf '{"broker_count":%s,"topics":%s,"partitions":%s,"deployment_profile":"%s","controller_voter_count":%s,"roles_share_process":%s,"batch_records":%s,"batch_bytes":%s,"metadata_cache_capacity":%s,"modes":"%s","ack_levels":"%s","result_dir":"%s"}\n' \
        "$broker_count" "$topic_count" "$partition_count" "$deployment_profile" "$controller_voters" "$roles_share_process" "$batch_records" "$batch_bytes" "$metadata_cache_capacity" "$modes" "$ack_levels" \
        "$(json_escape "$case_dir")" >> "$case_index"
    done
  done
done
IFS=$old_ifs
printf '%s\n' "scale benchmark results: $output_dir"
