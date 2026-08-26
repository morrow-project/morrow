#!/bin/sh
set -eu
usage() { echo "Usage: $0 --server ADDRESS --client-config FILE [options]"; }
die() { echo "error: $*" >&2; exit 1; }
server=; client_config=; broker_counts=1,3,5; topics=1,10,100; partitions=1,4,16
clients=5; duration=10s; payload_size=128
output_dir="target/scale-benchmarks/$(date -u +%Y%m%dT%H%M%SZ)"; build=true
while test "$#" -gt 0; do
  case "$1" in
    --server) test "$#" -ge 2 || die "$1 requires a value"; server=$2; shift 2 ;;
    --client-config) test "$#" -ge 2 || die "$1 requires a value"; client_config=$2; shift 2 ;;
    --broker-counts) test "$#" -ge 2 || die "$1 requires a value"; broker_counts=$2; shift 2 ;;
    --topics) test "$#" -ge 2 || die "$1 requires a value"; topics=$2; shift 2 ;;
    --partitions) test "$#" -ge 2 || die "$1 requires a value"; partitions=$2; shift 2 ;;
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
if "$build"; then cargo build --release -p cli --locked; fi
test -x target/release/morrow || die "target/release/morrow is not executable"
mkdir -p "$output_dir"
printf '{"commit":"%s","server":"%s","clients":%s,"duration":"%s","payload_size":%s,"broker_counts":[%s],"topics":[%s],"partitions":[%s],"uname":"%s"}\n' "$(git rev-parse HEAD)" "$server" "$clients" "$duration" "$payload_size" "$broker_counts" "$topics" "$partitions" "$(uname -a)" >"$output_dir/manifest.json"
old_ifs=$IFS
IFS=,
for broker_count in $broker_counts; do
  for topic_count in $topics; do
    for partition_count in $partitions; do
      case_dir="$output_dir/brokers-$broker_count/topics-$topic_count/partitions-$partition_count"
      mkdir -p "$case_dir"
      scripts/run-publish-benchmark-matrix.sh --topology external --client-config "$client_config" --server "$server" --clients "$clients" --duration "$duration" --payload-size "$payload_size" --subjects "$topic_count" --partitions "$partition_count" --output-dir "$case_dir" --quiet
    done
  done
done
IFS=$old_ifs
printf '%s\n' "scale benchmark results: $output_dir"
