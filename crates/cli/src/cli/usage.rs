use super::*;

pub(super) fn usage() -> CliError {
    CliError::msg(
        "usage: morrow-cli [--config client.json] [--server host:port] <ping|pub|sub|request|reply|bench>\n\
         pub <subject> <payload>\n\
         sub <subject> [--sid sid] [--queue group] [--ack] [--max-messages n]\n\
         request <subject> <payload> [--timeout-ms n]\n\
         reply <subject> [--queue group]\n\
         bench <pub|sub|pubsub|request|serve> <subject> [options]\n\
         bench <consume|fetch> <consumer> [options]\n\
             [--clients n] [--messages n|--duration 30s] [--throughput n]\n\
             [--payload-size n|--payload file] [--header K:V] [--sleep 1ms]\n\
             [--mode fire-and-forget|sync|async|batch] [--ack-level level]\n\
             [--max-in-flight n] [--batch-size n] [--subjects n]\n\
             [--subject-order sequential|random] [--key-cardinality n]\n\
             [--warmup 5s] [--seed n] [--queue group] [--ack]\n\
             [--durable-id id] [--json] [--csv file]
             [--stream name --partition-metadata file]
             [--stream name --partition-metadata-url http://host:port/path
              --partition-metadata-token token]",
    )
}
