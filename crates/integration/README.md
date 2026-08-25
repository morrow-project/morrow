# Integration crate

The `integration` crate contains cross-crate smoke tests. It exercises real
TCP, TLS, authentication, routing, partition replication, and OpenRaft paths.

Keep deterministic semantic coverage in the server crate; use this crate for
wiring and end-to-end behavior that cannot be covered in-process.

## Real-process lifecycle campaign

The ignored `process_lifecycle` smoke test runs the actual server binary and
exercises SIGTERM and SIGINT with isolated ports and storage:

```text
cargo build -p server --bin morrow-server
MORROW_SERVER_BIN=target/debug/morrow-server \
  cargo test -p integration --test process_lifecycle -- --ignored --nocapture
```

The campaign preserves the server's inherited logs and temporary storage on a
test failure. Extend this harness with deterministic failpoints for crash-boundary
campaigns; do not use wall-clock sleeps as correctness assertions.
