# WebAssembly execution pooling

Middleware generations resolve broker host imports once at installation and
retain a Wasmtime `InstancePre` and indexed process export for each module.
Installation also validates that every module exports `process(i32) -> i32`,
so malformed entry points do not fail on the message path and execution avoids
repeating the named-export lookup.

The engine uses Wasmtime's pooling allocator with 64 concurrent core-instance
and memory slots. A call that arrives while every slot is occupied fails
immediately with `middleware execution pool is busy`; the middleware failure
policy then determines whether that error is fail-open, fail-closed, or a drop.
The global pooled-memory ceiling is 256 MiB, while each call still has the
smaller memory limit declared by its middleware manifest.

Only allocator slots are reused. Every call creates a fresh `Store`, host
state, and instance. Wasmtime is configured to retain no resident linear-memory
pages when returning a slot, so linear memory, globals, tables, capabilities,
fuel, emissions, and allocation counters begin from module and manifest state
on every call. This avoids a custom reset protocol that could miss guest state.

Message mutations are staged in host state. The original payload and headers
move into the call without being cloned; successful execution commits only the
changed fields, while a trap or budget failure returns the original message.
This also prevents partial guest mutations from escaping a failed invocation.

## Benchmark

The ignored `benchmark_noop_middleware_overhead` test measures the former
per-call linker/import-resolution path and the prepared-generation path in the
same optimized binary. On the development machine, 10,000 no-op calls produced:

| Path | Throughput | p50 | p95 | p99 |
| --- | ---: | ---: | ---: | ---: |
| Per-call linker | 168,709/s | 5.875 us | 6.000 us | 6.084 us |
| Prepared generation | 904,343/s | 1.083 us | 1.125 us | 1.167 us |

These are local microbenchmark results, not a production capacity guarantee.
Re-run with:

```bash
cargo test -p server middleware::tests::benchmark_noop_middleware_overhead \
  --release -- --ignored --nocapture
```
