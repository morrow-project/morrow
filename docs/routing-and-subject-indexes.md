# Routing trie and segment subject index

## Design

Concrete publish subjects are matched against a compiled token trie. Literal
edges, one-token `*` edges, and terminal `>` interests share prefixes without
materializing every concrete subject. The stream catalog, transient local
subscriptions, durable consumer filters, and each remote route peer maintain
their own typed trie values. Removing a subscription removes only its value, so
fan-out and durable queue membership remain unchanged.

Each sealed partition segment also gets an optional `.sidx` file. It contains a
sorted concrete-subject dictionary and an offset postings list per dictionary
entry. Exact filters use a dictionary binary search. Selective wildcard filters
resolve the dictionary once and merge ordered postings. Broad `>` filters and
wildcards whose postings exceed one quarter of the segment fall back to a
sequential segment scan; the broker's live durable-delivery path keeps broad
wildcard scans in its existing in-memory record view.

Indexes are not authoritative. They are rebuilt atomically from immutable
segments on open and rotation. Missing or malformed index files are overwritten
from the segment. If subject cardinality exceeds 4,096 entries, postings exceed
65,536 offsets, a file exceeds 4 MiB, or the per-partition cache reaches 4 MiB,
the segment remains readable through a sequential scan. Sealed-segment fallback
reads decode the checksummed source segment rather than retaining an unbounded
duplicate subject table.

## Benchmark record

The ignored release benchmarks can be reproduced with:

```bash
cargo test -p protocol --release benchmark_trie_exact_star_and_tail_matching \
  -- --ignored --nocapture
cargo test -p server --release benchmark_sealed_subject_index_exact_star_and_tail_filters \
  -- --ignored --nocapture
```

On 2026-08-21, 3,000 trie lookups across 10,000 mixed exact, `*`, and `>`
interests took 0.640 ms, versus 764.363 ms for reference full scans.

The segment benchmark used 10,000 records, 1,000 concrete subjects, and 100
queries per filter:

| Filter | Adaptive segment query | In-memory full scan | Decision |
| --- | ---: | ---: | --- |
| `orders.42.event` | 0.538 ms | 37.696 ms | Keep exact index lookup |
| `orders.*.event` | 2,016.517 ms | 56.068 ms | Use live in-memory scan for broad wildcard |
| `orders.>` | 1,981.971 ms | 35.619 ms | Use live in-memory scan for broad tail wildcard |

The broad-filter numbers include bounded fallback reads from immutable segment
files and therefore are not a claim that disk scans beat the broker's existing
memory view. They justify the adaptive policy: keep the sealed index for exact
and selective filters, retain the correct scan path for broad filters, and do
not spend unbounded memory caching every subject or postings list.
