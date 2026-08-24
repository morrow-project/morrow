# Protocol v1 conformance and validation

The protocol crate owns a shared corpus in `src/conformance.rs`. Every wire
codec must round-trip that corpus to the same semantic model. The normal test
suite runs these cases; the ignored benchmark can be run in release mode:

```bash
cargo test -p protocol --release benchmark_codec_round_trips -- --ignored --nocapture
```

Decoder fuzz targets live under `fuzz/fuzz_targets/`:

```bash
cargo fuzz run cbor_decode
cargo fuzz run text_decode
```

Fuzz inputs are untrusted bytes. Targets must never panic, allocate beyond
configured limits, or invoke broker state. Add a conformance vector whenever a
new frame kind or semantic field is introduced.
