# Building Morrow

## Requirements

- Stable Rust and Cargo.
- A C compiler and linker required by the Rust dependencies.
- Docker Compose for the local cluster workflow.
- `just` is optional; the repository's `Justfile` provides shortcuts.

## Common commands

```bash
cargo fmt --all
cargo check --workspace --locked
cargo test --workspace --locked
cargo build --release --workspace --locked
git diff --check
```

Focused checks:

```bash
cargo test -p server --locked
cargo test -p integration --locked
cargo test -p protocol --locked
```

## Binaries

```bash
cargo build --release -p server --bin morrow-server --locked
cargo build --release -p cli --bin morrow-cli --locked
cargo build --release -p connector --bin morrow-connector --locked
```

The `dev` profile keeps dependency debug information small. The `debugging`
profile enables full debug information, while `release-fast` favors iteration
speed over maximum release optimization:

```bash
cargo build --profile debugging --workspace --locked
cargo build --profile release-fast --workspace --locked
```

Equivalent `just` commands are available with `just --list`, including
`just build-debug`, `just build-release`, `just build-release-fast`, and
`just test`.

## Docker

Validate and build the local image with:

```bash
docker compose config
docker build -t morrow:dev .
```

Pass `CARGO_PROFILE=dev CARGO_PROFILE_DIR=debug` for a debug image; the
defaults build the release profile.

The image starts `morrow-server` with `/etc/morrow/morrow.json` and stores data
under `/var/lib/morrow`.

## Release builds

Release artifacts should be built from a version tag with `--locked`, tested on
each supported target, accompanied by SHA-256 checksums, and uploaded to the
release page. Package managers consume immutable source or platform archives;
do not package arbitrary branch snapshots.
