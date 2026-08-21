set shell := ["bash", "-eu", "-o", "pipefail", "-c"]

default:
    @just --list

update:
    cargo update

build-debug:
    cargo build --debug --locked --workspace

build-release:
    cargo build --release --locked --workspace

clean-debug:
    rm -rf target/debug

clean-release:
    rm -rf target/release

clean: clean-debug clean-release

test:
    cargo test --locked --workspace
