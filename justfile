default:
    @just --list

build:
    cargo build

build-release:
    cargo build --release

run *ARGS:
    cargo run -- {{ARGS}}

lint:
    cargo clippy --workspace --all-features --all-targets -- -D warnings

fmt:
    cargo +nightly fmt

fmt-check:
    cargo +nightly fmt --check

test:
    cargo nextest run --workspace --all-features

# Run all CI checks (fmt, lint, test)
ci: fmt-check lint test

clean:
    cargo clean
