#!/usr/bin/env bash
# The gate: every check CI runs, in sequence, as one command. A commit is made only after
# this has exited 0 on the exact tree being committed (CLAUDE.md). `set -e` stops at the
# first failure and `pipefail` makes a failing pipeline count, so a red step can never be
# scrolled past.
set -euo pipefail
cd "$(dirname "$0")/.."
export RUSTFLAGS="-D warnings" RUSTDOCFLAGS="-D warnings"
# Sweeps run this many seeds; CI runs 100 and the nightly 10 000 (sim/lib.rs). The
# gate must stay under a minute as the storage engine grows.
export ANANKE_SEEDS="${ANANKE_SEEDS:-20}"
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
scripts/check-direct-io.sh
cargo doc --workspace --no-deps --all-features
cargo test --workspace --all-features --all-targets
cargo test --workspace --all-features --doc
echo "gate: green"
