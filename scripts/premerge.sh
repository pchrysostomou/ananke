#!/usr/bin/env bash
# The pre-merge tier (D-040): every sweep at a thousand seeds, in release, seeds in
# parallel, on the machine in front of you. The gate's twenty seeds catch the
# shallow bugs and CI's hundred the next layer; this is the layer under that, in
# about a quarter of an hour. Ten thousand seeds are the nightly's on GitHub, not a
# laptop's job. Every sweep's catch rates are printed (`--nocapture`), so the numbers
# a branch is merged on are in the terminal. Run it on a branch before asking for
# the merge; the gate still precedes every commit.
set -euo pipefail
cd "$(dirname "$0")/.."
export RUSTFLAGS="-D warnings"
export ANANKE_SEEDS="${ANANKE_SEEDS:-1000}"
cargo test --workspace --all-features --release --all-targets -- --nocapture
echo "premerge: green at $ANANKE_SEEDS seeds"
