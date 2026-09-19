#!/usr/bin/env bash
# One job of the nightly (D-064). `scripts/nightly.sh <1-6>` runs one shard: the tests of
# ananke-sim's integration binaries (sim/tests/*.rs) that scripts/nightly-shards.txt assigns
# to it, binary by binary. `scripts/nightly.sh rest` runs every other test in the workspace,
# skipping the table's by exact name, so the seven jobs together run each test once. The
# seed counts come from the environment as at every tier: the workflow sets ANANKE_SEEDS to
# 10 000 and ANANKE_DEEP_SEEDS to 1 000, and the same command runs a shard at any count here.
# Anything after the shard goes to the test harness: `scripts/nightly.sh 3 --list` names the
# tests shard 3 runs without running them.
set -euo pipefail
cd "$(dirname "$0")/.."
shard="${1:?usage: scripts/nightly.sh <shard 1-6 | rest> [test harness arguments]}"
shift
extra=("$@")
table=scripts/nightly-shards.txt
export RUSTFLAGS="${RUSTFLAGS:--D warnings}"

if [ "$shard" = rest ]; then
    set -- --nocapture --exact ${extra[@]+"${extra[@]}"}
    while IFS=$'\t' read -r _ _ _ test; do
        set -- "$@" --skip "$test"
    done < <(grep -v '^#' "$table")
    cargo test --workspace --all-features --release --all-targets -- "$@"
    echo "nightly: the rest green at ${ANANKE_SEEDS:-20} seeds"
    exit 0
fi

binaries=$(awk -F'\t' -v s="$shard" '!/^#/ && $1 == s { print $2 }' "$table" | sort -u)
if [ -z "$binaries" ]; then
    echo "nightly: $table has no shard $shard" >&2
    exit 2
fi
for binary in $binaries; do
    set -- --nocapture --exact ${extra[@]+"${extra[@]}"}
    while IFS= read -r test; do
        set -- "$@" "$test"
    done < <(awk -F'\t' -v s="$shard" -v b="$binary" '!/^#/ && $1 == s && $2 == b { print $4 }' "$table")
    cargo test -p ananke-sim --all-features --release --test "$binary" -- "$@"
done
echo "nightly: shard $shard green at ${ANANKE_SEEDS:-20} seeds"
