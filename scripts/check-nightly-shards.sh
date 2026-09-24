#!/usr/bin/env bash
# Holds the nightly's shard table (scripts/nightly-shards.txt, D-064) to the tests that
# exist, so a sweep can neither fall out of the nightly nor run in it twice. It fails when
# a row names a shard outside 1-6, a binary outside sim/tests, or a test that binary does
# not have; when a row appears twice; when a test of sim/tests/*.rs is in no row; and when
# a name in the table also names a test outside the table, which the `rest` job's
# `--skip`, matching by name alone, would then skip. It lists tests from the binaries the
# gate's `cargo test` has just built, so it builds nothing new; if listing fails, it prints
# cargo's own errors rather than failing silently.
set -euo pipefail
cd "$(dirname "$0")/.."
table=scripts/nightly-shards.txt
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
fail=0

grep -v '^#' "$table" | awk -F'\t' '{ print $2 "\t" $4 }' | sort > "$work/table"
grep -v '^#' "$table" | awk -F'\t' '$1 !~ /^[1-6]$/ || NF != 4 { print }' > "$work/malformed"
if [ -s "$work/malformed" ]; then
    echo "check-nightly-shards: rows that are not <shard 1-6> <binary> <cpu_s> <test>:" >&2
    cat "$work/malformed" >&2
    fail=1
fi
uniq -d "$work/table" > "$work/twice"
if [ -s "$work/twice" ]; then
    echo "check-nightly-shards: rows listed twice:" >&2
    cat "$work/twice" >&2
    fail=1
fi

: > "$work/exists"
for file in sim/tests/*.rs; do
    binary=$(basename "$file" .rs)
    cargo test -q -p ananke-sim --all-features --test "$binary" -- --list 2> "$work/err" |
        sed -n 's/: test$//p' | awk -v binary="$binary" '{ print binary "\t" $0 }' >> "$work/exists" ||
        { echo "check-nightly-shards: could not list the tests of sim/tests/$binary.rs:" >&2; cat "$work/err" >&2; exit 1; }
done
sort -o "$work/exists" "$work/exists"
comm -23 "$work/table" "$work/exists" > "$work/stale"
comm -13 "$work/table" "$work/exists" > "$work/missing"
if [ -s "$work/stale" ]; then
    echo "check-nightly-shards: the table names tests that do not exist (renamed or removed?):" >&2
    cat "$work/stale" >&2
    fail=1
fi
if [ -s "$work/missing" ]; then
    echo "check-nightly-shards: tests in no shard of $table (name one for each):" >&2
    cat "$work/missing" >&2
    fail=1
fi

cargo test -q --workspace --all-features --all-targets -- --list 2> "$work/err" |
    sed -n 's/: test$//p' | sort > "$work/everywhere" ||
    { echo "check-nightly-shards: could not list the workspace's tests:" >&2; cat "$work/err" >&2; exit 1; }
cut -f2 "$work/table" | sort > "$work/names"
comm -12 <(sort -u "$work/names") <(sort -u "$work/everywhere") |
    while IFS= read -r name; do
        in_table=$(grep -cxF "$name" "$work/names")
        in_workspace=$(grep -cxF "$name" "$work/everywhere")
        if [ "$in_workspace" -ne "$in_table" ]; then
            echo "$name"
        fi
    done > "$work/shadowed"
if [ -s "$work/shadowed" ]; then
    echo "check-nightly-shards: names in the table that also name a test outside it, which the rest job would skip:" >&2
    cat "$work/shadowed" >&2
    fail=1
fi

# The six `# shard N: X cpu s` headers are the table's own summary and nothing recomputed
# them: PROPOSED D-086 added four rows, moved four shards' totals and left all six headers
# reading the figures from before, which the prose two lines above them then contradicted.
# The gate could not see it, so it checks it now. A tenth of a CPU second of slack absorbs
# the rounding of the rows themselves.
# PROPOSED(D-086): the shard headers are checked against the rows they summarise.
for shard in 1 2 3 4 5 6; do
    declared=$(sed -n "s/^# shard $shard: \([0-9.]*\) cpu s\$/\1/p" "$table")
    if [ -z "$declared" ]; then
        echo "check-nightly-shards: no '# shard $shard: <cpu_s> cpu s' header in $table" >&2
        fail=1
        continue
    fi
    computed=$(grep -v '^#' "$table" | awk -F'\t' -v s="$shard" '$1 == s { t += $3 } END { printf "%.1f", t }')
    if ! awk -v a="$declared" -v b="$computed" 'BEGIN { exit ((a - b) < 0.11 && (b - a) < 0.11) ? 0 : 1 }'; then
        echo "check-nightly-shards: shard $shard's header says $declared cpu s, its rows total $computed" >&2
        fail=1
    fi
done

if [ "$fail" -ne 0 ]; then
    exit 1
fi
echo "check-nightly-shards: $(wc -l < "$work/table" | tr -d ' ') tests in six shards, none missing, none twice"
