#!/usr/bin/env bash
# The pre-merge tier (D-040): every sweep of the current phase at a thousand seeds, and
# the released phases' at CI's hundred (PROPOSED D-094), in release, seeds in parallel,
# on the machine in front of you. The gate's twenty seeds catch the
# shallow bugs and CI's hundred the next layer; this is the layer under that, in
# about a quarter of an hour. Ten thousand seeds are the nightly's on GitHub, not a
# laptop's job. Every sweep's catch rates are printed (`--nocapture`), so the numbers
# a branch is merged on are in the terminal. Run it on a branch before asking for
# the merge; the gate still precedes every commit.
#
# It prints the machine's state before and after, and its own wall time (D-070): a
# premerge figure means nothing on its own, and one tree has run in 613 s and in
# 1 473 s on this laptop two days apart. A run that was slow because the machine was
# on battery, throttled, or already busy should say so in its own output rather than
# leave a number nobody can compare a week later.
set -euo pipefail
cd "$(dirname "$0")/.."
export RUSTFLAGS="-D warnings"
export ANANKE_SEEDS="${ANANKE_SEEDS:-1000}"
# The released phases' sweeps — Phase 1's storage sweeps and Phase 2's one-group Raft
# sweeps, each with a ten-thousand-seed nightly of its own — run at CI's hundred here, so
# the thousand is the current phase's (PROPOSED D-094). Only this script sets it: the
# gate, CI and the nightly leave it unset and run those sweeps at their own count.
export ANANKE_RELEASED_SEEDS="${ANANKE_RELEASED_SEEDS:-100}"

# The load, the power source and any thermal pressure, in one line, on macOS or on
# Linux. Whatever a machine does not report reads `unknown`, never nothing.
machine_state() {
    local load power thermal
    load=$(uptime | sed 's/.*averages*: *//' | tr -s ' ' | tr ' ' '/')
    if command -v pmset > /dev/null 2>&1; then
        power=$(pmset -g batt 2>/dev/null | sed -n "s/Now drawing from '\(.*\)'/\1/p")
        thermal=$(pmset -g therm 2>/dev/null |
            sed -n 's/.*CPU_Speed_Limit[ \t]*=[ \t]*\([0-9]*\).*/CPU speed limit \1%/p' | head -1)
        if [ -z "${thermal:-}" ] && pmset -g therm 2>/dev/null | grep -qi 'no thermal warning'; then
            thermal="no thermal warning recorded"
        fi
    elif [ -d /sys/class/power_supply ]; then
        # A machine that lists no supply at all — a container, a VM — reads `unknown`
        # (D-070), not `Battery Power`: the absence of an `online` flag says nothing.
        # PROPOSED(D-094): found by the measurement in this entry.
        if ls /sys/class/power_supply/*/online > /dev/null 2>&1; then
            if grep -qs 1 /sys/class/power_supply/A*/online; then power="AC Power"; else power="Battery Power"; fi
        fi
        thermal=$(awk '{printf "thermal zone 0 at %.0f C", $1 / 1000}' \
            /sys/class/thermal/thermal_zone0/temp 2>/dev/null)
    fi
    printf 'load %s, %s, %s' "$load" "${power:-unknown power source}" "${thermal:-unknown thermal state}"
}

cpu=unknown
cores=$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo unknown)
if command -v sysctl > /dev/null 2>&1 && sysctl -n machdep.cpu.brand_string > /dev/null 2>&1; then
    cpu=$(sysctl -n machdep.cpu.brand_string)
elif [ -r /proc/cpuinfo ]; then
    cpu=$(sed -n 's/^model name[ \t]*: *//p' /proc/cpuinfo | head -1)
fi

started=$(date +%s)
echo "premerge: $(uname -srm), $cpu, $cores cores"
echo "premerge: before, $(machine_state)"
# The state after matters most when the run was slow or failed, so the sweeps' status is
# kept and reported after that line rather than ending the script at the failure.
status=0
cargo test --workspace --all-features --release --all-targets -- --nocapture || status=$?
echo "premerge: after, $(machine_state)"
if [ "$status" -ne 0 ]; then
    echo "premerge: FAILED at $ANANKE_SEEDS seeds, the released phases' sweeps at $ANANKE_RELEASED_SEEDS, after $(($(date +%s) - started)) s"
    exit "$status"
fi
echo "premerge: green at $ANANKE_SEEDS seeds, the released phases' sweeps at $ANANKE_RELEASED_SEEDS, in $(($(date +%s) - started)) s"
